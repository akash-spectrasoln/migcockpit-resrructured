"""
Migration router — validate, execute, status, cancel, health.
Delegates to services: orchestrator, planner, models.
Job state is in Redis (shared with Celery worker) or in-memory fallback.
"""

import asyncio
from datetime import datetime
import logging
import re
from typing import Any, Optional
import uuid

from fastapi import APIRouter, HTTPException, Request, status
import httpx
from models import (
    JobStatus,
    MigrationRequest,
    MigrationResponse,
    MigrationStatus,
)
from orchestrator import (
    PushdownExecutionError,
    execute_pipeline_pushdown,
)
from state.job_store import get_job, list_job_ids, set_job, update_job

logger = logging.getLogger(__name__)

WEBSOCKET_SERVICE_URL = "http://localhost:8004"

router = APIRouter(tags=["migration"])


def _normalize_config_keys(config: dict[str, Any]) -> dict[str, Any]:
    """Normalize camelCase/snake_case config keys used across Django/FastAPI."""
    if not isinstance(config, dict):
        return {}
    out = dict(config)
    if "source_configs" not in out and isinstance(out.get("sourceConfigs"), dict):
        out["source_configs"] = out["sourceConfigs"]
    if "destination_configs" not in out and isinstance(out.get("destinationConfigs"), dict):
        out["destination_configs"] = out["destinationConfigs"]
    if "connection_config" not in out and isinstance(out.get("connectionConfig"), dict):
        out["connection_config"] = out["connectionConfig"]
    return out


def _rewrite_saved_plan_for_job(plan_data: dict[str, Any], job_id: str) -> Optional[dict[str, Any]]:
    """
    Validate and rewrite cached plan SQL for the current job.
    Returns rewritten plan dict when safe to reuse, else None.
    """
    if not isinstance(plan_data, dict):
        return None

    # Reject template placeholders that indicate broken/unrendered SQL.
    def _has_placeholders(sql: str) -> bool:
        return "{join_sql}" in sql or "{_quote_staging_table" in sql

    def _has_forbidden_source_select_star(sql: str) -> bool:
        if not isinstance(sql, str):
            return False
        compact = " ".join(sql.strip().split())
        return bool(
            re.search(
                r"(?i)create\s+table\s+.+?\s+as\s+select\s+\*\s+from\s+",
                compact,
            )
        )

    old_job = str(plan_data.get("job_id") or "").strip()
    if not old_job:
        return None

    old_job_token = old_job.replace("-", "_")
    new_job_token = job_id.replace("-", "_")

    rewritten = dict(plan_data)
    rewritten["job_id"] = job_id

    def _rewrite_sql(sql: str) -> Optional[str]:
        if not isinstance(sql, str):
            return None
        if _has_placeholders(sql):
            return None
        if _has_forbidden_source_select_star(sql):
            return None
        # Rebind old cached staging table names to current job token.
        return sql.replace(f"job_{old_job_token}_", f"job_{new_job_token}_")

    levels = []
    for lvl in plan_data.get("levels", []) or []:
        qlist = []
        for q in lvl.get("queries", []) or []:
            new_sql = _rewrite_sql(q.get("sql"))
            if not new_sql:
                return None
            q2 = dict(q)
            q2["sql"] = new_sql
            qlist.append(q2)
        lvl2 = dict(lvl)
        lvl2["queries"] = qlist
        levels.append(lvl2)
    rewritten["levels"] = levels

    cleanup_sql = rewritten.get("cleanup_sql")
    if isinstance(cleanup_sql, str):
        new_cleanup = _rewrite_sql(cleanup_sql)
        if not new_cleanup:
            return None
        rewritten["cleanup_sql"] = new_cleanup

    for key in ("final_insert_sql", "destination_create_sql"):
        val = rewritten.get(key)
        if isinstance(val, str):
            new_val = _rewrite_sql(val)
            if not new_val:
                return None
            rewritten[key] = new_val

    for key in ("final_inserts", "destination_creates"):
        arr = rewritten.get(key)
        if isinstance(arr, list):
            out = []
            for s in arr:
                new_s = _rewrite_sql(s)
                if not new_s:
                    return None
                out.append(new_s)
            rewritten[key] = out

    # Safety net: reject unresolved placeholders anywhere after rewrite.
    rewritten_blob = str(rewritten)
    if re.search(r"\{[^{}]+\}", rewritten_blob):
        return None

    return rewritten

async def broadcast_update(job_id: str, message: dict[str, Any]) -> bool:
    """Broadcast update to WebSocket server."""
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            await client.post(f"{WEBSOCKET_SERVICE_URL}/broadcast/{job_id}", json=message)
            return True
    except Exception as e:
        logger.debug(f"Broadcast failed for {job_id}: {e}")
        return False

def update_migration_progress(
    job_id: str,
    step: str,
    progress: float,
    *,
    current_level: Optional[int] = None,
    total_levels: Optional[int] = None,
    level_status: Optional[str] = None,
):
    data = {"progress": max(0.0, min(100.0, float(progress))), "current_step": step}
    if current_level is not None:
        data["current_level"] = current_level
    if total_levels is not None:
        data["total_levels"] = total_levels
    if level_status is not None:
        data["level_status"] = level_status
    update_job(job_id, **data)

def update_node_progress(
    job_id: str, node_id: str, status: str, progress: float, message: Optional[str] = None
):
    job = get_job(job_id) or {}
    node_progress_map = job.get("node_progress") or {}
    progress_clamped = max(0.0, min(100.0, float(progress)))
    node_progress_map[node_id] = {
        "node_id": node_id,
        "status": status,
        "progress": progress_clamped,
        "message": message,
    }
    update_job(job_id, node_progress=node_progress_map)

async def execute_migration_pipeline(
    job_id: str,
    canvas_id: int,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    config: dict[str, Any],
    execution_plan: Optional[dict[str, Any]] = None,
):
    """Execute migration pipeline using SQL pushdown (service layer). Updates job_store (Redis) so GET /status works.
    When execution_plan (dict) is provided, it is deserialized and passed to execute_pipeline_pushdown so compilation is skipped."""
    import time as _t
    _t0 = _t.perf_counter()
    logger.info(
        f"[PIPELINE] ENTER job={job_id} canvas={canvas_id} "
        f"nodes={len(nodes)} edges={len(edges)} "
        f"saved_plan={'YES' if execution_plan else 'NO'} t=0"
    )
    try:
        update_job(
            job_id,
            status=JobStatus.RUNNING,
            current_step="Initializing SQL Pushdown",
        )
        await broadcast_update(
            job_id,
            {"type": "status", "status": "running", "progress": 0.0, "current_step": "Initializing SQL Pushdown"},
        )

        seen_edges = set()
        unique_edges = []
        for edge in edges:
            key = (edge.get("source"), edge.get("target"))
            if key not in seen_edges:
                seen_edges.add(key)
                unique_edges.append(edge)
        edges = unique_edges

        async def pushdown_progress(step: str, progress: float):
            update_migration_progress(job_id, step, progress)
            await broadcast_update(
                job_id, {"type": "status", "status": "running", "progress": progress, "current_step": step}
            )

        config_with_canvas = {**config, "canvas_id": canvas_id}
        plan_obj = None
        if execution_plan:
            from planner import deserialize_plan
            _td = _t.perf_counter()
            logger.info(f"[PIPELINE] deserialize START job={job_id} t={_td-_t0:.2f}s")
            plan_obj = deserialize_plan(execution_plan)
            logger.info(f"[PIPELINE] deserialize DONE job={job_id} levels={len(plan_obj.levels)} queries={plan_obj.total_queries} elapsed={_t.perf_counter()-_td:.2f}s")
        else:
            logger.info(f"[PIPELINE] No saved plan — pushdown will build (SQL compile) job={job_id} t={_t.perf_counter()-_t0:.2f}s")
        _tpush = _t.perf_counter()
        logger.info(f"[PIPELINE] execute_pipeline_pushdown START job={job_id} t={_tpush-_t0:.2f}s")
        result = await execute_pipeline_pushdown(
            job_id=job_id,
            nodes=nodes,
            edges=edges,
            config=config_with_canvas,
            progress_callback=pushdown_progress,
            execution_plan=plan_obj,
        )
        logger.info(f"[PIPELINE] execute_pipeline_pushdown DONE job={job_id} elapsed={_t.perf_counter()-_tpush:.2f}s total={_t.perf_counter()-_t0:.2f}s")
        update_job(
            job_id,
            status=JobStatus.COMPLETED,
            progress=100.0,
            current_step="Data fully loaded to destination",
            stats=result,
            level_status="complete",
        )
        await broadcast_update(
            job_id,
            {
                "type": "complete",
                "status": "completed",
                "progress": 100.0,
                "current_step": "Data fully loaded to destination",
                "stats": result,
                "level_status": "complete",
            },
        )
    except PushdownExecutionError as e:
        logger.error(f"[PUSHDOWN] FAILED - Job: {job_id}: {e}")
        update_job(job_id, status=JobStatus.FAILED, error=str(e), current_step="Failed")
        await broadcast_update(job_id, {"type": "error", "status": "failed", "error": str(e)})
    except Exception as e:
        logger.error(f"[PUSHDOWN] UNEXPECTED ERROR - Job: {job_id}: {e}", exc_info=True)
        update_job(job_id, status=JobStatus.FAILED, error=str(e), current_step="Failed")
        await broadcast_update(job_id, {"type": "error", "status": "failed", "error": str(e)})

@router.post("/validate")
async def validate_pipeline_endpoint(request: Request):
    """Validate pipeline and create execution plan. Delegates to planner service."""
    try:
        body = await request.json()
        job_id = body.get("job_id", "validate_unknown")
        nodes = body.get("nodes", [])
        edges = body.get("edges", [])
        config = _normalize_config_keys(body.get("config", {}))
        canvas_id = body.get("canvas_id") or config.get("canvas_id")
        connection_config = (
            body.get("connection_config")
            or config.get("connection_config")
            or config.get("connectionConfig")
        )
        persist = body.get("persist", False)

        seen_edges = set()
        unique_edges = []
        for edge in edges:
            key = (edge.get("source"), edge.get("target"))
            if key not in seen_edges:
                seen_edges.add(key)
                unique_edges.append(edge)
        edges = unique_edges

        from planner import (
            PipelineValidationError,
            build_execution_plan,
            compute_plan_hash,
            detect_materialization_points,
            save_execution_plan_to_db,
            validate_pipeline,
        )
        # Ensure source nodes always have normalized node_output_metadata so
        # plan build and SQL compilation don't fail when upstream metadata is missing.
        from orchestrator.pipeline_executor import (
            _ensure_source_metadata_for_plan,
            _get_customer_connection_if_available,
            _load_node_metadata_from_cache,
            _enrich_config_with_metadata,
        )

        try:
            validate_pipeline(nodes, edges)
        except PipelineValidationError as e:
            return {"success": False, "errors": [str(e)], "metadata": {}}

        try:
            from planner.metadata_generator import generate_all_node_metadata
            nodes_dict = {n.get("id"): n for n in nodes if n.get("id")}
            generate_all_node_metadata(
                nodes=nodes_dict, edges=edges, canvas_id=canvas_id,
                connection_config=connection_config, config=config,
            )
        except Exception as e:
            logger.warning(f"[VALIDATE] Metadata generation failed: {e}")

        if connection_config and canvas_id is not None:
            try:
                temp_config = {"connection_config": connection_config, "canvas_id": canvas_id}
                conn = _get_customer_connection_if_available(temp_config)
                if conn:
                    try:
                        _load_node_metadata_from_cache(conn, int(canvas_id), config)
                        _enrich_config_with_metadata(conn, nodes, config)
                        _ensure_source_metadata_for_plan(nodes, config)
                    finally:
                        if not conn.closed:
                            conn.close()
            except Exception as e:
                logger.warning(f"[VALIDATE] Could not load node_output_metadata: {e}")
        else:
            _ensure_source_metadata_for_plan(nodes, config)

        pushdown_plan = {}
        pushed_filter_nodes = []
        try:
            from planner.filter_pushdown import analyze_filter_pushdown
            pushdown_config = {**config, "canvas_id": canvas_id, "connection_config": connection_config}
            pushdown_result = analyze_filter_pushdown(nodes, edges, pushdown_config)
            if isinstance(pushdown_result, dict) and "plan" in pushdown_result:
                pushdown_plan = pushdown_result.get("plan", {})
                pushed_filter_nodes = pushdown_result.get("fully_pushed_nodes", [])
            else:
                pushdown_plan = pushdown_result
        except Exception as e:
            logger.warning(f"[VALIDATE] Filter pushdown analysis failed: {e}")

        linear_branches = config.get("linear_branches", True)
        materialization_points, shared_source_terminals = detect_materialization_points(
            nodes, edges, job_id, linear_branches=linear_branches, config=config
        )
        config_with_pushdown = {
            **config,
            "filter_pushdown_plan": pushdown_plan,
            "pushed_filter_nodes": pushed_filter_nodes,
        }
        execution_plan = build_execution_plan(
            nodes, edges, materialization_points, config_with_pushdown, job_id,
            shared_source_terminals=shared_source_terminals,
        )
        # Hash must match Execute: do not include materialization_points (Execute does not have them yet)
        plan_hash = compute_plan_hash(nodes, edges, materialization_points=None, config=config_with_pushdown)

        plan_persisted = False
        if persist and connection_config and canvas_id:
            plan_persisted = save_execution_plan_to_db(
                connection_config=connection_config,
                canvas_id=str(canvas_id),
                plan_hash=plan_hash,
                plan_obj=execution_plan,
            )

        return {
            "success": True,
            "errors": [],
            "metadata": {
                "job_id": job_id,
                "canvas_id": canvas_id,
                "staging_schema": execution_plan.staging_schema,
                "levels": len(execution_plan.levels),
                "total_queries": execution_plan.total_queries,
                "plan_hash": plan_hash,
                "materialization_points": len(materialization_points),
                "plan_persisted": plan_persisted,
                "validated_at": datetime.now().isoformat(),
            },
        }
    except Exception as e:
        logger.error(f"[VALIDATE] Validation FAILED: {e}", exc_info=True)
        return {"success": False, "errors": [str(e)], "metadata": {}}

def _enqueue_pipeline_task(
    job_id: str,
    canvas_id: int,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    config: dict[str, Any],
    execution_plan: Optional[dict[str, Any]] = None,
) -> None:
    """Enqueue pipeline execution via Celery. Raises on failure so caller can fall back to BackgroundTasks."""
    from tasks import run_migration_pipeline_task
    run_migration_pipeline_task.delay(job_id, canvas_id, nodes, edges, config, execution_plan)
    logger.info(f"[PUSHDOWN] Enqueued Celery task for job {job_id}")

@router.post("/execute", response_model=MigrationResponse, status_code=status.HTTP_202_ACCEPTED)
async def execute_migration(request: MigrationRequest):
    """Start migration job. Pipeline runs via asyncio.create_task (more reliable than BackgroundTasks).
    If no execution_plan is provided, computes plan hash and reuses saved plan from DB when unchanged."""
    import time as _time
    t0 = _time.perf_counter()
    logger.info(f"[EXECUTE] ENTER canvas={request.canvas_id} t=0")
    try:
        config = _normalize_config_keys(request.config or {})
        # Preserve top-level connection payloads when client doesn't nest them under config.
        if not config.get("connection_config"):
            top_cc = request.connection_config or request.connectionConfig
            if isinstance(top_cc, dict):
                config["connection_config"] = top_cc
        if not config.get("destination_configs"):
            top_dc = request.destination_configs or request.destinationConfigs
            if isinstance(top_dc, dict):
                config["destination_configs"] = top_dc
        logger.info(
            "[EXECUTE] config debug: source_configs=%s destination_configs=%s has_connection_config=%s keys(sample)=%s",
            len(config.get("source_configs", {}) or {}),
            len(config.get("destination_configs", {}) or {}),
            bool(config.get("connection_config") or config.get("connectionConfig")),
            list((config.get("source_configs", {}) or {}).keys())[:8],
        )
        job_id = (request.job_id and str(request.job_id).strip()) or (config.get("job_id") and str(config.get("job_id")).strip()) or None
        if not job_id:
            job_id = str(uuid.uuid4())
        canvas_id = request.canvas_id
        logger.info(f"[EXECUTE] job_id={job_id} t={_time.perf_counter()-t0:.2f}s")

        # Only block if this exact job is RUNNING (pipeline actively executing).
        # PENDING/COMPLETED/FAILED: allow (re)run so pipeline actually executes.
        existing = get_job(job_id)
        if existing and existing.get("status") == JobStatus.RUNNING:
            logger.info(f"[EXECUTE] Blocked: job={job_id} already RUNNING")
            return MigrationResponse(
                job_id=job_id,
                status=JobStatus.RUNNING,
                message="Job already running",
            )
        # Only block when a job for this canvas is actually RUNNING. If PENDING, it may be stuck
        # (e.g. BackgroundTasks not yet started), so allow this new job and run the pipeline.
        for existing_job_id in list_job_ids():
            job_data = get_job(existing_job_id)
            if job_data and job_data.get("canvas_id") == canvas_id and job_data.get("status") == JobStatus.RUNNING:
                logger.info(f"[EXECUTE] Blocked: canvas={canvas_id} already has running job={existing_job_id}")
                return MigrationResponse(
                    job_id=existing_job_id,
                    status=JobStatus.RUNNING,
                    message="Job already running for this canvas",
                )

        set_job(job_id, {
            "status": JobStatus.PENDING,
            "progress": 0.0,
            "current_step": None,
            "error": None,
            "stats": {},
            "canvas_id": canvas_id,
            "current_level": None,
            "total_levels": None,
            "level_status": None,
            "node_progress": {},
        })

        # Execute = run saved plan from CANVAS_CACHE (no SQL/CTE compilation) OR build+run when no plan saved.
        t_plan = _time.perf_counter()
        logger.info(f"[EXECUTE] PLAN_RESOLVE_START job={job_id} canvas={canvas_id} nodes={len(request.nodes)} edges={len(request.edges)} t={t_plan-t0:.2f}s")
        execution_plan = request.execution_plan
        if execution_plan is not None:
            logger.info("[EXECUTE] execution_plan provided in request payload — will use it directly")
        elif canvas_id is not None:
            from planner import compute_plan_hash, get_latest_plan
            t_hash = _time.perf_counter()
            current_hash = compute_plan_hash(request.nodes, request.edges, materialization_points=None, config=config)
            logger.info(f"[EXECUTE] hash_computed={current_hash[:16]} t={_time.perf_counter()-t_hash:.2f}s")

            # Collect candidate DBs to look for CANVAS_CACHE.execution_plans
            configs_to_try = []
            cc = config.get("connection_config") or config.get("connectionConfig")
            if cc:
                configs_to_try.append(("customer", cc))
                logger.info(f"[EXECUTE] will check customer DB: host={cc.get('host')} db={cc.get('database')}")
            else:
                logger.warning("[EXECUTE] no connection_config in config — will only try destination DBs")

            for i, dest in enumerate(list(config.get("destination_configs", {}).values())):
                dc = dest.get("connection_config") or dest.get("connectionConfig")
                if dc:
                    configs_to_try.append((f"destination_{i}", dc))
                    logger.info(f"[EXECUTE] will check destination_{i} DB: host={dc.get('host')} db={dc.get('database')}")

            if not configs_to_try:
                logger.warning("[EXECUTE] no DB config available — cannot load saved plan; will build plan (SQL compilation)")

            for label, conn_cfg in configs_to_try:
                t_db = _time.perf_counter()
                logger.info(f"[EXECUTE] get_latest_plan START from {label} canvas={canvas_id} t={t_db-t0:.2f}s")
                saved = get_latest_plan(conn_cfg, str(canvas_id))
                logger.info(f"[EXECUTE] get_latest_plan DONE from {label} elapsed={_time.perf_counter()-t_db:.2f}s")
                if saved is None:
                    logger.info(f"[EXECUTE]   → no row in CANVAS_CACHE.execution_plans for canvas {canvas_id} on {label}")
                else:
                    stored_hash = saved.get("plan_hash", "")
                    logger.info(f"[EXECUTE]   → found row: stored_hash={stored_hash[:16]}  current_hash={current_hash[:16]}  match={stored_hash == current_hash}")
                    if stored_hash == current_hash:
                        cached_plan = saved.get("plan_data")
                        rebound_plan = _rewrite_saved_plan_for_job(cached_plan, job_id)
                        if rebound_plan is not None:
                            logger.info(f"[EXECUTE] ✅ Reusing saved plan (rebinding SQL to current job) canvas={canvas_id} source={label}")
                            execution_plan = rebound_plan
                            break
                        logger.warning(
                            "[EXECUTE] Cached plan matched hash but is invalid/stale (placeholders or bad SQL); recompiling"
                        )
                    else:
                        logger.info("[EXECUTE]   → hash mismatch — trying next DB or will recompile")

            if execution_plan is None:
                logger.info(
                    f"[EXECUTE] ⚠️  No saved plan matched for canvas {canvas_id}. "
                    "Will build execution plan (SQL compilation runs) then run and save it."
                )
        else:
            logger.warning("[EXECUTE] canvas_id is None — skipping plan load")

        # Dispatch pipeline: use asyncio.create_task so pipeline runs immediately in event loop.
        # BackgroundTasks can fail to run in some uvicorn/worker setups; create_task is more reliable.
        t_dispatch = _time.perf_counter()
        logger.info(f"[EXECUTE] DISPATCH_START job={job_id} plan_cached={execution_plan is not None} t={t_dispatch-t0:.2f}s")
        logger.info(f"[EXECUTE] Scheduling pipeline via asyncio.create_task job={job_id} plan_cached={execution_plan is not None}")

        def _on_pipeline_done(task: asyncio.Task):
            try:
                task.result()
            except Exception as e:
                logger.error(f"[EXECUTE] Pipeline task failed for job={job_id}: {e}", exc_info=True)

        pipeline_task = asyncio.create_task(
            execute_migration_pipeline(
                job_id,
                request.canvas_id,
                request.nodes,
                request.edges,
                config,
                execution_plan,
            )
        )
        pipeline_task.add_done_callback(_on_pipeline_done)
        logger.info(f"[EXECUTE] EXIT 202 job={job_id} total={_time.perf_counter()-t0:.2f}s")
        return MigrationResponse(
            job_id=job_id,
            status=JobStatus.PENDING,
            message=f"Migration job started. Job ID: {job_id}",
        )
    except Exception as e:
        logger.error(f"[MIGRATION] Error starting migration: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.get("/{job_id}/status", response_model=MigrationStatus)
async def get_migration_status(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job {job_id} not found")
    node_progress_map = job.get("node_progress") or {}
    node_progress_list = list(node_progress_map.values())
    progress_clamped = max(0.0, min(100.0, float(job.get("progress", 0.0))))
    return MigrationStatus(
        job_id=job_id,
        status=job.get("status", JobStatus.PENDING),
        progress=progress_clamped,
        current_step=job.get("current_step"),
        error=job.get("error"),
        stats=job.get("stats"),
        node_progress=node_progress_list,
        current_level=job.get("current_level"),
        total_levels=job.get("total_levels"),
        level_status=job.get("level_status"),
    )

@router.post("/{job_id}/cancel")
async def cancel_migration(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job {job_id} not found")
    if job.get("status") not in ("pending", "running"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Cannot cancel job in {job.get('status')} status")
    update_job(job_id, status=JobStatus.CANCELLED, current_step="Cancelled")
    await broadcast_update(job_id, {"type": "cancelled", "status": "cancelled"})
    return {"message": f"Job {job_id} cancelled successfully", "job_id": job_id, "status": JobStatus.CANCELLED}

@router.get("/health")
async def health_check():
    return {"status": "healthy", "service": "migration_service"}
