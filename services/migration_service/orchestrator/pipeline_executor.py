"""
SQL Pushdown Pipeline Executor
Executes deterministic SQL-based ETL with zero Python row processing.
Integrated with execution state tracking and real-time WebSocket progress.
"""

from datetime import datetime
import hashlib
import json
import logging
import re
import time
from typing import Any, Callable, Optional

from orchestrator.execution_state import NodePhase, get_execution_store
from orchestrator.progress_emitter import get_ws_emitter
from planner import (
    ExecutionPlan,
    build_execution_plan,
    compute_plan_hash,
    detect_materialization_points,
    save_execution_plan_to_db,
    validate_pipeline,
)
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import Json

logger = logging.getLogger(__name__)

def _get_source_configs(config: dict[str, Any]) -> dict[str, Any]:
    """Return source configs with tolerant key handling."""
    source_configs = config.get("source_configs")
    if isinstance(source_configs, dict):
        return source_configs
    source_configs = config.get("sourceConfigs")
    if isinstance(source_configs, dict):
        return source_configs
    return {}

class PushdownExecutionError(Exception):
    """Raised when SQL pushdown execution fails."""
    pass

async def execute_pipeline_pushdown(
    job_id: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    config: dict[str, Any],
    progress_callback: Optional[Callable[[str, float], None]] = None,
    execution_plan: Optional[ExecutionPlan] = None,
) -> dict[str, Any]:
    """
    Execute pipeline using SQL pushdown strategy with real-time progress tracking.

    When execution_plan is provided (e.g. loaded from DB after Validate), compilation
    is skipped and the plan is executed directly. Otherwise the plan is built from
    nodes/edges (validate, materialization, build plan, then execute).

    Args:
        job_id: Unique job identifier
        nodes: List of pipeline nodes (used for state and source_table_map; required even when execution_plan is set)
        edges: List of pipeline edges
        config: Configuration with source/destination connections
        progress_callback: Optional callback for progress updates
        execution_plan: Optional pre-built plan; when set, validation and compilation are skipped

    Returns:
        Execution result with stats
    """
    start_time = time.time()
    _t_last = start_time

    def _log_phase(phase: str, done: bool = False):
        nonlocal _t_last
        elapsed = time.time() - _t_last
        logger.info(f"[PUSHDOWN] Job {job_id}: {phase} {'done' if done else 'start'} elapsed={elapsed:.2f}s total={time.time()-start_time:.2f}s")
        if done:
            _t_last = time.time()

    execution_store = get_execution_store()
    ws_emitter = get_ws_emitter()

    node_ids = [node["id"] for node in nodes]
    _last_phase = None
    db_connection = None
    customer_conn = None
    execution_conn = None

    try:
        # PHASE 3: Get database connection (always needed)
        _last_phase = "PHASE_3_DB_CONNECTION"
        _log_phase(_last_phase)
        db_connection = _get_destination_connection(config)
        _log_phase(_last_phase, done=True)

        if execution_plan is None:
            # PHASE 1: Validation
            _last_phase = "PHASE_1_VALIDATION"
            _log_phase(_last_phase)
            validate_pipeline(nodes, edges)
            _log_phase(_last_phase, done=True)

            # PHASE 2: Detect materialization points
            _last_phase = "PHASE_2_MATERIALIZATION"
            linear_branches = config.get("linear_branches", True)
            _log_phase(f"{_last_phase} linear_branches={linear_branches}")
            materialization_points, shared_source_terminals = detect_materialization_points(
                nodes, edges, job_id, linear_branches=linear_branches, config=config
            )
            _log_phase(_last_phase, done=True)

            # PHASE 4: Enrich config with source metadata
            _last_phase = "PHASE_4_ENRICH_METADATA"
            _log_phase(_last_phase)
            source_configs_dbg = _get_source_configs(config)
            logger.info(
                "[PUSHDOWN] Source config debug: total=%s keys(sample)=%s",
                len(source_configs_dbg),
                list(source_configs_dbg.keys())[:8],
            )
            source_node_ids = {
                n["id"] for n in nodes
                if (n.get("type") or n.get("data", {}).get("type") or "").lower().strip().startswith("source")
            }
            customer_conn = _get_customer_connection_if_available(config)
            _enrich_config_with_metadata(customer_conn or db_connection, nodes, config)
            canvas_id = config.get("canvas_id")
            if canvas_id is not None:
                _load_node_metadata_from_cache(
                    customer_conn or db_connection, int(canvas_id), config, source_node_ids=source_node_ids
                )
            _backfill_source_metadata_from_destination(db_connection, nodes, config, source_node_ids)
            _ensure_source_metadata_for_plan(nodes, config)
            # Run filter pushdown so config has filter_pushdown_plan and pushed_filter_nodes
            try:
                from planner.filter_pushdown import analyze_filter_pushdown
                pushdown_result = analyze_filter_pushdown(nodes, edges, config)
                if isinstance(pushdown_result, dict):
                    config["filter_pushdown_plan"] = pushdown_result.get("plan", {})
                    config["pushed_filter_nodes"] = pushdown_result.get("fully_pushed_nodes", [])
            except Exception as e:
                logger.warning("[PUSHDOWN] Filter pushdown analysis failed: %s", e)
            _log_phase(_last_phase, done=True)

            execution_conn = customer_conn or db_connection
            execution_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            # PHASE 5: Build execution plan (SQL compilation — can be slow)
            _last_phase = "PHASE_5_BUILD_PLAN"
            _log_phase(_last_phase)
            execution_plan = build_execution_plan(
                nodes, edges, materialization_points, config, job_id,
                shared_source_terminals=shared_source_terminals
            )
            _log_phase(f"{_last_phase} levels={len(execution_plan.levels)} queries={execution_plan.total_queries}", done=True)

            # Persist execution plan to customer DB for reuse on next Execute
            canvas_id = config.get("canvas_id")
            connection_config = config.get("connection_config") or config.get("connectionConfig")
            if not connection_config and config.get("destination_configs"):
                dest_config = next(iter(config["destination_configs"].values()))
                connection_config = dest_config.get("connection_config") or dest_config.get("connectionConfig")
            if canvas_id is not None and connection_config:
                plan_hash = compute_plan_hash(nodes, edges, materialization_points=None, config=config)
                if save_execution_plan_to_db(
                    connection_config=connection_config,
                    canvas_id=str(canvas_id),
                    plan_hash=plan_hash,
                    plan_obj=execution_plan,
                ):
                    logger.info(f"[PUSHDOWN] Job {job_id}: Execution plan saved to DB for canvas {canvas_id}")
                else:
                    logger.warning(f"[PUSHDOWN] Job {job_id}: Could not save execution plan to DB")
        else:
            # Saved plan from DB: no SQL compilation, no CTE generation — just run the stored SQL.
            customer_conn = _get_customer_connection_if_available(config)
            execution_conn = customer_conn or db_connection
            execution_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            _log_phase(f"SAVED_PLAN levels={len(execution_plan.levels)} queries={execution_plan.total_queries}", done=True)

        # PHASE 6: Initialize execution state (all nodes PENDING) — runs for both built and pre-built plan
        _last_phase = "PHASE_6_INIT_STATE"
        _log_phase(_last_phase)
        state = await execution_store.initialize_execution(
            job_id=job_id,
            node_ids=node_ids,
            total_levels=len(execution_plan.levels)
        )
        _log_phase(_last_phase, done=True)

        # PHASE 7: Start pipeline execution (PENDING → RUNNING)
        _last_phase = "PHASE_7_PIPELINE_START"
        _log_phase(_last_phase)
        await execution_store.start_pipeline(job_id, "Creating staging schema")
        await ws_emitter.emit_pipeline_started(job_id, state)
        _log_phase(_last_phase, done=True)

        # PHASE 8: Create staging schema (in customer DB when available)
        _last_phase = "PHASE_8_CREATE_STAGING_SCHEMA"
        _log_phase(_last_phase)
        _create_staging_schema(execution_conn, execution_plan.staging_schema)
        _log_phase(_last_phase, done=True)

        # Map (schema, table) -> source_node_id so we run source reads on source DB, not execution DB
        source_table_map = _build_source_table_to_node_id_map(nodes, config)

        # PHASE 9: Execute levels sequentially with node-level tracking
        total_levels = len(execution_plan.levels)
        # Context of last executed query (for error logging)
        _last_level_num = None
        _last_query_idx_1based = None
        _last_total_in_level = None
        _last_node_id = None
        _last_sql_preview = None

        for idx, level in enumerate(execution_plan.levels):
            level_num = idx + 1
            level_start = time.time()
            _last_phase = f"PHASE_9_LEVEL_{level_num}_of_{total_levels}"
            _log_phase(f"{_last_phase} queries={len(level.queries)}")

            # Update pipeline step
            await execution_store.update_pipeline_step(
                job_id=job_id,
                current_step=f"Level {level_num}/{total_levels}",
                current_level=level_num,
                total_levels=total_levels,
                level_status="running"
            )

            if progress_callback:
                # Progress from 10% to 90%
                progress = 10 + (idx / total_levels) * 80
                await progress_callback(
                    f"Level {level_num}/{total_levels}: {len(level.queries)} queries",
                    progress
                )

            # Execute queries in level with node tracking
            for query_idx, compiled_sql in enumerate(level.queries):
                query_start = time.time()

                # Determine which node this query belongs to
                current_node_id = level.node_ids[query_idx] if query_idx < len(level.node_ids) else None

                if current_node_id:
                    await execution_store.start_node(
                        job_id=job_id,
                        node_id=current_node_id,
                        phase=NodePhase.TRANSFORM
                    )
                    state = await execution_store.get_state(job_id)
                    if state and current_node_id in state.nodes:
                        await ws_emitter.emit_node_started(job_id, state.nodes[current_node_id])

                sql_hash = hashlib.md5(compiled_sql.sql.encode()).hexdigest()[:8]
                node_short = (current_node_id[:8] + "...") if current_node_id else "n/a"
                logger.info(
                    f"[PUSHDOWN] Job {job_id}: Level {level_num}/{total_levels}, "
                    f"Query {query_idx + 1}/{len(level.queries)}, Node: {node_short}, Hash: {sql_hash}"
                )
                logger.debug(f"[PUSHDOWN] SQL:\n{compiled_sql.sql}")

                _last_level_num = level_num
                _last_query_idx_1based = query_idx + 1
                _last_total_in_level = len(level.queries)
                _last_node_id = current_node_id
                _first_line = compiled_sql.sql.strip().split("\n")[0].strip()
                _last_sql_preview = (_first_line[:200] + "…") if len(_first_line) > 200 else _first_line

                try:
                    rowcount_result = _execute_source_staging_query(
                        execution_conn, compiled_sql.sql, config, nodes, source_table_map
                    )
                    if rowcount_result is not None:
                        rowcount = rowcount_result
                    else:
                        logger.debug("[PUSHDOWN] Running query on execution DB (not a source-staging query)")
                        try:
                            rowcount = _execute_sql(execution_conn, compiled_sql.sql)
                        except Exception as run_err:
                            err_str = str(run_err)
                            if "does not exist" in err_str and "column" in err_str.lower():
                                if "JOIN" in compiled_sql.sql.upper():
                                    rewritten = _rewrite_join_for_actual_columns(
                                        execution_conn, compiled_sql.sql, run_err
                                    )
                                    if rewritten:
                                        logger.info(
                                            "[PUSHDOWN] Join failed (column mismatch), retrying with rewritten SQL"
                                        )
                                        try:
                                            rowcount = _execute_sql(execution_conn, rewritten)
                                        except Exception as rewritten_err:
                                            rewritten_err_str = str(rewritten_err)
                                            if "specified more than once" in rewritten_err_str:
                                                rewritten_unique = _rewrite_create_as_unique_output_columns(rewritten)
                                                if rewritten_unique:
                                                    logger.info(
                                                        "[PUSHDOWN] Rewritten JOIN SQL had duplicate output columns; retrying with unique aliases"
                                                    )
                                                    rowcount = _execute_sql(execution_conn, rewritten_unique)
                                                else:
                                                    forced_alias_sql = _force_alias_create_as_select_columns(rewritten)
                                                    if forced_alias_sql:
                                                        logger.info(
                                                            "[PUSHDOWN] Rewritten JOIN SQL still had duplicate outputs; forcing explicit unique aliases"
                                                        )
                                                        rowcount = _execute_sql(execution_conn, forced_alias_sql)
                                                    else:
                                                        raise
                                            else:
                                                raise
                                    else:
                                        raise
                                else:
                                    # CREATE TABLE AS SELECT from staging (e.g. after join rewrite)
                                    parsed = _parse_create_table_as_select(compiled_sql.sql)
                                    if parsed and parsed["from_schema"] == "staging_jobs":
                                        cursor = execution_conn.cursor()
                                        try:
                                            cursor.execute(
                                                """
                                                SELECT column_name FROM information_schema.columns
                                                WHERE table_schema = %s AND table_name = %s
                                                ORDER BY ordinal_position
                                                """,
                                                (parsed["from_schema"], parsed["from_table"]),
                                            )
                                            actual_cols = [r[0] for r in cursor.fetchall()]
                                        finally:
                                            cursor.close()
                                        if actual_cols:
                                            rewritten_sql = _rewrite_select_for_actual_columns(
                                                parsed["select_sql"],
                                                parsed["from_schema"],
                                                parsed["from_table"],
                                                actual_cols,
                                            )
                                            if rewritten_sql:
                                                full_rewritten = (
                                                    f'CREATE TABLE "{parsed["create_schema"]}"."{parsed["create_table"]}" AS\n'
                                                    f"{rewritten_sql}"
                                                )
                                                logger.info(
                                                    "[PUSHDOWN] Staging SELECT failed (column mismatch), retrying with actual columns"
                                                )
                                                rowcount = _execute_sql(execution_conn, full_rewritten)
                                            else:
                                                raise
                                        else:
                                            raise
                                    else:
                                        raise
                            else:
                                if "specified more than once" in err_str:
                                    rewritten_unique = _rewrite_create_as_unique_output_columns(compiled_sql.sql)
                                    if rewritten_unique:
                                        logger.info(
                                            "[PUSHDOWN] CREATE AS SELECT had duplicate output columns; retrying with unique aliases"
                                        )
                                        rowcount = _execute_sql(execution_conn, rewritten_unique)
                                    else:
                                        forced_alias_sql = _force_alias_create_as_select_columns(compiled_sql.sql)
                                        if forced_alias_sql:
                                            logger.info(
                                                "[PUSHDOWN] CREATE AS SELECT duplicate outputs persisted; forcing explicit unique aliases"
                                            )
                                            rowcount = _execute_sql(execution_conn, forced_alias_sql)
                                        else:
                                            raise
                                else:
                                    raise
                except Exception as query_err:
                    _last_phase = (
                        f"PHASE_9_LEVEL_{level_num}_QUERY_{query_idx + 1}_NODE_{current_node_id[:8] if current_node_id else 'n/a'}"
                    )
                    logger.error(
                        "[PUSHDOWN FAIL] %s | job=%s level=%s query=%s/%s node=%s | %s",
                        _last_phase, job_id, level_num, query_idx + 1, len(level.queries),
                        current_node_id[:8] if current_node_id else "n/a", query_err,
                    )
                    logger.error("[PUSHDOWN FAIL] SQL preview: %s", _last_sql_preview)
                    raise

                query_duration = time.time() - query_start

                if current_node_id:
                    await execution_store.complete_node(
                        job_id=job_id,
                        node_id=current_node_id,
                        success=True
                    )
                    state = await execution_store.get_state(job_id)
                    if state and current_node_id in state.nodes:
                        await ws_emitter.emit_node_completed(
                            job_id,
                            state.nodes[current_node_id],
                            state.overall_progress
                        )
                    if state:
                        await ws_emitter.emit_pipeline_progress(job_id, state)

                _log_query_metrics(
                    job_id=job_id,
                    level=level.level_num,
                    query_idx=query_idx,
                    sql_hash=sql_hash,
                    duration=query_duration,
                    rowcount=rowcount,
                    status="success"
                )

                level_duration = time.time() - level_start
                logger.info(
                    f"[PUSHDOWN] Job {job_id}: Level {level_num}/{total_levels} DONE "
                    f"elapsed={level_duration:.2f}s total={time.time()-start_time:.2f}s"
                )
                _t_last = time.time()

        # PHASE 10: Create destination tables (one per destination)
        dest_creates = getattr(execution_plan, "destination_creates", None) or []
        if not dest_creates and execution_plan.destination_create_sql:
            dest_creates = [execution_plan.destination_create_sql]
        if dest_creates:
            _last_phase = "PHASE_10_DESTINATION_CREATE"
            _log_phase(_last_phase)
            for _idx, create_sql in enumerate(dest_creates):
                if create_sql and create_sql.strip():
                    create_sql = _deduplicate_create_table_columns(create_sql)
                    _ensure_target_schema(execution_conn, create_sql)
                    _execute_sql(execution_conn, create_sql)
            _log_phase(_last_phase, done=True)
        else:
            # Fallback: when destination_creates was empty (e.g. missing metadata), derive CREATE from each INSERT
            final_ins_pre = getattr(execution_plan, "final_inserts", None) or []
            if not final_ins_pre and execution_plan.final_insert_sql:
                final_ins_pre = [execution_plan.final_insert_sql]
            if final_ins_pre:
                _last_phase = "PHASE_10_DESTINATION_CREATE_FALLBACK"
                _log_phase(_last_phase)
                for insert_sql in final_ins_pre:
                    if insert_sql and insert_sql.strip():
                        create_sql = _generate_create_from_insert(execution_conn, insert_sql)
                        if create_sql:
                            create_sql = _deduplicate_create_table_columns(create_sql)
                            _ensure_target_schema(execution_conn, create_sql)
                            _execute_sql(execution_conn, create_sql)
                            logger.info("[PUSHDOWN] Created destination table from INSERT fallback")
                _log_phase(_last_phase, done=True)

        # PHASE 11: INSERT into each destination
        rowcount = 0
        final_ins = getattr(execution_plan, "final_inserts", None) or []
        if not final_ins and execution_plan.final_insert_sql:
            final_ins = [execution_plan.final_insert_sql]
        if final_ins:
            _last_phase = "PHASE_11_FINAL_INSERT"
            _log_phase(_last_phase)
            if progress_callback:
                await progress_callback("Final INSERT to destination", 95)
            insert_start = time.time()
            for insert_sql in final_ins:
                if insert_sql and insert_sql.strip():
                    insert_sql = _deduplicate_insert_columns(insert_sql)
                    _ensure_target_schema(execution_conn, insert_sql)
                    try:
                        rowcount += _execute_sql(execution_conn, insert_sql)
                    except Exception as insert_err:
                        err_str = str(insert_err)
                        if "specified more than once" in err_str:
                            raise  # Deduplication ran; if still failing, re-raise
                        elif "does not exist" in err_str and "column" in err_str.lower():
                            rewritten = _rewrite_final_insert_for_actual_columns(
                                execution_conn, insert_sql, insert_err
                            )
                            if rewritten:
                                logger.info(
                                    "[PUSHDOWN] Final INSERT failed (missing column), rewrote using actual staging columns"
                                )
                                rowcount += _execute_sql(execution_conn, rewritten)
                            else:
                                raise
                        elif "does not exist" in err_str and "relation" in err_str.lower():
                            # Target table missing (e.g. destination_creates was empty); create from staging and retry
                            create_sql = _generate_create_from_insert(execution_conn, insert_sql)
                            if create_sql:
                                logger.info(
                                    "[PUSHDOWN] Final INSERT failed (table missing), creating from staging and retrying"
                                )
                                create_sql = _deduplicate_create_table_columns(create_sql)
                                _ensure_target_schema(execution_conn, create_sql)
                                _execute_sql(execution_conn, create_sql)
                                _ensure_target_schema(execution_conn, insert_sql)
                                rowcount += _execute_sql(execution_conn, insert_sql)
                            else:
                                raise
                        else:
                            raise
            insert_duration = time.time() - insert_start
            _log_phase(f"{_last_phase} rows={rowcount}", done=True)

        # PHASE 12: Cleanup this job's staging tables
        _last_phase = "PHASE_12_CLEANUP"
        _log_phase(_last_phase)
        await execution_store.update_pipeline_step(
            job_id=job_id,
            current_step="Cleanup"
        )
        if execution_plan.cleanup_sql:
            # Execute each DROP separately (psycopg2 runs one statement per execute)
            for stmt in _split_sql_statements(execution_plan.cleanup_sql):
                _execute_sql(execution_conn, stmt)
        _log_phase(_last_phase, done=True)

        # PHASE 13: Mark pipeline as completed
        _last_phase = "PHASE_13_COMPLETE"
        await execution_store.complete_pipeline(job_id, success=True)
        state = await execution_store.get_state(job_id)
        if state:
            await ws_emitter.emit_pipeline_completed(job_id, state)

        total_duration = time.time() - start_time
        if progress_callback:
            await progress_callback("Pipeline completed", 100)

        result = {
            "success": True,
            "job_id": job_id,
            "duration_seconds": total_duration,
            "levels_executed": len(execution_plan.levels),
            "queries_executed": execution_plan.total_queries,
            "rows_inserted": rowcount,
            "execution_mode": "sql_pushdown"
        }
        logger.info(
            f"[PUSHDOWN] Job {job_id}: SUCCESS in {total_duration:.2f}s, "
            f"{result['rows_inserted']} rows"
        )
        return result

    except Exception as e:
        # Mark pipeline as failed and skip remaining nodes
        error_msg = str(e)
        try:
            fail_phase = _last_phase
        except NameError:
            fail_phase = "unknown"
        logger.error(
            "[PUSHDOWN FAIL] Job %s failed at %s: %s",
            job_id, fail_phase, error_msg,
            exc_info=True,
        )
        # Log which query caused the failure (when context was set)
        try:
            if _last_level_num is not None and _last_query_idx_1based is not None:
                node_short = (_last_node_id[:8] + "...") if _last_node_id else "n/a"
                logger.error(
                    "[PUSHDOWN FAIL] Failed query: Level %s, Query %s/%s, Node: %s",
                    _last_level_num, _last_query_idx_1based, _last_total_in_level or "?", node_short,
                )
                if _last_sql_preview:
                    logger.error("[PUSHDOWN FAIL] SQL preview: %s", _last_sql_preview)
        except NameError:
            pass

        await execution_store.fail_remaining_nodes(job_id)
        await execution_store.complete_pipeline(job_id, success=False, error=error_msg)
        state = await execution_store.get_state(job_id)
        if state:
            await ws_emitter.emit_pipeline_failed(job_id, state)

        if progress_callback:
            await progress_callback(f"Execution failed: {error_msg}", 0.0)

        # Cleanup on failure
        try:
            if 'db_connection' in locals() and db_connection and not db_connection.closed:
                db_connection.close()
            if 'customer_conn' in locals() and customer_conn and not customer_conn.closed:
                customer_conn.close()
        except Exception as cleanup_error:
            logger.error(f"[PUSHDOWN] Cleanup failed: {cleanup_error}")

        raise PushdownExecutionError(
            f"Pipeline execution failed at {fail_phase}: {error_msg}"
        ) from e

    finally:
        # Always close connections
        if db_connection and not db_connection.closed:
            db_connection.close()
        if customer_conn and not customer_conn.closed:
            customer_conn.close()

def _get_destination_connection(config: dict[str, Any]):
    """Get PostgreSQL connection to execution/destination database.

    Priority:
    1) top-level connection_config / connectionConfig (customer DB execution target)
    2) first entry from destination_configs / destinationConfigs
    """
    connection_config = (
        config.get("connection_config")
        or config.get("connectionConfig")
    )

    if not isinstance(connection_config, dict) or not connection_config:
        destination_configs = (
            config.get("destination_configs")
            or config.get("destinationConfigs")
            or {}
        )
        dest_config = None
        if isinstance(destination_configs, dict) and destination_configs:
            dest_config = next(iter(destination_configs.values()))
        elif isinstance(destination_configs, list) and destination_configs:
            dest_config = destination_configs[0]

        if isinstance(dest_config, dict):
            connection_config = (
                dest_config.get("connection_config")
                or dest_config.get("connectionConfig")
                or dest_config
            )

    if not isinstance(connection_config, dict) or not connection_config:
        raise PushdownExecutionError("No destination configuration found")

    host = connection_config.get("host") or connection_config.get("hostname")
    database = connection_config.get("database") or connection_config.get("dbname")
    user = connection_config.get("user") or connection_config.get("username")
    if not host or not database or not user:
        raise PushdownExecutionError(
            "Destination configuration is incomplete (host/database/user required)"
        )

    # Connect
    conn = psycopg2.connect(
        host=host,
        port=int(connection_config.get("port", 5432)),
        dbname=database,
        user=user,
        password=connection_config.get("password", "")
    )

    # Set autocommit for DDL operations
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    return conn

def _create_staging_schema(connection, schema_name: str):
    """Create shared staging schema if not exists (single schema for all jobs)."""
    cursor = connection.cursor()
    cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
    cursor.close()

def _parse_create_table_as_select(sql: str) -> Optional[dict[str, Any]]:
    """
    Parse SQL of form: CREATE TABLE "schema"."table" AS\\nSELECT ... FROM "from_schema"."from_table" ...
    Returns dict with create_schema, create_table, select_sql, from_schema, from_table or None.
    """
    sql = sql.strip()
    # CREATE TABLE "schema"."table" AS
    create_m = re.match(r'CREATE\s+TABLE\s+"([^"]+)"\s*\.\s*"([^"]+)"\s+AS\s*\n?(.*)', sql, re.DOTALL | re.IGNORECASE)
    if not create_m:
        return None
    create_schema, create_table, rest = create_m.group(1), create_m.group(2), create_m.group(3).strip()
    if not rest.upper().startswith("SELECT"):
        return None
    # FROM "schema"."table" (optional WHERE/rest)
    from_m = re.search(r'\bFROM\s+"([^"]+)"\s*\.\s*"([^"]+)"\s*', rest, re.IGNORECASE)
    if not from_m:
        return None
    from_schema, from_table = from_m.group(1), from_m.group(2)
    # Do not treat staging_jobs as source table
    if create_schema == "staging_jobs" or from_schema == "staging_jobs":
        pass  # from_schema is the source table's schema
    return {
        "create_schema": create_schema,
        "create_table": create_table,
        "select_sql": rest,
        "from_schema": from_schema,
        "from_table": from_table,
    }

def _build_source_table_to_node_id_map(nodes: list[dict[str, Any]], config: dict[str, Any]) -> dict[tuple[str, str], str]:
    """Build (schema_name, table_name) -> source_node_id from nodes and config."""
    out = {}
    source_configs = _get_source_configs(config)
    for node in nodes:
        ntype = (node.get("type") or node.get("data", {}).get("type") or "").lower().strip()
        # Support variant node types like 'source-postgresql', 'source-mysql', 'sourcepostgresql', etc.
        if not ntype or not ntype.startswith("source"):
            continue
        node_id = node.get("id")
        if not node_id:
            continue
        node_config = node.get("data", {}).get("config", {})
        sc = source_configs.get(node_id, {})
        table = (
            node_config.get("tableName")
            or node_config.get("table_name")
            or sc.get("table_name")
        )
        schema = (
            node_config.get("schema")
            or node_config.get("schema_name")
            or sc.get("schema_name")
            or "public"
        )
        if table:
            # Store exact and normalized keys so lookup is robust to case/quote drift.
            out[(schema, table)] = node_id
            out[(str(schema).strip('"').lower(), str(table).strip('"').lower())] = node_id
    return out

def _get_source_connection(config: dict[str, Any], source_node_id: str):
    """Return a connection to the source DB for the given source node, or None."""
    source_configs = _get_source_configs(config)
    sc = source_configs.get(source_node_id, {})
    if not sc:
        logger.warning(
            "[PUSHDOWN] Source config missing for node=%s available_keys(sample)=%s",
            source_node_id[:8],
            list(source_configs.keys())[:8],
        )
    conn_cfg = (
        sc.get("connection_config")
        or sc.get("connectionConfig")
        or sc  # tolerate flat shape: source_configs[node_id] = {host,port,...}
    )
    if not conn_cfg:
        return None
    try:
        conn = psycopg2.connect(
            host=conn_cfg.get("host") or conn_cfg.get("hostname"),
            port=int(conn_cfg.get("port", 5432)),
            dbname=conn_cfg.get("database"),
            user=conn_cfg.get("user") or conn_cfg.get("username"),
            password=conn_cfg.get("password", "")
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
    except Exception as e:
        redacted = {
            "host": conn_cfg.get("host") or conn_cfg.get("hostname"),
            "port": conn_cfg.get("port"),
            "database": conn_cfg.get("database"),
            "user": conn_cfg.get("user") or conn_cfg.get("username"),
        }
        logger.warning(
            "[PUSHDOWN] Source connection open failed for node %s cfg=%s error=%s",
            source_node_id[:8],
            redacted,
            e,
        )
        return None

# Common PostgreSQL type OIDs for cursor.description type_code -> type name
_PG_TYPE_OID_TO_NAME = {
    16: "boolean",
    17: "bytea",
    20: "bigint",
    21: "smallint",
    23: "integer",
    25: "text",
    114: "json",
    700: "real",
    701: "double precision",
    1042: "character",
    1043: "character varying",
    1082: "date",
    1114: "timestamp without time zone",
    1184: "timestamp with time zone",
    1186: "interval",
    1700: "numeric",
    3802: "jsonb",
}

def _rewrite_select_for_actual_columns(
    select_sql: str, from_schema: str, from_table: str, actual_columns: list[str]
) -> Optional[str]:
    """
    When the plan's SELECT references columns that don't exist (e.g. metadata mismatch, reserved-word
    confusion), rebuild the SELECT using only columns that exist in the source. Maps plan columns
    to actual columns by exact match, then case-insensitive, then common variations (table->table_name).
    """
    actual_lower = {c.lower(): c for c in actual_columns}
    # Common reserved-word / metadata mismatches
    ALIASES = {
        "table": ["table_name", "tablename", "tbl", "table"],
        "table_name": ["table", "tablename", "tbl"],
        "user": ["username", "user_name", "user"],
        "order": ["order_by", "orderby", "order"],
        "group": ["group_name", "groupby", "group"],
        "key": ["id", "key_id", "key"],
    }

    def find_actual(plan_col: str) -> Optional[str]:
        if plan_col in actual_columns:
            return plan_col
        if plan_col.lower() in actual_lower:
            return actual_lower[plan_col.lower()]
        for cand in ALIASES.get(plan_col.lower(), [plan_col]):
            if cand in actual_columns:
                return cand
            if cand.lower() in actual_lower:
                return actual_lower[cand.lower()]
        # Strip node prefix (8 hex chars + underscore): 31f72c84_updated_at -> updated_at
        m = re.match(r"^[a-f0-9]{8}_(.+)$", plan_col, re.IGNORECASE)
        if m:
            base = m.group(1)
            if base in actual_columns:
                return base
            if base.lower() in actual_lower:
                return actual_lower[base.lower()]
        # Reverse: plan_col="connection_id", actual has "31f72c84_connection_id" (join output after rewrite)
        suffix = "_" + plan_col
        suffix_lower = suffix.lower()
        for ac in actual_columns:
            if ac.endswith(suffix) or ac.lower().endswith(suffix_lower):
                return ac
        return None

    # Parse SELECT list: "col" AS "alias" or "col"
    sel_match = re.match(r"SELECT\s+(.+?)\s+FROM\s+", select_sql, re.DOTALL | re.IGNORECASE)
    if not sel_match:
        return None
    select_list_str = sel_match.group(1).strip()
    from_rest = select_sql[sel_match.end() :].strip()
    qualified = f'"{from_schema}"."{from_table}"'
    if not from_rest.upper().startswith(qualified.upper()) and not from_rest.upper().startswith('"'):
        return None

    # Rewrite quoted identifiers in the remainder (WHERE/ON/etc), not just the SELECT list.
    # This fixes cases where the plan references unprefixed columns like "status"
    # but the actual staging table only has prefixed columns like "31f72c84_status".
    quoted_ident_pat = re.compile(r'"([^"]+)"')

    def rewrite_quoted_identifiers(text: str) -> str:
        def repl(match: re.Match[str]) -> str:
            plan_col = match.group(1)
            actual = find_actual(plan_col)
            if actual and actual != plan_col:
                return f'"{actual}"'
            return match.group(0)

        return quoted_ident_pat.sub(repl, text)

    from_rest = rewrite_quoted_identifiers(from_rest)

    parts = []
    for item in re.split(r"\s*,\s*", select_list_str):
        as_m = re.match(r'"([^"]+)"\s+AS\s+"([^"]+)"', item.strip())
        if as_m:
            plan_col, alias = as_m.group(1), as_m.group(2)
        else:
            col_m = re.match(r'"([^"]+)"', item.strip())
            if col_m:
                plan_col = alias = col_m.group(1)
            else:
                continue
        actual = find_actual(plan_col)
        if actual:
            parts.append(f'"{actual}" AS "{alias}"')
        else:
            logger.warning(
                "[PUSHDOWN] Column '%s' not in upstream relation; skipping (alias=%s). Actual columns: %s",
                plan_col, alias, actual_columns[:10],
            )
    if not parts:
        return None
    new_select = "SELECT " + ", ".join(parts) + " FROM " + from_rest
    return new_select

def _execute_source_staging_query(
    execution_conn,
    sql: str,
    config: dict[str, Any],
    nodes: list[dict[str, Any]],
    source_table_map: dict[tuple[str, str], str],
) -> Optional[int]:
    """
    If sql is CREATE TABLE ... AS SELECT ... FROM source_table and we have a source connection
    for that table, run SELECT on source and create table + insert on execution (customer) DB.
    Returns rowcount if handled, None if not a source-staging query or no source conn (caller runs sql on execution_conn).
    When plan columns don't exist (metadata mismatch), rewrites SELECT using actual source columns.
    """
    parsed = _parse_create_table_as_select(sql)
    if not parsed:
        logger.info("[PUSHDOWN] Source staging skipped: query is not parseable as CREATE TABLE AS SELECT source-read")
        return None
    from_key = (parsed["from_schema"], parsed["from_table"])
    norm_from_key = (
        str(parsed["from_schema"]).strip('"').lower(),
        str(parsed["from_table"]).strip('"').lower(),
    )
    source_node_id = source_table_map.get(from_key) or source_table_map.get(norm_from_key)
    if not source_node_id:
        # Staging reads are expected here; only source tables should map to source_node_id.
        if str(parsed.get("from_schema", "")).lower() == "staging_jobs":
            logger.debug(
                "[PUSHDOWN] Source map skip for staging read: requested=%s normalized=%s",
                from_key,
                norm_from_key,
            )
            return None
        # Emit concise diagnostics to expose real source schema/table mismatches in logs.
        sample_keys = list(source_table_map.keys())[:8]
        logger.warning(
            "[PUSHDOWN] Source map miss: requested=%s normalized=%s available_keys(sample)=%s",
            from_key,
            norm_from_key,
            sample_keys,
        )
        return None
    source_conn = _get_source_connection(config, source_node_id)
    if not source_conn:
        source_configs = _get_source_configs(config)
        sc = source_configs.get(source_node_id, {})
        has_conn_cfg = isinstance(sc, dict) and bool(
            sc.get("connection_config") or sc.get("connectionConfig") or sc.get("host") or sc.get("hostname")
        )
        logger.warning(
            "[PUSHDOWN] Source staging skipped: source connection missing for node=%s key=%s has_source_config=%s has_conn_cfg=%s available_source_keys(sample)=%s; running on execution DB",
            source_node_id[:8],
            from_key,
            bool(sc),
            has_conn_cfg,
            list(source_configs.keys())[:8],
        )
        return None
    create_schema = parsed["create_schema"]
    create_table = parsed["create_table"]
    logger.info(
        "[PUSHDOWN] Source staging: reading from source DB (%s.%s) → staging %s.%s",
        from_key[0], from_key[1], create_schema, create_table,
    )
    try:
        select_sql = parsed["select_sql"]
        from_schema = parsed["from_schema"]
        from_table = parsed["from_table"]
        qualified_from = f'"{from_schema}"."{from_table}"'

        # 1) On source: get column names and types (SELECT ... LIMIT 0)
        src_cur = source_conn.cursor()
        try:
            src_cur.execute(select_sql + " LIMIT 0" if "LIMIT" not in select_sql.upper() else select_sql)
        except Exception as e:
            if "UndefinedColumn" not in type(e).__name__ and "does not exist" not in str(e):
                raise
            # Plan references columns that don't exist (e.g. metadata mismatch, reserved-word confusion)
            src_cur.close()
            logger.info("[PUSHDOWN] Plan SELECT failed (column missing), fetching actual columns: %s", e)
            cur2 = source_conn.cursor()
            cur2.execute(f'SELECT * FROM {qualified_from} LIMIT 0')
            actual_cols = [d[0] for d in (cur2.description or [])]
            cur2.close()
            if not actual_cols:
                raise
            rewritten = _rewrite_select_for_actual_columns(
                select_sql, from_schema, from_table, actual_cols
            )
            if rewritten:
                select_sql = rewritten
                logger.info("[PUSHDOWN] Rewrote SELECT using actual columns: %s", actual_cols[:8])
            else:
                raise
            src_cur = source_conn.cursor()
            src_cur.execute(select_sql + " LIMIT 0" if "LIMIT" not in select_sql.upper() else select_sql)

        desc = src_cur.description
        if not desc:
            src_cur.close()
            return None
        col_names = [d[0] for d in desc]
        type_codes = [d[1] for d in desc]
        src_cur.close()
        # 2) Resolve type names (OID -> name)
        type_names = []
        for oid in type_codes:
            name = _PG_TYPE_OID_TO_NAME.get(oid)
            if not name:
                cur = source_conn.cursor()
                cur.execute("SELECT typname FROM pg_type WHERE oid = %s", (oid,))
                row = cur.fetchone()
                cur.close()
                name = row[0] if row else "text"
            type_names.append(name)
        # 3) On execution_conn: CREATE TABLE
        cols_ddl = ", ".join(f'"{c}" {t}' for c, t in zip(col_names, type_names))
        create_ddl = f'CREATE TABLE "{create_schema}"."{create_table}" ({cols_ddl})'
        exec_cur = execution_conn.cursor()
        exec_cur.execute(create_ddl)
        exec_cur.close()
        # 4) Stream SELECT from source, INSERT into execution (batches)
        # Use regular cursor (not named): AUTOCOMMIT mode doesn't support named cursors.
        batch_size = 5000
        src_cur = source_conn.cursor()
        src_cur.execute(select_sql)
        placeholders = ", ".join(["%s"] * len(col_names))
        quoted_cols = ", ".join(f'"{c}"' for c in col_names)
        insert_sql = f'INSERT INTO "{create_schema}"."{create_table}" ({quoted_cols}) VALUES ({placeholders})'
        def _adapt_row_for_insert(row, col_names):
            """Convert dict/list values to psycopg2 Json so JSONB columns insert correctly."""
            if isinstance(row, dict):
                vals = [row.get(c) for c in col_names]
            else:
                vals = list(row)
            return tuple(
                Json(v) if isinstance(v, (dict, list)) else v
                for v in vals
            )

        total = 0
        last_log_at = 0
        while True:
            rows = src_cur.fetchmany(batch_size)
            if not rows:
                break
            adapted = [_adapt_row_for_insert(r, col_names) for r in rows]
            exec_cur = execution_conn.cursor()
            exec_cur.executemany(insert_sql, adapted)
            total += len(rows)
            exec_cur.close()
            if total - last_log_at >= 25000:
                logger.info("[PUSHDOWN] Source staging: %d rows copied so far...", total)
                last_log_at = total
        src_cur.close()
        logger.info("[PUSHDOWN] Source staging: read %d rows from source into %s.%s", total, create_schema, create_table)
        return total
    finally:
        if source_conn and not source_conn.closed:
            source_conn.close()

def _split_sql_statements(sql: str):
    """Split SQL string into individual statements (DROP, etc.) by semicolons or newlines.
    psycopg2 cursor.execute() runs one statement at a time; multiple DROPs must be split."""
    if not sql or not sql.strip():
        return
    # Split by semicolon first, then yield each non-empty DROP statement
    for part in sql.split(";"):
        stmt = part.strip()
        if stmt and not stmt.startswith("--") and stmt.upper().startswith("DROP"):
            yield stmt + ";"  # cursor needs trailing semicolon for DROP

def _split_sql_list(s: str) -> list:
    """Split comma-separated SQL list by top-level commas (outside parens/quotes)."""
    result = []
    current = []
    depth = 0
    in_quote = False
    in_single_quote = False
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if in_quote:
            current.append(c)
            if c == '"':
                in_quote = False
            i += 1
            continue
        if in_single_quote:
            current.append(c)
            if c == "'":
                # Handle escaped '' inside single-quoted literals.
                if i + 1 < n and s[i + 1] == "'":
                    current.append(s[i + 1])
                    i += 2
                    continue
                in_single_quote = False
            i += 1
            continue
        if c == '"':
            in_quote = True
            current.append(c)
            i += 1
            continue
        if c == "'":
            in_single_quote = True
            current.append(c)
            i += 1
            continue
        if c in "([{":
            depth += 1
            current.append(c)
            i += 1
            continue
        if c in ")]}":
            depth -= 1
            current.append(c)
            i += 1
            continue
        if c == "," and depth == 0:
            result.append("".join(current).strip())
            current = []
            i += 1
            continue
        current.append(c)
        i += 1
    if current:
        result.append("".join(current).strip())
    return result

def _deduplicate_insert_columns(sql: str) -> str:
    """
    Remove duplicate column names from INSERT INTO ... (col1, col2, ...) SELECT ...
    Cached plans may have duplicates. Keeps first occurrence; drops corresponding SELECT expr.
    """
    m = re.match(
        r'(INSERT\s+INTO\s+"[^"]+"\s*\.\s*"[^"]+"\s*\(\s*)(.*?)(\s*\)\s+SELECT\s+)(.*?)(\s+FROM\s+.+)',
        sql.strip(),
        re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return sql
    prefix, col_list_str, mid, sel_list_str, suffix = m.groups()
    cols = _split_sql_list(col_list_str)
    sels = _split_sql_list(sel_list_str)
    if len(cols) != len(sels) or len(cols) == 0:
        return sql
    seen = set()
    unique_cols = []
    unique_sels = []
    for col, sel in zip(cols, sels):
        # Extract bare column name for comparison (strip quotes)
        col_name = col.strip().strip('"')
        if not col_name:
            continue
        if col_name.lower() in seen:
            continue
        seen.add(col_name.lower())
        unique_cols.append(col)
        unique_sels.append(sel)
    if len(unique_cols) == len(cols):
        return sql
    new_col_list = ", ".join(unique_cols)
    new_sel_list = ", ".join(unique_sels)
    return f"{prefix}{new_col_list}{mid}{new_sel_list}{suffix}"

def _deduplicate_create_table_columns(sql: str) -> str:
    """
    Remove duplicate column definitions from CREATE TABLE SQL.
    Cached plans may have duplicates (e.g. connection_id from both join branches).
    Keeps first occurrence of each column name.
    Handles multiple CREATE TABLE statements in one string.
    """
    pattern = re.compile(
        r'(CREATE\s+TABLE\s+"[^"]+"\s*\.\s*"[^"]+"\s*\(\s*)(.*?)(\s*\))',
        re.DOTALL | re.IGNORECASE,
    )
    # Process matches from end to start so indices stay valid
    matches = list(pattern.finditer(sql))
    for m in reversed(matches):
        col_defs_str = m.group(2)
        parts = re.findall(r'"([^"]+)"\s+([^,\)]+)', col_defs_str)
        seen = set()
        unique_parts = []
        for col_name, col_type in parts:
            col_type = col_type.strip()
            if col_name.lower() in seen:
                continue
            seen.add(col_name.lower())
            unique_parts.append(f'"{col_name}" {col_type}')
        if len(unique_parts) < len(parts):
            new_col_defs = ",\n  ".join(unique_parts)
            sql = sql[: m.start(2)] + new_col_defs + sql[m.end(2) :]
    return sql

def _rewrite_create_as_unique_output_columns(sql: str) -> Optional[str]:
    """
    Rewrite CREATE TABLE ... AS SELECT ... so SELECT output names are unique.
    Prevents PostgreSQL error: column "X" specified more than once.
    """
    m = re.match(
        r'(?is)^\s*CREATE\s+TABLE\s+"([^"]+)"\s*\.\s*"([^"]+)"\s+AS\s*(SELECT\s+.+)\s*$',
        sql.strip(),
    )
    if not m:
        return None
    create_schema, create_table, select_sql = m.group(1), m.group(2), m.group(3)

    sel_m = re.search(r'(?is)\bSELECT\s+(.*?)\s+FROM\s+', select_sql)
    if not sel_m:
        return None

    select_list = sel_m.group(1).strip()
    items = _split_sql_list(select_list)
    if not items:
        return None

    seen: dict[str, int] = {}
    changed = False
    rewritten_items: list[str] = []

    for item in items:
        out_name = None
        alias_m = re.search(r'(?is)\bAS\s+"([^"]+)"\s*$', item)
        if alias_m:
            out_name = alias_m.group(1)
        else:
            # Common implicit refs: "col" or alias."col"
            qcol_m = re.search(r'(?is)(?:\b\w+\.)?"([^"]+)"\s*$', item)
            if qcol_m:
                out_name = qcol_m.group(1)

        if out_name:
            key = out_name.lower()
            count = seen.get(key, 0)
            if count > 0:
                new_alias = f'{out_name}__dup{count + 1}'
                changed = True
                if alias_m:
                    item = re.sub(
                        r'(?is)\bAS\s+"([^"]+)"\s*$',
                        f'AS "{new_alias}"',
                        item,
                        count=1,
                    )
                else:
                    item = f'{item} AS "{new_alias}"'
            seen[key] = count + 1

        rewritten_items.append(item)

    if not changed:
        return None

    new_select_list = ", ".join(rewritten_items)
    rewritten_select = re.sub(
        r'(?is)\bSELECT\s+.*?\s+FROM\s+',
        f"SELECT {new_select_list} FROM ",
        select_sql,
        count=1,
    )
    return f'CREATE TABLE "{create_schema}"."{create_table}" AS\n{rewritten_select}'

def _force_alias_create_as_select_columns(sql: str) -> Optional[str]:
    """
    Last-resort fallback for CREATE TABLE AS SELECT:
    force every SELECT item to have a unique explicit alias.
    """
    m = re.match(
        r'(?is)^\s*CREATE\s+TABLE\s+"([^"]+)"\s*\.\s*"([^"]+)"\s+AS\s*(SELECT\s+.+)\s*$',
        sql.strip(),
    )
    if not m:
        return None
    create_schema, create_table, select_sql = m.group(1), m.group(2), m.group(3)

    sel_m = re.search(r'(?is)\bSELECT\s+(.*?)\s+FROM\s+', select_sql)
    if not sel_m:
        return None
    select_list = sel_m.group(1).strip()
    items = _split_sql_list(select_list)
    if not items:
        return None

    used: set[str] = set()
    rewritten_items: list[str] = []
    for idx, item in enumerate(items, start=1):
        alias = None
        alias_m = re.search(r'(?is)\bAS\s+"([^"]+)"\s*$', item)
        if alias_m:
            alias = alias_m.group(1)
        else:
            m_col = re.search(r'(?is)(?:\b\w+\.)?"([^"]+)"\s*$', item)
            if m_col:
                alias = m_col.group(1)
            else:
                m_cast = re.match(r'(?is)^\s*NULL\s*::\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*$', item)
                alias = m_cast.group(1) if m_cast else f"col_{idx}"
            item = f'{item} AS "{alias}"'

        base = alias
        n = 1
        while alias.lower() in used:
            n += 1
            alias = f"{base}__dup{n}"
        used.add(alias.lower())
        item = re.sub(r'(?is)\bAS\s+"([^"]+)"\s*$', f'AS "{alias}"', item, count=1)
        rewritten_items.append(item)

    new_select = ", ".join(rewritten_items)
    rewritten_select = re.sub(
        r'(?is)\bSELECT\s+.*?\s+FROM\s+',
        f"SELECT {new_select} FROM ",
        select_sql,
        count=1,
    )
    return f'CREATE TABLE "{create_schema}"."{create_table}" AS\n{rewritten_select}'

def _execute_sql(connection, sql: str) -> int:
    """
    Execute SQL statement.

    Returns:
        Number of rows affected
    """
    cursor = connection.cursor()

    try:
        cursor.execute(sql)
        rowcount = cursor.rowcount if cursor.rowcount >= 0 else 0
        return rowcount
    finally:
        cursor.close()

def _extract_target_schema(sql: str) -> Optional[str]:
    """
    Extract target schema from CREATE TABLE/INSERT INTO SQL.
    Returns schema when SQL references "schema"."table"; else None.
    """
    if not isinstance(sql, str):
        return None
    m_create = re.search(r'(?i)\bCREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+"([^"]+)"\s*\.\s*"[^"]+"', sql)
    if m_create:
        return m_create.group(1)
    m_insert = re.search(r'(?i)\bINSERT\s+INTO\s+"([^"]+)"\s*\.\s*"[^"]+"', sql)
    if m_insert:
        return m_insert.group(1)
    return None

def _ensure_target_schema(connection, sql: str) -> None:
    """
    Ensure referenced schemas exist before CREATE/INSERT.

    Notes:
      - The same SQL string may contain multiple CREATE TABLE / INSERT INTO statements.
      - We must create every referenced schema (e.g. destination schema 'repository'),
        not only the first one found.
    """
    if not isinstance(sql, str) or not sql.strip():
        return

    schema_set: set[str] = set()
    schema_set.update(
        re.findall(
            r'(?i)\bCREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+"([^"]+)"\s*\.\s*"[^"]+"',
            sql,
        )
    )
    schema_set.update(
        re.findall(
            r'(?i)\bINSERT\s+INTO\s+"([^"]+)"\s*\.\s*"[^"]+"',
            sql,
        )
    )
    # DROP TABLE does not require schema creation, but it may show up in some combined SQL.
    # Keeping it here is harmless and helps with robustness.
    schema_set.update(
        re.findall(
            r'(?i)\bDROP\s+TABLE(?:\s+IF\s+EXISTS)?\s+"([^"]+)"\s*\.\s*"[^"]+"',
            sql,
        )
    )

    schema_set = {s.strip() for s in schema_set if isinstance(s, str) and s.strip()}
    if not schema_set:
        return

    cur = connection.cursor()
    try:
        for schema in sorted(schema_set):
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    finally:
        cur.close()

def _generate_create_from_insert(connection, insert_sql: str) -> Optional[str]:
    """
    When destination_creates was empty, derive CREATE TABLE from an INSERT statement.
    Parses INSERT INTO "schema"."table" (col1, ...) SELECT ... FROM "staging_schema"."staging_table",
    queries staging table for column types, and returns CREATE TABLE IF NOT EXISTS.
    """
    # Match: INSERT INTO "schema"."table" (col1, col2, ...) SELECT sel1, sel2, ... FROM "staging_schema"."staging_table"
    match = re.match(
        r'INSERT\s+INTO\s+("[^"]+"\."[^"]+")\s*\(([^)]+)\)\s+SELECT\s+(.+?)\s+FROM\s+("[^"]+"\."[^"]+")\s*$',
        insert_sql.strip(),
        re.DOTALL | re.IGNORECASE,
    )
    if not match:
        # Fallback: INSERT INTO "schema"."table" SELECT * FROM ...
        match_simple = re.match(
            r'INSERT\s+INTO\s+("[^"]+"\."[^"]+")\s+SELECT\s+\*\s+FROM\s+("[^"]+"\."[^"]+")\s*$',
            insert_sql.strip(),
            re.IGNORECASE,
        )
        if match_simple:
            into_table, from_table = match_simple.groups()
            from_m = re.match(r'"([^"]+)"\s*\.\s*"([^"]+)"', from_table.strip())
            if from_m:
                schema_name, table_name = from_m.group(1), from_m.group(2)
                cursor = connection.cursor()
                try:
                    cursor.execute(
                        """
                        SELECT column_name, data_type FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                        """,
                        (schema_name, table_name),
                    )
                    rows = cursor.fetchall()
                finally:
                    cursor.close()
                if rows:
                    col_defs = []
                    for col_name, data_type in rows:
                        pg_type = "TEXT"
                        if data_type:
                            dt = str(data_type).upper()
                            if dt in ("INTEGER", "INT", "BIGINT", "SMALLINT"):
                                pg_type = "BIGINT"
                            elif dt in ("NUMERIC", "DECIMAL", "REAL", "FLOAT", "DOUBLE PRECISION"):
                                pg_type = "DOUBLE PRECISION"
                            elif dt in ("BOOLEAN", "BOOL"):
                                pg_type = "BOOLEAN"
                            elif "TIMESTAMP" in dt or dt == "DATE" or dt == "TIME":
                                pg_type = "TIMESTAMP"
                        col_defs.append(f'"{col_name}" {pg_type}')
                    if col_defs:
                        return f'CREATE TABLE IF NOT EXISTS {into_table} (\n  ' + ",\n  ".join(col_defs) + "\n)"
        return None

    into_table, dest_cols_str, select_list_str, from_table = match.groups()
    dest_cols = [c.strip().strip('"') for c in dest_cols_str.split(",")]
    select_cols = [c.strip().strip('"') for c in select_list_str.split(",")]
    if len(dest_cols) != len(select_cols):
        return None
    from_m = re.match(r'"([^"]+)"\s*\.\s*"([^"]+)"', from_table.strip())
    if not from_m:
        return None
    schema_name, table_name = from_m.group(1), from_m.group(2)

    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema_name, table_name),
        )
        staging_cols = {r[0]: r[1] for r in cursor.fetchall()}
    finally:
        cursor.close()
    if not staging_cols:
        return None

    col_defs = []
    for dest_col, sel_col in zip(dest_cols, select_cols):
        data_type = staging_cols.get(sel_col, "text")
        pg_type = "TEXT"
        if data_type:
            dt = str(data_type).upper()
            if dt in ("INTEGER", "INT", "BIGINT", "SMALLINT"):
                pg_type = "BIGINT"
            elif dt in ("NUMERIC", "DECIMAL", "REAL", "FLOAT", "DOUBLE PRECISION"):
                pg_type = "DOUBLE PRECISION"
            elif dt in ("BOOLEAN", "BOOL"):
                pg_type = "BOOLEAN"
            elif "TIMESTAMP" in dt or dt == "DATE" or dt == "TIME":
                pg_type = "TIMESTAMP"
        col_defs.append(f'"{dest_col}" {pg_type}')
    return f'CREATE TABLE IF NOT EXISTS {into_table} (\n  ' + ",\n  ".join(col_defs) + "\n)"

def _rewrite_join_for_actual_columns(
    connection, sql: str, _original_error: Exception
) -> Optional[str]:
    """
    When JOIN fails due to column l.31f72c84_cmp_id does not exist (staging tables
    have actual source column names like cmp_id, not technical_name), query actual
    staging columns and rewrite l."X"/r."Y" to use existing column names.
    Returns rewritten SQL or None if rewrite failed.
    """
    # Parse CREATE TABLE ... AS SELECT ... FROM "schema"."left_tbl" l JOIN "schema"."right_tbl" r ON ...
    create_m = re.match(
        r'CREATE\s+TABLE\s+"([^"]+)"\s*\.\s*"([^"]+)"\s+AS\s*\n?(.*)',
        sql.strip(),
        re.DOTALL | re.IGNORECASE,
    )
    if not create_m:
        return None
    rest = create_m.group(3).strip()
    if not rest.upper().startswith("SELECT"):
        return None
    # Find FROM "schema"."left" l ... JOIN "schema"."right" r
    from_m = re.search(
        r'\bFROM\s+"([^"]+)"\s*\.\s*"([^"]+)"\s+(\w+)\s+'
        r'(?:INNER|LEFT|RIGHT|FULL|CROSS)\s+JOIN\s+'
        r'"([^"]+)"\s*\.\s*"([^"]+)"\s+(\w+)\s+'
        r'(?:ON|$)',
        rest,
        re.IGNORECASE,
    )
    if not from_m:
        return None
    left_schema, left_table, left_alias = from_m.group(1), from_m.group(2), from_m.group(3)
    right_schema, right_table, right_alias = from_m.group(4), from_m.group(5), from_m.group(6)
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (left_schema, left_table),
        )
        left_actual = {r[0] for r in cursor.fetchall()}
        cursor.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (right_schema, right_table),
        )
        right_actual = {r[0] for r in cursor.fetchall()}
    finally:
        cursor.close()
    if not left_actual or not right_actual:
        return None

    def resolve_col(plan_col: str, actual_set: set) -> Optional[str]:
        """Return actual column name if it exists, else None (column should be dropped or NULL)."""
        if plan_col in actual_set:
            return plan_col
        # Handle legacy "<full-uuid>__<col>" technical names by reducing to "<col>".
        m_legacy = re.match(
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}__(.+)$",
            plan_col,
        )
        if m_legacy:
            legacy_base = m_legacy.group(1)
            if legacy_base in actual_set:
                return legacy_base
            suffix = "_" + legacy_base
            for ac in actual_set:
                if ac.lower().endswith(suffix.lower()):
                    return ac
        # Strip node prefix (8 hex chars + underscore): 31f72c84_cmp_id -> cmp_id
        m = re.match(r"^[a-f0-9]{8}_(.+)$", plan_col, re.IGNORECASE)
        if m:
            base = m.group(1)
            if base in actual_set:
                return base
        # If plan_col is a base name like "cmp_id", try to find the prefixed staging column
        # like "<node_prefix>_cmp_id" in actual staging.
        suffix = "_" + plan_col
        for ac in actual_set:
            if ac.lower().endswith(suffix.lower()):
                return ac
        return None

    def replacer(m: re.Match) -> str:
        alias, col = m.group(1), m.group(2)
        if alias.lower() == left_alias.lower():
            resolved = resolve_col(col, left_actual)
        elif alias.lower() == right_alias.lower():
            resolved = resolve_col(col, right_actual)
        else:
            return m.group(0)
        if resolved is not None:
            return f'{alias}."{resolved}"'
        # Column does not exist in staging (e.g. calculated column missing); use NULL to avoid failure
        # IMPORTANT: don't use "AS" here; this replacement can occur inside an ON clause.
        return 'NULL::text'

    # Replace l."col" and r."col" with resolved names (or NULL when column missing)
    pattern = rf'(\b{re.escape(left_alias)}\b|{re.escape(right_alias)}\b)\."([^"]+)"'
    rewritten = re.sub(pattern, replacer, rest)

    # Safety: ensure SELECT output column names are unique after rewriting.
    # Postgres can throw: "column \"X\" specified more than once" for CREATE TABLE AS
    # when multiple SELECT items resolve to the same output column name.
    def _split_top_level_commas(s: str) -> list[str]:
        parts: list[str] = []
        buf: list[str] = []
        depth = 0
        in_quotes = False
        i = 0
        while i < len(s):
            ch = s[i]
            if ch == '"' and (i == 0 or s[i - 1] != '\\'):
                in_quotes = not in_quotes
            if not in_quotes:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth = max(0, depth - 1)
                elif ch == ',' and depth == 0:
                    parts.append(''.join(buf).strip())
                    buf = []
                    i += 1
                    continue
            buf.append(ch)
            i += 1
        if buf:
            parts.append(''.join(buf).strip())
        return [p for p in parts if p]

    def _ensure_unique_select_output(rewritten_sql: str) -> str:
        select_m = re.search(r'(?is)\bSELECT\s+(.*?)\s+FROM\s+', rewritten_sql)
        if not select_m:
            return rewritten_sql
        select_str = select_m.group(1).strip()
        items = _split_top_level_commas(select_str)

        seen: dict[str, int] = {}
        new_items: list[str] = []
        for item in items:
            # Determine current output column name:
            # - if explicit alias: ... AS "alias"
            # - else for implicit refs like l."col" => "col"
            alias_m = re.search(r'(?is)\bAS\s+"([^"]+)"', item)
            out_name = alias_m.group(1) if alias_m else None
            if not out_name:
                m_col = re.search(
                    rf'(?is)\b(?:{re.escape(left_alias)}|{re.escape(right_alias)})\."([^"]+)"',
                    item,
                )
                out_name = m_col.group(1) if m_col else None
            if not out_name:
                # Expressions like NULL::text get implicit output name "text" in PostgreSQL.
                # Multiple such items collide unless we alias duplicates.
                m_cast = re.match(r'(?is)^\s*NULL\s*::\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*$', item)
                out_name = m_cast.group(1) if m_cast else None

            if out_name:
                count = seen.get(out_name, 0)
                if count > 0:
                    new_alias = f'{out_name}__dup{count + 1}'
                    if alias_m:
                        item = re.sub(
                            r'(?is)\bAS\s+"([^"]+)"',
                            f'AS "{new_alias}"',
                            item,
                            count=1,
                        )
                    else:
                        item = f'{item} AS "{new_alias}"'
                seen[out_name] = count + 1

            new_items.append(item)

        new_select = ', '.join(new_items)
        return re.sub(
            r'(?is)\bSELECT\s+.*?\s+FROM\s+',
            f'SELECT {new_select} FROM ',
            rewritten_sql,
            count=1,
        )

    rewritten = _ensure_unique_select_output(rewritten)

    if rewritten == rest:
        return None
    result = f'CREATE TABLE "{create_m.group(1)}"."{create_m.group(2)}" AS\n{rewritten}'
    logger.info(
        "[PUSHDOWN] Join failed (column mismatch), rewrote using actual staging columns "
        "(left=%s, right=%s)",
        list(left_actual)[:5],
        list(right_actual)[:5],
    )
    return result

def _rewrite_final_insert_for_actual_columns(
    connection, insert_sql: str, _original_error: Exception
) -> Optional[str]:
    """
    When final INSERT fails due to missing columns in staging or destination, query actual
    columns and rewrite to use only columns that exist. Handles:
    - Staging table has different columns (e.g. after join rewrite: 31f72c84_connection_id)
    - Destination table has different columns (e.g. pre-existing table with different schema)
    """
    # Parse INSERT INTO "schema"."table" (col1, col2, ...) SELECT sel1, sel2, ... FROM "schema"."staging"
    match = re.match(
        r'INSERT\s+INTO\s+("[^"]+"\."[^"]+"\s*)\s*\(([^)]+)\)\s+SELECT\s+(.+?)\s+FROM\s+("[^"]+"\."[^"]+")\s*$',
        insert_sql.strip(),
        re.DOTALL | re.IGNORECASE,
    )
    if not match:
        return None
    into_table, dest_cols_str, select_list_str, from_table = match.groups()
    dest_cols = [c.strip().strip('"') for c in dest_cols_str.split(",")]
    select_list = [c.strip().strip('"') for c in select_list_str.split(",")]
    if len(dest_cols) != len(select_list):
        return None
    # Parse schema.table for destination and staging
    into_m = re.match(r'"([^"]+)"\s*\.\s*"([^"]+)"', into_table.strip())
    from_m = re.match(r'"([^"]+)"\s*\.\s*"([^"]+)"', from_table.strip())
    if not into_m or not from_m:
        return None
    dest_schema, dest_table = into_m.group(1), into_m.group(2)
    staging_schema, staging_table = from_m.group(1), from_m.group(2)

    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (dest_schema, dest_table),
        )
        dest_actual = {r[0] for r in cursor.fetchall()}
        cursor.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (staging_schema, staging_table),
        )
        staging_actual = {r[0] for r in cursor.fetchall()}
    finally:
        cursor.close()
    if not dest_actual or not staging_actual:
        return None

    def find_dest_col(plan_col: str) -> Optional[str]:
        """Find destination column: exact match or suffix match (connection_id -> 31f72c84_connection_id)."""
        if plan_col in dest_actual:
            return plan_col
        suffix = "_" + plan_col
        for ac in dest_actual:
            if ac.endswith(suffix) or ac.lower().endswith("_" + plan_col.lower()):
                return ac
        return None

    def find_staging_col(plan_col: str) -> str:
        """Find staging column: exact match, suffix match (plan connection_id -> 31f72c84_connection_id)."""
        if plan_col in staging_actual:
            return f'"{plan_col}"'
        suffix = "_" + plan_col
        for ac in staging_actual:
            if ac.endswith(suffix) or ac.lower().endswith("_" + plan_col.lower()):
                return f'"{ac}"'
        return "NULL"

    # Build (dest_col, select_expr) only for dest_cols that exist in destination (exact or suffix match)
    # Deduplicate after mapping so rewritten INSERT statements never contain
    # duplicate destination columns (e.g. "_R_cmp_id" twice).
    new_pairs = []
    seen_dest: set[str] = set()
    for dest_col, sel_col in zip(dest_cols, select_list):
        actual_dest = find_dest_col(dest_col)
        if actual_dest is None:
            continue
        key = actual_dest.lower()
        if key in seen_dest:
            continue
        seen_dest.add(key)
        new_pairs.append((actual_dest, find_staging_col(sel_col)))
    if not new_pairs:
        return None
    new_dest = ", ".join(f'"{d}"' for d, _ in new_pairs)
    new_select = ", ".join(s for _, s in new_pairs)
    return f'INSERT INTO {into_table}({new_dest}) SELECT {new_select} FROM {from_table}'

def _log_query_metrics(
    job_id: str,
    level: int,
    query_idx: int,
    sql_hash: str,
    duration: float,
    rowcount: int,
    status: str
):
    """Log structured query metrics."""
    metrics = {
        "timestamp": datetime.utcnow().isoformat(),
        "job_id": job_id,
        "level": level,
        "query_idx": query_idx,
        "sql_hash": sql_hash,
        "duration_seconds": round(duration, 3),
        "rowcount": rowcount,
        "status": status
    }

    logger.info(f"[METRICS] {json.dumps(metrics)}")

def _ensure_node_cache_metadata_exists(connection):
    """Create CANVAS_CACHE schema and node_cache_metadata table if they do not exist."""
    cursor = connection.cursor()
    try:
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "CANVAS_CACHE"')
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS "CANVAS_CACHE".node_cache_metadata (
                id SERIAL PRIMARY KEY,
                canvas_id INTEGER NOT NULL,
                node_id VARCHAR(100) NOT NULL,
                node_name VARCHAR(255),
                node_type VARCHAR(50) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                config_hash VARCHAR(64),
                row_count INTEGER DEFAULT 0,
                column_count INTEGER DEFAULT 0,
                columns JSONB,
                source_node_ids JSONB,
                created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_valid BOOLEAN DEFAULT TRUE,
                UNIQUE(canvas_id, node_id)
            )
        """)
        connection.commit()
    finally:
        cursor.close()

def _get_customer_connection_if_available(config: dict[str, Any]):
    """
    Return a connection to the customer DB if config has connection_config (same DB where validate writes metadata).
    Return None if not available so caller can use destination connection and ensure schema/table exist.
    """
    conn_cfg = config.get("connection_config") or config.get("connectionConfig")
    if not conn_cfg:
        return None
    try:
        return psycopg2.connect(
            host=conn_cfg.get("host") or conn_cfg.get("hostname"),
            port=int(conn_cfg.get("port", 5432)),
            dbname=conn_cfg.get("database"),
            user=conn_cfg.get("user") or conn_cfg.get("username"),
            password=conn_cfg.get("password", "")
        )
    except Exception as e:
        logger.debug(f"[PUSHDOWN] No customer connection for metadata load: {e}")
        return None

def _load_node_metadata_from_cache(connection, canvas_id: int, config: dict[str, Any], source_node_ids: Optional[set] = None):
    """
    Load column metadata for all nodes from CANVAS_CACHE.node_cache_metadata.
    Ensures JOIN and staging table compilation use the same column names (technical_name).
    For source nodes already set by _enrich_config_with_metadata, merge: keep only columns that
    exist in the table (from enrich) and add technical_name from cache so we never SELECT missing columns.
    Creates schema and table if they do not exist (so query does not fail).
    """
    if "node_output_metadata" not in config:
        config["node_output_metadata"] = {}
    if source_node_ids is None:
        source_node_ids = set()
    try:
        _ensure_node_cache_metadata_exists(connection)
        cursor = connection.cursor()
        cursor.execute("""
            SELECT node_id, columns
            FROM "CANVAS_CACHE".node_cache_metadata
            WHERE canvas_id = %s AND is_valid = TRUE AND columns IS NOT NULL
        """, (canvas_id,))
        rows = cursor.fetchall()
        cursor.close()
        for row in rows:
            node_id, columns = row[0], row[1]
            if not node_id:
                continue
            if isinstance(columns, str):
                columns = json.loads(columns)
            # Allow empty column lists: we still want deterministic cache presence
            # for downstream logic, and sources/ensure step will fill when needed.
            if not isinstance(columns, list):
                continue
            # For source nodes, enrich already set columns from actual table (LIMIT 0). Merge: keep only
            # columns that exist in the table and add technical_name from cache so we don't SELECT missing columns.
            if node_id in source_node_ids:
                existing = config.get("node_output_metadata", {}).get(node_id, {}).get("columns", [])
                if existing and isinstance(existing[0], dict):
                    actual_names = [c.get("name") for c in existing if c.get("name")]
                    cache_by_name = {}
                    for c in columns:
                        if isinstance(c, dict):
                            n = c.get("name") or c.get("technical_name")
                            if n:
                                cache_by_name[n] = c
                    merged = []
                    for nm in actual_names:
                        ce = cache_by_name.get(nm, {})
                        tn = ce.get("technical_name") or ce.get("name") or nm
                        # Ensure destination CREATE uses display/business names.
                        # For sources, business/display is the real db column name (nm).
                        merged.append({
                            "name": nm,
                            "db_name": nm,
                            "technical_name": tn,
                            "business_name": nm,
                        })
                    if merged:
                        config["node_output_metadata"][node_id] = {"columns": merged}
                    continue
            config["node_output_metadata"][node_id] = {"columns": columns}
        if rows:
            logger.info(f"[PUSHDOWN] Loaded node_output_metadata from cache for canvas_id={canvas_id} ({len(rows)} nodes)")
    except Exception as e:
        try:
            connection.rollback()
        except Exception:
            pass
        logger.warning(f"[PUSHDOWN] Could not load node_cache_metadata (JOIN may use name vs technical_name): {e}")

def _backfill_source_metadata_from_destination(
    db_connection, nodes: list[dict[str, Any]], config: dict[str, Any], source_node_ids: set
):
    """
    When enrich failed (e.g. source tables not on customer DB), restrict source node metadata
    to columns that actually exist on the destination DB (where we run the pipeline).
    Merges with cache so we keep technical_name where available but only select existing columns.
    """
    if "node_output_metadata" not in config:
        return
    source_configs = _get_source_configs(config)
    for node in nodes:
        node_type = (node.get("type") or node.get("data", {}).get("type") or "").lower().strip()
        if not node_type.startswith("source"):
            continue
        node_id = node.get("id")
        if not node_id or node_id not in source_node_ids:
            continue
        node_data = node.get("data", {}).get("config", {})
        table_name = (
            node_data.get("tableName")
            or node_data.get("table_name")
            or source_configs.get(node_id, {}).get("table_name")
        )
        schema_name = (
            node_data.get("schema")
            or node_data.get("schema_name")
            or source_configs.get(node_id, {}).get("schema_name")
        )
        if not table_name:
            continue
        qualified = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
        try:
            cursor = db_connection.cursor()
            cursor.execute(f"SELECT * FROM {qualified} LIMIT 0")
            actual_columns = [desc[0] for desc in cursor.description]
            cursor.close()
        except Exception as e:
            try:
                db_connection.rollback()
            except Exception:
                pass
            logger.warning(
                f"[PUSHDOWN] Backfill: could not fetch columns for source {node_id[:8]}... ({qualified}): {e}"
            )
            continue
        cache_meta = config.get("node_output_metadata", {}).get(node_id, {}).get("columns", [])
        cache_by_name = {}
        if cache_meta and isinstance(cache_meta[0], dict):
            for c in cache_meta:
                n = c.get("name") or c.get("technical_name")
                if n:
                    cache_by_name[n] = c.get("technical_name") or n
        merged = []
        for col_name in actual_columns:
            tn = cache_by_name.get(col_name, col_name)
            merged.append({
                "name": col_name,
                "db_name": col_name,
                "technical_name": tn,
                # Destination should persist using business/display names.
                "business_name": col_name,
            })
        if merged:
            config["node_output_metadata"][node_id] = {"columns": merged}
            logger.info(f"[PUSHDOWN] Backfilled source {node_id[:8]}... with {len(merged)} columns from destination")

def _enrich_config_with_metadata(connection, nodes: list[dict[str, Any]], config: dict[str, Any]):
    """
    Fetch column metadata for all source nodes and inject into config.
    This allows the SQL compiler to handle column name collisions.
    """
    if "node_output_metadata" not in config:
        config["node_output_metadata"] = {}

    for node in nodes:
        # Extract node type
        node_type = (node.get("type") or node.get("data", {}).get("type") or "").lower().strip()
        if node_type.startswith("source"):
            node_id = node.get("id")
            if not node_id or node_id in config["node_output_metadata"]:
                continue

            node_data = node.get("data", {}).get("config", {})
            table_name = node_data.get("tableName") or node_data.get("table_name")
            schema_name = node_data.get("schema") or node_data.get("schema_name")

            if not table_name:
                continue

            qualified = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'

            # Use source connection when available (table lives in source DB); otherwise execution DB
            source_conn = _get_source_connection(config, node_id)
            conn_to_use = source_conn if source_conn else connection
            conn_label = "source DB (source_config.connection_config)" if source_conn else "execution DB (customer_conn/db_connection)"

            # Debug: log which connection we're using and where we're querying
            try:
                cur_info = conn_to_use.cursor()
                cur_info.execute("SELECT current_database(), current_schema()")
                row = cur_info.fetchone()
                cur_info.close()
                db_name, schema_name_actual = (row[0], row[1]) if row else ("?", "?")
            except Exception:
                db_name, schema_name_actual = "?", "?"
            logger.info(
                "[PUSHDOWN DEBUG] Source %s (%s): metadata fetch using %s → database=%s, current_schema=%s",
                node_id[:8], qualified, conn_label, db_name, schema_name_actual,
            )

            try:
                cursor = conn_to_use.cursor()
                # Fetch only column names via LIMIT 0
                cursor.execute(f"SELECT * FROM {qualified} LIMIT 0")
                columns = [desc[0] for desc in cursor.description]
                cursor.close()

                config["node_output_metadata"][node_id] = {
                    "columns": [{"name": c} for c in columns]
                }
                logger.info(f"[PUSHDOWN] Fetched {len(columns)} columns for source node {node_id} from {conn_label}")
            except Exception as e:
                try:
                    conn_to_use.rollback()
                except Exception:
                    pass
                logger.warning(
                    f"[PUSHDOWN] Could not fetch metadata for source {node_id} ({qualified}) from {conn_label}: {e}"
                )
            finally:
                if source_conn and not source_conn.closed:
                    source_conn.close()

def _ensure_source_metadata_for_plan(nodes: list[dict[str, Any]], config: dict[str, Any]) -> None:
    """
    Ensure every source node has node_output_metadata with columns that have db_name and technical_name.
    Derives technical_name = {source_id[:8]}_{db_name} when missing so plan build and SQL compiler
    never see incomplete metadata.
    """
    if "node_output_metadata" not in config:
        config["node_output_metadata"] = {}
    for node in nodes:
        node_type = (node.get("type") or node.get("data", {}).get("type") or "").lower().strip()
        if not node_type.startswith("source"):
            continue
        node_id = node.get("id")
        if not node_id:
            continue
        prefix = node_id[:8]
        meta = config["node_output_metadata"].get(node_id, {})
        columns = meta.get("columns") if isinstance(meta, dict) else None
        if not columns or not isinstance(columns, list):
            node_data = node.get("data", {}) if isinstance(node.get("data"), dict) else {}
            cfg = node_data.get("config", {}) if isinstance(node_data.get("config"), dict) else {}
            fallback_columns = []
            if isinstance(cfg.get("columns"), list):
                fallback_columns = cfg.get("columns") or []
            elif isinstance(node_data.get("columns"), list):
                fallback_columns = node_data.get("columns") or []
            else:
                out_meta = node_data.get("output_metadata", {})
                if isinstance(out_meta, dict) and isinstance(out_meta.get("columns"), list):
                    fallback_columns = out_meta.get("columns") or []
            columns = fallback_columns
        if not columns or not isinstance(columns, list):
            continue
        normalized = []
        for col in columns:
            if not isinstance(col, dict):
                continue
            name = col.get("name")
            db_name = col.get("db_name") or name
            tech_name = col.get("technical_name") or (f"{prefix}_{db_name}" if db_name else None)
            if not tech_name and name:
                tech_name = f"{prefix}_{name}"
            # Normalize inconsistent source technical-name formats:
            # - observed bad: "<full-node-uuid>__<col>" (full uuid + double underscore)
            # - desired: "{node_id[:8]}_{db_name}" (8-char prefix + single underscore)
            if isinstance(tech_name, str) and tech_name and node_id and node_id in tech_name and db_name:
                tech_name = f"{prefix}_{db_name}"
            normalized.append({
                "name": name or db_name,
                "db_name": db_name or name,
                "technical_name": tech_name or (f"{prefix}_{name}" if name else ""),
                # For destination column creation, prefer business/display names.
                # For 8hex_col technical keys, db_name is already derived as the real column.
                "business_name": db_name or name,
            })
        if normalized:
            config["node_output_metadata"][node_id] = {"columns": normalized}
