"""
Celery tasks for migration jobs.

Execute flow: Django create job -> enqueue this task -> return 202. This task runs in background,
POSTs once to FastAPI /execute; FastAPI runs the pipeline in its background_tasks. No blocking
in Django or UI. User sees status via polling or WebSocket.
"""

import asyncio
import logging

from celery import shared_task
import httpx

from api.models.migration_job import MigrationJob

logger = logging.getLogger(__name__)

MIGRATION_SERVICE_URL = "http://localhost:8003"
START_TIMEOUT = 30.0
# Status poll: migration service may be busy; use longer read timeout to avoid ReadTimeout
STATUS_READ_TIMEOUT = 60.0
STATUS_CONNECT_TIMEOUT = 10.0

@shared_task
def execute_migration_task(job_id: int):
    """
    Start migration on FastAPI. Called by the execute view after creating the job; runs in
    a Celery worker so Django returns immediately. POSTs to FastAPI /execute; migration
    runs asynchronously in FastAPI. Status is visible via poll/WebSocket.
    """
    try:
        job = MigrationJob.objects.select_related("canvas").get(id=job_id)
    except MigrationJob.DoesNotExist:
        logger.error("execute_migration_task: job id=%s not found", job_id)
        return "Job not found"

    canvas = job.canvas
    nodes = canvas.get_nodes()
    edges = canvas.get_edges()
    config = dict(job.config or {})

    # When flow_node_ids is present, run only that independent flow (filter to subgraph)
    flow_node_ids = config.pop("flow_node_ids", None)
    if flow_node_ids:
        flow_node_ids = set(flow_node_ids)
        nodes = [n for n in nodes if n.get("id") in flow_node_ids]
        edges = [
            e for e in edges
            if e.get("source") in flow_node_ids and e.get("target") in flow_node_ids
        ]
        logger.info(
            "execute_migration_task: running single flow for job %s (%d nodes, %d edges)",
            job.job_id, len(nodes), len(edges),
        )

    # No CTE/SQL compilation here. Migration service /execute will either reuse the saved
    # execution plan (from Validate) or build it and run; it does its own metadata enrich and plan build.

    try:
        response = httpx.post(
            f"{MIGRATION_SERVICE_URL}/execute",
            json={
                "job_id": job.job_id,
                "canvas_id": canvas.id,
                "nodes": nodes,
                "edges": edges,
                "config": config,
            },
            timeout=START_TIMEOUT,
        )
        response.raise_for_status()
        logger.info("execute_migration_task: started job %s on migration service", job.job_id)
        return f"Migration started for job {job.job_id}"
    except httpx.TimeoutException as e:
        logger.warning("execute_migration_task: migration service timed out for job %s: %s", job.job_id, e)
        job.status = "failed"
        job.error_message = "Migration service did not accept job in time."
        job.save()
        raise
    except httpx.HTTPStatusError as e:
        logger.warning("execute_migration_task: migration service error for job %s: %s", job.job_id, e)
        job.status = "failed"
        try:
            job.error_message = e.response.json().get("detail", str(e))
        except Exception:
            job.error_message = str(e)
        job.save()
        raise
    except Exception as e:
        logger.exception("execute_migration_task: failed for job %s: %s", job.job_id, e)
        job.status = "failed"
        job.error_message = str(e)
        job.save()
        raise

@shared_task
def update_migration_status(job_id: int):
    """
    Celery task to update migration job status from service
    """
    try:
        job = MigrationJob.objects.get(id=job_id)

        async def get_status():
            # Default applies to read/write/pool; override connect. Required by httpx: use default or set all four.
            timeout = httpx.Timeout(STATUS_READ_TIMEOUT, connect=STATUS_CONNECT_TIMEOUT)
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(
                    f"{MIGRATION_SERVICE_URL}/{job.job_id}/status"
                )
                if response.status_code == 404:
                    return None  # Job unknown to FastAPI (not started yet or service restarted)
                response.raise_for_status()
                return response.json()

        try:
            service_status = asyncio.run(get_status())
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.TimeoutException) as e:
            logger.warning(
                "update_migration_status: migration service timeout for job %s (%s). Will retry on next poll.",
                job.job_id, type(e).__name__
            )
            return f"Status check timed out for job {job.job_id}; will retry on next poll."
        if service_status is None:
            if job.status not in ("completed", "cancelled", "failed"):
                job.status = "failed"
                job.error_message = (
                    "Migration service no longer has this job (not started yet or service restarted)."
                )
                job.save()
            return f"Job {job.job_id} not found on service; updated DB."

        # Update job status
        job.status = service_status.get('status', job.status)
        job.progress = service_status.get('progress', job.progress)
        job.current_step = service_status.get('current_step', job.current_step)
        if service_status.get('error'):
            job.error_message = service_status['error']
        if service_status.get('stats'):
            job.stats = service_status['stats']
        # Store pass-through fields for non-blocking status API
        extra = {}
        if service_status.get('node_progress') is not None:
            extra['node_progress'] = service_status['node_progress']
        if service_status.get('current_level') is not None:
            extra['current_level'] = service_status['current_level']
        if service_status.get('total_levels') is not None:
            extra['total_levels'] = service_status['total_levels']
        if service_status.get('level_status') is not None:
            extra['level_status'] = service_status['level_status']
        if extra:
            job.status_extra = {**(job.status_extra or {}), **extra}
        job.save()

        return f"Status updated for job {job.job_id}"

    except Exception as e:
        logger.error(f"Error updating migration status for job {job_id}: {e}")
        raise
