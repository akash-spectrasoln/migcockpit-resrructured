"""
Celery tasks for the migration service. Run pipeline in a worker so FastAPI stays responsive.
"""

import asyncio
import logging
import os
import sys
from typing import Optional

# Ensure service dir is on path when worker imports this module
_service_dir = os.path.dirname(os.path.abspath(__file__))
if _service_dir not in sys.path:
    sys.path.insert(0, _service_dir)

from celery_app import app

logger = logging.getLogger(__name__)

@app.task(bind=True, name="migration_service.run_migration_pipeline")
def run_migration_pipeline_task(
    self,
    job_id: str,
    canvas_id: int,
    nodes: list,
    edges: list,
    config: dict,
    execution_plan: Optional[dict] = None,
):
    """
    Run the migration pipeline in a Celery worker. Updates job state in Redis so GET /status works.
    When execution_plan is provided (e.g. loaded from DB), compilation is skipped.
    """
    from routers.migration_routes import execute_migration_pipeline

    logger.info(
        "[Celery] Starting pipeline job=%s canvas=%s nodes=%d edges=%d saved_plan=%s",
        job_id, canvas_id, len(nodes or []), len(edges or []),
        "YES (no compilation)" if execution_plan else "NO (will compile)",
    )
    try:
        asyncio.run(
            execute_migration_pipeline(
                job_id=job_id,
                canvas_id=canvas_id,
                nodes=nodes,
                edges=edges,
                config=config or {},
                execution_plan=execution_plan,
            )
        )
        logger.info("[Celery] Pipeline finished for job %s", job_id)
        return {"job_id": job_id, "status": "completed"}
    except Exception as e:
        # Error message includes failure phase (e.g. "failed at PHASE_5_BUILD_PLAN: ...") when raised from execute_pipeline_pushdown
        fail_detail = getattr(e, "detail", None) or str(e)
        current_step = fail_detail[:200] if fail_detail else "Failed"  # Store phase + message for debugging
        logger.exception(
            "[Celery FAIL] Job %s failed at: %s",
            job_id, fail_detail[:500],
        )
        from models import JobStatus
        from state.job_store import update_job
        update_job(job_id, status=JobStatus.FAILED, error=fail_detail[:2000], current_step=current_step)
        raise
