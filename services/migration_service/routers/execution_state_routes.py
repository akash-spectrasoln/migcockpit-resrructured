"""
Execution state router — real-time execution state for catch-up and monitoring.
"""

import logging
from typing import Any

from fastapi import APIRouter
from orchestrator.execution_state import get_execution_store

logger = logging.getLogger(__name__)

router = APIRouter(tags=["execution-state"])

@router.get("/execution/{job_id}/state")
async def get_execution_state(job_id: str) -> dict[str, Any]:
    """
    Get current execution state for a job.
    Returns ephemeral runtime state if available, None if expired/not found.
    """
    execution_store = get_execution_store()
    state = await execution_store.get_state(job_id)

    if state is None:
        return {
            "job_id": job_id,
            "found": False,
            "message": "Execution state not found or expired"
        }

    return {
        "job_id": job_id,
        "found": True,
        "state": state.to_dict()
    }
