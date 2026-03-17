"""
WebSocket Event Emitter - Real-time Progress Broadcasting
Handles all WebSocket emissions for execution progress tracking.
"""

import logging
from typing import Any, Optional

import httpx
from orchestrator.execution_state import (
    NodeExecutionState,
    PipelineExecutionState,
)

logger = logging.getLogger(__name__)

WEBSOCKET_SERVICE_URL = "http://localhost:8004"

class WebSocketEmitter:
    """
    Handles WebSocket event emissions for execution progress.

    All emissions are:
    - Awaited (non-blocking)
    - Ordered (sequential per job)
    - Idempotent (safe to retry)
    """

    def __init__(self, ws_url: str = WEBSOCKET_SERVICE_URL):
        self.ws_url = ws_url
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client for WebSocket service."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=5.0)
        return self._client

    async def close(self):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _emit(self, job_id: str, event_type: str, data: dict[str, Any]):
        """
        Emit event to WebSocket service.

        Args:
            job_id: Job identifier
            event_type: Event type (status, node_progress, complete, error)
            data: Event payload
        """
        try:
            client = await self._get_client()
            response = await client.post(
                f"{self.ws_url}/broadcast/{job_id}",
                json={
                    "type": event_type,
                    **data
                }
            )
            response.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning(f"WebSocket emission failed for {job_id}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error emitting WebSocket event: {e}")

    async def emit_pipeline_started(
        self,
        job_id: str,
        state: PipelineExecutionState
    ):
        """
        Emit pipeline_running event.

        Sent when execution begins.
        """
        await self._emit(
            job_id,
            "status",
            {
                "status": "running",
                "progress": 0,
                "current_step": state.current_step or "Starting execution",
                "total_nodes": state.total_nodes,
                "node_progress": [node.to_dict() for node in state.nodes.values()],
            }
        )
        logger.info(f"[WS] Pipeline {job_id} started")

    async def emit_node_started(
        self,
        job_id: str,
        node: NodeExecutionState
    ):
        """
        Emit node_running event.

        Sent when node transitions PENDING → RUNNING.
        """
        await self._emit(
            job_id,
            "node_progress",
            {
                "node_id": node.node_id,
                "status": "running",
                "phase": node.phase.value if node.phase else None,
                "phase_progress": 0,
                "overall_progress": 0,
            }
        )
        logger.info(f"[WS] Node {node.node_id} started")

    async def emit_node_progress(
        self,
        job_id: str,
        node: NodeExecutionState,
        overall_progress: float
    ):
        """
        Emit node_progress event.

        Sent during node execution to update progress.
        """
        await self._emit(
            job_id,
            "node_progress",
            {
                "node_id": node.node_id,
                "status": "running",
                "phase": node.phase.value if node.phase else None,
                "phase_progress": node.phase_progress,
                "overall_progress": node.overall_progress,
            }
        )

    async def emit_node_completed(
        self,
        job_id: str,
        node: NodeExecutionState,
        overall_progress: float
    ):
        """
        Emit node_completed event.

        Sent when node transitions RUNNING → SUCCESS.
        """
        await self._emit(
            job_id,
            "node_progress",
            {
                "node_id": node.node_id,
                "status": "success",
                "overall_progress": 100,
            }
        )
        logger.info(f"[WS] Node {node.node_id} completed")

    async def emit_node_failed(
        self,
        job_id: str,
        node: NodeExecutionState
    ):
        """
        Emit node_failed event.

        Sent when node transitions RUNNING → FAILED.
        """
        await self._emit(
            job_id,
            "node_progress",
            {
                "node_id": node.node_id,
                "status": "failed",
                "error": node.error,
            }
        )
        logger.info(f"[WS] Node {node.node_id} failed: {node.error}")

    async def emit_pipeline_progress(
        self,
        job_id: str,
        state: PipelineExecutionState
    ):
        """
        Emit overall pipeline progress update.

        Sent periodically during execution.
        """
        await self._emit(
            job_id,
            "status",
            {
                "status": "running",
                "progress": state.overall_progress,
                "current_step": state.current_step,
                "current_level": state.current_level,
                "total_levels": state.total_levels,
                "level_status": state.level_status,
                "completed_nodes": state.completed_nodes,
                "total_nodes": state.total_nodes,
                "node_progress": [node.to_dict() for node in state.nodes.values()],
            }
        )

    async def emit_pipeline_completed(
        self,
        job_id: str,
        state: PipelineExecutionState
    ):
        """
        Emit pipeline_success event.

        Sent when entire pipeline completes successfully.
        """
        await self._emit(
            job_id,
            "complete",
            {
                "status": "completed",
                "progress": 100,
                "completed_nodes": state.completed_nodes,
                "total_nodes": state.total_nodes,
                "duration": state.completed_at - state.started_at if state.completed_at and state.started_at else 0,
            }
        )
        logger.info(f"[WS] Pipeline {job_id} completed successfully")

    async def emit_pipeline_failed(
        self,
        job_id: str,
        state: PipelineExecutionState
    ):
        """
        Emit pipeline_failed event.

        Sent when pipeline fails.
        Marks remaining nodes as SKIPPED.
        """
        await self._emit(
            job_id,
            "error",
            {
                "status": "failed",
                "error": state.error,
                "progress": state.overall_progress,
                "completed_nodes": state.completed_nodes,
                "failed_nodes": state.failed_nodes,
                "total_nodes": state.total_nodes,
                "node_progress": [node.to_dict() for node in state.nodes.values()],
            }
        )
        logger.info(f"[WS] Pipeline {job_id} failed: {state.error}")

# Global singleton instance
_ws_emitter: Optional[WebSocketEmitter] = None

def get_ws_emitter() -> WebSocketEmitter:
    """Get global WebSocket emitter instance."""
    global _ws_emitter
    if _ws_emitter is None:
        _ws_emitter = WebSocketEmitter()
    return _ws_emitter
