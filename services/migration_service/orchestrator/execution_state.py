"""
Execution State Store - In-Memory Runtime State Management
Ephemeral execution progress tracking with TTL auto-expiry.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
import logging
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

class NodeStatus(str, Enum):
    """Node execution status - strict state machine."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"

class PipelineStatus(str, Enum):
    """Pipeline execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"

class NodePhase(str, Enum):
    """Node execution phases for multi-step operations."""
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    FINALIZE = "finalize"

@dataclass
class NodeExecutionState:
    """Runtime state for a single node execution."""
    node_id: str
    status: NodeStatus = NodeStatus.PENDING
    phase: Optional[NodePhase] = None
    phase_progress: float = 0.0  # 0-100 within current phase
    overall_progress: float = 0.0  # 0-100 for this node
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for WebSocket emission."""
        return {
            "node_id": self.node_id,
            "status": self.status.value,
            "phase": self.phase.value if self.phase else None,
            "phase_progress": self.phase_progress,
            "overall_progress": self.overall_progress,
            "error": self.error,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }

@dataclass
class PipelineExecutionState:
    """Runtime state for entire pipeline execution."""
    job_id: str
    status: PipelineStatus = PipelineStatus.PENDING
    overall_progress: float = 0.0  # 0-100
    current_step: Optional[str] = None
    nodes: dict[str, NodeExecutionState] = field(default_factory=dict)
    total_nodes: int = 0
    completed_nodes: int = 0
    failed_nodes: int = 0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None

    # Execution metadata
    current_level: Optional[int] = None
    total_levels: Optional[int] = None
    level_status: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for WebSocket emission."""
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "overall_progress": self.overall_progress,
            "current_step": self.current_step,
            "total_nodes": self.total_nodes,
            "completed_nodes": self.completed_nodes,
            "failed_nodes": self.failed_nodes,
            "current_level": self.current_level,
            "total_levels": self.total_levels,
            "level_status": self.level_status,
            "error": self.error,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "node_progress": [node.to_dict() for node in self.nodes.values()],
        }

class ExecutionStateStore:
    """
    In-memory execution state store with TTL auto-expiry.

    Design principles:
    - Zero DB writes for runtime ticks
    - Strict state machine transitions
    - Deterministic progress calculation
    - Auto-expiry after completion
    """

    def __init__(self, ttl_minutes: int = 15):
        """
        Initialize execution state store.

        Args:
            ttl_minutes: Time-to-live for completed executions (default 15 minutes)
        """
        self._states: dict[str, PipelineExecutionState] = {}
        self._ttl_minutes = ttl_minutes
        self._cleanup_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def start(self):
        """Start background cleanup task."""
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.info(f"ExecutionStateStore started with TTL={self._ttl_minutes}min")

    async def stop(self):
        """Stop background cleanup task."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
            logger.info("ExecutionStateStore stopped")

    async def _cleanup_loop(self):
        """Background task to auto-expire completed executions."""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                await self._cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup loop error: {e}")

    async def _cleanup_expired(self):
        """Remove expired execution states."""
        async with self._lock:
            now = time.time()
            ttl_seconds = self._ttl_minutes * 60
            expired_jobs = []

            for job_id, state in self._states.items():
                if state.completed_at and (now - state.completed_at) > ttl_seconds:
                    expired_jobs.append(job_id)

            for job_id in expired_jobs:
                del self._states[job_id]
                logger.info(f"Expired execution state for job {job_id}")

    async def initialize_execution(
        self,
        job_id: str,
        node_ids: list[str],
        total_levels: Optional[int] = None
    ) -> PipelineExecutionState:
        """
        Initialize a new pipeline execution.

        All nodes start in PENDING state.
        Pipeline status is PENDING until first node starts.

        Args:
            job_id: Unique job identifier
            node_ids: List of all node IDs in execution order
            total_levels: Total execution levels (for pushdown)

        Returns:
            Initialized pipeline state
        """
        async with self._lock:
            state = PipelineExecutionState(
                job_id=job_id,
                status=PipelineStatus.PENDING,
                total_nodes=len(node_ids),
                total_levels=total_levels,
                started_at=time.time()
            )

            # Initialize all nodes as PENDING
            for node_id in node_ids:
                state.nodes[node_id] = NodeExecutionState(
                    node_id=node_id,
                    status=NodeStatus.PENDING
                )

            self._states[job_id] = state
            logger.info(f"Initialized execution state for job {job_id}: {len(node_ids)} nodes")
            return state

    async def start_pipeline(self, job_id: str, current_step: str = "Starting execution"):
        """Mark pipeline as RUNNING."""
        async with self._lock:
            if job_id not in self._states:
                raise ValueError(f"Job {job_id} not initialized")

            state = self._states[job_id]
            state.status = PipelineStatus.RUNNING
            state.current_step = current_step
            logger.info(f"Pipeline {job_id} started")

    async def start_node(
        self,
        job_id: str,
        node_id: str,
        phase: Optional[NodePhase] = None
    ):
        """
        Transition node from PENDING → RUNNING.

        Validates state machine: only PENDING nodes can start.
        """
        async with self._lock:
            if job_id not in self._states:
                raise ValueError(f"Job {job_id} not initialized")

            state = self._states[job_id]
            if node_id not in state.nodes:
                raise ValueError(f"Node {node_id} not found in job {job_id}")

            node = state.nodes[node_id]

            # Validate state transition
            if node.status != NodeStatus.PENDING:
                logger.warning(
                    f"Invalid state transition for node {node_id}: "
                    f"{node.status} → RUNNING (only PENDING allowed)"
                )
                return

            node.status = NodeStatus.RUNNING
            node.phase = phase
            node.started_at = time.time()

            logger.info(f"Node {node_id} started (phase: {phase})")

    async def update_node_progress(
        self,
        job_id: str,
        node_id: str,
        phase_progress: float,
        overall_progress: Optional[float] = None,
        phase: Optional[NodePhase] = None
    ):
        """
        Update node execution progress.

        Args:
            job_id: Job identifier
            node_id: Node identifier
            phase_progress: Progress within current phase (0-100)
            overall_progress: Overall node progress (0-100), calculated if None
            phase: Current execution phase
        """
        async with self._lock:
            if job_id not in self._states:
                return

            state = self._states[job_id]
            if node_id not in state.nodes:
                return

            node = state.nodes[node_id]
            node.phase_progress = min(100.0, max(0.0, phase_progress))

            if phase:
                node.phase = phase

            if overall_progress is not None:
                node.overall_progress = min(100.0, max(0.0, overall_progress))

    async def complete_node(
        self,
        job_id: str,
        node_id: str,
        success: bool = True,
        error: Optional[str] = None
    ):
        """
        Mark node as completed (SUCCESS or FAILED).

        Validates state machine: only RUNNING nodes can complete.
        Updates pipeline overall progress.
        """
        async with self._lock:
            if job_id not in self._states:
                return

            state = self._states[job_id]
            if node_id not in state.nodes:
                return

            node = state.nodes[node_id]

            # Validate state transition
            if node.status != NodeStatus.RUNNING:
                logger.warning(
                    f"Invalid state transition for node {node_id}: "
                    f"{node.status} → {'SUCCESS' if success else 'FAILED'} "
                    "(only RUNNING allowed)"
                )
                return

            node.status = NodeStatus.SUCCESS if success else NodeStatus.FAILED
            node.overall_progress = 100.0 if success else node.overall_progress
            node.completed_at = time.time()
            node.error = error

            # Update pipeline counters
            if success:
                state.completed_nodes += 1
            else:
                state.failed_nodes += 1

            # Calculate overall pipeline progress
            state.overall_progress = self._calculate_pipeline_progress(state)

            logger.info(
                f"Node {node_id} completed: {node.status.value} "
                f"(pipeline: {state.overall_progress:.1f}%)"
            )

    async def complete_pipeline(
        self,
        job_id: str,
        success: bool = True,
        error: Optional[str] = None
    ):
        """Mark entire pipeline as completed."""
        async with self._lock:
            if job_id not in self._states:
                return

            state = self._states[job_id]
            state.status = PipelineStatus.SUCCESS if success else PipelineStatus.FAILED
            state.overall_progress = 100.0 if success else state.overall_progress
            state.completed_at = time.time()
            state.error = error

            logger.info(f"Pipeline {job_id} completed: {state.status.value}")

    async def fail_remaining_nodes(self, job_id: str):
        """Mark all PENDING nodes as SKIPPED when pipeline fails."""
        async with self._lock:
            if job_id not in self._states:
                return

            state = self._states[job_id]
            skipped_count = 0

            for node in state.nodes.values():
                if node.status == NodeStatus.PENDING:
                    node.status = NodeStatus.SKIPPED
                    skipped_count += 1

            if skipped_count > 0:
                logger.info(f"Marked {skipped_count} pending nodes as SKIPPED")

    async def update_pipeline_step(
        self,
        job_id: str,
        current_step: str,
        current_level: Optional[int] = None,
        total_levels: Optional[int] = None,
        level_status: Optional[str] = None
    ):
        """Update pipeline execution metadata."""
        async with self._lock:
            if job_id not in self._states:
                return

            state = self._states[job_id]
            state.current_step = current_step

            if current_level is not None:
                state.current_level = current_level
            if total_levels is not None:
                state.total_levels = total_levels
            if level_status is not None:
                state.level_status = level_status

    async def get_state(self, job_id: str) -> Optional[PipelineExecutionState]:
        """Get current execution state (returns None if expired/not found)."""
        async with self._lock:
            return self._states.get(job_id)

    def _calculate_pipeline_progress(self, state: PipelineExecutionState) -> float:
        """
        Calculate deterministic pipeline progress.

        Formula: (completed_nodes + sum(running_node_progress)) / total_nodes * 100
        """
        if state.total_nodes == 0:
            return 0.0

        completed = state.completed_nodes
        partial = 0.0

        for node in state.nodes.values():
            if node.status == NodeStatus.RUNNING:
                partial += node.overall_progress / 100.0

        progress = (completed + partial) / state.total_nodes * 100.0
        return min(100.0, max(0.0, progress))

# Global singleton instance
_execution_store: Optional[ExecutionStateStore] = None

def get_execution_store() -> ExecutionStateStore:
    """Get global execution state store instance."""
    global _execution_store
    if _execution_store is None:
        _execution_store = ExecutionStateStore(ttl_minutes=15)
    return _execution_store
