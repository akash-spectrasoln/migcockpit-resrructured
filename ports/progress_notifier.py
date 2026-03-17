"""
IProgressNotifier port.
Abstracts real-time progress emission (WebSocket, polling, etc.).
"""
from abc import ABC, abstractmethod


class IProgressNotifier(ABC):

    @abstractmethod
    def emit_progress(self, job_id: str, step: str, percent: float) -> None:
        """Emit incremental progress update."""

    @abstractmethod
    def emit_node_complete(self, job_id: str, node_id: str, row_count: int) -> None:
        """Emit completion of a single pipeline node."""

    @abstractmethod
    def emit_error(self, job_id: str, error_message: str) -> None:
        """Emit a job error."""

    @abstractmethod
    def emit_complete(self, job_id: str, stats: dict) -> None:
        """Emit final job completion with summary stats."""
