"""
Migration Job domain objects.
Pure Python — zero framework imports.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class JobStatus(Enum):
    PENDING   = 'pending'
    RUNNING   = 'running'
    COMPLETED = 'completed'
    FAILED    = 'failed'
    CANCELLED = 'cancelled'

    def is_terminal(self) -> bool:
        return self in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)

    def can_transition_to(self, new_status: 'JobStatus') -> bool:
        allowed = {
            JobStatus.PENDING:   {JobStatus.RUNNING, JobStatus.CANCELLED},
            JobStatus.RUNNING:   {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED},
            JobStatus.COMPLETED: set(),
            JobStatus.FAILED:    set(),
            JobStatus.CANCELLED: set(),
        }
        return new_status in allowed[self]

@dataclass
class MigrationJob:
    job_id: str
    canvas_id: int
    customer_id: str
    status: JobStatus = JobStatus.PENDING
    progress: float = 0.0
    current_step: str = ''
    error_message: str = ''
    stats: dict[str, Any] = field(default_factory=dict)
