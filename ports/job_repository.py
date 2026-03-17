"""
IJobRepository port.
Abstracts persistence of migration job state.
"""
from abc import ABC, abstractmethod
from typing import Optional

from domain.job.migration_job import JobStatus, MigrationJob


class IJobRepository(ABC):

    @abstractmethod
    def create(self, job: MigrationJob) -> MigrationJob:
        """Persist a new job. Returns the saved job with any auto-generated fields."""

    @abstractmethod
    def update_status(self, job_id: str, status: JobStatus,
                      progress: float = 0.0, current_step: str = '',
                      error_message: str = '') -> None:
        """Update job status fields atomically."""

    @abstractmethod
    def get_by_id(self, job_id: str) -> Optional[MigrationJob]:
        """Return job or None if not found."""

    @abstractmethod
    def get_logs(self, job_id: str) -> list[dict]:
        """Return log entries for a job, ordered by timestamp."""
