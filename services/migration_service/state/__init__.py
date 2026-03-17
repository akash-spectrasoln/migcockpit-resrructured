"""Shared state for migration jobs (Redis or in-memory). Used by FastAPI and Celery worker."""

from .job_store import get_job_store

__all__ = ["get_job_store"]
