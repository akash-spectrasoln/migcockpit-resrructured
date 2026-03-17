"""
Validated plan storage — interface and in-memory implementation for persisting
pipeline validation state and execution plans (DRAFT → VALIDATED → SUCCESS/FAILED).
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from .state_machine import PipelineMetadata, PipelineState


class PipelineStorage(ABC):
    """
    Abstract storage interface for pipeline lifecycle.
    Implementations persist: state, execution_plan_json, plan_hash, validated_at, started_at, finished_at.
    """

    @abstractmethod
    def save_validation(
        self,
        job_id: str,
        state: PipelineState,
        execution_plan_json: str,
        plan_hash: str,
        validated_at: datetime
    ) -> None:
        pass

    @abstractmethod
    def invalidate_validation(
        self,
        job_id: str,
        state: PipelineState,
        execution_plan_json: Optional[str],
        plan_hash: Optional[str],
        validated_at: Optional[datetime]
    ) -> None:
        pass

    @abstractmethod
    def update_state(
        self,
        job_id: str,
        state: PipelineState,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None
    ) -> None:
        pass

    @abstractmethod
    def get_pipeline_metadata(self, job_id: str) -> PipelineMetadata:
        pass

class InMemoryPipelineStorage(PipelineStorage):
    """In-memory implementation for testing. Production should use database-backed storage."""

    def __init__(self):
        self._storage = {}

    def save_validation(
        self,
        job_id: str,
        state: PipelineState,
        execution_plan_json: str,
        plan_hash: str,
        validated_at: datetime
    ) -> None:
        self._storage[job_id] = {
            "state": state,
            "execution_plan_json": execution_plan_json,
            "plan_hash": plan_hash,
            "validated_at": validated_at,
            "started_at": None,
            "finished_at": None
        }

    def invalidate_validation(
        self,
        job_id: str,
        state: PipelineState,
        execution_plan_json: Optional[str],
        plan_hash: Optional[str],
        validated_at: Optional[datetime]
    ) -> None:
        if job_id in self._storage:
            self._storage[job_id].update({
                "state": state,
                "execution_plan_json": execution_plan_json,
                "plan_hash": plan_hash,
                "validated_at": validated_at
            })
        else:
            self._storage[job_id] = {
                "state": state,
                "execution_plan_json": execution_plan_json,
                "plan_hash": plan_hash,
                "validated_at": validated_at,
                "started_at": None,
                "finished_at": None
            }

    def update_state(
        self,
        job_id: str,
        state: PipelineState,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None
    ) -> None:
        if job_id not in self._storage:
            raise KeyError(f"Job {job_id} not found")
        self._storage[job_id]["state"] = state
        if started_at:
            self._storage[job_id]["started_at"] = started_at
        if finished_at:
            self._storage[job_id]["finished_at"] = finished_at

    def get_pipeline_metadata(self, job_id: str) -> PipelineMetadata:
        if job_id not in self._storage:
            return PipelineMetadata(
                state=PipelineState.DRAFT,
                execution_plan_json=None,
                plan_hash=None,
                validated_at=None,
                started_at=None,
                finished_at=None
            )
        data = self._storage[job_id]
        return PipelineMetadata(
            state=data["state"],
            execution_plan_json=data["execution_plan_json"],
            plan_hash=data["plan_hash"],
            validated_at=data["validated_at"],
            started_at=data["started_at"],
            finished_at=data["finished_at"]
        )
