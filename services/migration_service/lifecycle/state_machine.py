"""
Pipeline State Machine
Implements strict finite state machine for validation-gated execution.

STATES:
- DRAFT: DAG edited, no valid plan, execution forbidden
- VALIDATED: DAG verified, plan frozen, execution allowed
- RUNNING: Executor processing stored plan
- SUCCESS: Terminal state - successful completion
- FAILED: Terminal state - execution failed

INVARIANT:
Execution ONLY allowed when state == VALIDATED AND plan exists AND hash matches
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import hashlib
import json
from typing import Any, Optional


class PipelineState(Enum):
    """Pipeline lifecycle states."""
    DRAFT = "draft"
    VALIDATED = "validated"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

@dataclass
class ValidationResult:
    """Result of pipeline validation."""
    is_valid: bool
    execution_plan: Optional[dict[str, Any]]
    plan_hash: Optional[str]
    validated_at: Optional[datetime]
    errors: list[str]

@dataclass
class PipelineMetadata:
    """Pipeline state and execution plan metadata."""
    state: PipelineState
    execution_plan_json: Optional[str]
    plan_hash: Optional[str]
    validated_at: Optional[datetime]
    started_at: Optional[datetime]
    finished_at: Optional[datetime]

class ValidationGateError(Exception):
    """Raised when validation gate prevents execution."""
    pass

def compute_plan_hash(execution_plan: dict[str, Any]) -> str:
    """
    Compute deterministic hash of execution plan.

    Args:
        execution_plan: Execution plan dict

    Returns:
        SHA256 hash of normalized plan JSON
    """
    # Normalize: sort keys, compact JSON
    normalized = json.dumps(execution_plan, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

def compute_dag_hash(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> str:
    """
    Compute deterministic hash of DAG.

    Args:
        nodes: List of node dicts
        edges: List of edge dicts

    Returns:
        SHA256 hash of normalized DAG JSON
    """
    dag = {
        "nodes": sorted(nodes, key=lambda n: n["id"]),
        "edges": sorted(edges, key=lambda e: (e["source"], e["target"]))
    }
    normalized = json.dumps(dag, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

def validate_pipeline(
    job_id: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    config: dict[str, Any],
    storage: Any  # Storage backend (Django model, database, etc.)
) -> ValidationResult:
    """
    Validate pipeline and create frozen execution plan.

    STEPS:
    1. DAG correctness validation
    2. Build deterministic execution plan
    3. Compute plan hash
    4. Persist validation result

    Args:
        job_id: Job ID
        nodes: List of node dicts
        edges: List of edge dicts
        config: Configuration
        storage: Storage backend

    Returns:
        ValidationResult with plan and hash
    """
    from planner import PipelineValidationError, build_execution_plan, detect_materialization_points
    from planner import validate_pipeline as validate_dag

    errors = []

    # STEP 1: DAG correctness validation
    try:
        validate_dag(nodes, edges)
    except PipelineValidationError as e:
        errors.append(f"DAG validation failed: {e!s}")
        return ValidationResult(
            is_valid=False,
            execution_plan=None,
            plan_hash=None,
            validated_at=None,
            errors=errors
        )

    # STEP 2: Build deterministic execution plan
    try:
        linear_branches = (config or {}).get("linear_branches", True)
        mat_points, shared_source_terminals = detect_materialization_points(
            nodes, edges, job_id, linear_branches=linear_branches, config=config
        )
        execution_plan = build_execution_plan(
            nodes, edges, mat_points, config, job_id,
            shared_source_terminals=shared_source_terminals
        )

        # Convert to serializable dict (include destination_creates/final_inserts for multi-destination)
        plan_dict = {
            "job_id": execution_plan.job_id,
            "staging_schema": execution_plan.staging_schema,
            "levels": [
                {
                    "level_num": level.level_num,
                    "queries": [
                        {
                            "sql": q.sql,
                            "node_id": q.node_id,
                            "creates_table": q.creates_table
                        }
                        for q in level.queries
                    ],
                    "node_ids": level.node_ids
                }
                for level in execution_plan.levels
            ],
            "destination_create_sql": execution_plan.destination_create_sql,
            "final_insert_sql": execution_plan.final_insert_sql,
            "destination_creates": getattr(execution_plan, "destination_creates", []),
            "final_inserts": getattr(execution_plan, "final_inserts", []),
            "cleanup_sql": execution_plan.cleanup_sql,
            "total_queries": execution_plan.total_queries
        }

    except Exception as e:
        errors.append(f"Plan generation failed: {e!s}")
        return ValidationResult(
            is_valid=False,
            execution_plan=None,
            plan_hash=None,
            validated_at=None,
            errors=errors
        )

    # STEP 3: Compute plan hash
    plan_hash = compute_plan_hash(plan_dict)
    validated_at = datetime.utcnow()

    # STEP 4: Persist validation result
    try:
        storage.save_validation(
            job_id=job_id,
            state=PipelineState.VALIDATED,
            execution_plan_json=json.dumps(plan_dict),
            plan_hash=plan_hash,
            validated_at=validated_at
        )
    except Exception as e:
        errors.append(f"Failed to persist validation: {e!s}")
        return ValidationResult(
            is_valid=False,
            execution_plan=None,
            plan_hash=None,
            validated_at=None,
            errors=errors
        )

    return ValidationResult(
        is_valid=True,
        execution_plan=plan_dict,
        plan_hash=plan_hash,
        validated_at=validated_at,
        errors=[]
    )

def invalidate_validation(job_id: str, storage: Any) -> None:
    """
    Invalidate validation when DAG is mutated.

    CRITICAL: Any DAG change MUST call this function.

    Mutations that trigger invalidation:
    - Node added/removed
    - Edge added/removed
    - Node configuration changed
    - Source/destination parameters changed

    Args:
        job_id: Job ID
        storage: Storage backend
    """
    storage.invalidate_validation(
        job_id=job_id,
        state=PipelineState.DRAFT,
        execution_plan_json=None,
        plan_hash=None,
        validated_at=None
    )

def can_execute(
    job_id: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    storage: Any
) -> tuple[bool, Optional[str]]:
    """
    Check if pipeline can be executed.

    EXECUTION GUARD:
    Execution allowed ONLY IF:
    1. pipeline_state == VALIDATED
    2. execution_plan_json IS NOT NULL
    3. stored_plan_hash == recomputed_dag_hash

    Args:
        job_id: Job ID
        nodes: Current DAG nodes
        edges: Current DAG edges
        storage: Storage backend

    Returns:
        (can_execute, error_message)
    """
    metadata = storage.get_pipeline_metadata(job_id)

    # Check 1: State must be VALIDATED
    if metadata.state != PipelineState.VALIDATED:
        return False, f"Pipeline state is {metadata.state.value}, must be VALIDATED"

    # Check 2: Execution plan must exist
    if not metadata.execution_plan_json:
        return False, "No execution plan found. Run validation first."

    # Check 3: Plan hash must match current DAG
    current_dag_hash = compute_dag_hash(nodes, edges)

    # Load stored plan and recompute its hash
    stored_plan = json.loads(metadata.execution_plan_json)
    stored_plan_hash = compute_plan_hash(stored_plan)

    if metadata.plan_hash != stored_plan_hash:
        return False, "Stored plan hash mismatch. Data corruption detected."

    # Verify DAG hasn't changed since validation
    # (This is a safety check - DAG mutations should have called invalidate_validation)
    if metadata.plan_hash != current_dag_hash:
        # DAG changed but validation wasn't invalidated - this is a bug
        # Force invalidation now
        invalidate_validation(job_id, storage)
        return False, "DAG changed since validation. Please re-validate."

    return True, None

def execute_validated_plan(
    job_id: str,
    storage: Any,
    executor: Any  # Executor function/class
) -> dict[str, Any]:
    """
    Execute pipeline using stored validated plan.

    CRITICAL: Executor MUST NOT rebuild the plan.
    It MUST use stored execution_plan_json.

    Args:
        job_id: Job ID
        storage: Storage backend
        executor: Executor that runs the plan

    Returns:
        Execution result

    Raises:
        ValidationGateError: If execution guard fails
    """
    metadata = storage.get_pipeline_metadata(job_id)

    # PRE-EXECUTION GUARD
    if metadata.state != PipelineState.VALIDATED:
        raise ValidationGateError(
            f"Cannot execute: pipeline state is {metadata.state.value}, must be VALIDATED"
        )

    if not metadata.execution_plan_json:
        raise ValidationGateError(
            "Cannot execute: no execution plan found. Run validation first."
        )

    # Load stored plan (SOURCE OF TRUTH)
    execution_plan = json.loads(metadata.execution_plan_json)

    # STATE TRANSITION: VALIDATED → RUNNING
    storage.update_state(
        job_id=job_id,
        state=PipelineState.RUNNING,
        started_at=datetime.utcnow()
    )

    try:
        # EXECUTE USING STORED PLAN (NOT REBUILT)
        result = executor.execute_plan(execution_plan)

        # STATE TRANSITION: RUNNING → SUCCESS
        storage.update_state(
            job_id=job_id,
            state=PipelineState.SUCCESS,
            finished_at=datetime.utcnow()
        )

        return result

    except Exception:
        # STATE TRANSITION: RUNNING → FAILED
        storage.update_state(
            job_id=job_id,
            state=PipelineState.FAILED,
            finished_at=datetime.utcnow()
        )

        raise

def get_pipeline_state(job_id: str, storage: Any) -> PipelineMetadata:
    """
    Get current pipeline state and metadata.

    Args:
        job_id: Job ID
        storage: Storage backend

    Returns:
        PipelineMetadata
    """
    return storage.get_pipeline_metadata(job_id)
