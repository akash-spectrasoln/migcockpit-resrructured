"""
SQL Pushdown ETL Planner
Deterministic DAG-based execution with zero Python row processing.
"""

from .execution_plan import (
    ExecutionPlan,
    build_execution_plan,
    compute_plan_hash,
    deserialize_plan,
    get_latest_plan,
    save_execution_plan_to_db,
)
from .materialization import (
    AnchorNode,
    MaterializationPoint,
    MaterializationReason,
    classify_compute_node,
    detect_anchor_nodes,
    detect_materialization_points,
    get_required_fields_for_branch,
    should_share_source,
)
from .sql_compiler import SQLCompilationError, compile_nested_sql
from .validation import PipelineValidationError, validate_pipeline

__all__ = [
    "validate_pipeline",
    "PipelineValidationError",
    "detect_materialization_points",
    "detect_anchor_nodes",
    "MaterializationPoint",
    "MaterializationReason",
    "AnchorNode",
    "classify_compute_node",
    "should_share_source",
    "get_required_fields_for_branch",
    "compile_nested_sql",
    "SQLCompilationError",
    "build_execution_plan",
    "ExecutionPlan",
    "save_execution_plan_to_db",
    "get_latest_plan",
    "compute_plan_hash",
    "deserialize_plan",
]
