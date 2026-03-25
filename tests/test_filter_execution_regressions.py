"""
Regression tests for filter expression handling and execution ordering.
"""

from services.migration_service.planner.execution_plan import _build_execution_levels
from services.migration_service.planner.sql_compiler import (
    _build_filter_col_to_upstream,
    _rewrite_expression_column_refs,
)
from services.migration_service.planner.validation import (
    PipelineValidationError,
    validate_pipeline,
)


def test_filter_column_mapping_handles_prefixed_and_join_side_tokens():
    upstream_columns = [
        "2f9b6281_status",
        "1b3f3cee_status",
        "2f9b6281_cmp_id",
        "1b3f3cee_cmp_id",
    ]

    mapping = _build_filter_col_to_upstream(upstream_columns)

    # Plain business/display names resolve to technical staging names.
    assert mapping["status"] in {"2f9b6281_status", "1b3f3cee_status"}
    assert mapping["cmp_id"] in {"2f9b6281_cmp_id", "1b3f3cee_cmp_id"}

    # Join side aliases resolve deterministically by branch order.
    assert mapping["_L_status"] == "2f9b6281_status"
    assert mapping["_R_status"] == "1b3f3cee_status"
    assert mapping["_L_cmp_id"] == "2f9b6281_cmp_id"
    assert mapping["_R_cmp_id"] == "1b3f3cee_cmp_id"


def test_expression_rewrite_uses_technical_names_and_text_cast():
    rewritten = _rewrite_expression_column_refs(
        "UPPER(status) = 'ACTIVE' AND deleted = false",
        {"status": "2f9b6281_status", "deleted": "2f9b6281_deleted"},
    )

    # Column refs are rewritten to technical names and UPPER is text-safe.
    assert '"2f9b6281_status"' in rewritten
    assert '"2f9b6281_deleted"' in rewritten
    assert "UPPER((" in rewritten and ")::text)" in rewritten


def test_execution_levels_respect_dependency_order():
    nodes = [
        {"id": "src_a", "type": "source"},
        {"id": "src_b", "type": "source"},
        {"id": "join_1", "type": "join"},
        {"id": "filter_1", "type": "filter"},
        {"id": "dest_1", "type": "destination"},
    ]
    edges = [
        {"source": "src_a", "target": "join_1"},
        {"source": "src_b", "target": "join_1"},
        {"source": "join_1", "target": "filter_1"},
        {"source": "filter_1", "target": "dest_1"},
    ]

    levels = _build_execution_levels(nodes, edges)
    level_index = {nid: i for i, level in enumerate(levels) for nid in level}

    # Parents must be scheduled before children.
    for edge in edges:
        assert level_index[edge["source"]] < level_index[edge["target"]]


def test_validate_pipeline_rejects_aggregate_with_multiple_parents():
    nodes = [
        {"id": "source_1", "type": "source"},
        {"id": "source_2", "type": "source"},
        {"id": "agg_1", "type": "aggregate"},
        {"id": "dest_1", "type": "destination"},
    ]
    edges = [
        {"source": "source_1", "target": "agg_1"},
        {"source": "source_2", "target": "agg_1"},
        {"source": "agg_1", "target": "dest_1"},
    ]

    try:
        validate_pipeline(nodes, edges)
        assert False, "Expected PipelineValidationError for aggregate with multiple parents"
    except PipelineValidationError as exc:
        assert "AGGREGATE node 'agg_1' must have exactly 1 parent" in str(exc)
