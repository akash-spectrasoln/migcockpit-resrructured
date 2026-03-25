import os
import sys


_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MIGRATION_SERVICE_DIR = os.path.join(_ROOT, "services", "migration_service")
if _MIGRATION_SERVICE_DIR not in sys.path:
    sys.path.insert(0, _MIGRATION_SERVICE_DIR)

from api.pipeline.filter_builder import build_sql_where_clause, parse_filter_from_canvas
from orchestrator import pipeline_executor
from planner.sql_compiler import flatten_segment


def test_filter_builder_all_condition_operators_generate_sql():
    cases = [
        ({"column": "a", "operator": "=", "value": 1}, '"a" = %s', [1]),
        ({"column": "a", "operator": "!=", "value": 1}, '"a" != %s', [1]),
        ({"column": "a", "operator": ">", "value": 1}, '"a" > %s', [1]),
        ({"column": "a", "operator": "<", "value": 1}, '"a" < %s', [1]),
        ({"column": "a", "operator": ">=", "value": 1}, '"a" >= %s', [1]),
        ({"column": "a", "operator": "<=", "value": 1}, '"a" <= %s', [1]),
        ({"column": "a", "operator": "LIKE", "value": "x"}, '"a" LIKE %s', ["%x%"]),
        ({"column": "a", "operator": "ILIKE", "value": "x"}, '"a" ILIKE %s', ["%x%"]),
        ({"column": "a", "operator": "IN", "value": [1, 2]}, '"a" IN (%s,%s)', [1, 2]),
        ({"column": "a", "operator": "NOT IN", "value": [1, 2]}, '"a" NOT IN (%s,%s)', [1, 2]),
        ({"column": "a", "operator": "BETWEEN", "value": [1, 2]}, '"a" BETWEEN %s AND %s', [1, 2]),
        ({"column": "a", "operator": "IS NULL", "value": None}, '"a" IS NULL', []),
        ({"column": "a", "operator": "IS NOT NULL", "value": None}, '"a" IS NOT NULL', []),
    ]

    for condition, expected_sql, expected_params in cases:
        spec = {"type": "condition", **condition}
        where_sql, params = build_sql_where_clause(spec, table_alias="")
        assert expected_sql in where_sql
        assert params == expected_params


def test_filter_parse_mixed_builder_conditions_builds_nested_structure():
    canvas_filter = {
        "conditions": [
            {"column": "a", "operator": "=", "value": 1},
            {"column": "b", "operator": ">", "value": 2, "logicalOperator": "OR"},
            {"column": "c", "operator": "<", "value": 3, "logicalOperator": "AND"},
        ]
    }
    spec = parse_filter_from_canvas(canvas_filter)
    assert spec
    sql, params = build_sql_where_clause(spec, table_alias="")
    assert sql
    assert len(params) == 3


def test_projection_include_exclude_modes_compile_stably():
    nodes = {
        "p1": {
            "id": "p1",
            "type": "projection",
            "data": {"config": {"columns": ["a", "b"], "selectedMode": "INCLUDE"}},
        },
        "p2": {
            "id": "p2",
            "type": "projection",
            "data": {"config": {"excludeMode": True, "excludedColumns": ["b"]}},
        },
    }

    include_sql = flatten_segment(
        segment_node_ids=["p1"],
        nodes=nodes,
        edges=[],
        config={},
        upstream_source_or_staging='"public"."t"',
        name_to_technical={"a": "a", "b": "b", "c": "c"},
    )
    assert 'SELECT "a", "b"' in include_sql

    exclude_sql = flatten_segment(
        segment_node_ids=["p2"],
        nodes=nodes,
        edges=[],
        config={},
        upstream_source_or_staging='"public"."t"',
        name_to_technical={"a": "a", "b": "b", "c": "c"},
    )
    assert '"b"' not in exclude_sql.split("FROM")[0]


def test_ctas_duplicate_text_columns_rewriter_makes_unique_aliases():
    sql = (
        'CREATE TABLE "staging_jobs"."x" AS '
        'SELECT "__L__"."cmp_id" AS "cmp_id", "__R__"."cmp_id" AS "cmp_id" FROM "staging_jobs"."y"'
    )
    rewritten = pipeline_executor._rewrite_create_as_unique_output_columns(sql)
    assert rewritten is not None
    assert 'AS "cmp_id__dup2"' in rewritten


def test_ctas_force_alias_fallback_aliases_every_select_item():
    sql = (
        'CREATE TABLE "staging_jobs"."x" AS '
        'SELECT NULL::text, NULL::text, "__L__"."cmp_id" FROM "staging_jobs"."y"'
    )
    rewritten = pipeline_executor._force_alias_create_as_select_columns(sql)
    assert rewritten is not None
    assert rewritten.count(' AS "') >= 3
