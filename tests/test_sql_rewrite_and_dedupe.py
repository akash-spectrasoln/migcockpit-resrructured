import os
import sys
from unittest.mock import Mock


# Ensure we can import internal migration_service packages.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MIGRATION_SERVICE_DIR = os.path.join(_ROOT, "services", "migration_service")
if _MIGRATION_SERVICE_DIR not in sys.path:
    sys.path.insert(0, _MIGRATION_SERVICE_DIR)

from orchestrator import pipeline_executor
from planner.execution_plan import _parse_select_columns_from_create_as


def test_rewrite_select_for_actual_columns_rewrites_where_conditions():
    """
    When the plan references unprefixed columns in WHERE (e.g. "status"),
    the rewritten query must use actual staging columns (e.g. "<node>_status").
    """
    select_sql = (
        'SELECT "id", "upper" FROM "staging_jobs"."job_x" '
        'WHERE "status" = \'active\' AND "cmp_id" > 0'
    )
    from_schema = "staging_jobs"
    from_table = "job_x"
    actual_columns = [
        "id",
        "upper",
        "2f9b6281_status",
        "2f9b6281_cmp_id",
    ]

    rewritten = pipeline_executor._rewrite_select_for_actual_columns(
        select_sql=select_sql,
        from_schema=from_schema,
        from_table=from_table,
        actual_columns=actual_columns,
    )
    assert rewritten is not None
    assert '"status"' not in rewritten
    assert '"cmp_id"' not in rewritten
    assert '"2f9b6281_status"' in rewritten
    assert '"2f9b6281_cmp_id"' in rewritten


def test_rewrite_final_insert_for_actual_columns_dedupes_dest_columns():
    """
    If INSERT INTO ... has the same destination column twice (duplicate mapping),
    the rewritten INSERT must only list it once.
    """
    connection = Mock()
    cur = Mock()
    connection.cursor.return_value = cur

    # First call: destination table columns
    # Second call: staging table columns
    cur.fetchall.side_effect = [
        [("_R_cmp_id",), ("created_at",)],
        [("2f9b6281_cmp_id",), ("created_at",)],
    ]

    insert_sql = (
        'INSERT INTO "repository"."output_table" ("_R_cmp_id", "_R_cmp_id", "created_at") '
        'SELECT "2f9b6281_cmp_id", "2f9b6281_cmp_id", "created_at" '
        'FROM "staging_jobs"."staging_x"'
    )

    rewritten = pipeline_executor._rewrite_final_insert_for_actual_columns(
        connection=connection,
        insert_sql=insert_sql,
        _original_error=Exception("missing column"),
    )
    assert rewritten is not None

    # Destination column should not be duplicated.
    assert rewritten.count('"_R_cmp_id"') == 1
    # Selected expression should be duplicated at most once after rewrite.
    assert rewritten.count('"2f9b6281_cmp_id"') == 1


def test_parse_select_columns_from_create_as_quote_aware_and_dedupes():
    sql = (
        'CREATE TABLE "staging_jobs"."t1" AS '
        'SELECT "a", \'hello,world\' AS "greeting", "b" AS "b", "b" AS "b2" '
        'FROM "staging_jobs"."src"'
    )
    cols = _parse_select_columns_from_create_as(sql)
    assert cols == ["a", "greeting", "b", "b2"]

    # Same output column name repeated should be deduped (case-insensitive).
    sql_dedupe = (
        'CREATE TABLE "staging_jobs"."t2" AS '
        'SELECT "b" AS "b", "b" AS "b" FROM "staging_jobs"."src"'
    )
    cols2 = _parse_select_columns_from_create_as(sql_dedupe)
    assert cols2 == ["b"]

