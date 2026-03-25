import os
from unittest.mock import Mock, patch

import django


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "datamigrationapi.settings")
django.setup()

from api.views.sources import RepositoryFilterExecutionView, _normalize_condition_column


def test_normalize_condition_column_uuid_double_underscore_suffix():
    assert (
        _normalize_condition_column("0087c1b8-de04-4287-9c0f-7dda034f46ce__dst_schema")
        == "dst_schema"
    )


def test_normalize_condition_column_8hex_underscore_prefix():
    assert _normalize_condition_column("1a2b3c4d_some_col") == "some_col"


def test_normalize_condition_column_table_col():
    assert _normalize_condition_column("some_table.some_col") == "some_col"


def test_repository_filter_view_normalizes_column_before_building_sql():
    """
    Regression test: repository filter SQL must reference the real column
    name (db_name), not the technical prefixed form produced by the pipeline.
    """
    view = RepositoryFilterExecutionView()

    request = Mock()
    request.user = Mock()
    request.data = {
        "table_name": "output_table",
        "schema": "repository",
        "page": 1,
        "page_size": 1,
        "conditions": [
            {
                "column": "0087c1b8-de04-4287-9c0f-7dda034f46ce__dst_schema",
                "operator": "=",
                "value": "ACTIVE",
                "logicalOperator": "AND",
            }
        ],
    }

    mock_customer = Mock()
    mock_customer.cust_db = "cust_db_name"

    mock_conn = Mock()
    mock_cur = Mock()
    mock_conn.cursor.return_value = mock_cur
    mock_conn.close = Mock()
    mock_cur.close = Mock()
    mock_conn.autocommit = False

    # Data query returns no rows; count query returns 0 rows.
    mock_cur.fetchall.return_value = []
    mock_cur.description = [("dst_schema",)]
    mock_cur.fetchone.return_value = (0,)

    with patch("api.views.sources.ensure_user_has_customer", return_value=mock_customer), patch(
        "api.views.sources.psycopg2.connect", return_value=mock_conn
    ):
        response = view.post(request)

    assert response.status_code == 200

    # First execute call should contain the normalized column in the SQL.
    assert mock_cur.execute.call_count >= 2
    data_sql_obj = mock_cur.execute.call_args_list[0].args[0]
    sql_text = str(data_sql_obj)

    assert '"dst_schema"' in sql_text
    assert "__dst_schema" not in sql_text

