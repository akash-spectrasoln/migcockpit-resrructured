from unittest.mock import Mock, patch
import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "datamigrationapi.settings")
django.setup()

from api.views.expressions import ValidateExpressionView


def test_validate_expression_sql_syntax_success():
    view = ValidateExpressionView()
    columns = [
        {"name": "status", "datatype": "TEXT"},
        {"name": "age", "datatype": "INTEGER"},
    ]

    with patch("api.views.expressions.psycopg2.connect") as mock_connect:
        mock_conn = Mock()
        mock_cur = Mock()
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn

        err = view._validate_expression_sql_syntax("status = 'active' AND age > 18", columns)

        assert err is None
        assert mock_cur.execute.called


def test_validate_expression_sql_syntax_reports_db_error():
    view = ValidateExpressionView()
    columns = [{"name": "status", "datatype": "TEXT"}]

    with patch("api.views.expressions.psycopg2.connect") as mock_connect:
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.execute.side_effect = Exception('syntax error at or near "AND"')
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn

        err = view._validate_expression_sql_syntax("status AND", columns)

        assert err is not None
        assert "syntax error" in err
