import os
import sys
from unittest.mock import Mock, patch


_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MIGRATION_SERVICE_DIR = os.path.join(_ROOT, "services", "migration_service")
if _MIGRATION_SERVICE_DIR not in sys.path:
    sys.path.insert(0, _MIGRATION_SERVICE_DIR)

from planner.metadata_generator import _generate_source_metadata
from orchestrator import pipeline_executor


def test_repository_source_metadata_is_fetched_from_information_schema():
    node_id = "abcdef12-3456-7890-abcd-ef1234567890"
    cursor = Mock()
    cursor.fetchall.return_value = [
        ("dst_schema", "character varying"),
        ("created_at", "timestamp without time zone"),
    ]

    node_config = {
        "sourceId": -1,
        "tableName": "output_table",
        "schema": "repository",
        "isRepository": True,
    }

    columns = _generate_source_metadata(
        node_id=node_id,
        node_config=node_config,
        cursor=cursor,
        canvas_id=123,
        nodes={},
    )

    assert len(columns) == 2
    assert columns[0]["business_name"] == "dst_schema"
    assert columns[0]["db_name"] == "dst_schema"
    assert columns[0]["technical_name"] == "abcdef12_dst_schema"

    # Ensure we didn't attempt to read GENERAL.source for repository nodes
    executed_sql = " ".join(str(c.args[0]) for c in cursor.execute.call_args_list)
    assert "GENERAL.source" not in executed_sql
    assert "information_schema.columns" in executed_sql


def test_node_cache_loader_loads_empty_columns_arrays():
    connection = Mock()
    cur = Mock()
    # One row: node_id='node1', columns=[]
    cur.fetchall.return_value = [("node1", [])]
    connection.cursor.return_value = cur

    config = {}

    with patch.object(pipeline_executor, "_ensure_node_cache_metadata_exists", return_value=None):
        pipeline_executor._load_node_metadata_from_cache(
            connection=connection,
            canvas_id=99,
            config=config,
            source_node_ids=set(),
        )

    assert "node_output_metadata" in config
    assert "node1" in config["node_output_metadata"]
    assert config["node_output_metadata"]["node1"]["columns"] == []

