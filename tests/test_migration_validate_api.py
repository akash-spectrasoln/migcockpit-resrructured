"""
API-level tests for migration service validate endpoint.
"""

import asyncio
import os
import sys


_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MIGRATION_SERVICE_DIR = os.path.join(_ROOT, "services", "migration_service")
if _MIGRATION_SERVICE_DIR not in sys.path:
    sys.path.insert(0, _MIGRATION_SERVICE_DIR)

from routers.migration_routes import validate_pipeline_endpoint


class _DummyRequest:
    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return self._payload


def test_validate_endpoint_success_for_simple_pipeline():
    payload = {
        "job_id": "validate_job_001",
        "canvas_id": 5,
        "persist": False,
        "nodes": [
            {
                "id": "src_1",
                "type": "source",
                "data": {
                    "type": "source",
                    "config": {
                        "tableName": "tool_connection",
                        "schema": "public",
                        "columns": [
                            {"name": "status", "business_name": "status", "technical_name": "2f9b6281_status", "datatype": "TEXT"},
                            {"name": "id", "business_name": "id", "technical_name": "2f9b6281_id", "datatype": "INTEGER"},
                        ],
                    },
                    "output_metadata": {
                        "columns": [
                            {"name": "status", "business_name": "status", "technical_name": "2f9b6281_status", "datatype": "TEXT"},
                            {"name": "id", "business_name": "id", "technical_name": "2f9b6281_id", "datatype": "INTEGER"},
                        ]
                    },
                },
            },
            {
                "id": "filter_1",
                "type": "filter",
                "data": {
                    "type": "filter",
                    "config": {
                        "conditions": [
                            {"column": "status", "operator": "=", "value": "active"}
                        ]
                    },
                },
            },
            {
                "id": "dest_1",
                "type": "destination",
                "data": {
                    "type": "destination",
                    "config": {"tableName": "dest_table", "schema": "public"},
                },
            },
        ],
        "edges": [
            {"source": "src_1", "target": "filter_1"},
            {"source": "filter_1", "target": "dest_1"},
        ],
        "config": {
            "linear_branches": True,
            "node_output_metadata": {
                "src_1": {
                    "columns": [
                        {"name": "status", "business_name": "status", "technical_name": "2f9b6281_status", "datatype": "TEXT"},
                        {"name": "id", "business_name": "id", "technical_name": "2f9b6281_id", "datatype": "INTEGER"},
                    ]
                }
            },
        },
    }

    result = asyncio.run(validate_pipeline_endpoint(_DummyRequest(payload)))

    assert result["success"] is True
    assert result["errors"] == []
    assert result["metadata"]["job_id"] == "validate_job_001"
    assert result["metadata"]["levels"] >= 1
    assert result["metadata"]["total_queries"] >= 1


def test_validate_endpoint_returns_error_for_invalid_pipeline():
    payload = {
        "job_id": "validate_job_002",
        "nodes": [
            {"id": "s1", "type": "source", "data": {"type": "source", "config": {}}},
            {"id": "s2", "type": "source", "data": {"type": "source", "config": {}}},
            {"id": "agg", "type": "aggregate", "data": {"type": "aggregate", "config": {}}},
            {"id": "dest", "type": "destination", "data": {"type": "destination", "config": {"tableName": "t"}}},
        ],
        "edges": [
            {"source": "s1", "target": "agg"},
            {"source": "s2", "target": "agg"},
            {"source": "agg", "target": "dest"},
        ],
        "config": {},
    }

    result = asyncio.run(validate_pipeline_endpoint(_DummyRequest(payload)))

    assert result["success"] is False
    assert any("must have exactly 1 parent" in err for err in result["errors"])
