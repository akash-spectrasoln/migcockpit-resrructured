import os
import sys


_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MIGRATION_SERVICE_DIR = os.path.join(_ROOT, "services", "migration_service")
if _MIGRATION_SERVICE_DIR not in sys.path:
    sys.path.insert(0, _MIGRATION_SERVICE_DIR)

from planner.execution_plan import (  # noqa: E402
    _derive_business_name_from_technical_name,
    _normalize_anchor_columns,
)


def test_derive_business_name_from_technical_8hex_underscore():
    assert _derive_business_name_from_technical_name("1a2b3c4d_dst_schema") == "dst_schema"


def test_normalize_anchor_columns_backfills_missing_business_name():
    cols = [
        {
            "name": "1a2b3c4d_dst_schema",
            "db_name": "dst_schema",
            "technical_name": "1a2b3c4d_dst_schema",
            "datatype": "TEXT",
        }
    ]
    normalized = _normalize_anchor_columns(cols)
    assert normalized[0]["business_name"] == "dst_schema"


def test_normalize_anchor_columns_keeps_L_R_prefix_when_present():
    cols = [
        {
            "name": "_L_created_at",
            "technical_name": "_L_created_at",
            "datatype": "TIMESTAMP",
            "business_name": "_L_created_at",
        }
    ]
    normalized = _normalize_anchor_columns(cols)
    assert normalized[0]["business_name"] == "_L_created_at"


def test_normalize_anchor_columns_derives_when_business_name_is_technical_key():
    # business_name may be incorrectly set to the technical key (8hex prefix).
    cols = [
        {
            "name": "2f9b6281_status",
            "technical_name": "2f9b6281_status",
            "datatype": "TEXT",
            "business_name": "2f9b6281_status",
        }
    ]
    normalized = _normalize_anchor_columns(cols)
    assert normalized[0]["business_name"] == "status"


def test_derive_business_name_from_technical_uuid_dunder():
    # Some older/buggy metadata formats used "<full uuid>__<col>".
    assert (
        _derive_business_name_from_technical_name("e8a3fcb5-1111-2222-3333-444455556666__status")
        == "status"
    )

