"""
Unit tests for remap_rows_to_business_names.

Covers:
1. Simple projection rename
2. Join with _l/_r technical names -> clean business names
3. Missing metadata fallback
4. Duplicate business names resolution
5. Null + datatype preservation
6. Pure function / no mutation
"""

from pathlib import Path
import sys

# Add project root so we can import from services.migration_service
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import pytest

from services.migration_service.utils.business_name_remap import remap_rows_to_business_names


class TestSimpleProjectionRename:
    """Test simple projection rename: technical -> business."""

    def test_simple_rename(self):
        rows = [{"id": 1, "name": "Alice"}]
        metadata = [
            {"technical_name": "id", "business_name": "Customer ID", "name": "Customer ID"},
            {"technical_name": "name", "business_name": "Customer Name", "name": "Customer Name"},
        ]
        result = remap_rows_to_business_names(rows, metadata)
        assert result == [{"Customer ID": 1, "Customer Name": "Alice"}]

    def test_uses_db_name_when_no_technical_match(self):
        rows = [{"user_id": 1, "full_name": "Bob"}]
        metadata = [
            {"technical_name": "user_id", "db_name": "user_id", "business_name": "User ID"},
            {"technical_name": "full_name", "db_name": "full_name", "business_name": "Full Name"},
        ]
        result = remap_rows_to_business_names(rows, metadata)
        assert result == [{"User ID": 1, "Full Name": "Bob"}]


class TestJoinTechnicalNames:
    """Test join with _l/_r technical names -> clean business names."""

    def test_join_l_r_to_business(self):
        rows = [
            {"src1_id_l": 1, "src2_name": "A"},
            {"src1_id_l": 2, "src2_name": "B"},
        ]
        metadata = [
            {"technical_name": "src1_id_l", "business_name": "Customer ID", "name": "Customer ID"},
            {"technical_name": "src2_name", "business_name": "Customer Name", "name": "Customer Name"},
        ]
        result = remap_rows_to_business_names(rows, metadata)
        assert result == [
            {"Customer ID": 1, "Customer Name": "A"},
            {"Customer ID": 2, "Customer Name": "B"},
        ]

    def test_join_L_R_extraction_format(self):
        """Extraction service returns __L__.col, __R__.col keys."""
        rows = [
            {"__L__.connection_id": 1, "__R__.status": "active"},
            {"__L__.connection_id": 2, "__R__.status": "inactive"},
        ]
        metadata = [
            {"technical_name": "connection_id_l", "business_name": "Customer ID", "name": "Customer ID"},
            {"technical_name": "status_r", "business_name": "Status", "name": "Status"},
        ]
        result = remap_rows_to_business_names(rows, metadata)
        assert result == [
            {"Customer ID": 1, "Status": "active"},
            {"Customer ID": 2, "Status": "inactive"},
        ]

    def test_join_L_R_frontend_format(self):
        """Frontend uses _L_col, _R_col keys."""
        rows = [
            {"_L_connection_id": 1, "_R_status": "active", "_L_cmp_id": 100},
        ]
        metadata = [
            {"technical_name": "connection_id_l", "business_name": "Connection ID", "name": "Connection ID"},
            {"technical_name": "status_r", "business_name": "Status", "name": "Status"},
            {"technical_name": "cmp_id_l", "business_name": "Company ID", "name": "Company ID"},
        ]
        result = remap_rows_to_business_names(rows, metadata)
        assert result == [
            {"Connection ID": 1, "Status": "active", "Company ID": 100},
        ]


class TestMissingMetadataFallback:
    """Test backward compatibility when metadata is missing."""

    def test_empty_metadata_returns_copy(self):
        rows = [{"id": 1, "name": "X"}]
        result = remap_rows_to_business_names(rows, [])
        assert result == [{"id": 1, "name": "X"}]

    def test_none_metadata_returns_copy(self):
        rows = [{"id": 1}]
        result = remap_rows_to_business_names(rows, None)
        assert result == [{"id": 1}]

    def test_empty_rows_returns_empty(self):
        result = remap_rows_to_business_names([], [{"technical_name": "id", "business_name": "ID"}])
        assert result == []

    def test_business_name_empty_fallback_to_technical(self):
        rows = [{"id": 1}]
        metadata = [{"technical_name": "id", "business_name": "", "name": ""}]
        result = remap_rows_to_business_names(rows, metadata)
        assert result == [{"id": 1}]


class TestDuplicateBusinessNames:
    """Test duplicate business names get _1, _2 suffix."""

    def test_duplicate_business_names(self):
        rows = [{"col_a": 1, "col_b": 2}]
        metadata = [
            {"technical_name": "col_a", "business_name": "Value", "name": "Value"},
            {"technical_name": "col_b", "business_name": "Value", "name": "Value"},
        ]
        result = remap_rows_to_business_names(rows, metadata)
        assert "Value" in result[0]
        assert "Value_2" in result[0]
        assert result[0]["Value"] == 1
        assert result[0]["Value_2"] == 2


class TestNullAndDatatypePreservation:
    """Test null values and datatypes are preserved."""

    def test_null_preserved(self):
        rows = [{"id": 1, "name": None}]
        metadata = [
            {"technical_name": "id", "business_name": "ID"},
            {"technical_name": "name", "business_name": "Name"},
        ]
        result = remap_rows_to_business_names(rows, metadata)
        assert result == [{"ID": 1, "Name": None}]

    def test_mixed_types_preserved(self):
        rows = [
            {"a": 1, "b": 3.14, "c": True, "d": "hi", "e": None},
        ]
        metadata = [
            {"technical_name": "a", "business_name": "A"},
            {"technical_name": "b", "business_name": "B"},
            {"technical_name": "c", "business_name": "C"},
            {"technical_name": "d", "business_name": "D"},
            {"technical_name": "e", "business_name": "E"},
        ]
        result = remap_rows_to_business_names(rows, metadata)
        assert result[0]["A"] == 1
        assert result[0]["B"] == 3.14
        assert result[0]["C"] is True
        assert result[0]["D"] == "hi"
        assert result[0]["E"] is None


class TestUnknownExtraKeys:
    """Test unknown extra keys are appended unchanged."""

    def test_unknown_keys_preserved(self):
        rows = [{"id": 1, "extra_field": "unchanged"}]
        metadata = [{"technical_name": "id", "business_name": "ID"}]
        result = remap_rows_to_business_names(rows, metadata)
        assert result[0]["ID"] == 1
        assert result[0]["extra_field"] == "unchanged"


class TestPureFunction:
    """Test no mutation of input."""

    def test_input_not_mutated(self):
        rows = [{"id": 1}]
        metadata = [{"technical_name": "id", "business_name": "ID"}]
        original = dict(rows[0])
        remap_rows_to_business_names(rows, metadata)
        assert rows[0] == original

    def test_returns_new_list(self):
        rows = [{"id": 1}]
        result = remap_rows_to_business_names(rows, [])
        assert result is not rows
        assert result[0] is not rows[0]


class TestColumnOrder:
    """Test column order from metadata is preserved."""

    def test_metadata_order_first(self):
        rows = [{"z": 3, "a": 1, "m": 2}]
        metadata = [
            {"technical_name": "a", "business_name": "A"},
            {"technical_name": "m", "business_name": "M"},
            {"technical_name": "z", "business_name": "Z"},
        ]
        result = remap_rows_to_business_names(rows, metadata)
        keys = list(result[0].keys())
        assert keys[:3] == ["A", "M", "Z"]


class TestLoaderReceivesBusinessNames:
    """Test that remapped output has business names as keys (what loader receives)."""

    def test_remapped_keys_are_business_names_for_loader(self):
        """Simulate SOURCE->JOIN->PROJECTION->DESTINATION: internal technical keys -> remap -> business names."""
        internal_rows = [
            {"src1_id_l": 1, "src2_name": "A"},
            {"src1_id_l": 2, "src2_name": "B"},
        ]
        metadata = [
            {"technical_name": "src1_id_l", "business_name": "Customer ID", "name": "Customer ID"},
            {"technical_name": "src2_name", "business_name": "Customer Name", "name": "Customer Name"},
        ]
        remapped = remap_rows_to_business_names(internal_rows, metadata)
        columns = list(remapped[0].keys()) if remapped else []
        assert "Customer ID" in columns
        assert "Customer Name" in columns
        assert "src1_id_l" not in columns
        assert "src2_name" not in columns


class TestLoaderCreatesTableFromBusinessMetadata:
    """Test loader creates table with business names only (no _L_/_R_)."""

    def test_get_business_columns_from_metadata(self):
        """get_business_columns_from_metadata returns business names for schema."""
        from services.migration_service.utils.business_name_remap import get_business_columns_from_metadata
        metadata = [
            {"technical_name": "_L_status", "business_name": "Connection Status", "name": "Connection Status"},
            {"technical_name": "_R_status", "business_name": "Log Status", "name": "Log Status"},
            {"technical_name": "_L_cmp_id", "business_name": "Company ID", "name": "Company ID"},
        ]
        cols = get_business_columns_from_metadata(metadata)
        assert "Connection Status" in cols
        assert "Log Status" in cols
        assert "Company ID" in cols
        assert "_L_status" not in cols
        assert "_R_status" not in cols
        assert "_L_cmp_id" not in cols

    def test_join_technical_to_business_no_technical_in_output(self):
        """Pipeline with _L_/_R_ keys must produce only business names for destination."""
        rows = [{"_L_status": "active", "_R_status": "ok", "_L_cmp_id": 100}]
        metadata = [
            {"technical_name": "status_l", "business_name": "Connection Status", "name": "Connection Status"},
            {"technical_name": "status_r", "business_name": "Log Status", "name": "Log Status"},
            {"technical_name": "cmp_id_l", "business_name": "Company ID", "name": "Company ID"},
        ]
        result = remap_rows_to_business_names(rows, metadata)
        keys = list(result[0].keys())
        assert all("_L_" not in k and "_R_" not in k for k in keys)
        assert "Connection Status" in keys
        assert "Log Status" in keys
        assert "Company ID" in keys
