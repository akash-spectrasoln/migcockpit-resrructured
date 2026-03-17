"""
Unit tests for ColumnLineage.
Run with: python -m pytest tests/unit/domain/test_column_lineage.py -v
No database or Django required.
"""
from domain.pipeline.column import ColumnLineage, ColumnMetadata


def test_source_column_has_correct_origin_type():
    lineage = ColumnLineage(
        technical_name='customer_id',
        origin_node_id='node_abc',
        origin_type='SOURCE',
    )
    assert lineage.origin_type == 'SOURCE'
    assert lineage.origin_node_id == 'node_abc'
    assert lineage.expression is None


def test_join_column_stores_origin_branch():
    lineage = ColumnLineage(
        technical_name='order_id',
        origin_node_id='node_join_1',
        origin_type='JOIN',
        origin_branch='left',
    )
    assert lineage.origin_branch == 'left'


def test_calculated_column_stores_expression():
    lineage = ColumnLineage(
        technical_name='full_name',
        origin_node_id='node_proj_1',
        origin_type='PROJECTION',
        expression="CONCAT(first_name, ' ', last_name)",
    )
    assert lineage.expression == "CONCAT(first_name, ' ', last_name)"
    assert lineage.origin_type == 'PROJECTION'


def test_compute_column_origin_type():
    lineage = ColumnLineage(
        technical_name='ml_score',
        origin_node_id='node_compute_1',
        origin_type='COMPUTE',
    )
    assert lineage.origin_type == 'COMPUTE'
    assert lineage.origin_branch is None


def test_column_metadata_defaults():
    col = ColumnMetadata(
        name='customer_id',
        technical_name='src1_customer_id',
        business_name='Customer ID',
        datatype='INTEGER',
    )
    assert col.nullable is True
    assert col.source == 'base'
    assert col.expression is None
    assert col.db_name is None
