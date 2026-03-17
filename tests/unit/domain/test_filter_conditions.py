"""
Unit tests for FilterCondition and FilterOperator.
No database or Django required.
"""
from domain.pipeline.filter import FilterCondition, FilterOperator, LogicalOperator


def test_filter_condition_stores_column_and_value():
    fc = FilterCondition(column='status', operator=FilterOperator.EQUALS, value='active')
    assert fc.column == 'status'
    assert fc.value == 'active'
    assert fc.operator == FilterOperator.EQUALS


def test_is_null_condition_has_none_value():
    fc = FilterCondition(column='deleted_at', operator=FilterOperator.IS_NULL)
    assert fc.value is None
    assert fc.operator == FilterOperator.IS_NULL


def test_filter_operator_enum_has_expected_values():
    assert FilterOperator.EQUALS.value == 'eq'
    assert FilterOperator.NOT_EQUALS.value == 'neq'
    assert FilterOperator.IS_NULL.value == 'is_null'
    assert FilterOperator.CONTAINS.value == 'contains'


def test_two_identical_filter_conditions_are_equal():
    fc1 = FilterCondition(column='age', operator=FilterOperator.GREATER_THAN, value=18)
    fc2 = FilterCondition(column='age', operator=FilterOperator.GREATER_THAN, value=18)
    assert fc1 == fc2


def test_default_logical_operator_is_and():
    fc = FilterCondition(column='name', operator=FilterOperator.EQUALS, value='Alice')
    assert fc.logical_operator == LogicalOperator.AND
