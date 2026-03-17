"""
Filter condition domain objects.
Pure Python — zero framework imports.
"""
from dataclasses import dataclass
from enum import Enum
from typing import Any


class FilterOperator(Enum):
    EQUALS         = 'eq'
    NOT_EQUALS     = 'neq'
    GREATER_THAN   = 'gt'
    LESS_THAN      = 'lt'
    GREATER_EQUAL  = 'gte'
    LESS_EQUAL     = 'lte'
    CONTAINS       = 'contains'
    NOT_CONTAINS   = 'not_contains'
    STARTS_WITH    = 'starts_with'
    ENDS_WITH      = 'ends_with'
    IS_NULL        = 'is_null'
    IS_NOT_NULL    = 'is_not_null'
    IN             = 'in'
    NOT_IN         = 'not_in'

class LogicalOperator(Enum):
    AND = 'AND'
    OR  = 'OR'

@dataclass
class FilterCondition:
    column: str
    operator: FilterOperator
    value: Any = None
    logical_operator: LogicalOperator = LogicalOperator.AND

@dataclass
class FilterGroup:
    conditions: list[FilterCondition]
    logical_operator: LogicalOperator = LogicalOperator.AND
