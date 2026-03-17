# Moved from: api/utils/filters.py
"""
Reusable Filter System for Django
Provides a centralized filter utility that can be used across multiple views and APIs.
"""
import json
import logging
from typing import Any, Optional

from django.db.models import Q

logger = logging.getLogger(__name__)

class FilterExpression:
    """
    Represents a filter expression that can be applied to a queryset.
    Supports columns, operators, logical operators, and functions.
    """

    def __init__(self, expression: dict[str, Any]):
        self.expression = expression
        self.type = expression.get('type', 'condition')  # 'condition', 'logical', 'function'
        self.validated = False
        self.error = None

    def validate(self, available_columns: list[str]) -> bool:
        """Validate the filter expression against available columns."""
        try:
            if self.type == 'condition':
                column = self.expression.get('column')
                operator = self.expression.get('operator')

                if not column:
                    self.error = "Column is required"
                    return False

                if column not in available_columns:
                    self.error = f"Column '{column}' not found"
                    return False

                if not operator:
                    self.error = "Operator is required"
                    return False

                valid_operators = ['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'ILIKE', 'IN', 'NOT IN', 'BETWEEN', 'IS NULL', 'IS NOT NULL']
                if operator not in valid_operators:
                    self.error = f"Invalid operator: {operator}"
                    return False

                # Check value requirements
                value_required_ops = ['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'ILIKE', 'IN', 'NOT IN', 'BETWEEN']
                if operator in value_required_ops:
                    value = self.expression.get('value')
                    if value is None or value == '':
                        self.error = f"Value is required for operator '{operator}'"
                        return False

                    # Validate BETWEEN operator
                    if operator == 'BETWEEN':
                        if not isinstance(value, (list, dict)) or len(value) != 2:
                            self.error = "BETWEEN operator requires two values [min, max]"
                            return False

            elif self.type == 'logical':
                operator = self.expression.get('operator')
                if operator not in ['AND', 'OR', 'NOT']:
                    self.error = f"Invalid logical operator: {operator}"
                    return False

                expressions = self.expression.get('expressions', [])
                if not expressions:
                    self.error = "Logical operator requires at least one expression"
                    return False

                # Recursively validate nested expressions
                for expr in expressions:
                    filter_expr = FilterExpression(expr)
                    if not filter_expr.validate(available_columns):
                        self.error = filter_expr.error
                        return False

            elif self.type == 'function':
                func_name = self.expression.get('function')
                if not func_name:
                    self.error = "Function name is required"
                    return False

                # Validate function arguments
                self.expression.get('arguments', [])
                # Function validation would depend on the specific function

            self.validated = True
            return True

        except Exception as e:
            self.error = str(e)
            return False

def apply_filters(queryset, filter_spec: dict[str, Any], available_columns: Optional[list[str]] = None) -> tuple[Any, Optional[str]]:
    """
    Apply filter specification to a Django queryset.

    Args:
        queryset: Django queryset to filter
        filter_spec: Filter specification dictionary with structure:
            {
                "type": "logical",
                "operator": "AND",
                "expressions": [
                    {
                        "type": "condition",
                        "column": "name",
                        "operator": "=",
                        "value": "John"
                    },
                    {
                        "type": "condition",
                        "column": "age",
                        "operator": ">",
                        "value": 25
                    }
                ]
            }
        available_columns: Optional list of available column names for validation

    Returns:
        Tuple of (filtered_queryset, error_message)
    """
    try:
        if not filter_spec:
            return queryset, None

        # Validate filter specification
        filter_expr = FilterExpression(filter_spec)
        if available_columns:
            if not filter_expr.validate(available_columns):
                return queryset, filter_expr.error

        # Build Q object from filter specification
        q_object = _build_q_object(filter_spec)

        if q_object:
            queryset = queryset.filter(q_object)

        return queryset, None

    except Exception as e:
        logger.error(f"Error applying filters: {e}")
        return queryset, str(e)

def _build_q_object(filter_spec: dict[str, Any]) -> Optional[Q]:
    """Build Django Q object from filter specification."""
    if not filter_spec:
        return None

    expr_type = filter_spec.get('type', 'condition')

    if expr_type == 'condition':
        return _build_condition_q(filter_spec)

    elif expr_type == 'logical':
        operator = filter_spec.get('operator', 'AND')
        expressions = filter_spec.get('expressions', [])

        if not expressions:
            return None

        q_objects = []
        for expr in expressions:
            q_obj = _build_q_object(expr)
            if q_obj:
                q_objects.append(q_obj)

        if not q_objects:
            return None

        if operator == 'AND':
            # Combine all Q objects with AND
            combined = q_objects[0]
            for q_obj in q_objects[1:]:
                combined = combined & q_obj
            return combined

        elif operator == 'OR':
            # Combine all Q objects with OR
            combined = q_objects[0]
            for q_obj in q_objects[1:]:
                combined = combined | q_obj
            return combined

        elif operator == 'NOT':
            # Negate the first expression
            if q_objects:
                return ~q_objects[0]
            return None

    elif expr_type == 'function':
        # Handle function expressions
        return _build_function_q(filter_spec)

    return None

def _build_condition_q(condition: dict[str, Any]) -> Optional[Q]:
    """Build Q object for a single condition."""
    column = condition.get('column')
    operator = condition.get('operator')
    value = condition.get('value')

    if not column or not operator:
        return None

    # Handle NULL checks
    if operator == 'IS NULL':
        return Q(**{f"{column}__isnull": True})

    if operator == 'IS NOT NULL':
        return Q(**{f"{column}__isnull": False})

    # Handle value-based operators
    if value is None:
        return None

    # Map operators to Django lookup types
    lookup_map = {
        '=': 'exact',
        '!=': 'exact',  # Will be negated
        '>': 'gt',
        '<': 'lt',
        '>=': 'gte',
        '<=': 'lte',
        'LIKE': 'icontains',  # Case-insensitive contains
        'ILIKE': 'icontains',
    }

    if operator in lookup_map:
        lookup = lookup_map[operator]
        if operator == '!=':
            return ~Q(**{f"{column}__{lookup}": value})
        return Q(**{f"{column}__{lookup}": value})

    # Handle IN and NOT IN
    if operator == 'IN':
        if isinstance(value, str):
            # Comma-separated string
            value_list = [v.strip() for v in value.split(',')]
        elif isinstance(value, list):
            value_list = value
        else:
            value_list = [value]
        return Q(**{f"{column}__in": value_list})

    if operator == 'NOT IN':
        if isinstance(value, str):
            value_list = [v.strip() for v in value.split(',')]
        elif isinstance(value, list):
            value_list = value
        else:
            value_list = [value]
        return ~Q(**{f"{column}__in": value_list})

    # Handle BETWEEN
    if operator == 'BETWEEN':
        if isinstance(value, (list, dict)):
            if isinstance(value, dict):
                min_val = value.get('min') or value.get('from')
                max_val = value.get('max') or value.get('to')
            else:
                min_val = value[0] if len(value) > 0 else None
                max_val = value[1] if len(value) > 1 else None

            if min_val is not None and max_val is not None:
                return Q(**{f"{column}__gte": min_val}) & Q(**{f"{column}__lte": max_val})

    return None

def _build_function_q(function_expr: dict[str, Any]) -> Optional[Q]:
    """Build Q object for function expressions."""
    func_name = function_expr.get('function', '').upper()
    arguments = function_expr.get('arguments', [])
    operator = function_expr.get('operator', '=')
    value = function_expr.get('value')

    # This is a simplified implementation
    # Full function support would require more complex handling

    # Example: UPPER(column) = 'VALUE'
    if func_name == 'UPPER' and len(arguments) == 1:
        column = arguments[0]
        if operator == '=' and value:
            # Use Django's Upper function
            return Q(**{f"{column}__iexact": value.upper()})

    # Example: LOWER(column) = 'value'
    if func_name == 'LOWER' and len(arguments) == 1:
        column = arguments[0]
        if operator == '=' and value:
            return Q(**{f"{column}__iexact": value.lower()})

    # Add more function support as needed

    return None

def parse_filter_from_canvas(canvas_filter: dict[str, Any]) -> dict[str, Any]:
    """
    Parse filter configuration from canvas format to internal filter specification.

    Canvas format:
    {
        "conditions": [
            {
                "id": "condition-1",
                "column": "name",
                "operator": "=",
                "value": "John",
                "logicalOperator": "AND"
            }
        ]
    }

    Returns internal format:
    {
        "type": "logical",
        "operator": "AND",
        "expressions": [...]
    }
    """
    conditions = canvas_filter.get('conditions', [])

    if not conditions:
        return {}

    # Group conditions by logical operator
    expressions = []

    for i, condition in enumerate(conditions):
        # Get logical operator (default to AND for first condition, use from condition for others)
        logical_op = condition.get('logicalOperator', 'AND') if i > 0 else 'AND'

        # Build condition expression
        expr = {
            "type": "condition",
            "column": condition.get('column', '').strip() if condition.get('column') else '',
            "operator": condition.get('operator', '='),
            "value": condition.get('value'),
        }

        # Handle BETWEEN operator value format
        if condition.get('operator') == 'BETWEEN':
            value = condition.get('value')
            if isinstance(value, str):
                # Try to parse as JSON or comma-separated
                try:
                    value = json.loads(value)
                except Exception:
                    parts = [v.strip() for v in value.split(',')]
                    if len(parts) == 2:
                        value = parts
            expr['value'] = value

        # Store logical operator with expression for proper grouping
        expr['_logical_op'] = logical_op
        expressions.append(expr)

    # Build the filter specification
    # If all conditions use the same logical operator, use a simple logical structure
    # Otherwise, group by operator
    if len(expressions) == 1:
        # Remove the _logical_op metadata before returning
        expr = expressions[0].copy()
        expr.pop('_logical_op', None)
        return expr

    # Check if all conditions use the same logical operator
    logical_ops = [expr.get('_logical_op', 'AND') for expr in expressions]
    if len(set(logical_ops)) == 1:
        # All same operator - simple structure
        clean_expressions = [expr.copy() for expr in expressions]
        for expr in clean_expressions:
            expr.pop('_logical_op', None)
        return {
            "type": "logical",
            "operator": logical_ops[0],
            "expressions": clean_expressions
        }
    else:
        # Mixed operators - group by operator (simplified: AND first, then OR)
        and_exprs = [expr.copy() for expr in expressions if expr.get('_logical_op') == 'AND']
        or_exprs = [expr.copy() for expr in expressions if expr.get('_logical_op') == 'OR']

        for expr in and_exprs + or_exprs:
            expr.pop('_logical_op', None)

        # If we have both AND and OR, we need nested structure
        # For simplicity, combine all AND conditions first, then OR
        if and_exprs and or_exprs:
            # Create nested structure: (AND conditions) OR (OR conditions)
            and_group = {
                "type": "logical",
                "operator": "AND",
                "expressions": and_exprs
            } if len(and_exprs) > 1 else (and_exprs[0] if and_exprs else None)

            or_group = {
                "type": "logical",
                "operator": "OR",
                "expressions": or_exprs
            } if len(or_exprs) > 1 else (or_exprs[0] if or_exprs else None)

            if and_group and or_group:
                return {
                    "type": "logical",
                    "operator": "OR",
                    "expressions": [and_group, or_group]
                }
            elif and_group:
                return and_group
            elif or_group:
                return or_group

        # Default: use AND for all
        clean_expressions = [expr.copy() for expr in expressions]
        for expr in clean_expressions:
            expr.pop('_logical_op', None)
        return {
            "type": "logical",
            "operator": "AND",
            "expressions": clean_expressions
        }

def build_sql_where_clause(filter_spec: dict[str, Any], table_alias: str = '') -> tuple[str, list[Any]]:
    """
    Build SQL WHERE clause from filter specification.
    Useful for raw SQL queries or when not using Django ORM.

    Returns:
        Tuple of (WHERE clause string, parameter list)
    """
    if not filter_spec:
        return "", []

    where_parts = []
    params = []

    _build_sql_condition(filter_spec, where_parts, params, table_alias)

    where_clause = " AND ".join(where_parts) if where_parts else ""
    return where_clause, params

def _build_sql_condition(filter_spec: dict[str, Any], where_parts: list[str], params: list[Any], table_alias: str = ''):
    """Recursively build SQL conditions."""
    expr_type = filter_spec.get('type', 'condition')

    if expr_type == 'condition':
        column = filter_spec.get('column')
        operator = filter_spec.get('operator')
        value = filter_spec.get('value')

        if not column or not operator:
            return

        col_ref = f'"{table_alias}"."{column}"' if table_alias else f'"{column}"'

        if operator == 'IS NULL':
            where_parts.append(f"{col_ref} IS NULL")

        elif operator == 'IS NOT NULL':
            where_parts.append(f"{col_ref} IS NOT NULL")

        elif operator == '=':
            where_parts.append(f"{col_ref} = %s")
            params.append(value)

        elif operator == '!=':
            where_parts.append(f"{col_ref} != %s")
            params.append(value)

        elif operator in ['>', '<', '>=', '<=']:
            where_parts.append(f"{col_ref} {operator} %s")
            params.append(value)

        elif operator in ['LIKE', 'ILIKE']:
            where_parts.append(f"{col_ref} {operator} %s")
            params.append(f"%{value}%")

        elif operator == 'IN':
            if isinstance(value, (list, tuple)):
                placeholders = ','.join(['%s'] * len(value))
                where_parts.append(f"{col_ref} IN ({placeholders})")
                params.extend(value)
            else:
                where_parts.append(f"{col_ref} = %s")
                params.append(value)

        elif operator == 'NOT IN':
            if isinstance(value, (list, tuple)):
                placeholders = ','.join(['%s'] * len(value))
                where_parts.append(f"{col_ref} NOT IN ({placeholders})")
                params.extend(value)

        elif operator == 'BETWEEN':
            if isinstance(value, (list, tuple)) and len(value) == 2:
                where_parts.append(f"{col_ref} BETWEEN %s AND %s")
                params.extend(value)
            elif isinstance(value, dict):
                min_val = value.get('min') or value.get('from')
                max_val = value.get('max') or value.get('to')
                if min_val is not None and max_val is not None:
                    where_parts.append(f"{col_ref} BETWEEN %s AND %s")
                    params.extend([min_val, max_val])

    elif expr_type == 'logical':
        operator = filter_spec.get('operator', 'AND')
        expressions = filter_spec.get('expressions', [])

        if len(expressions) > 1:
            sub_parts = []
            sub_params = []
            for expr in expressions:
                _build_sql_condition(expr, sub_parts, sub_params, table_alias)

            if sub_parts:
                if operator == 'OR':
                    combined = f"({' OR '.join(sub_parts)})"
                else:  # AND
                    combined = f"({' AND '.join(sub_parts)})"
                where_parts.append(combined)
                params.extend(sub_params)
