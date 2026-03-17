"""
Filter Pushdown for Calculated Columns
Handles pushing filters on calculated/computed columns.
"""

from enum import Enum
from typing import Any, Optional


class CalculatedColumnType(Enum):
    """Types of calculated columns for pushdown analysis."""
    SIMPLE_EXPRESSION = "simple_expression"  # e.g., col1 + col2
    AGGREGATE = "aggregate"  # e.g., SUM(col1)
    WINDOW_FUNCTION = "window_function"  # e.g., ROW_NUMBER() OVER (...)
    CROSS_TABLE = "cross_table"  # Uses columns from multiple joined tables

class FilterPushdownAnalyzer:
    """Analyzes filters on calculated columns for pushdown opportunities."""

    def __init__(self, nodes: dict[str, Any], edges: list[dict[str, Any]]):
        self.nodes = nodes
        self.edges = edges
        self.column_definitions = {}  # {node_id: {col_name: definition}}
        self._build_column_definitions()

    def _build_column_definitions(self):
        """Build a map of calculated column definitions."""
        for node_id, node in self.nodes.items():
            node_type = node.get("type") or node.get("data", {}).get("type")
            config = node.get("data", {}).get("config", {})

            if node_type == "projection":
                # Track calculated columns from projection
                calculated_cols = config.get("calculated_columns", [])
                self.column_definitions[node_id] = {}

                for calc_col in calculated_cols:
                    col_name = calc_col.get("name")
                    expression = calc_col.get("expression")

                    if col_name and expression:
                        self.column_definitions[node_id][col_name] = {
                            "expression": expression,
                            "type": self._classify_expression(expression),
                            "dependencies": self._extract_column_dependencies(expression)
                        }

    def _classify_expression(self, expression: str) -> CalculatedColumnType:
        """Classify the type of calculated column."""
        expr_upper = expression.upper()

        # Check for aggregates
        if any(agg in expr_upper for agg in ["SUM(", "COUNT(", "AVG(", "MIN(", "MAX("]):
            return CalculatedColumnType.AGGREGATE

        # Check for window functions
        if "OVER(" in expr_upper or "OVER (" in expr_upper:
            return CalculatedColumnType.WINDOW_FUNCTION

        # Default to simple expression
        return CalculatedColumnType.SIMPLE_EXPRESSION

    def _extract_column_dependencies(self, expression: str) -> list[str]:
        """Extract column names referenced in the expression."""
        # Simple regex-based extraction (can be improved)
        import re
        # Match quoted column names: "column_name"
        columns = re.findall(r'"([^"]+)"', expression)
        return columns

    def analyze_filter_on_calculated_column(
        self,
        filter_node_id: str,
        filter_condition: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Analyze if a filter on a calculated column can be pushed down.

        Returns:
            {
                "can_push": bool,
                "push_to": str,  # node_id to push to
                "rewritten_condition": Dict,  # condition with expression substituted
                "reason": str  # explanation
            }
        """
        column = filter_condition.get("column")

        # Find the node that defines this calculated column
        defining_node = self._find_calculated_column_definition(filter_node_id, column)

        if not defining_node:
            return {
                "can_push": False,
                "reason": f"Column '{column}' is not a calculated column"
            }

        node_id, col_def = defining_node
        calc_type = col_def["type"]
        expression = col_def["expression"]
        dependencies = col_def["dependencies"]

        # Decision logic based on calculated column type
        if calc_type == CalculatedColumnType.AGGREGATE:
            return {
                "can_push": False,
                "reason": "Cannot push filters on aggregate columns (use HAVING instead)"
            }

        if calc_type == CalculatedColumnType.WINDOW_FUNCTION:
            return {
                "can_push": False,
                "reason": "Cannot push filters on window functions"
            }

        if calc_type == CalculatedColumnType.SIMPLE_EXPRESSION:
            # Check if all dependencies are available upstream
            upstream_columns = self._get_upstream_columns(node_id)

            if all(dep in upstream_columns for dep in dependencies):
                # Can push! Rewrite the condition to use the expression
                rewritten_condition = {
                    **filter_condition,
                    "column": None,  # Remove column reference
                    "expression": expression  # Use expression instead
                }

                return {
                    "can_push": True,
                    "push_to": self._find_best_push_target(node_id, dependencies),
                    "rewritten_condition": rewritten_condition,
                    "reason": "Simple expression can be evaluated upstream"
                }
            else:
                return {
                    "can_push": False,
                    "reason": f"Dependencies {dependencies} not available upstream"
                }

        return {
            "can_push": False,
            "reason": "Unknown calculated column type"
        }

    def _find_calculated_column_definition(
        self,
        filter_node_id: str,
        column_name: str
    ) -> Optional[tuple[str, dict]]:
        """Find which upstream node defines this calculated column."""
        # Traverse upstream to find the projection that defines this column
        visited = set()
        queue = [filter_node_id]

        while queue:
            current_id = queue.pop(0)
            if current_id in visited:
                continue
            visited.add(current_id)

            # Check if this node defines the column
            if current_id in self.column_definitions:
                if column_name in self.column_definitions[current_id]:
                    return (current_id, self.column_definitions[current_id][column_name])

            # Add parents to queue
            for edge in self.edges:
                if edge["target"] == current_id:
                    queue.append(edge["source"])

        return None

    def _get_upstream_columns(self, node_id: str) -> list[str]:
        """Get all columns available upstream of this node."""
        # Simplified - would need full column lineage tracking
        # For now, return empty list
        return []

    def _find_best_push_target(self, node_id: str, dependencies: list[str]) -> str:
        """Find the best node to push the filter to."""
        # Simplified - push to immediate upstream
        for edge in self.edges:
            if edge["target"] == node_id:
                return edge["source"]
        return node_id

def rewrite_filter_with_expression(
    filter_condition: dict[str, Any],
    expression: str
) -> str:
    """
    Rewrite a filter condition to use an expression instead of a column name.

    Args:
        filter_condition: {column: "total_price", operator: ">", value: 1000}
        expression: "quantity * unit_price"

    Returns:
        SQL WHERE clause: "(quantity * unit_price) > 1000"
    """
    operator = filter_condition.get("operator", "=")
    value = filter_condition.get("value")

    # Format value
    if isinstance(value, str):
        value_str = f"'{value}'"
    elif value is None:
        value_str = "NULL"
    else:
        value_str = str(value)

    # Wrap expression in parentheses for safety
    return f"({expression}) {operator} {value_str}"

# Example Usage:
"""
# Pipeline: Source → Projection (total = qty * price) → Filter (total > 1000)

analyzer = FilterPushdownAnalyzer(nodes, edges)

filter_condition = {"column": "total_price", "operator": ">", "value": 1000}
analysis = analyzer.analyze_filter_on_calculated_column(filter_node_id, filter_condition)

if analysis["can_push"]:
    # Push to source with expression
    push_to_node = analysis["push_to"]
    rewritten_cond = analysis["rewritten_condition"]

    # Generate SQL:
    # SELECT * FROM orders WHERE (quantity * unit_price) > 1000
else:
    # Keep filter at current location
    print(f"Cannot push: {analysis['reason']}")
"""
