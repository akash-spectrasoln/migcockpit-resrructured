"""
Filter Pushdown
Merged from: filter_pushdown.py, filter_optimizer.py
"""

# ============================================================
# From: filter_pushdown.py
# ============================================================
"""
Filter Pushdown Prototype - Simple Case
Handles pushing filters down to source nodes when column names match exactly.
"""

from collections import defaultdict
from typing import Any, Optional


def analyze_simple_filter_pushdown(
    nodes: dict[str, Any],
    edges: list[dict[str, Any]]
) -> dict[str, list[dict]]:
    """
    Analyze which filters can be pushed down to source nodes.

    Simple heuristic:
    - If a filter is directly downstream of a source (no JOIN in between)
    - And the column name exists in the source
    - Then push the filter to the source

    Returns:
        {source_node_id: [filter_conditions]}
    """
    # Build adjacency
    forward_adj = defaultdict(list)
    reverse_adj = defaultdict(list)
    for edge in edges:
        forward_adj[edge["source"]].append(edge["target"])
        reverse_adj[edge["target"]].append(edge["source"])

    pushed_filters = defaultdict(list)

    # Find all filter nodes
    for node_id, node in nodes.items():
        node_type = node.get("type") or node.get("data", {}).get("type")

        if node_type != "filter":
            continue

        config = node.get("data", {}).get("config", {})
        conditions = config.get("conditions", [])

        if not conditions:
            continue

        # Trace back to find source nodes
        source_nodes = _find_upstream_sources(node_id, nodes, reverse_adj, forward_adj)

        # For each condition, try to push to a source
        for cond in conditions:
            column = cond.get("column")
            if not column:
                continue

            # Try to find a source that has this column
            for source_id in source_nodes:
                source_config = nodes[source_id].get("data", {}).get("config", {})
                source_columns = source_config.get("columns", [])

                # Check if column exists in source
                if any(col.get("name") == column for col in source_columns):
                    pushed_filters[source_id].append(cond)
                    break

    return dict(pushed_filters)

def _find_upstream_sources(
    node_id: str,
    nodes: dict[str, Any],
    reverse_adj: dict[str, list[str]],
    forward_adj: dict[str, list[str]],
    visited: Optional[set[str]] = None
) -> list[str]:
    """
    Find all source nodes upstream of the given node.
    Stops at JOIN nodes (doesn't traverse through them).
    """
    if visited is None:
        visited = set()

    if node_id in visited:
        return []

    visited.add(node_id)

    node_type = nodes[node_id].get("type") or nodes[node_id].get("data", {}).get("type")

    # If this is a source, return it
    if node_type == "source":
        return [node_id]

    # If this is a JOIN, stop (don't push through JOINs in simple version)
    if node_type == "join":
        return []

    # Otherwise, recurse to parents
    sources = []
    for parent_id in reverse_adj.get(node_id, []):
        sources.extend(_find_upstream_sources(parent_id, nodes, reverse_adj, forward_adj, visited))

    return sources

def inject_filter_into_source_sql(source_sql: str, conditions: list[dict]) -> str:
    """
    Inject WHERE clause into source SELECT statement.

    Args:
        source_sql: Original source SQL (e.g., 'SELECT * FROM "public"."table"')
        conditions: Filter conditions to inject

    Returns:
        Modified SQL with WHERE clause
    """
    if not conditions:
        return source_sql

    # Build WHERE clause
    where_parts = []
    for cond in conditions:
        column = cond.get("column")
        operator = cond.get("operator", "=")
        value = cond.get("value")

        if not column:
            continue

        # Format value
        if isinstance(value, str):
            value_str = f"'{value}'"
        elif value is None:
            value_str = "NULL"
        else:
            value_str = str(value)

        where_parts.append(f'"{column}" {operator} {value_str}')

    if not where_parts:
        return source_sql

    " AND ".join(where_parts)

    # Wrap source SQL and add WHERE
    return """SELECT * FROM (
    {source_sql}
) src
WHERE {where_clause}"""

# Example usage in sql_compiler.py:
"""
def _compile_source_node_with_pushdown(node, nodes, edges, config, pushed_filters=None):
    # ... existing source compilation logic ...
    base_sql = _compile_source_node(node, nodes, edges, config)

    # Inject pushed-down filters
    if pushed_filters and node_id in pushed_filters:
        base_sql = inject_filter_into_source_sql(base_sql, pushed_filters[node_id])

    return base_sql
"""

# ============================================================
# From: filter_optimizer.py
# ============================================================
"""
Filter Pushdown Optimizer
Analyzes and optimizes filter placement in the execution plan.

This module handles:
1. Simple column filters (push to source)
2. Calculated column filters (substitute expressions)
3. Cross-table filters (push to JOIN output)
4. Aggregate filters (convert to HAVING)
5. Column lineage tracking through transformations
"""

from enum import Enum
import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

# =============================================================================
# ENUMS AND DATA STRUCTURES
# =============================================================================

class CalculatedColumnType(Enum):
    """Types of calculated columns for pushdown analysis."""
    SIMPLE_EXPRESSION = "simple_expression"  # e.g., col1 + col2
    AGGREGATE = "aggregate"  # e.g., SUM(col1)
    WINDOW_FUNCTION = "window_function"  # e.g., ROW_NUMBER() OVER (...)
    CROSS_TABLE = "cross_table"  # Uses columns from multiple joined tables

class PushdownDecision:
    """Result of filter pushdown analysis."""
    def __init__(
        self,
        can_push: bool,
        push_to_node: Optional[str] = None,
        rewritten_condition: Optional[dict] = None,
        reason: str = ""
    ):
        self.can_push = can_push
        self.push_to_node = push_to_node
        self.rewritten_condition = rewritten_condition
        self.reason = reason

# =============================================================================
# COLUMN LINEAGE TRACKER
# =============================================================================

class ColumnLineage:
    """
    Tracks column provenance through the pipeline.
    Knows which columns come from which sources and how they're transformed.
    """

    def __init__(self):
        self.lineage = {}  # {node_id: {column_name: source_info}}

    def track_source(self, node_id: str, columns: list[str], source_table: str):
        """Track columns from a source node."""
        self.lineage[node_id] = {
            col: {
                "source_node": node_id,
                "source_table": source_table,
                "original_name": col,
                "transformation": "source"
            }
            for col in columns
        }
        logger.debug(f"[LINEAGE] Tracked {len(columns)} columns from source {node_id}")

    def track_projection(self, node_id: str, parent_node_id: str, column_mapping: dict[str, str]):
        """
        Track columns through a projection.

        Args:
            column_mapping: {output_col: input_col or expression}
        """
        self.lineage[node_id] = {}
        parent_lineage = self.lineage.get(parent_node_id, {})

        for output_col, input_ref in column_mapping.items():
            if input_ref in parent_lineage:
                # Pass-through column
                self.lineage[node_id][output_col] = {
                    **parent_lineage[input_ref],
                    "transformation": "projection"
                }
            else:
                # Calculated column (expression)
                self.lineage[node_id][output_col] = {
                    "source_node": node_id,
                    "original_name": output_col,
                    "expression": input_ref,
                    "transformation": "calculated"
                }

    def track_join(
        self,
        join_node_id: str,
        left_node_id: str,
        right_node_id: str,
        ambiguous_columns: set[str]
    ):
        """
        Track columns through a JOIN with renaming for ambiguous columns.

        Args:
            ambiguous_columns: Columns that exist in both left and right tables
        """
        self.lineage[join_node_id] = {}

        # Track left columns (may be renamed with _L_ prefix)
        for col, info in self.lineage.get(left_node_id, {}).items():
            if col in ambiguous_columns:
                new_name = f"_L_{col}"
            else:
                new_name = col

            self.lineage[join_node_id][new_name] = {
                **info,
                "renamed_from": col,
                "join_side": "left",
                "transformation": "join"
            }

        # Track right columns (may be renamed with _R_ prefix)
        for col, info in self.lineage.get(right_node_id, {}).items():
            if col in ambiguous_columns:
                new_name = f"_R_{col}"
            else:
                new_name = col

            self.lineage[join_node_id][new_name] = {
                **info,
                "renamed_from": col,
                "join_side": "right",
                "transformation": "join"
            }

        logger.debug(f"[LINEAGE] Tracked JOIN {join_node_id}: {len(ambiguous_columns)} ambiguous columns")

    def get_source_info(self, node_id: str, column_name: str) -> Optional[dict]:
        """Get the source information for a column at a given node."""
        return self.lineage.get(node_id, {}).get(column_name)

    def get_original_column(self, node_id: str, column_name: str) -> Optional[tuple[str, str]]:
        """
        Get the original source node and column name.

        Returns:
            (source_node_id, original_column_name) or None
        """
        info = self.get_source_info(node_id, column_name)
        if info:
            return (info.get("source_node"), info.get("original_name"))
        return None

# =============================================================================
# CALCULATED COLUMN ANALYZER
# =============================================================================

class CalculatedColumnAnalyzer:
    """Analyzes calculated columns to determine pushdown feasibility."""

    @staticmethod
    def classify_expression(expression: str) -> CalculatedColumnType:
        """Classify the type of calculated column."""
        expr_upper = expression.upper()

        # Check for aggregates
        aggregates = ["SUM(", "COUNT(", "AVG(", "MIN(", "MAX(", "STDDEV(", "VARIANCE("]
        if any(agg in expr_upper for agg in aggregates):
            return CalculatedColumnType.AGGREGATE

        # Check for window functions
        if "OVER(" in expr_upper or "OVER (" in expr_upper:
            return CalculatedColumnType.WINDOW_FUNCTION

        # Default to simple expression
        return CalculatedColumnType.SIMPLE_EXPRESSION

    @staticmethod
    def extract_column_dependencies(expression: str) -> list[str]:
        """
        Extract column names referenced in the expression.
        Matches quoted column names: "column_name"
        """
        columns = re.findall(r'"([^"]+)"', expression)
        return columns

    @staticmethod
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

# =============================================================================
# FILTER PUSHDOWN OPTIMIZER
# =============================================================================

class FilterPushdownOptimizer:
    """
    Main optimizer for filter pushdown analysis and execution.
    """

    def __init__(self, nodes: dict[str, Any], edges: list[dict[str, Any]], config: Optional[dict[str, Any]] = None):
        logger.debug("[PUSHDOWN] Initializing optimizer...")
        self.nodes = nodes
        self.edges = edges
        self.config = config or {}  # Store config for metadata access
        self.lineage = ColumnLineage()

        # Debug: Log edge format
        if edges:
            sample_edge = edges[0]
            logger.debug(f"[PUSHDOWN] Edge format: {type(sample_edge)}, sample: {sample_edge}")

        try:
            logger.debug("[PUSHDOWN] Building reverse adjacency...")
            self.reverse_adj = self._build_reverse_adjacency()
            logger.debug(f"[PUSHDOWN] Reverse adjacency built: {len(self.reverse_adj)} nodes")

            logger.debug("[PUSHDOWN] Building forward adjacency...")
            self.forward_adj = self._build_forward_adjacency()
            logger.debug(f"[PUSHDOWN] Forward adjacency built: {len(self.forward_adj)} nodes")

            self.calculated_columns = {}  # {node_id: {col_name: expression}}

            # Build lineage and calculated column index
            logger.debug("[PUSHDOWN] Building column lineage...")
            self._build_lineage()
            logger.debug(f"[PUSHDOWN] Lineage built for {len(self.lineage.lineage)} nodes")

            logger.debug("[PUSHDOWN] Indexing calculated columns...")
            self._index_calculated_columns()
            logger.debug(f"[PUSHDOWN] Indexed {sum(len(cols) for cols in self.calculated_columns.values())} calculated columns")

        except Exception as e:
            logger.error(f"[PUSHDOWN] Error during initialization: {e}")
            raise

    def _build_reverse_adjacency(self) -> dict[str, list[str]]:
        """Build reverse adjacency list (target -> sources)."""
        reverse_adj = defaultdict(list)
        for edge in self.edges:
            # Handle both dict and tuple formats
            if isinstance(edge, dict):
                source = edge.get("source")
                target = edge.get("target")
            elif isinstance(edge, (list, tuple)) and len(edge) >= 2:
                source, target = edge[0], edge[1]
            else:
                logger.warning(f"[PUSHDOWN] Skipping invalid edge format: {edge}")
                continue

            if source and target:
                reverse_adj[target].append(source)
        return dict(reverse_adj)

    def _build_forward_adjacency(self) -> dict[str, list[str]]:
        """Build forward adjacency list (source -> targets)."""
        forward_adj = defaultdict(list)
        for edge in self.edges:
            # Handle both dict and tuple formats
            if isinstance(edge, dict):
                source = edge.get("source")
                target = edge.get("target")
            elif isinstance(edge, (list, tuple)) and len(edge) >= 2:
                source, target = edge[0], edge[1]
            else:
                logger.warning(f"[PUSHDOWN] Skipping invalid edge format: {edge}")
                continue

            if source and target:
                forward_adj[source].append(target)
        return dict(forward_adj)

    def _build_lineage(self):
        """Build column lineage by reading from node_cache_metadata table."""
        # Try to get metadata from config first (if provided during execution)
        node_output_metadata = self.config.get("node_output_metadata", {})

        if node_output_metadata:
            logger.debug(f"[LINEAGE] Using metadata from config: {len(node_output_metadata)} nodes")
            self._build_lineage_from_metadata(node_output_metadata)
            return

        # Fallback: Read from database cache
        canvas_id = self.config.get("canvas_id")
        if not canvas_id:
            logger.warning("[LINEAGE] No canvas_id in config, cannot read from cache")
            return

        logger.debug(f"[LINEAGE] Reading metadata from node_cache_metadata for canvas {canvas_id}")

        try:
            import psycopg2

            connection_config = self.config.get("connection_config") or self.config.get("connectionConfig") or {}
            if not connection_config:
                logger.warning("[LINEAGE] No connection_config, cannot read from cache")
                return

            conn = psycopg2.connect(
                host=connection_config.get("host") or connection_config.get("hostname"),
                port=int(connection_config.get("port", 5432)),
                dbname=connection_config.get("database"),
                user=connection_config.get("user") or connection_config.get("username"),
                password=connection_config.get("password", ""),
            )
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT node_id, columns
                    FROM "CANVAS_CACHE".node_cache_metadata
                    WHERE canvas_id = %s AND is_valid = TRUE
                """, (canvas_id,))
                rows = cursor.fetchall()
            finally:
                conn.close()

            logger.info(f"[LINEAGE] Loaded metadata for {len(rows)} nodes from cache")

            # Build metadata dict
            cached_metadata = {}
            for node_id, columns_json in rows:
                if columns_json:
                    cached_metadata[node_id] = {"columns": columns_json}

            self._build_lineage_from_metadata(cached_metadata)

        except Exception as e:
            logger.error(f"[LINEAGE] Failed to read from cache: {e}")
            import traceback
            logger.debug(f"[LINEAGE] Traceback: {traceback.format_exc()}")

    def _build_lineage_from_metadata(self, node_output_metadata: dict):
        """Build lineage from metadata dict."""
        logger.debug(f"[LINEAGE] Building from metadata: {len(node_output_metadata)} nodes")

        # Process all nodes
        for node_id in self.nodes:
            if node_id not in node_output_metadata:
                logger.debug(f"[LINEAGE] No metadata for node {node_id[:8]}, skipping")
                continue

            metadata = node_output_metadata[node_id]
            columns_meta = metadata.get("columns", [])

            logger.debug(f"[LINEAGE] Processing node {node_id[:8]} with {len(columns_meta)} columns")

            self.lineage.lineage[node_id] = {}

            for col_meta in columns_meta:
                # Get all column identifiers (business_name = user-facing name shown in UI)
                name = col_meta.get("business_name") or col_meta.get("name")
                technical_name = col_meta.get("technical_name")
                db_name = col_meta.get("db_name")
                base_node = col_meta.get("base")

                if not name:
                    continue

                # Determine original column name and source node
                if base_node:
                    # Column comes from a specific source node
                    # Use db_name as the original column name (what exists in the source table)
                    original_name = db_name or technical_name or name
                    source_node = base_node
                else:
                    # Column defined at this node (source or calculated)
                    original_name = db_name or technical_name or name
                    source_node = node_id

                # Store lineage with both display name and technical name for backtracking
                lineage_info = {
                    "source_node": source_node,
                    "original_name": original_name,
                    "technical_name": technical_name,
                    "db_name": db_name,
                    "transformation": "metadata"
                }

                # Principal index by name (display/ref name)
                self.lineage.lineage[node_id][name] = lineage_info

                # Secondary index by technical_name for rename-safety/backtracking
                if technical_name and technical_name != name:
                    self.lineage.lineage[node_id][technical_name] = lineage_info

                logger.debug(f"[LINEAGE]   {name} ({technical_name}) → source={source_node[:8] if source_node else 'N/A'}, original={original_name}")

            logger.debug(f"[LINEAGE] Tracked {len(self.lineage.lineage[node_id])} columns for node {node_id[:8]}")

        logger.info(f"[LINEAGE] Built lineage for {len(self.lineage.lineage)} nodes")

    def _index_calculated_columns(self):
        """Index all calculated columns in the pipeline."""
        for node_id, node in self.nodes.items():
            node_type = node.get("type") or node.get("data", {}).get("type")
            config = node.get("data", {}).get("config", {})

            if node_type == "projection":
                calculated_cols = config.get("calculated_columns", [])
                self.calculated_columns[node_id] = {}

                for calc_col in calculated_cols:
                    col_name = calc_col.get("name")
                    expression = calc_col.get("expression")

                    if col_name and expression:
                        self.calculated_columns[node_id][col_name] = expression

    def analyze_filter(
        self,
        filter_node_id: str,
        filter_condition: dict[str, Any]
    ) -> PushdownDecision:
        """
        Analyze if a filter can be pushed down.

        Returns:
            PushdownDecision with can_push, push_to_node, and rewritten_condition
        """
        column = filter_condition.get("column")

        # Check if this is a calculated column
        calc_info = self._find_calculated_column(filter_node_id, column)

        if calc_info:
            return self._analyze_calculated_column_filter(
                filter_node_id,
                filter_condition,
                calc_info
            )
        else:
            return self._analyze_simple_column_filter(
                filter_node_id,
                filter_condition
            )

    def _find_calculated_column(
        self,
        filter_node_id: str,
        column_name: str
    ) -> Optional[tuple[str, str, CalculatedColumnType]]:
        """
        Find if a column is calculated and get its expression.

        Returns:
            (defining_node_id, expression, calc_type) or None
        """
        # Traverse upstream to find the node that defines this calculated column
        visited = set()
        queue = [filter_node_id]

        while queue:
            current_id = queue.pop(0)
            if current_id in visited:
                continue
            visited.add(current_id)

            # Check if this node defines the column
            if current_id in self.calculated_columns:
                if column_name in self.calculated_columns[current_id]:
                    expression = self.calculated_columns[current_id][column_name]
                    calc_type = CalculatedColumnAnalyzer.classify_expression(expression)
                    return (current_id, expression, calc_type)

            # Add parents to queue
            for parent_id in self.reverse_adj.get(current_id, []):
                queue.append(parent_id)

        return None

    def _analyze_calculated_column_filter(
        self,
        filter_node_id: str,
        filter_condition: dict[str, Any],
        calc_info: tuple[str, str, CalculatedColumnType]
    ) -> PushdownDecision:
        """Analyze filter on a calculated column."""
        defining_node_id, expression, calc_type = calc_info

        if calc_type == CalculatedColumnType.AGGREGATE:
            return PushdownDecision(
                can_push=False,
                reason="Cannot push filters on aggregate columns (use HAVING instead)"
            )

        if calc_type == CalculatedColumnType.WINDOW_FUNCTION:
            return PushdownDecision(
                can_push=False,
                reason="Cannot push filters on window functions"
            )

        if calc_type == CalculatedColumnType.SIMPLE_EXPRESSION:
            # Check if all dependencies are available upstream
            dependencies = CalculatedColumnAnalyzer.extract_column_dependencies(expression)

            # Find the best node to push to (immediate parent of defining node)
            push_target = self._find_push_target(defining_node_id, dependencies)

            if push_target:
                # Rewrite condition to use expression
                where_clause = CalculatedColumnAnalyzer.rewrite_filter_with_expression(
                    filter_condition,
                    expression
                )

                return PushdownDecision(
                    can_push=True,
                    push_to_node=push_target,
                    rewritten_condition={
                        **filter_condition,
                        "expression": expression,
                        "where_clause": where_clause
                    },
                    reason=f"Simple expression can be pushed to {push_target}"
                )
            else:
                return PushdownDecision(
                    can_push=False,
                    reason=f"Dependencies {dependencies} not available upstream"
                )

        return PushdownDecision(
            can_push=False,
            reason="Unknown calculated column type"
        )

    def _analyze_simple_column_filter(
        self,
        filter_node_id: str,
        filter_condition: dict[str, Any]
    ) -> PushdownDecision:
        """Analyze filter on a simple (non-calculated) column."""
        column = filter_condition.get("column")

        logger.debug(f"[PUSHDOWN] Analyzing simple column filter: {column}")
        logger.debug(f"[PUSHDOWN] Filter node: {filter_node_id[:8]}")

        # Check if we have lineage for this node
        if filter_node_id not in self.lineage.lineage:
            logger.warning(f"[PUSHDOWN] No lineage found for filter node {filter_node_id[:8]}")
            logger.debug(f"[PUSHDOWN] Available lineage nodes: {list(self.lineage.lineage.keys())}")
            return PushdownDecision(
                can_push=False,
                reason="No lineage information for filter node"
            )

        # Check if column exists in this node's lineage.
        # Fallback: try _L_/_R_ stripped name (e.g. _L_cmp_id -> cmp_id) and suffix match for join output columns.
        node_lineage = self.lineage.lineage[filter_node_id]
        resolved_column = column
        if column not in node_lineage:
            if column.startswith("_L_") and len(column) > 3:
                resolved_column = column[3:]
            elif column.startswith("_R_") and len(column) > 3:
                resolved_column = column[3:]
            if resolved_column not in node_lineage:
                # Try suffix match (e.g. filter uses _L_cmp_id, lineage has 39ef59b7_cmp_id)
                base = resolved_column
                for k in node_lineage.keys():
                    if k == base or k.endswith("_" + base) or k == "_L_" + base or k == "_R_" + base:
                        resolved_column = k
                        break
                else:
                    logger.warning(f"[PUSHDOWN] Column '{column}' not found in lineage for node {filter_node_id[:8]}")
                    logger.debug(f"[PUSHDOWN] Available columns: {list(node_lineage.keys())}")
                    return PushdownDecision(
                        can_push=False,
                        reason=f"Column '{column}' not in lineage (available: {list(node_lineage.keys())[:5]})"
                    )

        # Find the source node for this column
        source_info = self.lineage.get_original_column(filter_node_id, resolved_column)

        if source_info:
            source_node_id, original_col = source_info
            logger.info(f"[PUSHDOWN] Traced '{column}' → source {source_node_id[:8]}, original name: '{original_col}'")

            # Rewrite condition with original column name (in case it was renamed)
            rewritten_condition = {
                **filter_condition,
                "column": original_col
            }

            return PushdownDecision(
                can_push=True,
                push_to_node=source_node_id,
                rewritten_condition=rewritten_condition,
                reason=f"Column '{column}' can be pushed to source {source_node_id[:8]} as '{original_col}'"
            )
        else:
            logger.warning(f"[PUSHDOWN] Could not trace '{column}' to source")
            return PushdownDecision(
                can_push=False,
                reason=f"Cannot determine source for column '{column}'"
            )

    def _find_push_target(self, defining_node_id: str, dependencies: list[str]) -> Optional[str]:
        """Find the best node to push a filter to."""
        # For now, push to immediate parent of defining node
        parents = self.reverse_adj.get(defining_node_id, [])
        if parents:
            return parents[0]
        return None

    def optimize_all_filters(self) -> dict[str, Any]:
        """
        Analyze all filter nodes and generate pushdown plan.

        Returns:
            {
                "plan": {target_node_id: [filter_conditions_to_push]},
                "fully_pushed_nodes": [node_ids_to_skip]
            }
        """
        pushdown_plan = defaultdict(list)
        fully_pushed_nodes = []

        logger.debug(f"[PUSHDOWN] Scanning {len(self.nodes)} nodes for filters...")
        filter_count = 0

        for node_id, node in self.nodes.items():
            node_type = node.get("type") or node.get("data", {}).get("type")

            if node_type != "filter":
                continue

            filter_count += 1
            config = node.get("data", {}).get("config", {})
            conditions = config.get("conditions", [])

            if not conditions:
                continue

            pushed_count = 0
            for idx, condition in enumerate(conditions):
                decision = self.analyze_filter(node_id, condition)

                if decision.can_push:
                    pushdown_plan[decision.push_to_node].append(decision.rewritten_condition)
                    pushed_count += 1
                else:
                    logger.debug(f"[PUSHDOWN] Condition {idx} for node {node_id[:8]} not pushed: {decision.reason}")

            # If all conditions for this node were pushed, it can be skipped in execution
            if pushed_count == len(conditions) and pushed_count > 0:
                fully_pushed_nodes.append(node_id)
                logger.info(f"[PUSHDOWN] Filter node {node_id[:8]} is fully pushed and can be skipped.")

        if filter_count == 0:
            logger.info("[PUSHDOWN] No filter nodes found in pipeline")
        else:
            logger.info(f"[PUSHDOWN] Analyzed {filter_count} filter node(s), generated pushdown for {len(pushdown_plan)} target(s)")
            if fully_pushed_nodes:
                logger.info(f"[PUSHDOWN] {len(fully_pushed_nodes)} filter node(s) can be skipped entirely")

        return {
            "plan": dict(pushdown_plan),
            "fully_pushed_nodes": fully_pushed_nodes
        }

# =============================================================================
# PUBLIC API
# =============================================================================

def analyze_filter_pushdown(
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    config: Optional[dict[str, Any]] = None
) -> dict[str, list[dict]]:
    """
    Analyze all filters in the pipeline and generate pushdown plan.

    Args:
        nodes: Pipeline nodes (can be dict or list)
        edges: Pipeline edges
        config: Pipeline config containing node_output_metadata

    Returns:
        {target_node_id: [filter_conditions_to_push]}
    """
    try:
        logger.info(f"[PUSHDOWN] Starting analysis with {len(nodes)} nodes, {len(edges)} edges")

        # Debug: Log input types
        logger.debug(f"[PUSHDOWN] Nodes type: {type(nodes)}, Edges type: {type(edges)}")
        logger.debug(f"[PUSHDOWN] Config provided: {config is not None}")

        # Normalize nodes to dict format if it's a list
        if isinstance(nodes, list):
            logger.debug("[PUSHDOWN] Converting nodes from list to dict...")
            nodes_dict = {}
            for node in nodes:
                if isinstance(node, dict):
                    node_id = node.get("id")
                    if node_id:
                        nodes_dict[node_id] = node
                    else:
                        logger.warning(f"[PUSHDOWN] Node missing 'id' field: {node}")
                else:
                    logger.warning(f"[PUSHDOWN] Invalid node type: {type(node)}")
            nodes = nodes_dict
            logger.debug(f"[PUSHDOWN] Converted to dict with {len(nodes)} nodes")

        # Debug: Log sample node
        if nodes:
            sample_node_id = next(iter(nodes.keys()))
            sample_node = nodes[sample_node_id]
            logger.debug(f"[PUSHDOWN] Sample node {sample_node_id[:8]}: type={type(sample_node)}")
            logger.debug(f"[PUSHDOWN] Sample node keys: {list(sample_node.keys()) if isinstance(sample_node, dict) else 'N/A'}")

        # Debug: Log sample edge
        if edges:
            sample_edge = edges[0]
            logger.debug(f"[PUSHDOWN] Sample edge: type={type(sample_edge)}, value={sample_edge}")

        # Create optimizer with config
        logger.debug("[PUSHDOWN] Creating FilterPushdownOptimizer...")
        optimizer = FilterPushdownOptimizer(nodes, edges, config)
        logger.debug("[PUSHDOWN] Optimizer created successfully")

        # Run optimization
        logger.debug("[PUSHDOWN] Running optimize_all_filters...")
        result = optimizer.optimize_all_filters()
        logger.debug(f"[PUSHDOWN] Optimization complete, result type: {type(result)}")

        if isinstance(result, dict) and "plan" in result:
            logger.info(f"[PUSHDOWN] Analysis complete: {len(result['plan'])} nodes with pushed filters")
        else:
            logger.info(f"[PUSHDOWN] Analysis complete: {len(result)} nodes with pushed filters")
        return result

    except Exception as e:
        logger.error(f"[PUSHDOWN] ✗ Error during analysis: {e}")
        logger.error(f"[PUSHDOWN] Error type: {type(e).__name__}")

        import traceback
        logger.error("[PUSHDOWN] Full traceback:")
        for line in traceback.format_exc().split('\n'):
            logger.error(f"[PUSHDOWN]   {line}")

        # Return empty dict to allow validation to continue
        return {}
