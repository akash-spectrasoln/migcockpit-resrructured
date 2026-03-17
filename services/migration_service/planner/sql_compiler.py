"""
SQL Compiler Module
Compiles nodes into nested SQL or staging table creation.

RULES:
- Linear chains → nested SELECT
- Stop at source or previous JOIN materialization
- Zero CREATE TABLE in nested SQL
- Zero CTE (WITH) in nested SQL
- Pure SELECT only
"""

from dataclasses import dataclass
import logging
import re
from typing import Any, Optional

from .staging_naming import get_staging_table_name

logger = logging.getLogger(__name__)

class SQLCompilationError(Exception):
    """Raised when SQL compilation fails."""
    pass

@dataclass
class CompiledSQL:
    """Result of SQL compilation."""
    sql: str
    is_nested: bool  # True if nested SELECT, False if CREATE TABLE
    dependencies: list[str]  # Node IDs this SQL depends on

def _predicate_signature(pred: str) -> str:
    """
    Normalize a predicate to a signature for deduplication.
    E.g. '"4fa62c23_employee_range" = \'50-100\'' and '"employee_range" = \'50-100\''
    both map to the same signature.
    """
    pred = pred.strip()
    if not pred:
        return ""
    # Simple pattern: "col" op val or col op val
    m = re.match(r'^\(?["\']?([a-zA-Z0-9_]+)["\']?\)?\s*([=<>!]+|IN|IS\s+NULL|IS\s+NOT\s+NULL|LIKE)\s*(.+)', pred, re.IGNORECASE | re.DOTALL)
    if m:
        col, op, val = m.group(1).strip(), m.group(2).upper().replace(" ", ""), m.group(3).strip()
        # Normalize column: strip source prefix (8-char hex + underscore)
        if "_" in col and re.match(r"^[a-f0-9]{8}_", col, re.I):
            col_base = col.split("_", 1)[-1]
        else:
            col_base = col
        return f"{col_base}|{op}|{val}"
    return pred  # fallback: use as-is

def _dedupe_where_parts(where_parts: list[str]) -> list[str]:
    """Remove duplicate WHERE predicates (exact and semantic duplicates)."""
    seen_exact: set[str] = set()
    seen_sig: set[str] = set()
    result: list[str] = []
    for p in where_parts:
        norm = p.strip()
        if not norm:
            continue
        if norm in seen_exact:
            continue
        sig = _predicate_signature(norm)
        if sig and sig in seen_sig:
            continue
        seen_exact.add(norm)
        if sig:
            seen_sig.add(sig)
        result.append(p)
    return result

def _quote_staging_table(staging_table: str) -> str:
    """Properly quote a staging table name for PostgreSQL.

    Converts 'staging_job_xxx.node_yyy' → '"staging_job_xxx"."node_yyy"'
    PostgreSQL requires schema and table names to be quoted separately.
    """
    if '.' in staging_table:
        schema, table = staging_table.split('.', 1)
        return f'"{schema}"."{table}"'
    return f'"{staging_table}"'

def _flatten_nested_select(nested_sql: str, col_list: str) -> Optional[str]:
    """
    When nested_sql is SELECT ... FROM ( inner ) alias (one or more levels),
    reduce to a single SELECT ... FROM base_table [ WHERE ... ] when possible.
    Uses the current level's select list (including calculated column expressions)
    when recursing so the flattened query preserves expressions. Falls back to
    col_list only when the final inner is SELECT * FROM ....
    """
    current = nested_sql.strip()
    # Match: SELECT anything FROM ( inner ) alias
    outer = re.match(
        r"SELECT\s+.+?\s+FROM\s+\(\s*(.+)\s*\)\s+(\w+)\s*$",
        current,
        re.IGNORECASE | re.DOTALL,
    )
    if not outer:
        return None
    inner = outer.group(1).strip()
    # This level's select list (may include calculated columns e.g. (UPPER("status")) AS "UPPER_STATUS").
    outer_select_match = re.match(r"SELECT\s+(.+?)\s+FROM\s+", current, re.IGNORECASE | re.DOTALL)
    this_level_select = outer_select_match.group(1).strip() if outer_select_match else col_list
    inner_flat = _flatten_nested_select(inner, this_level_select)
    if inner_flat is not None:
        inner = inner_flat
    # Inner must be a single SELECT FROM table [ WHERE ... ] (no nested FROM)
    if re.search(r"\s+FROM\s+\(", inner, re.IGNORECASE):
        return None
    inner_match = re.match(
        r"SELECT\s+(.+?)\s+FROM\s+(.+?)(?:\s+WHERE\s+(.+))?\s*$",
        inner,
        re.IGNORECASE | re.DOTALL,
    )
    if not inner_match:
        return None
    inner_select_list = inner_match.group(1).strip()
    from_clause = inner_match.group(2).strip()
    where_clause = inner_match.group(3)
    if where_clause:
        where_clause = where_clause.strip()
    # When inner is a source (SELECT db_name AS technical_name FROM base_table), keep the inner
    # select list so we never produce SELECT technical_name FROM base_table (which would fail).
    # Only do this when flattening down to a base table (not staging); otherwise keep outer list.
    is_base_table = "staging_jobs" not in from_clause
    if is_base_table and " AS " in inner_select_list:
        out_select = inner_select_list
    else:
        out_select = this_level_select
    if where_clause:
        return f"SELECT {out_select} FROM {from_clause} WHERE {where_clause}"
    return f"SELECT {out_select} FROM {from_clause}"

def compile_nested_sql(
    node_id: str,
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    materialization_points: dict[str, Any],
    config: dict[str, Any]
) -> CompiledSQL:
    """
    Compile a node into nested SQL.

    Traverses upstream until:
    - Source node, OR
    - Previous JOIN materialization

    Returns single SELECT statement.

    Args:
        node_id: Target node to compile
        nodes: Map of node_id -> node dict
        edges: List of edges
        materialization_points: Nodes that are materialized
        config: Configuration with source/destination configs

    Returns:
        CompiledSQL with nested SELECT
    """
    node = nodes[node_id]
    node_type = _get_node_type(node)
    reverse_adjacency = _build_reverse_adjacency(edges)

    # Build nested SQL by traversing upstream
    dependencies = []

    def traverse_upstream(current_id: str, alias_suffix: str = "") -> str:
        """Recursively build nested SQL."""
        current_node = nodes[current_id]
        current_type = _get_node_type(current_node)
        parents = reverse_adjacency.get(current_id, [])

        # STOP CONDITION 1: Materialized node (shared source, JOIN, or branch end) - read from staging, not DB
        # Must check before source: a shared source has staging created in Level 0; downstream must read from it.
        if current_id in materialization_points and current_id != node_id:
            staging_table = materialization_points[current_id].staging_table
            dependencies.append(current_id)
            return f'SELECT * FROM {_quote_staging_table(staging_table)}'

        # STOP CONDITION 2: Source node (not materialized - e.g. single branch, read from DB once in chain)
        if current_type == "source":
            source_sql = _compile_source_node(current_node, nodes, edges, config)
            dependencies.append(current_id)
            return source_sql

        # Recursive case: build nested SQL
        if not parents:
            raise SQLCompilationError(
                f"Node '{current_id}' has no parents and is not a source"
            )

        # Get upstream SQL (should be single parent for linear chain)
        if len(parents) > 1:
            raise SQLCompilationError(
                f"Node '{current_id}' has multiple parents but is not a JOIN"
            )

        upstream_sql = traverse_upstream(parents[0], alias_suffix + "_up")

        # Check if this node should be skipped (e.g., fully pushed down filter)
        pushed_nodes = config.get("pushed_filter_nodes", [])
        if current_id in pushed_nodes:
            logger.info(f"[SQL] Skipping fully pushed filter node {current_id[:8]}")
            return upstream_sql

        # Apply transformation for current node
        return _apply_transformation(
            current_node,
            upstream_sql,
            current_type,
            alias_suffix,
            config=config,
            node_id=current_id,
        )

    # Start traversal from target node
    _build_reverse_adjacency(edges)
    if node_type == "source":
        sql = _compile_source_node(node, nodes, edges, config)
        dependencies.append(node_id)
    else:
        sql = traverse_upstream(node_id)

    return CompiledSQL(
        sql=sql,
        is_nested=True,
        dependencies=dependencies
    )

def compile_aggregation_sql(
    agg_node_id: str,
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    materialization_points: dict[str, Any],
    config: dict[str, Any],
    job_id: str,
) -> CompiledSQL:
    """
    Build CREATE TABLE staging AS SELECT group_by_cols, agg_exprs
    FROM upstream_staging GROUP BY group_by_cols [HAVING ...].
    Pre-aggregation filters belong in WHERE on upstream (handled by segment).
    """
    agg_node = nodes.get(agg_node_id, {})
    agg_config = agg_node.get("data", {}).get("config", {})
    reverse_adj = _build_reverse_adjacency(edges)
    parents = reverse_adj.get(agg_node_id, [])
    if len(parents) != 1:
        raise SQLCompilationError(f"Aggregation node '{agg_node_id}' must have exactly one parent")
    parent_id = parents[0]
    if parent_id not in materialization_points:
        raise SQLCompilationError(f"Aggregation upstream '{parent_id}' must be materialized")
    upstream_staging = materialization_points[parent_id].staging_table
    quoted_upstream = _quote_staging_table(upstream_staging)
    staging_table = get_staging_table_name(job_id, agg_node_id)

    group_by = agg_config.get("groupBy", []) or agg_config.get("group_by", [])
    aggregates = agg_config.get("aggregates", []) or agg_config.get("aggregations", [])
    having = agg_config.get("having", []) or agg_config.get("havingConditions", [])

    select_parts: list[str] = []
    for col in group_by:
        c = col.get("column") or col.get("name") if isinstance(col, dict) else col
        if c:
            select_parts.append(f'"{c}"')
    for agg in aggregates:
        if not isinstance(agg, dict):
            continue
        func = (agg.get("function") or agg.get("aggregation") or "SUM").upper().strip()
        col = agg.get("column") or agg.get("field")
        alias = agg.get("alias") or agg.get("name") or (f"{func}_{col}" if col else f"{func}_1")
        if col == "*" or not col:
            select_parts.append(f'{func}(*) AS "{alias}"')
        else:
            select_parts.append(f'{func}("{col}") AS "{alias}"')

    if not select_parts:
        select_parts = ['*']
    select_clause = ", ".join(select_parts)
    group_clause = ""
    if group_by:
        group_cols = [c.get("column") or c.get("name") if isinstance(c, dict) else c for c in group_by if (c if not isinstance(c, dict) else c.get("column") or c.get("name"))]
        group_clause = " GROUP BY " + ", ".join(f'"{c}"' for c in group_cols if c)

    having_parts: list[str] = []
    for cond in having:
        if isinstance(cond, dict) and cond.get("column") is not None:
            col = cond.get("column")
            op = cond.get("operator", "=")
            val = cond.get("value")
            if isinstance(val, str):
                val_s = f"'{val}'"
            elif val is None:
                val_s = "NULL"
            else:
                val_s = str(val)
            having_parts.append(f'"{col}" {op} {val_s}')
    having_clause = " HAVING " + " AND ".join(having_parts) if having_parts else ""

    sql = f'CREATE TABLE {_quote_staging_table(staging_table)} AS\nSELECT {select_clause}\nFROM {quoted_upstream}{group_clause}{having_clause}'
    return CompiledSQL(sql=sql, is_nested=False, dependencies=[parent_id])

def compile_join_sql(
    join_node_id: str,
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    materialization_points: dict[str, Any],
    config: dict[str, Any],
    job_id: str
) -> CompiledSQL:
    """
    Compile JOIN node into CREATE TABLE statement.
    """
    join_node = nodes[join_node_id]
    join_config = join_node.get("data", {}).get("config", {})
    reverse_adjacency = _build_reverse_adjacency(edges)
    parents = reverse_adjacency.get(join_node_id, [])
    if len(parents) < 2:
        raise SQLCompilationError(
            f"JOIN node '{join_node_id}' must have >= 2 parents"
        )

    # Determine left/right from edge targetHandle or sourceHandle so order is deterministic
    left_id = None
    right_id = None
    for edge in edges:
        if edge.get("target") != join_node_id:
            continue
        handle = (edge.get("targetHandle") or edge.get("sourceHandle") or "").lower()
        if "left" in handle:
            left_id = edge["source"]
        elif "right" in handle:
            right_id = edge["source"]
    if not left_id or not right_id:
        left_id = parents[0]
        right_id = parents[1]

    if left_id not in materialization_points:
        raise SQLCompilationError(
            f"JOIN left branch '{left_id}' is not materialized"
        )
    if right_id not in materialization_points:
        raise SQLCompilationError(
            f"JOIN right branch '{right_id}' is not materialized"
        )

    _ = materialization_points[left_id].staging_table
    _ = materialization_points[right_id].staging_table

    # Build JOIN SQL
    _ = join_config.get("joinType", "INNER").upper()
    conditions = join_config.get("conditions", [])

    if not conditions:
        raise SQLCompilationError(
            f"JOIN node '{join_node_id}' has no join conditions"
        )

    # Map config column names to actual staging column names (technical_name when metadata present)
    def _name_to_staging_col(node_id: str, config_name: str) -> str:
        meta = config.get("node_output_metadata", {}).get(node_id, {})
        if meta and meta.get("columns"):
            for c in meta["columns"]:
                if c.get("business_name") == config_name or c.get("name") == config_name or c.get("technical_name") == config_name:
                    return c.get("technical_name") or c.get("business_name") or c.get("name") or config_name
        return config_name

    # Build ON clause (use staging column names so they exist in l/r tables)
    on_parts = []
    for cond in conditions:
        left_col = cond.get("leftColumn")
        right_col = cond.get("rightColumn")
        operator = cond.get("operator", "=")
        if not left_col or not right_col:
            raise SQLCompilationError(
                f"JOIN condition missing columns: {cond}"
            )
        left_staging = _name_to_staging_col(left_id, left_col)
        right_staging = _name_to_staging_col(right_id, right_col)
        on_parts.append(f'l."{left_staging}" {operator} r."{right_staging}"')
    _ = " AND ".join(on_parts)

    # Create staging table for JOIN result
    _ = get_staging_table_name(job_id, join_node_id)

    # Build select clause with collision handling
    reverse_adj = _build_reverse_adjacency(edges)
    left_columns = _infer_columns(left_id, nodes, edges, config, reverse_adj=reverse_adj)
    right_columns = _infer_columns(right_id, nodes, edges, config, reverse_adj=reverse_adj)

    if left_columns and right_columns:
        # Resolve to staging column names (technical_name); staging tables use technical_name, not db_name
        left_staging_names = [_name_to_staging_col(left_id, c) for c in left_columns]
        right_staging_names = [_name_to_staging_col(right_id, c) for c in right_columns]
        # Exclude columns that belong to the other branch (wrong cache can put e.g. 5d2647be_* in right node's metadata)
        left_prefix = left_id[:8]
        right_prefix = right_id[:8]
        left_pairs = [(c, sc) for c, sc in zip(left_columns, left_staging_names) if not sc.startswith(right_prefix)]
        right_pairs = [(c, sc) for c, sc in zip(right_columns, right_staging_names) if not sc.startswith(left_prefix)]
        # Never add back columns that belong to the other branch (keep filter in fallback)
        if not left_pairs:
            left_pairs = [(c, sc) for c, sc in zip(left_columns, left_staging_names) if not sc.startswith(right_prefix)]
        if not right_pairs:
            right_pairs = [(c, sc) for c, sc in zip(right_columns, right_staging_names) if not sc.startswith(left_prefix)]
        filtered_right = len(right_columns) - len(right_pairs)
        if filtered_right > 0:
            logger.info(
                f"[SQL] JOIN {join_node_id[:8]}: filtered {filtered_right} right-column(s) with prefix {left_prefix} "
                f"(l={left_id[:8]}, r={right_id[:8]})"
            )
        if not right_pairs and right_columns:
            logger.warning(
                f"[SQL] JOIN {join_node_id[:8]}: all right columns were filtered (prefix {left_prefix}); "
                f"right branch may have wrong metadata (l={left_id[:8]}, r={right_id[:8]})"
            )
        left_staging_names = [sc for _, sc in left_pairs]
        right_staging_names = [sc for _, sc in right_pairs]
        left_set = set(left_staging_names)
        right_set = set(right_staging_names)
        ambiguous = left_set & right_set

        # Reuse same staging select-list rule as linear: output columns are technical_name (_L_/_R_ for ambiguous)
        left_select = _build_staging_select_list(
            left_staging_names, table_alias="l", ambiguous_set=ambiguous, ambiguous_suffix="_L_"
        )
        right_select = _build_staging_select_list(
            right_staging_names, table_alias="r", ambiguous_set=ambiguous, ambiguous_suffix="_R_"
        )
        parts = [p for p in (left_select, right_select) if p]
        ", ".join(parts) if parts else "l.*, r.*"
    else:
        # Fallback to * if columns unknown
        pass

    # Base SQL for the JOIN
    join_sql = '''SELECT {select_clause}
FROM {_quote_staging_table(left_table)} l
{join_type} JOIN {_quote_staging_table(right_table)} r
ON {on_clause}'''

    # Apply pushed-down filters to the JOIN result if any
    pushdown_plan = config.get("filter_pushdown_plan", {})
    pushed_filters = pushdown_plan.get(join_node_id, [])

    if pushed_filters:
        where_parts = []
        for cond in pushed_filters:
            column = cond.get("column")
            operator = cond.get("operator", "=")
            value = cond.get("value")

            if not column:
                continue

            # Format value for SQL
            if isinstance(value, str):
                value_str = f"'{value}'"
            elif value is None:
                value_str = "NULL"
            else:
                value_str = str(value)

            where_parts.append(f'"{column}" {operator} {value_str}')

        if where_parts:
            where_clause = " AND ".join(where_parts)
            # Apply filter to join result
            join_sql = f"SELECT * FROM ({join_sql}) AS joined_filtered WHERE {where_clause}"
            logger.info(f"[SQL] Injected {len(where_parts)} pushed filters into JOIN node {join_node_id[:8]}")

    sql = '''CREATE TABLE {_quote_staging_table(staging_table)} AS
{join_sql}'''

    return CompiledSQL(
        sql=sql,
        is_nested=False,
        dependencies=[left_id, right_id]
    )

def _build_staging_select_list(
    column_names: list[str],
    table_alias: Optional[str] = None,
    ambiguous_set: Optional[set] = None,
    ambiguous_suffix: Optional[str] = None,
) -> str:
    """
    Build SELECT list fragment from staging column names (technical_name).
    Reused by linear staging and join so both use the same rule: staging columns = technical_name.
    """
    if not column_names:
        return ""
    parts = []
    for c in column_names:
        quoted = f'"{c}"'
        if table_alias:
            part = f'{table_alias}.{quoted}'
            if ambiguous_set is not None and ambiguous_suffix is not None and c in ambiguous_set:
                part = f'{part} AS "{ambiguous_suffix}{c}"'
        else:
            part = quoted
        parts.append(part)
    return ", ".join(parts)

def _infer_columns(
    node_id: str,
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    config: dict[str, Any],
    cache: Optional[dict[str, list[str]]] = None,
    reverse_adj: Optional[dict[str, list[str]]] = None
) -> Optional[list[str]]:
    """Recursively infer column names for a node."""
    if cache is None:
        cache = {}
    if node_id in cache:
        return cache[node_id]

    # 1. Check node_output_metadata (use technical_name when present so JOIN/staging match actual table columns)
    metadata = config.get("node_output_metadata", {}).get(node_id)
    if metadata and "columns" in metadata:
        res = []
        for c in metadata["columns"]:
            name = c.get("technical_name") or c.get("business_name") or c.get("name")
            if name:
                res.append(name)
        if res:
            cache[node_id] = res
            return res

    node = nodes.get(node_id, {})
    node_config = node.get("data", {}).get("config", {})
    node_type = _get_node_type(node)

    # 2. Check Projection config: base columns + calculated column names
    if node_type == "projection":
        base_cols = node_config.get("columns") or node_config.get("selectedColumns") or []
        calculated = node_config.get("calculated_columns") or node_config.get("calculatedColumns") or []
        res = []
        for c in base_cols:
            if isinstance(c, dict):
                # Prefer technical_name (matches staging); fall back to name/label/id
                name = c.get("technical_name") or c.get("name") or c.get("label") or c.get("id")
                if name and name not in res:
                    res.append(name)
            elif isinstance(c, str) and c not in res:
                res.append(c)
        for calc in calculated:
            if isinstance(calc, dict):
                name = calc.get("name") or calc.get("alias")
                if name and name not in res:
                    res.append(name)
        if res:
            # Resolve to parent's output column names (technical_name); projection reads from staging which has technical_name only
            if reverse_adj is None:
                reverse_adj = _build_reverse_adjacency(edges)
            parents = reverse_adj.get(node_id, [])
            if len(parents) == 1:
                parent_id = parents[0]
                parent_meta = config.get("node_output_metadata", {}).get(parent_id, {}).get("columns", [])
                parent_cols = _infer_columns(parent_id, nodes, edges, config, cache, reverse_adj)
                name_to_tech = {}
                if parent_meta and isinstance(parent_meta[0], dict):
                    for col in parent_meta:
                        tech = col.get("technical_name") or col.get("business_name") or col.get("name")
                        if tech:
                            bn = col.get("business_name") or col.get("name")
                            if bn:
                                name_to_tech[bn] = tech
                            if col.get("technical_name"):
                                name_to_tech[col["technical_name"]] = tech
                parent_set = set(parent_cols) if parent_cols else set()
                resolved = []
                unresolved = []
                for name in res:
                    if name in parent_set:
                        resolved.append(name)
                    elif name in name_to_tech:
                        resolved.append(name_to_tech[name])
                    else:
                        # Parent may be join with no metadata: match by suffix (e.g. dst_schema -> 9aad5245_dst_schema)
                        # Use case-insensitive suffix match so "DELETED" matches "9aad5245_DELETED"
                        name_suffix = "_" + name
                        name_suffix_lower = name_suffix.lower()
                        matches = [
                            c for c in (parent_cols or [])
                            if c == name or c.endswith(name_suffix) or c.lower().endswith(name_suffix_lower)
                        ]
                        if len(matches) >= 1:
                            # Prefer single match; if multiple (e.g. both branches have "status"), take first (left)
                            resolved.append(matches[0])
                            if len(matches) > 1:
                                unresolved.append((name, len(matches), matches[:3]))
                        else:
                            resolved.append(name)
                            unresolved.append((name, 0, []))
                if unresolved:
                    logger.warning(
                        "[SQL] _infer_columns projection %s: could not resolve to parent columns: %s; "
                        "parent=%s parent_cols_sample=%s",
                        node_id[:8], unresolved, parent_id[:8], (parent_cols or [])[:15],
                    )
                res = resolved
            cache[node_id] = res
            return res

    # 3. Recursive trace
    if reverse_adj is None:
        reverse_adj = _build_reverse_adjacency(edges)
    parents = reverse_adj.get(node_id, [])

    res = None
    if node_type == "join" and len(parents) >= 2:
        left_cols = _infer_columns(parents[0], nodes, edges, config, cache, reverse_adj)
        r = _infer_columns(parents[1], nodes, edges, config, cache, reverse_adj)
        if left_cols and r:
            left_set = set(left_cols)
            right_set = set(r)
            ambiguous = left_set & right_set

            res = []
            for c in left_cols:
                res.append(f"_L_{c}" if c in ambiguous else c)
            for c in r:
                res.append(f"_R_{c}" if c in ambiguous else c)
    elif node_type == "compute" and parents:
        p = _infer_columns(parents[0], nodes, edges, config, cache, reverse_adj)
        if p:
            compute_cols = [c.get("alias") or c.get("name") for c in node_config.get("computedColumns", [])]
            res = p + [c for c in compute_cols if c]
    elif parents:
        # Pass-through for Filter, etc.
        res = _infer_columns(parents[0], nodes, edges, config, cache, reverse_adj)

    cache[node_id] = res
    return res

def _get_linear_segment_to_materialized_or_source(
    node_id: str,
    nodes: dict[str, Any],
    reverse_adjacency: dict[str, list[str]],
    materialization_points: dict[str, Any],
) -> Optional[tuple]:
    """
    Walk backwards from node_id. If the path is linear (single parent each step),
    return (segment_node_ids_from_upstream_to_downstream, upstream_id)
    where upstream_id is either a source node or a materialized node.
    Otherwise return None.
    """
    segment = []
    current = node_id
    visited = set()
    while current and current not in visited:
        visited.add(current)
        # Check materialization FIRST (even if it's also a source node)
        if current in materialization_points and current != node_id:
            segment.append(current)
            return (segment, current)
        node = nodes.get(current, {})
        node_type = _get_node_type(node)
        if node_type == "source":
            segment.append(current)
            return (segment, current)
        parents = reverse_adjacency.get(current, [])
        if len(parents) != 1:
            return None
        segment.append(current)
        current = parents[0]
    return None

def compile_staging_table_sql(
    node_id: str,
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    materialization_points: dict[str, Any],
    config: dict[str, Any],
    job_id: str
) -> CompiledSQL:
    """
    Compile node into CREATE TABLE AS <flat or nested SQL>.
    Uses flatten_segment when the path from this node back to source/staging is linear (Rule 3).
    """
    reverse_adj = _build_reverse_adjacency(edges)
    staging_table = get_staging_table_name(job_id, node_id)
    quoted_staging = _quote_staging_table(staging_table)

    def _guard_self_ref(sql: str) -> None:
        # Only check the source part (after AS) to avoid false positives on CREATE TABLE X AS
        normalized = sql.replace(" AS\n", " AS ")
        if " AS " in normalized:
            _, source_part = normalized.split(" AS ", 1)
        else:
            source_part = sql
        if staging_table in source_part or quoted_staging in source_part:
            raise SQLCompilationError(
                f"Self-reference detected for node {node_id}: compiled SQL references "
                f"its own staging table '{staging_table}'. "
                "Check traverse_upstream() resolves the correct parent node."
            )

    flat_sql = None
    dependencies: list[str] = []

    seg_result = _get_linear_segment_to_materialized_or_source(
        node_id, nodes, reverse_adj, materialization_points
    )
    if seg_result:
        segment_ids, upstream_id = seg_result
        segment_ids.reverse()
        node_type = _get_node_type(nodes.get(node_id, {}))
        upstream_type = _get_node_type(nodes.get(upstream_id, {}))
        if upstream_type == "source" and node_type not in ("join", "aggregation"):
            non_source_segment = [nid for nid in segment_ids if nid != upstream_id]
            from_override = None
            # When source is materialized (shared/multi_branch) and we're compiling a DOWNSTREAM node,
            # read from staging instead of source. Never use staging when compiling the source itself.
            if node_id != upstream_id and upstream_id in materialization_points:
                staging_table = materialization_points[upstream_id].staging_table
                from_override = _quote_staging_table(staging_table)
            flat_sql = flatten_segment_from_source(
                non_source_segment, upstream_id, nodes, edges, config,
                from_table_override=from_override,
            )
            dependencies = [upstream_id]
        elif upstream_id in materialization_points and node_type not in ("join", "aggregation"):
            upstream_staging = materialization_points[upstream_id].staging_table
            quoted = _quote_staging_table(upstream_staging)
            if len(segment_ids) > 1:
                upstream_cols = _infer_columns(upstream_id, nodes, edges, config)
                name_to_tech = _build_filter_col_to_upstream(upstream_cols or [])
                # When this node feeds a destination, include anchor columns so _R_cmp_id etc. are not NULL
                required_dest_cols: Optional[set[str]] = None
                children = [e["target"] for e in edges if isinstance(e, dict) and e.get("source") == node_id]
                for cid in children:
                    child_node = nodes.get(cid, {})
                    if _get_node_type(child_node) in ("destination", "destination-postgresql", "destination-postgres"):
                        schema_anchor_id = _find_schema_anchor(node_id, reverse_adj, nodes)
                        anchor_meta = config.get("node_output_metadata", {}).get(schema_anchor_id, {})
                        cols_meta = anchor_meta.get("columns", [])
                        tech_names = {
                            c.get("technical_name") for c in cols_meta
                            if c.get("technical_name")
                        }
                        if tech_names:
                            required_dest_cols = tech_names
                        break
                flat_sql = flatten_segment(
                    segment_ids, nodes, edges, config, quoted,
                    name_to_technical=name_to_tech,
                    required_destination_columns=required_dest_cols,
                )
                dependencies = [upstream_id]
            else:
                flat_sql = f"SELECT * FROM {quoted}"
                dependencies = [upstream_id]
        else:
            flat_sql = None

    if flat_sql:
        sql = f'CREATE TABLE {quoted_staging} AS\n{flat_sql}'
        _guard_self_ref(sql)
        return CompiledSQL(sql=sql, is_nested=False, dependencies=dependencies)

    nested = compile_nested_sql(
        node_id, nodes, edges, materialization_points, config
    )
    columns = _infer_columns(node_id, nodes, edges, config)
    resolved_via_upstream = False

    # When reading from a single staging table, resolve column names to that table's actual columns (technical_name)
    if columns and len(nested.dependencies) == 1 and "select" in nested.sql.lower() and "node_" in nested.sql:
        upstream_id = nested.dependencies[0]
        upstream_columns = _infer_columns(upstream_id, nodes, edges, config)
        if upstream_columns:
            upstream_set = set(upstream_columns)
            resolved = []
            for c in columns:
                if c in upstream_set:
                    resolved.append(c)
                else:
                    suf = "_" + c
                    suf_lower = suf.lower()
                    matches = [u for u in upstream_columns if u == c or u.endswith(suf) or u.lower().endswith(suf_lower)]
                    resolved.append(matches[0] if matches else c)
            columns = resolved
            resolved_via_upstream = True

    if columns:
        col_list = _build_staging_select_list(columns)
        # When we resolved against upstream, do NOT flatten and do NOT use nested.sql for the subquery:
        # nested.sql may contain inner SELECTs with wrong column names (dst_schema, DELETED, etc.).
        # Use a simple SELECT * FROM upstream_staging so only our resolved col_list is applied.
        if resolved_via_upstream and len(nested.dependencies) == 1:
            upstream_id = nested.dependencies[0]
            if upstream_id in materialization_points:
                upstream_staging = materialization_points[upstream_id].staging_table
                inner_sql = f'SELECT * FROM {_quote_staging_table(upstream_staging)}'
                sql = f'CREATE TABLE {quoted_staging} AS\nSELECT {col_list} FROM (\n{inner_sql}\n) nested'
            else:
                flat_sql = _flatten_nested_select(nested.sql, col_list)
                sql = f'CREATE TABLE {quoted_staging} AS\n{flat_sql}' if flat_sql else f'CREATE TABLE {quoted_staging} AS\nSELECT {col_list} FROM (\n{nested.sql}\n) nested'
        else:
            flat_sql = None if resolved_via_upstream else _flatten_nested_select(nested.sql, col_list)
            if flat_sql:
                sql = f'CREATE TABLE {quoted_staging} AS\n{flat_sql}'
            else:
                sql = f'CREATE TABLE {quoted_staging} AS\nSELECT {col_list} FROM (\n{nested.sql}\n) nested'
        # Debug: log when SELECT list contains names that look like raw db/display names (no source prefix)
        def has_prefix(s):
            return "_" in s and (s.startswith("9aad5245") or s.startswith("83654bba") or s.startswith("_L_") or s.startswith("_R_") or len(s) > 10)
        suspicious = [c for c in columns if not has_prefix(c)]
        if suspicious:
            logger.info(
                "[SQL] compile_staging_table_sql node %s: %s columns; may not exist in upstream: %s (upstream has technical_name)",
                node_id[:8], len(columns), suspicious[:15],
            )
    else:
        sql = f'CREATE TABLE {quoted_staging} AS\n{nested.sql}'
    _guard_self_ref(sql)
    return CompiledSQL(
        sql=sql,
        is_nested=False,
        dependencies=nested.dependencies
    )

def _collect_columns_and_calculated_from_branch(
    terminal_id: str,
    source_id: str,
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
) -> tuple:
    """
    Walk from terminal_id back to source_id, collecting base column names and
    calculated column definitions from projection nodes on the path.
    Returns (base_names: set, calculated: list of dicts with name, expression, node_id).
    """
    reverse_adj = _build_reverse_adjacency(edges)
    base_names = set()
    calculated_list = []  # list of {name, expression}; dedupe by name later
    seen_calc_names = set()
    current = terminal_id
    visited = set()
    while current and current != source_id and current not in visited:
        visited.add(current)
        node = nodes.get(current, {})
        node_type = _get_node_type(node)
        node_config = node.get("data", {}).get("config", {})
        if node_type == "projection":
            for col in node_config.get("columns", []) or node_config.get("selectedColumns", []):
                if isinstance(col, dict):
                    name = col.get("name")
                    if name:
                        base_names.add(name)
                elif isinstance(col, str):
                    base_names.add(col)
            for calc in node_config.get("calculated_columns", []) or node_config.get("calculatedColumns", []):
                if not isinstance(calc, dict):
                    continue
                name = calc.get("name") or calc.get("alias")
                expr = calc.get("expression")
                if name and expr and name not in seen_calc_names:
                    seen_calc_names.add(name)
                    calculated_list.append({"name": name, "expression": expr, "node_id": current})
                base_names.update(_extract_expression_column_refs(expr or ""))
        elif node_type == "compute":
            for comp in node_config.get("computedColumns", []):
                if isinstance(comp, dict):
                    name = comp.get("alias") or comp.get("name")
                    expr = comp.get("expression")
                    if name and expr and name not in seen_calc_names:
                        seen_calc_names.add(name)
                        calculated_list.append({"name": name, "expression": expr, "node_id": current})
        parents = reverse_adj.get(current, [])
        if len(parents) != 1:
            break
        current = parents[0]
    return (base_names, calculated_list)

def _get_segment_from_terminal_to_source(
    terminal_id: str,
    source_id: str,
    reverse_adj: dict[str, list[str]],
) -> list[str]:
    """Get segment node IDs in source-to-terminal order (from source's child to terminal)."""
    path = [terminal_id]
    current = terminal_id
    while current != source_id:
        parents = reverse_adj.get(current, [])
        if len(parents) != 1:
            break
        current = parents[0]
        path.append(current)
    if current != source_id:
        return []
    # path = [terminal, ..., source]; segment = [source's child, ..., terminal] = path[:-1] reversed
    return list(reversed(path[:-1]))

def _get_branch_filter_where_parts(
    terminal_id: str,
    source_id: str,
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    config: dict[str, Any],
) -> list[str]:
    """
    Get filter conditions for a single branch (terminal to source path) as SQL WHERE parts.
    Uses source db_name for column refs. Returns empty list if branch has no filters.
    """
    reverse_adj = _build_reverse_adjacency(edges)
    segment = _get_segment_from_terminal_to_source(terminal_id, source_id, reverse_adj)
    if not segment:
        return []

    metadata = config.get("node_output_metadata", {}).get(source_id, {}).get("columns", [])
    tech_to_db: dict[str, str] = {}
    if metadata and isinstance(metadata[0], dict):
        for col in metadata:
            db = col.get("db_name") or col.get("business_name") or col.get("name")
            tech = col.get("technical_name") or col.get("business_name") or col.get("name")
            bn = col.get("business_name") or col.get("name")
            if db:
                tech_to_db[tech] = db
                tech_to_db[db] = db
                if bn:
                    tech_to_db[bn] = db
        # Also map prefix_col (e.g. 4fa62c23_pricing) -> db_name for filters that use staging col names
        for col in metadata:
            db = col.get("db_name") or col.get("business_name") or col.get("name")
            if db:
                tech_to_db[f"{source_id[:8]}_{db}"] = db

    source_id[:8]
    calc_col_map: dict[str, str] = {}
    for node_id in segment:
        node = nodes.get(node_id, {})
        ntype = _get_node_type(node)
        nc = node.get("data", {}).get("config", {})
        if ntype == "projection":
            for calc in nc.get("calculated_columns", []) or nc.get("calculatedColumns", []):
                if not isinstance(calc, dict):
                    continue
                name = calc.get("name") or calc.get("alias")
                expr = calc.get("expression")
                if name and expr:
                    resolved = resolve_formula(expr, calc_col_map)
                    resolved_db = _rewrite_expression_column_refs(resolved, tech_to_db)
                    calc_col_map[name] = resolved_db
                    calc_col_map[f"{node_id[:8]}_{name}"] = resolved_db
        if ntype == "compute":
            for comp in nc.get("computedColumns", []):
                if isinstance(comp, dict) and comp.get("alias") and comp.get("expression"):
                    resolved = resolve_formula(comp["expression"], calc_col_map)
                    alias = comp["alias"]
                    calc_col_map[alias] = _rewrite_expression_column_refs(resolved, tech_to_db)
                    calc_col_map[f"{node_id[:8]}_{alias}"] = calc_col_map[alias]

    where_parts: list[str] = []
    for node_id in segment:
        node = nodes.get(node_id, {})
        if _get_node_type(node) != "filter":
            continue
        for cond in node.get("data", {}).get("config", {}).get("conditions", []):
            part = inline_calc_cols(cond, calc_col_map)
            if not part:
                continue
            col = cond.get("column")
            db = tech_to_db.get(col, col)
            if col and col in tech_to_db and col != db:
                part = part.replace(f'"{col}"', f'"{db}"')
            part = _rewrite_expression_column_refs(part, tech_to_db)
            where_parts.append(part)
    return where_parts

def compile_source_staging_sql(
    source_id: str,
    branch_terminal_ids: list[str],
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    config: dict[str, Any],
    job_id: str,
) -> CompiledSQL:
    """
    Compile a single source staging table for a shared source: union of base columns
    and all calculated column expressions from all branches. Used when one source
    feeds multiple projections (branch terminals); read base table once into staging.
    """
    all_base = set()
    all_calculated = {}  # name -> {name, expression}; dedupe by name
    for terminal_id in branch_terminal_ids:
        base_names, calc_list = _collect_columns_and_calculated_from_branch(
            terminal_id, source_id, nodes, edges
        )
        all_base.update(base_names)
        for c in calc_list:
            name = c["name"]
            if name not in all_calculated:
                all_calculated[name] = c
    # Base columns that are not calculated column names
    base_only = sorted(all_base - set(all_calculated.keys()))
    # Map base column names to (db_name, technical_name) so staging table columns match _infer_columns
    metadata = config.get("node_output_metadata", {}).get(source_id, {}).get("columns", [])
    name_to_db_tech = {}
    if metadata and isinstance(metadata[0], dict):
        for col in metadata:
            n = col.get("business_name") or col.get("name") or col.get("technical_name")
            if n:
                db = col.get("db_name") or col.get("business_name") or col.get("name")
                tech = col.get("technical_name") or col.get("business_name") or col.get("name")
                name_to_db_tech[n] = (db, tech)
            if col.get("technical_name"):
                db = col.get("db_name") or col.get("business_name") or col.get("name")
                name_to_db_tech[col["technical_name"]] = (db, col["technical_name"])
    # Source: always read by db_name, create staging with technical_name (prefix + name) for consistency
    prefix = source_id[:8]
    select_parts = []
    for c in base_only:
        pair = name_to_db_tech.get(c)
        if pair:
            db_name, tech_name = pair
            if not tech_name or (tech_name == db_name and not str(tech_name).startswith(prefix + "_")):
                tech_name = f"{prefix}_{db_name or c}"
            db_name = db_name or c
            # Always emit db_name AS technical_name so staging never has raw db column names
            select_parts.append(f'"{db_name}" AS "{tech_name}"')
        else:
            # No metadata: assume c is db column name; staging column = prefix_c
            tech_name = f"{prefix}_{c}" if not c.startswith(prefix + "_") else c
            select_parts.append(f'"{c}" AS "{tech_name}"')
    # Map metadata names to db_name for source table column refs in expressions
    tech_or_name_to_db: dict[str, str] = {}
    for col in metadata if metadata and isinstance(metadata[0], dict) else []:
        db = col.get("db_name") or col.get("business_name") or col.get("name")
        tech = col.get("technical_name")
        bn = col.get("business_name") or col.get("name")
        if db:
            tech_or_name_to_db[db] = db
            if tech:
                tech_or_name_to_db[tech] = db
            if bn:
                tech_or_name_to_db[bn] = db
            tech_or_name_to_db[f"{source_id[:8]}_{db}"] = db
    # Resolve calculated column refs: e.g. lower_trial = LOWER(upper_trial) -> LOWER(UPPER(is_trial))
    # Expressions may reference other calculated columns; resolve_formula inlines them to base columns only.
    # First add all raw expressions, then resolve (so dependencies like upper_trial are available when resolving lower_trial)
    calc_col_map: dict[str, str] = {}
    for name, c in sorted(all_calculated.items()):
        expr = c.get("expression", "")
        if expr:
            calc_col_map[name] = expr
    for name in list(calc_col_map.keys()):
        resolved = resolve_formula(calc_col_map[name], calc_col_map)
        resolved_db = _rewrite_expression_column_refs(resolved, tech_or_name_to_db)
        calc_col_map[name] = resolved_db
    for name, c in sorted(all_calculated.items()):
        tech_name = f"{prefix}_{name}"
        resolved_expr = calc_col_map.get(name, c.get("expression", ""))
        if resolved_expr:
            select_parts.append(f'({resolved_expr}) AS "{tech_name}"')
    if not select_parts:
        select_parts = ["*"]
    select_clause = ", ".join(select_parts)

    source_node = nodes.get(source_id, {})
    if not source_node:
        raise SQLCompilationError(f"Source node '{source_id}' not found")
    node_config = source_node.get("data", {}).get("config", {})
    source_config = config.get("source_configs", {}).get(source_id, {})
    table_name = node_config.get("tableName") or source_config.get("table_name")
    schema_name = node_config.get("schema") or source_config.get("schema_name")
    if not table_name:
        raise SQLCompilationError(f"Source node '{source_id}' missing table name")
    qualified_table = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'

    # When source feeds multiple branches:
    # - AND at source would over-restrict (branch A needs X, branch B needs Y — both would wrongly restrict).
    # - Use OR at source: WHERE (branch1_filter) OR (branch2_filter) to reduce data loaded while
    #   each branch still applies its own filter when reading from staging.
    # - Within same branch: conditions are ANDed.
    # - If any branch has no filters, we cannot restrict at source (that branch needs all rows).
    pushdown_plan = config.get("filter_pushdown_plan", {})
    where_clause = None
    if len(branch_terminal_ids) >= 2:
        branch_where_parts_list = [
            _dedupe_where_parts(_get_branch_filter_where_parts(tid, source_id, nodes, edges, config))
            for tid in branch_terminal_ids
        ]
        if all(parts for parts in branch_where_parts_list):
            # All branches have filters: apply OR at source to reduce data
            branch_exprs = ["(" + " AND ".join(parts) + ")" for parts in branch_where_parts_list]
            where_clause = " OR ".join(branch_exprs)
            logger.info(
                "[SQL] Shared source %s: applying OR-combined filters from %s branches",
                source_id[:8], len(branch_where_parts_list),
            )
        # else: at least one branch has no filters → no source filter (load all rows)
    else:
        pushed_filters = pushdown_plan.get(source_id, [])
        if pushed_filters:
            where_parts = []
            for cond in pushed_filters:
                if cond.get("where_clause"):
                    where_parts.append(cond["where_clause"])
                    continue
                column = cond.get("column")
                operator = cond.get("operator", "=")
                value = cond.get("value")
                if not column:
                    continue
                column_db = tech_or_name_to_db.get(column, column)
                if isinstance(value, str):
                    value_str = f"'{value}'"
                elif value is None:
                    value_str = "NULL"
                else:
                    value_str = str(value)
                where_parts.append(f'"{column_db}" {operator} {value_str}')
            if where_parts:
                where_clause = " AND ".join(_dedupe_where_parts(where_parts))

    if where_clause:
        sql = f"SELECT {select_clause} FROM {qualified_table} WHERE {where_clause}"
    else:
        sql = f"SELECT {select_clause} FROM {qualified_table}"
    staging_table = get_staging_table_name(job_id, source_id)
    create_sql = f'CREATE TABLE {_quote_staging_table(staging_table)} AS\n{sql}'
    return CompiledSQL(
        sql=create_sql,
        is_nested=False,
        dependencies=[source_id],
    )

def _compile_source_node(node: dict[str, Any], nodes: dict[str, Any], edges: list[dict[str, Any]], config: dict[str, Any]) -> str:
    """Compile source node into SELECT FROM source_table."""
    node_id = node["id"]
    node_config = node.get("data", {}).get("config", {})

    # Get source configuration
    source_configs = config.get("source_configs", {})
    source_config = source_configs.get(node_id, {})

    table_name = node_config.get("tableName") or source_config.get("table_name")
    schema_name = node_config.get("schema") or source_config.get("schema_name")

    if not table_name:
        raise SQLCompilationError(f"Source node '{node_id}' missing table name")

    qualified_table = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'

    # Source tables: always SELECT by db_name (actual DB columns), create staging with technical_name
    # so downstream and JOIN use consistent names. Never use SELECT * when we can derive column list.
    metadata = config.get("node_output_metadata", {}).get(node_id)
    prefix = node_id[:8]
    select_parts = []
    if metadata and metadata.get("columns"):
        for col in metadata["columns"]:
            db_name = col.get("db_name") or col.get("business_name") or col.get("name")
            tech_name = col.get("technical_name") or (f"{prefix}_{db_name}" if db_name else None)
            if not tech_name:
                tech_name = db_name
            if db_name:
                if db_name == tech_name and not (str(tech_name).startswith(prefix + "_")):
                    tech_name = f"{prefix}_{db_name}"
                select_parts.append(f'"{db_name}" AS "{tech_name}"')
    if not select_parts:
        # Metadata missing or empty: derive from source_config columns if available (e.g. from enrich)
        source_columns = source_config.get("columns") or []
        if source_columns and isinstance(source_columns[0], dict):
            for col in source_columns:
                name = col.get("name") or col.get("db_name")
                if name:
                    tech_name = col.get("technical_name") or f"{prefix}_{name}"
                    select_parts.append(f'"{name}" AS "{tech_name}"')
        elif source_columns:
            for c in source_columns:
                if isinstance(c, str):
                    tech_name = f"{prefix}_{c}" if not c.startswith(prefix + "_") else c
                    select_parts.append(f'"{c}" AS "{tech_name}"')
        if not select_parts:
            logger.warning(
                "[SQL] _compile_source_node: no node_output_metadata for source %s; "
                "ensure validate/execute populate node_output_metadata for all sources to avoid SELECT *",
                node_id[:8],
            )
            sql = f"SELECT * FROM {qualified_table}"
        else:
            select_list = ", ".join(select_parts)
            sql = f"SELECT {select_list} FROM {qualified_table}"
    else:
        select_list = ", ".join(select_parts)
        sql = f"SELECT {select_list} FROM {qualified_table}"

    # Step 2: Apply any pushed-down filters for this source (WHERE must use db_name: source table has db_name columns)
    tech_or_name_to_db = {}
    if metadata and metadata.get("columns"):
        for col in metadata["columns"]:
            db = col.get("db_name") or col.get("business_name") or col.get("name")
            tech = col.get("technical_name")
            bn = col.get("business_name") or col.get("name")
            if db:
                tech_or_name_to_db[db] = db
                if tech:
                    tech_or_name_to_db[tech] = db
                if bn:
                    tech_or_name_to_db[bn] = db

    pushdown_plan = config.get("filter_pushdown_plan", {})
    pushed_filters = pushdown_plan.get(node_id, [])

    if pushed_filters:
        where_parts = []
        for cond in pushed_filters:
            # Use pre-rewritten WHERE clause if available (for calculated columns)
            if cond.get("where_clause"):
                where_parts.append(cond["where_clause"])
                continue

            column = cond.get("column")
            operator = cond.get("operator", "=")
            value = cond.get("value")

            if not column:
                continue

            column_db = tech_or_name_to_db.get(column, column)

            # Format value for SQL
            if isinstance(value, str):
                value_str = f"'{value}'"
            elif value is None:
                value_str = "NULL"
            else:
                value_str = str(value)

            where_parts.append(f'"{column_db}" {operator} {value_str}')

        if where_parts:
            where_clause = " AND ".join(where_parts)
            # Wrap in subquery or append to WHERE?
            # Appending to WHERE is more efficient but requires parsing if qualified_table has a WHERE already
            # Since these are source nodes from our canvas, they don't have WHERE yet.
            sql = f"{sql} WHERE {where_clause}"
            logger.info(f"[SQL] Injected {len(where_parts)} pushed filters into source node {node_id[:8]}")

    return sql

def _apply_transformation(
    node: dict[str, Any],
    upstream_sql: str,
    node_type: str,
    alias: str,
    *,
    config: Optional[dict[str, Any]] = None,
    node_id: Optional[str] = None,
) -> str:
    """Apply transformation to upstream SQL."""
    node_config = node.get("data", {}).get("config", {})

    if node_type == "projection":
        return _apply_projection(upstream_sql, node_config, alias, pipeline_config=config, node_id=node_id)
    elif node_type == "filter":
        meta = {}
        if config and node_id:
            raw_meta = config.get("node_output_metadata", {}).get(node_id, {})
            if raw_meta and raw_meta.get("columns"):
                for c in raw_meta["columns"]:
                    n = c.get("business_name") or c.get("name")
                    t = c.get("technical_name") or n
                    if n and t:
                        meta[n] = t
        return _apply_filter(upstream_sql, node_config, alias, meta_columns=meta)
    elif node_type == "compute":
        return _apply_compute(upstream_sql, node_config, alias)
    else:
        # Pass-through
        return upstream_sql

def _apply_projection(
    upstream_sql: str,
    node_config: dict[str, Any],
    alias: str,
    *,
    pipeline_config: Optional[dict[str, Any]] = None,
    node_id: Optional[str] = None,
) -> str:
    """Apply projection: only necessary base columns + calculated column expressions.
    When pipeline_config/node_id provide node_output_metadata, use technical_name for AS so
    staging table columns match _infer_columns (and JOIN can reference them).
    """
    base_columns = node_config.get("columns", []) or node_config.get("selectedColumns", []) or []
    calculated = node_config.get("calculated_columns", []) or node_config.get("calculatedColumns", [])

    if not base_columns and not calculated:
        return upstream_sql

    # Resolve name -> technical_name for columns when metadata is available
    meta_columns = {}
    if pipeline_config and node_id:
        meta = pipeline_config.get("node_output_metadata", {}).get(node_id, {})
        if meta and meta.get("columns"):
            for c in meta["columns"]:
                n = c.get("business_name") or c.get("name")
                t = c.get("technical_name") or n
                if n and t:
                    meta_columns[n] = t

    # Base column names (only necessary fields from projection); use technical_name if in metadata
    base_names = []
    for col in base_columns:
        if isinstance(col, dict):
            name = col.get("name")
            if name:
                out_name = meta_columns.get(name, name)
                if out_name not in base_names:
                    base_names.append(out_name)
        elif isinstance(col, str):
            out_name = meta_columns.get(col, col)
            if out_name not in base_names:
                base_names.append(out_name)

    # Dedupe by value (we may have appended same technical_name)
    seen_base = set()
    ordered_base_unique = []
    for b in base_names:
        if b not in seen_base:
            seen_base.add(b)
            ordered_base_unique.append(b)
    base_names = ordered_base_unique

    # Calculated column names: we emit these as expressions only; use technical_name when in metadata
    calc_names = set()
    for calc in calculated:
        if isinstance(calc, dict):
            name = calc.get("name") or calc.get("alias")
            if name:
                calc_names.add(meta_columns.get(name, name))

    # Calculated columns may reference base columns; add those refs to required set (by config name)
    required_base = set()
    for col in base_columns:
        if isinstance(col, dict) and col.get("name"):
            required_base.add(col.get("name"))
        elif isinstance(col, str):
            required_base.add(col)
    for calc in calculated:
        expr = calc.get("expression") if isinstance(calc, dict) else None
        if expr:
            required_base.update(_extract_expression_column_refs(expr))
    # Map to technical_name for ordered_base
    required_base_tech = {meta_columns.get(n, n) for n in required_base}
    extra_deps = sorted(required_base_tech - set(base_names))
    ordered_base = [b for b in base_names if b not in calc_names] + [x for x in extra_deps if x not in base_names and x not in calc_names]

    select_parts = [f'"{c}"' for c in ordered_base]

    # Add calculated columns as expressions only; use technical_name when in metadata
    # Rewrite column refs in expression to quoted technical_name so they match upstream/staging columns
    seen_calc = set()
    for calc in calculated:
        if not isinstance(calc, dict):
            continue
        expr = calc.get("expression")
        name = calc.get("name") or calc.get("alias")
        if not name or not expr or name in seen_calc:
            continue
        out_name = meta_columns.get(name, name)
        seen_calc.add(name)
        expr_rewritten = _rewrite_expression_column_refs(expr, meta_columns)
        select_parts.append(f'({expr_rewritten}) AS "{out_name}"')

    if not select_parts:
        return upstream_sql

    ", ".join(select_parts)

    return """SELECT {select_clause}
FROM (
    {upstream_sql}
) proj{alias}"""

def _apply_filter(
    upstream_sql: str,
    config: dict[str, Any],
    alias: str,
    *,
    meta_columns: Optional[dict[str, str]] = None,
) -> str:
    """Apply filter (WHERE clause). Resolve column via meta_columns to technical_name when provided."""
    conditions = config.get("conditions", [])
    if not conditions:
        return upstream_sql
    where_parts = []
    for cond in conditions:
        column = cond.get("column")
        if meta_columns and column:
            column = meta_columns.get(column, column)
        if column and (column.startswith("_L_") or column.startswith("_R_")):
            column = column[3:]
        operator = cond.get("operator", "=")
        value = cond.get("value")
        if not column:
            continue
        if isinstance(value, str):
            value_str = f"'{value}'"
        elif value is None:
            value_str = "NULL"
        else:
            value_str = str(value)
        where_parts.append(f'"{column}" {operator} {value_str}')

    if not where_parts:
        return upstream_sql

    " AND ".join(where_parts)

    return """SELECT *
FROM (
    {upstream_sql}
) filt{alias}
WHERE {where_clause}"""

def _apply_compute(upstream_sql: str, config: dict[str, Any], alias: str) -> str:
    """Apply computed columns."""
    computed_columns = config.get("computedColumns", [])

    if not computed_columns:
        return upstream_sql

    # Build SELECT with computed columns
    select_parts = ["*"]  # Include all existing columns

    for comp_col in computed_columns:
        expression = comp_col.get("expression")
        col_alias = comp_col.get("alias") or comp_col.get("name")

        if expression and col_alias:
            select_parts.append(f'{expression} AS "{col_alias}"')

    ", ".join(select_parts)

    return """SELECT {select_clause}
FROM (
    {upstream_sql}
) comp{alias}"""

def _extract_expression_column_refs(expression: str) -> list[str]:
    """Extract column names referenced in a calculated column expression (quoted refs)."""
    if not expression or not isinstance(expression, str):
        return []
    return re.findall(r'"([^"]+)"', expression)

def extract_source_refs(expression: str) -> set[str]:
    """Find all column/source field names used in an expression (quoted and unquoted identifiers)."""
    if not expression or not isinstance(expression, str):
        return set()
    refs = set(re.findall(r'"([^"]+)"', expression))
    # Unquoted identifiers (word chars, not SQL keywords)
    words = set(re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b", expression))
    skip = {"AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "UPPER", "LOWER", "COALESCE", "CASE", "WHEN", "THEN", "ELSE", "END", "AS", "IN", "IS"}
    for w in words:
        if w.upper() not in skip and not w.isdigit():
            refs.add(w)
    return refs

def resolve_formula(formula: str, calc_col_map: dict[str, str]) -> str:
    """
    Recursively replace calculated column references in formula with their definitions.
    Stops when no more replacements possible (handles circular refs by leaving as-is after one pass per key).
    """
    if not formula or not calc_col_map:
        return formula
    out = formula
    max_iter = len(calc_col_map) + 1
    for _ in range(max_iter):
        changed = False
        for name, expr in calc_col_map.items():
            # Replace quoted ref to this calc col with (expr)
            escaped = re.escape(name)
            pattern = r'"' + escaped + r'"'
            replacement = f"({expr})"
            if re.search(pattern, out):
                out = re.sub(pattern, replacement, out)
                changed = True
            pattern_word = r"\b" + escaped + r"\b"
            if re.search(pattern_word, out):
                out = re.sub(pattern_word, replacement, out)
                changed = True
        if not changed:
            break
    return out

def inline_calc_cols(condition: dict[str, Any], calc_col_map: dict[str, str]) -> Optional[str]:
    """
    Build one SQL condition from a filter condition dict (column, operator, value).
    Replace column with its formula from calc_col_map if it is a calculated column.
    Returns e.g. '("col" = 1)' or '((UPPER("status")) = \'ACTIVE\')'.
    """
    column = condition.get("column")
    operator = condition.get("operator", "=")
    value = condition.get("value")
    if not column:
        return None
    col_expr = f'"{column}"'
    if column in calc_col_map:
        col_expr = f"({resolve_formula(calc_col_map[column], calc_col_map)})"
    if isinstance(value, str):
        value_str = f"'{value}'"
    elif value is None:
        value_str = "NULL"
    else:
        value_str = str(value)
    return f"{col_expr} {operator} {value_str}"

def build_flat_select(
    source: str,
    select_expressions: dict[str, str],
    where_clauses: list[str],
) -> str:
    """
    Emit one flat SELECT with no subqueries.
    source: quoted table or staging e.g. '"public"."t"' or '"staging_jobs"."job_xxx_node_yyy"'
    select_expressions: output_name -> SQL expression (e.g. '"a"' or '(UPPER("b"))')
    where_clauses: list of SQL predicate strings
    """
    select_parts = [f'{expr} AS "{name}"' if expr != f'"{name}"' else f'"{name}"' for name, expr in select_expressions.items()]
    select_clause = ", ".join(select_parts)
    sql = f"SELECT {select_clause} FROM {source}"
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
    return sql

def flatten_segment_from_source(
    segment_node_ids: list[str],
    source_id: str,
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    config: dict[str, Any],
    from_table_override: Optional[str] = None,
) -> str:
    """
    When a linear segment starts at a SOURCE node: produce ONE flat SELECT FROM source table
    (or from staging when source is materialized) with db_name -> technical_name mapping,
    projection pushdown, and filters pushed to WHERE.
    NEVER use SELECT * from source.
    When from_table_override is set (shared/multi_branch source staging), read from that
    table and use staging column names (prefix_db) in expressions.
    """
    source_node = nodes.get(source_id, {})
    node_config = source_node.get("data", {}).get("config", {})
    source_config = config.get("source_configs", {}).get(source_id, {})
    metadata = config.get("node_output_metadata", {}).get(source_id, {}).get("columns", [])
    if from_table_override:
        qualified_table = from_table_override
    else:
        table_name = node_config.get("tableName") or source_config.get("table_name")
        schema_name = node_config.get("schema") or source_config.get("schema_name") or "public"
        if not table_name:
            raise SQLCompilationError(f"Source node '{source_id}' missing table name")
        qualified_table = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'

    tech_to_db: dict[str, str] = {}
    db_to_tech: dict[str, str] = {}
    if metadata and isinstance(metadata[0], dict):
        for col in metadata:
            db = col.get("db_name") or col.get("business_name") or col.get("name")
            tech = col.get("technical_name") or col.get("business_name") or col.get("name")
            bn = col.get("business_name") or col.get("name")
            # When db_name is missing but technical_name has node prefix (31f72c84_cmp_id),
            # derive db_name from suffix so we SELECT actual source columns
            if tech and (not db or db == tech) and re.match(r"^[a-f0-9]{8}_(.+)$", tech, re.I):
                derived_db = re.match(r"^[a-f0-9]{8}_(.+)$", tech, re.I).group(1)
                if not db or db == tech:
                    db = derived_db
            if db:
                if from_table_override:
                    # Staging columns use prefix_db; map all refs to staging col name
                    staging_col = tech if tech else f"{source_id[:8]}_{db}"
                    tech_to_db[tech] = staging_col
                    tech_to_db[db] = staging_col
                    db_to_tech[staging_col] = tech or db
                    if bn:
                        tech_to_db[bn] = staging_col
                else:
                    tech_to_db[tech] = db
                    db_to_tech[db] = tech or db
                    if bn:
                        tech_to_db[bn] = db

    prefix = source_id[:8]

    def _resolve_db_for_source(tech_or_col: str) -> str:
        """Resolve column to db_name for source table. Use tech_to_db or derive from prefix."""
        db = tech_to_db.get(tech_or_col, tech_or_col)
        if db == tech_or_col and re.match(r"^[a-f0-9]{8}_(.+)$", tech_or_col, re.I):
            return re.match(r"^[a-f0-9]{8}_(.+)$", tech_or_col, re.I).group(1)
        return db

    calc_col_map: dict[str, str] = {}
    for node_id in segment_node_ids:
        node = nodes.get(node_id, {})
        ntype = _get_node_type(node)
        nc = node.get("data", {}).get("config", {})
        if ntype == "projection":
            for calc in nc.get("calculated_columns", []) or nc.get("calculatedColumns", []):
                if not isinstance(calc, dict):
                    continue
                name = calc.get("name") or calc.get("alias")
                expr = calc.get("expression")
                if name and expr:
                    resolved = resolve_formula(expr, calc_col_map)
                    resolved_db = _rewrite_expression_column_refs(resolved, tech_to_db)
                    calc_col_map[name] = resolved_db
                    # Also key by node_id_name so required_tech (e.g. "0377bcbd_upper_trial") matches
                    calc_col_map[f"{node_id[:8]}_{name}"] = resolved_db
            for comp in nc.get("computedColumns", []):
                if isinstance(comp, dict) and comp.get("alias") and comp.get("expression"):
                    resolved = resolve_formula(comp["expression"], calc_col_map)
                    alias = comp["alias"]
                    calc_col_map[alias] = _rewrite_expression_column_refs(resolved, tech_to_db)
                    calc_col_map[f"{node_id[:8]}_{alias}"] = calc_col_map[alias]
        if ntype == "compute":
            for comp in nc.get("computedColumns", []):
                if isinstance(comp, dict) and comp.get("alias") and comp.get("expression"):
                    resolved = resolve_formula(comp["expression"], calc_col_map)
                    alias = comp["alias"]
                    calc_col_map[alias] = _rewrite_expression_column_refs(resolved, tech_to_db)
                    calc_col_map[f"{node_id[:8]}_{alias}"] = calc_col_map[alias]

    # Fallback: populate calc_col_map from node_output_metadata when config lacks calculated_columns
    # (e.g. when metadata is cached but node config structure differs)
    for node_id in segment_node_ids:
        node_meta = config.get("node_output_metadata", {}).get(node_id, {}).get("columns", [])
        if not node_meta or not isinstance(node_meta[0], dict):
            continue
        for col in node_meta:
            if col.get("source") != "calculated" and not col.get("isCalculated"):
                continue  # only process calculated columns
            tech_name = col.get("technical_name") or col.get("business_name") or col.get("name")
            expr = col.get("expression")
            bn = col.get("business_name") or col.get("name")
            if tech_name and expr and tech_name not in calc_col_map:
                resolved = resolve_formula(expr, calc_col_map)
                resolved_db = _rewrite_expression_column_refs(resolved, tech_to_db)
                calc_col_map[tech_name] = resolved_db
                if bn and bn != tech_name:
                    calc_col_map[bn] = resolved_db

    final_id = segment_node_ids[-1] if segment_node_ids else source_id
    final_node = nodes.get(final_id, {})
    final_config = final_node.get("data", {}).get("config", {})

    # Staging before destination keeps technical names; destination INSERT maps staging technical -> dest business.
    # When it feeds a JOIN, keep technical names so the JOIN can read l."39ef59b7_*" and r."4fa62c23_*".
    any(
        _get_node_type(nodes.get(e.get("target"), {})) in ("destination", "destination-postgresql", "destination-postgres")
        for e in edges if isinstance(e, dict) and e.get("source") == final_id
    )
    feeds_join = any(
        _get_node_type(nodes.get(e.get("target"), {})) in ("join", "aggregation", "aggregate")
        for e in edges if isinstance(e, dict) and e.get("source") == final_id
    )
    required_tech: set[str] = set()
    select_parts: list[str] = []
    for col in final_config.get("columns", []) or final_config.get("selectedColumns", []):
        name = col.get("name") if isinstance(col, dict) else col
        if name:
            required_tech.add(name)
    for calc in final_config.get("calculated_columns", []) or final_config.get("calculatedColumns", []):
        if isinstance(calc, dict) and (calc.get("name") or calc.get("alias")):
            required_tech.add(calc.get("name") or calc.get("alias"))
    if not required_tech:
        cols = _infer_columns(final_id, nodes, edges, config)
        required_tech = set(cols) if cols else set()

    # Always include calculated columns from projections in the segment. Cached metadata for the
    # filter (final_id) may be stale and omit them, causing join to fail with "column r.X does not exist".
    for key in calc_col_map:
        if key and key not in required_tech:
            required_tech.add(key)

    # Populate tech_to_db for required_tech when metadata lacks mapping (so WHERE uses db_name)
    for tech in required_tech:
        if tech not in tech_to_db and re.match(r"^[a-f0-9]{8}_(.+)$", tech, re.I):
            tech_to_db[tech] = re.match(r"^[a-f0-9]{8}_(.+)$", tech, re.I).group(1)

    # Add filter-condition columns to required_tech so they are selected (Fix for NULL in dest).
    # When a filter uses WHERE col = value, we must also SELECT col; otherwise staging lacks it and dest gets NULL.
    for node_id in segment_node_ids:
        node = nodes.get(node_id, {})
        if _get_node_type(node) != "filter":
            continue
        for cond in node.get("data", {}).get("config", {}).get("conditions", []):
            col = cond.get("column")
            if not col:
                continue
            # Resolve to tech name: col may be db_name, technical_name, or display name
            if col in tech_to_db or col in db_to_tech:
                tech_name = db_to_tech.get(col, col)
                required_tech.add(tech_name)

    def _find_calc_expr(t: str):
        """Resolve calculated column expression: exact match, or by technical_name suffix (e.g. 0377bcbd_upper_trial -> upper_trial)."""
        if t in calc_col_map:
            return calc_col_map[t]
        for k, expr in calc_col_map.items():
            if t.endswith("_" + k) or t == k:
                return expr
        return None

    def _output_col_name(tech: str, db: str, tech_name: str, from_staging: bool = False) -> str:
        """Output column name: always technical in staging. Destination INSERT maps staging technical -> dest business."""
        if feeds_join:
            # JOIN expects l."39ef59b7_*" and r."4fa62c23_*" - keep technical names
            return db if (from_staging and db.startswith(prefix + "_")) else tech_name
        # Staging always uses technical names; destination INSERT maps via metadata
        return tech_name

    seen_out_names: set[str] = set()
    for tech in sorted(required_tech):
        db = _resolve_db_for_source(tech)
        tech_name = tech if tech.startswith(prefix + "_") else f"{prefix}_{db}"
        calc_expr = _find_calc_expr(tech)
        if calc_expr:
            out_name = _output_col_name(tech, db, tech_name, from_staging=False)
            if out_name in seen_out_names:
                continue
            seen_out_names.add(out_name)
            select_parts.append(f'({calc_expr}) AS "{out_name}"')
        elif from_table_override:
            # Reading from shared staging: column already has the correct prefixed name.
            # When feeding JOIN, keep technical name; when feeding destination, alias to business.
            out_name = _output_col_name(tech, db, tech_name, from_staging=True)
            if out_name in seen_out_names:
                continue
            seen_out_names.add(out_name)
            if db == out_name:
                select_parts.append(f'"{db}"')
            else:
                select_parts.append(f'"{db}" AS "{out_name}"')
        else:
            if tech not in tech_to_db and tech not in db_to_tech:
                logger.warning(
                    "[SQL] flatten_segment_from_source: column '%s' not in source and not a calculated column; "
                    "may cause runtime error. calc_col_map keys: %s",
                    tech, list(calc_col_map.keys())[:10],
                )
            out_name = _output_col_name(tech, db, tech_name, from_staging=False)
            if out_name in seen_out_names:
                continue
            seen_out_names.add(out_name)
            select_parts.append(f'"{db}" AS "{out_name}"')

    where_parts: list[str] = []

    # When source feeds multiple branches (MULTI_BRANCH_FEED or SHARED_SOURCE), do NOT apply
    # filters at source: each branch has its own filter conditions. Applying one branch's filter
    # would over-restrict data for other branches.
    source_children = [e["target"] for e in edges if isinstance(e, dict) and e.get("source") == source_id]
    source_feeds_multiple_branches = len(set(source_children)) >= 2

    # When reading from staging (from_table_override), the staging was already created. Do NOT
    # add filter_pushdown_plan here: (a) each branch must apply only its own filters (in-segment),
    # else we'd over-restrict with another branch's conditions; (b) if staging was created with
    # filters (single-branch source), re-applying would be redundant.
    apply_pushdown = not source_feeds_multiple_branches and not from_table_override

    # 1) Cross-segment pushed conditions from filter_pushdown_plan targeting this source.
    #    Only when reading directly from source table (not from staging) and single branch.
    if apply_pushdown:
        for cond in config.get("filter_pushdown_plan", {}).get(source_id, []):
            if cond.get("where_clause"):
                wc = cond["where_clause"]
                wc = _rewrite_expression_column_refs(wc, tech_to_db)
                where_parts.append(wc)
            elif cond.get("column"):
                col = cond["column"]
                op = cond.get("operator", "=")
                val = cond.get("value")
                db = _resolve_db_for_source(col)
                if isinstance(val, str):
                    val_s = f"'{val}'"
                elif val is None:
                    val_s = "NULL"
                else:
                    val_s = str(val)
                where_parts.append(f'"{db}" {op} {val_s}')

    # 2) Conditions from filter nodes physically in the segment.
    for node_id in segment_node_ids:
        node = nodes.get(node_id, {})
        if _get_node_type(node) != "filter":
            continue
        for cond in node.get("data", {}).get("config", {}).get("conditions", []):
            part = inline_calc_cols(cond, calc_col_map)
            if not part:
                continue
            col = cond.get("column")
            db = _resolve_db_for_source(col)
            if col and col != db:
                part = part.replace(f'"{col}"', f'"{db}"')
            part = _rewrite_expression_column_refs(part, tech_to_db)
            where_parts.append(part)

    if not select_parts:
        if tech_to_db:
            if from_table_override:
                select_parts = [f'"{c}"' for c in sorted(db_to_tech.keys())]
            else:
                select_parts = [f'"{db}" AS "{prefix}_{db}"' for db in sorted(db_to_tech.keys())]
        else:
            # Fallback only when metadata missing; prefer to avoid SELECT * from source
            return f"SELECT * FROM {qualified_table}"
    select_clause = ", ".join(select_parts)
    sql = f"SELECT {select_clause} FROM {qualified_table}"
    if where_parts:
        sql += " WHERE " + " AND ".join(_dedupe_where_parts(where_parts))
    return sql

def _build_filter_col_to_upstream(upstream_columns: list[str]) -> dict[str, str]:
    """
    Build mapping from filter column names to actual upstream staging column names.
    Join output may use source-prefixed columns (e.g. 39ef59b7_cmp_id) while filter
    config uses _L_/_R_ (e.g. _L_cmp_id). Map _L_X -> first upstream col ending with _X,
    _R_X -> second (join order: left cols first, then right).
    """
    out = {}
    for c in upstream_columns:
        if c.startswith("_L_") or c.startswith("_R_"):
            base = c[3:]
            if base and base not in out:
                out[base] = c
        out[c] = c

    # Fallback: map _L_X / _R_X to actual join output columns (e.g. 39ef59b7_cmp_id)
    # when upstream has source-prefixed names but filter uses _L_/_R_
    for c in upstream_columns:
        if "_" in c and len(c) > 10:
            # Pattern: 8hex_colname (e.g. 39ef59b7_cmp_id)
            parts = c.split("_", 1)
            if len(parts) == 2 and len(parts[0]) == 8 and all(h in "0123456789abcde" for h in parts[0].lower()):
                base = parts[1]
                if base:
                    # Find position of this column among all that end with _base
                    suffix = "_" + base
                    suffix_lower = suffix.lower()
                    matches = [u for u in upstream_columns if u.endswith(suffix) or u.lower().endswith(suffix_lower)]
                    if matches:
                        idx = matches.index(c)
                        if idx == 0 and f"_L_{base}" not in out:
                            out[f"_L_{base}"] = c
                        elif idx == 1 and f"_R_{base}" not in out:
                            out[f"_R_{base}"] = c
    return out

def _find_schema_anchor(
    start_node_id: str,
    reverse_adjacency: dict[str, list[str]],
    node_map: dict[str, Any],
) -> str:
    """Trace back to find schema anchor (join, aggregation, or source) for full column set."""
    visited = set()
    queue = [start_node_id]
    while queue:
        nid = queue.pop(0)
        if nid in visited:
            continue
        visited.add(nid)
        node = node_map.get(nid, {})
        ntype = _get_node_type(node)
        if ntype in ("join", "aggregation", "aggregate"):
            return nid
        if ntype == "source":
            return nid
        for pid in reverse_adjacency.get(nid, []):
            queue.append(pid)
    return start_node_id

def flatten_segment(
    segment_node_ids: list[str],
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    config: dict[str, Any],
    upstream_source_or_staging: str,
    *,
    name_to_technical: Optional[dict[str, str]] = None,
    required_destination_columns: Optional[set[str]] = None,
) -> str:
    """
    Given a linear chain of nodes (from upstream to downstream) and a starting point
    (source table or staging table), produce ONE flat SELECT with no subqueries.
    Applies projection pushdown (only needed fields) and filter pushdown (WHERE).
    """
    if not segment_node_ids:
        return f"SELECT * FROM {upstream_source_or_staging}"
    name_to_technical = name_to_technical or {}
    calc_col_map: dict[str, str] = {}
    _build_reverse_adjacency(edges)

    # Step 1: Build calc col dependency map walking the segment
    for node_id in segment_node_ids:
        node = nodes.get(node_id, {})
        node_type = _get_node_type(node)
        node_config = node.get("data", {}).get("config", {})
        if node_type == "projection":
            for calc in node_config.get("calculated_columns", []) or node_config.get("calculatedColumns", []):
                if not isinstance(calc, dict):
                    continue
                name = calc.get("name") or calc.get("alias")
                expr = calc.get("expression")
                if name and expr:
                    out_name = name_to_technical.get(name, name)
                    resolved = resolve_formula(expr, calc_col_map)
                    calc_col_map[out_name] = _rewrite_expression_column_refs(resolved, name_to_technical)
            for comp in node_config.get("computedColumns", []):
                if isinstance(comp, dict):
                    alias = comp.get("alias") or comp.get("name")
                    expr = comp.get("expression")
                    if alias and expr:
                        out_name = name_to_technical.get(alias, alias)
                        calc_col_map[out_name] = _rewrite_expression_column_refs(resolve_formula(expr, calc_col_map), name_to_technical)
        if node_type == "compute":
            for comp in node_config.get("computedColumns", []):
                if isinstance(comp, dict):
                    alias = comp.get("alias") or comp.get("name")
                    expr = comp.get("expression")
                    if alias and expr:
                        out_name = name_to_technical.get(alias, alias)
                        calc_col_map[out_name] = _rewrite_expression_column_refs(resolve_formula(expr, calc_col_map), name_to_technical)

    # Step 2: From final node determine required fields and select expressions
    final_id = segment_node_ids[-1]
    final_node = nodes.get(final_id, {})
    _get_node_type(final_node)
    final_node.get("data", {}).get("config", {})
    required_fields: set[str] = set()
    select_expressions: dict[str, str] = {}

    def add_output_fields_from_node(nid: str):
        node = nodes.get(nid, {})
        _get_node_type(node)
        nc = node.get("data", {}).get("config", {})
        base_cols = nc.get("columns", []) or nc.get("selectedColumns", []) or []
        calcs = nc.get("calculated_columns", []) or nc.get("calculatedColumns", []) or []
        for col in base_cols:
            name = col.get("name") if isinstance(col, dict) else col
            if not name:
                continue
            out_name = name_to_technical.get(name, name)
            if out_name in calc_col_map:
                select_expressions[out_name] = calc_col_map[out_name]
                required_fields.update(extract_source_refs(calc_col_map[out_name]))
            else:
                select_expressions[out_name] = f'"{name}"'
                required_fields.add(name)
        for calc in calcs:
            if isinstance(calc, dict):
                name = calc.get("name") or calc.get("alias")
                if name:
                    out_name = name_to_technical.get(name, name)
                    select_expressions[out_name] = calc_col_map.get(out_name, f'"{name}"')
                    if out_name in calc_col_map:
                        required_fields.update(extract_source_refs(calc_col_map[out_name]))

    add_output_fields_from_node(final_id)
    # Only source columns (not calc cols) are required from upstream
    required_fields -= set(calc_col_map.keys())
    if not select_expressions:
        cols = _infer_columns(final_id, nodes, edges, config)
        if cols:
            for c in cols:
                select_expressions[c] = f'"{c}"'
                required_fields.add(c)
        else:
            return f"SELECT * FROM {upstream_source_or_staging}"

    # Step 3: Push filters down (use actual staging column names).
    # Never skip filters here: flatten_segment handles post-staging segments (e.g. after a join).
    # If a filter was also pushed to a source via filter_pushdown_plan, applying it again here is
    # redundant but safe (the source-level filter already reduced rows before the join).
    where_clauses: list[str] = []
    filter_condition_cols: set[str] = set()
    for node_id in segment_node_ids:
        node = nodes.get(node_id, {})
        if _get_node_type(node) != "filter":
            continue
        conditions = node.get("data", {}).get("config", {}).get("conditions", [])
        for cond in conditions:
            col = cond.get("column")
            if not col:
                continue
            resolved_col = name_to_technical.get(col, col)
            filter_condition_cols.add(resolved_col)
            part = inline_calc_cols({**cond, "column": resolved_col}, calc_col_map)
            if part:
                where_clauses.append(part)

    # Step 3b: Include filter-condition columns in SELECT so destination receives them (Fix for NULL in dest).
    # When a filter uses WHERE col = value, we must also SELECT col; otherwise staging lacks it and dest gets NULL.
    upstream_id = segment_node_ids[0] if segment_node_ids else None
    upstream_cols = _infer_columns(upstream_id, nodes, edges, config) if upstream_id else []
    upstream_cols_set = set(upstream_cols) if upstream_cols else set()
    for col in filter_condition_cols:
        if col not in select_expressions and col in upstream_cols_set:
            select_expressions[col] = f'"{col}"'

    # Step 3c: Include destination-required columns (e.g. _R_cmp_id) when segment feeds a destination.
    # Projection may exclude them; staging must have them for final INSERT.
    if required_destination_columns:
        for col in required_destination_columns:
            if col not in select_expressions and col in upstream_cols_set:
                select_expressions[col] = f'"{col}"'

    # Step 4: Restrict select to required fields only (projection pushdown)
    # If upstream is a base table we already have only required in select; if staging we select only needed columns
    final_select = {}
    for name, expr in select_expressions.items():
        final_select[name] = expr
    required_ordered = list(select_expressions.keys())

    select_parts = []
    for name in required_ordered:
        expr = final_select[name]
        if expr == f'"{name}"':
            select_parts.append(f'"{name}"')
        else:
            select_parts.append(f'{expr} AS "{name}"')
    select_clause = ", ".join(select_parts)
    sql = f"SELECT {select_clause} FROM {upstream_source_or_staging}"
    if where_clauses:
        sql += " WHERE " + " AND ".join(_dedupe_where_parts(where_clauses))
    return sql

def _ensure_upper_lower_text_safe(expression: str) -> str:
    """
    Cast UPPER/LOWER arguments to text so boolean (and other non-text) columns work.
    PostgreSQL's UPPER/LOWER require text; boolean causes 'function upper(boolean) does not exist'.
    Replaces UPPER(col) and LOWER(col) with UPPER((col)::text) and LOWER((col)::text).
    """
    if not expression:
        return expression
    out = expression
    # Match UPPER/LOWER with simple quoted or unquoted identifier; avoid double-wrapping (::text)
    for _ in range(10):  # limit iterations for nested
        prev = out
        # Quoted: UPPER("col") -> UPPER(("col")::text)
        out = re.sub(
            r'\b(UPPER|LOWER)\s*\(\s*"([^"]+)"\s*\)(?!\s*::)',
            r'\1(("\2")::text)',
            out,
            flags=re.IGNORECASE,
        )
        # Unquoted: UPPER(col) -> UPPER(("col")::text)
        out = re.sub(
            r'\b(UPPER|LOWER)\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)(?!\s*::)',
            r'\1(("\2")::text)',
            out,
            flags=re.IGNORECASE,
        )
        if out == prev:
            break
    return out

def _rewrite_expression_column_refs(expression: str, name_to_technical_name: dict[str, str]) -> str:
    """
    Rewrite column references in a calculated column expression to use quoted technical_name.
    Handles both quoted ("message") and unquoted (message) refs so the expression matches
    the upstream/staging column names (technical_name).
    Also casts UPPER/LOWER args to text so boolean columns work in PostgreSQL.
    """
    if not expression:
        return expression
    out = expression
    if name_to_technical_name:
        # Replace longer names first to avoid partial matches (e.g. message_id before message)
        for name in sorted(name_to_technical_name.keys(), key=len, reverse=True):
            tech = name_to_technical_name[name]
            if name == tech:
                continue
            escaped = re.escape(name)
            # Quoted: "name" -> "tech"
            out = re.sub(r'"' + escaped + r'"', f'"{tech}"', out)
            # Unquoted (word boundary): message -> "tech"
            out = re.sub(r'\b' + escaped + r'\b', f'"{tech}"', out)
    out = _ensure_upper_lower_text_safe(out)
    return out

def _get_node_type(node: dict[str, Any]) -> str:
    """Extract node type."""
    return (node.get("type") or node.get("data", {}).get("type") or "").lower().strip()

def _build_reverse_adjacency(edges: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Build reverse adjacency list."""
    reverse = {}
    for edge in edges:
        source = edge["source"]
        target = edge["target"]
        if target not in reverse:
            reverse[target] = []
        reverse[target].append(source)
    return reverse
