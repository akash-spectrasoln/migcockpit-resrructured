"""
Execution Plan Builder
Builds deterministic execution plan with topological sort and parallel levels.
Anchor nodes (JOIN, Aggregation, Compute multi-branch, Destination) define materialization
boundaries; linear segments between anchors are compiled as flat SQL where possible.
Includes persistence logic for saving/retrieving plans from customer DB.
"""

from dataclasses import asdict, dataclass
import hashlib
import json
import logging
import re
from typing import Any, Optional

import psycopg2

from .sql_compiler import (
    CompiledSQL,
    _quote_staging_table,
    compile_aggregation_sql,
    compile_join_sql,
    compile_nested_sql,
    compile_source_staging_sql,
    compile_staging_table_sql,
)
from .staging_naming import STAGING_SCHEMA

logger = logging.getLogger(__name__)

@dataclass
class ExecutionLevel:
    """Represents a level of parallel execution."""
    level_num: int
    queries: list[CompiledSQL]
    node_ids: list[str]

@dataclass
class ExecutionPlan:
    """Complete execution plan for a pipeline."""
    job_id: str
    staging_schema: str
    levels: list[ExecutionLevel]
    destination_create_sql: Optional[str]
    final_insert_sql: Optional[str]
    destination_creates: list[str]  # One CREATE per destination; use when multiple destinations
    final_inserts: list[str]  # One INSERT per destination; use when multiple destinations
    cleanup_sql: str
    total_queries: int

def build_execution_plan(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    materialization_points: dict[str, Any],
    config: dict[str, Any],
    job_id: str,
    shared_source_terminals: Optional[dict[str, list[str]]] = None,
) -> ExecutionPlan:
    """
    Build complete execution plan.

    Steps:
    1. Create staging schema
    2. Topological sort into execution levels
    3. Compile SQL for each level (including shared source staging when applicable)
    4. Generate final INSERT
    5. Generate cleanup SQL

    Args:
        nodes: List of node dicts
        edges: List of edge dicts
        materialization_points: Nodes to materialize
        config: Configuration
        job_id: Job ID
        shared_source_terminals: Optional map source_id -> [terminal_id, ...] for shared sources

    Returns:
        ExecutionPlan with ordered levels and queries
    """
    if shared_source_terminals is None:
        shared_source_terminals = {}
    from .materialization import MaterializationReason
    # Remove any shared_source materialization where only 1 unique terminal exists
    for source_id in list(materialization_points.keys()):
        mp = materialization_points[source_id]
        if getattr(mp, "reason", None) == MaterializationReason.SHARED_SOURCE:
            terminals = shared_source_terminals.get(source_id, [])
            if len(set(terminals)) < 2:
                del materialization_points[source_id]
                shared_source_terminals.pop(source_id, None)
                logger.info(
                    f"[PLAN] Removed false shared source staging for {source_id[:8]}: "
                    f"only {len(terminals)} unique terminal(s)"
                )
    logger.info(
        "[PLAN DEBUG] After cleanup: materialization_points=%s, shared_source_terminals=%s",
        {nid[:8]: getattr(mp, "reason", None) and getattr(mp.reason, "value", str(mp.reason)) for nid, mp in materialization_points.items()},
        {k[:8] if k else k: [t[:8] for t in v] for k, v in shared_source_terminals.items()},
    )
    node_map = {node["id"]: node for node in nodes}
    staging_schema = STAGING_SCHEMA

    # Anchor nodes (JOIN, Aggregation, Compute multi-branch, Destination) define split points;
    # materialization_points already reflects only boundaries (Rule 1 & 2).
    from .materialization import detect_anchor_nodes
    _anchors = detect_anchor_nodes(nodes, edges)
    if _anchors:
        logger.debug("[PLAN] Anchor nodes: %s", [a.node_id for a in _anchors])

    # Build execution levels via topological sort (sources first, then dependencies)
    execution_levels = _build_execution_levels(nodes, edges)
    logger.info(
        "[PLAN DEBUG] execution_levels (level_num implied by index): %s",
        [[nid[:8] for nid in level] for level in execution_levels],
    )
    # Compile SQL for each level. We keep a separate output_level_num so that
    # levels that have no queries (all nodes skipped) do NOT create gaps in
    # the final level numbering (0,1,2,3,...).
    levels = []
    total_queries = 0
    output_level_num = 0

    for level_num, level_nodes in enumerate(execution_levels):
        queries = []
        query_node_ids = []  # one node_id per query, same order as queries

        for node_id in level_nodes:
            node = node_map[node_id]
            node_type = _get_node_type(node)

            # Skip destination nodes (handled separately)
            if node_type in ("destination", "destination-postgresql", "destination-postgres"):
                logger.info("[PLAN DEBUG]   level %s: skip node %s (destination)", level_num, node_id[:8])
                continue

            # Shared source: one source feeds multiple branches; emit source staging first
            if node_type == "source" and node_id in materialization_points and shared_source_terminals.get(node_id):
                logger.info("[PLAN DEBUG]   level %s: node %s → shared source staging", level_num, node_id[:8])
                compiled = compile_source_staging_sql(
                    node_id,
                    shared_source_terminals[node_id],
                    node_map,
                    edges,
                    config,
                    job_id,
                )
                queries.append(compiled)
                query_node_ids.append(node_id)

            elif node_type == "join":
                logger.info("[PLAN DEBUG]   level %s: node %s → compile_join_sql", level_num, node_id[:8])
                compiled = compile_join_sql(
                    node_id, node_map, edges, materialization_points, config, job_id
                )
                queries.append(compiled)
                query_node_ids.append(node_id)

            elif node_type == "aggregation":
                logger.info("[PLAN DEBUG]   level %s: node %s → compile_aggregation_sql", level_num, node_id[:8])
                compiled = compile_aggregation_sql(
                    node_id, node_map, edges, materialization_points, config, job_id
                )
                queries.append(compiled)
                query_node_ids.append(node_id)

            elif node_id in materialization_points:
                logger.info(
                    "[PLAN DEBUG]   level %s: node %s (type=%s) → compile_staging_table_sql (in materialization_points)",
                    level_num, node_id[:8], node_type,
                )
                # Branch end or final node - create staging table (reads from source staging if shared)
                compiled = compile_staging_table_sql(
                    node_id, node_map, edges, materialization_points, config, job_id
                )
                queries.append(compiled)
                query_node_ids.append(node_id)
            else:
                logger.info(
                    "[PLAN DEBUG]   level %s: node %s (type=%s) → SKIP (not dest, not join, not aggregation, not in materialization_points)",
                    level_num, node_id[:8], node_type,
                )

        if queries:
            levels.append(ExecutionLevel(
                level_num=output_level_num,
                queries=queries,
                node_ids=query_node_ids
            ))
            output_level_num += 1
            total_queries += len(queries)

    # Generate creation SQL and INSERT for each destination (supports multiple destinations)
    destination_creates = _generate_all_destination_creates(
        nodes, edges, node_map, config,
        materialization_points=materialization_points,
        levels=levels,
        job_id=job_id,
    )
    final_inserts = _generate_all_final_inserts(
        nodes, edges, node_map, materialization_points, config, job_id,
        levels=levels,
    )
    destination_create_sql = "\n".join(destination_creates) if destination_creates else None
    final_insert_sql = "\n".join(final_inserts) if final_inserts else None

    # Generate cleanup SQL: drop only this job's staging tables (schema is shared)
    # Each DROP must end with semicolon so PostgreSQL can execute multiple statements
    staging_tables = [mp.staging_table for mp in materialization_points.values()]
    cleanup_sql = "\n".join(
        f"DROP TABLE IF EXISTS {_quote_staging_table(t)} CASCADE;" for t in staging_tables
    ) if staging_tables else ""

    return ExecutionPlan(
        job_id=job_id,
        staging_schema=staging_schema,
        levels=levels,
        destination_create_sql=destination_create_sql,
        final_insert_sql=final_insert_sql,
        destination_creates=destination_creates,
        final_inserts=final_inserts,
        cleanup_sql=cleanup_sql,
        total_queries=total_queries
    )

# =============================================================================
# PLAN HASH COMPUTATION
# =============================================================================

def compute_plan_hash(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    materialization_points: Optional[dict[str, Any]] = None,
    config: Optional[dict[str, Any]] = None,
) -> str:
    """
    Compute a deterministic hash of the pipeline structure and config.

    This hash uniquely identifies a pipeline configuration based on:
    - Node IDs, types, and configuration
    - Edge structure (connections)
    - Materialization points (if provided)
    - source_configs and node_output_metadata (so connection/metadata changes invalidate cache)

    Args:
        nodes: List of node dictionaries
        edges: List of edge dictionaries
        materialization_points: Optional dict of materialization points
        config: Optional config dict; when provided, source_configs and node_output_metadata are included in hash

    Returns:
        SHA-256 hash (hex string) of the pipeline structure
    """
    plan_structure = {
        "nodes": sorted([
            {
                "id": n["id"],
                "type": n.get("type") or n.get("data", {}).get("type"),
                "config_hash": hashlib.md5(
                    json.dumps(n.get("data", {}).get("config", {}), sort_keys=True).encode()
                ).hexdigest()
            }
            for n in nodes
        ], key=lambda x: x["id"]),
        "edges": sorted([
            {"source": e["source"], "target": e["target"]}
            for e in edges
        ], key=lambda x: (x["source"], x["target"])),
        "materialization_points": sorted(materialization_points.keys()) if materialization_points else []
    }
    # Only source_configs in hash so Validate and Execute match (Execute does not send node_output_metadata).
    if config:
        plan_structure["source_configs_hash"] = hashlib.md5(
            json.dumps(config.get("source_configs", {}), sort_keys=True).encode()
        ).hexdigest()
    normalized_plan = json.dumps(plan_structure, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(normalized_plan.encode('utf-8')).hexdigest()

# =============================================================================
# PERSISTENCE LOGIC
# =============================================================================

def save_execution_plan_to_db(connection_config: dict[str, Any], canvas_id: str, plan_hash: str, plan_obj: ExecutionPlan) -> bool:
    """Saves the execution plan to the customer database."""
    conn = None
    try:
        # Validate inputs
        if not connection_config:
            logger.error("[PLAN PERSIST] No connection_config provided")
            return False

        if not canvas_id:
            logger.error("[PLAN PERSIST] No canvas_id provided")
            return False

        host = connection_config.get("host") or connection_config.get("hostname")
        port = int(connection_config.get("port", 5432))
        dbname = connection_config.get("database")
        user = connection_config.get("user") or connection_config.get("username")
        password = connection_config.get("password", "")

        logger.info(f"[PLAN PERSIST] Attempting to connect: host={host}, port={port}, db={dbname}, user={user}")

        if not dbname or not user:
            logger.error(f"[PLAN PERSIST] Missing required connection params: dbname={dbname}, user={user}")
            return False

        conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password, connect_timeout=10
        )
        conn.autocommit = True
        cursor = conn.cursor()

        logger.info("[PLAN PERSIST] Connected successfully, creating schema/table")
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "CANVAS_CACHE"')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS "CANVAS_CACHE"."execution_plans" (
                canvas_id VARCHAR(255) PRIMARY KEY,
                plan_hash VARCHAR(64),
                plan_data JSONB,
                staging_schema VARCHAR(255),
                total_queries INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        logger.info("[PLAN PERSIST] Serializing plan object")

        # Custom serialization to handle nested dataclasses
        def serialize_plan(obj):
            """Recursively convert dataclasses to dicts."""
            if hasattr(obj, '__dataclass_fields__'):
                return {k: serialize_plan(v) for k, v in asdict(obj).items()}
            elif isinstance(obj, list):
                return [serialize_plan(item) for item in obj]
            elif isinstance(obj, dict):
                return {k: serialize_plan(v) for k, v in obj.items()}
            else:
                return obj

        try:
            plan_dict = serialize_plan(plan_obj)
            plan_json = json.dumps(plan_dict, indent=2)
            logger.debug(f"[PLAN PERSIST] Serialized plan size: {len(plan_json)} bytes")
        except Exception as ser_err:
            logger.error(f"[PLAN PERSIST] Serialization error: {ser_err}", exc_info=True)
            return False

        staging_schema = plan_obj.staging_schema
        total_queries = plan_obj.total_queries

        logger.info(f"[PLAN PERSIST] Inserting plan: canvas={canvas_id}, hash={plan_hash[:12]}, queries={total_queries}")
        cursor.execute('''
            INSERT INTO "CANVAS_CACHE"."execution_plans"
            (canvas_id, plan_hash, plan_data, staging_schema, total_queries)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (canvas_id) DO UPDATE SET
                plan_hash = EXCLUDED.plan_hash,
                plan_data = EXCLUDED.plan_data,
                staging_schema = EXCLUDED.staging_schema,
                total_queries = EXCLUDED.total_queries,
                created_at = CURRENT_TIMESTAMP
        ''', (str(canvas_id), plan_hash, plan_json, staging_schema, total_queries))

        logger.info(f"[PLAN PERSIST] ✓ Successfully saved plan {plan_hash[:12]} for canvas {canvas_id}")
        return True
    except Exception as e:
        logger.error(f"[PLAN PERSIST] ✗ FAILED: {type(e).__name__}: {e}", exc_info=True)
        return False
    finally:
        if conn:
            conn.close()

def get_latest_plan(connection_config: dict[str, Any], canvas_id: str) -> Optional[dict[str, Any]]:
    """
    Retrieves the most recent execution plan for a canvas.
    Returns {"plan_hash": str, "plan_data": dict} or None if no plan exists.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=connection_config.get("host") or connection_config.get("hostname"),
            port=int(connection_config.get("port", 5432)),
            dbname=connection_config.get("database"),
            user=connection_config.get("user") or connection_config.get("username"),
            password=connection_config.get("password")
        )
        cursor = conn.cursor()
        cursor.execute('''
            SELECT plan_hash, plan_data FROM "CANVAS_CACHE"."execution_plans"
            WHERE canvas_id = %s
        ''', (str(canvas_id),))
        row = cursor.fetchone()
        if not row:
            return None
        plan_hash, plan_data = row[0], row[1]
        if plan_data is None:
            return None
        if isinstance(plan_data, str):
            plan_data = json.loads(plan_data)
        return {"plan_hash": plan_hash or "", "plan_data": plan_data}
    except Exception as e:
        logger.error(f"[PLAN PERSIST] Error: {e}")
        return None
    finally:
        if conn:
            conn.close()

def deserialize_plan(plan_dict: dict[str, Any]) -> "ExecutionPlan":
    """
    Rebuild an ExecutionPlan from a serialized dict (e.g. from get_latest_plan or JSON).
    """
    levels = []
    for lev in plan_dict.get("levels", []):
        queries = [
            CompiledSQL(
                sql=q.get("sql", ""),
                is_nested=bool(q.get("is_nested", False)),
                dependencies=list(q.get("dependencies", [])),
            )
            for q in lev.get("queries", [])
        ]
        levels.append(ExecutionLevel(
            level_num=int(lev.get("level_num", 0)),
            queries=queries,
            node_ids=list(lev.get("node_ids", [])),
        ))
    dest_creates = plan_dict.get("destination_creates")
    final_ins = plan_dict.get("final_inserts")
    if dest_creates is None:
        dest_creates = [plan_dict["destination_create_sql"]] if plan_dict.get("destination_create_sql") else []
    if final_ins is None:
        final_ins = [plan_dict["final_insert_sql"]] if plan_dict.get("final_insert_sql") else []

    return ExecutionPlan(
        job_id=str(plan_dict.get("job_id", "")),
        staging_schema=str(plan_dict.get("staging_schema", "")),
        levels=levels,
        destination_create_sql=plan_dict.get("destination_create_sql"),
        final_insert_sql=plan_dict.get("final_insert_sql"),
        destination_creates=dest_creates if isinstance(dest_creates, list) else [],
        final_inserts=final_ins if isinstance(final_ins, list) else [],
        cleanup_sql=plan_dict.get("cleanup_sql") or "",
        total_queries=int(plan_dict.get("total_queries", 0)),
    )

# =============================================================================
# INTERNAL HELPERS
# =============================================================================

def _find_schema_anchor(
    start_node_id: str,
    reverse_adjacency: dict[str, list[str]],
    node_map: dict[str, Any],
) -> str:
    """
    Trace back from a node to find the schema anchor (join, aggregation, or source)
    that produces the full column set. Use this for destination schema when the
    path goes through a projection that may filter columns.
    """
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

def _generate_all_destination_creates(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    node_map: dict[str, Any],
    config: dict[str, Any],
    *,
    materialization_points: Optional[dict[str, Any]] = None,
    levels: Optional[list[Any]] = None,
    job_id: str = "",
) -> list[str]:
    """Generate CREATE TABLE for each destination. Returns list of SQL statements."""
    dest_nodes = [
        n for n in nodes
        if _get_node_type(n) in ("destination", "destination-postgresql", "destination-postgres")
    ]
    staging_columns_by_node = _build_staging_columns_from_plan(levels or [], job_id) if levels else {}
    result = []
    for dest_node in dest_nodes:
        sql = _generate_destination_create_one(
            dest_node, edges, node_map, config,
            materialization_points=materialization_points or {},
            staging_columns_by_node=staging_columns_by_node,
        )
        if sql:
            result.append(sql)
    return result

def _parse_select_columns_from_create_as(sql: str) -> list[str]:
    """
    Extract output column names from CREATE TABLE ... AS SELECT ... SQL (ordered).
    Used to ensure final INSERT only selects columns that exist in staging.
    """
    if not sql or " AS " not in sql.upper():
        return []
    match = re.search(r"CREATE\s+TABLE\s+.+?\s+AS\s+SELECT\s+(.+?)\s+FROM\s+", sql, re.DOTALL | re.IGNORECASE)
    if not match:
        return []
    select_list = match.group(1).strip()
    cols: list[str] = []
    # Split by comma, respecting quoted strings
    depth = 0
    start = 0
    for i, c in enumerate(select_list + ","):
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif c == "," and depth == 0:
            part = select_list[start:i].strip()
            start = i + 1
            # Match AS "alias" or "col" (alias = col)
            as_m = re.search(r'\s+AS\s+"([^"]+)"\s*$', part, re.IGNORECASE)
            if as_m:
                cols.append(as_m.group(1))
            else:
                col_m = re.match(r'"([^"]+)"', part.strip())
                if col_m:
                    cols.append(col_m.group(1))
    return cols

def _build_staging_columns_from_plan(levels: list[Any], job_id: str) -> dict[str, list[str]]:
    """Build node_id -> ordered list of staging column names from compiled plan levels."""
    result: dict[str, list[str]] = {}
    for level in levels:
        for q, node_id in zip(level.queries, level.node_ids):
            if hasattr(q, "sql") and q.sql and "CREATE TABLE" in q.sql.upper() and " AS " in q.sql.upper():
                cols = _parse_select_columns_from_create_as(q.sql)
                if cols:
                    result[node_id] = cols
    return result

def _generate_all_final_inserts(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    node_map: dict[str, Any],
    materialization_points: dict[str, Any],
    config: dict[str, Any],
    job_id: str,
    levels: Optional[list[Any]] = None,
) -> list[str]:
    """Generate INSERT for each destination. Returns list of SQL statements."""
    staging_columns_by_node = _build_staging_columns_from_plan(levels or [], job_id) if levels else {}
    dest_nodes = [
        n for n in nodes
        if _get_node_type(n) in ("destination", "destination-postgresql", "destination-postgres")
    ]
    result = []
    for dest_node in dest_nodes:
        sql = _generate_final_insert_one(
            dest_node, edges, node_map, materialization_points, config, job_id,
            staging_columns_by_node=staging_columns_by_node,
        )
        if sql:
            result.append(sql)
    return result

def _normalize_column_to_business_name(name: str, all_names: list[str]) -> str:
    """Use _L_/_R_ prefix everywhere (same as UI). No conversion to _left/_right suffix."""
    if not name or (not name.startswith("_L_") and not name.startswith("_R_")):
        return name
    # Keep _L_X and _R_X as-is (business name = prefix format, same as UI)
    return name

def _normalize_anchor_columns(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize columns: remove _L_/_R_ for destination, use business names. Keep orig_name for staging lookup."""
    if not columns:
        return []
    all_names = [c.get("business_name") or c.get("name") or "" for c in columns]
    out = []
    for c in columns:
        business_name = c.get("business_name") or c.get("label") or c.get("name")
        if not business_name:
            continue
        normalized = _normalize_column_to_business_name(business_name, all_names)
        out.append({**c, "business_name": normalized, "orig_name": business_name})
    return out

def _generate_destination_create_one(
    dest_node: dict[str, Any],
    edges: list[dict[str, Any]],
    node_map: dict[str, Any],
    config: dict[str, Any],
    *,
    materialization_points: Optional[dict[str, Any]] = None,
    staging_columns_by_node: Optional[dict[str, list[str]]] = None,
) -> Optional[str]:
    """Generate CREATE TABLE IF NOT EXISTS for a single destination."""
    reverse_adjacency = _build_reverse_adjacency(edges)
    dest_id = dest_node["id"]
    dest_config = dest_node.get("data", {}).get("config", {})

    table_name = dest_config.get("tableName")
    schema_name = dest_config.get("schema", "")

    if not table_name:
        return None

    if schema_name:
        qualified_table = f'"{schema_name}"."{table_name}"'
    else:
        qualified_table = f'"{table_name}"'

    parents = reverse_adjacency.get(dest_id, [])
    if not parents:
        return None

    parent_id = parents[0]
    materialization_points = materialization_points or {}
    staging_columns_by_node = staging_columns_by_node or {}

    # When parent is materialization point and we have staging columns, use ONLY those (source of truth).
    # Otherwise we create destination with schema anchor columns, causing NULLs for excluded columns.
    if parent_id in materialization_points and parent_id in staging_columns_by_node:
        staging_cols = staging_columns_by_node[parent_id]
        anchor_meta = config.get("node_output_metadata", {}).get(_find_schema_anchor(parent_id, reverse_adjacency, node_map), {})
        parent_meta = config.get("node_output_metadata", {}).get(parent_id, {})
        anchor_columns = _normalize_anchor_columns(anchor_meta.get("columns", []))
        parent_columns = parent_meta.get("columns", [])
        tech_to_business = {c.get("technical_name") or c.get("name"): c.get("business_name") or c.get("name") for c in anchor_columns + parent_columns if (c.get("technical_name") or c.get("name")) and (c.get("business_name") or c.get("name"))}
        tech_to_business.update({c.get("name"): c.get("business_name") or c.get("name") for c in anchor_columns + parent_columns if c.get("name")})
        columns_meta = []
        for sc in staging_cols:
            business_name = tech_to_business.get(sc)
            if not business_name:
                # Fallback: technical_name = {node_prefix}_{col_name} (e.g. f9a679f3_upper_name -> upper_name)
                m = re.match(r"^[a-f0-9]{8}_(.+)$", sc, re.IGNORECASE)
                business_name = m.group(1) if m else sc
            if business_name:
                columns_meta.append({"business_name": business_name, "technical_name": sc})
    else:
        schema_anchor_id = _find_schema_anchor(parent_id, reverse_adjacency, node_map)
        output_metadata = config.get("node_output_metadata", {}).get(schema_anchor_id, {})
        columns_meta = _normalize_anchor_columns(output_metadata.get("columns", []))
        if not columns_meta:
            parent_meta = config.get("node_output_metadata", {}).get(parent_id, {})
            columns_meta = _normalize_anchor_columns(parent_meta.get("columns", []))
    if not columns_meta:
        return None

    col_defs = []
    seen_business = set()
    for col in columns_meta:
        business_name = col.get("business_name") or col.get("label") or col.get("name")
        if not business_name or business_name in seen_business:
            continue
        seen_business.add(business_name)
        datatype = col.get("datatype") or col.get("data_type")

        # Map types to Postgres
        pg_type = "TEXT"
        if datatype:
            dt = str(datatype).upper()
            if dt in ("INTEGER", "INT", "BIGINT", "SMALLINT"):
                pg_type = "BIGINT"
            elif dt in ("NUMERIC", "DECIMAL", "REAL", "FLOAT", "DOUBLE"):
                pg_type = "DOUBLE PRECISION"
            elif dt in ("BOOLEAN", "BOOL"):
                pg_type = "BOOLEAN"
            elif dt in ("TIMESTAMP", "DATE", "TIME", "DATETIME"):
                pg_type = "TIMESTAMP"

        col_defs.append(f'"{business_name}" {pg_type}')

    if not col_defs:
        return None

    newline_join = ",\n  ".join(col_defs)

    # Handle different materialization strategies.
    # insert (default): CREATE IF NOT EXISTS - create only when table is not present.
    # Uses metadata (schema anchor → destination) with business names for column definitions.
    load_mode = dest_config.get("loadMode") or "insert"

    if load_mode == "drop_and_reload":
        return f'DROP TABLE IF EXISTS {qualified_table} CASCADE;\nCREATE TABLE {qualified_table} (\n  {newline_join}\n)'

    elif load_mode == "replace":
        return (
            f'CREATE TABLE IF NOT EXISTS {qualified_table} (\n  {newline_join}\n);\n'
            f'TRUNCATE TABLE {qualified_table} CASCADE;'
        )

    # insert: create if not present, using business names from metadata
    return f'CREATE TABLE IF NOT EXISTS {qualified_table} (\n  {newline_join}\n)'

def _build_execution_levels(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]]
) -> list[list[str]]:
    """Build execution levels via topological sort."""
    node_map = {node["id"]: node for node in nodes}
    adjacency = _build_adjacency(edges)
    reverse_adjacency = _build_reverse_adjacency(edges)

    in_degree = {}
    for node_id in node_map.keys():
        in_degree[node_id] = len(reverse_adjacency.get(node_id, []))

    levels = []
    degree = dict(in_degree)

    while True:
        current_level = [nid for nid, deg in degree.items() if deg == 0]
        if not current_level:
            break
        levels.append(current_level)
        for node_id in current_level:
            degree[node_id] = -1
            for child_id in adjacency.get(node_id, []):
                if degree[child_id] >= 0:
                    degree[child_id] -= 1
    return levels

def _generate_final_insert_one(
    dest_node: dict[str, Any],
    edges: list[dict[str, Any]],
    node_map: dict[str, Any],
    materialization_points: dict[str, Any],
    config: dict[str, Any],
    job_id: str,
    *,
    staging_columns_by_node: Optional[dict[str, list[str]]] = None,
) -> Optional[str]:
    """Generate final INSERT INTO a single destination."""
    reverse_adjacency = _build_reverse_adjacency(edges)
    dest_config = dest_node.get("data", {}).get("config", {})
    table_name = dest_config.get("tableName")
    schema_name = dest_config.get("schema", "")
    if not table_name:
        return None

    qualified_table = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
    parents = reverse_adjacency.get(dest_node["id"], [])
    if not parents:
        return None

    parent_id = parents[0]
    if parent_id in materialization_points:
        source_table = materialization_points[parent_id].staging_table
        if '.' in source_table:
            schema, table = source_table.split('.', 1)
            quoted_source = f'"{schema}"."{table}"'
        else:
            quoted_source = f'"{source_table}"'
    else:
        compiled = compile_nested_sql(parent_id, node_map, edges, materialization_points, config)
        source_base_sql = compiled.sql

    # Use schema anchor for full column set. Normalize _L_/_R_ to business names.
    schema_anchor_id = _find_schema_anchor(parent_id, reverse_adjacency, node_map)
    anchor_meta = config.get("node_output_metadata", {}).get(schema_anchor_id, {})
    parent_meta = config.get("node_output_metadata", {}).get(parent_id, {})
    anchor_columns = _normalize_anchor_columns(anchor_meta.get("columns", []))
    parent_columns = parent_meta.get("columns", [])

    # Staging columns that actually exist in parent's output.
    # Prefer parsed columns from compiled staging SQL (source of truth); fallback to metadata.
    staging_columns_by_node = staging_columns_by_node or {}
    if parent_id in staging_columns_by_node:
        staging_cols_list = staging_columns_by_node[parent_id]
        staging_cols_set = set(staging_cols_list)
    else:
        staging_cols_list = None
        staging_cols_set = {c.get("technical_name") or c.get("business_name") or c.get("name") for c in parent_columns if c.get("technical_name") or c.get("business_name") or c.get("name")}
    # When parent is a filter, staging includes filter-condition columns (added by flatten_segment).
    # Parent metadata is pass-through from projection and may omit them; augment staging_cols_set.
    # Filter config may use _L_X / _R_X (join UI) while staging has technical_name (e.g. 39ef59b7_cmp_id).
    parent_node = node_map.get(parent_id, {})
    if _get_node_type(parent_node) == "filter":
        for cond in parent_node.get("data", {}).get("config", {}).get("conditions", []):
            col = cond.get("column")
            if not col:
                continue
            staging_cols_set.add(col)
            for anc in anchor_columns:
                bn = anc.get("business_name") or anc.get("name")
                tn = anc.get("technical_name")
                if bn and tn and (col == bn or col == tn):
                    staging_cols_set.add(tn)
            # Resolve _L_X / _R_X to technical_name (staging uses source-prefixed names)
            if col.startswith("_L_") or col.startswith("_R_"):
                for anc in anchor_columns:
                    bn = anc.get("business_name") or anc.get("name")
                    tn = anc.get("technical_name")
                    if tn and bn and bn == col:
                        staging_cols_set.add(tn)
                        break
    # Only add anchor columns when we don't have parsed staging columns (metadata fallback).
    if parent_id in materialization_points and parent_id not in staging_columns_by_node:
        staging_cols_set |= {anc.get("technical_name") for anc in anchor_columns if anc.get("technical_name")}
    # Build lookup: staging has technical names; map dest column -> staging column
    parent_by_business = {}
    parent_by_tech = {}
    for c in parent_columns:
        bn = c.get("business_name") or c.get("label") or c.get("name")
        tn = c.get("technical_name") or c.get("business_name") or c.get("name")
        if bn:
            parent_by_business[bn] = tn or bn
            parent_by_tech[bn] = tn or bn
        if tn:
            parent_by_tech[tn] = tn
    # For join: map _L_X / _R_X to technical_name (from metadata)
    for c in anchor_meta.get("columns", []):
        ref_name = c.get("business_name") or c.get("name")
        tn = c.get("technical_name")
        if ref_name and tn and (ref_name.startswith("_L_") or ref_name.startswith("_R_")):
            parent_by_tech[ref_name] = tn
            parent_by_business[ref_name] = tn

    # When we have staging columns from parsed SQL, use ONLY those (source of truth).
    # This avoids creating destination with schema anchor columns and inserting NULL for excluded ones.
    if staging_cols_list and parent_id in materialization_points:
        tech_to_business = {}
        for c in anchor_columns + parent_columns:
            tn = c.get("technical_name") or c.get("name")
            bn = c.get("business_name") or c.get("label") or c.get("name")
            if tn and bn:
                tech_to_business[tn] = bn
        for c in anchor_meta.get("columns", []) + parent_meta.get("columns", []):
            tn = c.get("technical_name")
            bn = c.get("business_name") or c.get("name")
            if tn and bn:
                tech_to_business[tn] = bn
        dest_cols = []
        select_expressions = []
        for staging_col in staging_cols_list:
            business_name = tech_to_business.get(staging_col)
            if not business_name:
                # Fallback: technical_name = {node_prefix}_{col_name} (e.g. f9a679f3_upper_name -> upper_name)
                # Use business name for destination so calculated columns show as upper_name, not technical name
                m = re.match(r"^[a-f0-9]{8}_(.+)$", staging_col, re.IGNORECASE)
                business_name = m.group(1) if m else staging_col
            dest_cols.append(f'"{business_name}"')
            select_expressions.append(f'"{staging_col}"')
        if dest_cols:
            col_list = ", ".join(dest_cols)
            select_list = ", ".join(select_expressions)
            return f'INSERT INTO {qualified_table} ({col_list})\nSELECT {select_list} FROM {quoted_source}'

    if anchor_columns and parent_id in materialization_points:
        dest_cols = []
        select_expressions = []
        for col in anchor_columns:
            business_name = col.get("business_name") or col.get("name")
            tech_name = col.get("technical_name")
            orig_name = col.get("orig_name") or col.get("business_name") or col.get("name")  # original e.g. _L_X for lookup
            if not business_name:
                continue
            # Resolve staging column: technical_name from metadata, or lookup by orig/business name
            staging_col = (
                tech_name
                or parent_by_tech.get(orig_name)
                or parent_by_tech.get(business_name)
                or parent_by_business.get(business_name)
            )
            # Use NULL only when staging does not have this column (e.g. filter selected subset)
            if staging_col and staging_col not in staging_cols_set:
                staging_col = None
            dest_cols.append(f'"{business_name}"')
            if staging_col:
                select_expressions.append(f'"{staging_col}"')
            else:
                select_expressions.append("NULL")
                logger.warning(
                    "Destination column '%s' will be NULL: not selected in projection/filter chain",
                    business_name,
                )
        if dest_cols:
            col_list = ", ".join(dest_cols)
            select_list = ", ".join(select_expressions)
            return f'INSERT INTO {qualified_table} ({col_list})\nSELECT {select_list} FROM {quoted_source}'

    # Fallback: use parent metadata only. Prefer business_name for staging (when aliased).
    output_metadata = config.get("node_output_metadata", {}).get(parent_id, {})
    columns_meta = output_metadata.get("columns", [])
    if columns_meta and parent_id in materialization_points:
        dest_cols = []
        select_expressions = []
        for col in columns_meta:
            business_name = col.get("business_name") or col.get("label") or col.get("name")
            staging_col = business_name or col.get("technical_name") or col.get("business_name") or col.get("name")
            if staging_col and business_name:
                dest_cols.append(f'"{business_name}"')
                select_expressions.append(f'"{staging_col}"')
        if dest_cols:
            col_list = ", ".join(dest_cols)
            select_list = ", ".join(select_expressions)
            return f'INSERT INTO {qualified_table} ({col_list})\nSELECT {select_list} FROM {quoted_source}'

    if parent_id in materialization_points:
        return f'INSERT INTO {qualified_table}\nSELECT * FROM {quoted_source}'
    return f'INSERT INTO {qualified_table}\n{source_base_sql}'

def _get_node_type(node: dict[str, Any]) -> str:
    """Extract node type. Normalize frontend 'aggregate' to 'aggregation'."""
    raw = (node.get("type") or node.get("data", {}).get("type") or "").lower().strip()
    if raw == "aggregate":
        return "aggregation"
    return raw

def _build_adjacency(edges: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Build forward adjacency list."""
    adjacency = {}
    for edge in edges:
        source, target = edge["source"], edge["target"]
        if source not in adjacency:
            adjacency[source] = []
        adjacency[source].append(target)
    return adjacency

def _build_reverse_adjacency(edges: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Build reverse adjacency list."""
    reverse = {}
    for edge in edges:
        source, target = edge["source"], edge["target"]
        if target not in reverse:
            reverse[target] = []
        reverse[target].append(source)
    return reverse
