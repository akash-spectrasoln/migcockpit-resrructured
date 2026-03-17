"""
Metadata Generator for Pipeline Nodes

Generates column metadata for all nodes in a pipeline during validation.
This ensures metadata is available for filter pushdown analysis without
requiring manual preview of each node.
"""

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Optional integration with the Django `api` package. When running the
# standalone migration service (without Django on PYTHONPATH), this import
# may fail; in that case we gracefully skip live metadata generation and
# fall back to cached node_output_metadata.
try:
    from api.utils.db_connection import get_customer_db_connection
    from api.utils.helpers import decrypt_source_data
except ModuleNotFoundError as e:
    logger.info(
        "[METADATA] Django 'api' package not available; "
        "skipping live metadata generation: %s",
        e,
    )
    get_customer_db_connection = None  # type: ignore[assignment]
    decrypt_source_data = None  # type: ignore[assignment]

def generate_all_node_metadata(
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    canvas_id: int,
    connection_config: dict[str, Any],
    config: dict[str, Any]
) -> int:
    """
    Generate and save metadata for all nodes in the pipeline.

    This function:
    1. Executes each node in topological order
    2. Generates column metadata (name, technical_name, db_name, base)
    3. Saves metadata to node_cache_metadata table

    Args:
        nodes: Dictionary of node definitions
        edges: List of edge definitions
        canvas_id: Canvas ID
        connection_config: Database connection config
        config: Pipeline configuration

    Returns:
        Number of nodes for which metadata was generated
    """
    # When running without the Django `api` package, live metadata generation
    # is not available; callers should rely on cached metadata instead.
    if get_customer_db_connection is None:
        logger.info(
            "[METADATA] Skipping metadata generation: Django 'api' package "
            "is not available (get_customer_db_connection missing)."
        )
        return 0

    if not canvas_id or not connection_config:
        logger.warning(
            "[METADATA] Cannot generate metadata: missing canvas_id or connection_config. "
            "Pass connection_config in the validate request body to persist metadata (including calculated columns) to the DB."
        )
        return 0

    logger.info(f"[METADATA] Generating metadata for {len(nodes)} nodes in canvas {canvas_id}")

    try:
        # Get database connection
        conn = get_customer_db_connection(connection_config)
        cursor = conn.cursor()

        # Ensure schema and metadata table exist (so we don't rely on Django NodeCacheManager)
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "CANVAS_CACHE";')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS "CANVAS_CACHE".node_cache_metadata (
                id SERIAL PRIMARY KEY,
                canvas_id INTEGER NOT NULL,
                node_id VARCHAR(100) NOT NULL,
                node_name VARCHAR(255),
                node_type VARCHAR(50) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                config_hash VARCHAR(64),
                row_count INTEGER DEFAULT 0,
                column_count INTEGER DEFAULT 0,
                columns JSONB,
                source_node_ids JSONB,
                created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_valid BOOLEAN DEFAULT TRUE,
                UNIQUE(canvas_id, node_id)
            );
        ''')
        conn.commit()

        # Invalidate existing metadata for this canvas so we never have stale rows.
        # (Orphaned rows from deleted nodes, or old columns from config changes.)
        cursor.execute(
            'DELETE FROM "CANVAS_CACHE".node_cache_metadata WHERE canvas_id = %s',
            (canvas_id,)
        )
        deleted = cursor.rowcount
        if deleted:
            logger.debug(f"[METADATA] Cleared {deleted} existing metadata row(s) for canvas {canvas_id}")

        # Build topological order
        node_order = _topological_sort(nodes, edges)
        logger.debug(f"[METADATA] Processing nodes in order: {[n[:8] for n in node_order]}")

        metadata_count = 0

        for node_id in node_order:
            node = nodes[node_id]
            node_type = node.get('type') or node.get('data', {}).get('type')
            # Merge config from top-level and data.config so calculated_columns are found from either place
            node_config = { **node.get('config', {}), **node.get('data', {}).get('config', {}) }
            node_name = node.get('data', {}).get('label', node_type)

            logger.debug(f"[METADATA] Generating metadata for {node_type} node {node_id[:8]}...")

            try:
                # Generate metadata based on node type
                columns_metadata = _generate_node_metadata(
                    node_id=node_id,
                    node_type=node_type,
                    node_config=node_config,
                    nodes=nodes,
                    edges=edges,
                    cursor=cursor,
                    canvas_id=canvas_id
                )

                if columns_metadata:
                    # Save to database
                    _save_metadata_to_db(
                        cursor=cursor,
                        canvas_id=canvas_id,
                        node_id=node_id,
                        node_name=node_name,
                        node_type=node_type,
                        columns=columns_metadata,
                        config=node_config
                    )

                    metadata_count += 1
                    logger.debug(f"[METADATA] ✓ Saved {len(columns_metadata)} columns for node {node_id[:8]}")
                else:
                    logger.debug(f"[METADATA] ⚠ No metadata generated for node {node_id[:8]}")

            except Exception as e:
                logger.warning(f"[METADATA] Failed to generate metadata for node {node_id[:8]}: {e}")
                continue

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"[METADATA] ✓ Generated metadata for {metadata_count}/{len(nodes)} nodes")
        return metadata_count

    except Exception as e:
        logger.error(f"[METADATA] Failed to generate metadata: {e}")
        import traceback
        logger.debug(f"[METADATA] Traceback: {traceback.format_exc()}")
        return 0

def _topological_sort(nodes: dict[str, Any], edges: list[dict[str, Any]]) -> list[str]:
    """Sort nodes in topological order (sources first, then downstream)."""
    from collections import defaultdict, deque

    # Build adjacency list
    in_degree = {node_id: 0 for node_id in nodes}
    adj = defaultdict(list)

    for edge in edges:
        source = edge.get('source')
        target = edge.get('target')
        if source and target:
            adj[source].append(target)
            in_degree[target] += 1

    # Find all sources (nodes with no incoming edges)
    queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
    result = []

    while queue:
        node_id = queue.popleft()
        result.append(node_id)

        for neighbor in adj[node_id]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    return result

def _generate_node_metadata(
    node_id: str,
    node_type: str,
    node_config: dict[str, Any],
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    cursor: Any,
    canvas_id: int
) -> list[dict[str, Any]]:
    # Normalize node type
    node_type = (node_type or "").lower().strip()
    if node_type.endswith('node'):
        node_type = node_type[:-4]

    logger.debug(f"[METADATA] Normalized node type: {node_type} for node {node_id[:8]}")

    if node_type == 'source':
        return _generate_source_metadata(node_id, node_config, cursor, canvas_id, nodes)
    elif node_type == 'join':
        return _generate_join_metadata(node_id, nodes, edges, cursor, canvas_id)
    elif node_type in ('filter', 'sort', 'order_by'):
        return _generate_filter_metadata(node_id, nodes, edges, cursor, canvas_id)
    elif node_type == 'projection':
        return _generate_projection_metadata(node_id, node_config, nodes, edges, cursor, canvas_id)
    elif node_type in ('aggregate', 'aggregation', 'group', 'group_by'):
        # For now, pass through or handle basic aggregation
        return _read_upstream_metadata_fallback(node_id, edges, cursor, canvas_id)
    elif node_type == 'union':
        return _read_upstream_metadata_fallback(node_id, edges, cursor, canvas_id)
    else:
        logger.debug(f"[METADATA] Unsupported or unknown node type: {node_type}, attempting fallback")
        return _read_upstream_metadata_fallback(node_id, edges, cursor, canvas_id)

def _read_upstream_metadata_fallback(node_id: str, edges: list[dict[str, Any]], cursor: Any, canvas_id: int) -> list[dict[str, Any]]:
    """Fallback: try to read metadata from the first upstream node."""
    upstream_ids = [e['source'] for e in edges if e['target'] == node_id]
    if upstream_ids:
        return _read_metadata_from_db(cursor, canvas_id, upstream_ids[0])
    return []

def _generate_source_metadata(node_id: str, node_config: dict[str, Any], cursor: Any, canvas_id: int, nodes: dict[str, Any]) -> list[dict[str, Any]]:
    """Generate metadata for source node by querying the actual source database."""
    source_id = node_config.get('sourceId')
    schema = node_config.get('schema')
    table = node_config.get('table') or node_config.get('tableName') or node_config.get('selectedTable')

    # If table is still missing, check in 'tables' list
    if not table and 'tables' in node_config and isinstance(node_config['tables'], list) and len(node_config['tables']) > 0:
        table_info = node_config['tables'][0]
        if isinstance(table_info, dict):
            table = table_info.get('name') or table_info.get('table_name')
            schema = schema or table_info.get('schema') or table_info.get('schema_name')
        else:
            table = str(table_info)

    if not table:
        logger.warning(f"[METADATA] Source node {node_id} missing table name.")
        return []

    if not schema:
        schema = 'public'

    # If we don't have a sourceId, we can't connect to get real columns
    if not source_id:
        logger.warning(f"[METADATA] Source node {node_id} missing sourceId, using fallback empty metadata")
        return []

    try:
        # 1. Fetch source configuration from the customer DB (where the cursor is currently pointed)
        cursor.execute('SELECT source_config, created_on FROM "GENERAL".source WHERE id = %s', (source_id,))
        source_row = cursor.fetchone()

        if not source_row:
            logger.warning(f"[METADATA] Source ID {source_id} not found in GENERAL.source")
            return []

        encrypted_config, created_on = source_row

        # 2. Get Customer ID for decryption
        # dbname == cust_id is the standard convention here
        conn = cursor.connection
        if hasattr(conn, 'get_dsn_parameters'):
            dbname = conn.get_dsn_parameters().get('dbname')
        elif hasattr(conn, 'info') and hasattr(conn.info, 'dbname'):
            dbname = conn.info.dbname
        else:
            # Last resort: try to get from the config or a default
            dbname = "C00008"

        cust_id = dbname

        # 3. Decrypt the configuration
        decrypted_config = decrypt_source_data(encrypted_config, cust_id, created_on)
        if not decrypted_config:
            logger.error(f"[METADATA] Failed to decrypt source config for source {source_id}")
            return []

        # 4. Connect to the ACTUAL source database
        source_conn = get_customer_db_connection(decrypted_config)
        source_cursor = source_conn.cursor()

        try:
            # 5. Query the source for column information
            source_cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table))

            columns = []
            for col_name, data_type in source_cursor.fetchall():
                # Generate technical_name using node_id prefix
                technical_name = f"{node_id[:8]}_{col_name}"

                columns.append({
                    "business_name": col_name,
                    "technical_name": technical_name,
                    "db_name": col_name,
                    "base": node_id,
                    "datatype": data_type,
                    "source": "base"
                })

            return columns
        finally:
            source_cursor.close()
            source_conn.close()

    except Exception as e:
        logger.error(f"[METADATA] Failed to fetch source metadata for {schema}.{table}: {e}")
        return []

def _generate_join_metadata(
    node_id: str,
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    cursor: Any,
    canvas_id: int
) -> list[dict[str, Any]]:
    """Generate metadata for join node. Use _L_/_R_ prefix for conflicting columns (same as UI).
    For conflicting column names (same base on both sides), use _L_{base} and _R_{base}."""
    # Find upstream nodes using targetHandle to distinguish left/right
    input_edges = [e for e in edges if e['target'] == node_id]

    left_edge = next((e for e in input_edges if e.get('targetHandle') == 'left'), None)
    right_edge = next((e for e in input_edges if e.get('targetHandle') == 'right'), None)

    if not left_edge or not right_edge:
        # Fallback if handles are not set
        if len(input_edges) == 2:
            left_id, right_id = input_edges[0]['source'], input_edges[1]['source']
        else:
            logger.warning(f"[METADATA] Join node {node_id} should have exactly 2 inputs, found {len(input_edges)}")
            return []
    else:
        left_id, right_id = left_edge['source'], right_edge['source']

    # Read upstream metadata from database
    left_meta = _read_metadata_from_db(cursor, canvas_id, left_id)
    right_meta = _read_metadata_from_db(cursor, canvas_id, right_id)

    if not left_meta or not right_meta:
        logger.warning(f"[METADATA] Missing upstream metadata for join {node_id} (Left: {bool(left_meta)}, Right: {bool(right_meta)})")
        return []

    # Collect base names from both sides to detect conflicts
    left_bases = {col.get('db_name') or col.get('business_name') or col.get('name') for col in left_meta}
    right_bases = {col.get('db_name') or col.get('business_name') or col.get('name') for col in right_meta}
    conflicting = left_bases & right_bases

    columns = []

    # Add left columns: use _L_{base} prefix when conflicting (same as UI convention)
    for col in left_meta:
        base = col.get('db_name') or col.get('business_name') or col.get('name')
        business_name = f"_L_{base}" if base in conflicting else base
        columns.append({
            "business_name": business_name,
            "technical_name": col.get('technical_name'),
            "db_name": col.get('db_name'),
            "base": col.get('base'),
            "datatype": col.get('datatype'),
            "source": col.get('source')
        })

    # Add right columns: use _R_{base} prefix when conflicting (same as UI convention)
    for col in right_meta:
        base = col.get('db_name') or col.get('business_name') or col.get('name')
        business_name = f"_R_{base}" if base in conflicting else base
        columns.append({
            "business_name": business_name,
            "technical_name": col.get('technical_name'),
            "db_name": col.get('db_name'),
            "base": col.get('base'),
            "datatype": col.get('datatype'),
            "source": col.get('source')
        })

    return columns

def _generate_filter_metadata(
    node_id: str,
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    cursor: Any,
    canvas_id: int
) -> list[dict[str, Any]]:
    """Generate metadata for filter node (same as upstream)."""
    # Find upstream node
    upstream_ids = [e['source'] for e in edges if e['target'] == node_id]

    if not upstream_ids:
        logger.warning("[METADATA] Filter node has no upstream")
        return []

    upstream_id = upstream_ids[0]

    # Read upstream metadata
    upstream_meta = _read_metadata_from_db(cursor, canvas_id, upstream_id)

    # Filter doesn't change columns, just pass through
    return upstream_meta if upstream_meta else []

def _generate_projection_metadata(
    node_id: str,
    node_config: dict[str, Any],
    nodes: dict[str, Any],
    edges: list[dict[str, Any]],
    cursor: Any,
    canvas_id: int
) -> list[dict[str, Any]]:
    """Generate metadata for projection node based on selected columns."""
    # Find upstream node
    upstream_ids = [e['source'] for e in edges if e['target'] == node_id]

    if not upstream_ids:
        logger.warning(f"[METADATA] Projection node {node_id} has no upstream")
        return []

    upstream_id = upstream_ids[0]
    upstream_meta = _read_metadata_from_db(cursor, canvas_id, upstream_id)

    # Get configuration (support multiple formats)
    selected_columns = (node_config.get('selectedColumns') or
                      node_config.get('columns') or
                      node_config.get('includedColumns') or
                      node_config.get('output_columns') or
                      [])

    excluded_columns = node_config.get('excludedColumns', [])
    mode = node_config.get('selectedMode') or node_config.get('mode', 'INCLUDE')

    # If no upstream metadata yet, still build minimal metadata from config (so calculated columns get saved)
    if not upstream_meta:
        columns = []
        column_names = []
        if selected_columns:
            if isinstance(selected_columns, list) and selected_columns and isinstance(selected_columns[0], str):
                column_names = selected_columns
            else:
                column_names = [c.get('name') or c.get('column') for c in selected_columns if isinstance(c, dict) and (c.get('name') or c.get('column'))]
        for col_name in column_names:
            columns.append({'business_name': col_name, 'technical_name': col_name, 'db_name': col_name, 'base': upstream_id, 'datatype': 'TEXT'})
        _append_calculated_columns_to_metadata(columns, node_config, node_id)
        if columns:
            logger.debug(f"[METADATA] Projection {node_id[:8]} upstream metadata missing; built from config ({len(columns)} cols)")
        return columns

    # If no specific columns defined, pass through all (then add calculated columns if any)
    if not selected_columns and not excluded_columns:
        columns = list(upstream_meta) if upstream_meta else []
        _append_calculated_columns_to_metadata(columns, node_config, node_id)
        return columns

    columns = []

    if mode == 'EXCLUDE' or node_config.get('excludeMode'):
        # Exclude mode: select all except excluded
        columns = [c for c in upstream_meta if (c.get('business_name') or c.get('name') or '') not in excluded_columns]
    else:
        # Include mode: select only selected columns
        # selected_columns might be list of strings or list of objects
        column_names = []
        if selected_columns:
            if isinstance(selected_columns[0], str):
                column_names = selected_columns
            else:
                column_names = [c.get('name') or c.get('column') for c in selected_columns if c.get('included', True)]

        for col_name in column_names:
            matching = [c for c in upstream_meta if (c.get('business_name') or c.get('name')) == col_name or c.get('technical_name') == col_name]
            if matching:
                columns.append(matching[0])
                continue
            # Map _L_X / _R_X (frontend join UI) to join metadata (uses _L_/_R_ prefix)
            if col_name.startswith("_L_") and len(col_name) > 3:
                for c in upstream_meta:
                    bn = c.get('business_name') or c.get('name')
                    if bn == col_name or bn == col_name[3:]:  # _L_cmp_id or cmp_id (no conflict)
                        columns.append(c)
                        break
            elif col_name.startswith("_R_") and len(col_name) > 3:
                for c in upstream_meta:
                    bn = c.get('business_name') or c.get('name')
                    if bn == col_name or bn == col_name[3:]:  # _R_cmp_id or cmp_id (no conflict)
                        columns.append(c)
                        break
            # When user selects "cmp_id" (ambiguous base), add BOTH _L_cmp_id and _R_cmp_id
            elif col_name and not col_name.startswith("_L_") and not col_name.startswith("_R_"):
                left_col = next((c for c in upstream_meta if (c.get('business_name') or c.get('name')) == f"_L_{col_name}"), None)
                right_col = next((c for c in upstream_meta if (c.get('business_name') or c.get('name')) == f"_R_{col_name}"), None)
                if left_col:
                    columns.append(left_col)
                if right_col:
                    columns.append(right_col)

    _append_calculated_columns_to_metadata(columns, node_config, node_id)
    return columns

def _append_calculated_columns_to_metadata(
    columns: list[dict[str, Any]],
    node_config: dict[str, Any],
    projection_node_id: str,
) -> None:
    """Append calculated column entries to columns list (in-place). So they are saved to node_cache_metadata.
    base is the projection node itself (where the column is calculated)."""
    calculated = node_config.get('calculated_columns') or node_config.get('calculatedColumns') or []
    seen_names = {c.get('business_name') or c.get('name') or c.get('technical_name') for c in columns if c.get('business_name') or c.get('name') or c.get('technical_name')}
    added = 0
    for calc in calculated:
        if not isinstance(calc, dict):
            continue
        name = calc.get('name') or calc.get('alias')
        expression = calc.get('expression')
        if not name:
            continue
        if name in seen_names:
            continue
        seen_names.add(name)
        added += 1
        datatype = calc.get('datatype') or calc.get('data_type') or 'TEXT'
        # technical_name uses same prefix pattern as base columns: {node_id[:8]}_{column_name}
        technical_name = f"{projection_node_id[:8]}_{name}"
        columns.append({
            'business_name': name,
            'technical_name': technical_name,
            'db_name': None,  # calculated columns have no physical column
            'expression': expression,
            'base': projection_node_id,  # projection node where the column is calculated
            'datatype': datatype,
            'source': 'calculated',  # mark as calculated so UI/consumers can distinguish from base columns
            'isCalculated': True,
        })
    if added:
        logger.info(f"[METADATA] Projection {projection_node_id[:8]}: added {added} calculated column(s) to metadata (will be saved to node_cache_metadata)")

def _read_metadata_from_db(cursor: Any, canvas_id: int, node_id: str) -> list[dict[str, Any]]:
    """Read column metadata from database for a node."""

    try:
        cursor.execute("""
            SELECT columns
            FROM "CANVAS_CACHE".node_cache_metadata
            WHERE canvas_id = %s AND node_id = %s AND is_valid = TRUE
        """, (canvas_id, node_id))

        row = cursor.fetchone()
        if row and row[0]:
            columns_json = row[0]
            if isinstance(columns_json, str):
                return json.loads(columns_json)
            return columns_json

        return []

    except Exception as e:
        logger.debug(f"[METADATA] Could not read metadata for node {node_id[:8]}: {e}")
        return []

def _save_metadata_to_db(
    cursor: Any,
    canvas_id: int,
    node_id: str,
    node_name: str,
    node_type: str,
    columns: list[dict[str, Any]],
    config: dict[str, Any]
) -> None:
    """Save column metadata to database. Existing rows for this canvas are cleared before
    generate_all_node_metadata runs, so this is always a fresh insert."""
    from datetime import date, datetime
    from decimal import Decimal
    import hashlib

    def _json_default(obj):
        """Handle non-JSON-serializable types."""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    # Compute config hash (use default for any non-serializable values)
    try:
        config_str = json.dumps(config, sort_keys=True, default=str)
    except (TypeError, ValueError):
        config_str = "{}"
    config_hash = hashlib.sha256(config_str.encode()).hexdigest()[:16]

    # Sanitize node_type for table_name (e.g. "destination-postgresql" -> safe)
    safe_node_type = (node_type or "node").replace("-", "_")[:50]
    table_name = f"cv{canvas_id}_{safe_node_type}_{node_id[:8]}"

    # Serialize columns safely
    try:
        columns_json = json.dumps(columns, default=_json_default)
    except (TypeError, ValueError) as e:
        logger.warning(f"[METADATA] Could not serialize columns for node {node_id[:8]}: {e}")
        columns_json = "[]"

    cursor.execute("""
        INSERT INTO "CANVAS_CACHE".node_cache_metadata
        (canvas_id, node_id, node_name, node_type, table_name, config_hash,
         row_count, column_count, columns, is_valid)
        VALUES (%s, %s, %s, %s, %s, %s, 0, %s, %s, TRUE)
        ON CONFLICT (canvas_id, node_id) DO UPDATE SET
            node_name = EXCLUDED.node_name,
            node_type = EXCLUDED.node_type,
            table_name = EXCLUDED.table_name,
            config_hash = EXCLUDED.config_hash,
            column_count = EXCLUDED.column_count,
            columns = EXCLUDED.columns,
            last_accessed = CURRENT_TIMESTAMP,
            is_valid = TRUE
    """, (
        canvas_id, str(node_id), node_name or "", node_type or "node", table_name, config_hash,
        len(columns), columns_json
    ))
