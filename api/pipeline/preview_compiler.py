# Moved from: api/utils/sql_compiler.py
"""
SQL Compiler for single-query preview compilation.
Compiles entire pipeline DAG into a single SQL query using CTEs.

IMPORTANT: Compute Node Boundaries
- Compute nodes are HARD execution boundaries
- SQL compilation STOPS before any compute node
- Compute nodes must NOT appear in SQL CTEs or subqueries
- Compute nodes consume materialized DataFrames from upstream SQL
- Downstream nodes after compute require compute output metadata (not DB introspection)
"""
import logging
from typing import Any, Optional

from api.utils.expression_translator import ExpressionTranslator
from api.utils.filters import build_sql_where_clause, parse_filter_from_canvas
from api.utils.graph_utils import (
    find_sql_compilable_nodes,
    find_sql_compilable_nodes_from,
)
from api.utils.helpers import decrypt_source_data

_METADATA_TABLES = {'source', 'destination', 'canvas', 'node_cache_metadata', '_checkpoint_metadata'}
logger = logging.getLogger(__name__)

class SQLCompiler:
    """
    Compiles pipeline DAG into a single SQL query using CTEs.
    """

    def __init__(
        self,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        target_node_id: str,
        customer,
        db_type: str = 'postgresql',
        start_node_id: Optional[str] = None,
        initial_rows: Optional[list[dict[str, Any]]] = None,
        initial_columns: Optional[list[dict[str, Any]]] = None,
        start_table_ref: Optional[str] = None,
    ):
        """
        Initialize SQL compiler.

        Args:
            nodes: List of all nodes in pipeline
            edges: List of edges connecting nodes
            target_node_id: ID of target node to preview
            customer: Customer object
            db_type: Database type ('postgresql', 'mysql', 'sqlserver', 'oracle')
            start_node_id: Optional. When set, compile only from this node to target (resume-from-cache).
            initial_rows: Cached rows for start_node_id when start_node_id is set.
            initial_columns: Column metadata for start_node_id (list of dicts with name, datatype, etc.).
            start_table_ref: Optional physical table reference to start from (Checkpoint Cache).
        """
        self.nodes = nodes
        self.edges = edges
        self.target_node_id = target_node_id
        self.customer = customer
        self.db_type = db_type.lower()
        self.start_node_id = start_node_id
        self.initial_rows = initial_rows or []
        self.initial_columns = initial_columns or []
        self.start_table_ref = start_table_ref

        self.node_map = {n['id']: n for n in nodes}
        self.cte_map = {}  # node_id -> CTE name
        self.metadata_map = {}  # node_id -> output_metadata
        self.source_configs = {}  # source_node_id -> decrypted source config
        self.params = []  # SQL parameters
        self.pushed_down_filters = set()  # Track filters that were pushed down
        self.column_lineage = {}  # technical_name -> {'origin_node_id': str, 'origin_type': str, 'expression': str|None, 'origin_branch': 'left'|'right'|None}
        # Keyed by technical name so lineage is rename-safe; lookup supports name or technical_name via _resolve_lineage_key.
        self.filter_pushdown_info = {}  # filter_node_id -> {'pushdown_targets': [{'pushdown_node_id': str, 'conditions': list[Dict]}, ...]}

        # Validate target node exists
        if target_node_id not in self.node_map:
            raise ValueError(f"Target node {target_node_id} not found")

    def compile(self) -> tuple[str, list[Any], dict]:
        """
        Compile entire pipeline to single SQL query.
        STOPS at Compute nodes - Compute nodes are execution boundaries.

        Returns:
            (sql_query, params, output_metadata)
        """
        logger.info(f"Compiling pipeline for target node: {self.target_node_id}")

        # 1. Find SQL-compilable nodes (stops at compute boundaries; for compute
        # targets this will compile up to the compute node's input).
        # When start_node_id is set (resume-from-cache), compile only from that node to target.
        if self.start_node_id and self.initial_rows is not None:
            sql_nodes = find_sql_compilable_nodes_from(
                self.start_node_id, self.target_node_id, self.nodes, self.edges
            )
            logger.info(f"SQL-compilable nodes from cache start {self.start_node_id}: {sql_nodes}")
        else:
            sql_nodes = find_sql_compilable_nodes(self.nodes, self.edges, self.target_node_id)
        logger.info(f"SQL-compilable nodes (in order, stopping at compute boundaries): {sql_nodes}")

        if not sql_nodes:
            raise ValueError(f"No SQL-compilable nodes found for target {self.target_node_id}. Pipeline may start with a compute node.")

        # 2.5. FIRST PASS: Build CTEs to establish column lineage
        # We need lineage before we can analyze filter pushdown
        # Save initial params count to reset later
        len(self.params)

        for node_id in sql_nodes:
            node = self.node_map[node_id]
            node_type = node.get('data', {}).get('type')

            if node_type == 'compute':
                continue

            # Resume-from-cache: first node uses cached rows or table as seed/checkpoint CTE
            if self.start_node_id and node_id == self.start_node_id:
                if self.start_table_ref:
                    cte_sql, metadata = self._build_checkpoint_cte(self.start_table_ref, self.initial_columns)
                elif self.initial_rows is not None:
                    cte_sql, metadata = self._build_seed_cte(self.initial_rows, self.initial_columns)
                else:
                    # Fallback to normal build if no cache provided
                    if node_type == 'source':
                        cte_sql, metadata = self._build_source_cte(node)
                    elif node_type == 'filter':
                        cte_sql, metadata = self._build_filter_cte_pass1(node)
                    elif node_type == 'join':
                        cte_sql, metadata = self._build_join_cte(node)
                    elif node_type == 'projection':
                        cte_sql, metadata = self._build_projection_cte(node)
                    elif node_type == 'aggregate':
                        cte_sql, metadata = self._build_aggregate_cte(node)
            elif node_type == 'source':
                cte_sql, metadata = self._build_source_cte(node)
            elif node_type == 'filter':
                # Build filter CTE without pushdown analysis (will analyze in second pass)
                cte_sql, metadata = self._build_filter_cte_pass1(node)
            elif node_type == 'join':
                cte_sql, metadata = self._build_join_cte(node)
            elif node_type == 'projection':
                cte_sql, metadata = self._build_projection_cte(node)
            elif node_type == 'aggregate':
                cte_sql, metadata = self._build_aggregate_cte(node)
            else:
                raise ValueError(f"Unsupported node type for SQL compilation: {node_type}")

            cte_name = self._get_cte_name(node_id)
            self.cte_map[node_id] = cte_name
            self.metadata_map[node_id] = metadata

        # 2.6. SECOND PASS: Analyze filter pushdown
        # Now that we have lineage, analyze which filters can be pushed down
        filter_nodes = [nid for nid in sql_nodes if self.node_map[nid].get('data', {}).get('type') == 'filter']
        for filter_node_id in filter_nodes:
            filter_node = self.node_map[filter_node_id]
            filter_config = filter_node.get('data', {}).get('config', {})
            conditions = filter_config.get('conditions', [])

            if conditions:
                can_pushdown, pushdown_targets, unsafe_columns = self._analyze_filter_pushdown(filter_node_id, conditions)

                if can_pushdown and pushdown_targets:
                    self.pushed_down_filters.add(filter_node_id)
                    self.filter_pushdown_info[filter_node_id] = {'pushdown_targets': pushdown_targets}
                    logger.info(f"Filter {filter_node_id} will be pushed down to {len(pushdown_targets)} target(s)")

                    if not hasattr(self, '_cache_rewrite_signals'):
                        self._cache_rewrite_signals = []
                    for target in pushdown_targets:
                        self._cache_rewrite_signals.append({
                            'filter_node_id': filter_node_id,
                            'pushdown_node_id': target['pushdown_node_id']
                        })

        # 2.7. Reset params and rebuild CTEs with pushed-down filters
        # Clear params added in first pass (they'll be regenerated with correct values)
        self.params = []

        # Rebuild CTEs that have pushed-down filters
        nodes_to_rebuild = set()
        for _filter_node_id, pushdown_info in self.filter_pushdown_info.items():
            targets = pushdown_info.get('pushdown_targets', [])
            if not targets and pushdown_info.get('pushdown_node_id'):
                targets = [{'pushdown_node_id': pushdown_info['pushdown_node_id'], 'conditions': pushdown_info.get('conditions', [])}]
            for target in targets:
                pushdown_node_id = target['pushdown_node_id']
                nodes_to_rebuild.add(pushdown_node_id)
                # Also rebuild all nodes downstream of this pushdown target
                for node_id in sql_nodes:
                    if node_id != pushdown_node_id and self._is_downstream(node_id, pushdown_node_id):
                        nodes_to_rebuild.add(node_id)

        # 3. THIRD PASS: Rebuild CTEs with pushed-down filters included
        # Column lineage is tracked as we build CTEs
        ctes = []
        last_cte_node_id = None  # Track the last node that produced a CTE
        for node_id in sql_nodes:
            node = self.node_map[node_id]
            node_type = node.get('data', {}).get('type')

            # This should never happen due to find_sql_compilable_nodes, but double-check
            if node_type == 'compute':
                logger.warning(f"Skipping compute node {node_id} in SQL compilation - compute nodes are execution boundaries")
                continue

            # FILTER PUSHDOWN: Skip filter nodes that were pushed down
            # CRITICAL: Filter is schema-transparent. Its output = its input (e.g. p1 with upper_name).
            # Use the Filter's INPUT node for cte_map/metadata_map, NOT the pushdown target (source).
            # Otherwise p2 would read from source and miss columns added by p1 (e.g. calculated upper_name).
            if node_type == 'filter' and node_id in self.pushed_down_filters:
                logger.info(f"Skipping filter node {node_id} - already pushed down")
                input_node_id = self._get_input_node_id(node_id)
                if input_node_id and input_node_id in self.cte_map:
                    self.cte_map[node_id] = self.cte_map[input_node_id]
                    self.metadata_map[node_id] = self.metadata_map[input_node_id]
                continue

            # Rebuild CTE if it needs to include pushed-down filters
            if node_id in nodes_to_rebuild:
                logger.info(f"Rebuilding CTE for node {node_id} (type: {node_type}) to include pushed-down filters")
            else:
                logger.info(f"Building CTE for node {node_id} (type: {node_type})")

            if self.start_node_id and node_id == self.start_node_id and self.initial_rows is not None:
                cte_sql, metadata = self._build_seed_cte(self.initial_rows, self.initial_columns)
            elif node_type == 'source':
                cte_sql, metadata = self._build_source_cte(node)
            elif node_type == 'filter':
                cte_sql, metadata = self._build_filter_cte(node)
            elif node_type == 'join':
                cte_sql, metadata = self._build_join_cte(node)
            elif node_type == 'projection':
                cte_sql, metadata = self._build_projection_cte(node)
            elif node_type == 'aggregate':
                cte_sql, metadata = self._build_aggregate_cte(node)
            else:
                raise ValueError(f"Unsupported node type for SQL compilation: {node_type}")

            cte_name = self._get_cte_name(node_id)
            self.cte_map[node_id] = cte_name
            self.metadata_map[node_id] = metadata

            ctes.append(f"{cte_name} AS (\n    {cte_sql}\n)")
            last_cte_node_id = node_id

        # Debug: log each node's metadata (visible in Django logs when LOG_LEVEL=DEBUG)
        for nid in sql_nodes:
            node = self.node_map.get(nid)
            ntype = node.get('data', {}).get('type') if node else None
            meta = self.metadata_map.get(nid, {})
            cols = meta.get('columns', [])
            col_summary = [
                {"name": c.get('name'), "business_name": c.get('business_name'), "technical_name": c.get('technical_name')}
                for c in cols[:20]
            ]
            if len(cols) > 20:
                col_summary.append(f"... and {len(cols) - 20} more")
            logger.debug(
                "Node metadata | node_id=%s | type=%s | column_count=%s | columns=%s",
                nid,
                ntype,
                len(cols),
                col_summary,
            )

        # 4. Resolve final executable node for SELECT.
        # For targets downstream of a compute boundary, we want the LAST SQL node
        # that actually produced a CTE (not an upstream pushed-down filter or source),
        # so we use last_cte_node_id when target is not directly SQL-compilable.
        if self.target_node_id not in sql_nodes:
            last_compilable_node = sql_nodes[-1] if sql_nodes else None
            effective_node_id = last_cte_node_id or last_compilable_node
            if effective_node_id:
                if effective_node_id != last_compilable_node:
                    logger.warning(
                        "Target node %s is downstream of compute boundary. "
                        "Using last SQL CTE node %s (computed from DAG) as boundary instead of raw last_compilable_node %s.",
                        self.target_node_id,
                        effective_node_id,
                        last_compilable_node,
                    )
                else:
                    logger.warning(
                        "Target node %s is downstream of compute boundary. Using last SQL-compilable node %s as boundary.",
                        self.target_node_id,
                        effective_node_id,
                    )
                final_node_id = effective_node_id
            else:
                raise ValueError(
                    f"Cannot compile SQL: target node {self.target_node_id} is not SQL-compilable and no SQL-compilable nodes found."
                )
        else:
            final_node_id = self.target_node_id

        # When target is a pushed-down filter, its cte_map points to the pushdown target (e.g. source).
        # We must SELECT from the filter's *input* node's CTE so preview shows the filter's logical
        # output (same schema as input, rows already filtered by pushdown).
        if final_node_id in self.pushed_down_filters:
            input_node_id = self._get_input_node_id(final_node_id)
            if input_node_id and input_node_id in self.cte_map:
                logger.info(
                    "Pushed-down filter target %s: using filter input node %s for final SELECT",
                    final_node_id,
                    input_node_id,
                )
                final_node_id = input_node_id

        # Derive final_cte and final_metadata from final_node_id only (no fallback to sql_nodes[0] or source).
        if final_node_id not in self.pushed_down_filters:
            final_cte = self._get_cte_name(final_node_id)
            final_metadata = self.metadata_map.get(final_node_id)
            if not final_metadata:
                final_cte = self.cte_map.get(final_node_id, final_cte)
                final_metadata = self.metadata_map.get(final_node_id, {})
        else:
            final_cte = self.cte_map[final_node_id]
            final_metadata = self.metadata_map[final_node_id]

        # Build SELECT clause from metadata
        select_parts = []
        for col in final_metadata.get('columns', []):
            col_name = col.get('name')
            if col_name:
                select_parts.append(f'"{col_name}"')

        if not select_parts:
            # Fallback: SELECT *
            select_clause = "*"
        else:
            select_clause = ", ".join(select_parts)

        # Build complete query
        newline = '\n'
        if ctes:
            ctes_str = f',{newline}'.join(ctes)
            query = f"WITH {ctes_str}{newline}SELECT {select_clause}{newline}FROM {final_cte}{newline}LIMIT %s"
            self.params.append(50)  # Default page size
        else:
            # No CTEs (shouldn't happen, but handle gracefully)
            query = f"SELECT {select_clause}{newline}FROM {final_cte}{newline}LIMIT %s"
            self.params.append(50)

        logger.info("Final preview SELECT node: %s", final_node_id)
        # Debug: final node column metadata (field names; use business_name as display when set)
        final_cols = final_metadata.get('columns', [])
        logger.debug(
            "Final node column metadata | node_id=%s | columns=%s",
            final_node_id,
            [{"name": c.get('name'), "business_name": c.get('business_name')} for c in final_cols],
        )
        logger.info(f"Compiled SQL query:\n{query}")
        logger.info(f"Parameters: {self.params}")

        return query, self.params, final_metadata

    def _build_checkpoint_cte(
        self,
        table_ref: str,
        initial_columns: list[dict[str, Any]],
    ) -> tuple[str, dict]:
        """
        Build a CTE from a physical checkpoint table.
        Returns (cte_sql, output_metadata) compatible with downstream nodes.
        """
        if not initial_columns:
            # Fallback to SELECT * if no metadata provided
            return f"SELECT * FROM {table_ref}", {"columns": []}

        columns_meta = []
        for c in initial_columns:
            name = c.get("name") or c.get("technical_name")
            if not name:
                continue
            columns_meta.append({
                "name": name,
                "technical_name": c.get("technical_name", name),
                "business_name": c.get("business_name", name),
                "datatype": c.get("datatype", "TEXT"),
                "nullable": c.get("nullable", True),
                "source": c.get("source", "base"),
            })
        metadata = {"columns": columns_meta}

        # Build SELECT with specific columns to ensure schema alignment
        col_names = [f'"{c["technical_name"]}" AS "{c["name"]}"' for c in columns_meta]
        cte_sql = f"SELECT {', '.join(col_names)} FROM {table_ref}"

        # Track lineage: since this is a checkpoint, its columns originate from here in the current sub-graph
        for c in columns_meta:
            self._track_column_lineage(c["name"], self.start_node_id, 'CHECKPOINT')

        return cte_sql, metadata

    def _get_cte_name(self, node_id: str) -> str:
        """Generate CTE name for a node."""
        # Sanitize node_id for SQL identifier
        sanitized = node_id.replace('-', '_').replace('.', '_')[:50]
        return f"node_{sanitized}"

    def _build_seed_cte(
        self,
        initial_rows: list[dict[str, Any]],
        initial_columns: list[dict[str, Any]],
    ) -> tuple[str, dict]:
        """
        Build a CTE from cached rows (resume-from-cache). Uses VALUES with params.
        Returns (cte_sql, output_metadata) compatible with downstream nodes.
        """
        if not initial_columns:
            raise ValueError("initial_columns required for seed CTE")
        col_names = []
        for c in initial_columns:
            name = c.get("name") or c.get("technical_name")
            if not name:
                continue
            col_names.append(name)
        if not col_names:
            raise ValueError("No column names in initial_columns for seed CTE")
        # Build metadata in same shape as other nodes
        columns_meta = []
        for c in initial_columns:
            name = c.get("name") or c.get("technical_name")
            if not name:
                continue
            columns_meta.append({
                "name": name,
                "technical_name": c.get("technical_name", name),
                "business_name": c.get("business_name", name),
                "datatype": c.get("datatype", "TEXT"),
                "nullable": c.get("nullable", True),
                "source": c.get("source", "base"),
            })
        metadata = {"columns": columns_meta}
        if not initial_rows:
            # Empty cache: produce zero rows with correct columns (no params)
            cols_sql = ", ".join(f'"{n}"' for n in col_names)
            nulls = ", ".join("NULL::text" for _ in col_names)
            cte_sql = f"SELECT * FROM (VALUES ({nulls})) AS _seed({cols_sql}) WHERE 1=0"
            return cte_sql, metadata
        # VALUES row placeholders and params
        placeholders_per_row = ["%s"] * len(col_names)
        row_placeholders = "(" + ", ".join(placeholders_per_row) + ")"
        rows_sql = []
        for row in initial_rows:
            if isinstance(row, dict):
                values = [row.get(name) for name in col_names]
            else:
                values = list(row) if len(row) >= len(col_names) else list(row) + [None] * (len(col_names) - len(row))
            rows_sql.append(row_placeholders)
            self.params.extend(values)
        values_sql = ",\n    ".join(rows_sql)
        cols_sql = ", ".join(f'"{n}"' for n in col_names)
        cte_sql = f"SELECT * FROM (VALUES\n    {values_sql}\n) AS _seed({cols_sql})"
        return cte_sql, metadata

    def _build_source_cte(self, node: dict[str, Any]) -> tuple[str, dict]:
        """
        Build CTE for source node with filter pushdown optimization.
        Checks for filters that were pushed down to this source and includes them in WHERE clause.

        Returns:
            (cte_sql, output_metadata)
        """
        node_id = node['id']
        node_data = node.get('data', {})
        config = node_data.get('config', {})

        source_id = config.get('sourceId')
        table_name = config.get('tableName')
        schema = config.get('schema', 'public')

        if not source_id or not table_name:
            raise ValueError(f"Source node {node_id} missing sourceId or tableName")

        # VALIDATION: Prevent using metadata tables as data sources
        # These are system tables that should not be queried as data sources
        if schema and schema.upper() == 'GENERAL' and table_name in _METADATA_TABLES:
            raise ValueError(
                f"Cannot use metadata table '{schema}.{table_name}' as data source. "
                "Please select a valid data table from your source database."
            )

        # Get source config (cache it)
        if source_id not in self.source_configs:
            self.source_configs[source_id] = self._get_source_config(source_id)

        source_config = self.source_configs[source_id]

        # Use schema from source config if not specified in node
        if not schema:
            schema = source_config.get('schema', 'public')

        # Build table reference
        if schema:
            table_ref = f'"{schema}"."{table_name}"'
        else:
            table_ref = f'"{table_name}"'

        # Auto-generate prefix from source node id (unique per source on canvas)
        raw_id = (node.get('id') or '') if isinstance(node.get('id'), str) else str(node.get('id') or '')
        sanitized = ''.join(c if c.isalnum() or c == '_' else '_' for c in raw_id).strip('_') or 'src'
        prefix = (sanitized[:8] if len(sanitized) >= 8 else sanitized) or 'src'

        # Get metadata early so we know source column names for pushdown rewriting
        metadata = self._get_table_metadata(source_id, table_name, schema, prefix=prefix)
        source_column_names = {col.get('db_name') or col.get('name') for col in metadata.get('columns', []) if (col.get('db_name') or col.get('name'))}

        # Check for filters pushed down to this source
        pushed_down_conditions = []
        for filter_node_id, pushdown_info in self.filter_pushdown_info.items():
            targets = pushdown_info.get('pushdown_targets', [])
            if not targets and pushdown_info.get('pushdown_node_id') == node_id:
                pushed_down_conditions.extend(pushdown_info.get('conditions', []))
                logger.info(f"Including pushed-down filter conditions from {filter_node_id} in source {node_id}")
            else:
                for target in targets:
                    if target.get('pushdown_node_id') == node_id:
                        pushed_down_conditions.extend(target.get('conditions', []))
                        logger.info(f"Including pushed-down filter conditions from {filter_node_id} in source {node_id}")

        # Rewrite conditions: map technical_name (or _L_/_R_ or name) to db_name for extraction/source WHERE
        rewritten_conditions = []
        for cond in pushed_down_conditions:
            col = (cond.get('column') or '').strip()
            if not col:
                continue
            meta = next((m for m in metadata.get('columns', []) if (m.get('technical_name') == col or m.get('name') == col or m.get('db_name') == col)), None)
            if meta:
                db_col = meta.get('db_name') or meta.get('name')
            else:
                base_col = col[3:] if (col.startswith('_L_') or col.startswith('_R_')) else col
                if base_col.startswith(prefix + '_'):
                    db_col = base_col[len(prefix) + 1:]
                else:
                    db_col = base_col
            if db_col not in source_column_names:
                logger.warning(
                    "Column '%s' not found in source table %s.%s, skipping pushed-down condition",
                    col, schema or 'public', table_name
                )
                continue
            rewritten_conditions.append({**cond, 'column': db_col})

        pushed_down_conditions = rewritten_conditions

        # Build WHERE clause if we have pushed-down filters
        where_clause = None
        if pushed_down_conditions:
            filter_spec = parse_filter_from_canvas({'conditions': pushed_down_conditions})
            where_clause, where_params = build_sql_where_clause(filter_spec, table_alias='')

            if where_clause:
                self.params.extend(where_params)
                logger.info(f"Filter pushdown: Added WHERE clause to source {node_id}: {where_clause}")

        # Build CTE SQL with pushed-down filters
        if where_clause:
            cte_sql = f'SELECT * FROM {table_ref} WHERE {where_clause}'
        else:
            cte_sql = f'SELECT * FROM {table_ref}'

        # Track column lineage by technical name (source: DB column name = technical name)
        for col in metadata.get('columns', []):
            tech = col.get('technical_name') or col.get('name')
            if tech:
                self._track_column_lineage(tech, node_id, 'SOURCE')

        return cte_sql, metadata

    def _build_filter_cte_pass1(self, node: dict[str, Any]) -> tuple[str, dict]:
        """
        First pass: Build filter CTE without pushdown analysis.
        Used to establish column lineage before analyzing pushdown.
        """
        node_id = node['id']
        node_data = node.get('data', {})
        config = node_data.get('config', {})

        # Get input node
        input_node_id = self._get_input_node_id(node_id)
        if not input_node_id:
            raise ValueError(f"Filter node {node_id} has no input")

        input_cte = self.cte_map[input_node_id]
        input_metadata = self.metadata_map[input_node_id]

        # Get filter conditions
        conditions = config.get('conditions', [])
        if not conditions:
            # No filter - just pass through
            cte_sql = f'SELECT * FROM {input_cte}'
            return cte_sql, input_metadata

        # Apply filter locally (will be analyzed for pushdown in second pass)
        filter_spec = parse_filter_from_canvas({'conditions': conditions})
        where_clause, where_params = build_sql_where_clause(filter_spec, table_alias='')

        self.params.extend(where_params)

        if where_clause:
            cte_sql = f'SELECT * FROM {input_cte} WHERE {where_clause}'
        else:
            cte_sql = f'SELECT * FROM {input_cte}'

        # Metadata is same as input (filter doesn't change schema)
        return cte_sql, input_metadata

    def _build_filter_cte(self, node: dict[str, Any]) -> tuple[str, dict]:
        """
        Build CTE for filter node with lineage-based pushdown.

        CRITICAL RULE: Filter pushdown is based on column lineage, not node type.
        All columns in filter must be pushdown-safe, otherwise entire filter stays local.

        Returns:
            (cte_sql, output_metadata)
        """
        node_id = node['id']
        node_data = node.get('data', {})
        config = node_data.get('config', {})

        # Get input node
        input_node_id = self._get_input_node_id(node_id)
        if not input_node_id:
            raise ValueError(f"Filter node {node_id} has no input")

        input_cte = self.cte_map[input_node_id]
        input_metadata = self.metadata_map[input_node_id]

        # Get filter conditions
        conditions = config.get('conditions', [])
        if not conditions:
            # No filter - just pass through
            cte_sql = f'SELECT * FROM {input_cte}'
            # Track column lineage (pass through - columns keep their origins)
            for col in input_metadata.get('columns', []):
                col_name = col.get('name')
                if col_name:
                    origin = self._get_column_origin(col_name)
                    if origin:
                        # Keep existing lineage
                        pass  # Already tracked
            return cte_sql, input_metadata

        # LINEAGE-BASED PUSHDOWN ANALYSIS
        can_pushdown, pushdown_targets, unsafe_columns = self._analyze_filter_pushdown(node_id, conditions)

        if can_pushdown and pushdown_targets:
            # Filter can be pushed down - mark it for pushdown
            self.pushed_down_filters.add(node_id)
            self.filter_pushdown_info[node_id] = {'pushdown_targets': pushdown_targets}
            logger.info(f"Filter {node_id} is pushdown-eligible to {len(pushdown_targets)} target(s) - marking for pushdown")

            # If single target and that node already built, rebuild it with the filter
            if len(pushdown_targets) == 1 and pushdown_targets[0]['pushdown_node_id'] in self.cte_map:
                pushdown_node_id = pushdown_targets[0]['pushdown_node_id']
                pushdown_node = self.node_map[pushdown_node_id]
                pushdown_node_type = pushdown_node.get('data', {}).get('type')

                if pushdown_node_type == 'source':
                    # Rebuild source CTE with filter
                    cte_sql, metadata = self._build_source_cte(pushdown_node)
                    cte_name = self._get_cte_name(pushdown_node_id)
                    self.cte_map[pushdown_node_id] = cte_name
                    self.metadata_map[pushdown_node_id] = metadata
                    # Note: CTE will be rebuilt in next iteration, so we'll use the input CTE for now
                    input_cte = self.cte_map[pushdown_node_id]
                else:
                    # For non-source nodes, use input CTE (filter will be applied at pushdown node)
                    input_cte = self.cte_map[pushdown_node_id]
            else:
                # Pushdown node not built yet - use input CTE
                # Filter will be included when pushdown node is built
                input_cte = self.cte_map[input_node_id]

            # Filter was pushed down - output = pushdown node output (filter already applied there)
            cte_sql = f'SELECT * FROM {input_cte}'
        else:
            # Filter cannot be pushed down - apply locally
            logger.info(f"Filter {node_id} cannot be pushed down - unsafe columns: {unsafe_columns}")

            filter_spec = parse_filter_from_canvas({'conditions': conditions})
            where_clause, where_params = build_sql_where_clause(filter_spec, table_alias='')

            self.params.extend(where_params)

            if where_clause:
                cte_sql = f'SELECT * FROM {input_cte} WHERE {where_clause}'
            else:
                cte_sql = f'SELECT * FROM {input_cte}'

        # Track column lineage (filter doesn't change column origins)
        for col in input_metadata.get('columns', []):
            col_name = col.get('name')
            if col_name:
                origin = self._get_column_origin(col_name)
                if origin:
                    # Keep existing lineage
                    pass  # Already tracked

        # Metadata is same as input (filter doesn't change schema)
        return cte_sql, input_metadata

    def _build_join_cte(self, node: dict[str, Any]) -> tuple[str, dict]:
        """
        Build CTE for join node.

        Returns:
            (cte_sql, output_metadata)
        """
        node_id = node['id']
        node_data = node.get('data', {})
        config = node_data.get('config', {})

        # Get left and right input nodes
        input_edges = [e for e in self.edges if e.get('target') == node_id]
        left_edge = next((e for e in input_edges if e.get('targetHandle') == 'left'), None)
        right_edge = next((e for e in input_edges if e.get('targetHandle') == 'right'), None)

        if not left_edge or not right_edge:
            # Fallback: use first two edges
            if len(input_edges) >= 2:
                left_edge = input_edges[0]
                right_edge = input_edges[1]
            else:
                raise ValueError(f"Join node {node_id} requires two inputs")

        left_node_id = left_edge.get('source')
        right_node_id = right_edge.get('source')

        if left_node_id not in self.cte_map or right_node_id not in self.cte_map:
            raise ValueError(f"Join node {node_id} missing input CTEs")

        left_cte = self.cte_map[left_node_id]
        right_cte = self.cte_map[right_node_id]

        # Get join configuration
        join_type = config.get('joinType', 'INNER').upper()
        conditions = config.get('conditions', [])
        output_columns = config.get('outputColumns', [])

        # Build join conditions
        join_conditions = []
        for cond in conditions:
            left_col = cond.get('leftColumn', '')
            right_col = cond.get('rightColumn', '')
            operator = cond.get('operator', '=')

            if left_col and right_col:
                # Remove alias prefix if present (e.g., "__L__.column" -> "column")
                left_col_clean = left_col.replace('__L__.', '').replace('__L__."', '').replace('"', '')
                right_col_clean = right_col.replace('__R__.', '').replace('__R__."', '').replace('"', '')

                # Add table aliases
                left_ref = f'__L__."{left_col_clean}"'
                right_ref = f'__R__."{right_col_clean}"'

                join_conditions.append(f'{left_ref} {operator} {right_ref}')

        join_on = ' AND '.join(join_conditions) if join_conditions else '1=1'

        # Build SELECT clause with output columns
        # ENFORCE STRICT UNIQUENESS: All output column names must be unique
        resolved_names_map = {}  # Track resolved output names: (column_clean, source) -> final_output_name

        if output_columns:
            # Get metadata for both tables to validate column existence
            left_metadata = self.metadata_map.get(left_node_id, {})
            right_metadata = self.metadata_map.get(right_node_id, {})
            left_col_names = {col.get('name') for col in left_metadata.get('columns', [])}
            right_col_names = {col.get('name') for col in right_metadata.get('columns', [])}

            select_parts = []
            output_names_used = {}  # Track: output_name -> (table_alias, column_clean, source)

            for col_config in output_columns:
                if isinstance(col_config, dict):
                    included = col_config.get('included', True)
                    if not included:
                        continue

                    output_name = col_config.get('outputName') or col_config.get('output_name')
                    column = col_config.get('column') or col_config.get('column_name')
                    source = col_config.get('source', 'left')  # 'left' or 'right'

                    if not column:
                        continue

                    # Clean column name (remove alias prefix if present)
                    column_clean = column.replace('__L__.', '').replace('__R__.', '').replace('__L__."', '').replace('__R__."', '').replace('"', '')

                    # Validate column exists in specified source, fallback to other source if not
                    if source == 'left':
                        if column_clean in left_col_names:
                            table_alias = '__L__'
                            actual_source = 'left'
                        elif column_clean in right_col_names:
                            # Column doesn't exist in left, but exists in right - use right
                            table_alias = '__R__'
                            actual_source = 'right'
                            logger.warning(f"Column '{column_clean}' specified as 'left' but not found in left table, using right table instead")
                        else:
                            # Column not found in either table - skip it
                            logger.warning(f"Column '{column_clean}' not found in either left or right table, skipping")
                            continue
                    else:  # source == 'right'
                        if column_clean in right_col_names:
                            table_alias = '__R__'
                            actual_source = 'right'
                        elif column_clean in left_col_names:
                            # Column doesn't exist in right, but exists in left - use left
                            table_alias = '__L__'
                            actual_source = 'left'
                            logger.warning(f"Column '{column_clean}' specified as 'right' but not found in right table, using left table instead")
                        else:
                            # Column not found in either table - skip it
                            logger.warning(f"Column '{column_clean}' not found in either left or right table, skipping")
                            continue

                    # Determine final output name (use provided outputName, or default to column_clean)
                    final_output_name = output_name if output_name else column_clean

                    # UNIQUENESS ENFORCEMENT: Check for duplicate output names
                    if final_output_name in output_names_used:
                        # Conflict detected - auto-resolve with suffix
                        existing_info = output_names_used[final_output_name]
                        existing_source = existing_info[2]

                        # Resolve conflict: add _L_ or _R_ prefix (e.g. _L_cmp_id, _R_cmp_id)
                        if actual_source == 'left':
                            resolved_name = f"_L_{final_output_name}"
                        else:
                            resolved_name = f"_R_{final_output_name}"

                        # Also update the existing one if needed
                        if existing_source == 'left' and resolved_name.startswith('_R_'):
                            # Both are from different sources, update existing to have _L_
                            existing_resolved = f"_L_{final_output_name}"
                            # Update the existing entry in select_parts
                            for i, part in enumerate(select_parts):
                                if f'AS "{final_output_name}"' in part:
                                    select_parts[i] = part.replace(f'AS "{final_output_name}"', f'AS "{existing_resolved}"')
                                    break
                            output_names_used[existing_resolved] = output_names_used.pop(final_output_name)
                            output_names_used[resolved_name] = (table_alias, column_clean, actual_source)
                            final_output_name = resolved_name
                            logger.warning(f"Column name conflict resolved: '{column_clean}' from {actual_source} table aliased as '{resolved_name}'")
                        elif existing_source == 'right' and resolved_name.startswith('_L_'):
                            # Both are from different sources, update existing to have _R_
                            existing_resolved = f"_R_{final_output_name}"
                            # Update the existing entry in select_parts
                            for i, part in enumerate(select_parts):
                                if f'AS "{final_output_name}"' in part:
                                    select_parts[i] = part.replace(f'AS "{final_output_name}"', f'AS "{existing_resolved}"')
                                    break
                            output_names_used[existing_resolved] = output_names_used.pop(final_output_name)
                            output_names_used[resolved_name] = (table_alias, column_clean, actual_source)
                            final_output_name = resolved_name
                            logger.warning(f"Column name conflict resolved: '{column_clean}' from {actual_source} table aliased as '{resolved_name}'")
                        else:
                            # Same source conflict (shouldn't happen, but handle gracefully)
                            resolved_name = f"{'_L_' if actual_source == 'left' else '_R_'}{final_output_name}"
                            output_names_used[resolved_name] = (table_alias, column_clean, actual_source)
                            final_output_name = resolved_name
                            logger.warning(f"Column name conflict resolved: '{column_clean}' aliased as '{resolved_name}'")
                    else:
                        # No conflict - use as-is
                        output_names_used[final_output_name] = (table_alias, column_clean, actual_source)

                    # Build column reference with explicit alias (ALWAYS use alias)
                    col_ref = f'{table_alias}."{column_clean}"'
                    select_parts.append(f'{col_ref} AS "{final_output_name}"')
                else:
                    # Simple string column name - try to find it in either table
                    column_clean = str(col_config).replace('__L__.', '').replace('__R__.', '').replace('__L__."', '').replace('__R__."', '').replace('"', '')
                    final_output_name = column_clean

                    # Check for uniqueness
                    if final_output_name in output_names_used:
                        # Auto-resolve: determine which table it's from (_L_/_R_ prefix)
                        if column_clean in left_col_names:
                            final_output_name = f"_L_{column_clean}"
                            table_alias = '__L__'
                        elif column_clean in right_col_names:
                            final_output_name = f"_R_{column_clean}"
                            table_alias = '__R__'
                        else:
                            logger.warning(f"Column '{column_clean}' not found in either table, skipping")
                            continue
                        logger.warning(f"Column name conflict resolved: '{column_clean}' aliased as '{final_output_name}'")
                    else:
                        if column_clean in left_col_names:
                            table_alias = '__L__'
                        elif column_clean in right_col_names:
                            table_alias = '__R__'
                        else:
                            logger.warning(f"Column '{column_clean}' not found in either table, skipping")
                            continue
                        output_names_used[final_output_name] = (table_alias, column_clean, 'left' if table_alias == '__L__' else 'right')

                    # Always use explicit alias
                    select_parts.append(f'{table_alias}."{column_clean}" AS "{final_output_name}"')

            select_clause = ', '.join(select_parts) if select_parts else '*'
            # Store resolved output names mapping: (column_clean, source) -> final_output_name
            # This will be used for metadata building
            for final_name, (_table_alias, col_clean, src) in output_names_used.items():
                resolved_names_map[(col_clean, src)] = final_name
        else:
            # No outputColumns specified - select all columns from both tables with explicit aliases
            # ENFORCE UNIQUENESS: Auto-resolve conflicts with _l/_r suffixes
            left_metadata = self.metadata_map.get(left_node_id, {})
            right_metadata = self.metadata_map.get(right_node_id, {})

            select_parts = []
            output_names_used = set()  # Track used output names for uniqueness

            # Get all column names from both sides
            left_cols = {col.get('name') for col in left_metadata.get('columns', [])}
            right_cols = {col.get('name') for col in right_metadata.get('columns', [])}

            # Select columns from left table
            for col in left_metadata.get('columns', []):
                col_name = col.get('name')
                if col_name:
                    output_name = col_name
                    # Check if this name will conflict with right table
                    if col_name in right_cols:
                        # Conflict: use _L_ prefix (e.g. _L_cmp_id)
                        output_name = f"_L_{col_name}"
                        logger.info(f"Auto-resolving column name conflict: '{col_name}' from left table aliased as '{output_name}'")
                    output_names_used.add(output_name)
                    select_parts.append(f'__L__."{col_name}" AS "{output_name}"')

            # Select columns from right table
            for col in right_metadata.get('columns', []):
                col_name = col.get('name')
                if col_name:
                    if col_name in left_cols:
                        # Conflict: use _R_ prefix (e.g. _R_cmp_id)
                        output_name = f"_R_{col_name}"
                        logger.info(f"Auto-resolving column name conflict: '{col_name}' from right table aliased as '{output_name}'")
                    else:
                        # No conflict - use original name
                        output_name = col_name

                    # Double-check uniqueness (shouldn't happen, but be safe)
                    if output_name in output_names_used:
                        output_name = f"_R_{col_name}"
                        logger.warning(f"Additional conflict detected for '{col_name}', using '{output_name}'")

                    output_names_used.add(output_name)
                    select_parts.append(f'__R__."{col_name}" AS "{output_name}"')

            select_clause = ', '.join(select_parts) if select_parts else '*'

        # Build CTE SQL
        join_sql_map = {
            'INNER': 'INNER JOIN',
            'LEFT': 'LEFT JOIN',
            'RIGHT': 'RIGHT JOIN',
            'FULL': 'FULL OUTER JOIN',
            'FULL OUTER': 'FULL OUTER JOIN',
            'CROSS': 'CROSS JOIN'
        }

        join_keyword = join_sql_map.get(join_type, 'INNER JOIN')

        if join_type == 'CROSS':
            cte_sql = f'SELECT {select_clause}\n    FROM {left_cte} AS __L__\n    {join_keyword} {right_cte} AS __R__'
        else:
            cte_sql = f'SELECT {select_clause}\n    FROM {left_cte} AS __L__\n    {join_keyword} {right_cte} AS __R__\n    ON {join_on}'

        # Build output metadata from outputColumns
        # ENFORCE UNIQUENESS: Metadata must match the resolved output names from SELECT clause
        output_metadata_cols = []
        seen_output_names = set()  # Track for uniqueness validation

        if output_columns:
            # Get metadata for both tables (already retrieved above if output_columns exists)
            left_metadata = self.metadata_map.get(left_node_id, {})
            right_metadata = self.metadata_map.get(right_node_id, {})
            left_col_names = {col.get('name') for col in left_metadata.get('columns', [])}
            right_col_names = {col.get('name') for col in right_metadata.get('columns', [])}
            left_techs = {c.get('technical_name') or c.get('name') for c in left_metadata.get('columns', []) if (c.get('technical_name') or c.get('name'))}
            right_techs = {c.get('technical_name') or c.get('name') for c in right_metadata.get('columns', []) if (c.get('technical_name') or c.get('name'))}

            for col_config in output_columns:
                if isinstance(col_config, dict) and col_config.get('included', True):
                    provided_output_name = col_config.get('outputName') or col_config.get('output_name')
                    column = col_config.get('column') or col_config.get('column_name')
                    source = col_config.get('source', 'left')

                    if not column:
                        continue

                    # Clean column name
                    column_clean = column.replace('__L__.', '').replace('__R__.', '').replace('__L__."', '').replace('__R__."', '').replace('"', '')

                    # Determine which table actually has this column (same logic as SELECT clause)
                    if source == 'left':
                        if column_clean in left_col_names:
                            actual_source = 'left'
                            input_metadata = left_metadata
                        elif column_clean in right_col_names:
                            actual_source = 'right'
                            input_metadata = right_metadata
                        else:
                            # Column not found - skip
                            continue
                    else:  # source == 'right'
                        if column_clean in right_col_names:
                            actual_source = 'right'
                            input_metadata = right_metadata
                        elif column_clean in left_col_names:
                            actual_source = 'left'
                            input_metadata = left_metadata
                        else:
                            # Column not found - skip
                            continue

                    # Get the resolved output name (same logic as SELECT clause)
                    # Use the resolved name from the SELECT clause if available
                    final_output_name = None
                    if (column_clean, actual_source) in resolved_names_map:
                        final_output_name = resolved_names_map[(column_clean, actual_source)]
                    else:
                        # Fallback: determine output name (may need conflict resolution)
                        final_output_name = provided_output_name if provided_output_name else column_clean

                        # Check for conflicts and resolve
                        if final_output_name in seen_output_names:
                            # Conflict detected - resolve with _L_/_R_ prefix
                            if actual_source == 'left':
                                final_output_name = f"_L_{column_clean}"
                            else:
                                final_output_name = f"_R_{column_clean}"
                            logger.warning(f"Metadata: Column name conflict resolved: '{column_clean}' aliased as '{final_output_name}'")

                    # UNIQUENESS VALIDATION: Ensure output name is unique
                    if final_output_name in seen_output_names:
                        # This shouldn't happen if resolved_names_map is used, but be safe
                        logger.error(f"Metadata: Duplicate output name '{final_output_name}' detected, skipping")
                        continue

                    seen_output_names.add(final_output_name)

                    # Find column metadata from input; use technical_name as-is, append _L_/_R_ only on real conflict
                    col_meta = next((c for c in input_metadata.get('columns', []) if c.get('name') == column_clean), None)
                    tech = (col_meta.get('technical_name') or column_clean) if col_meta else column_clean
                    if actual_source == 'left' and tech in right_techs:
                        tech = f"_L_{tech}"
                    elif actual_source == 'right' and tech in left_techs:
                        tech = f"_R_{tech}"
                    join_entry = {
                        'name': final_output_name,
                        'business_name': final_output_name,
                        'technical_name': tech,
                        'datatype': col_meta.get('datatype', 'TEXT') if col_meta else 'TEXT',
                        'source': 'base',
                        'nullable': col_meta.get('nullable', True) if col_meta else True
                    }
                    if col_meta and col_meta.get('db_name') is not None:
                        join_entry['db_name'] = col_meta.get('db_name')
                    output_metadata_cols.append(join_entry)
        else:
            # No outputColumns specified - build metadata matching the resolved names from SELECT clause
            left_metadata = self.metadata_map.get(left_node_id, {})
            right_metadata = self.metadata_map.get(right_node_id, {})
            left_cols = {col.get('name') for col in left_metadata.get('columns', [])}
            right_cols = {col.get('name') for col in right_metadata.get('columns', [])}
            left_techs = {col.get('technical_name') or col.get('name') for col in left_metadata.get('columns', []) if (col.get('technical_name') or col.get('name'))}
            right_techs = {col.get('technical_name') or col.get('name') for col in right_metadata.get('columns', []) if (col.get('technical_name') or col.get('name'))}

            output_metadata_cols = []
            seen_names = set()

            # Add left table columns: technical_name as-is; only append _L_ on real conflict (same tech from right)
            for col in left_metadata.get('columns', []):
                col_name = col.get('name')
                if col_name:
                    output_name = col_name
                    if col_name in right_cols:
                        output_name = f"_L_{col_name}"
                    seen_names.add(output_name)
                    tech = col.get('technical_name') or col_name
                    out_tech = f"_L_{tech}" if tech in right_techs else tech
                    left_entry = {
                        'name': output_name,
                        'business_name': output_name,
                        'technical_name': out_tech,
                        'datatype': col.get('datatype', 'TEXT'),
                        'source': 'base',
                        'nullable': col.get('nullable', True)
                    }
                    if col.get('db_name') is not None:
                        left_entry['db_name'] = col.get('db_name')
                    output_metadata_cols.append(left_entry)

            # Add right table columns: technical_name as-is; only append _R_ on real conflict (same tech from left)
            for col in right_metadata.get('columns', []):
                col_name = col.get('name')
                if col_name:
                    if col_name in left_cols:
                        output_name = f"_R_{col_name}"
                    else:
                        output_name = col_name
                    if output_name in seen_names:
                        output_name = f"_R_{col_name}"
                    seen_names.add(output_name)
                    tech = col.get('technical_name') or col_name
                    out_tech = f"_R_{tech}" if tech in left_techs else tech
                    right_entry = {
                        'name': output_name,
                        'business_name': output_name,
                        'technical_name': out_tech,
                        'datatype': col.get('datatype', 'TEXT'),
                        'source': 'base',
                        'nullable': col.get('nullable', True)
                    }
                    if col.get('db_name') is not None:
                        right_entry['db_name'] = col.get('db_name')
                    output_metadata_cols.append(right_entry)

        # Final uniqueness validation
        final_names = [col.get('name') for col in output_metadata_cols]
        if len(final_names) != len(set(final_names)):
            duplicates = [name for name in final_names if final_names.count(name) > 1]
            logger.error(f"Join metadata contains duplicate column names: {duplicates}")
            # Remove duplicates (keep first occurrence)
            seen = set()
            unique_metadata = []
            for col in output_metadata_cols:
                col_name = col.get('name')
                if col_name not in seen:
                    seen.add(col_name)
                    unique_metadata.append(col)
                else:
                    logger.warning(f"Removing duplicate column '{col_name}' from metadata")
            output_metadata_cols = unique_metadata

        metadata = {'columns': output_metadata_cols}

        # Track column lineage for join output columns
        # Join columns inherit lineage from their source tables
        # For columns that come from left/right tables, they keep their original origin
        # We mark them as JOIN to indicate they passed through a join, but their actual origin
        # is from the source tables
        left_node_id = left_edge.get('source')
        right_node_id = right_edge.get('source')

        for col_meta in output_metadata_cols:
            col_name = col_meta.get('name')
            tech = col_meta.get('technical_name') or col_name
            if tech:
                # Check if column exists in left or right input metadata
                left_metadata = self.metadata_map.get(left_node_id, {})
                right_metadata = self.metadata_map.get(right_node_id, {})

                left_cols = [c.get('name') for c in left_metadata.get('columns', [])]
                right_cols = [c.get('name') for c in right_metadata.get('columns', [])]

                # If column exists in input, inherit its lineage and record which branch (left/right)
                # Otherwise, mark as JOIN origin. Key lineage by technical_name.
                if col_name in left_cols or col_name in right_cols:
                    origin = self._get_column_origin(tech)
                    if not origin:
                        in_left = col_name in left_cols
                        branch = 'left' if in_left else 'right'
                        self._track_column_lineage(tech, node_id, 'JOIN', origin_branch=branch)
                else:
                    self._track_column_lineage(tech, node_id, 'JOIN')

        return cte_sql, metadata

    def _build_projection_cte(self, node: dict[str, Any]) -> tuple[str, dict]:
        """
        Build CTE for projection node.

        Returns:
            (cte_sql, output_metadata)
        """
        node_id = node['id']
        node_data = node.get('data', {})
        config = node_data.get('config', {})

        # Get input node
        input_node_id = self._get_input_node_id(node_id)
        if not input_node_id:
            raise ValueError(f"Projection node {node_id} has no input")

        input_cte = self.cte_map[input_node_id]
        input_metadata = self.metadata_map[input_node_id]

        # Get available columns from input metadata (with full metadata including types)
        available_columns = [col.get('name') for col in input_metadata.get('columns', [])]
        available_columns_set = set(available_columns)  # For fast lookup
        # Build column metadata map for type checking
        column_metadata_map = {col.get('name'): col for col in input_metadata.get('columns', [])}

        # Get projection configuration from multiple sources
        # config.output_columns is updated on every toggle; projection.columns comes from columnOrder
        # (which may be stale if user toggled without full Save Projection)
        config_selected = (config.get('selectedColumns') or
                          config.get('output_columns') or
                          config.get('includedColumns') or
                          [])
        config_selected = list(config_selected) if config_selected else []

        projection_config = node_data.get('projection', {})
        proj_selected = []
        if projection_config and projection_config.get('columns'):
            projection_cols = projection_config.get('columns', [])
            if isinstance(projection_cols, list) and len(projection_cols) > 0:
                if isinstance(projection_cols[0], str):
                    proj_selected = projection_cols
                else:
                    proj_selected = [col.get('name') or col for col in projection_cols if col.get('included', True)]

        # Prefer config when it has more columns (user toggled new columns like upper_name)
        selected_columns = config_selected if len(config_selected) >= len(proj_selected) else proj_selected

        excluded_columns = config.get('excludedColumns', [])
        selected_mode = config.get('selectedMode') or projection_config.get('mode', 'INCLUDE')
        calculated_columns = config.get('calculatedColumns') or node_data.get('calculatedColumns', [])
        calculated_col_names = {cc.get('name') for cc in calculated_columns if cc.get('name')}

        # Determine which columns to select
        if selected_mode == 'EXCLUDE' or config.get('excludeMode'):
            # Exclude mode: select all except excluded
            columns_to_select = [col for col in available_columns if col not in excluded_columns]
        else:
            # Include mode: select only selected columns
            if selected_columns:
                # Resolve each entry (may be technical_name or name) to actual column name for SQL
                input_cols = input_metadata.get('columns', [])
                resolved_selected = []
                for col in selected_columns:
                    # Match by technical_name or name (rename-safe: frontend can persist technical_name)
                    meta = next((c for c in input_cols if (c.get('technical_name') or c.get('name')) == col or c.get('name') == col), None)
                    resolved = (meta.get('name') if meta else None) or col
                    resolved_selected.append(resolved)
                # Map selected columns to actual available columns
                # This handles cases where projection was configured before join conflict resolution
                mapped_columns = []
                for col in resolved_selected:
                    if col in available_columns_set:
                        mapped_columns.append(col)
                    elif col in calculated_col_names:
                        # Calculated column — skip base mapping; will be added as expression later
                        pass
                    else:
                        # Map base name (e.g. cmp_id) to _L_/_R_ prefixed names (e.g. _L_cmp_id, _R_cmp_id)
                        # Backward compat: old suffix (cmp_id_l, cmp_id_r) -> new prefix (_L_cmp_id, _R_cmp_id)
                        if col.endswith('_l') and len(col) > 2:
                            col_l, col_r = f"_L_{col[:-2]}", None
                        elif col.endswith('_r') and len(col) > 2:
                            col_l, col_r = None, f"_R_{col[:-2]}"
                        else:
                            col_l, col_r = f"_L_{col}", f"_R_{col}"
                        if col_l and col_r and col_l in available_columns_set and col_r in available_columns_set:
                            mapped_columns.append(col_l)
                            mapped_columns.append(col_r)
                            logger.info(f"Projection: Column '{col}' mapped to both '{col_l}' and '{col_r}'")
                        elif col_l in available_columns_set:
                            mapped_columns.append(col_l)
                            logger.info(f"Projection: Column '{col}' mapped to '{col_l}'")
                        elif col_r in available_columns_set:
                            mapped_columns.append(col_r)
                            logger.info(f"Projection: Column '{col}' mapped to '{col_r}'")
                        else:
                            logger.warning(f"Projection: Column '{col}' not found in available columns, skipping")
                if mapped_columns:
                    columns_to_select = mapped_columns
                else:
                    logger.warning("Projection: No valid columns after mapping, using all available columns")
                    columns_to_select = available_columns
            else:
                columns_to_select = available_columns

        # Deduplicate columns while preserving order (avoids "column reference is ambiguous" when
        # config has both "cmp_id" and "_L_cmp_id"/"_R_cmp_id", or join output lists same column twice)
        _seen = set()
        columns_to_select = [c for c in columns_to_select if c not in _seen and not _seen.add(c)]

        # Remove calculated columns from base columns (they'll be added as expressions)
        base_columns = [col for col in columns_to_select if col not in calculated_col_names]

        # Final validation: ensure all base columns exist in available columns
        # This is a safety check - should not be needed if mapping worked correctly
        validated_base_columns = [col for col in base_columns if col in available_columns_set]
        if len(validated_base_columns) < len(base_columns):
            missing = set(base_columns) - set(validated_base_columns)
            logger.warning(f"Projection: Some columns not found in input, skipping: {missing}")

        # CRITICAL: Also validate that calculated column names don't conflict with base columns
        # If a calculated column name matches an available column, it will be treated as base column
        # This prevents selecting non-existent columns
        validated_calculated_cols = []
        for calc_col in calculated_columns:
            calc_name = calc_col.get('name')
            if calc_name and calc_name not in available_columns_set:
                # Calculated column name doesn't exist as base column - safe to add
                validated_calculated_cols.append(calc_col)
            elif calc_name in available_columns_set:
                # Calculated column name conflicts with base column - skip calculated, use base
                logger.warning(f"Projection: Calculated column '{calc_name}' conflicts with base column, using base column")

        # If no validated columns, fall back to all available (except calculated)
        if not validated_base_columns:
            logger.warning("Projection: No valid columns after validation, using all available columns")
            validated_base_columns = [col for col in available_columns if col not in calculated_col_names]

        # Build SELECT clause
        select_parts = []

        # Add base columns - DOUBLE CHECK: ensure they exist in available columns
        for col in validated_base_columns:
            if col in available_columns_set:
                select_parts.append(f'"{col}"')
            else:
                logger.error(f"Projection: CRITICAL - Column '{col}' passed validation but not in available_columns_set, skipping")

        # Add calculated columns as SQL expressions
        # Pass column metadata map for type validation
        translator = ExpressionTranslator(available_columns, self.db_type, column_metadata_map)
        successfully_translated_calc_cols = []  # Track which calculated columns were successfully translated
        for calc_col in validated_calculated_cols:
            calc_name = calc_col.get('name')
            calc_expression = calc_col.get('expression', '')

            if not calc_name or not calc_expression:
                continue

            # Double-check: don't add if it's already in base columns
            if calc_name in validated_base_columns:
                logger.warning(f"Projection: Skipping calculated column '{calc_name}' - already exists as base column")
                continue

            try:
                sql_expression = translator.translate(calc_expression)
                select_parts.append(f'{sql_expression} AS "{calc_name}"')
                # Only add to successfully_translated list if translation succeeded
                successfully_translated_calc_cols.append(calc_col)
            except Exception as e:
                logger.warning(f"Failed to translate calculated column {calc_name}: {e}")
                # Skip this calculated column - don't add to successfully_translated_calc_cols
                continue

        if not select_parts:
            select_clause = "*"
        else:
            select_clause = ", ".join(select_parts)

        cte_sql = f'SELECT {select_clause} FROM {input_cte}'

        # Build output metadata
        output_metadata_cols = []

        # Add base columns (use validated_base_columns); include technical_name and db_name for lineage/refetch
        for col in validated_base_columns:
            col_meta = next((c for c in input_metadata.get('columns', []) if c.get('name') == col), None)
            tech = (col_meta.get('technical_name') or col) if col_meta else col
            entry = {
                'name': col,
                'business_name': col,
                'technical_name': tech,
                'datatype': col_meta.get('datatype', 'TEXT') if col_meta else 'TEXT',
                'source': 'base',
                'nullable': col_meta.get('nullable', True) if col_meta else True
            }
            if col_meta and col_meta.get('db_name') is not None:
                entry['db_name'] = col_meta.get('db_name')
            output_metadata_cols.append(entry)

        # Add calculated columns with technical_name (display name = name; technical = stable id)
        for calc_col in successfully_translated_calc_cols:
            calc_name = calc_col.get('name')
            if calc_name:
                calc_tech = calc_col.get('technical_name') or calc_name
                output_metadata_cols.append({
                    'name': calc_name,
                    'business_name': calc_name,
                    'technical_name': calc_tech,
                    'datatype': calc_col.get('dataType', 'STRING'),
                    'source': 'calculated',
                    'expression': calc_col.get('expression', ''),
                    'nullable': True
                })

        metadata = {'columns': output_metadata_cols}

        # Base columns: lineage already from input (keyed by technical_name there)
        # Calculated columns: track by technical_name
        for calc_col in successfully_translated_calc_cols:
            calc_name = calc_col.get('name')
            calc_tech = calc_col.get('technical_name') or calc_name
            calc_expression = calc_col.get('expression', '')
            if calc_tech:
                self._track_column_lineage(calc_tech, node_id, 'PROJECTION', calc_expression)

        return cte_sql, metadata

    def _build_aggregate_cte(self, node: dict[str, Any]) -> tuple[str, dict]:
        """
        Build CTE for aggregate node.

        Returns:
            (cte_sql, output_metadata)
        """
        node_id = node['id']
        node_data = node.get('data', {})
        config = node_data.get('config', {})

        # Get input node
        input_node_id = self._get_input_node_id(node_id)
        if not input_node_id:
            raise ValueError(f"Aggregate node {node_id} has no input")

        input_cte = self.cte_map[input_node_id]

        # Get aggregate configuration
        aggregate_columns = config.get('aggregateColumns', [])
        group_by_columns = config.get('groupByColumns', [])
        selected_columns = config.get('selectedColumns', [])

        input_metadata = self.metadata_map[input_node_id]

        if not aggregate_columns:
            # No aggregates - just pass through
            cte_sql = f'SELECT * FROM {input_cte}'
            return cte_sql, input_metadata

        # Build SELECT and GROUP BY clauses
        all_group_by_cols = set()
        if selected_columns:
            for col in selected_columns:
                all_group_by_cols.add(col)
        elif group_by_columns:
            for col in group_by_columns:
                all_group_by_cols.add(col)
        else:
            for agg in aggregate_columns:
                if 'groupBy' in agg and isinstance(agg['groupBy'], list):
                    for gb in agg['groupBy']:
                        all_group_by_cols.add(gb)

        # Only keep group by columns that exist in the input node's output
        input_col_names = {c.get('name') for c in input_metadata.get('columns', [])}
        input_col_technical_names = {c.get('technical_name') for c in input_metadata.get('columns', [])}

        valid_group_by_fields = []
        for gb_col in all_group_by_cols:
            if gb_col in input_col_names or gb_col in input_col_technical_names:
                valid_group_by_fields.append(gb_col)
            else:
                logger.warning(f"Dropping missing group_by column '{gb_col}' in aggregate {node_id}")

        group_by_fields = valid_group_by_fields
        select_fields = []

        # Add GROUP BY columns to SELECT
        for gb_col in group_by_fields:
            select_fields.append(f'"{gb_col}"')

        # Add Aggregate columns to SELECT
        for agg_col in aggregate_columns:
            func = agg_col.get('function', '').upper()
            col = agg_col.get('column', '')
            alias = agg_col.get('alias', '')

            if func == 'COUNT_DISTINCT':
                select_fields.append(f'COUNT(DISTINCT "{col}") AS "{alias}"')
            elif func == 'COUNT' and not col:
                select_fields.append(f'COUNT(*) AS "{alias}"')
            else:
                select_fields.append(f'{func}("{col}") AS "{alias}"')

        select_clause = ', '.join(select_fields) if select_fields else '*'

        if group_by_fields:
            group_by_clause = f'GROUP BY {", ".join([f"{chr(34)}{gb}{chr(34)}" for gb in group_by_fields])}'
            cte_sql = f'SELECT {select_clause} FROM {input_cte} {group_by_clause}'
        else:
            cte_sql = f'SELECT {select_clause} FROM {input_cte}'

        # Build output metadata with technical_name for rename-safe lineage
        output_metadata_cols = []

        # 1. Group by columns
        for gb_col in group_by_fields:
            output_metadata_cols.append({
                'name': gb_col,
                'business_name': gb_col,
                'technical_name': gb_col,
                'datatype': 'TEXT',
                'source': 'base',
                'nullable': True
            })

        # 2. Track aggregated columns lineage by technical_name (alias = output/technical name)
        for agg_col in aggregate_columns:
            alias = agg_col.get('alias', '')
            if not alias:
                continue
            func = agg_col.get('function', '').upper()
            col = agg_col.get('column', '')

            output_metadata_cols.append({
                'name': alias,
                'business_name': alias,
                'technical_name': alias,
                'datatype': 'INTEGER' if func in ('COUNT', 'COUNT_DISTINCT') else 'NUMERIC',
                'source': 'aggregate',
                'nullable': True
            })

            expression = f'{func}({col})' if col else f'{func}(*)'
            self._track_column_lineage(alias, node_id, 'AGGREGATE', expression)

        metadata = {'columns': output_metadata_cols}

        return cte_sql, metadata

    def _get_input_node_id(self, node_id: str) -> Optional[str]:
        """Get input node ID for a node."""
        input_edge = next((e for e in self.edges if e.get('target') == node_id), None)
        return input_edge.get('source') if input_edge else None

    def _resolve_lineage_key(self, column_name: str) -> Optional[str]:
        """
        Resolve a column identifier (name or technical_name) to the lineage key (technical_name).
        Lineage is keyed by technical_name for rename-safety; lookup supports either name.
        """
        direct = self.column_lineage.get(column_name)
        if direct is not None:
            return column_name
        for meta in self.metadata_map.values():
            for col in meta.get('columns', []):
                if col.get('technical_name') == column_name or col.get('name') == column_name:
                    return col.get('technical_name') or col.get('name')
        return None

    def _track_column_lineage(
        self,
        technical_name: str,
        node_id: str,
        origin_type: str,
        expression: Optional[str] = None,
        origin_branch: Optional[str] = None
    ):
        """
        Track column lineage by technical name (rename-safe).

        Args:
            technical_name: Stable column identifier (key for lineage)
            node_id: Node ID where column is created/output
            origin_type: 'SOURCE', 'JOIN', 'PROJECTION', 'COMPUTE', 'AGGREGATE'
            expression: Expression if calculated (None for base columns)
            origin_branch: For JOIN columns, 'left' or 'right' (which side of the join)
        """
        entry = {
            'origin_node_id': node_id,
            'origin_type': origin_type,
            'expression': expression
        }
        if origin_branch is not None:
            entry['origin_branch'] = origin_branch
        self.column_lineage[technical_name] = entry
        logger.debug(
            f"Column lineage: {technical_name} -> {origin_type}@{node_id}"
            + (f" (expr: {expression})" if expression else "")
            + (f" branch={origin_branch}" if origin_branch else "")
        )

    def _get_column_origin(self, column_name: str) -> Optional[dict[str, Any]]:
        """
        Get column origin information. Accepts name or technical_name (resolve via _resolve_lineage_key).

        Returns:
            {'origin_node_id': str, 'origin_type': str, 'expression': str|None} or None if not found
        """
        key = self._resolve_lineage_key(column_name)
        return self.column_lineage.get(key) if key else self.column_lineage.get(column_name)

    def _can_pushdown_filter_column(self, column_name: str) -> tuple[bool, Optional[str]]:
        """
        Check if a filter column can be pushed down based on its origin.

        CRITICAL RULE: Pushdown is based on column origin, not node type.
        - SOURCE/JOIN/PROJECTION columns: Can push down (column exists upstream)
        - COMPUTE/AGGREGATE columns: Cannot push down (column created in these nodes)

        Returns:
            (can_pushdown: bool, earliest_pushdown_node_id: str|None)
        """
        origin = self._get_column_origin(column_name)

        if not origin:
            # Column not found in lineage - check if it exists in input metadata
            # This handles cases where lineage hasn't been tracked yet
            input_node_id = self._get_input_node_id(self.node_map.get(column_name, {}).get('id', ''))
            if input_node_id and input_node_id in self.metadata_map:
                input_metadata = self.metadata_map[input_node_id]
                input_cols = [c.get('name') for c in input_metadata.get('columns', [])]
                if column_name in input_cols:
                    # Column exists in input - can push down
                    # Find earliest source node
                    return True, self._find_earliest_source_node(input_node_id)
            logger.warning(f"Column {column_name} not found in lineage, cannot determine pushdown eligibility")
            return False, None

        origin_type = origin['origin_type']
        origin_node_id = origin['origin_node_id']

        # Pushdown rules based on origin type
        if origin_type == 'SOURCE':
            # Column from source - push to source itself
            return True, origin_node_id
        elif origin_type == 'JOIN':
            # Column from join - can push down to correct branch
            return True, self._find_earliest_source_node(origin_node_id)
        elif origin_type == 'PROJECTION':
            # Pass-through columns can push down; calculated columns (with expression) cannot
            expression = origin.get('expression')
            if expression is not None and str(expression).strip():
                # Simple pass-through: expression is exactly the column name (identity)
                if str(expression).strip() != column_name:
                    return False, None  # calculated column - does not exist upstream
            return True, self._find_earliest_source_node(origin_node_id)
        elif origin_type in ['COMPUTE', 'AGGREGATE']:
            # Column created in Compute/Aggregate - cannot push down
            # These columns don't exist upstream
            return False, None

        return False, None

    def _is_downstream(self, node_id: str, ancestor_node_id: str) -> bool:
        """
        Check if node_id is downstream of ancestor_node_id in the DAG.

        Args:
            node_id: Node to check if it's downstream
            ancestor_node_id: Potential ancestor node

        Returns:
            True if node_id is downstream of ancestor_node_id
        """
        if node_id == ancestor_node_id:
            return False

        # Build forward adjacency (source -> targets)
        forward_adjacency = {}
        for edge in self.edges:
            source_id = edge.get('source')
            target_id = edge.get('target')
            if source_id not in forward_adjacency:
                forward_adjacency[source_id] = []
            forward_adjacency[source_id].append(target_id)

        # BFS from ancestor_node_id to see if we can reach node_id
        from collections import deque
        queue = deque([ancestor_node_id])
        visited = set()

        while queue:
            current_id = queue.popleft()

            if current_id == node_id:
                return True

            if current_id in visited:
                continue
            visited.add(current_id)

            # Check downstream nodes
            if current_id in forward_adjacency:
                for downstream_id in forward_adjacency[current_id]:
                    if downstream_id not in visited:
                        queue.append(downstream_id)

        return False

    def _find_earliest_source_node(self, node_id: str) -> Optional[str]:
        """
        Find the earliest source node upstream from a given node.
        Used to determine where filters can be pushed down.
        """
        visited = set()

        def dfs(current_id: str) -> Optional[str]:
            if current_id in visited:
                return None
            visited.add(current_id)

            node = self.node_map.get(current_id)
            if not node:
                return None

            node_type = node.get('data', {}).get('type')

            # If this is a source, return it
            if node_type == 'source':
                return current_id

            # Otherwise, check upstream nodes
            input_node_id = self._get_input_node_id(current_id)
            if input_node_id:
                result = dfs(input_node_id)
                if result:
                    return result

            return None

        return dfs(node_id)

    def _get_join_left_right_node_ids(self, join_node_id: str) -> tuple[Optional[str], Optional[str]]:
        """Get left and right input node IDs for a join node from edges."""
        input_edges = [e for e in self.edges if e.get('target') == join_node_id]
        left_edge = next((e for e in input_edges if e.get('targetHandle') == 'left'), None)
        right_edge = next((e for e in input_edges if e.get('targetHandle') == 'right'), None)
        if not left_edge and not right_edge and len(input_edges) >= 2:
            left_edge, right_edge = input_edges[0], input_edges[1]
        left_node_id = left_edge.get('source') if left_edge else None
        right_node_id = right_edge.get('source') if right_edge else None
        return left_node_id, right_node_id

    def _get_table_label_for_node(self, node_id: str) -> Optional[str]:
        """
        Resolve the source table label (schema.table) for a node.
        For source nodes: returns schema.table from config.
        For other nodes: traces back to earliest source and returns that table label.
        """
        node = self.node_map.get(node_id)
        if not node:
            return None
        node_type = node.get('data', {}).get('type')
        if node_type == 'source':
            config = node.get('data', {}).get('config', {})
            schema = config.get('schema') or 'public'
            table_name = config.get('tableName') or ''
            if not table_name:
                return None
            return f'{schema}.{table_name}' if schema else table_name
        source_node_id = self._find_earliest_source_node(node_id)
        if source_node_id:
            return self._get_table_label_for_node(source_node_id)
        return None

    def get_column_lineage_for_display(self) -> dict[str, dict[str, Any]]:
        """
        Build display-ready column lineage for the target node's output columns.
        For each column returns: origin_type, origin_node_id, origin_branch (for JOIN),
        source_table (resolved table for this column), source_table_left/right (for JOIN context),
        is_calculated (True when created as calculated in projection), expression (if calculated).
        """
        result = {}
        target_metadata = self.metadata_map.get(self.target_node_id)
        if not target_metadata:
            return result
        output_columns = target_metadata.get('columns', [])
        for col in output_columns:
            col_name = col.get('name')
            if not col_name:
                continue
            origin = self._get_column_origin(col.get('technical_name') or col_name)
            if not origin:
                result[col_name] = {
                    'origin_type': 'unknown',
                    'source_table': None,
                    'origin_branch': None,
                    'is_calculated': False,
                    'expression': None,
                }
                continue
            origin_type = origin.get('origin_type', '')
            origin_node_id = origin.get('origin_node_id', '')
            origin_branch = origin.get('origin_branch')
            expression = origin.get('expression')
            is_calculated = (
                origin_type == 'PROJECTION'
                and expression is not None
                and str(expression).strip()
                and str(expression).strip() != col_name
            )
            source_table = None
            source_table_left = None
            source_table_right = None
            if origin_type == 'SOURCE':
                source_table = self._get_table_label_for_node(origin_node_id)
            elif origin_type == 'JOIN':
                left_id, right_id = self._get_join_left_right_node_ids(origin_node_id)
                if left_id:
                    source_table_left = self._get_table_label_for_node(
                        self._find_earliest_source_node(left_id) or left_id
                    )
                if right_id:
                    source_table_right = self._get_table_label_for_node(
                        self._find_earliest_source_node(right_id) or right_id
                    )
                source_table = source_table_left if origin_branch == 'left' else source_table_right
            elif origin_type == 'PROJECTION':
                source_table = self._get_table_label_for_node(origin_node_id)
            elif origin_type in ('AGGREGATE', 'COMPUTE'):
                source_table = None
            result[col_name] = {
                'origin_type': origin_type,
                'origin_node_id': origin_node_id,
                'origin_branch': origin_branch,
                'source_table': source_table,
                'source_table_left': source_table_left,
                'source_table_right': source_table_right,
                'is_calculated': is_calculated,
                'expression': expression,
            }
        return result

    def _analyze_filter_pushdown(
        self, filter_node_id: str, conditions: list[dict[str, Any]]
    ) -> tuple[bool, list[dict[str, Any]], list[str]]:
        """
        Analyze if a filter can be pushed down based on column lineage.

        CRITICAL RULE: All columns must be pushdown-safe, otherwise entire filter stays local.
        When join columns have origin_branch, partition conditions by left/right and push to correct source.

        Returns:
            (can_pushdown: bool, pushdown_targets: [{'pushdown_node_id': str, 'conditions': list[Dict]}, ...], unsafe_columns: list[str])
        """
        unsafe_columns = []
        pushdown_node_ids = []
        column_branches = {}  # column_name -> 'left' | 'right' (only for JOIN columns with branch)

        # Extract column names from conditions
        filter_columns = set()
        for condition in conditions:
            column = condition.get('column')
            if column:
                filter_columns.add(column)

        # Check each column and collect pushdown nodes and branches
        for column_name in filter_columns:
            can_push, push_node_id = self._can_pushdown_filter_column(column_name)
            if not can_push:
                unsafe_columns.append(column_name)
            else:
                if push_node_id:
                    pushdown_node_ids.append(push_node_id)
                origin = self._get_column_origin(column_name)
                if origin and origin.get('origin_branch') in ('left', 'right'):
                    column_branches[column_name] = origin['origin_branch']

        # ALL columns must be pushdown-safe (non-negotiable rule)
        can_pushdown = len(unsafe_columns) == 0
        if not can_pushdown:
            logger.info(f"Filter {filter_node_id} cannot be pushed down - unsafe columns: {unsafe_columns}")
            return False, [], unsafe_columns

        pushdown_targets = []

        if column_branches:
            # Partition conditions by origin_branch and push to left/right source
            join_node_id = None
            for col in filter_columns:
                origin = self._get_column_origin(col)
                if origin and origin.get('origin_type') == 'JOIN' and origin.get('origin_branch'):
                    join_node_id = origin['origin_node_id']
                    break
            if join_node_id:
                left_input_id, right_input_id = self._get_join_left_right_node_ids(join_node_id)
                left_source_id = self._find_earliest_source_node(left_input_id) if left_input_id else None
                right_source_id = self._find_earliest_source_node(right_input_id) if right_input_id else None
                left_conditions = [c for c in conditions if c.get('column') and column_branches.get(c.get('column')) == 'left']
                right_conditions = [c for c in conditions if c.get('column') and column_branches.get(c.get('column')) == 'right']
                if left_conditions and left_source_id:
                    pushdown_targets.append({'pushdown_node_id': left_source_id, 'conditions': left_conditions})
                if right_conditions and right_source_id:
                    pushdown_targets.append({'pushdown_node_id': right_source_id, 'conditions': right_conditions})

        if not pushdown_targets and pushdown_node_ids:
            # Single-branch: one target with all conditions
            source_nodes = [nid for nid in pushdown_node_ids
                           if self.node_map.get(nid, {}).get('data', {}).get('type') == 'source']
            earliest = source_nodes[0] if source_nodes else pushdown_node_ids[0]
            pushdown_targets = [{'pushdown_node_id': earliest, 'conditions': conditions}]

        if can_pushdown and pushdown_targets:
            logger.info(f"Filter {filter_node_id} pushdown targets: {[t['pushdown_node_id'] for t in pushdown_targets]}")

        return can_pushdown, pushdown_targets, unsafe_columns

    def _get_source_config(self, source_id: int) -> dict[str, Any]:
        """Get and decrypt source configuration."""
        # IMPORTANT: Source connection configs are stored centrally in the main app DB
        # (GENERAL.source), not in each customer's database. Reuse the shared
        # default-DB connection helper so we don't duplicate settings logic.
        from api.utils.db_connection import get_default_db_connection

        conn = get_default_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        try:
            # Get column names from GENERAL.source table
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'GENERAL' AND table_name = 'source'
            """)
            available_columns = [row[0] for row in cursor.fetchall()]

            # Determine which column names exist (handle different schema versions)
            name_column = 'source_name' if 'source_name' in available_columns else 'src_name'
            config_column = 'source_config' if 'source_config' in available_columns else 'src_config'

            # Validate that required columns exist
            if name_column not in available_columns:
                raise ValueError(f"Source table missing name column. Available columns: {available_columns}")
            if config_column not in available_columns:
                raise ValueError(f"Source table missing config column. Available columns: {available_columns}")
            if 'created_on' not in available_columns:
                raise ValueError(f"Source table missing created_on column. Available columns: {available_columns}")

            # Quote identifiers and SELECT only columns that actually exist
            name_column_sql = f'"{name_column}"'
            config_column_sql = f'"{config_column}"'

            cursor.execute(
                f'''
                SELECT {name_column_sql}, {config_column_sql}, created_on
                FROM "GENERAL".source
                WHERE id = %s
                ''',
                (source_id,),
            )

            row = cursor.fetchone()
            if not row:
                raise ValueError(f"Source {source_id} not found")

            source_name, source_config_encrypted, source_created_on = row
            source_config = decrypt_source_data(source_config_encrypted, self.customer.cust_id, source_created_on)

            if not source_config:
                raise ValueError(f"Failed to decrypt source {source_id}")

            return source_config

        finally:
            cursor.close()
            conn.close()

    def _connect_to_source_postgres(self, source_config: dict[str, Any], max_retries: int = 3):
        """Connect to source PostgreSQL with retries and timeouts for flaky networks."""
        import time

        import psycopg2
        host = source_config.get('hostname') or source_config.get('host')
        port = source_config.get('port') or 5432
        user = source_config.get('user') or source_config.get('username')
        password = source_config.get('password')
        database = source_config.get('database') or source_config.get('dbname')
        connect_timeout = int(source_config.get('connect_timeout', 15))
        last_err = None
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(
                    host=host,
                    port=int(port) if port else 5432,
                    user=user or '',
                    password=password or '',
                    dbname=database or '',
                    connect_timeout=connect_timeout,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                )
                conn.autocommit = True
                return conn
            except Exception as e:
                last_err = e
                logger.warning(
                    "Source DB connection attempt %s/%s failed: %s",
                    attempt + 1, max_retries, e,
                )
                if attempt < max_retries - 1:
                    time.sleep(2)
        if last_err:
            raise last_err

    def _get_table_metadata(self, source_id: int, table_name: str, schema: str, prefix: Optional[str] = None) -> dict:
        """Get table metadata (column names and types).
        prefix: optional; when set, technical_name = prefix_colname for uniqueness across sources.
        db_name: actual DB column name (used for fetch and filter pushdown).
        """
        source_config = self.source_configs.get(source_id)
        if not source_config:
            source_config = self._get_source_config(source_id)

        # Connect to source database
        db_type = source_config.get('db_type', 'postgresql').lower()

        if db_type == 'postgresql':
            conn = self._connect_to_source_postgres(source_config)
            cursor = conn.cursor()

            try:
                # Get column metadata
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema or 'public', table_name))

                columns = []
                for row in cursor.fetchall():
                    col_name, data_type, is_nullable = row
                    tech = f"{prefix}_{col_name}" if (prefix and prefix.strip()) else col_name
                    columns.append({
                        'name': col_name,
                        'technical_name': tech,
                        'db_name': col_name,
                        'datatype': data_type.upper(),
                        'source': 'base',
                        'nullable': is_nullable == 'YES'
                    })

                return {'columns': columns}

            finally:
                cursor.close()
                conn.close()
        else:
            # For other DB types, return basic metadata
            # Full implementation would query appropriate system tables
            return {'columns': []}
