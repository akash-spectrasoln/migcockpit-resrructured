"""
Migration Orchestrator
Builds and executes migration pipelines.
Supports parallel execution: nodes in the same "level" (same wave of dependencies)
run in parallel until a join or downstream step synchronizes them.
"""

import asyncio
import logging
from typing import Any, Awaitable, Callable, Optional

import httpx
from loaders.postgres_loader import PostgresLoader

from utils import remap_rows_to_business_names

try:
    from loaders.hana_loader import HanaLoader
except ImportError:
    HanaLoader = None  # type: ignore[misc, assignment]

logger = logging.getLogger(__name__)

# Prefixes for join output so destination has all columns from both sides (no overwrite)
def _join_in_memory(
    left_data: list[dict[str, Any]],
    right_data: list[dict[str, Any]],
    join_type: str,
    conditions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Perform in-memory join of two lists of dicts. Conditions are list of
    { leftColumn, rightColumn, operator } (operator '=' supported).

    IMPORTANT COLUMN NAMING RULES (to match SQL compiler + metadata so destination uses business names):
    - Base column names are preserved where possible.
    - When BOTH left and right have a column with the same name, we suffix:
        left column  -> `<name>_l`
        right column -> `<name>_r`
      so both are kept and names stay consistent with join metadata.
    """
    # Compute key sets once so we can detect conflicts
    left_keys: set = set()
    right_keys: set = set()
    for lr in left_data or []:
        left_keys.update(lr.keys())
    for rr in right_data or []:
        right_keys.update(rr.keys())
    conflicting_keys = left_keys & right_keys

    def rename_left_row(row: dict[str, Any]) -> dict[str, Any]:
        """Rename left row keys with _l suffix only when there is a conflict with right keys."""
        out: dict[str, Any] = {}
        for k, v in row.items():
            out_key = f"{k}_l" if k in conflicting_keys else k
            out[out_key] = v
        return out

    def rename_right_row(row: dict[str, Any]) -> dict[str, Any]:
        """Rename right row keys with _r suffix only when there is a conflict with left keys."""
        out: dict[str, Any] = {}
        for k, v in row.items():
            out_key = f"{k}_r" if k in conflicting_keys or k in out else k
            out[out_key] = v
        return out
    if not conditions:
        # CROSS join: every left row with every right row (can be huge)
        if join_type.upper() == "CROSS":
            out: list[dict[str, Any]] = []
            for lr in left_data:
                left_row = rename_left_row(lr)
                for rr in right_data:
                    right_row = rename_right_row(rr)
                    out.append({**left_row, **right_row})
            return out
        return []

    left_cols = [c.get("leftColumn") or c.get("left_column") for c in conditions if (c.get("leftColumn") or c.get("left_column"))]
    right_cols = [c.get("rightColumn") or c.get("right_column") for c in conditions if (c.get("rightColumn") or c.get("right_column"))]
    if len(left_cols) != len(right_cols) or not left_cols:
        logger.warning("Join conditions missing or mismatched left/right columns; returning empty.")
        return []

    def key_from_row(row: dict[str, Any], cols: list[str]) -> tuple:
        return tuple(row.get(c) for c in cols)

    # Index right by join key (support 1:n by storing list of rows per key)
    right_index: dict[tuple, list[dict[str, Any]]] = {}
    for rr in right_data:
        k = key_from_row(rr, right_cols)
        if k not in right_index:
            right_index[k] = []
        right_index[k].append(rr)

    out = []
    join_upper = (join_type or "INNER").upper()
    matched_right_keys: set = set()
    for lr in left_data:
        lk = key_from_row(lr, left_cols)
        matches = right_index.get(lk, [])
        left_row = rename_left_row(lr)
        if matches:
            for rr in matches:
                matched_right_keys.add(key_from_row(rr, right_cols))
                right_row = rename_right_row(rr)
                out.append({**left_row, **right_row})
        elif join_upper in ("LEFT", "LEFT OUTER"):
            # LEFT OUTER: unmatched right side is NULL for all right keys (renamed consistently)
            template_right = rename_right_row(right_data[0]) if right_data else {}
            null_right = {k: None for k in template_right.keys()}
            out.append({**left_row, **null_right})
    if join_upper in ("RIGHT", "RIGHT OUTER"):
        # RIGHT OUTER: unmatched left side is NULL for all left keys (renamed consistently)
        template_left = rename_left_row(left_data[0]) if left_data else {}
        null_left = {k: None for k in template_left.keys()}
        for rr in right_data:
            if key_from_row(rr, right_cols) not in matched_right_keys:
                right_row = rename_right_row(rr)
                out.append({**null_left, **right_row})
    return out

class MigrationOrchestrator:
    def __init__(self, extraction_service_url: str):
        self.extraction_service_url = extraction_service_url
        self.client = httpx.AsyncClient(timeout=300.0)

    def build_pipeline(self, nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Build migration pipeline from canvas nodes and edges
        Returns pipeline structure with execution order
        """
        # Build node map
        node_map = {node["id"]: node for node in nodes}

        # Build adjacency list
        adjacency = {}
        for edge in edges:
            source = edge["source"]
            target = edge["target"]
            if source not in adjacency:
                adjacency[source] = []
            adjacency[source].append(target)

        # Find source nodes (nodes with no incoming edges)
        source_nodes = [
            node_id for node_id in node_map.keys()
            if not any(edge["target"] == node_id for edge in edges)
        ]

        # Build execution order using topological sort (for backward compatibility)
        execution_order = self._topological_sort(node_map, adjacency)
        # Build execution levels: each level is a list of node_ids that can run in parallel
        execution_levels = self._execution_levels(node_map, adjacency)

        return {
            "nodes": node_map,
            "edges": edges,
            "execution_order": execution_order,
            "execution_levels": execution_levels,
            "source_nodes": source_nodes
        }

    def _execution_levels(self, node_map: dict, adjacency: dict) -> list[list[str]]:
        """
        Group nodes into levels (waves). Nodes in the same level have no dependency
        on each other and can run in parallel. Level 0 = all sources; level 1 = nodes
        that only depend on level 0; join nodes appear in a level only after both
        inputs are ready.
        """
        in_degree = {node_id: 0 for node_id in node_map.keys()}
        for _source, targets in adjacency.items():
            for target in targets:
                in_degree[target] = in_degree.get(target, 0) + 1

        levels = []
        # Copy so we can mutate
        degree = dict(in_degree)

        while True:
            # All nodes that are ready (no pending dependencies)
            level = [nid for nid, d in degree.items() if d == 0]
            if not level:
                break
            levels.append(level)
            for node_id in level:
                degree[node_id] = -1  # mark processed
                for target in adjacency.get(node_id, []):
                    if degree.get(target, 0) > 0:
                        degree[target] -= 1

        return levels

    def _topological_sort(self, node_map: dict, adjacency: dict) -> list[str]:
        """Topological sort to determine execution order"""
        in_degree = {node_id: 0 for node_id in node_map.keys()}

        # Calculate in-degrees
        for _source, targets in adjacency.items():
            for target in targets:
                in_degree[target] = in_degree.get(target, 0) + 1

        # Kahn's algorithm
        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        result = []

        while queue:
            node_id = queue.pop(0)
            result.append(node_id)

            if node_id in adjacency:
                for neighbor in adjacency[node_id]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)

        return result

    async def execute_pipeline(
        self,
        pipeline: dict[str, Any],
        config: dict[str, Any],
        progress_callback: Optional[Callable[[str, float], None]] = None,
        on_node_start: Optional[Callable[[str], Awaitable[None]]] = None,
        on_node_complete: Optional[Callable[[str, dict[str, Any]], Awaitable[None]]] = None,
    ) -> dict[str, Any]:
        """
        Execute migration pipeline. Nodes in the same dependency level (e.g.
        multiple sources, or parallel branches until a join) run in parallel.
        on_node_start(node_id) is called when a node is about to run.
        on_node_complete(node_id, result) is called when a node has finished.
        """
        nodes = pipeline["nodes"]
        edges = pipeline.get("edges", [])
        # Inject pipeline structure so _execute_source_node can resolve pushable filters (source columns only)
        config = dict(config or {})
        config["_nodes"] = nodes
        config["_edges"] = edges

        execution_levels = pipeline.get("execution_levels")
        if not execution_levels:
            # Fallback: treat execution_order as one node per level
            execution_order = pipeline.get("execution_order", [])
            execution_levels = [[nid] for nid in execution_order]

        total_steps = sum(len(level) for level in execution_levels)
        results = {}
        step_done = 0

        async def _call_progress(step_msg: str, pct: float, **kwargs):
            if progress_callback is None:
                return
            try:
                result = progress_callback(step_msg, pct, **kwargs)
                if asyncio.iscoroutine(result):
                    await result
            except TypeError:
                progress_callback(step_msg, pct)

        for level_idx, level in enumerate(execution_levels):
            step_done_at_level_start = step_done
            # Report progress at start of level: use steps completed so far (not level index)
            # so monitor % matches actual work done and graph can show correct running node(s)
            await _call_progress(
                f"Level {level_idx + 1}/{len(execution_levels)}: running {len(level)} node(s) in parallel",
                (step_done / total_steps) * 100.0 if total_steps else 0.0,
                level_index=level_idx + 1,
                total_levels=len(execution_levels),
                level_status="running",
            )

            # Notify that these nodes are starting (only these should show as "running" on the graph)
            if on_node_start:
                for node_id in level:
                    try:
                        if asyncio.iscoroutinefunction(on_node_start):
                            await on_node_start(node_id)
                        else:
                            on_node_start(node_id)
                    except Exception as e:
                        logger.warning("on_node_start callback error for %s: %s", node_id, e)

            # Run all nodes in this level in parallel
            tasks = [
                self._execute_single_node(node_id, nodes[node_id], nodes, results, config, edges)
                for node_id in level
            ]
            level_results = await asyncio.gather(*tasks, return_exceptions=True)

            for node_id, r in zip(level, level_results):
                step_done += 1
                if isinstance(r, Exception):
                    logger.exception(f"Node {node_id} failed: {r}")
                    results[node_id] = {
                        "success": False,
                        "node_id": node_id,
                        "error": str(r)
                    }
                else:
                    results[node_id] = r

                # Notify that this node has completed (so UI shows tickmark and only current level stays yellow)
                if on_node_complete:
                    try:
                        res = results[node_id]
                        if asyncio.iscoroutinefunction(on_node_complete):
                            await on_node_complete(node_id, res)
                        else:
                            on_node_complete(node_id, res)
                    except Exception as e:
                        logger.warning("on_node_complete callback error for %s: %s", node_id, e)

                # Update overall progress after each node so step message matches graph (X of Y complete)
                if total_steps:
                    pct = (step_done / total_steps) * 100.0
                    completed_in_level = step_done - step_done_at_level_start
                    level_status = "complete" if completed_in_level >= len(level) else "running"
                    if completed_in_level < len(level):
                        step_msg = f"Level {level_idx + 1}/{len(execution_levels)}: {completed_in_level} of {len(level)} node(s) complete"
                    else:
                        step_msg = f"Level {level_idx + 1} complete"
                    await _call_progress(step_msg, pct, level_index=level_idx + 1, total_levels=len(execution_levels), level_status=level_status)

            if step_done < total_steps and (step_done - step_done_at_level_start) < len(level):
                # Level had no nodes or we did not report "Level X complete" in the loop (edge case)
                pct = (step_done / total_steps) * 100.0
                await _call_progress(f"Level {level_idx + 1} complete", pct, level_index=level_idx + 1, total_levels=len(execution_levels), level_status="complete")

        await _call_progress("Data fully loaded to destination", 100.0, level_index=len(execution_levels), total_levels=len(execution_levels), level_status="complete")

        return {
            "total_nodes": total_steps,
            "successful_nodes": len([r for r in results.values() if r.get("success")]),
            "results": results
        }

    async def _execute_single_node(
        self,
        node_id: str,
        node: dict[str, Any],
        nodes: dict[str, Any],
        results: dict[str, Any],
        config: dict[str, Any],
        edges: Optional[list[dict[str, Any]]] = None
    ) -> dict[str, Any]:
        """Execute one node (used for parallel execution within a level)."""
        node_type = (node.get("type") or node.get("data", {}).get("type") or "").lower().strip()
        # Map canvas node types to execution handlers
        if node_type == "source":
            return await self._execute_source_node(node, config)
        if node_type in ("transform", "projection", "join", "compute", "filter"):
            return await self._execute_transform_node(node, results, config)
        if node_type in ("destination", "destination-postgresql", "destination-postgres"):
            return await self._execute_destination_node(node, results, config)
        logger.warning(f"Unknown node type: {node_type!r} for node {node_id}")
        return {"success": False, "node_id": node_id, "error": f"Unknown type: {node_type}"}

    def _get_pushable_filter_for_source(self, source_node: dict[str, Any], config: dict[str, Any]) -> Optional[dict[str, Any]]:
        """
        Return a filter spec to push to extraction only when the filter uses source table columns
        (not created/calculated columns). We only push when the direct downstream of this source
        is a filter node (no projection/calculated in between), so all filter columns are source columns.
        If the direct child is projection, calculated, or join, we do not push.
        """
        edges = config.get("_edges") or []
        nodes = config.get("_nodes") or {}
        source_id = source_node.get("id")
        if not source_id:
            return None
        # Direct children of this source
        direct_targets = [e["target"] for e in edges if e.get("source") == source_id]
        if not direct_targets:
            return None
        # Only push when there is exactly one direct child and it is a filter node
        if len(direct_targets) != 1:
            return None
        child_id = direct_targets[0]
        child = nodes.get(child_id, {})
        child_data = child.get("data", {}) or {}
        child_type = (child.get("type") or child_data.get("type") or "").lower().strip()
        if child_type != "filter":
            return None
        # Get filter conditions (same format as extraction execute_filter: list or dict)
        conditions = child_data.get("config", {}).get("conditions") or child_data.get("conditions") or []
        if not conditions:
            return None
        # Build filter spec: extraction service expects list of {column, operator, value} or dict with expressions
        if isinstance(conditions, list) and len(conditions) > 0:
            return {"conditions": conditions}
        if isinstance(conditions, dict):
            return conditions
        return None

    async def _execute_source_node(self, node: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """
        Execute source node - extract data from the Extraction Service.
        connection_config must be provided (e.g. via source_configs from Django).

        Data flow: We currently fetch the FULL table (no filter pushdown). The Extraction
        Service supports optional where_clause; to push filters we would need to:
        1) Resolve the filter from downstream filter node(s) that only use source columns,
        2) Pass where_clause (or filter spec) in extraction_request.
        Filters that use calculated columns cannot be pushed (must apply after fetch).
        See docs/EXECUTION_DATA_FLOW.md.
        """
        try:
            node_data = node.get("data", {}) or {}
            # connection_config must be provided via config.source_configs[node_id] (e.g. from Django when starting migration)
            source_configs = (config or {}).get("source_configs") or {}
            connection_config = source_configs.get(node.get("id"), {}).get("connection_config")

            table_name = (node_data.get("tableName") or node_data.get("config", {}).get("tableName") or "").strip()
            if not table_name:
                return {
                    "success": False,
                    "node_id": node.get("id"),
                    "error": "Source node missing table_name. Configure the source node with a table."
                }
            if not connection_config:
                return {
                    "success": False,
                    "node_id": node.get("id"),
                    "error": "Source connection_config is required (host, port, database, username, password). "
                            "Ensure the migration is started with source_configs so the extraction service can connect."
                }
            source_type = (node_data.get("connectionType") or node_data.get("config", {}).get("connectionType") or "postgresql").lower()
            extraction_request = {
                "source_type": source_type,
                "connection_config": connection_config,
                "table_name": table_name,
                "schema_name": node_data.get("schema") or node_data.get("config", {}).get("schema"),
                "chunk_size": config.get("chunk_size", 10000),
            }
            # Approach B: push filter to extraction only when filter uses source table columns (not created/calculated)
            pushable_filter = self._get_pushable_filter_for_source(node, config)
            if pushable_filter:
                extraction_request["filter_spec"] = pushable_filter
                logger.info(f"Pushing filter to extraction for source {node.get('id')} (source columns only)")
            response = await self.client.post(
                f"{self.extraction_service_url}/extract",
                json=extraction_request
            )
            if response.status_code >= 400:
                try:
                    body = response.json()
                    detail = body.get("detail", body.get("error", response.text))
                except Exception:
                    detail = response.text or f"HTTP {response.status_code}"
                raise RuntimeError(f"Extraction service error: {detail}")
            response.raise_for_status()

            extraction_result = response.json()
            job_id = extraction_result["job_id"]

            data = await self._wait_for_extraction(job_id)

            return {
                "success": True,
                "node_id": node.get("id"),
                "data": data,
                "rows_extracted": len(data) if isinstance(data, list) else 0
            }

        except Exception as e:
            logger.error(f"Error executing source node: {e}")
            return {
                "success": False,
                "node_id": node.get("id"),
                "error": str(e)
            }

    async def _execute_transform_node(
        self,
        node: dict[str, Any],
        previous_results: dict[str, Any],
        config: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Execute transform node. For join nodes: merge all direct upstream data with join conditions
        so the destination gets all columns from both sides (prefixed left_ / right_).
        Other transform types (projection, compute, filter) pass through first upstream.
        """
        try:
            node_type = (node.get("type") or node.get("data", {}).get("type") or "").lower().strip()
            node_id = node.get("id")

            if node_type == "join":
                upstream_list = self._get_all_upstream_data(node, previous_results, config)
                if len(upstream_list) < 2:
                    logger.warning(f"Join node {node_id} has {len(upstream_list)} upstream; need 2. Passing through first.")
                    input_data = upstream_list[0][1] if upstream_list else []
                else:
                    left_id, left_data = upstream_list[0]
                    right_id, right_data = upstream_list[1]
                    node_config = (node.get("data") or {}).get("config") or {}
                    join_type = node_config.get("joinType") or "INNER"
                    raw_conditions = node_config.get("conditions") or []
                    conditions = []
                    for c in raw_conditions:
                        if isinstance(c, dict):
                            lc = c.get("leftColumn") or c.get("left_column")
                            rc = c.get("rightColumn") or c.get("right_column")
                            if lc and rc:
                                conditions.append({"leftColumn": lc, "rightColumn": rc, "operator": c.get("operator", "=")})
                    input_data = _join_in_memory(left_data, right_data, join_type, conditions)
                    logger.info(
                        "Join node %s: merged left(%s)=%s rows, right(%s)=%s rows -> %s rows (columns prefixed left_ / right_)",
                        node_id, left_id, len(left_data), right_id, len(right_data), len(input_data),
                    )
                return {
                    "success": True,
                    "node_id": node_id,
                    "data": input_data,
                    "stats": {"rows_transformed": len(input_data) if isinstance(input_data, list) else 0},
                }
            # Pass-through for projection, compute, filter
            input_data = self._get_input_data(node, previous_results, config)
            logger.debug("Transform node %s (%s) pass-through: %s rows", node_id, node_type, len(input_data) if input_data else 0)
            return {
                "success": True,
                "node_id": node_id,
                "data": input_data,
                "stats": {"rows_transformed": len(input_data) if isinstance(input_data, list) else 0},
            }
        except Exception as e:
            logger.error(f"Error executing transform node: {e}")
            return {
                "success": False,
                "node_id": node.get("id"),
                "error": str(e)
            }

    async def _execute_destination_node(
        self,
        node: dict[str, Any],
        previous_results: dict[str, Any],
        config: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute destination node - load data to HANA/PostgreSQL. Uses config.destination_configs if present."""
        try:
            # Get data from previous nodes (use edges so we get data from direct upstream)
            input_data = self._get_input_data(node, previous_results, config)
            edges = (config or {}).get("_edges") or []
            dest_node_id = node.get("id")
            upstream_id = None
            for e in edges:
                if e.get("target") == dest_node_id:
                    upstream_id = e.get("source")
                    break
            # Remap row keys from technical_name/db_name to business_name ONLY at destination boundary.
            # Internal computation uses technical names; destination persistence uses business names.
            node_output_metadata = (config or {}).get("node_output_metadata") or {}
            output_metadata = node_output_metadata.get(upstream_id) if upstream_id else None
            if output_metadata and output_metadata.get("columns") and input_data:
                try:
                    remapped = remap_rows_to_business_names(
                        input_data,
                        output_metadata["columns"],
                    )
                    if remapped and len(remapped[0].keys()) > 0:
                        input_data = remapped
                        logger.info(
                            "Destination row keys after remap: %s",
                            list(input_data[0].keys()) if input_data else [],
                        )
                        logger.debug(
                            "Destination node %s: remapped %s rows to business names (technical_name/db_name -> business_name)",
                            dest_node_id,
                            len(input_data),
                        )
                    else:
                        logger.warning(
                            "Destination node %s: remap would produce empty row keys (upstream keys don't match metadata); keeping original column names",
                            dest_node_id,
                        )
                except Exception as remap_err:
                    logger.warning(
                        "Destination node %s: remap failed (%s); keeping original column names",
                        dest_node_id,
                        remap_err,
                    )
            data_count = len(input_data) if input_data else 0
            if data_count == 0:
                upstream_ids = [e.get("source") for e in edges if e.get("target") == dest_node_id]
                print(f"[DESTINATION] No data to load (0 rows). Upstream node ids from edges: {upstream_ids}; previous_results keys: {list(previous_results.keys())}")
                logger.warning(
                    "Destination node %s: 0 rows to load. Upstream from edges: %s; results keys: %s",
                    dest_node_id, upstream_ids, list(previous_results.keys()),
                )

            node_data = node.get("data", {})
            node_config = node_data.get("config") or {}
            node_id = dest_node_id
            destination_id = node_data.get("destinationId")
            destination_configs = (config or {}).get("destination_configs") or {}
            dest_entry = destination_configs.get(node_id) or destination_configs.get(str(destination_id)) or {}
            dest_config = dest_entry.get("connection_config") or {}
            if not dest_config and destination_id:
                dest_entry = destination_configs.get(str(destination_id)) or {}
                dest_config = dest_entry.get("connection_config") or {}

            db_type = (dest_entry.get("db_type") or "").lower().strip()
            if not db_type:
                node_type = (node_data.get("type") or node.get("type") or "").lower().strip()
                db_type = "postgresql" if node_type in ("destination-postgresql", "destination-postgres") else "hana"

            port = dest_config.get("port")
            try:
                port_int = int(port) if port is not None else None
            except (TypeError, ValueError):
                port_int = None
            if port_int == 5432 and db_type not in ("postgresql", "postgres"):
                logger.warning(
                    "Destination port is 5432 (PostgreSQL) but db_type was %s; using PostgresLoader to avoid connection errors.",
                    db_type or "empty",
                )
                db_type = "postgresql"

            schema = node_config.get("schema") or dest_config.get("schema") or ""
            load_mode = node_config.get("loadMode") or "insert"
            create_if_not_exists = load_mode == "insert" or load_mode == "drop_and_reload"
            drop_and_reload = load_mode == "drop_and_reload"

            table_name = node_config.get("tableName") or node_data.get("tableName") or ""
            if not (table_name and table_name.strip()):
                print(f"[DESTINATION] WARNING: tableName is empty for node {node_id}. Set table name in destination node config (Insert mode).")

            print(f"[DESTINATION] node_id={node_id} | data_rows={data_count} | table={table_name!r} | schema={schema!r} | create_if_not_exists={create_if_not_exists} | db_type={db_type}")
            logger.info(
                "Destination node %s: %s rows to load, table=%s, schema=%s, create_if_not_exists=%s, db_type=%s",
                node_id, data_count, table_name, schema, create_if_not_exists, db_type,
            )

            if db_type in ("postgresql", "postgres"):
                loader = PostgresLoader()
                print(f"[DESTINATION] Using PostgresLoader for table {table_name!r}")
            else:
                if HanaLoader is None:
                    raise RuntimeError(
                        "Destination db_type is %s but HanaLoader is not available (hana_loader module not installed). "
                        "Use a PostgreSQL destination or install the HANA loader." % db_type
                    )
                loader = HanaLoader()
                print(f"[DESTINATION] Using HanaLoader for table {table_name!r}")

            # Pass column_metadata so loader creates table from business names only (never technical keys)
            column_metadata = output_metadata.get("columns") if output_metadata else None
            logger.info("Rows BEFORE loader (after remap): %s", list(input_data[0].keys()) if input_data else [])
            logger.info("Metadata passed to loader: %s", [c.get("business_name") or c.get("name") for c in (column_metadata or [])[:15]])
            result = await loader.load_data(
                data=input_data,
                destination_config=dest_config,
                table_name=table_name,
                schema=schema,
                create_if_not_exists=create_if_not_exists,
                drop_and_reload=drop_and_reload,
                column_metadata=column_metadata,
            )

            rows_loaded = result.get("rows_loaded", 0)
            print(f"[DESTINATION] Load complete: {rows_loaded} rows inserted into destination table {result.get('table_name', table_name)!r}")
            logger.info("Destination node %s: loaded %s rows to %s", node_id, rows_loaded, result.get("table_name", table_name))

            return {
                "success": True,
                "node_id": node.get("id"),
                "rows_loaded": rows_loaded,
            }

        except Exception as e:
            print(f"[DESTINATION] ERROR: {e}")
            logger.error("Error executing destination node: %s", e)
            return {
                "success": False,
                "node_id": node.get("id"),
                "error": str(e)
            }

    def _get_all_upstream_data(
        self,
        node: dict[str, Any],
        previous_results: dict[str, Any],
        config: Optional[dict[str, Any]] = None,
    ) -> list[tuple]:
        """
        Get data from all direct upstream nodes. Returns list of (upstream_node_id, data).
        For join nodes, edges may have targetHandle 'left' / 'right'; order is [left, right] when present.
        """
        node_id = node.get("id")
        edges = (config or {}).get("_edges") or []
        if not node_id or not edges:
            return []
        input_edges = [e for e in edges if e.get("target") == node_id]
        if not input_edges:
            return []
        # Order: left then right if targetHandle present; else by edge order
        left_e = next((e for e in input_edges if e.get("targetHandle") == "left"), None)
        right_e = next((e for e in input_edges if e.get("targetHandle") == "right"), None)
        if left_e and right_e:
            ordered = [left_e, right_e]
        else:
            ordered = input_edges[:2]
        out = []
        for e in ordered:
            uid = e.get("source")
            if not uid:
                continue
            result = previous_results.get(uid)
            if result and result.get("success") and "data" in result:
                data = result.get("data")
                out.append((uid, data if isinstance(data, list) else []))
        return out

    def _get_input_data(
        self,
        node: dict[str, Any],
        previous_results: dict[str, Any],
        config: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """Get input data from previous nodes. Prefer direct upstream node(s) when edges are in config."""
        node_id = node.get("id")
        edges = (config or {}).get("_edges") or []
        if node_id and edges:
            upstream_ids = [e["source"] for e in edges if e.get("target") == node_id]
            for uid in upstream_ids:
                result = previous_results.get(uid)
                if result and result.get("success") and "data" in result:
                    return result.get("data") if isinstance(result.get("data"), list) else []
        for result in previous_results.values():
            if result.get("success") and "data" in result:
                return result.get("data") if isinstance(result.get("data"), list) else []
        return []

    async def _wait_for_extraction(self, job_id: str, max_wait: int = 300) -> list[dict[str, Any]]:
        """Wait for extraction job to complete, then fetch and return extracted data."""
        import asyncio

        elapsed = 0
        while elapsed < max_wait:
            response = await self.client.get(
                f"{self.extraction_service_url}/extract/{job_id}/status"
            )
            response.raise_for_status()
            status = response.json()

            if status["status"] == "completed":
                data_response = await self.client.get(
                    f"{self.extraction_service_url}/extract/{job_id}/data"
                )
                data_response.raise_for_status()
                payload = data_response.json()
                data = payload.get("data", [])
                logger.info("Extraction job %s completed: fetched %s rows for pipeline", job_id, len(data))
                return data if isinstance(data, list) else []
            elif status["status"] == "failed":
                raise Exception(f"Extraction failed: {status.get('error')}")

            await asyncio.sleep(2)
            elapsed += 2

        raise Exception("Extraction timeout")
