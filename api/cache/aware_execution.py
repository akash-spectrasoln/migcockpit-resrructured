# Moved from: api/utils/cache_aware_execution.py
"""
Cache-aware execution utilities for resuming pipeline execution from nearest upstream cache.
"""
from collections import deque
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

def find_nearest_upstream_cache(
    target_node_id: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    cached_node_ids: set[str]
) -> Optional[str]:
    """
    Find the nearest cached node upstream from target node.

    Uses BFS to find the closest cached node, ensuring minimal recomputation.

    Args:
        target_node_id: Target node ID
        nodes: All nodes in pipeline
        edges: All edges in pipeline
        cached_node_ids: Set of cached node IDs

    Returns:
        Nearest cached node ID, or None if no cache found upstream
    """
    node_map = {n['id']: n for n in nodes}

    # Build reverse adjacency (target -> sources)
    reverse_adjacency: dict[str, list[str]] = {}
    for edge in edges:
        source_id = edge.get('source')
        target_id = edge.get('target')
        if source_id is not None and target_id is not None:
            if target_id not in reverse_adjacency:
                reverse_adjacency[target_id] = []
            reverse_adjacency[target_id].append(str(source_id))

    # BFS to find nearest cached node
    queue: deque = deque([(target_node_id, 0)])
    visited = set()

    while queue:
        current_id, depth = queue.popleft()

        if current_id in visited:
            continue
        visited.add(current_id)

        # Check if this node is cached
        if current_id in cached_node_ids:
            logger.info(f"Found nearest upstream cache: {current_id} at depth {depth} from {target_node_id}")
            return current_id

        # Check upstream nodes
        if str(current_id) in reverse_adjacency:
            for upstream_id in reverse_adjacency.get(str(current_id), []):
                if upstream_id in node_map:
                    queue.append((str(upstream_id), depth + 1))

    logger.info(f"No upstream cache found for target {target_node_id}")
    return None

def get_execution_path_from_cache(
    cache_node_id: str,
    target_node_id: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]]
) -> list[str]:
    """
    Get the path of nodes that need to be executed from cache to target.

    Args:
        cache_node_id: Starting node (has cache)
        target_node_id: Target node
        nodes: All nodes in pipeline
        edges: All edges in pipeline

    Returns:
        List of node IDs in execution order (cache_node_id ... target_node_id)
    """
    node_map = {n['id']: n for n in nodes}

    # Build forward adjacency
    adjacency: dict[str, list[str]] = {}
    for edge in edges:
        source_id = edge.get('source')
        target_id = edge.get('target')
        if source_id is not None and target_id is not None:
            if source_id not in adjacency:
                adjacency[source_id] = []
            adjacency[source_id].append(str(target_id))

    # BFS from cache node to target
    logger.info(f"[EXECUTION PATH] Building execution path from cache node {cache_node_id} to target {target_node_id}")

    queue: deque = deque([(cache_node_id, [cache_node_id])])
    visited = set()

    while queue:
        current_id, path = queue.popleft()

        if current_id == target_node_id:
            logger.info(f"[EXECUTION PATH SUCCESS] Execution path from cache {cache_node_id} to {target_node_id}: {path}")
            logger.info(f"[EXECUTION PATH] Path length: {len(path)} nodes (including cache node)")
            if len(path) > 1:
                nodes_to_execute: list[str] = [path[i] for i in range(1, len(path))]
                logger.info(f"[EXECUTION PATH] Nodes to execute (excluding cache): {nodes_to_execute}")
            return path

        if current_id in visited:
            continue
        visited.add(current_id)

        # Check downstream nodes
        if str(current_id) in adjacency:
            downstream_nodes = adjacency.get(str(current_id), [])
            logger.debug(f"[EXECUTION PATH] Node {current_id} has {len(downstream_nodes)} downstream nodes: {downstream_nodes}")
            for downstream_id in downstream_nodes:
                if downstream_id in node_map:
                    queue.append((downstream_id, [*path, downstream_id]))
        else:
            logger.debug(f"[EXECUTION PATH] Node {current_id} has no downstream nodes (dead end)")

    logger.warning(f"[EXECUTION PATH FAILED] No path found from cache {cache_node_id} to target {target_node_id}")
    logger.debug(f"[EXECUTION PATH] Nodes checked: {sorted(visited)}")
    return []

def invalidate_downstream_caches(
    node_id: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    cache_manager,
    pipeline_id: str
) -> set[str]:
    """
    Invalidate caches for all downstream nodes.

    **When invalidation runs (call sites):**
    - **Node delete**: `api/views/node_management.py` calls this for the deleted node so all
      downstream cache entries are removed.
    - **Node insert / edge change**: `api/views/node_addition.py` (and node_management where
      edges change) calls this so the graph change is reflected (downstream caches are stale).
    - **Config or upstream change**: No explicit delete is required; the next preview uses
      version hashes (node_version_hash, upstream_version_hash). A changed config produces
      different hashes, so the existing cache row is not used (implicit invalidation by lookup).
    - **TTL**: Rows with expires_at <= CURRENT_TIMESTAMP are ignored on read.

    Args:
        node_id: Node ID to start invalidation from
        nodes: All nodes in pipeline
        edges: All edges in pipeline
        cache_manager: CheckpointCacheManager instance
        pipeline_id: Pipeline/Canvas ID

    Returns:
        Set of invalidated node IDs
    """
    downstream_nodes = find_downstream_nodes(node_id, nodes, edges)

    invalidated = set()
    for downstream_id in downstream_nodes:
        cache_manager.invalidate_cache(pipeline_id, downstream_id)
        invalidated.add(downstream_id)

    logger.info(f"Invalidated {len(invalidated)} downstream caches from node {node_id}")
    return invalidated

def find_downstream_nodes(
    node_id: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]]
) -> set[str]:
    """
    Find all downstream nodes from a given node.

    Args:
        node_id: Starting node ID
        nodes: All nodes in pipeline
        edges: All edges in pipeline

    Returns:
        Set of downstream node IDs
    """
    downstream = set()
    visited = set()

    def dfs(current_id: str):
        if current_id in visited:
            return
        visited.add(current_id)

        # Find all nodes that this node connects to
        for edge in edges:
            if edge.get('source') == current_id:
                target_id = edge.get('target')
                if target_id:
                    downstream.add(target_id)
                    dfs(target_id)

    dfs(node_id)
    return downstream

def validate_metadata_compatibility(
    parent_node: dict[str, Any],
    child_node: dict[str, Any]
) -> tuple[bool, Optional[str]]:
    """
    Validate that parent node's output metadata is compatible with child node's input requirements.

    Args:
        parent_node: Parent node dict
        child_node: Child node dict

    Returns:
        (is_compatible: bool, warning_message: Optional[str])
    """
    parent_data = parent_node.get('data', {})
    child_data = child_node.get('data', {})

    _parent_type = parent_data.get('type')
    child_type = child_data.get('type')

    # Get parent output metadata
    parent_output_metadata = parent_data.get('output_metadata') or {}
    parent_columns = parent_output_metadata.get('columns', [])
    parent_column_names = {col.get('name') for col in parent_columns if isinstance(col, dict)}

    # Get child input requirements
    child_config = child_data.get('config', {})

    # Check compatibility based on child node type
    if child_type == 'filter':
        # Filter needs columns referenced in conditions
        conditions = child_config.get('conditions', [])
        required_columns = {cond.get('column') for cond in conditions if cond.get('column')}

        missing = required_columns - parent_column_names
        if missing:
            return False, f"Filter requires columns {missing} which are not in parent output"

    elif child_type == 'projection':
        # Projection needs selected columns
        selected_columns = child_config.get('selectedColumns', [])
        calculated_columns = child_config.get('calculatedColumns', [])

        # Check base columns (calculated columns are created in projection)
        calc_col_names = {cc.get('name') for cc in calculated_columns if isinstance(cc, dict)}
        base_columns = [col for col in selected_columns if col not in calc_col_names]
        missing = set(base_columns) - parent_column_names

        if missing:
            return False, f"Projection requires columns {missing} which are not in parent output"

    elif child_type == 'join':
        # Join needs columns from both inputs - this is more complex
        # For now, assume compatible if parent has any columns
        if not parent_column_names:
            return False, "Join requires input columns but parent has no output columns"

    elif child_type == 'aggregate':
        # Aggregate needs columns for grouping and aggregation
        selected_columns = child_config.get('selectedColumns', [])
        aggregate_columns = child_config.get('aggregateColumns', [])

        required_columns = set(selected_columns)
        for agg_col in aggregate_columns:
            if isinstance(agg_col, dict):
                col = agg_col.get('column')
                if col:
                    required_columns.add(col)

        missing = required_columns - parent_column_names
        if missing:
            return False, f"Aggregate requires columns {missing} which are not in parent output"

    elif child_type == 'compute':
        # Compute nodes use Python code - harder to validate statically
        # For now, assume compatible (will fail at execution if incompatible)
        pass

    # If we get here, metadata appears compatible
    return True, None
