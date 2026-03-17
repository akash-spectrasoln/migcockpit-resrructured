# Moved from: api/utils/cache_strategy.py
"""
Cache strategy utilities for adaptive node caching.
Provides functions to analyze pipeline structure for caching decisions.
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)

def compute_depth_since_last_cache(
    node_id: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    cached_node_ids: set[str]
) -> int:
    """
    Compute depth (number of nodes) since last cached node upstream.

    Args:
        node_id: Current node ID
        nodes: All nodes in pipeline
        edges: All edges in pipeline
        cached_node_ids: Set of node IDs that are cached

    Returns:
        Depth since last cache (0 if node itself should be cached)
    """
    node_map = {n['id']: n for n in nodes}

    # Build reverse adjacency (target -> sources)
    reverse_adjacency = {}
    for edge in edges:
        source_id = edge.get('source')
        target_id = edge.get('target')
        if target_id not in reverse_adjacency:
            reverse_adjacency[target_id] = []
        reverse_adjacency[target_id].append(source_id)

    # DFS to find nearest cached node upstream
    visited = set()

    def dfs(current_id: str, current_depth: int) -> int:
        if current_id in visited:
            return current_depth
        visited.add(current_id)

        # If this node is cached, return its depth
        if current_id in cached_node_ids:
            return current_depth

        # Check upstream nodes
        if current_id in reverse_adjacency:
            min_depth = float('in')
            for upstream_id in reverse_adjacency[current_id]:
                if upstream_id in node_map:
                    upstream_depth = dfs(upstream_id, current_depth + 1)
                    min_depth = min(min_depth, upstream_depth)
            return min_depth if min_depth != float('in') else current_depth

        # No upstream nodes (source node)
        return current_depth

    result = dfs(node_id, 0)
    return result if result != float('in') else 0

def compute_fan_out(
    node_id: str,
    edges: list[dict[str, Any]]
) -> int:
    """
    Compute fan-out (number of downstream nodes) for a node.

    Args:
        node_id: Node ID
        edges: All edges in pipeline

    Returns:
        Number of downstream nodes
    """
    downstream_count = 0
    for edge in edges:
        if edge.get('source') == node_id:
            downstream_count += 1
    return downstream_count

def find_upstream_cached_nodes(
    node_id: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    cached_node_ids: set[str]
) -> list[str]:
    """
    Find all cached nodes upstream from a given node.

    Args:
        node_id: Current node ID
        nodes: All nodes in pipeline
        edges: All edges in pipeline
        cached_node_ids: Set of cached node IDs

    Returns:
        List of upstream cached node IDs (in topological order)
    """
    node_map = {n['id']: n for n in nodes}

    # Build reverse adjacency
    reverse_adjacency = {}
    for edge in edges:
        source_id = edge.get('source')
        target_id = edge.get('target')
        if target_id not in reverse_adjacency:
            reverse_adjacency[target_id] = []
        reverse_adjacency[target_id].append(source_id)

    # DFS to find cached nodes upstream
    visited = set()
    cached_upstream = []

    def dfs(current_id: str):
        if current_id in visited:
            return
        visited.add(current_id)

        # Check upstream nodes first
        if current_id in reverse_adjacency:
            for upstream_id in reverse_adjacency[current_id]:
                if upstream_id in node_map:
                    dfs(upstream_id)

        # Add current node if cached
        if current_id in cached_node_ids:
            cached_upstream.append(current_id)

    dfs(node_id)
    return cached_upstream

def estimate_row_reduction(
    input_node_id: str,
    output_node_id: str,
    nodes: list[dict[str, Any]],
    cached_data: dict[str, dict[str, Any]]
) -> float:
    """
    Estimate row reduction percentage between input and output nodes.

    Args:
        input_node_id: Input node ID
        output_node_id: Output node ID
        nodes: All nodes in pipeline
        cached_data: Dict mapping node_id to cached data

    Returns:
        Reduction percentage (0.0 to 1.0)
    """
    input_data = cached_data.get(input_node_id, {})
    output_data = cached_data.get(output_node_id, {})

    input_rows = input_data.get('metadata', {}).get('row_count', 0)
    output_rows = output_data.get('metadata', {}).get('row_count', 0)

    if input_rows == 0:
        return 0.0

    reduction = (input_rows - output_rows) / input_rows
    return max(0.0, min(1.0, reduction))

def is_filter_pushdown_candidate(
    filter_node_id: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    column_lineage: dict[str, dict[str, Any]]
) -> bool:
    """
    Check if a filter node is a pushdown candidate based on column lineage.

    Args:
        filter_node_id: Filter node ID
        nodes: All nodes in pipeline
        edges: All edges in pipeline
        column_lineage: Column lineage map from SQL compiler

    Returns:
        True if filter can be pushed down
    """
    filter_node = next((n for n in nodes if n['id'] == filter_node_id), None)
    if not filter_node:
        return False

    filter_config = filter_node.get('data', {}).get('config', {})
    conditions = filter_config.get('conditions', [])

    if not conditions:
        return False

    # Check if all columns in filter conditions are pushdown-safe
    for condition in conditions:
        column_name = condition.get('column')
        if column_name:
            origin = column_lineage.get(column_name)
            if origin:
                origin_type = origin.get('origin_type')
                # Filter can be pushed down if column originates from SOURCE/JOIN/PROJECTION
                if origin_type in ['COMPUTE', 'AGGREGATE']:
                    return False

    return True
