# Moved from: api/utils/graph_utils.py
"""
Graph traversal utilities for pipeline DAG analysis.
Provides functions for topological sorting, dependency resolution, and cycle detection.
"""
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

def find_upstream_nodes(nodes: list[dict[str, Any]], edges: list[dict[str, Any]], target_node_id: str) -> list[str]:
    """
    Find all upstream nodes required to compute the target node.
    Returns nodes in topological order (sources first, target last).

    Args:
        nodes: List of all nodes in the pipeline
        edges: List of edges connecting nodes
        target_node_id: ID of the target node to preview

    Returns:
        List of node IDs in topological order
    """
    node_map = {str(n['id']): n for n in nodes if 'id' in n}

    # Build adjacency list (reverse: target -> sources)
    reverse_adjacency: dict[str, list[str]] = {}
    for edge in edges:
        source_id = edge.get('source')
        target_id = edge.get('target')
        if source_id is not None and target_id is not None:
            if target_id not in reverse_adjacency:
                reverse_adjacency[str(target_id)] = []
            reverse_adjacency[str(target_id)].append(str(source_id))

    # DFS to find all upstream nodes
    visited: set[str] = set()
    upstream_nodes: list[str] = []

    def dfs(node_id: str):
        if node_id in visited:
            return
        visited.add(node_id)

        # Add dependencies first (DFS ensures topological order)
        if node_id in reverse_adjacency:
            for dep_id in reverse_adjacency[node_id]:
                if dep_id in node_map:
                    dfs(dep_id)

        # Add current node after dependencies
        upstream_nodes.append(node_id)

    # Start from target node
    if target_node_id not in node_map:
        raise ValueError(f"Target node {target_node_id} not found in nodes")

    dfs(target_node_id)

    return upstream_nodes

def topological_sort(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> list[str]:
    """
    Perform topological sort on the pipeline DAG.
    Uses Kahn's algorithm.
    Edges that reference a node not in nodes are ignored (stale/orphan edges).

    Args:
        nodes: List of all nodes
        edges: List of edges

    Returns:
        List of node IDs in topological order
    """
    node_map = {n['id']: n for n in nodes}
    node_ids = set(node_map.keys())

    # Only consider edges where both endpoints exist in nodes (ignore stale edges)
    valid_edges = [
        e for e in edges
        if e.get('source') in node_ids and e.get('target') in node_ids
    ]

    adjacency: dict[str, list[str]] = {}
    in_degree: dict[str, int] = {str(node_id): 0 for node_id in node_ids}

    for edge in valid_edges:
        source_id = edge.get('source')
        target_id = edge.get('target')

        if source_id is not None and target_id is not None:
            if str(source_id) not in adjacency:
                adjacency[str(source_id)] = []
            adjacency[str(source_id)].append(str(target_id))

            in_degree[str(target_id)] += 1

    # Kahn's algorithm
    queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
    result: list[str] = []

    while queue:
        node_id = queue.pop(0)
        result.append(str(node_id))

        if str(node_id) in adjacency:
            for neighbor_id in adjacency.get(str(node_id), []):
                in_degree[neighbor_id] = in_degree.get(neighbor_id, 0) - 1
                if in_degree.get(neighbor_id, 0) == 0:
                    queue.append(neighbor_id)

    # Check for cycles
    if len(result) != len(node_ids):
        remaining = set(node_ids) - set(result)
        raise ValueError(f"Cycle detected in pipeline. Nodes not processed: {remaining}")

    return result

def get_node_dependencies(node_id: str, edges: list[dict[str, Any]]) -> list[str]:
    """
    Get direct dependencies (input nodes) for a given node.

    Args:
        node_id: Node ID
        edges: List of edges

    Returns:
        List of dependency node IDs
    """
    dependencies = []
    for edge in edges:
        if edge.get('target') == node_id:
            source_id = edge.get('source')
            if source_id:
                dependencies.append(source_id)
    return dependencies

def strip_orphaned_edges(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Return only edges whose source and target exist in nodes.
    Use before validate_dag when saving so stale edges (e.g. after a node delete)
    don't cause save to fail.
    """
    node_ids = {n.get('id') for n in nodes if n.get('id')}
    return [
        e for e in edges
        if e.get('source') in node_ids and e.get('target') in node_ids
    ]

def validate_dag(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> tuple[bool, Optional[str]]:
    """
    Validate that the pipeline is a valid DAG (no cycles).
    Only edges whose source/target are in nodes are considered.

    Args:
        nodes: List of nodes
        edges: List of edges (should not reference missing nodes; use strip_orphaned_edges first)

    Returns:
        (is_valid, error_message)
    """
    node_ids = {n.get('id') for n in nodes if n.get('id')}
    missing = set()
    for edge in edges:
        src, tgt = edge.get('source'), edge.get('target')
        if src and src not in node_ids:
            missing.add(src)
        if tgt and tgt not in node_ids:
            missing.add(tgt)
    if missing:
        return False, f"Edges reference node(s) not in the pipeline: {', '.join(sorted(missing))}. Remove or reconnect the affected edges."
    try:
        topological_sort(nodes, edges)
        return True, None
    except ValueError as e:
        return False, str(e)

def get_source_nodes(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> list[str]:
    """
    Find all source nodes (nodes with no incoming edges).

    Args:
        nodes: List of nodes
        edges: List of edges

    Returns:
        List of source node IDs
    """
    node_ids = {n['id']: n for n in nodes}
    has_incoming = {edge.get('target') for edge in edges if edge.get('target')}

    source_nodes = []
    for node_id in node_ids:
        if node_id not in has_incoming:
            node_type = node_ids[node_id].get('data', {}).get('type')
            if node_type == 'source':
                source_nodes.append(node_id)

    return source_nodes

def find_sql_compilable_nodes(nodes: list[dict[str, Any]], edges: list[dict[str, Any]], target_node_id: str) -> list[str]:
    """
    Find nodes that can be compiled to SQL, stopping at Compute node boundaries.
    Compute nodes are execution boundaries and cannot be compiled to SQL.

    Args:
        nodes: List of all nodes in the pipeline
        edges: List of edges connecting nodes
        target_node_id: ID of the target node to compile

    Returns:
        List of node IDs in topological order that can be compiled to SQL
        (stops before any compute node)
    """
    node_map = {str(n['id']): n for n in nodes if 'id' in n}

    # Build adjacency list (reverse: target -> sources)
    reverse_adjacency: dict[str, list[str]] = {}
    for edge in edges:
        source_id = edge.get('source')
        target_id = edge.get('target')
        if source_id is not None and target_id is not None:
            if target_id not in reverse_adjacency:
                reverse_adjacency[str(target_id)] = []
            reverse_adjacency[str(target_id)].append(str(source_id))

    # DFS to find all upstream nodes, stopping at compute nodes
    visited: set[str] = set()
    sql_nodes: list[str] = []

    def dfs(node_id: str):
        if node_id in visited:
            return
        visited.add(node_id)

        node = node_map.get(node_id)
        if not node:
            return

        node_type = node.get('data', {}).get('type')

        # Stop at compute nodes - they are execution boundaries
        if node_type == 'compute':
            logger.info(f"Stopping SQL compilation at compute node boundary: {node_id}")
            return

        # Add dependencies first (DFS ensures topological order)
        if node_id in reverse_adjacency:
            for dep_id in reverse_adjacency[node_id]:
                if dep_id in node_map:
                    dfs(dep_id)

        # Add current node after dependencies (only if not compute)
        if node_type != 'compute':
            sql_nodes.append(node_id)

    # Start from target node
    if target_node_id not in node_map:
        raise ValueError(f"Target node {target_node_id} not found in nodes")

    target_node = node_map[target_node_id]
    target_node_type = target_node.get('data', {}).get('type')

    # If target is compute, we can't compile SQL for it
    if target_node_type == 'compute':
        # Find the input node to compute (compile up to that)
        for e in edges:
            if e.get('target') == target_node_id:
                src = e.get('source')
                if src is not None:
                    dfs(str(src))
                break
        return sql_nodes

    # Otherwise, compile normally (will stop at compute boundaries)
    dfs(target_node_id)

    return sql_nodes

def find_sql_compilable_nodes_from(
    start_node_id: str,
    target_node_id: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
) -> list[str]:
    """
    Return SQL-compilable nodes from start_node_id through target_node_id (inclusive).
    Used for resume-from-cache: compile only from cached ancestor to target.
    """
    full_list = find_sql_compilable_nodes(nodes, edges, target_node_id)
    try:
        idx = full_list.index(start_node_id)
        res: list[str] = []
        for i in range(idx, len(full_list)):
            res.append(full_list[i])
        return res
    except ValueError:
        return []

def find_connected_components(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> list[set[str]]:
    """
    Find connected components (independent flows) in the pipeline graph.
    Uses undirected connectivity: two nodes are in the same component if there is a path between them.

    Args:
        nodes: List of all nodes
        edges: List of edges

    Returns:
        List of sets; each set contains node IDs for one independent flow
    """
    node_ids = {n.get("id") for n in nodes if n.get("id")}
    if not node_ids:
        return []

    # Build undirected adjacency (both source->target and target->source)
    adjacency: dict[str, set[str]] = {str(nid): set() for nid in node_ids}
    for edge in edges:
        src, tgt = edge.get("source"), edge.get("target")
        if src in node_ids and tgt in node_ids:
            adjacency[str(src)].add(str(tgt))
            adjacency[str(tgt)].add(str(src))

    visited: set[str] = set()
    components: list[set[str]] = []

    for start_id in node_ids:
        start_id_str = str(start_id)
        if start_id_str in visited:
            continue
        component: set[str] = set()
        queue = [start_id_str]
        visited.add(start_id_str)
        component.add(start_id_str)
        while queue:
            curr = queue.pop(0)
            for neighbor in adjacency.get(curr, set()):
                if neighbor not in visited:
                    visited.add(neighbor)
                    component.add(neighbor)
                    queue.append(neighbor)
        components.append(component)

    return components

def get_flow_labels(components: list[set[str]], nodes: list[dict[str, Any]]) -> list[str]:
    """
    Generate human-readable labels for each flow (connected component).
    Uses source node table names or labels when available.

    Args:
        components: List of node ID sets from find_connected_components
        nodes: List of all nodes

    Returns:
        List of labels, one per component
    """
    node_map = {n.get("id"): n for n in nodes if n.get("id")}

    labels = []
    for component in components:
        source_names = []
        for nid in component:
            node = node_map.get(nid)
            if not node:
                continue
            node_type = (node.get("data") or {}).get("type") or node.get("type")
            if node_type == "source":
                label = (
                    (node.get("data") or {}).get("config", {}).get("table_name")
                    or (node.get("data") or {}).get("business_name")
                    or (node.get("data") or {}).get("label")
                    or ("%.8s" % str(nid))
                )
                source_names.append(str(label))
        if not source_names:
            import itertools
            for nid in itertools.islice(component, 2):
                source_names.append("%.8s" % str(nid))

        if len(source_names) <= 3:
            prefix = ", ".join(source_names)
        else:
            filtered = []
            for i in range(3):
                filtered.append(source_names[i])
            prefix = ", ".join(filtered) + "..."
        labels.append(prefix)
    return labels

def find_downstream_nodes(start_node_id: str, nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> set[str]:
    """
    Find all nodes that are reachable from the given node (descendants) in the DAG.
    Used for cache invalidation.
    """
    forward_adj: dict[str, list[str]] = {}
    for edge in edges:
        src = edge.get('source')
        dst = edge.get('target')
        if src is not None and dst is not None:
            if str(src) not in forward_adj:
                forward_adj[str(src)] = []
            forward_adj[str(src)].append(str(dst))

    node_ids = {str(n.get('id')) for n in nodes if n.get('id')}
    reachable: set[str] = set()
    stack = [start_node_id]
    visited = {start_node_id}

    while stack:
        curr = stack.pop()
        for neighbor in forward_adj.get(curr, []):
            if neighbor in node_ids and neighbor not in visited:
                visited.add(neighbor)
                reachable.add(neighbor)
                stack.append(neighbor)
    return reachable
