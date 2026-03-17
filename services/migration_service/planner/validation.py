"""
Pipeline Validation Module
Validates DAG structure before execution.
"""

from dataclasses import dataclass
from typing import Any


class PipelineValidationError(Exception):
    """Raised when pipeline validation fails."""
    pass

@dataclass
class DAGNode:
    """Represents a node in the DAG."""
    node_id: str
    node_type: str
    config: dict[str, Any]
    parents: list[str]
    children: list[str]

def validate_pipeline(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> None:
    """
    Validate pipeline DAG structure.

    Ensures:
    - DAG is acyclic
    - All nodes are reachable
    - JOIN has >= 2 parents
    - Destination has exactly 1 parent

    Args:
        nodes: List of node dictionaries
        edges: List of edge dictionaries

    Raises:
        PipelineValidationError: If validation fails
    """
    if not nodes:
        raise PipelineValidationError("Pipeline has no nodes")

    # Build node map and adjacency
    node_map = {node["id"]: node for node in nodes}
    adjacency = _build_adjacency(edges)
    reverse_adjacency = _build_reverse_adjacency(edges)

    # Validate DAG is acyclic
    if _has_cycle(node_map, adjacency):
        raise PipelineValidationError("Pipeline contains cycles (not a DAG)")

    # Validate all nodes are reachable
    source_nodes = [nid for nid in node_map.keys() if not reverse_adjacency.get(nid)]
    if not source_nodes:
        raise PipelineValidationError("Pipeline has no source nodes")

    reachable = _get_reachable_nodes(source_nodes, adjacency)
    unreachable = set(node_map.keys()) - reachable
    if unreachable:
        raise PipelineValidationError(
            f"Unreachable nodes detected: {unreachable}"
        )

    # Validate JOIN nodes
    for node_id, node in node_map.items():
        node_type = _get_node_type(node)
        parents = reverse_adjacency.get(node_id, [])

        if node_type == "join":
            if len(parents) < 2:
                raise PipelineValidationError(
                    f"JOIN node '{node_id}' must have >= 2 parents, has {len(parents)}"
                )

        # Validate destination nodes
        if node_type in ("destination", "destination-postgresql", "destination-postgres"):
            if len(parents) != 1:
                raise PipelineValidationError(
                    f"Destination node '{node_id}' must have exactly 1 parent, has {len(parents)}"
                )

    # Validate node types
    valid_types = {"source", "projection", "filter", "compute", "join",
                   "destination", "destination-postgresql", "destination-postgres"}

    for node_id, node in node_map.items():
        node_type = _get_node_type(node)
        if node_type not in valid_types:
            raise PipelineValidationError(
                f"Node '{node_id}' has invalid type: '{node_type}'"
            )

def _get_node_type(node: dict[str, Any]) -> str:
    """Extract node type from node dict."""
    return (node.get("type") or node.get("data", {}).get("type") or "").lower().strip()

def _build_adjacency(edges: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Build forward adjacency list (parent -> children)."""
    adjacency = {}
    for edge in edges:
        source = edge["source"]
        target = edge["target"]
        if source not in adjacency:
            adjacency[source] = []
        adjacency[source].append(target)
    return adjacency

def _build_reverse_adjacency(edges: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Build reverse adjacency list (child -> parents)."""
    reverse = {}
    for edge in edges:
        source = edge["source"]
        target = edge["target"]
        if target not in reverse:
            reverse[target] = []
        reverse[target].append(source)
    return reverse

def _has_cycle(node_map: dict[str, Any], adjacency: dict[str, list[str]]) -> bool:
    """Detect cycles using DFS."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {nid: WHITE for nid in node_map.keys()}

    def visit(node_id: str) -> bool:
        if color[node_id] == GRAY:
            return True  # Back edge = cycle
        if color[node_id] == BLACK:
            return False  # Already processed

        color[node_id] = GRAY
        for child in adjacency.get(node_id, []):
            if visit(child):
                return True
        color[node_id] = BLACK
        return False

    for node_id in node_map.keys():
        if color[node_id] == WHITE:
            if visit(node_id):
                return True

    return False

def _get_reachable_nodes(source_nodes: list[str], adjacency: dict[str, list[str]]) -> set[str]:
    """Get all nodes reachable from sources via BFS."""
    reachable = set(source_nodes)
    queue = list(source_nodes)

    while queue:
        node_id = queue.pop(0)
        for child in adjacency.get(node_id, []):
            if child not in reachable:
                reachable.add(child)
                queue.append(child)

    return reachable
