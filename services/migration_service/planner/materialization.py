"""
Materialization Detection Module
Determines which nodes require staging tables.

All rules are data-driven and reused — no hardcoded node-type branches.
Anchor types are defined once; the same recursive formula applies everywhere
(find staging before, flatten linear chain).
"""

from dataclasses import dataclass
from enum import Enum
import logging
import re
from typing import Any, Optional

from .staging_naming import get_staging_table_name

logger = logging.getLogger(__name__)

# When one source feeds multiple branches: share source if overlap ratio above this
SHARED_SOURCE_OVERLAP_THRESHOLD = 0.7
# Share source if estimated rows above this (avoids reading large remote table N times)
SHARED_SOURCE_ESTIMATED_ROWS_THRESHOLD = 100_000

# Anchor types — reused across rules (add new types here, not scattered in code)
MERGE_ANCHOR_TYPES: tuple[str, ...] = ("join", "aggregation")
ANCHOR_TYPES_NEED_PARENT_STAGING: tuple[str, ...] = ("compute", "destination", "destination-postgresql", "destination-postgres")
DESTINATION_TYPES: tuple[str, ...] = ("destination", "destination-postgresql", "destination-postgres")

class MaterializationReason(Enum):
    """Why a node is materialized."""
    BRANCH_END_BEFORE_JOIN = "branch_end_before_join"
    JOIN_RESULT = "join_result"
    AGGREGATION_RESULT = "aggregation_result"
    COMPUTE_MULTI_BRANCH = "compute_multi_branch"
    PRE_COMPUTE_STAGING = "pre_compute_staging"  # Staging before compute (parent of compute node)
    PRE_DESTINATION_STAGING = "pre_destination_staging"
    SHARED_SOURCE = "shared_source"  # One source feeds multiple branches; materialize source once
    MULTI_BRANCH_FEED = "multi_branch_feed"  # Node feeds multiple downstream branches; materialize so all read from same staging

@dataclass
class MaterializationPoint:
    """Represents a node that must be materialized."""
    node_id: str
    reason: MaterializationReason
    staging_table: str  # staging_jobs.job_<job_id>_node_<node_id>

@dataclass
class AnchorNode:
    """A natural boundary in the DAG where we split into segments."""
    node_id: str
    kind: str  # "join" | "aggregation" | "compute" | "destination"
    feeds_multiple: bool = False  # For compute: True if multiple downstream branches

def detect_anchor_nodes(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> list[AnchorNode]:
    """
    Scan the DAG and identify all anchor nodes (split points).
    Anchors: JOIN, Aggregation, Compute (if references multiple upstream branches), Destination.
    Returns list of AnchorNode ordered by discovery (topological order not required here).
    """
    node_map = {node["id"]: node for node in nodes}
    adjacency = _build_adjacency(edges)
    _build_reverse_adjacency(edges)
    anchors: list[AnchorNode] = []

    for node_id, node in node_map.items():
        node_type = _get_node_type(node)
        if node_type in DESTINATION_TYPES:
            anchors.append(AnchorNode(node_id=node_id, kind="destination", feeds_multiple=False))
            continue
        if node_type in MERGE_ANCHOR_TYPES:
            anchors.append(AnchorNode(node_id=node_id, kind=node_type, feeds_multiple=False))
            continue
        if node_type == "compute":
            children = adjacency.get(node_id, [])
            feeds_multiple = len(children) > 1
            anchors.append(AnchorNode(node_id=node_id, kind="compute", feeds_multiple=feeds_multiple))
            continue
    return anchors

def classify_compute_node(
    node_id: str,
    node: dict[str, Any],
    edges: list[dict[str, Any]],
    node_map: dict[str, Any],
) -> str:
    """
    Classify compute node as "anchor" (stage it) or "inline" (treat as calc col in flat SELECT).
    Anchor if: uses window functions; references multiple upstream staging tables; or
    output consumed by multiple downstream branches. Otherwise inline.
    """
    _build_reverse_adjacency(edges)
    adjacency = _build_adjacency(edges)
    children = adjacency.get(node_id, [])

    if len(children) > 1:
        return "anchor"
    node_config = node.get("data", {}).get("config", {})
    for comp in node_config.get("computedColumns", []):
        if not isinstance(comp, dict):
            continue
        expr = (comp.get("expression") or "").upper()
        if any(w in expr for w in ("OVER(", "ROW_NUMBER", "RANK(", "DENSE_RANK", "NTILE(", "LAG(", "LEAD(")):
            return "anchor"
    return "inline"

def get_required_fields_for_branch(
    branch_terminal_id: str,
    source_id: str,
    node_map: dict[str, Any],
    edges: list[dict[str, Any]],
    config: dict[str, Any],
) -> set[str]:
    """
    Collect required field names (for projection pushdown) for one branch:
    output columns of the terminal + columns referenced in filter conditions and calc expressions.
    """
    reverse_adj = _build_reverse_adjacency(edges)
    required: set[str] = set()
    current = branch_terminal_id
    visited = set()
    while current and current != source_id and current not in visited:
        visited.add(current)
        node = node_map.get(current, {})
        ntype = _get_node_type(node)
        nc = node.get("data", {}).get("config", {})
        if ntype == "projection":
            for col in nc.get("columns", []) or nc.get("selectedColumns", []):
                name = col.get("name") if isinstance(col, dict) else col
                if name:
                    required.add(name)
            for calc in nc.get("calculated_columns", []) or nc.get("calculatedColumns", []):
                if isinstance(calc, dict):
                    required.add(calc.get("name") or calc.get("alias") or "")
                    expr = calc.get("expression") or ""
                    required.update(re.findall(r'"([^"]+)"', expr))
        elif ntype == "filter":
            for cond in nc.get("conditions", []):
                if isinstance(cond, dict) and cond.get("column"):
                    required.add(cond["column"])
        elif ntype == "compute":
            for comp in nc.get("computedColumns", []):
                if isinstance(comp, dict):
                    required.add(comp.get("alias") or comp.get("name") or "")
                    expr = comp.get("expression") or ""
                    required.update(re.findall(r'"([^"]+)"', expr))
        parents = reverse_adj.get(current, [])
        if len(parents) != 1:
            break
        current = parents[0]
    return required

def should_share_source(
    source_id: str,
    branch_terminal_ids: list[str],
    node_map: dict[str, Any],
    edges: list[dict[str, Any]],
    config: dict[str, Any],
) -> bool:
    """
    Decide whether to materialize shared source once (Option B) or let each branch
    read independently (Option A — projection pushdown per branch).
    Share if source is expensive (remote / large) or branches need mostly overlapping columns.
    """
    if len(branch_terminal_ids) < 2:
        return False
    source_config = config.get("source_configs", {}).get(source_id, {})
    is_remote = source_config.get("is_remote", False)
    estimated_rows = source_config.get("estimated_rows") or 0
    try:
        estimated_rows = int(estimated_rows)
    except (TypeError, ValueError):
        estimated_rows = 0
    if is_remote or estimated_rows > SHARED_SOURCE_ESTIMATED_ROWS_THRESHOLD:
        return True
    branch_fields = [
        get_required_fields_for_branch(tid, source_id, node_map, edges, config)
        for tid in branch_terminal_ids
    ]
    if not branch_fields:
        return True
    intersection = set.intersection(*branch_fields)
    union = set.union(*branch_fields)
    if not union:
        return True
    overlap_ratio = len(intersection) / len(union)
    return overlap_ratio >= SHARED_SOURCE_OVERLAP_THRESHOLD

def _find_source_for_branch(
    start_node_id: str,
    node_map: dict[str, Any],
    reverse_adjacency: dict[str, list[str]]
) -> str:
    """
    Walk backwards from start_node_id (following single parent) until we reach a source node.
    Returns that source node's id, or start_node_id if no source is found (e.g. already source).
    """
    current = start_node_id
    visited = set()
    while current:
        if current in visited:
            break
        visited.add(current)
        node = node_map.get(current)
        if node and _get_node_type(node) == "source":
            return current
        parents = reverse_adjacency.get(current, [])
        if len(parents) != 1:
            break
        current = parents[0]
    return start_node_id

def detect_materialization_points(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    job_id: str,
    linear_branches: bool = False,
    config: Optional[dict[str, Any]] = None,
) -> tuple:
    """
    Detect which nodes require staging tables.

    Staging ONLY at boundaries (branch ends, JOINs, aggregation, compute anchor, pre-destination).
    When one source feeds multiple branches: if config is provided, use should_share_source() to
    decide shared source (Option B) vs per-branch read (Option A). Otherwise use linear_branches flag.

    Returns:
        Tuple of (materialization_points: dict[str, MaterializationPoint],
                  shared_source_terminals: dict[str, list[str]])  # source_id -> [terminal_id, ...]
    """
    node_map = {node["id"]: node for node in nodes}
    adjacency = _build_adjacency(edges)
    reverse_adjacency = _build_reverse_adjacency(edges)

    materialization_points = {}
    # Map source_id -> list of branch terminal node_ids that use this source
    source_to_terminals: dict[str, list[str]] = {}

    # BOUNDARY A: Branch ends before merge (JOIN, aggregation) — reused for any merge anchor
    for node_id, node in node_map.items():
        node_type = _get_node_type(node)
        if node_type not in MERGE_ANCHOR_TYPES:
            continue
        # Use only the two actual left/right parents (from edge handles) for join; for aggregation use all parents
        all_parents = reverse_adjacency.get(node_id, [])
        if node_type == "join":
            merge_parent_ids = _get_join_left_right_parents(node_id, edges, reverse_adjacency)
        else:
            merge_parent_ids = all_parents[:2] if len(all_parents) >= 2 else all_parents
        logger.info(
            "[MAT DEBUG] %s %s: reverse_adj parents=%s, merge parent ids=%s",
            node_type, node_id[:8], [p[:8] for p in all_parents], [p[:8] for p in merge_parent_ids],
        )
        for parent_id in merge_parent_ids:
            branch_terminal = _find_branch_terminal(
                parent_id, node_id, node_map, reverse_adjacency, adjacency
            )
            if branch_terminal and branch_terminal not in materialization_points:
                materialization_points[branch_terminal] = MaterializationPoint(
                    node_id=branch_terminal,
                    reason=MaterializationReason.BRANCH_END_BEFORE_JOIN,
                    staging_table=get_staging_table_name(job_id, branch_terminal)
                )
                source_id = _find_source_for_branch(
                    branch_terminal, node_map, reverse_adjacency
                )
                if source_id not in source_to_terminals:
                    source_to_terminals[source_id] = []
                if branch_terminal not in source_to_terminals[source_id]:
                    source_to_terminals[source_id].append(branch_terminal)
                logger.info(
                    "[MAT DEBUG]   branch_terminal=%s, source_id=%s → source_to_terminals[%s]=%s",
                    branch_terminal[:8], source_id[:8] if source_id else None,
                    source_id[:8] if source_id else None,
                    [t[:8] for t in source_to_terminals.get(source_id, [])],
                )

    logger.info(
        "[MAT DEBUG] source_to_terminals (before shared-source): %s",
        {k[:8] if k else k: [t[:8] for t in v] for k, v in source_to_terminals.items()},
    )
    # Shared source: when one source feeds multiple branch terminals, materialize the source once
    # only if should_share_source says so (or if linear_branches=False and no config).
    shared_source_terminals: dict[str, list[str]] = {}
    for source_id, terminal_ids in source_to_terminals.items():
        unique_terminals = list(dict.fromkeys(terminal_ids))
        logger.info(
            "[MAT DEBUG] source %s: terminal_ids=%s, unique_terminals=%s, len=%s",
            source_id[:8] if source_id else source_id,
            [t[:8] for t in terminal_ids], [t[:8] for t in unique_terminals], len(unique_terminals),
        )
        if len(unique_terminals) < 2:
            logger.info("[MAT DEBUG]   → skip shared source (single branch)")
            continue  # single branch — never share source
        if node_map.get(source_id) and _get_node_type(node_map[source_id]) != "source":
            continue
        use_shared = linear_branches is False
        if config is not None:
            use_shared = should_share_source(source_id, unique_terminals, node_map, edges, config)
        if use_shared:
            if source_id not in materialization_points:
                materialization_points[source_id] = MaterializationPoint(
                    node_id=source_id,
                    reason=MaterializationReason.SHARED_SOURCE,
                    staging_table=get_staging_table_name(job_id, source_id)
                )
            shared_source_terminals[source_id] = unique_terminals
            logger.info("[MAT DEBUG]   → ADDED shared source staging for %s", source_id[:8])

    logger.info(
        "[MAT DEBUG] materialization_points after BOUNDARY A+shared: %s",
        {nid[:8]: str(mp.reason.value) for nid, mp in materialization_points.items()},
    )
    # BOUNDARY B: Merge anchor results (JOIN, aggregation) — reused for any merge type
    for node_id, node in node_map.items():
        node_type = _get_node_type(node)
        if node_type in MERGE_ANCHOR_TYPES:
            reason = MaterializationReason.JOIN_RESULT if node_type == "join" else MaterializationReason.AGGREGATION_RESULT
            materialization_points[node_id] = MaterializationPoint(
                node_id=node_id,
                reason=reason,
                staging_table=get_staging_table_name(job_id, node_id)
            )

    # BOUNDARY B3: Compute — anchor only when classify_compute_node says "anchor"
    for node_id, node in node_map.items():
        if _get_node_type(node) != "compute":
            continue
        if classify_compute_node(node_id, node, edges, node_map) == "anchor":
            materialization_points[node_id] = MaterializationPoint(
                node_id=node_id,
                reason=MaterializationReason.COMPUTE_MULTI_BRANCH,
                staging_table=get_staging_table_name(job_id, node_id)
            )

    # BOUNDARY B3b: Shared multi-branch — any node that feeds multiple downstream branches (reused)
    for node_id, node in node_map.items():
        children = adjacency.get(node_id, [])
        if len(children) < 2:
            continue
        node_type = _get_node_type(node)
        if node_type in DESTINATION_TYPES:
            continue
        if node_id not in materialization_points:
            materialization_points[node_id] = MaterializationPoint(
                node_id=node_id,
                reason=MaterializationReason.MULTI_BRANCH_FEED,
                staging_table=get_staging_table_name(job_id, node_id)
            )
            logger.info(
                "[MAT DEBUG] Added multi_branch_feed: %s (feeds %s downstream branches)",
                node_id[:8], len(children),
            )

    # BOUNDARY B4 & C: Parent staging — materialize parent of compute/destination (reused pattern)
    # Same recursive formula: find staging before, flatten linear chain
    for node_id, node in node_map.items():
        node_type = _get_node_type(node)
        if node_type not in ANCHOR_TYPES_NEED_PARENT_STAGING:
            continue
        parents = reverse_adjacency.get(node_id, [])
        if not parents:
            continue
        parent_id = parents[0]
        if parent_id in materialization_points:
            continue
        reason = MaterializationReason.PRE_COMPUTE_STAGING if node_type == "compute" else MaterializationReason.PRE_DESTINATION_STAGING
        materialization_points[parent_id] = MaterializationPoint(
            node_id=parent_id,
            reason=reason,
            staging_table=get_staging_table_name(job_id, parent_id)
        )
        logger.info(
            "[MAT DEBUG] Added %s: %s (parent of %s %s)",
            reason.value, parent_id[:8], node_type, node_id[:8],
        )

    # When a SOURCE has MULTI_BRANCH_FEED, add it to shared_source_terminals so compile_source_staging_sql
    # is used (with OR-combined filters). Find terminals = materialization points that trace back to this source.
    for node_id, mp in list(materialization_points.items()):
        if getattr(mp, "reason", None) != MaterializationReason.MULTI_BRANCH_FEED:
            continue
        node = node_map.get(node_id, {})
        if _get_node_type(node) != "source":
            continue
        terminals = []
        for other_id in materialization_points:
            if other_id == node_id:
                continue
            src = _find_source_for_branch(other_id, node_map, reverse_adjacency)
            if src == node_id:
                terminals.append(other_id)
        if len(terminals) >= 2:
            shared_source_terminals[node_id] = list(dict.fromkeys(terminals))
            logger.info(
                "[MAT DEBUG] MULTI_BRANCH_FEED source %s → shared_source_terminals with %s terminals: %s",
                node_id[:8], len(terminals), [t[:8] for t in shared_source_terminals[node_id]],
            )

    logger.info(
        "[MAT DEBUG] FINAL materialization_points: %s",
        {nid[:8]: str(mp.reason.value) for nid, mp in materialization_points.items()},
    )
    logger.info(
        "[MAT DEBUG] FINAL shared_source_terminals: %s",
        {k[:8] if k else k: [t[:8] for t in v] for k, v in shared_source_terminals.items()},
    )
    return (materialization_points, shared_source_terminals)

def _get_join_left_right_parents(
    join_node_id: str,
    edges: list[dict[str, Any]],
    reverse_adjacency: dict[str, list[str]],
) -> list[str]:
    """
    Return exactly the two parents that are the left and right inputs to the JOIN,
    using edge targetHandle/sourceHandle so we don't treat intermediate nodes (e.g. p1)
    as separate "parents" when only p3 (left) and p2 (right) should be branch terminals.
    """
    parents = reverse_adjacency.get(join_node_id, [])
    if len(parents) < 2:
        return parents
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
    if left_id is not None and right_id is not None:
        return [left_id, right_id]
    return [parents[0], parents[1]]

def _find_branch_terminal(
    start_node_id: str,
    join_node_id: str,
    node_map: dict[str, Any],
    reverse_adjacency: dict[str, list[str]],
    adjacency: dict[str, list[str]],
) -> str:
    """
    Return the terminal node of a branch leading to a JOIN.
    start_node_id is already the direct parent of the JOIN (from reverse_adjacency);
    return it directly so only one terminal per branch is registered.
    """
    return start_node_id

def _get_children(node_id: str, node_map: dict[str, Any], reverse_adjacency: dict[str, list[str]]) -> list[str]:
    """Get children of a node."""
    children = []
    for child_id, parents in reverse_adjacency.items():
        if node_id in parents:
            children.append(child_id)
    return children

def _get_node_type(node: dict[str, Any]) -> str:
    """Extract node type from node dict. Normalize frontend 'aggregate' to 'aggregation'."""
    raw = (node.get("type") or node.get("data", {}).get("type") or "").lower().strip()
    if raw == "aggregate":
        return "aggregation"
    return raw

def _build_adjacency(edges: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Build forward adjacency list."""
    adjacency = {}
    for edge in edges:
        source = edge["source"]
        target = edge["target"]
        if source not in adjacency:
            adjacency[source] = []
        adjacency[source].append(target)
    return adjacency

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
