"""
Unit tests for core/pipeline/graph_traversal.py

Tests every public function. No database, no Django, no network.
Run with: python -m pytest tests/unit/pipeline/test_graph_traversal.py -v
"""
import sys

sys.path.insert(0, '.')

from api.pipeline.graph_traversal import (
    find_downstream_nodes,
    find_sql_compilable_nodes,
    find_upstream_nodes,
    get_node_dependencies,
    get_source_nodes,
    strip_orphaned_edges,
    topological_sort,
    validate_dag,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_node(id, type='source'):
    return {'id': id, 'data': {'type': type}}

def make_edge(source, target, handle=None):
    e = {'id': f'{source}->{target}', 'source': source, 'target': target}
    if handle:
        e['targetHandle'] = handle
    return e

# Simple linear pipeline: src → filter → projection → destination
LINEAR_NODES = [
    make_node('n_src', 'source'),
    make_node('n_flt', 'filter'),
    make_node('n_prj', 'projection'),
    make_node('n_dst', 'destination'),
]
LINEAR_EDGES = [
    make_edge('n_src', 'n_flt'),
    make_edge('n_flt', 'n_prj'),
    make_edge('n_prj', 'n_dst'),
]

# Pipeline with a join: src_a → join ← src_b → projection
JOIN_NODES = [
    make_node('src_a', 'source'),
    make_node('src_b', 'source'),
    make_node('join1', 'join'),
    make_node('proj1', 'projection'),
]
JOIN_EDGES = [
    make_edge('src_a', 'join1', 'left'),
    make_edge('src_b', 'join1', 'right'),
    make_edge('join1', 'proj1'),
]

# Pipeline with a compute node: src → filter → compute → projection
COMPUTE_NODES = [
    make_node('src1', 'source'),
    make_node('flt1', 'filter'),
    make_node('cmp1', 'compute'),
    make_node('prj1', 'projection'),
]
COMPUTE_EDGES = [
    make_edge('src1', 'flt1'),
    make_edge('flt1', 'cmp1'),
    make_edge('cmp1', 'prj1'),
]


# ── find_upstream_nodes ────────────────────────────────────────────────────────

def test_find_upstream_returns_all_ancestors_in_order():
    result = find_upstream_nodes(LINEAR_NODES, LINEAR_EDGES, 'n_prj')
    # src must come before filter, filter before projection
    assert result.index('n_src') < result.index('n_flt')
    assert result.index('n_flt') < result.index('n_prj')


def test_find_upstream_target_is_last():
    result = find_upstream_nodes(LINEAR_NODES, LINEAR_EDGES, 'n_prj')
    assert result[-1] == 'n_prj'


def test_find_upstream_source_node_returns_just_itself():
    result = find_upstream_nodes(LINEAR_NODES, LINEAR_EDGES, 'n_src')
    assert result == ['n_src']


def test_find_upstream_join_includes_both_branches():
    result = find_upstream_nodes(JOIN_NODES, JOIN_EDGES, 'proj1')
    assert 'src_a' in result
    assert 'src_b' in result
    assert 'join1' in result
    assert result.index('src_a') < result.index('join1')
    assert result.index('src_b') < result.index('join1')


def test_find_upstream_raises_for_unknown_target():
    try:
        find_upstream_nodes(LINEAR_NODES, LINEAR_EDGES, 'nonexistent')
        assert False, "Should have raised ValueError"
    except ValueError:
        pass


def test_find_upstream_excludes_unrelated_nodes():
    # destination is not upstream of projection
    result = find_upstream_nodes(LINEAR_NODES, LINEAR_EDGES, 'n_prj')
    assert 'n_dst' not in result


# ── topological_sort ───────────────────────────────────────────────────────────

def test_topological_sort_sources_come_first():
    result = topological_sort(LINEAR_NODES, LINEAR_EDGES)
    assert result.index('n_src') < result.index('n_flt')
    assert result.index('n_flt') < result.index('n_prj')


def test_topological_sort_all_nodes_present():
    result = topological_sort(LINEAR_NODES, LINEAR_EDGES)
    assert set(result) == {'n_src', 'n_flt', 'n_prj', 'n_dst'}


def test_topological_sort_handles_join():
    result = topological_sort(JOIN_NODES, JOIN_EDGES)
    assert result.index('src_a') < result.index('join1')
    assert result.index('src_b') < result.index('join1')
    assert result.index('join1') < result.index('proj1')


def test_topological_sort_single_node():
    nodes = [make_node('only')]
    result = topological_sort(nodes, [])
    assert result == ['only']


# ── validate_dag ───────────────────────────────────────────────────────────────

def test_validate_dag_linear_is_valid():
    valid, error = validate_dag(LINEAR_NODES, LINEAR_EDGES)
    assert valid is True
    assert error is None


def test_validate_dag_detects_cycle():
    nodes = [make_node('a'), make_node('b'), make_node('c')]
    edges = [make_edge('a', 'b'), make_edge('b', 'c'), make_edge('c', 'a')]
    valid, error = validate_dag(nodes, edges)
    assert valid is False
    assert error is not None


def test_validate_dag_disconnected_graph_is_valid():
    nodes = [make_node('x'), make_node('y')]
    edges = []
    valid, error = validate_dag(nodes, edges)
    assert valid is True


# ── get_source_nodes ───────────────────────────────────────────────────────────

def test_get_source_nodes_returns_only_sources():
    result = get_source_nodes(LINEAR_NODES, LINEAR_EDGES)
    assert 'n_src' in result
    assert 'n_flt' not in result


def test_get_source_nodes_join_pipeline_has_two_sources():
    result = get_source_nodes(JOIN_NODES, JOIN_EDGES)
    assert set(result) == {'src_a', 'src_b'}


# ── find_sql_compilable_nodes ──────────────────────────────────────────────────

def test_sql_compilable_stops_before_compute_node():
    result = find_sql_compilable_nodes(COMPUTE_NODES, COMPUTE_EDGES, 'prj1')
    # compute node should NOT be in the result
    assert 'cmp1' not in result


def test_sql_compilable_boundary_splits_at_compute():
    # When target is AFTER a compute node, the compiler starts a new SQL segment.
    # prj1 is after cmp1 (compute boundary) — so the result is just ['prj1'].
    result = find_sql_compilable_nodes(COMPUTE_NODES, COMPUTE_EDGES, 'prj1')
    assert 'cmp1' not in result  # compute node never in SQL compilable set
    # Target node before the compute boundary includes upstream nodes
    result_before = find_sql_compilable_nodes(COMPUTE_NODES, COMPUTE_EDGES, 'flt1')
    assert 'src1' in result_before
    assert 'flt1' in result_before


def test_sql_compilable_full_pipeline_no_compute():
    result = find_sql_compilable_nodes(LINEAR_NODES, LINEAR_EDGES, 'n_prj')
    assert 'n_src' in result
    assert 'n_flt' in result
    assert 'n_prj' in result


# ── get_node_dependencies ──────────────────────────────────────────────────────

def test_get_node_dependencies_single_input():
    deps = get_node_dependencies('n_flt', LINEAR_EDGES)
    assert deps == ['n_src']


def test_get_node_dependencies_join_has_two_inputs():
    deps = get_node_dependencies('join1', JOIN_EDGES)
    assert set(deps) == {'src_a', 'src_b'}


def test_get_node_dependencies_source_has_no_deps():
    deps = get_node_dependencies('n_src', LINEAR_EDGES)
    assert deps == []


# ── strip_orphaned_edges ───────────────────────────────────────────────────────

def test_strip_orphaned_edges_removes_edges_with_missing_nodes():
    nodes = [make_node('a'), make_node('b')]
    edges = [
        make_edge('a', 'b'),
        make_edge('a', 'ghost'),   # ghost doesn't exist in nodes
        make_edge('ghost2', 'b'),  # ghost2 doesn't exist
    ]
    result = strip_orphaned_edges(nodes, edges)
    assert len(result) == 1
    assert result[0]['source'] == 'a' and result[0]['target'] == 'b'


def test_strip_orphaned_edges_keeps_all_valid_edges():
    result = strip_orphaned_edges(LINEAR_NODES, LINEAR_EDGES)
    assert len(result) == len(LINEAR_EDGES)


# ── find_downstream_nodes ──────────────────────────────────────────────────────

def test_find_downstream_from_source():
    result = find_downstream_nodes('n_src', LINEAR_NODES, LINEAR_EDGES)
    assert 'n_flt' in result
    assert 'n_prj' in result
    assert 'n_dst' in result


def test_find_downstream_from_middle_node():
    result = find_downstream_nodes('n_flt', LINEAR_NODES, LINEAR_EDGES)
    assert 'n_prj' in result
    assert 'n_dst' in result
    assert 'n_src' not in result


def test_find_downstream_from_last_node_is_empty():
    result = find_downstream_nodes('n_dst', LINEAR_NODES, LINEAR_EDGES)
    assert len(result) == 0
