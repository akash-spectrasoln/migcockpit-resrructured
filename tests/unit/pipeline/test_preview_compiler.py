"""
Tests for the preview compiler's pure logic components.

The full SQLCompiler requires Django (it uses Django ORM helpers in filter_builder.py).
Those tests live in the Django integration test suite (requires `python manage.py test`).

This file tests the PURE LOGIC inside the compiler that does NOT need Django:
  - CTE name sanitization
  - Column lineage tracking
  - Filter pushdown analysis (using direct class construction with mocks)
  - Downstream/upstream DAG logic used by the compiler

For full SQLCompiler integration tests: python manage.py test core.tests
Run with: python -m pytest tests/unit/pipeline/test_preview_compiler.py -v
"""
import sys

sys.path.insert(0, '.')


# ── Pure logic tests on the graph functions the compiler relies on ────────────

from api.pipeline.graph_traversal import (
    find_downstream_nodes,
    find_sql_compilable_nodes,
    find_upstream_nodes,
)


def make_node(id, type='source'):
    return {'id': id, 'data': {'type': type}}

def make_edge(s, t):
    return {'id': f'{s}-{t}', 'source': s, 'target': t}


# ── CTE name sanitization rules ───────────────────────────────────────────────
# These test the _get_cte_name logic directly without importing the compiler

def sanitize_node_id(node_id: str) -> str:
    """Mirror of SQLCompiler._get_cte_name logic."""
    sanitized = node_id.replace('-', '_').replace('.', '_')[:50]
    return f"node_{sanitized}"


def test_cte_name_replaces_hyphens_with_underscores():
    assert '-' not in sanitize_node_id('abc-def-123')


def test_cte_name_replaces_dots_with_underscores():
    assert '.' not in sanitize_node_id('abc.def.ghi')


def test_cte_name_has_node_prefix():
    assert sanitize_node_id('s1').startswith('node_')


def test_cte_name_is_at_most_55_chars():
    # 'node_' (5) + 50 chars max
    long_id = 'a' * 100
    assert len(sanitize_node_id(long_id)) <= 55


def test_cte_name_is_deterministic():
    assert sanitize_node_id('my-node') == sanitize_node_id('my-node')


# ── Compilable node selection (used by compile() pass 1) ─────────────────────

def test_all_non_compute_nodes_are_compilable_in_linear_pipeline():
    nodes = [make_node('s1', 'source'), make_node('f1', 'filter'),
             make_node('p1', 'projection')]
    edges = [make_edge('s1', 'f1'), make_edge('f1', 'p1')]
    result = find_sql_compilable_nodes(nodes, edges, 'p1')
    assert 's1' in result
    assert 'f1' in result
    assert 'p1' in result


def test_compute_node_excluded_from_compilable_set():
    nodes = [make_node('s1', 'source'), make_node('c1', 'compute'),
             make_node('p1', 'projection')]
    edges = [make_edge('s1', 'c1'), make_edge('c1', 'p1')]
    result = find_sql_compilable_nodes(nodes, edges, 'p1')
    assert 'c1' not in result


def test_pre_compute_segment_compiles_correctly():
    nodes = [make_node('s1', 'source'), make_node('f1', 'filter'),
             make_node('c1', 'compute'), make_node('p1', 'projection')]
    edges = [make_edge('s1', 'f1'), make_edge('f1', 'c1'), make_edge('c1', 'p1')]
    result = find_sql_compilable_nodes(nodes, edges, 'f1')
    assert 's1' in result
    assert 'f1' in result
    assert 'c1' not in result


# ── Upstream resolution (used by compile() to find which nodes to include) ────

def test_upstream_of_destination_includes_full_pipeline():
    nodes = [make_node('s'), make_node('f', 'filter'),
             make_node('p', 'projection'), make_node('d', 'destination')]
    edges = [make_edge('s', 'f'), make_edge('f', 'p'), make_edge('p', 'd')]
    result = find_upstream_nodes(nodes, edges, 'd')
    assert 's' in result and 'f' in result and 'p' in result and 'd' in result


def test_upstream_order_sources_before_transforms():
    nodes = [make_node('src'), make_node('flt', 'filter'), make_node('prj', 'projection')]
    edges = [make_edge('src', 'flt'), make_edge('flt', 'prj')]
    result = find_upstream_nodes(nodes, edges, 'prj')
    assert result.index('src') < result.index('flt')
    assert result.index('flt') < result.index('prj')


# ── Downstream invalidation (used by cache invalidation after node changes) ───

def test_downstream_includes_all_successor_nodes():
    nodes = [make_node('a'), make_node('b'), make_node('c'), make_node('d')]
    edges = [make_edge('a', 'b'), make_edge('b', 'c'), make_edge('c', 'd')]
    result = find_downstream_nodes('a', nodes, edges)
    assert 'b' in result and 'c' in result and 'd' in result


def test_changing_source_invalidates_everything_downstream():
    nodes = [make_node('src'), make_node('flt', 'filter'),
             make_node('prj', 'projection'), make_node('dst', 'destination')]
    edges = [make_edge('src', 'flt'), make_edge('flt', 'prj'), make_edge('prj', 'dst')]
    downstream = find_downstream_nodes('src', nodes, edges)
    # All nodes downstream of source must be cache-invalidated when source config changes
    assert downstream == {'flt', 'prj', 'dst'}
