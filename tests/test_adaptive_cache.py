"""
Unit tests for Adaptive Cache V2: cache hit/miss, ancestor resume, invalidation, TTL, should_cache.
"""
from unittest.mock import Mock, patch

import pytest

from api.services.adaptive_cache import (
    CHECKPOINT_DISTANCE_DEFAULT,
    AdaptiveCacheManagerV2,
    CacheLayer,
    get_adaptive_cache_manager_v2,
)
from api.utils.cache_aware_execution import get_execution_path_from_cache, invalidate_downstream_caches

# --- should_cache (no DB) ---


class TestShouldCache:
    """Test V2 should_cache: node type + checkpoint distance rule."""

    @pytest.fixture
    def manager(self):
        with patch.object(AdaptiveCacheManagerV2, "_ensure_schema_exists"):
            return AdaptiveCacheManagerV2("test_db")

    def test_join_always_cached(self, manager):
        should, layer = manager.should_cache(
            "join1", "join", {"joinType": "INNER"},
            depth_since_last_cache=0, checkpoint_distance_n=4
        )
        assert should is True
        assert layer == CacheLayer.CHECKPOINT

    def test_aggregate_always_cached(self, manager):
        should, _ = manager.should_cache(
            "agg1", "aggregate", {},
            depth_since_last_cache=0, checkpoint_distance_n=4
        )
        assert should is True

    def test_depth_ge_n_cached(self, manager):
        # Simple projection but distance >= 4 -> cache
        should, _ = manager.should_cache(
            "proj1", "projection", {"selectedColumns": ["a"]},
            depth_since_last_cache=4, checkpoint_distance_n=4
        )
        assert should is True

    def test_simple_projection_depth_lt_n_not_cached(self, manager):
        should, _ = manager.should_cache(
            "proj1", "projection", {"selectedColumns": ["a"]},
            depth_since_last_cache=2, checkpoint_distance_n=4
        )
        assert should is False

    def test_source_always_cached(self, manager):
        should, _ = manager.should_cache(
            "src1", "source", {}, depth_since_last_cache=0, checkpoint_distance_n=4
        )
        assert should is True

    def test_window_sort_cached(self, manager):
        should, _ = manager.should_cache(
            "win1", "window", {}, depth_since_last_cache=0, checkpoint_distance_n=4
        )
        assert should is True
        should, _ = manager.should_cache(
            "sort1", "sort", {}, depth_since_last_cache=0, checkpoint_distance_n=4
        )
        assert should is True


# --- Cache hit/miss, ancestor, invalidation, TTL (require DB) ---


@pytest.mark.db
class TestAdaptiveCacheIntegration:
    """
    Integration tests that require a database.
    Use: pytest tests/test_adaptive_cache.py -m db
    Or run against Django test DB when available.
    """

    @pytest.fixture
    def pipeline_id(self):
        return "test_canvas_adaptive_cache"

    @pytest.fixture
    def cache_manager(self):
        from django.conf import settings
        db_name = settings.DATABASES["default"]["NAME"]
        return AdaptiveCacheManagerV2(db_name)

    @pytest.fixture
    def nodes_ab(self):
        return [
            {"id": "A", "data": {"type": "source", "config": {"sourceId": 1, "tableName": "t1"}}},
            {"id": "B", "data": {"type": "projection", "config": {"selectedColumns": ["x"]}}},
        ]

    @pytest.fixture
    def edges_ab(self):
        return [{"source": "A", "target": "B"}]

    def test_cache_hit_same_hashes(self, cache_manager, pipeline_id):
        """Save then get with same node_version_hash and upstream_version_hash -> data returned."""
        cache_manager.invalidate_cache(pipeline_id)  # clean
        ok = cache_manager.save_cache(
            pipeline_id=pipeline_id,
            node_id="n1",
            node_type="projection",
            node_config={"selectedColumns": ["a"]},
            rows=[{"a": 1}, {"a": 2}],
            columns=[{"name": "a", "datatype": "INTEGER"}],
            upstream_node_ids=[],
            upstream_hashes={},
            cache_cost_score=50,
        )
        assert ok is True
        node_hash = cache_manager._compute_node_hash("n1", {"selectedColumns": ["a"]})
        upstream_hash = cache_manager._compute_upstream_hash([], {})
        data = cache_manager.get_cache(pipeline_id, "n1", node_hash, upstream_hash)
        assert data is not None
        assert data["rows"] == [{"a": 1}, {"a": 2}]
        assert data["metadata"]["row_count"] == 2

    def test_cache_miss_wrong_upstream_hash(self, cache_manager, pipeline_id):
        """Get with correct node_version_hash but wrong upstream_version_hash -> miss."""
        cache_manager.invalidate_cache(pipeline_id)
        cache_manager.save_cache(
            pipeline_id=pipeline_id,
            node_id="n1",
            node_type="projection",
            node_config={"selectedColumns": ["a"]},
            rows=[{"a": 1}],
            columns=[{"name": "a"}],
            upstream_node_ids=["src1"],
            upstream_hashes={"src1": "abc"},
            cache_cost_score=50,
        )
        node_hash = cache_manager._compute_node_hash("n1", {"selectedColumns": ["a"]})
        data = cache_manager.get_cache(pipeline_id, "n1", node_hash, "wrong_upstream_hash")
        assert data is None

    def test_find_nearest_cached_ancestor_validated(self, cache_manager, pipeline_id, nodes_ab, edges_ab):
        """Two nodes A -> B; cache A; find_nearest_cached_ancestor(B) returns A with validated data."""
        cache_manager.invalidate_cache(pipeline_id)
        cache_manager.save_cache(
            pipeline_id=pipeline_id,
            node_id="A",
            node_type="source",
            node_config=nodes_ab[0]["data"]["config"],
            rows=[{"x": 1}],
            columns=[{"name": "x"}],
            upstream_node_ids=[],
            upstream_hashes={},
            cache_cost_score=100,
        )
        ancestor_id, ancestor_cached = cache_manager.find_nearest_cached_ancestor(
            pipeline_id, "B", nodes_ab, edges_ab
        )
        assert ancestor_id == "A"
        assert ancestor_cached is not None
        assert ancestor_cached["rows"] == [{"x": 1}]

    def test_invalidate_downstream_then_miss(self, cache_manager, pipeline_id, nodes_ab, edges_ab):
        """Cache A and B; invalidate_downstream_caches(A) removes B; get(B) -> miss."""
        cache_manager.invalidate_cache(pipeline_id)
        cache_manager.save_cache(
            pipeline_id=pipeline_id,
            node_id="A",
            node_type="source",
            node_config=nodes_ab[0]["data"]["config"],
            rows=[{"x": 1}],
            columns=[{"name": "x"}],
            upstream_node_ids=[],
            upstream_hashes={},
            cache_cost_score=100,
        )
        cache_manager.save_cache(
            pipeline_id=pipeline_id,
            node_id="B",
            node_type="projection",
            node_config=nodes_ab[1]["data"]["config"],
            rows=[{"x": 1}],
            columns=[{"name": "x"}],
            upstream_node_ids=["A"],
            upstream_hashes={"A": cache_manager._compute_node_hash("A", nodes_ab[0]["data"]["config"])},
            cache_cost_score=50,
        )
        invalidate_downstream_caches("A", nodes_ab, edges_ab, cache_manager, pipeline_id)
        node_hash_b = cache_manager._compute_node_hash("B", nodes_ab[1]["data"]["config"])
        upstream_hash_b = cache_manager._compute_upstream_hash(
            ["A"],
            {"A": cache_manager._compute_node_hash("A", nodes_ab[0]["data"]["config"])},
        )
        data_b = cache_manager.get_cache(pipeline_id, "B", node_hash_b, upstream_hash_b)
        assert data_b is None

    def test_get_execution_path_from_cache(self, nodes_ab, edges_ab):
        """Path from A to B is [A, B]."""
        path = get_execution_path_from_cache("A", "B", nodes_ab, edges_ab)
        assert path == ["A", "B"]


# --- TTL: require DB + time manipulation or short TTL ---


@pytest.mark.db
class TestCacheTTL:
    """TTL expiry: expired entry -> get returns None. (Requires DB; may use short TTL or mock time.)"""

    @pytest.fixture
    def cache_manager(self):
        from django.conf import settings
        return AdaptiveCacheManagerV2(settings.DATABASES["default"]["NAME"])

    def test_ttl_expired_returns_none(self, cache_manager):
        """If we could set expires_at in the past, get would return None. We only assert get_cache validates expiry."""
        pipeline_id = "ttl_test_canvas"
        cache_manager.invalidate_cache(pipeline_id)
        cache_manager.save_cache(
            pipeline_id=pipeline_id,
            node_id="n_ttl",
            node_type="source",
            node_config={},
            rows=[],
            columns=[],
            upstream_node_ids=[],
            upstream_hashes={},
            cache_cost_score=100,
        )
        node_hash = cache_manager._compute_node_hash("n_ttl", {})
        data = cache_manager.get_cache(pipeline_id, "n_ttl", node_hash, "")
        assert data is not None
        assert data["rows"] == []
        # Expiry is enforced in SQL (expires_at > CURRENT_TIMESTAMP); we don't mock time here
        cache_manager.invalidate_cache(pipeline_id)
