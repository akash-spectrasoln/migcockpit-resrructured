# Moved from: api/services/adaptive_cache.py
"""
Adaptive Cache Manager (V2)

Historically this module was "disabled" and preview caching was handled by
CheckpointCacheManager. The test suite still imports and exercises AdaptiveCacheManagerV2,
so this file provides a compatibility implementation backed by the checkpoint cache.
"""

from enum import Enum
import hashlib
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Default threshold for whether a projection should be cached.
CHECKPOINT_DISTANCE_DEFAULT = 4


class NodeCost(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class CacheLayer(Enum):
    CHECKPOINT = "checkpoint"


class AdaptiveCacheManager:
    """Base cache manager (compat)."""

    def __init__(self, customer_db: str):
        self.customer_db = customer_db
        # Unit tests patch this method, so it must exist.
        self._ensure_schema_exists()

    def _ensure_schema_exists(self) -> None:
        """No-op placeholder for backward compatibility."""

    def should_cache(self, *args: Any, **kwargs: Any) -> tuple[bool, CacheLayer]:
        return False, CacheLayer.CHECKPOINT

    def get_cache(self, *args: Any, **kwargs: Any) -> Optional[dict[str, Any]]:
        return None

    def save_cache(self, *args: Any, **kwargs: Any) -> bool:
        return False

    def invalidate_cache(self, *args: Any, **kwargs: Any) -> None:
        return None

    def get_cached_node_ids_for_pipeline(self, *args: Any, **kwargs: Any) -> set[str]:
        return set()

    def _compute_node_hash(self, node_id: str, config: dict[str, Any]) -> str:
        import json

        config_str = json.dumps(config or {}, sort_keys=True)
        return hashlib.sha256(f"{node_id}:{config_str}".encode()).hexdigest()

    def _compute_upstream_hash(self, upstream_node_ids: list[str], upstream_hashes: dict[str, str]) -> str:
        """
        Stable upstream hash:
        - If no upstream nodes: returns empty string (tests expect this exact value).
        - Otherwise sha256 over sorted "node_id:upstream_node_hash" pairs.
        """
        if not upstream_node_ids:
            return ""
        u_str = ":".join(sorted([f"{nid}:{upstream_hashes.get(nid, '')}" for nid in upstream_node_ids]))
        return hashlib.sha256(u_str.encode()).hexdigest()

    def invalidate_downstream_caches(self, *args: Any, **kwargs: Any) -> set[str]:
        return set()


class AdaptiveCacheManagerV2(AdaptiveCacheManager):
    """
    Compatibility implementation of AdaptiveCacheManagerV2 used by tests.

    Node-level caching decisions are implemented in `should_cache`.
    Stored cache entries are materialized as physical tables via CheckpointCacheManager.
    """

    # Nodes that are always cached (matches CheckpointCacheManager complexity checkpoints).
    _ALWAYS_CACHED_NODE_TYPES = {"join", "aggregate", "source", "window", "sort", "compute"}

    def should_cache(
        self,
        node_id: str,
        node_type: str,
        node_config: dict[str, Any],
        depth_since_last_cache: int = 0,
        checkpoint_distance_n: int = CHECKPOINT_DISTANCE_DEFAULT,
    ) -> tuple[bool, CacheLayer]:
        nt = (node_type or "").lower()

        if nt in self._ALWAYS_CACHED_NODE_TYPES:
            return True, CacheLayer.CHECKPOINT

        if nt == "projection":
            return depth_since_last_cache >= checkpoint_distance_n, CacheLayer.CHECKPOINT

        return False, CacheLayer.CHECKPOINT

    def _checkpoint_manager(self, pipeline_id: str):
        # Import here to avoid circular imports at module load time.
        from api.services.checkpoint_cache import CheckpointCacheManager

        return CheckpointCacheManager(self.customer_db, pipeline_id)

    def invalidate_cache(self, pipeline_id: str, node_id: Optional[str] = None) -> None:
        from django.conf import settings
        import psycopg2

        checkpoint_mgr = self._checkpoint_manager(pipeline_id)

        if node_id is None:
            checkpoint_mgr.cleanup_schema()
            return

        schema_name = checkpoint_mgr.schema_name
        node_table = f"node_{str(node_id).replace('-', '_')}_cache"

        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(
                host=settings.DATABASES["default"]["HOST"],
                port=settings.DATABASES["default"]["PORT"],
                user=settings.DATABASES["default"]["USER"],
                password=settings.DATABASES["default"]["PASSWORD"],
                database=self.customer_db,
            )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(f'DROP TABLE IF EXISTS "{schema_name}"."{node_table}"')
            cursor.execute(
                f'DELETE FROM "{schema_name}"."_checkpoint_metadata" WHERE node_id = %s',
                (str(node_id),),
            )
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def save_cache(
        self,
        pipeline_id: str,
        node_id: str,
        node_type: str,
        node_config: dict[str, Any],
        rows: Optional[list[dict[str, Any]]],
        columns: Optional[list[dict[str, Any]]],
        upstream_node_ids: list[str],
        upstream_hashes: dict[str, str],
        cache_cost_score: int,
    ) -> bool:
        from api.services.checkpoint_cache import CheckpointCacheManager

        _ = cache_cost_score  # currently unused by the checkpoint cache backend
        checkpoint_mgr = self._checkpoint_manager(pipeline_id)
        upstream_version_hash = self._compute_upstream_hash(upstream_node_ids, upstream_hashes)

        return checkpoint_mgr.save_checkpoint(
            node_id=str(node_id),
            node_type=str(node_type),
            node_config=node_config or {},
            upstream_version_hash=upstream_version_hash,
            columns=columns or [],
            rows=rows if rows is not None else None,
        )

    def get_cache(
        self,
        pipeline_id: str,
        node_id: str,
        node_version_hash: str,
        upstream_version_hash: str,
    ) -> Optional[dict[str, Any]]:
        from django.conf import settings
        import psycopg2

        checkpoint_mgr = self._checkpoint_manager(pipeline_id)
        checkpoint = checkpoint_mgr.get_valid_checkpoint(str(node_id), node_version_hash, upstream_version_hash)
        if not checkpoint:
            return None

        table_ref = checkpoint["table_ref"]

        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(
                host=settings.DATABASES["default"]["HOST"],
                port=settings.DATABASES["default"]["PORT"],
                user=settings.DATABASES["default"]["USER"],
                password=settings.DATABASES["default"]["PASSWORD"],
                database=self.customer_db,
            )
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table_ref}")
            col_names = [d[0] for d in (cursor.description or [])]
            raw_rows = cursor.fetchall() or []
            dict_rows = [dict(zip(col_names, row)) for row in raw_rows]

            return {
                "rows": dict_rows,
                "metadata": {
                    "row_count": len(dict_rows),
                    "columns": checkpoint.get("columns", []),
                },
            }
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def find_nearest_cached_ancestor(
        self,
        pipeline_id: str,
        target_node_id: str,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> tuple[Optional[str], Optional[dict[str, Any]]]:
        from django.conf import settings
        import psycopg2

        checkpoint_mgr = self._checkpoint_manager(pipeline_id)
        ancestor_id, checkpoint = checkpoint_mgr.find_nearest_checkpoint(str(target_node_id), nodes, edges)
        if not ancestor_id or not checkpoint:
            return None, None

        table_ref = checkpoint["table_ref"]

        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(
                host=settings.DATABASES["default"]["HOST"],
                port=settings.DATABASES["default"]["PORT"],
                user=settings.DATABASES["default"]["USER"],
                password=settings.DATABASES["default"]["PASSWORD"],
                database=self.customer_db,
            )
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table_ref}")
            col_names = [d[0] for d in (cursor.description or [])]
            raw_rows = cursor.fetchall() or []
            dict_rows = [dict(zip(col_names, row)) for row in raw_rows]

            return ancestor_id, {"rows": dict_rows, "metadata": {"row_count": len(dict_rows)}}
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


def get_adaptive_cache_manager(customer) -> AdaptiveCacheManager:
    """Returns an inert instance of AdaptiveCacheManager."""
    db_name = customer.cust_db if hasattr(customer, "cust_db") else str(customer)
    return AdaptiveCacheManager(db_name)


def get_adaptive_cache_manager_v2(customer) -> AdaptiveCacheManagerV2:
    """Returns an AdaptiveCacheManagerV2 instance."""
    db_name = customer.cust_db if hasattr(customer, "cust_db") else str(customer)
    return AdaptiveCacheManagerV2(db_name)
