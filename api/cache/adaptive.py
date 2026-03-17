# Moved from: api/services/adaptive_cache.py
"""
Adaptive Node Cache Manager (DISABLED)
Bypassed in favor of CheckpointCacheManager.
"""
from enum import Enum
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class NodeCost(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"

class CacheLayer(Enum):
    CHECKPOINT = "checkpoint"

class AdaptiveCacheManager:
    """Inert AdaptiveCacheManager for backward compatibility."""
    def __init__(self, customer_db: str):
        self.customer_db = customer_db
    def should_cache(self, *args, **kwargs) -> tuple[bool, CacheLayer]:
        return False, CacheLayer.CHECKPOINT
    def get_cache(self, *args, **kwargs) -> Optional[dict]:
        return None
    def save_cache(self, *args, **kwargs) -> bool:
        return False
    def invalidate_cache(self, *args, **kwargs):
        pass
    def get_cached_node_ids_for_pipeline(self, *args, **kwargs) -> set:
        return set()
    def _compute_node_hash(self, node_id, config) -> str:
        import hashlib
        import json
        config_str = json.dumps(config, sort_keys=True)
        return hashlib.sha256(f"{node_id}:{config_str}".encode()).hexdigest()
    def _compute_upstream_hash(self, *args, **kwargs) -> str:
        return ""
    def invalidate_downstream_caches(self, *args, **kwargs) -> set:
        return set()

class AdaptiveCacheManagerV2(AdaptiveCacheManager):
    """Inert AdaptiveCacheManagerV2 for backward compatibility."""
    pass

def get_adaptive_cache_manager(customer) -> AdaptiveCacheManager:
    """Returns an inert instance of AdaptiveCacheManager."""
    db_name = customer.cust_db if hasattr(customer, 'cust_db') else str(customer)
    return AdaptiveCacheManager(db_name)

def get_adaptive_cache_manager_v2(customer) -> AdaptiveCacheManagerV2:
    """Returns an inert instance of AdaptiveCacheManagerV2."""
    db_name = customer.cust_db if hasattr(customer, 'cust_db') else str(customer)
    return AdaptiveCacheManagerV2(db_name)
