"""
Job state store: Redis-backed when available so FastAPI and Celery worker share state.
GET /status reads from here; Celery task updates here.
Fallback: in-memory dict when Redis is not configured or unavailable.
"""

import json
import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

_REDIS_URL = os.getenv("REDIS_URL") or os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
_KEY_PREFIX = "migration:job:"
_TTL_SECONDS = 86400 * 2  # 2 days

_in_memory_store: dict[str, dict[str, Any]] = {}
_redis_client = None

def _get_redis():
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    try:
        import redis
        _redis_client = redis.from_url(_REDIS_URL, decode_responses=True)
        _redis_client.ping()
        logger.info("[job_store] Using Redis for job state (shared with Celery worker)")
        return _redis_client
    except Exception as e:
        logger.warning("[job_store] Redis unavailable (%s), using in-memory store", e)
        _redis_client = None
        return None

def get_job(job_id: str) -> Optional[dict[str, Any]]:
    """Get job state by id. Returns None if not found."""
    r = _get_redis()
    if r:
        try:
            raw = r.get(_KEY_PREFIX + job_id)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception as e:
            logger.warning("[job_store] Redis get failed: %s", e)
            return _in_memory_store.get(job_id)
    return _in_memory_store.get(job_id)

def set_job(job_id: str, data: dict[str, Any]) -> None:
    """Set full job state. Enums and other non-JSON types are serialized via default=str."""
    r = _get_redis()
    if r:
        try:
            r.setex(
                _KEY_PREFIX + job_id,
                _TTL_SECONDS,
                json.dumps(data, default=str),
            )
            return
        except Exception as e:
            logger.warning("[job_store] Redis set failed: %s", e)
    _in_memory_store[job_id] = dict(data)

def update_job(job_id: str, **kwargs: Any) -> None:
    """Merge kwargs into existing job state. Creates job if missing."""
    current = get_job(job_id) or {}
    current.update(kwargs)
    set_job(job_id, current)

def delete_job(job_id: str) -> None:
    """Remove job state (e.g. after TTL or cancel)."""
    r = _get_redis()
    if r:
        try:
            r.delete(_KEY_PREFIX + job_id)
            return
        except Exception as e:
            logger.warning("[job_store] Redis delete failed: %s", e)
    _in_memory_store.pop(job_id, None)

def list_job_ids() -> list:
    """List known job ids (for in-memory fallback; Redis can use SCAN if needed)."""
    r = _get_redis()
    if r:
        try:
            keys = r.keys(_KEY_PREFIX + "*")
            return [k.replace(_KEY_PREFIX, "") for k in keys]
        except Exception as e:
            logger.warning("[job_store] Redis keys failed: %s", e)
    return list(_in_memory_store.keys())

def get_job_store() -> "JobStore":
    """Return a small facade so routers can use store.get/set/update."""
    return JobStore()

class JobStore:
    """Facade for get/set/update job state."""

    def get(self, job_id: str) -> Optional[dict[str, Any]]:
        return get_job(job_id)

    def set(self, job_id: str, data: dict[str, Any]) -> None:
        set_job(job_id, data)

    def update(self, job_id: str, **kwargs: Any) -> None:
        update_job(job_id, **kwargs)

    def delete(self, job_id: str) -> None:
        delete_job(job_id)

    def __contains__(self, job_id: str) -> bool:
        return get_job(job_id) is not None
