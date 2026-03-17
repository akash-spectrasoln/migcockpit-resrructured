"""
ICacheStore port.
Abstracts caching of node preview results and execution checkpoints.
"""
from abc import ABC, abstractmethod
from typing import Any, Optional


class ICacheStore(ABC):

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Return cached value or None if not found / expired."""

    @abstractmethod
    def set(self, key: str, value: Any, ttl_seconds: int = 3600) -> None:
        """Store value. ttl_seconds=0 means no expiry."""

    @abstractmethod
    def invalidate(self, key: str) -> None:
        """Delete a single cache key."""

    @abstractmethod
    def invalidate_canvas(self, canvas_id: int) -> None:
        """Invalidate all cached data for an entire canvas."""
