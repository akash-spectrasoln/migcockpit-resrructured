"""
DB loaders for migration destination (PostgreSQL, HANA).
"""

from .postgres_loader import PostgresLoader

try:
    from .hana_loader import HanaLoader
except ImportError:
    HanaLoader = None  # type: ignore[misc, assignment]

__all__ = ["PostgresLoader", "HanaLoader"]
