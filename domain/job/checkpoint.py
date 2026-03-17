"""
Checkpoint value object — represents a resume point in a long-running migration.
Pure Python — zero framework imports.
"""
from dataclasses import dataclass
from typing import Any


@dataclass
class Checkpoint:
    """
    Immutable resume point for a migration job.
    Stores enough information to resume execution from a specific node
    without re-reading upstream data from the source database.
    """
    job_id: str
    node_id: str
    table_ref: str               # Physical staging table name
    columns: list[dict[str, Any]]
    row_count: int = 0
    config_hash: str = ''
