"""
Column metadata and lineage tracking domain objects.
Pure Python — zero framework imports.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class ColumnMetadata:
    name: str
    technical_name: str
    business_name: str
    datatype: str
    nullable: bool = True
    source: str = 'base'          # 'base' | 'calculated' | 'aggregate'
    db_name: Optional[str] = None # actual DB column name for pushdown rewriting
    expression: Optional[str] = None

@dataclass
class ColumnLineage:
    """Tracks where a column originated in the pipeline DAG."""
    technical_name: str
    origin_node_id: str
    origin_type: str              # SOURCE | JOIN | PROJECTION | AGGREGATE | COMPUTE | CHECKPOINT
    expression: Optional[str] = None
    origin_branch: Optional[str] = None  # 'left' | 'right' for JOIN columns
