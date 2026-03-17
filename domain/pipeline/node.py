"""
Pipeline Node domain objects.
Pure Python — zero framework imports.
"""
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class NodeType(Enum):
    SOURCE             = 'source'
    FILTER             = 'filter'
    PROJECTION         = 'projection'
    JOIN               = 'join'
    CALCULATED_COLUMN  = 'calculated_column'
    AGGREGATE          = 'aggregate'
    COMPUTE            = 'compute'
    DESTINATION        = 'destination'

@dataclass
class Node:
    id: str
    node_type: NodeType
    config: dict[str, Any]
    position_x: float = 0.0
    position_y: float = 0.0
    business_name: str = ''
    technical_name: str = ''

    def is_sql_compilable(self) -> bool:
        """Compute nodes are execution boundaries — not SQL compilable."""
        return self.node_type != NodeType.COMPUTE

@dataclass
class Edge:
    id: str
    source_node_id: str
    target_node_id: str
    source_handle: Optional[str] = None
    target_handle: Optional[str] = None
