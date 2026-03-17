"""
Execution plan domain objects.
Represents the compiled plan before actual execution begins.
Pure Python — zero framework imports.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class ExecutionStepType(Enum):
    SQL_QUERY    = 'sql_query'      # Single SQL CTE query
    COMPUTE      = 'compute'        # Python compute node
    LOAD         = 'load'           # Write to destination

@dataclass
class PushdownDecision:
    filter_node_id: str
    target_node_id: str
    conditions: list[dict[str, Any]]

@dataclass
class ExecutionStep:
    node_id: str
    step_type: ExecutionStepType
    sql: Optional[str] = None
    params: list[Any] = field(default_factory=list)
    depends_on: list[str] = field(default_factory=list)

@dataclass
class ExecutionPlan:
    canvas_id: int
    steps: list[ExecutionStep] = field(default_factory=list)
    pushdown_decisions: list[PushdownDecision] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
