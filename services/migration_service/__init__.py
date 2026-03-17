"""
Migration Service — SQL pushdown ETL orchestration.

Structure (see README.md for full layout):
  - main.py          Entry (FastAPI app)
  - models.py        Request/response models
  - planner/         DAG validation, SQL compilation, execution plan
  - orchestrator/    Pipeline execution, state, WebSocket
  - lifecycle/       Validation-gated state machine
  - api/             Execution state HTTP API
  - *_loader.py      DB loaders (postgres, hana)
  - *_utils.py       Shared utilities
"""

__all__ = []
