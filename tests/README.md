# MigCockpit Test Suite

## The Point

Every time you change code, run the relevant test group and know instantly
whether something broke — **without needing a running database**.

## Test Structure

```
tests/
├── unit/                          ← No DB, no Django, runs in < 2 seconds total
│   ├── domain/
│   │   ├── test_column_lineage.py
│   │   ├── test_filter_conditions.py
│   │   ├── test_job_status.py
│   │   └── test_domain_complete.py    ← All domain objects (comprehensive)
│   ├── pipeline/
│   │   ├── test_graph_traversal.py    ← DAG traversal, cycle detection, pushdown
│   │   ├── test_expression_translator.py
│   │   └── test_preview_compiler.py   ← CTE compiler (mocked DB)
│   ├── connections/
│   │   ├── test_encryption.py         ← AES-GCM roundtrip, isolation, wrong-key
│   │   └── test_tenant_provisioning.py ← DB provisioning (mocked psycopg2)
│   └── ports/
│       └── test_port_contracts.py     ← All 7 ABCs + mock implementations
│
└── integration/                   ← Requires real PostgreSQL — auto-skipped if unavailable
    └── test_postgresql_connector.py
```

## Commands

```bash
# Tier A (PR required, fast)
python -m pytest tests/unit/ tests/test_node_regression_backend_matrix.py -v
cd frontend && npm run test -- --run src/pipeline-engine/__tests__/pipeline.integration.test.ts

# Tier B (merge/nightly, full)
python -m pytest tests/ -v
cd frontend && npm run test
cd frontend && npm run test:e2e

# Run all unit tests (always works, no DB needed)
python -m pytest tests/unit/ -v

# Run specific file
python -m pytest tests/unit/pipeline/test_graph_traversal.py -v

# Run integration tests (need real DB)
export TEST_PG_HOST=localhost TEST_PG_PORT=5432
export TEST_PG_USER=postgres TEST_PG_PASSWORD=secret TEST_PG_DB=postgres
python -m pytest tests/integration/ -v

# Run everything
python -m pytest tests/ -v
```

## Regression Matrix Artifacts

- Node-by-node scenario matrix: `tests/NODE_TEST_MATRIX.md`
- Backend matrix suite: `tests/test_node_regression_backend_matrix.py`
- Frontend integration suite: `frontend/src/pipeline-engine/__tests__/pipeline.integration.test.ts`
- Critical browser journeys: `frontend/e2e/critical-pipeline-flows.spec.ts`

## What to run after each change

| Changed file | Run this test |
|---|---|
| `core/pipeline/graph_traversal.py` | `tests/unit/pipeline/test_graph_traversal.py` |
| `core/pipeline/preview_compiler.py` | `tests/unit/pipeline/test_preview_compiler.py` |
| `core/pipeline/expression_translator.py` | `tests/unit/pipeline/test_expression_translator.py` |
| `core/connections/encryption.py` | `tests/unit/connections/test_encryption.py` |
| `core/connections/tenant_provisioning.py` | `tests/unit/connections/test_tenant_provisioning.py` |
| Anything in `domain/` | `tests/unit/domain/` |
| Anything in `ports/` | `tests/unit/ports/test_port_contracts.py` |
| Any connector in `services/extraction_service/connectors/` | `tests/integration/` |
| **Anything at all** | `tests/unit/` — always safe, always fast |
