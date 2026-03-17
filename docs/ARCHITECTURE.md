# MigCockpit Architecture

## Layer Overview

```
domain/          — Pure Python domain objects. Zero framework imports.
ports/           — Abstract interfaces (ABCs) defining contracts.
api/pipeline/    — SQL compiler (preview), graph traversal, expression translator.
api/connections/ — DB connectors, encryption, tenant provisioning.
api/cache/       — Node result cache, checkpoint cache, adaptive cache.
api/views/       — Thin HTTP handlers. Each calls one use case.
services/        — FastAPI microservices (extraction, migration, websocket).
frontend/        — React + TypeScript + Chakra UI.
```

## Key Design Decisions

1. **Domain layer is framework-free.** `domain/` has zero Django/FastAPI imports.
   Test it with `python3 -m pytest tests/unit/domain/` — no database needed.

2. **Two SQL compilers exist intentionally:**
   - `api/pipeline/preview_compiler.py` — CTE-based, for live node preview in Django
   - `services/migration_service/planner/sql_compiler.py` — full execution compiler

3. **Deprecated re-export stubs** — files in `api/utils/` and `api/services/` now
   contain only `from <new_location> import *`. Clean them up by updating imports
   to point directly to `api/pipeline/`, `api/connections/`, or `api/cache/`.

4. **Per-customer tenant databases** — each Customer gets an isolated PostgreSQL DB.
   Provisioning is handled by `api/connections/tenant_provisioning.TenantProvisioningService`.

## See Also
- CANVAS_ARCHITECTURE.md — how the pipeline DAG works
- SQL_COMPILATION_ARCHITECTURE.md — the 3-pass compiler algorithm
- DEVELOPER_GUIDE.md — local setup and running services
