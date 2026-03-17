# Migration Service — Structure and Order

Standard layout: **Routers → Models → Services**. Routes live in `routers/`; business logic in service packages.  
**File names are purpose-clear.** See [FILE_INDEX.md](FILE_INDEX.md) for a one-line purpose of every file.

---

## Ordered layout (routers / models / services)

### 1. Entry

| File | Purpose |
|------|--------|
| `main.py` | FastAPI app: lifespan, middleware, **include_router** for routers. **Start here.** |

### 2. Routers (HTTP only)

| File | Purpose |
|------|--------|
| `routers/migration_routes.py` | Validate, execute, status, cancel, health. Delegates to services. |
| `routers/execution_state_routes.py` | GET execution state for WebSocket catch-up. |

### 3. Models

| File | Purpose |
|------|--------|
| `models.py` | Request/response and config models (e.g. `MigrationRequest`, `JobStatus`). |

### 4. Services — Orchestrator (execution)

| File | Purpose |
|------|--------|
| `orchestrator/migration_orchestrator.py` | `MigrationOrchestrator` — high-level execution coordination. |
| `orchestrator/execute_pipeline_pushdown.py` | Runs the SQL pushdown plan (validate → plan → execute levels → insert → cleanup). |
| `orchestrator/execution_state.py` | In-memory execution state (per-node status, progress). |
| `orchestrator/ws_emitter.py` | WebSocket events for progress (node started, completed, pipeline done). |

### 5. Services — Planner (SQL generation)

| File | Purpose |
|------|--------|
| `planner/validation.py` | DAG validation (cycles, reachability, JOIN/destination rules). |
| `planner/materialization.py` | Which nodes get staging tables (branch ends, JOINs, pre-destination). |
| `planner/sql_compiler.py` | Compiles nodes to SQL (nested SELECTs, CREATE TABLE, JOINs). |
| `planner/execution_plan.py` | Builds full execution plan (levels, final INSERT, cleanup). |
| `planner/filter_pushdown.py` | Filter pushdown logic. |
| `planner/filter_optimizer.py` | Filter optimization. |
| `planner/calculated_column_pushdown.py` | Calculated column pushdown. |
| `planner/staging_naming.py` | Staging schema/table naming. |
| `planner/metadata_generator.py` | Metadata for plan/execution. |

### 6. Services — Lifecycle (validation-gated state)

| File | Purpose |
|------|--------|
| `lifecycle/state_machine.py` | States: DRAFT → VALIDATED → SUCCESS/FAILED; `can_execute`, `validate_pipeline`, etc. |
| `lifecycle/validated_plan_storage.py` | Storage for validated plans and state (`InMemoryPipelineStorage`). |

### 7. Services — Loaders

| File | Purpose |
|------|--------|
| `loaders/postgres_loader.py` | PostgreSQL load/connection helpers. |
| `loaders/hana_loader.py` | SAP HANA load/connection helpers. |

### 8. Services — Utils

| File | Purpose |
|------|--------|
| `utils/business_name_remap.py` | Remap row keys to business names at destination. |
| `utils/temp_table_manager.py` | Temporary table creation/cleanup. |

---

## Why so many files and folders?

- **Planner** is split so each concern is one place: validation, materialization, SQL compilation, execution plan, filter/calculated-column pushdown, naming, metadata.
- **Orchestrator** separates: core orchestration, pushdown execution, execution state, and WebSocket emission.
- **Lifecycle** separates state machine (rules) from storage (persistence).
- **Root** keeps the app entry (`main.py`) and shared **models**. **Loaders** and **utils** live in `loaders/` and `utils/` subpackages.

So the count comes from **many small modules** rather than a few large ones. The flow is: **main** → **routers** (HTTP only) → **services** (orchestrator, planner, lifecycle, loaders, utils). `api/` is legacy; use `routers/` for new routes.

---

## Execution flow (order of operations)

1. **main.py** mounts **routers** (migration, execution state).
2. **Routers** receive HTTP requests and call **services** (orchestrator, planner, lifecycle).
3. **Orchestrator** (e.g. `execute_pipeline_pushdown`): uses planner, lifecycle; creates staging schema; runs SQL levels; updates execution_state and ws_emitter.
4. **routers/execution_state_routes** serves current run state to the frontend.

For more detail, see project `docs/` (e.g. `docs/EXECUTION_PROGRESS_TRACKING.md`, `docs/SQL_PUSHDOWN_IMPLEMENTATION.md`).

---

## Where does execution run? (Celery vs BackgroundTasks)

**Execute is a background task** — the HTTP request returns 202 immediately; the pipeline runs after the response.

| Mode | When it runs | How to use |
|------|----------------|------------|
| **Celery worker** | When Redis is available and a migration-service Celery worker is running. | Run from `services/migration_service`: `celery -A celery_app worker -l info`. Job state is shared via Redis so GET `/status` works. |
| **FastAPI BackgroundTasks** | When Celery enqueue fails (no Redis, or no worker). | No extra process; pipeline runs in the same FastAPI process after the 202 response. |

**Recommendation:** Use the **Celery worker** for production so execution is in a separate process (reliability, time limits, scaling). BackgroundTasks is the fallback so the service still works without Redis/Celery.

---

## Extraction service vs orchestration (migration) service

| Service | Port | Role | Used by |
|---------|------|------|--------|
| **Migration service (orchestration)** | 8003 | Validate pipeline, **execute** pipeline (SQL pushdown), status, cancel. Runs the actual migration. | Django (POST /execute, GET /status), frontend (validate, status). |
| **Extraction service** | 8001 | Extract data from source DBs (chunked), metadata/filter/join APIs. Runs extraction jobs in BackgroundTasks. | Django API (sources, pipeline, metadata, destinations) for table listing, preview, metadata. **Not used by the current execute path.** |
| **WebSocket server** | 8004 | Broadcast progress to the UI. | Migration service (and optionally others) POST progress; frontend subscribes. |

**Current execute path:** The migration service runs **only** the **SQL pushdown** pipeline (`execute_pipeline_pushdown`): it reads from source DBs with direct connections (e.g. `_execute_source_staging_query`), builds staging in the customer/destination DB, and writes the final result. It does **not** call the extraction service during execute.

**When extraction service is used:** Django and the UI call it for browsing sources, metadata, and previews. The **MigrationOrchestrator** class (in `orchestrator/migration_orchestrator.py`) can run a different path that calls the extraction service to pull data and then load it — but the migration router does **not** use that path for POST /execute. So today, **both services are used**, but for different things: orchestration = execute + validate; extraction = metadata and source browsing, not the running pipeline.
