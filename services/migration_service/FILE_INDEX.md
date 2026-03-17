# File index — purpose of each file

Every file name is chosen so its **purpose** is clear. Use this index when navigating the service.

---

## Entry

| File | Purpose |
|------|--------|
| `main.py` | FastAPI app: lifespan, middleware, mounts routers. Entry point to run the service. |

---

## Routers (HTTP only)

| File | Purpose |
|------|--------|
| `routers/migration_routes.py` | HTTP endpoints: validate pipeline, execute job, get status, cancel, health. Delegates to orchestrator/planner. |
| `routers/execution_state_routes.py` | HTTP endpoint: GET current execution state for a job (WebSocket catch-up). |

---

## Models

| File | Purpose |
|------|--------|
| `models.py` | Request/response DTOs: MigrationRequest, MigrationResponse, MigrationStatus, JobStatus, NodeProgress, PipelineConfig. |

---

## Orchestrator (execution)

| File | Purpose |
|------|--------|
| `orchestrator/migration_orchestrator.py` | MigrationOrchestrator: build pipeline, execute nodes (source/transform/destination), call extraction service and loaders. |
| `orchestrator/execute_pipeline_pushdown.py` | Runs SQL pushdown plan: validate → plan → create staging schema → execute levels → final INSERT → cleanup. |
| `orchestrator/execution_state.py` | In-memory execution state per job: node status, progress, TTL. Used for real-time progress. |
| `orchestrator/ws_emitter.py` | Emits WebSocket events: pipeline_started, node_started, node_completed, pipeline_completed, etc. |

---

## Planner (SQL generation)

| File | Purpose |
|------|--------|
| `planner/validation.py` | DAG validation: cycles, reachability, JOIN/destination rules. Raises PipelineValidationError. |
| `planner/materialization.py` | Decides which nodes get staging tables (branch ends, JOINs, pre-destination). |
| `planner/sql_compiler.py` | Compiles nodes to SQL: nested SELECTs, CREATE TABLE, JOINs. |
| `planner/execution_plan.py` | Builds full execution plan: levels, final INSERT, cleanup; save/get plan from DB. |
| `planner/filter_pushdown.py` | Filter pushdown logic (push filters to source where possible). |
| `planner/filter_optimizer.py` | Analyzes and optimizes filter pushdown (analyze_filter_pushdown). |
| `planner/calculated_column_pushdown.py` | Pushdown for calculated columns. |
| `planner/staging_naming.py` | Staging schema and table naming conventions. |
| `planner/metadata_generator.py` | Generates node metadata for plan/execution (e.g. for filter pushdown). |

---

## Lifecycle (validation-gated state)

| File | Purpose |
|------|--------|
| `lifecycle/state_machine.py` | Pipeline states (DRAFT → VALIDATED → SUCCESS/FAILED); validate_pipeline, can_execute, execute_validated_plan. |
| `lifecycle/validated_plan_storage.py` | Storage interface and InMemoryPipelineStorage for validated plans and state. |

---

## Loaders (destination DB)

| File | Purpose |
|------|--------|
| `loaders/postgres_loader.py` | Load data into PostgreSQL (COPY/executemany); create table from metadata. |
| `loaders/hana_loader.py` | Load data into SAP HANA; create table from metadata. |

---

## Utils

| File | Purpose |
|------|--------|
| `utils/business_name_remap.py` | Remap row keys from technical/db names to business names at destination write path. |
| `utils/temp_table_manager.py` | Create/drop temp tables for aggregations, joins, window functions. |

---

## Why these folders?

| Folder | Why needed |
|--------|------------|
| **routers** | Single place for HTTP routes; keeps main.py thin and separates transport from logic. |
| **orchestrator** | Execution flow: call planner, run SQL pushdown, track state, emit WebSocket. |
| **planner** | SQL generation and plan building; many small modules (validation, materialization, compiler, etc.) keep each concern clear. |
| **lifecycle** | Validation-gated execution (can’t run without valid plan); state machine + storage. |
| **loaders** | Destination-specific write path (PostgreSQL, HANA); same interface. |
| **utils** | Shared helpers used at destination boundary (remap, temp tables). |

The `api/` folder is **removed**; its route lives in `routers/execution_state_routes.py`.
