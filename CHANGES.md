# MigCockpit — Refactoring Changes

## What was done to this codebase

### Phase 1 — Root Cleanup
- Deleted: check_metadata_table.py, check_saved_plan.py, check_source_columns.py,
  list_tables.py, list_tables_standalone.py, original_file.tsx, db.sqlite3, test_compile.py
- Deleted: .gemini/ folder (AI session notes)
- Deleted: 8 root-level session-note markdown files
- Created: scripts/debug/ folder
- Updated: .gitignore (sqlite3, local_settings, pycache)

### Phase 2 — Settings
- Added: datamigrationapi/local_settings.py (gitignored dev overrides)
- Added: local_settings import at bottom of settings.py
- Added: datamigrationapi/SETTINGS_README.md

### Phase 3A — Views cleanup
- Merged: project_views.py + projects.py → projects.py
- Merged: node_addition.py + node_management.py + node_cache_views.py → nodes.py
- Merged: expression.py + expression_testing.py + expression_validation.py → expressions.py
- Renamed: canvas_views.py → canvas.py
- Renamed: migration_views.py → migration.py
- Renamed: metadata_views.py → metadata.py
- Renamed: misc.py → query_parser.py (XML query parsing + aggregate SQL helpers)

### Phase 3B — Sub-module creation
- Created: api/pipeline/ (sql compiler, graph traversal, filters, expressions, etc.)
- Created: api/connections/ (db connectors, encryption, tenant provisioning)
- Created: api/cache/ (checkpoint, node cache, adaptive cache)
- Moved: hana_connection/ → services/extraction_service/connectors/
- Moved: encryption/encryption.py → api/connections/encryption_legacy.py
- Moved: fetch_sqlserver/ → api/connections/sqlserver_fetch.py
- NOTE: Old utils/ and services/ files contain deprecated re-export stubs.
  Update your imports to point directly to api/pipeline/, api/connections/, api/cache/.

### Phase 4 — Customer Model
- Extracted: Customer.create_customer_database() + create_customer_schemas()
  → api/connections/tenant_provisioning.TenantProvisioningService
- Updated: Customer.save() calls TenantProvisioningService().provision(self) for new customers
- Old methods in Customer model marked DEPRECATED

### Phase 5A — Frontend Chakra Migration
- Renamed: LoginPageChakra → LoginPage (old LoginPage deleted)
- Renamed: DashboardPageChakra → DashboardPage (old deleted)
- Renamed: DataFlowCanvasChakra → DataFlowCanvas (stub deleted)
- Renamed: EnhancedFilterConfigPanel → FilterConfigPanel (old deleted)
- Renamed: NodeConfigPanelChakra → NodeConfigPanel
- Renamed: NodePaletteChakra → NodePalette
- Renamed: NodeTypesChakra → NodeTypes
- Deleted: components/Canvas/legacy/ folder
- Deleted: build_errors.txt, errors.txt, tsc_output*.txt, deleted_func.txt

### Phase 5B — Canvas Organisation
- Created: components/canvas/nodes/ (8 node renderer components)
- Created: components/canvas/panels/ (12 config panel components)
- Created: components/canvas/interactions/ (8 edge/menu/modal components)
- Created: components/canvas/sidebar/ (2 sidebar components)
- Created: barrel index.ts files for each sub-folder
- NOTE: Original components/Canvas/ files still exist — update imports to use
  the new components/canvas/ paths.

### Phase 6 — Consolidate + Rename
- Split: api/pipeline/sql_compiler.py → api/pipeline/preview_compiler.py
  (Two compilers kept intentionally — preview vs execution are different algorithms)
- Merged: filter_optimizer.py + filter_pushdown.py → filter_pushdown.py
- Renamed: execute_pipeline_pushdown.py → pipeline_executor.py
- Renamed: ws_emitter.py → progress_emitter.py
- Renamed: services/websocket_server/ → services/websocket_service/
- Consolidated: encryption/encryption.py + api/services/encryption_service.py
  → api/connections/encryption.py (were identical)

### Phase 7 — Domain Layer (NEW)
- Created: domain/pipeline/node.py (Node, NodeType, Edge)
- Created: domain/pipeline/column.py (ColumnMetadata, ColumnLineage)
- Created: domain/pipeline/filter.py (FilterCondition, FilterOperator, FilterGroup)
- Created: domain/pipeline/execution_plan.py (ExecutionPlan, ExecutionStep)
- Created: domain/job/migration_job.py (MigrationJob, JobStatus)
- Created: domain/job/checkpoint.py (Checkpoint)
- Created: domain/connection/source.py (Source, SourceType)
- Created: domain/connection/credential.py (Credential)
- Created: domain/tenant/customer.py (Customer)
- Created: domain/exceptions.py (all domain exceptions)
- VERIFIED: All domain files import with zero Django dependencies

### Phase 8 — Ports Layer (NEW)
- Created: ports/source_connector.py (ISourceConnector ABC)
- Created: ports/pipeline_repository.py (IPipelineRepository ABC)
- Created: ports/job_repository.py (IJobRepository ABC)
- Created: ports/cache_store.py (ICacheStore ABC)
- Created: ports/progress_notifier.py (IProgressNotifier ABC)
- Created: ports/encryption_service.py (IEncryptionService ABC)
- Created: ports/tenant_repository.py (ITenantRepository ABC)
- VERIFIED: All ports import with zero Django dependencies

### Phase 9 — Tests + Docs
- Created: tests/unit/domain/test_column_lineage.py (5 tests, all pass)
- Created: tests/unit/domain/test_filter_conditions.py (5 tests, all pass)
- Created: tests/unit/domain/test_job_status.py (5 tests, all pass)
- VERIFIED: 15/15 unit tests pass with zero database setup
- Deleted: 49 session-note markdown files from docs/
- Kept: 9 real documentation files in docs/
- Added: docs/ARCHITECTURE.md (new — explains the layer structure)

## What still needs to be done (future work)
1. Update all import statements that still use the old api/utils/ and api/services/ paths
   to point directly to api/pipeline/, api/connections/, api/cache/
2. Update frontend imports that reference components/Canvas/ to use components/canvas/
3. Implement concrete adapters that implement the ports (ISourceConnector for each DB type)
4. Wire use cases using the ports (see architecture plan documents)

---

## Naming Pass (applied on top of refactoring)

### Core renames
- `api/` → `core/`  (name now reflects purpose: core business logic, not a protocol)
- `api_admin/` → `core_admin/`
- `ApiConfig` → `CoreConfig` in apps.py
- All Python imports updated: `from api.` → `from core.`
- INSTALLED_APPS updated in settings.py

### Service file renames
- `postgresql_connector.py` → `postgresql.py`
- `mysql_connector.py` → `mysql.py`
- `oracle_connector.py` → `oracle.py`
- `sqlserver_connector.py` → `sqlserver.py`
- `hana_service.py` + `hana_main.py` + `hana_models.py` → `hana.py` (single file)

### Frontend renames
- `components/Canvas/` → `components/canvas/` (lowercase, Python convention)
- Old `Canvas/` folder removed — `canvas/` with sub-folders is the only copy

### Cleanup
- `core/frondendviews.py` deleted (typo filename, was not imported anywhere)
- `core/views.py` (28-line flat stub) deleted (superseded by core/views/ directory)
- `core/pipeline/sql_compiler.py` updated to clearly document the split
- Deprecation READMEs added to: `encryption/`, `fetch_sqlserver/`, `hana_connection/`, `utils/`

### Not renamed (intentional)
- `datamigrationapi/` — Django project package name. Renaming requires coordinated
  deployment changes (WSGI server config, Docker, CI). Left as-is.
