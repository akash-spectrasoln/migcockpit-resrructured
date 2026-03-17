# Pipeline Rules: Linear vs Branch, Nested vs Flattened, Source → Staging → Destination

This doc describes the rules used in the migration service for **linear vs branch** nodes, how **nested or flattened** SQL is built, and the full flow from **source → staging → destination** with code references.

**All approaches are data-driven and reused** — anchor types (`MERGE_ANCHOR_TYPES`, `ANCHOR_TYPES_NEED_PARENT_STAGING`) are defined once in `materialization.py`; the same recursive formula (find staging before, flatten linear chain) applies everywhere.

---

## 1. Rules for linear nodes

**Definition:** A *linear segment* is a chain of nodes with a single parent and single child (e.g. source → projection → filter → …), with no JOINs and no branching.

**Rules (enforced in `planner/materialization.py`):**

- **No staging inside linear chains.** Staging tables are **only** created at *physical boundaries* (see below).
- **Linear segments are compiled as nested SQL only** — one long `SELECT … FROM ( SELECT … FROM ( … ) )` with no intermediate `CREATE TABLE`.
- **Forbidden:** Creating staging for individual projection/filter/compute nodes, or multiple staging tables in the same linear chain.

**Code:** `planner/materialization.py` (docstring and `detect_materialization_points`). Linear segments never appear in `materialization_points`; only boundary nodes do.

---

## 2. Rules for branches (materialization boundaries)

Staging is allowed **only** at these boundaries (see `MaterializationReason` in `planner/materialization.py`):

| Boundary | When | Meaning |
|----------|------|--------|
| **A. Branch end before JOIN** | A branch feeds into a JOIN | The *terminal* node of each branch (the node immediately before the JOIN) is materialized. One staging table per branch. |
| **B. JOIN result** | After a JOIN node | The JOIN output is always materialized (one staging table per JOIN). |
| **B3b. Shared multi-branch** | Node feeds multiple downstream branches | Any node with 2+ children is materialized so all branches read from the same staging. |
| **B4. Pre-compute staging** | Before a compute node | The parent of each compute node is materialized so compute reads from staging. |
| **C. Pre-destination staging** | Before a destination node | The single parent of the destination is materialized; final INSERT is staging → destination. |

**Post-join (same recursive formula):** After the join, find staging before (join), flatten linear chain to pre-compute node. Then staging before compute (e.g. 25-col projection). Compute reads from that staging. Pre-destination staging for final INSERT.

**Shared source (optional):** If **one source feeds multiple branches** (e.g. two projections from the same table going to two branches of a JOIN), we can materialize that source **once** and let both branches read from it — **unless** `linear_branches=True`. When `linear_branches=True`, we do **not** materialize the shared source; each branch is compiled as “fetch directly from source with only required columns” (one query per branch from source → … → staging).

**Code:** `planner/materialization.py` — `detect_materialization_points()`, `_find_branch_terminal()`, `_find_source_for_branch()`, and the three loops for JOIN parents (branch end), JOIN (result), and destination parent (pre-destination).

---

## 3. How nested vs flattened query is created

### 3.1 Nested SQL (linear chain, no CREATE TABLE)

- **Purpose:** For any node that is either a *materialization point* (then we emit `CREATE TABLE … AS <nested>`) or part of a linear chain, we first build a **single nested SELECT** that walks upstream until we hit a source or a previously materialized node.
- **Stops when:**
  - We reach a **materialized node** → use `SELECT * FROM "staging_schema"."staging_table"` (read from staging).
  - We reach a **source node** → use `SELECT … FROM "schema"."table"` (from config; filters can be pushed here).
- **No CREATE TABLE, no CTEs** — only SELECT. One recursive “layer” per node (projection, filter, compute, etc.).

**Code:** `planner/sql_compiler.py` — `compile_nested_sql()`, `traverse_upstream()`, stop at `materialization_points` or `source`; transformations applied in `_apply_transformation()`.

### 3.2 Flattened SQL (optional)

- **Purpose:** When we have nested SQL like `SELECT a FROM ( SELECT b FROM ( SELECT c FROM base ) inner ) outer`, we can sometimes **flatten** it to a single `SELECT … FROM base [ WHERE … ]` so the database runs one query instead of nested subqueries.
- **When:** Used inside `compile_staging_table_sql()` when building `CREATE TABLE staging AS <select>`. We try `_flatten_nested_select(nested_sql, col_list)`; if it returns a single-level SELECT, we use it; otherwise we keep the nested form.
- **Rule:** Flattening only happens when the inner part is a single `SELECT … FROM table [ WHERE … ]` (no nested `FROM ( … )`). The outer select list (including calculated columns) is preserved when recursing.

**Code:** `planner/sql_compiler.py` — `_flatten_nested_select()`. Used in `compile_staging_table_sql()` when building the final CREATE TABLE AS select.

### 3.3 CREATE TABLE … AS SELECT (staging)

- **Branch end / pre-destination:** For a materialized node we call `compile_staging_table_sql()`, which:
  - Builds **nested** SQL with `compile_nested_sql()` (traversing up to source or previous staging).
  - Wraps it as `CREATE TABLE "staging_schema"."staging_table" AS <nested_or_flattened_select>`.
- **Shared source (one source, many branches):** We use `compile_source_staging_sql()` — one `CREATE TABLE … AS SELECT … FROM "schema"."table"` with the union of columns and calculated expressions needed by all branches.
- **JOIN:** We use `compile_join_sql()` — `CREATE TABLE … AS SELECT … FROM left_staging l JOIN right_staging r ON …` (no nesting; reads from two staging tables).

**Code:** `planner/sql_compiler.py` — `compile_staging_table_sql()`, `compile_source_staging_sql()`, `compile_join_sql()`. Plan assembly in `planner/execution_plan.py` — `build_execution_plan()` chooses which compiler to call per node/level.

---

## 4. Execution levels (order of execution)

Levels are a **topological order** of nodes (sources first, then nodes whose dependencies are done).

- **Level 0:** Typically source nodes (and shared-source staging if used).
- **Next levels:** Branch terminals (staging from source or from shared source), then JOINs, then any linear chain after JOIN, then pre-destination staging.
- **Per level:** For each node we emit one or more **queries** (CompiledSQL). Each query is either:
  - `CREATE TABLE … AS SELECT … FROM source_table` (source or shared source), or
  - `CREATE TABLE … AS SELECT … FROM ( nested )` or flattened single SELECT, or
  - `CREATE TABLE … AS SELECT … FROM left_staging JOIN right_staging`.

**Code:** `planner/execution_plan.py` — `_build_execution_levels()` (topological sort), then loop over levels/nodes and call `compile_source_staging_sql`, `compile_join_sql`, or `compile_staging_table_sql`.

---

## 5. Full flow: source → staging → destination

### 5.1 Build phase (planner)

1. **Materialization:** `detect_materialization_points(nodes, edges, job_id, linear_branches)` → which nodes get staging (branch ends, JOINs, pre-destination; optional shared source).
2. **Levels:** `_build_execution_levels(nodes, edges)` → topological levels.
3. **Per-level SQL:** For each node in each level we produce `CompiledSQL`:
   - Source (shared) → `compile_source_staging_sql` → `CREATE TABLE staging AS SELECT … FROM "schema"."table"`.
   - Branch terminal / pre-destination → `compile_staging_table_sql` → `CREATE TABLE staging AS <nested or flattened SELECT>` (nested may read from source or from staging).
   - JOIN → `compile_join_sql` → `CREATE TABLE staging AS SELECT … FROM left_staging JOIN right_staging`.
4. **Destination:** `_generate_destination_create()` (CREATE TABLE destination if needed), `_generate_final_insert()` (INSERT INTO destination FROM last staging table), and cleanup (DROP staging tables).

**Code:** `planner/execution_plan.py` — `build_execution_plan()`.

### 5.2 Run phase (orchestrator)

1. **Create staging schema** on the **execution (customer/destination) DB**: `_create_staging_schema(execution_conn, staging_schema)` in `orchestrator/execute_pipeline_pushdown.py`.
2. **For each level query:**
   - If the query is **CREATE TABLE … AS SELECT … FROM "schema"."table"** (i.e. from a **source** table, not staging):
     - **Source → staging** is done by **`_execute_source_staging_query()`**:
       - Resolve `(schema, table)` to `source_node_id` via `_build_source_table_to_node_id_map()`.
       - Open **source DB** connection with `_get_source_connection(config, source_node_id)`.
       - On **source:** run the SELECT (with optional LIMIT 0 to get types); then stream rows in batches.
       - On **execution DB:** CREATE TABLE (staging schema.table), then INSERT batches from source. So **data is read from source DB and written into staging on the execution (customer) DB.**
     - If there is no source connection for that table, we fall back to running the whole SQL on `execution_conn` (which will fail if the table exists only on source).
   - Otherwise (query reads from staging or is a JOIN of two staging tables):
     - Run the SQL on the **execution connection** only: `_execute_sql(execution_conn, compiled_sql.sql)`. All staging tables and the destination live on the execution DB.
3. **Destination:** Run `destination_create_sql` (if any) then `final_insert_sql` (INSERT into destination from last staging) on `execution_conn`.
4. **Cleanup:** Run `cleanup_sql` (DROP staging tables) on `execution_conn`.

So in one sentence: **Source tables are read from the source DB and written into staging on the execution DB; all other staging and the destination live on the execution DB.**

**Code:** `orchestrator/execute_pipeline_pushdown.py`:
- `_build_source_table_to_node_id_map()` (used to detect “FROM source_table”).
- `_execute_source_staging_query()` — source DB read + execution DB CREATE/INSERT.
- `_execute_sql()` — run a single statement on a given connection.
- Level loop: try `_execute_source_staging_query()` first; if it returns `None`, call `_execute_sql(execution_conn, sql)`.

---

## 6. Filter rules (pushdown, shared source, deduplication)

### 6.1 Filter pushdown

- **Post-join filters on source columns** (e.g. `_L_cmp_id` from `tool_connection`): Should be pushed down to the source query when the column lineage is known.
- **Implementation:** `planner/filter_optimizer.py` — lineage lookup with fallbacks for `_L_`/`_R_` prefixes and source-prefixed column names (e.g. `39ef59b7_cmp_id` → `cmp_id`).

### 6.2 Shared source: OR at source, AND within branch

- When a **source feeds multiple branches** with different filters:
  - **At source:** Apply `(branch1_filter) OR (branch2_filter) OR ...` to reduce data loaded. Only when **all** branches have filters; if any branch has no filters, do not restrict at source.
  - **Within same branch:** Conditions are ANDed.
  - Each branch still applies its **own** filter when reading from shared staging.
- **Implementation:** `planner/sql_compiler.py` — `_get_branch_filter_where_parts()` collects per-branch filters; `compile_source_staging_sql` combines with OR when all branches have filters. When reading from staging (`from_table_override`), skip filter pushdown so each branch applies only its own filters.

### 6.3 Duplicate predicate deduplication

- Avoid duplicate or redundant filter conditions in generated SQL.
- **Exact duplicates:** Same predicate string removed.
- **Semantic duplicates:** Predicates like `"employee_range" = '50-100'` and `"4fa62c23_employee_range" = '50-100'` are treated as equivalent (same base column + operator + value).
- **Implementation:** `planner/sql_compiler.py` — `_dedupe_where_parts()` with `_predicate_signature()` for normalization. Applied in `compile_source_staging_sql`, `flatten_segment_from_source`, and `flatten_segment`.

### 6.4 Multiple destinations

- Each destination node gets its own `CREATE TABLE` and `INSERT` statement.
- **Implementation:** `planner/execution_plan.py` — `destination_creates`, `final_inserts` (lists); `_generate_all_destination_creates()`, `_generate_all_final_inserts()`. Orchestrator runs each CREATE and INSERT in sequence.

---

## 7. Quick reference: where each thing is done

| What | Where (file) | Function / logic |
|------|--------------|-------------------|
| Linear = no staging in middle | `planner/materialization.py` | Docstring + only boundaries get materialization |
| Branch end, JOIN, pre-destination | `planner/materialization.py` | `detect_materialization_points()` |
| Shared source vs linear_branches | `planner/materialization.py` | `source_to_terminals`, `linear_branches` flag |
| Nested SELECT (traverse upstream) | `planner/sql_compiler.py` | `compile_nested_sql()`, `traverse_upstream()` |
| Flatten nested to one SELECT | `planner/sql_compiler.py` | `_flatten_nested_select()` |
| CREATE staging from nested | `planner/sql_compiler.py` | `compile_staging_table_sql()` |
| CREATE staging from source table | `planner/sql_compiler.py` | `compile_source_staging_sql()`, `_compile_source_node()` |
| JOIN staging | `planner/sql_compiler.py` | `compile_join_sql()` |
| Levels and plan | `planner/execution_plan.py` | `_build_execution_levels()`, `build_execution_plan()` |
| Run source → staging (source DB read) | `orchestrator/execute_pipeline_pushdown.py` | `_execute_source_staging_query()`, `_get_source_connection()` |
| Run staging / JOIN / cleanup on execution DB | `orchestrator/execute_pipeline_pushdown.py` | `_execute_sql(execution_conn, sql)` |
| Staging schema + final INSERT | `orchestrator/execute_pipeline_pushdown.py` | `_create_staging_schema()`, then level loop, then `destination_create_sql` / `final_insert_sql` / `cleanup_sql` |
| Filter pushdown (post-join → source) | `planner/filter_optimizer.py` | Lineage lookup, `_L_`/`_R_` fallbacks |
| Shared source: skip filters at source | `planner/sql_compiler.py` | `compile_source_staging_sql`, `flatten_segment_from_source` |
| Duplicate predicate deduplication | `planner/sql_compiler.py` | `_dedupe_where_parts()`, `_predicate_signature()` |
| Multiple destinations | `planner/execution_plan.py` | `destination_creates`, `final_inserts`, `_generate_all_*` |

This is how linear vs branch rules, nested vs flattened SQL, filter rules, and the full source → staging → destination flow are implemented in the codebase.
