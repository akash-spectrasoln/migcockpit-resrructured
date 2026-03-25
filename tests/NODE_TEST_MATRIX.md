# Node Regression Matrix

This matrix defines expected regression coverage for each pipeline node type.
Use it as the source of truth for automation goals.

## Legend

- `Y`: Covered by automated tests now
- `P`: Partially covered (some scenarios)
- `N`: Not covered yet

## Coverage Matrix

| Node Type | Config Validation | Schema/Metadata | Preview (Saved) | Preview (Unsaved) | Execution SQL/Plan | Existing Tests |
|---|---|---|---|---|---|---|
| Source | P | P | P | N | P | `tests/test_sql_pushdown.py`, `tests/test_e2e_pushdown.py`, `tests/test_node_cache_repository_metadata.py` |
| Filter (builder) | Y | P | Y | P | Y | `tests/test_filter_execution_regressions.py`, `tests/unit/domain/test_filter_conditions.py`, `tests/unit/pipeline/test_preview_compiler.py` |
| Filter (expression) | Y | P | Y | P | Y | `tests/test_expression_sql_validation.py`, `tests/test_filter_execution_regressions.py`, `tests/unit/pipeline/test_preview_compiler.py` |
| Projection | P | P | P | N | P | `tests/test_projection_calc_dependency_order.py`, `tests/unit/pipeline/test_preview_compiler.py` |
| Calculated (projection calc columns) | P | P | P | N | P | `tests/test_projection_calc_dependency_order.py`, `tests/test_expression_sql_validation.py` |
| Join | P | P | P | N | P | `tests/test_sql_rewrite_and_dedupe.py`, `tests/unit/pipeline/test_preview_compiler.py` |
| Aggregate | P | P | P | N | P | `tests/test_sql_compiler.py`, `tests/test_sql_compiler_integration.py` |
| Compute | P | N | P | N | P | `tests/test_e2e_pushdown.py`, `tests/test_sql_pushdown.py` |
| Destination | P | Y | N/A | N/A | Y | `tests/test_destination_business_name_normalization.py`, `tests/test_sql_rewrite_and_dedupe.py` |

## Scenario Requirements By Node

### Source

- Source metadata load from remote/repository
- Source to staging with technical names
- Source pushdown with filter rewrites

### Filter

- Builder mode: all operators
  - `=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `ILIKE`, `IN`, `NOT IN`, `BETWEEN`, `IS NULL`, `IS NOT NULL`
- Expression mode:
  - date/time keywords (`CURRENT_DATE`, `CURRENT_TIMESTAMP`)
  - boolean-predicate validation
  - SQL parser validation
- Pushdown-safe and pushdown-unsafe cases
- Preview parity: saved vs unsaved config

### Projection / Calculated

- Include mode and exclude mode
- Column order stability
- Calculated dependency order (topological behavior)
- Expression translation edge cases (`CAST`, `COALESCE`, null-safety)
- Business name vs technical name correctness

### Join

- Inner/left/right/full outputs
- Ambiguous name conflict handling (`_L_`/`_R_`)
- Technical-name and prefixed-name resolution
- Rewriter fallback path when staging columns differ
- Duplicate output alias handling in CTAS

### Aggregate

- Group-by with aggregate combinations
- Missing group-by columns and safe fallback
- Result schema data-type expectations

### Compute

- Editor update + persistence behavior
- Compile/run API flow contract
- Output metadata propagation downstream
- Unsaved code preview/run behavior

### Destination

- Destination create SQL uses business/display names
- Final insert dedupe/rewrite behavior
- Target schema creation and schema fallback

## Full Pipeline Scenarios (Cross-Node)

- Source -> Projection -> Filter -> Join -> Projection -> Destination
- Mixed builder/expression filters in same pipeline
- Unsaved intermediate edits and preview behavior
- Validate -> Execute path should reuse saved plan where hash matches
- Regression safety: old flows still pass after compiler/executor changes

## Next Automation Targets

1. Expand backend parametrized tests for full filter operator matrix.
2. Add projection include/exclude + expression edge-case tests.
3. Add join rewrite + duplicate-output CTAS fallback tests.
4. Add frontend integration tests for panel unsaved state behavior.
5. Add browser E2E for critical flow (design, validate, preview, execute).
