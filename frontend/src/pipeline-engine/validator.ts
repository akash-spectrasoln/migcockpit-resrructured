/**
 * Pipeline Engine — Unified Validation Engine
 * Validates each node's config against its available input schema.
 * Produces structured SchemaError[] per node.
 * No React / Zustand dependencies.
 */
import type {
  ColumnSchema,
  SchemaError,
  RawNode,
  AggregateColumnConfig,
  ConditionConfig,
} from './types'

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

/** Build a fast-lookup set of available column names */
function availableSet(schema: ColumnSchema[]): Set<string> {
  const out = new Set<string>()
  schema.forEach((c) => {
    if (c?.name) out.add(c.name)
    // Allow configs to reference stable technical identifiers too
    if ((c as any)?.technical_name) out.add((c as any).technical_name)
    if ((c as any)?.column) out.add((c as any).column)
  })
  return out
}

/** Does the identifier `colName` appear as a whole word in `code`? */
function colUsedInCode(code: string, colName: string): boolean {
  const escaped = colName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const pattern = new RegExp(
    `(?<![a-zA-Z0-9_])${escaped}(?![a-zA-Z0-9_])`
  )
  return pattern.test(code)
}

// ─────────────────────────────────────────────────────────────
// Per-type validators
// ─────────────────────────────────────────────────────────────

function validateAggregate(
  node: RawNode,
  available: Set<string>
): SchemaError[] {
  const errors: SchemaError[] = []
  const aggregateColumns: AggregateColumnConfig[] =
    node.data.config?.aggregateColumns ?? []

  for (const agg of aggregateColumns) {
    // Measure column (not needed for COUNT(*))
    if (agg.column && agg.column !== '' && agg.function !== 'COUNT') {
      if (!available.has(agg.column)) {
        errors.push({
          source: 'schema_drift',
          type: 'missing_column',
          location: 'aggregation',
          aggId: agg.id,
          column: agg.column,
          alias: agg.alias,
          message: `Column '${agg.column}' not found in ${agg.function}(${agg.column}) — removed upstream`,
        })
      }
    }

    // Group-by columns within the aggregate
    for (const gbCol of agg.groupBy ?? []) {
      if (!available.has(gbCol)) {
        errors.push({
          source: 'schema_drift',
          type: 'missing_column',
          location: 'group_by',
          aggId: agg.id,
          column: gbCol,
          message: `Column '${gbCol}' not found in Group By — removed upstream`,
        })
      }
    }
  }
  return errors
}

function validateComputeAgainstMissing(
  node: RawNode,
  missingNames: string[]
): SchemaError[] {
  const code: string = node.data.config?.code ?? ''
  if (!code.trim() || missingNames.length === 0) return []

  return missingNames
    .filter((col) => colUsedInCode(code, col))
    .map((col) => ({
      source: 'schema_drift' as const,
      type: 'missing_column' as const,
      location: 'compute_expression' as const,
      column: col,
      message: `Column '${col}' not found in compute expression — removed upstream`,
    }))
}

function validateFilter(
  node: RawNode,
  available: Set<string>
): SchemaError[] {
  const conditions: ConditionConfig[] = node.data.config?.conditions ?? []
  const errors: SchemaError[] = []
  conditions.forEach((cond, idx) => {
    if (cond.column && !available.has(cond.column)) {
      errors.push({
        source: 'schema_drift',
        type: 'missing_column',
        location: 'filter_condition',
        conditionIndex: idx,
        column: cond.column,
        message: `Filter condition ${idx + 1}: column '${cond.column}' not found — removed upstream`,
      })
    }
  })
  return errors
}

function validateJoinConditions(
  node: RawNode,
  leftAvailable: Set<string>,
  rightAvailable: Set<string>
): SchemaError[] {
  const conditions: ConditionConfig[] = node.data.config?.conditions ?? []
  const errors: SchemaError[] = []
  conditions.forEach((cond, idx) => {
    const problems: string[] = []
    if (cond.leftColumn && !leftAvailable.has(cond.leftColumn)) {
      problems.push(`left column '${cond.leftColumn}'`)
    }
    if (cond.rightColumn && !rightAvailable.has(cond.rightColumn)) {
      problems.push(`right column '${cond.rightColumn}'`)
    }
    if (problems.length > 0) {
      errors.push({
        source: 'schema_drift',
        type: 'missing_column',
        location: 'join_condition',
        conditionIndex: idx,
        column: cond.leftColumn || cond.rightColumn,
        message: `Join condition ${idx + 1}: ${problems.join(' and ')} not found — removed upstream`,
      })
    }
  })
  return errors
}

// ─────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────

export interface ValidationResult {
  config_errors: SchemaError[]
  errors: string[]
}

/**
 * Validate a single node against its available input schema.
 *
 * @param node          The raw node from Zustand/React Flow
 * @param inputSchema   Columns available as input to this node
 * @param joinSides     For join nodes: schemas from left and right input nodes separately
 */
export function validateNode(
  node: RawNode,
  inputSchema: ColumnSchema[],
  joinSides?: { left: ColumnSchema[]; right: ColumnSchema[] }
): ValidationResult {
  const kind = (node.data.type || node.type || '').toLowerCase()
  const available = availableSet(inputSchema)
  let config_errors: SchemaError[] = []

  switch (kind) {
    case 'aggregate':
      config_errors = validateAggregate(node, available)
      break

    case 'compute': {
      // Find which columns from the PREVIOUS input_metadata are now missing
      const prevCols: ColumnSchema[] = node.data.input_metadata?.columns ?? []
      const prevNames = prevCols.map((c) => c.name)
      const missingNames = prevNames.filter((n) => !available.has(n))
      config_errors = validateComputeAgainstMissing(node, missingNames)
      break
    }

    case 'filter':
      config_errors = validateFilter(node, available)
      break

    case 'join': {
      const leftAvail = availableSet(joinSides?.left ?? inputSchema)
      const rightAvail = availableSet(joinSides?.right ?? [])
      config_errors = validateJoinConditions(node, leftAvail, rightAvail)
      break
    }

    default:
      // source, projection, destination, calculated, compute — no extra validation here
      break
  }

  // Build flat error summary for backwards compat (canvas chip tooltip)
  const errors: string[] = config_errors.map((e) => e.message)

  return { config_errors, errors }
}

/**
 * Heal validation errors for columns that have been re-added.
 * Returns a new array without errors referencing any of `readdedNames`.
 */
export function healErrors(
  existingErrors: SchemaError[],
  readdedNames: Set<string>
): SchemaError[] {
  return existingErrors.filter(
    (e) => !e.column || !readdedNames.has(e.column)
  )
}

/**
 * Merge new errors into an existing set, deduplicating by (location + column + aggId).
 */
export function mergeErrors(
  existing: SchemaError[],
  incoming: SchemaError[]
): SchemaError[] {
  const key = (e: SchemaError) =>
    `${e.source}|${e.location}|${e.column ?? ''}|${e.aggId ?? ''}|${e.conditionIndex ?? ''}`
  const existingKeys = new Set(existing.map(key))
  const toAdd = incoming.filter((e) => !existingKeys.has(key(e)))
  return [...existing, ...toAdd]
}
