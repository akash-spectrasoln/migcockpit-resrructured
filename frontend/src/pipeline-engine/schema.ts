/**
 * Pipeline Engine — Schema Computation
 * Pure functions that derive a node's output schema from its config + input schema.
 * No React / Zustand dependencies.
 */
import type {
  ColumnSchema,
  RawNode,
  OutputColumnConfig,
  AggregateColumnConfig,
  CalculatedColumnConfig,
} from './types'

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

export function getColKey(col: any): string {
  if (!col) return ''
  if (typeof col === 'string') return col
  return col.technical_name || col.name || col.column_name || col.db_name || String(col)
}

function toColumnSchema(raw: any, fallbackDatatype = 'TEXT'): ColumnSchema {
  const name = getColKey(raw)
  const column = raw.column || raw.db_name || name
  return {
    name,
    datatype: raw.datatype || raw.data_type || raw.type || fallbackDatatype,
    nullable: raw.nullable !== undefined ? raw.nullable : true,
    source: raw.source,
    outputName: raw.outputName,
    column,
    // Preserve any existing lineage information if present
    base: raw.base,
    technical_name: raw.technical_name,
  } as ColumnSchema
}

// ─────────────────────────────────────────────────────────────
// Source node — schema comes from live metadata snapshot
// ─────────────────────────────────────────────────────────────

export function computeSourceOutput(node: RawNode): ColumnSchema[] {
  const meta = node.data.output_metadata
  if (meta?.columns?.length) return meta.columns.map(toColumnSchema)
  const cfgCols = node.data.config?.columns
  if (Array.isArray(cfgCols)) return cfgCols.map(toColumnSchema)
  return []
}

// ─────────────────────────────────────────────────────────────
// Projection node
// ─────────────────────────────────────────────────────────────

export function computeProjectionOutput(
  node: RawNode,
  inputSchema: ColumnSchema[]
): ColumnSchema[] {
  const cfg = node.data.config || {}
  const nodeId = node.id

  // Determine the included set (preference order)
  const included: Set<string> | null = (() => {
    for (const key of [
      'includedColumns',
      'output_columns',
      'selectedColumns',
      'columns',
    ] as const) {
      const arr = cfg[key]
      if (Array.isArray(arr) && arr.length > 0) {
        return new Set<string>(arr.map(getColKey).filter(Boolean))
      }
    }
    return null
  })()

  // Base columns (filtered or all)
  let base: ColumnSchema[] = included
    ? inputSchema.filter((c) => included.has(c.name))
    : inputSchema

  // Apply column order if defined
  const order: string[] | undefined = Array.isArray(cfg.columnOrder)
    ? cfg.columnOrder.map((c: any) => getColKey(c))
    : undefined
  if (order?.length) {
    const orderMap = new Map(order.entries().map(([i, name]) => [name, i] as [string, number]))
    base = [...base].sort(
      (a, b) => (orderMap.get(a.name) ?? 9999) - (orderMap.get(b.name) ?? 9999)
    )
  }

  // Append calculated columns
  const calcCols: CalculatedColumnConfig[] = cfg.calculatedColumns ?? []
  const extras: ColumnSchema[] = calcCols
    .filter((c) => c.alias || c.name)
    .map((c) => ({
      name: c.alias || c.name || '',
      datatype: 'TEXT', // derived; actual type is runtime-determined
      nullable: true,
      source: 'derived' as const,
      base: nodeId,
      technical_name: `${nodeId}__${(c.alias || c.name || '').trim()}`,
    }))

  return [...base, ...extras]
}

// ─────────────────────────────────────────────────────────────
// Filter node — pass-through (schema unchanged)
// ─────────────────────────────────────────────────────────────

export function computeFilterOutput(inputSchema: ColumnSchema[]): ColumnSchema[] {
  return inputSchema
}

// ─────────────────────────────────────────────────────────────
// Join node — always rebuilt from both inputs
// ─────────────────────────────────────────────────────────────

export function computeJoinOutput(
  node: RawNode,
  leftSchema: ColumnSchema[],
  rightSchema: ColumnSchema[]
): ColumnSchema[] {
  const cfg = node.data.config || {}
  const LEFT_ALIAS = cfg.leftTable || 'L'
  const RIGHT_ALIAS = cfg.rightTable || 'R'

  // If outputColumns config exists and has entries, use it
  const outColsCfg: OutputColumnConfig[] | undefined = cfg.outputColumns
  if (outColsCfg && outColsCfg.length > 0) {
    return outColsCfg
      .filter((c) => c.included !== false)
      .map((col) => ({
        name: col.outputName || col.column || col.name || '',
        column: col.column || col.name || '',
        datatype: col.datatype || col.data_type || col.type || 'TEXT',
        source: col.source,
        nullable: col.nullable !== undefined ? col.nullable : true,
        outputName: col.outputName,
      }))
  }

  // Derive from raw input schemas
  const leftOut: ColumnSchema[] = leftSchema.map((c) => ({
    ...c,
    name: c.outputName || `${LEFT_ALIAS}_${c.name}`,
    outputName: c.outputName || `${LEFT_ALIAS}_${c.name}`,
    source: 'left' as const,
    column: c.name,
  }))
  const rightOut: ColumnSchema[] = rightSchema.map((c) => ({
    ...c,
    name: c.outputName || `${RIGHT_ALIAS}_${c.name}`,
    outputName: c.outputName || `${RIGHT_ALIAS}_${c.name}`,
    source: 'right' as const,
    column: c.name,
  }))

  return [...leftOut, ...rightOut]
}

// ─────────────────────────────────────────────────────────────
// Aggregate node
// ─────────────────────────────────────────────────────────────

export function computeAggregateOutput(node: RawNode): ColumnSchema[] {
  const aggregateColumns: AggregateColumnConfig[] =
    node.data.config?.aggregateColumns ?? []

  if (aggregateColumns.length === 0) {
    // Fall back to existing output_metadata if no config yet
    const meta = node.data.output_metadata
    return meta?.columns?.map(toColumnSchema) ?? []
  }

  const output: ColumnSchema[] = []

  // Collect unique groupBy columns across all aggregations
  const groupByCols = new Set<string>()
  for (const agg of aggregateColumns) {
    for (const gbCol of agg.groupBy ?? []) {
      groupByCols.add(gbCol)
    }
  }

  // Group-by keys appear first
  for (const colName of groupByCols) {
    output.push({ name: colName, datatype: 'TEXT', nullable: true, source: 'derived' })
  }

  // Aggregate result columns
  for (const agg of aggregateColumns) {
    const alias = agg.alias || `${agg.function.toLowerCase()}_${agg.column || 'col'}`
    output.push({
      name: alias,
      datatype: ['COUNT', 'SUM'].includes(agg.function) ? 'NUMERIC' : 'TEXT',
      nullable: true,
      source: 'derived' as const,
    })
  }

  return output
}

// ─────────────────────────────────────────────────────────────
// Compute node — output schema is unknown until executed
// ─────────────────────────────────────────────────────────────

export function computeComputeOutput(node: RawNode): ColumnSchema[] {
  // Use last-known output_metadata if available (set after execution)
  const meta = node.data.output_metadata
  return meta?.columns?.map(toColumnSchema) ?? []
}

// ─────────────────────────────────────────────────────────────
// Calculated column node — input + new column appended
// ─────────────────────────────────────────────────────────────

export function computeCalculatedOutput(
  node: RawNode,
  inputSchema: ColumnSchema[]
): ColumnSchema[] {
  const cfg = node.data.config || {}
  const newCol: CalculatedColumnConfig | undefined =
    cfg.newColumn || cfg.calculatedColumn
  if (!newCol) return inputSchema
  const colName = newCol.alias || newCol.name || 'new_column'
  return [
    ...inputSchema,
    {
      name: colName,
      datatype: 'TEXT',
      nullable: true,
      source: 'derived' as const,
      base: node.id,
      technical_name: `${node.id}__${colName}`,
    } as ColumnSchema,
  ]
}

// ─────────────────────────────────────────────────────────────
// Dispatcher — pick the right schema function for any node type
// ─────────────────────────────────────────────────────────────

export function computeNodeOutputSchema(
  node: RawNode,
  inputSchema: ColumnSchema[],
  /** For join nodes: the schemas from left and right input nodes */
  joinSides?: { left: ColumnSchema[]; right: ColumnSchema[] }
): ColumnSchema[] {
  const kind = (node.data.type || node.type || '').toLowerCase()

  switch (kind) {
    case 'source':
      return computeSourceOutput(node)

    case 'projection':
      return computeProjectionOutput(node, inputSchema)

    case 'filter':
      return computeFilterOutput(inputSchema)

    case 'join':
      return computeJoinOutput(
        node,
        joinSides?.left ?? inputSchema,
        joinSides?.right ?? []
      )

    case 'aggregate':
      return computeAggregateOutput(node)

    case 'compute':
      return computeComputeOutput(node)

    case 'calculated':
      return computeCalculatedOutput(node, inputSchema)

    case 'destination':
      return [] // sinks have no output

    default:
      return inputSchema // Unknown node: pass-through
  }
}
