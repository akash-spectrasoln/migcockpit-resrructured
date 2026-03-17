/**
 * Pipeline Engine — Shared Types
 * All types used across the pipeline-engine module.
 * Zero React / ReactFlow dependencies — pure TypeScript.
 */

// ─────────────────────────────────────────────────────────────
// Column descriptor
// ─────────────────────────────────────────────────────────────
export interface ColumnSchema {
  /** Canonical identifier used as key throughout the engine (technical_name → name → column_name) */
  name: string
  datatype?: string
  nullable?: boolean
  source?: 'left' | 'right' | 'derived' | 'source'
  /** For join nodes: the prefixed output name, e.g. "L_amount" */
  outputName?: string
  /** Raw column reference before aliasing */
  column?: string
}

// ─────────────────────────────────────────────────────────────
// Node types recognised by the engine
// ─────────────────────────────────────────────────────────────
export type NodeKind =
  | 'source'
  | 'projection'
  | 'filter'
  | 'join'
  | 'aggregate'
  | 'compute'
  | 'calculated'
  | 'destination'
  | string // fallback for unknown types

// ─────────────────────────────────────────────────────────────
// Config sub-types
// ─────────────────────────────────────────────────────────────
export interface AggregateColumnConfig {
  id: string
  function: 'SUM' | 'AVG' | 'MIN' | 'MAX' | 'COUNT' | string
  column: string
  alias: string
  groupBy?: string[]
  expression?: string
}

export interface ConditionConfig {
  column?: string
  leftColumn?: string
  rightColumn?: string
  operator?: string
  value?: any
}

export interface OutputColumnConfig {
  column?: string
  name?: string
  outputName?: string
  source?: 'left' | 'right'
  included?: boolean
  datatype?: string
  data_type?: string
  type?: string
  nullable?: boolean
}

export interface CalculatedColumnConfig {
  id?: string
  name?: string
  alias?: string
  expression?: string
}

// ─────────────────────────────────────────────────────────────
// Raw node data as stored in Zustand / React Flow
// ─────────────────────────────────────────────────────────────
export interface RawNodeData {
  type?: NodeKind
  config?: {
    // projection
    includedColumns?: string[]
    output_columns?: string[]
    selectedColumns?: string[]
    columns?: string[]
    columnOrder?: any[]
    calculatedColumns?: CalculatedColumnConfig[]
    // join
    leftTable?: string
    rightTable?: string
    outputColumns?: OutputColumnConfig[]
    selectedLeftColumns?: string[]
    selectedRightColumns?: string[]
    conditions?: ConditionConfig[]
    // aggregate
    aggregateColumns?: AggregateColumnConfig[]
    groupBy?: string[]
    group_by?: string[]
    // compute
    code?: string
    language?: string
    requirements?: string
    // source
    sourceId?: number
    tableName?: string
    schema?: string
    isFiltered?: boolean
    columnCount?: number
    [key: string]: any
  }
  input_metadata?: { columns?: ColumnSchema[] }
  output_metadata?: { columns?: ColumnSchema[] } | null
  errors?: string[]
  config_errors?: SchemaError[]
  business_name?: string
  label?: string
  node_name?: string
  technical_name?: string
  [key: string]: any
}

export interface RawNode {
  id: string
  type?: string
  data: RawNodeData
  position: { x: number; y: number }
}

export interface RawEdge {
  id: string
  source: string
  target: string
  sourceHandle?: string | null
  targetHandle?: string | null
}

// ─────────────────────────────────────────────────────────────
// Structured schema error (replaces plain string errors)
// ─────────────────────────────────────────────────────────────
export type ErrorLocation =
  | 'aggregation'
  | 'group_by'
  | 'compute_expression'
  | 'join_condition'
  | 'filter_condition'
  | 'projection'
  | 'general'

export interface SchemaError {
  source: 'schema_drift' | 'validation' | string
  type: 'missing_column' | 'type_mismatch' | string
  location: ErrorLocation
  column?: string
  /** For aggregate: the id of the AggregateColumnConfig */
  aggId?: string
  alias?: string
  /** For join/filter conditions: 0-based index */
  conditionIndex?: number
  message: string
}

// ─────────────────────────────────────────────────────────────
// Compiled node — what the engine produces per node
// ─────────────────────────────────────────────────────────────
export interface CompiledNode {
  nodeId: string
  kind: NodeKind
  /** Columns available as input to this node (union of upstream outputs) */
  inputSchema: ColumnSchema[]
  /** Columns this node emits to downstream nodes */
  outputSchema: ColumnSchema[]
  /** Validation errors found during compilation */
  errors: string[]
  config_errors: SchemaError[]
}

// ─────────────────────────────────────────────────────────────
// Full compiled pipeline graph
// ─────────────────────────────────────────────────────────────
export interface CompiledPipeline {
  /** Keyed by node id */
  nodes: Record<string, CompiledNode>
  /** Adjacency list: nodeId → [downstreamNodeId, ...] */
  adjacencyList: Record<string, string[]>
  /** Reverse adjacency: nodeId → [upstreamNodeId, ...] */
  reverseAdjacency: Record<string, string[]>
  /** Topological order (source → sinks) */
  topoOrder: string[]
  /** Timestamp of last compilation */
  compiledAt: number
}
