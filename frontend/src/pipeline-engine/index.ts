/**
 * Pipeline Engine — Public Index
 * Re-exports everything needed by React components and the Zustand store.
 */

// Types
export type {
  ColumnSchema,
  NodeKind,
  SchemaError,
  ErrorLocation,
  CompiledNode,
  CompiledPipeline,
  RawNode,
  RawEdge,
} from './types'

// Graph
export { buildGraph, bfsDownstream } from './graph'
export type { PipelineGraph } from './graph'

// Schema computation
export {
  getColKey,
  computeNodeOutputSchema,
  computeSourceOutput,
  computeProjectionOutput,
  computeFilterOutput,
  computeJoinOutput,
  computeAggregateOutput,
  computeComputeOutput,
  computeCalculatedOutput,
} from './schema'

// Validation
export { validateNode, healErrors, mergeErrors } from './validator'
export type { ValidationResult } from './validator'

// Propagation
export { propagateRemovedColumns, propagateAddedColumns } from './propagate'

// Compiler
export {
  compilePipeline,
  getInputSchema,
  getOutputSchema,
  getNodeErrors,
  diffColumnSets,
} from './compiler'
