/**
 * Pipeline Engine — Compiler
 * Compiles raw React Flow nodes + edges into a CompiledPipeline.
 *
 * The compiler:
 *  1. Builds the graph (adjacency, topo order)
 *  2. Walks nodes in topological order
 *  3. For each node: computes inputSchema from upstream outputs, runs schema
 *     computation, runs validation
 *  4. Returns a CompiledPipeline record keyed by node id
 *
 * This is O(N + E) and runs in <1ms for typical pipelines (≤100 nodes).
 * No React / Zustand dependencies — pure TypeScript.
 */
import type { RawNode, RawEdge, ColumnSchema, CompiledNode, CompiledPipeline } from './types'
import { buildGraph } from './graph'
import { computeNodeOutputSchema } from './schema'
import { validateNode } from './validator'

// ─────────────────────────────────────────────────────────────
// Compiler
// ─────────────────────────────────────────────────────────────

/**
 * Compile the pipeline from a snapshot of nodes and edges.
 *
 * @param rawNodes   React Flow Node[] (from Zustand)
 * @param rawEdges   React Flow Edge[] (from Zustand)
 */
export function compilePipeline(
  rawNodes: RawNode[],
  rawEdges: RawEdge[]
): CompiledPipeline {
  const graph = buildGraph(rawNodes, rawEdges)
  const nodeById = new Map(rawNodes.map((n) => [n.id, n]))

  // Map from nodeId → computed output schema (built incrementally during topo walk)
  const outputSchemas = new Map<string, ColumnSchema[]>()
  const compiledNodes: Record<string, CompiledNode> = {}

  for (const nodeId of graph.topoOrder) {
    const node = nodeById.get(nodeId)
    if (!node) continue

    const kind = (node.data.type || node.type || 'unknown').toLowerCase()
    const inEdges = graph.inEdges[nodeId] ?? []

    // ── Resolve input schema ────────────────────────────────
    // For most nodes: union of all upstream output schemas (typically just 1 parent)
    // For join nodes: we also need to separate left / right
    const leftEdge = inEdges.find((e) => e.targetHandle === 'left') ?? inEdges[0]
    const rightEdge = inEdges.find((e) => e.targetHandle === 'right') ?? inEdges[1]

    const getUpstreamSchema = (edge: RawEdge | undefined): ColumnSchema[] => {
      if (!edge) return []
      return outputSchemas.get(edge.source) ?? []
    }

    // inputSchema = union across all parents (de-duplicated by name)
    const allParentSchemas = inEdges.map((e) => getUpstreamSchema(e))
    const inputSchema = dedupeColumns(allParentSchemas.flat())

    // For join: separate left and right
    const joinSides =
      kind === 'join'
        ? {
            left: getUpstreamSchema(leftEdge),
            right: getUpstreamSchema(rightEdge),
          }
        : undefined

    // ── Compute output schema ──────────────────────────────
    const outputSchema = computeNodeOutputSchema(node, inputSchema, joinSides)
    outputSchemas.set(nodeId, outputSchema)

    // ── Validate node ──────────────────────────────────────
    const { config_errors, errors } = validateNode(node, inputSchema, joinSides)

    compiledNodes[nodeId] = {
      nodeId,
      kind,
      inputSchema,
      outputSchema,
      config_errors,
      errors,
    }
  }

  return {
    nodes: compiledNodes,
    adjacencyList: graph.adjacencyList,
    reverseAdjacency: graph.reverseAdjacency,
    topoOrder: graph.topoOrder,
    compiledAt: Date.now(),
  }
}

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

/** De-duplicate columns by name, keeping first occurrence. */
function dedupeColumns(cols: ColumnSchema[]): ColumnSchema[] {
  const seen = new Set<string>()
  return cols.filter((c) => {
    const key = c.name
    if (!key || seen.has(key)) return false
    seen.add(key)
    return true
  })
}

// ─────────────────────────────────────────────────────────────
// Utility selectors (used by React components)
// ─────────────────────────────────────────────────────────────

/**
 * Get the input schema for a node from a compiled pipeline.
 * Returns empty array if not compiled yet.
 */
export function getInputSchema(
  compiled: CompiledPipeline | null,
  nodeId: string
): ColumnSchema[] {
  return compiled?.nodes[nodeId]?.inputSchema ?? []
}

/**
 * Get the output schema for a node from a compiled pipeline.
 */
export function getOutputSchema(
  compiled: CompiledPipeline | null,
  nodeId: string
): ColumnSchema[] {
  return compiled?.nodes[nodeId]?.outputSchema ?? []
}

/**
 * Get all schema errors for a node.
 */
export function getNodeErrors(
  compiled: CompiledPipeline | null,
  nodeId: string
): { config_errors: CompiledNode['config_errors']; errors: string[] } {
  const cn = compiled?.nodes[nodeId]
  return {
    config_errors: cn?.config_errors ?? [],
    errors: cn?.errors ?? [],
  }
}

/**
 * Diff two column sets by name.
 * Returns { removed, added } arrays of column names.
 */
export function diffColumnSets(
  prevCols: ColumnSchema[],
  nextCols: ColumnSchema[]
): { removed: string[]; added: string[] } {
  const prevNames = new Set(prevCols.map((c) => c.name).filter(Boolean))
  const nextNames = new Set(nextCols.map((c) => c.name).filter(Boolean))
  const removed: string[] = []
  const added: string[] = []
  prevNames.forEach((n) => { if (!nextNames.has(n)) removed.push(n) })
  nextNames.forEach((n) => { if (!prevNames.has(n)) added.push(n) })
  return { removed, added }
}
