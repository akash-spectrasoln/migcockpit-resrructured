/**
 * Pipeline Engine — Propagation
 * Column include/exclude propagation through the DAG.
 * Uses the graph adjacency structures from graph.ts.
 * No React / Zustand dependencies.
 */
import type { ColumnSchema, RawNode, SchemaError } from './types'
import type { PipelineGraph } from './graph'
import { getColKey, computeNodeOutputSchema, computeJoinOutput } from './schema'
import { validateNode, healErrors } from './validator'

// ─────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────

function stripColumns(
  meta: { columns?: ColumnSchema[] } | null | undefined,
  removeSet: Set<string>
): { columns?: ColumnSchema[] } | null | undefined {
  if (!meta?.columns) return meta
  const next = meta.columns.filter((c) => !removeSet.has(c.name))
  return next.length === meta.columns.length ? meta : { ...meta, columns: next }
}

function cleanStringArray(arr: any[] | undefined, removeSet: Set<string>): any[] | undefined {
  if (!Array.isArray(arr)) return arr
  return arr.filter((v) => !removeSet.has(typeof v === 'string' ? v : getColKey(v)))
}

/**
 * Derive an effective input_metadata snapshot for a node.
 * Prefer the node's own input_metadata; if it's missing, fall back to the
 * first upstream node's output_metadata columns. This prevents over-reporting
 * errors when input_metadata was never initialised.
 */
function getEffectiveInputMeta(
  nodeId: string,
  nodeById: Map<string, RawNode>,
  graph: PipelineGraph
): { columns?: ColumnSchema[] } | null {
  const node = nodeById.get(nodeId)
  if (!node) return null
  const direct = node.data.input_metadata
  if (direct?.columns && direct.columns.length > 0) {
    return direct as { columns?: ColumnSchema[] }
  }

  const inEdges = graph.inEdges[nodeId] ?? []
  const firstSource = inEdges[0]?.source
  if (!firstSource) return null
  const upstream = nodeById.get(firstSource)
  const upstreamCols =
    (upstream?.data.output_metadata?.columns ??
      upstream?.data.input_metadata?.columns ??
      []) as ColumnSchema[]
  if (!upstreamCols.length) return null
  return { columns: upstreamCols }
}

/** Resolve the left and right input nodes for a join using edge targetHandle */
function resolveJoinSides(
  joinNodeId: string,
  nodeById: Map<string, RawNode>,
  graph: PipelineGraph
): { left: ColumnSchema[]; right: ColumnSchema[] } {
  const inEdges = graph.inEdges[joinNodeId] ?? []
  const leftEdge = inEdges.find((e) => e.targetHandle === 'left') ?? inEdges[0]
  const rightEdge = inEdges.find((e) => e.targetHandle === 'right') ?? inEdges[1]

  const getOutputCols = (nodeId: string | undefined): ColumnSchema[] => {
    if (!nodeId) return []
    const n = nodeById.get(nodeId)
    return (n?.data.output_metadata?.columns ?? []) as ColumnSchema[]
  }

  return {
    left: getOutputCols(leftEdge?.source),
    right: getOutputCols(rightEdge?.source),
  }
}

// ─────────────────────────────────────────────────────────────
// Public: propagate REMOVED columns
// ─────────────────────────────────────────────────────────────

/**
 * BFS downstream from `sourceNodeId`.
 * For every downstream node:
 *  1. Strip removed columns from input_metadata and output_metadata
 *  2. For JOIN nodes: fully rebuild output_metadata from both input nodes
 *  3. Clean config selection lists (includedColumns, outputColumns, etc.)
 *  4. Validate logical references (agg expressions, compute code, join/filter conditions)
 *  5. Set data.config_errors[] and data.errors[]
 *
 * Returns a new array — original nodes are NOT mutated.
 */
export function propagateRemovedColumns(
  nodes: RawNode[],
  graph: PipelineGraph,
  sourceNodeId: string,
  removedNames: string[]
): RawNode[] {
  if (!removedNames.length) return nodes

  const removeSet = new Set(removedNames)
  const nodeById = new Map(nodes.map((n) => [n.id, n]))

  // BFS downstream from sourceNodeId
  const queue: string[] = [...(graph.adjacencyList[sourceNodeId] ?? [])]
  const visited = new Set<string>([sourceNodeId])
  const modifiedIds = new Set<string>()

  // Process in BFS order so parent output is updated before child input is read
  while (queue.length > 0) {
    const nodeId = queue.shift()!
    if (visited.has(nodeId)) continue
    visited.add(nodeId)

    const node = nodeById.get(nodeId)
    if (!node) continue

    const data = node.data
    const cfg = data.config ?? {}
    const kind = (data.type || '').toLowerCase()

    // ── 1. Strip input_metadata (hard propagation) ───────────
    const baseInputMeta =
      kind === 'join'
        ? (data.input_metadata as { columns?: ColumnSchema[] } | null | undefined)
        : getEffectiveInputMeta(nodeId, nodeById, graph)
    const newInput = stripColumns(baseInputMeta, removeSet)

    // ── 2. Compute new output_metadata via schema engine ─────
    let newOutput: { columns?: ColumnSchema[] } | null | undefined = data.output_metadata
    const inputCols: ColumnSchema[] = (newInput?.columns ?? []) as ColumnSchema[]

    if (kind === 'join') {
      // Always rebuild join output from live node snapshots
      const sides = resolveJoinSides(nodeId, nodeById, graph)
      // After stripping, the left/right upstream nodes might have already been updated
      // in this same BFS pass; re-read from nodeById (which reflects updates)
      const leftNode = nodeById.get(
        graph.inEdges[nodeId]?.find((e) => e.targetHandle === 'left')?.source ?? ''
      )
      const rightNode = nodeById.get(
        graph.inEdges[nodeId]?.find((e) => e.targetHandle === 'right')?.source ??
          graph.inEdges[nodeId]?.[1]?.source ??
          ''
      )

      const leftCols: ColumnSchema[] = (leftNode?.data.output_metadata?.columns ??
        sides.left) as ColumnSchema[]
      const rightCols: ColumnSchema[] = (rightNode?.data.output_metadata?.columns ??
        sides.right) as ColumnSchema[]

      const rebuilt = computeJoinOutput(node, leftCols, rightCols)
      // Further filter rebuilt list by removeSet (in case join condition cols were removed)
      const filtered = rebuilt.filter((c) => !removeSet.has(c.column || c.name))
      newOutput = { ...(data.output_metadata || {}), columns: filtered }
    } else {
      // For all other node types, delegate to computeNodeOutputSchema using new input
      const recomputed = computeNodeOutputSchema(
        node,
        inputCols,
        undefined
      ) as ColumnSchema[]
      newOutput = { ...(data.output_metadata || {}), columns: recomputed }
      // Finally, strip any columns that should be removed but are still present
      newOutput = stripColumns(newOutput, removeSet)
    }

    // ── 3. Clean config selection lists ──────────────────────
    const nextConfig: any = { ...cfg }
    nextConfig.includedColumns = cleanStringArray(cfg.includedColumns, removeSet)
    nextConfig.output_columns = cleanStringArray(cfg.output_columns, removeSet)
    nextConfig.selectedColumns = cleanStringArray(cfg.selectedColumns, removeSet)
    nextConfig.columns = cleanStringArray(cfg.columns, removeSet)
    nextConfig.groupBy = cleanStringArray(cfg.groupBy, removeSet)
    nextConfig.group_by = cleanStringArray(cfg.group_by, removeSet)

    if (kind === 'join') {
      nextConfig.selectedLeftColumns = cleanStringArray(cfg.selectedLeftColumns, removeSet)
      nextConfig.selectedRightColumns = cleanStringArray(cfg.selectedRightColumns, removeSet)
      if (Array.isArray(cfg.outputColumns)) {
        nextConfig.outputColumns = cfg.outputColumns.filter((col: any) => {
          const name = col?.column || col?.name || getColKey(col)
          return !removeSet.has(name)
        })
      }
    }

    if (Array.isArray(cfg.columnOrder)) {
      nextConfig.columnOrder = cfg.columnOrder.filter(
        (col: any) => !removeSet.has(col?.name || getColKey(col))
      )
    }

    if (Array.isArray(cfg.aggregateColumns)) {
      nextConfig.aggregateColumns = cfg.aggregateColumns.map((agg: any) => ({
        ...agg,
        groupBy: Array.isArray(agg.groupBy)
          ? agg.groupBy.filter((col: string) => !removeSet.has(col))
          : agg.groupBy,
      }))
    }

    // ── 4. Validate logical references ───────────────────────
    const joinSidesForValidation =
      kind === 'join'
        ? resolveJoinSides(nodeId, nodeById, graph)
        : undefined

    const updatedNode: RawNode = {
      ...node,
      data: { ...data, config: nextConfig },
    }
    const { config_errors, errors } = validateNode(
      updatedNode,
      inputCols,
      joinSidesForValidation
    )

    // Merge with existing non-drift errors
    const prevConfigErrors: SchemaError[] = Array.isArray(data.config_errors)
      ? data.config_errors.filter((e: SchemaError) => e.source !== 'schema_drift')
      : []
    const mergedConfigErrors = [...prevConfigErrors, ...config_errors]

    const prevFlatErrors: string[] = Array.isArray(data.errors)
      ? data.errors.filter((e: string) => !e.includes('not found'))
      : []
    const mergedErrors =
      errors.length > 0 ? [...prevFlatErrors, errors[0]] : prevFlatErrors

    // ── 5. Write back ─────────────────────────────────────────
    const updated: RawNode = {
      ...node,
      data: {
        ...data,
        input_metadata: newInput as any,
        output_metadata: newOutput,
        config: nextConfig,
        errors: mergedErrors.length > 0 ? mergedErrors : undefined,
        config_errors: mergedConfigErrors.length > 0 ? mergedConfigErrors : undefined,
      },
    }
    nodeById.set(nodeId, updated)
    modifiedIds.add(nodeId)

    // Continue BFS
    for (const neighbor of graph.adjacencyList[nodeId] ?? []) {
      if (!visited.has(neighbor)) queue.push(neighbor)
    }
  }

  // Return new nodes array (only rebuild nodes that were modified)
  if (modifiedIds.size === 0) return nodes
  return nodes.map((n) => nodeById.get(n.id) ?? n)
}

// ─────────────────────────────────────────────────────────────
// Public: propagate ADDED columns (re-include)
// ─────────────────────────────────────────────────────────────

/**
 * Add `addedColumns` to the immediate downstream nodes' input_metadata.
 * Heals any errors that reference the re-added columns.
 * For JOIN nodes: rebuilds output_metadata.
 *
 * Only processes IMMEDIATE downstream (one hop).
 * Schema recompilation of further downstream nodes is handled by compilePipeline().
 */
export function propagateAddedColumns(
  nodes: RawNode[],
  graph: PipelineGraph,
  sourceNodeId: string,
  addedColumns: ColumnSchema[]
): RawNode[] {
  if (!addedColumns.length) return nodes

  const readdedNames = new Set(addedColumns.map((c) => c.name).filter(Boolean))
  const immediateTargets = new Set(graph.adjacencyList[sourceNodeId] ?? [])
  if (!immediateTargets.size) return nodes

  const nodeById = new Map(nodes.map((n) => [n.id, n]))

  return nodes.map((n) => {
    if (!immediateTargets.has(n.id)) return n

    const data = n.data
    const kind = (data.type || '').toLowerCase()

    // Merge added columns into input_metadata (soft propagation)
    const baseInputMeta =
      kind === 'join'
        ? (data.input_metadata as { columns?: ColumnSchema[] } | null | undefined)
        : getEffectiveInputMeta(n.id, nodeById, graph)
    const existingCols: ColumnSchema[] = (baseInputMeta?.columns ?? []) as ColumnSchema[]
    const existingNames = new Set(existingCols.map((c) => c.name))
    const toAdd = addedColumns.filter((c) => !existingNames.has(c.name))
    const newCols = toAdd.length > 0 ? [...existingCols, ...toAdd] : existingCols

    // Heal errors referencing re-added columns
    const healedConfigErrors = healErrors(
      (data.config_errors ?? []) as SchemaError[],
      readdedNames
    )
    const healedFlatErrors = (data.errors ?? []).filter(
      (e: string) => {
        const matched = (e.match(/Column '([^']+)'/) || [])[1]
        return !matched || !readdedNames.has(matched)
      }
    )

    // Recompute output_metadata for all node types using the schema engine
    let newOutput = data.output_metadata
    const inputSchemaForNode = newCols
    if (kind === 'join') {
      const inEdges = graph.inEdges[n.id] ?? []
      const leftEdge =
        inEdges.find((e) => e.targetHandle === 'left') ?? inEdges[0]
      const rightEdge =
        inEdges.find((e) => e.targetHandle === 'right') ?? inEdges[1]

      const leftNode = nodeById.get(leftEdge?.source ?? '')
      const rightNode = nodeById.get(rightEdge?.source ?? '')

      const leftCols: ColumnSchema[] = (leftNode?.data.output_metadata?.columns ??
        []) as ColumnSchema[]
      const rightCols: ColumnSchema[] = (rightNode?.data.output_metadata?.columns ??
        []) as ColumnSchema[]

      // For whichever side is the source, use newCols
      const actualLeft = leftEdge?.source === sourceNodeId ? newCols : leftCols
      const actualRight =
        rightEdge?.source === sourceNodeId ? newCols : rightCols

      const rebuilt = computeJoinOutput(n, actualLeft, actualRight)
      newOutput = { ...(data.output_metadata || {}), columns: rebuilt }
    } else {
      const recomputed = computeNodeOutputSchema(
        n,
        inputSchemaForNode,
        undefined
      ) as ColumnSchema[]
      newOutput = { ...(data.output_metadata || {}), columns: recomputed }
    }

    const changed =
      toAdd.length > 0 ||
      healedConfigErrors.length !== (data.config_errors?.length ?? 0) ||
      healedFlatErrors.length !== (data.errors?.length ?? 0)

    if (!changed) return n

    return {
      ...n,
      data: {
        ...data,
        input_metadata: { ...(data.input_metadata || {}), columns: newCols },
        output_metadata: newOutput,
        errors: healedFlatErrors.length > 0 ? healedFlatErrors : undefined,
        config_errors: healedConfigErrors.length > 0 ? healedConfigErrors : undefined,
      },
    }
  })
}
