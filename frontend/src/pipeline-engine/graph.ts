/**
 * Pipeline Engine — Graph Builder
 * Constructs adjacency lists and topological order from raw React Flow nodes/edges.
 * O(N + E) — no React dependencies.
 */
import type { RawNode, RawEdge } from './types'

export interface PipelineGraph {
  /** All node ids present in the graph */
  nodeIds: string[]
  /** Ordered by topology (sources first, sinks last).
   *  Nodes in a cycle or unreachable subgraph appear at the end. */
  topoOrder: string[]
  /** nodeId → list of downstream node ids */
  adjacencyList: Record<string, string[]>
  /** nodeId → list of upstream node ids */
  reverseAdjacency: Record<string, string[]>
  /** nodeId → list of incoming edges (for join: to identify left/right side) */
  inEdges: Record<string, RawEdge[]>
  /** nodeId → list of outgoing edges */
  outEdges: Record<string, RawEdge[]>
}

/**
 * Build a PipelineGraph from raw React Flow nodes and edges.
 */
export function buildGraph(nodes: RawNode[], edges: RawEdge[]): PipelineGraph {
  const nodeIds = nodes.map((n) => n.id)
  const nodeSet = new Set(nodeIds)

  // Build adjacency structures
  const adjacencyList: Record<string, string[]> = {}
  const reverseAdjacency: Record<string, string[]> = {}
  const inEdges: Record<string, RawEdge[]> = {}
  const outEdges: Record<string, RawEdge[]> = {}

  for (const id of nodeIds) {
    adjacencyList[id] = []
    reverseAdjacency[id] = []
    inEdges[id] = []
    outEdges[id] = []
  }

  for (const edge of edges) {
    if (!nodeSet.has(edge.source) || !nodeSet.has(edge.target)) continue
    adjacencyList[edge.source].push(edge.target)
    reverseAdjacency[edge.target].push(edge.source)
    inEdges[edge.target].push(edge)
    outEdges[edge.source].push(edge)
  }

  const topoOrder = topoSort(nodeIds, adjacencyList)

  return { nodeIds, topoOrder, adjacencyList, reverseAdjacency, inEdges, outEdges }
}

/**
 * Kahn's BFS topological sort. Nodes in cycles appear after a best-effort ordering.
 * Returns all nodeIds — nodes with no position in the topo order (cycles/orphans) go last.
 */
function topoSort(
  nodeIds: string[],
  adjacencyList: Record<string, string[]>
): string[] {
  const inDegree: Record<string, number> = {}
  for (const id of nodeIds) inDegree[id] = 0

  for (const id of nodeIds) {
    for (const neighbor of adjacencyList[id] ?? []) {
      inDegree[neighbor] = (inDegree[neighbor] ?? 0) + 1
    }
  }

  const queue: string[] = nodeIds.filter((id) => inDegree[id] === 0)
  const result: string[] = []

  while (queue.length > 0) {
    const current = queue.shift()!
    result.push(current)
    for (const neighbor of adjacencyList[current] ?? []) {
      inDegree[neighbor]--
      if (inDegree[neighbor] === 0) queue.push(neighbor)
    }
  }

  // Append any remaining nodes (cycles / disconnected)
  const resultSet = new Set(result)
  for (const id of nodeIds) {
    if (!resultSet.has(id)) result.push(id)
  }

  return result
}

/**
 * BFS from a start node, yielding downstream node ids in breadth-first order.
 * The start node itself is NOT included.
 */
export function bfsDownstream(
  startId: string,
  adjacencyList: Record<string, string[]>
): string[] {
  const visited = new Set<string>([startId])
  const queue = [...(adjacencyList[startId] ?? [])]
  const result: string[] = []

  while (queue.length > 0) {
    const current = queue.shift()!
    if (visited.has(current)) continue
    visited.add(current)
    result.push(current)
    for (const neighbor of adjacencyList[current] ?? []) {
      if (!visited.has(neighbor)) queue.push(neighbor)
    }
  }

  return result
}
