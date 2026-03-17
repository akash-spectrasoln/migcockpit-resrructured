/**
 * Graph utilities for pipeline flow detection.
 * Used to find independent flows (connected components) for separate job execution.
 */

export interface GraphNode {
  id: string
  type?: string
  data?: { type?: string; config?: { table_name?: string }; business_name?: string; label?: string }
}

export interface GraphEdge {
  source: string
  target: string
}

/**
 * Find connected components (independent flows) in the pipeline graph.
 * Uses undirected connectivity: two nodes are in the same component if there is a path between them.
 */
export function findConnectedComponents(
  nodes: GraphNode[],
  edges: GraphEdge[]
): Set<string>[] {
  const nodeIds = new Set(nodes.map((n) => n.id).filter(Boolean))
  if (nodeIds.size === 0) return []

  const adjacency = new Map<string, Set<string>>()
  nodeIds.forEach((id) => adjacency.set(id, new Set()))
  for (const edge of edges) {
    const src = edge.source
    const tgt = edge.target
    if (nodeIds.has(src) && nodeIds.has(tgt)) {
      adjacency.get(src)!.add(tgt)
      adjacency.get(tgt)!.add(src)
    }
  }

  const visited = new Set<string>()
  const components: Set<string>[] = []

  for (const startId of nodeIds) {
    if (visited.has(startId)) continue
    const component = new Set<string>()
    const queue = [startId]
    visited.add(startId)
    component.add(startId)
    while (queue.length > 0) {
      const curr = queue.shift()!
      for (const neighbor of adjacency.get(curr) ?? []) {
        if (!visited.has(neighbor)) {
          visited.add(neighbor)
          component.add(neighbor)
          queue.push(neighbor)
        }
      }
    }
    components.push(component)
  }

  return components
}

/**
 * Generate human-readable labels for each flow (connected component).
 */
export function getFlowLabels(components: Set<string>[], nodes: GraphNode[]): string[] {
  const nodeMap = new Map(nodes.map((n) => [n.id, n]))

  return components.map((component) => {
    const sourceNames: string[] = []
    for (const nid of component) {
      const node = nodeMap.get(nid)
      if (!node) continue
      const nodeType = node.data?.type ?? node.type
      if (nodeType === 'source') {
        const label =
          node.data?.config?.table_name ??
          node.data?.business_name ??
          node.data?.label ??
          nid.slice(0, 8)
        sourceNames.push(String(label))
      }
    }
    if (sourceNames.length === 0) {
      sourceNames.push(...[...component].slice(0, 2).map((id) => id.slice(0, 8)))
    }
    return sourceNames.slice(0, 3).join(', ') + (sourceNames.length > 3 ? '...' : '')
  })
}
