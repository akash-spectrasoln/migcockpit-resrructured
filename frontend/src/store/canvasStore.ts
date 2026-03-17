/**
 * Global Canvas State Store
 * Manages canvas graph, selected nodes, job state, and configuration.
 *
 * Performance design:
 *  - nodesById: O(1) node lookup by ID
 *  - selectedNodeId: only this changes on click (no full re-renders)
 *  - compilePipeline runs async (deferred) so node click is never blocked
 */
import { create } from 'zustand'
import { Node, Edge } from 'reactflow'
import { compilePipeline } from '../pipeline-engine'
import type { CompiledPipeline, RawNode, RawEdge } from '../pipeline-engine'

// ─── Helpers ─────────────────────────────────────────────────────────────────

/** Build a Record<id, Node> from an array. O(n) one-time cost. */
function buildNodesById(nodes: Node[]): Record<string, Node> {
  const map: Record<string, Node> = {}
  for (const n of nodes) map[n.id] = n
  return map
}

/** Shallow-compare two node arrays by id + position only (fast drag check). */
function samePositions(a: Node[], b: Node[]): boolean {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (
      a[i].id !== b[i].id ||
      a[i].position?.x !== b[i].position?.x ||
      a[i].position?.y !== b[i].position?.y
    ) return false
  }
  return true
}

/** Defer pipeline compilation to the next task so selection is instant. */
let _compileTimer: ReturnType<typeof setTimeout> | null = null
function scheduleCompile(
  nodes: Node[],
  edges: Edge[],
  apply: (compiled: CompiledPipeline) => void
) {
  if (_compileTimer) clearTimeout(_compileTimer)
  _compileTimer = setTimeout(() => {
    _compileTimer = null
    const compiled = compilePipeline(nodes as RawNode[], edges as RawEdge[])
    apply(compiled)
  }, 0)
}

// ─── State Interface ──────────────────────────────────────────────────────────

export interface CanvasState {
  // Canvas graph
  nodes: Node[]
  edges: Edge[]
  /** O(1) lookup map, always kept in sync with `nodes` */
  nodesById: Record<string, Node>
  canvasId: number | null
  canvasName: string

  // Compiled Pipeline (updated asynchronously after every setNodes / setEdges)
  compiledGraph: CompiledPipeline | null
  /** Manually trigger recompilation (e.g. after drift detection patches nodes). */
  recompile: () => void

  // Selection & UI state
  /** The single source of truth for which node is selected. */
  selectedNodeId: string | null
  /**
   * Convenience: returns the full Node object for the selected node.
   * Reads from nodesById — O(1).
   * @deprecated Prefer `nodesById[selectedNodeId]` with a narrow selector.
   */
  selectedNode: Node | null
  selectedNodeIds: string[]
  selectedEdge: Edge | null
  viewMode: 'design' | 'validate' | 'run' | 'monitor'

  // Job state
  activeJobId: string | null
  jobProgress: Record<string, number>
  nodeStatuses: Record<string, 'idle' | 'running' | 'success' | 'error'>

  // Lineage / propagation highlights
  lineageHighlight: { nodeIds: string[]; edgeIds: string[] } | null
  setLineageHighlight: (path: { nodeIds: string[]; edgeIds: string[] } | null) => void
  propagationHighlight: { nodeIds: string[]; edgeIds: string[] } | null
  setPropagationHighlight: (path: { nodeIds: string[]; edgeIds: string[] } | null) => void

  // Actions
  setNodes: (nodes: Node[]) => void
  updateNodePositions: (nodes: Node[]) => void
  setEdges: (edges: Edge[]) => void
  addNode: (node: Node) => void
  updateNode: (nodeId: string, updates: Partial<Node>) => void
  deleteNode: (nodeId: string) => void
  /** Delete node and reconnect parents to children (frontend-only; persist via Save Pipeline). */
  deleteNodeWithBridging: (nodeId: string) => void
  addEdge: (edge: Edge) => void
  deleteEdge: (edgeId: string) => void

  // Selection actions
  /** Primary selection setter — only updates selectedNodeId (O(1), no recompute). */
  setSelectedNodeId: (id: string | null) => void
  /** Backward-compat: accepts a full Node object. Internally only stores the ID. */
  setSelectedNode: (node: Node | null) => void
  setSelectedNodeIds: (ids: string[]) => void
  setSelectedEdge: (edge: Edge | null) => void

  setViewMode: (mode: 'design' | 'validate' | 'run' | 'monitor') => void
  setCanvas: (id: number | null, name: string) => void
  setActiveJob: (jobId: string | null) => void
  updateJobProgress: (nodeId: string, progress: number) => void
  updateNodeStatus: (nodeId: string, status: 'idle' | 'running' | 'success' | 'error' | 'completed' | 'failed') => void
  updateNodeStatusBatch: (updates: Record<string, 'idle' | 'running' | 'success' | 'error' | 'completed' | 'failed'>) => void
  setAllNodesStatus: (status: 'success' | 'error') => void
  clearNodeStatuses: () => void

  // Dirty state
  isDirty: boolean
  setIsDirty: (isDirty: boolean) => void

  // Preview state
  previewNodeId: string | null
  previewVisible: boolean
  previewData: { nodeId?: string; sourceId?: number; tableName?: string; schema?: string; directFilterConditions?: any[] } | null
  setPreview: (nodeId: string | null, visible: boolean, data?: CanvasState['previewData']) => void

  // Undo/Redo
  past: { nodes: Node[]; edges: Edge[] }[]
  future: { nodes: Node[]; edges: Edge[] }[]
  undo: () => void
  redo: () => void

  reset: () => void
}

// ─── Initial State ────────────────────────────────────────────────────────────

const initialState = {
  nodes: [] as Node[],
  edges: [] as Edge[],
  nodesById: {} as Record<string, Node>,
  canvasId: null as number | null,
  canvasName: '',
  compiledGraph: null as CompiledPipeline | null,
  selectedNodeId: null as string | null,
  selectedNode: null as Node | null,
  selectedNodeIds: [] as string[],
  selectedEdge: null as Edge | null,
  viewMode: 'design' as const,
  activeJobId: null as string | null,
  jobProgress: {} as Record<string, number>,
  nodeStatuses: {} as Record<string, 'idle' | 'running' | 'success' | 'error'>,
  lineageHighlight: null,
  propagationHighlight: null,
  isDirty: false,
  previewNodeId: null as string | null,
  previewVisible: false,
  previewData: null,
  past: [] as { nodes: Node[]; edges: Edge[] }[],
  future: [] as { nodes: Node[]; edges: Edge[] }[],
}

// ─── Store ────────────────────────────────────────────────────────────────────

export const useCanvasStore = create<CanvasState>((set, get) => ({
  ...initialState,

  // ── Graph mutations ────────────────────────────────────────────────────────

  setNodes: (nodes) => set((state) => {
    // Fast structural check: avoid full JSON.stringify
    if (state.nodes === nodes) return {}

    const nodesById = buildNodesById(nodes)

    // Keep selectedNode reference up-to-date (O(1))
    const selId = state.selectedNodeId
    const selectedNode = selId ? (nodesById[selId] ?? null) : null

    const next = {
      nodes,
      nodesById,
      selectedNode,
      past: [...state.past, { nodes: state.nodes, edges: state.edges }].slice(-50),
      future: [] as { nodes: Node[]; edges: Edge[] }[],
      isDirty: true,
    }

    // Schedule async compile — does NOT block the selection update
    scheduleCompile(nodes, state.edges, (compiled) => {
      set({ compiledGraph: compiled })
    })

    return next
  }),

  updateNodePositions: (nodes) => set((state) => {
    if (samePositions(state.nodes, nodes)) return {}
    const nodesById = buildNodesById(nodes)
    return { nodes, nodesById }
  }),

  setEdges: (edges) => set((state) => {
    if (state.edges === edges) return {}

    const next = {
      edges,
      past: [...state.past, { nodes: state.nodes, edges: state.edges }].slice(-50),
      future: [] as { nodes: Node[]; edges: Edge[] }[],
      isDirty: true,
    }

    scheduleCompile(state.nodes, edges, (compiled) => {
      set({ compiledGraph: compiled })
    })

    return next
  }),

  addNode: (node) => set((state) => {
    const nodes = [...state.nodes, node]
    return {
      nodes,
      nodesById: { ...state.nodesById, [node.id]: node },
      past: [...state.past, { nodes: state.nodes, edges: state.edges }].slice(-50),
      future: [],
      isDirty: true,
    }
  }),

  updateNode: (nodeId, updates) => set((state) => {
    const existing = state.nodesById[nodeId]
    if (!existing) return {}
    const updated = { ...existing, ...updates }
    const nodesById = { ...state.nodesById, [nodeId]: updated }
    const nodes = state.nodes.map((n) => n.id === nodeId ? updated : n)
    const selectedNode = state.selectedNodeId === nodeId ? updated : state.selectedNode
    return {
      nodes,
      nodesById,
      selectedNode,
      past: [...state.past, { nodes: state.nodes, edges: state.edges }].slice(-50),
      future: [],
      isDirty: true,
    }
  }),

  deleteNode: (nodeId) => set((state) => {
    const nodes = state.nodes.filter((n) => n.id !== nodeId)
    const edges = state.edges.filter((e) => e.source !== nodeId && e.target !== nodeId)
    const nodesById = { ...state.nodesById }
    delete nodesById[nodeId]
    return {
      nodes,
      edges,
      nodesById,
      selectedNodeId: state.selectedNodeId === nodeId ? null : state.selectedNodeId,
      selectedNode: state.selectedNode?.id === nodeId ? null : state.selectedNode,
      past: [...state.past, { nodes: state.nodes, edges: state.edges }].slice(-50),
      future: [],
      isDirty: true,
    }
  }),

  /**
   * Delete a node and reconnect parents to children (auto-bridge).
   * Frontend-only: no backend call. Persist by clicking "Save Pipeline".
   */
  deleteNodeWithBridging: (nodeId) => set((state) => {
    const nodeIds = new Set(state.nodes.map((n) => n.id))
    if (!nodeIds.has(nodeId)) return {}

    const parentIds: string[] = []
    const childIds: string[] = []
    const deletedToChildHandles: Record<string, { targetHandle?: string; sourceHandle?: string }> = {}

    for (const e of state.edges) {
      if (e.target === nodeId) {
        if (e.source && !parentIds.includes(e.source)) parentIds.push(e.source)
      } else if (e.source === nodeId) {
        if (e.target) {
          childIds.push(e.target)
          if (!deletedToChildHandles[e.target]) {
            deletedToChildHandles[e.target] = {
              targetHandle: e.targetHandle as string | undefined,
              sourceHandle: e.sourceHandle as string | undefined,
            }
          }
        }
      }
    }

    const existingKeys = new Set(
      state.edges
        .filter((e) => e.source !== nodeId && e.target !== nodeId)
        .map((e) => `${e.source}-${e.target}`)
    )

    const newEdges: Edge[] = state.edges.filter(
      (e) => e.source !== nodeId && e.target !== nodeId
    )

    for (const parentId of parentIds) {
      for (const childId of childIds) {
        const key = `${parentId}-${childId}`
        if (existingKeys.has(key)) continue
        existingKeys.add(key)
        const handle = deletedToChildHandles[childId]
        newEdges.push({
          id: key,
          source: parentId,
          target: childId,
          sourceHandle: 'output',
          targetHandle: (handle?.targetHandle as string) || 'input',
          type: 'smoothstep',
        } as Edge)
      }
    }

    const newNodes = state.nodes
      .filter((n) => n.id !== nodeId)
      .map((n) => {
        if (!childIds.includes(n.id)) return n
        const currentInputs = (n.data?.input_nodes as string[] | undefined) || []
        const updated = currentInputs.filter((id) => id !== nodeId)
        for (const pid of parentIds) {
          if (!updated.includes(pid)) updated.push(pid)
        }
        return {
          ...n,
          data: { ...n.data, input_nodes: updated },
        }
      })

    const nodesById = buildNodesById(newNodes)

    return {
      nodes: newNodes,
      edges: newEdges,
      nodesById,
      selectedNodeId: state.selectedNodeId === nodeId ? null : state.selectedNodeId,
      selectedNode: state.selectedNode?.id === nodeId ? null : (state.selectedNodeId && nodesById[state.selectedNodeId]) || null,
      past: [...state.past, { nodes: state.nodes, edges: state.edges }].slice(-50),
      future: [],
      isDirty: true,
    }
  }),

  addEdge: (edge) => set((state) => ({
    edges: [...state.edges, edge],
    past: [...state.past, { nodes: state.nodes, edges: state.edges }].slice(-50),
    future: [],
    isDirty: true,
  })),

  deleteEdge: (edgeId) => set((state) => ({
    edges: state.edges.filter((e) => e.id !== edgeId),
    selectedEdge: state.selectedEdge?.id === edgeId ? null : state.selectedEdge,
    past: [...state.past, { nodes: state.nodes, edges: state.edges }].slice(-50),
    future: [],
    isDirty: true,
  })),

  // ── Undo / Redo ────────────────────────────────────────────────────────────

  undo: () => set((state) => {
    if (state.past.length === 0) return state
    const previous = state.past[state.past.length - 1]
    const newPast = state.past.slice(0, -1)
    const nodesById = buildNodesById(previous.nodes)
    const selId = state.selectedNodeId
    const selectedNode = selId ? (nodesById[selId] ?? null) : null

    scheduleCompile(previous.nodes, previous.edges, (compiled) => {
      set({ compiledGraph: compiled })
    })

    return {
      nodes: previous.nodes,
      edges: previous.edges,
      nodesById,
      selectedNode,
      past: newPast,
      future: [{ nodes: state.nodes, edges: state.edges }, ...state.future],
      isDirty: true,
    }
  }),

  redo: () => set((state) => {
    if (state.future.length === 0) return state
    const next = state.future[0]
    const newFuture = state.future.slice(1)
    const nodesById = buildNodesById(next.nodes)
    const selId = state.selectedNodeId
    const selectedNode = selId ? (nodesById[selId] ?? null) : null

    scheduleCompile(next.nodes, next.edges, (compiled) => {
      set({ compiledGraph: compiled })
    })

    return {
      nodes: next.nodes,
      edges: next.edges,
      nodesById,
      selectedNode,
      past: [...state.past, { nodes: state.nodes, edges: state.edges }],
      future: newFuture,
      isDirty: true,
    }
  }),

  // ── Selection ──────────────────────────────────────────────────────────────

  /**
   * PRIMARY: Only stores the ID — O(1), no array scan, no propagation.
   * The selectedNode convenience field is derived from nodesById.
   */
  setSelectedNodeId: (id) => set((state) => {
    if (state.selectedNodeId === id) return {}
    const selectedNode = id ? (state.nodesById[id] ?? null) : null
    return {
      selectedNodeId: id,
      selectedNode,
      selectedNodeIds: id ? [id] : [],
      selectedEdge: null,
    }
  }),

  /** Backward-compat wrapper. Internally only stores the ID. */
  setSelectedNode: (node) => set((state) => {
    const id = node?.id ?? null
    if (state.selectedNodeId === id) return {}
    return {
      selectedNodeId: id,
      selectedNode: id ? (state.nodesById[id] ?? node) : null,
      selectedNodeIds: id ? [id] : [],
      selectedEdge: null,
    }
  }),

  setSelectedNodeIds: (ids) => set((state) => {
    // Fast single-item path
    if (ids.length === 1) {
      const id = ids[0]
      return {
        selectedNodeIds: ids,
        selectedNodeId: id,
        selectedNode: state.nodesById[id] ?? null,
        selectedEdge: null,
      }
    }
    return {
      selectedNodeIds: ids,
      selectedNodeId: ids.length === 0 ? null : state.selectedNodeId,
      selectedNode: ids.length === 0 ? null : state.selectedNode,
      selectedEdge: null,
    }
  }),

  setSelectedEdge: (edge) => set({
    selectedEdge: edge,
    selectedNodeId: null,
    selectedNode: null,
    selectedNodeIds: [],
  }),

  // ── Other actions ──────────────────────────────────────────────────────────

  setViewMode: (mode) => set({ viewMode: mode }),

  setCanvas: (id, name) => set({ canvasId: id, canvasName: name, past: [], future: [], isDirty: false }),

  setActiveJob: (jobId) => set({ activeJobId: jobId }),

  updateJobProgress: (nodeId, progress) => set((state) => ({
    jobProgress: { ...state.jobProgress, [nodeId]: progress },
  })),

  setLineageHighlight: (path) => set({ lineageHighlight: path }),
  setPropagationHighlight: (path) => set({ propagationHighlight: path }),

  updateNodeStatus: (nodeId, status) => set((state) => {
    const normalized: 'idle' | 'running' | 'success' | 'error' =
      status === 'completed' ? 'success'
        : status === 'failed' ? 'error'
          : (status === 'idle' || status === 'running' || status === 'success' || status === 'error' ? status : 'idle')

    const matches = (n: Node) => n.id === nodeId || (n.data as any)?.node_id === nodeId
    const nodes = state.nodes.map((n) =>
      matches(n) ? { ...n, data: { ...n.data, status: normalized } } : n
    )
    const nodesById = buildNodesById(nodes)
    const selectedNode = state.selectedNodeId ? (nodesById[state.selectedNodeId] ?? null) : null

    return {
      nodeStatuses: { ...state.nodeStatuses, [nodeId]: normalized },
      nodes,
      nodesById,
      selectedNode,
    }
  }),

  updateNodeStatusBatch: (updates) => set((state) => {
    if (!updates || Object.keys(updates).length === 0) return {}
    const normalizedMap: Record<string, 'idle' | 'running' | 'success' | 'error'> = {}
    Object.entries(updates).forEach(([id, s]) => {
      normalizedMap[id] =
        s === 'completed' ? 'success'
          : s === 'failed' ? 'error'
            : (s === 'idle' || s === 'running' || s === 'success' || s === 'error' ? s : 'idle')
    })
    const nodes = state.nodes.map((n) => {
      const id = n.id || (n.data as any)?.node_id
      const s = id ? normalizedMap[id] : undefined
      return s != null ? { ...n, data: { ...n.data, status: s } } : n
    })
    const nodesById = buildNodesById(nodes)
    const selectedNode = state.selectedNodeId ? (nodesById[state.selectedNodeId] ?? null) : null
    return {
      nodeStatuses: { ...state.nodeStatuses, ...normalizedMap },
      nodes,
      nodesById,
      selectedNode,
    }
  }),

  setAllNodesStatus: (status) => set((state) => {
    const normalized = status === 'error' ? 'error' : 'success'
    const nextStatuses: Record<string, 'idle' | 'running' | 'success' | 'error'> = {}
    state.nodes.forEach((n) => {
      const id = n.id || (n.data as any)?.node_id
      if (id) nextStatuses[id] = normalized
    })
    const nodes = state.nodes.map((n) => ({ ...n, data: { ...n.data, status: normalized } }))
    const nodesById = buildNodesById(nodes)
    const selectedNode = state.selectedNodeId ? (nodesById[state.selectedNodeId] ?? null) : null
    return {
      nodeStatuses: { ...state.nodeStatuses, ...nextStatuses },
      nodes,
      nodesById,
      selectedNode,
    }
  }),

  clearNodeStatuses: () => set((state) => {
    const nodes = state.nodes.map((n) => ({ ...n, data: { ...n.data, status: 'idle' as const } }))
    const nodesById = buildNodesById(nodes)
    const selectedNode = state.selectedNodeId ? (nodesById[state.selectedNodeId] ?? null) : null
    return { nodeStatuses: {}, jobProgress: {}, nodes, nodesById, selectedNode }
  }),

  isDirty: false,
  setIsDirty: (isDirty) => set({ isDirty }),

  setPreview: (nodeId, visible, data) => set({
    previewNodeId: nodeId ?? null,
    previewVisible: visible,
    previewData: data ?? null,
  }),

  recompile: () => {
    const { nodes, edges } = get()
    scheduleCompile(nodes, edges, (compiled) => set({ compiledGraph: compiled }))
  },

  reset: () => set(initialState),
}))
