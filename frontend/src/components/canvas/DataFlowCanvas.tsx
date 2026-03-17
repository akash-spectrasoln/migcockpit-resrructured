/**
 * Data Flow Canvas - Chakra UI Version
 * Main canvas component with Chakra UI layout and React Flow
 */
import React, { useCallback, useRef, useEffect, useState, useMemo } from 'react'
import ReactFlow, {
  Node,
  Edge,
  addEdge,
  Connection,
  applyNodeChanges,
  applyEdgeChanges,
  Controls,
  Background,
  MiniMap,
  Panel,
  NodeTypes,
  EdgeTypes,
  SelectionMode,
} from 'reactflow'
import 'reactflow/dist/style.css'
import {
  Box,
  HStack,
  VStack,
  Button,
  IconButton,
  useDisclosure,
  Tooltip,
  Text,
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalBody,
  ModalFooter,
  ModalCloseButton,
  FormControl,
  FormLabel,
  Input,
  useToast,
  Progress,
  Spinner,
  Alert,
  AlertIcon,
  Link,
  Badge,
} from '@chakra-ui/react'
import { useNavigate, Link as RouterLink } from 'react-router-dom'
import { useColorModeValue } from '../../hooks/useColorModeValue'
import { Save, Play, CheckCircle, Eye, BarChart3, X, Trash2, ChevronLeft, ChevronRight, ChevronUp, ChevronDown, Undo, Redo, RefreshCw } from 'lucide-react'
import { findConnectedComponents, getFlowLabels } from '../../utils/graphUtils'
import { useCanvasStore } from '../../store/canvasStore'
import { nodeTypes } from './nodes/NodeTypes'
import { edgeTypes } from './interactions/EdgeTypes'
import { NodeConfigurationPanel } from './panels/NodeConfigPanel'
import { SourceConnectionsSidebar } from './sidebar/SourceConnectionsSidebar'
import { DestinationSelectorModal } from './interactions/DestinationSelectorModal'
import { SourceDetailsPanel } from './panels/SourceDetailsPanel'
import { ColumnPropertiesPanel } from './panels/ColumnPropertiesPanel'
import { FilterConfigPanel } from './panels/FilterConfigPanel'
import { JoinConfigPanel } from './panels/JoinConfigPanel'
import { ProjectionConfigPanel } from './panels/ProjectionConfigPanel'
import { DestinationConfigPanel } from './panels/DestinationConfigPanel'
import { CalculatedColumnConfigPanel } from './panels/CalculatedColumnConfigPanel'
import { AggregatesConfigPanel } from './panels/AggregatesConfigPanel'
import { ComputeNodeConfigPanel } from './panels/ComputeNodeConfigPanel'
import { TableDataPanel } from './panels/TableDataPanel'
import { ColumnDefinitionsMenu } from './panels/ColumnDefinitionsMenu'
import { NodeContextMenu } from './interactions/NodeContextMenu'
import { EdgeContextMenu } from './interactions/EdgeContextMenu'
import { NodeTypeSelectionModal } from './interactions/NodeTypeSelectionModal'
import { getNodeTypeDefinition } from '../../types/nodeRegistry'
import { canvasApi, migrationApi, pipelineApi, connectionApi, metadataApi } from '../../services/api'
import { wsService } from '../../services/websocket'
import { useSchemaDrift, clearTableSchemaCache } from '../../hooks/useSchemaDrift'
import {
  compilePipeline,
  buildGraph,
  propagateRemovedColumns as enginePropagateRemoved,
  propagateAddedColumns as enginePropagateAdded,
  getInputSchema,
  diffColumnSets,
} from '../../pipeline-engine'
import type { RawNode, RawEdge } from '../../pipeline-engine'

interface DataFlowCanvasProps {
  canvasId?: number
  initialNodes?: Node[]
  initialEdges?: Edge[]
  sourceId?: number
  projectId?: number
}

export const DataFlowCanvas: React.FC<DataFlowCanvasProps> = ({
  canvasId,
  initialNodes = [],
  initialEdges = [],
  sourceId,
  projectId,
}) => {
  const [selectedSource, setSelectedSource] = React.useState<any>(null)

  // ── Granular Zustand selectors ──────────────────────────────────────────────
  // Each selector subscribes only to the field(s) it needs.
  // This means an unrelated node update (e.g. position drag, status badge)
  // will NOT re-render the right panel component.

  // Graph data (render concern)
  const storeNodes      = useCanvasStore((s) => s.nodes)
  const storeEdges      = useCanvasStore((s) => s.edges)
  const nodesById       = useCanvasStore((s) => s.nodesById)
  const compiledGraph   = useCanvasStore((s) => s.compiledGraph)
  const nodeStatuses    = useCanvasStore((s) => s.nodeStatuses)
  const isDirty         = useCanvasStore((s) => s.isDirty)
  const viewMode        = useCanvasStore((s) => s.viewMode)
  const activeJobId     = useCanvasStore((s) => s.activeJobId)
  const past            = useCanvasStore((s) => s.past)
  const future          = useCanvasStore((s) => s.future)

  // Selection (render concern — changes on node click)
  const selectedNodeId  = useCanvasStore((s) => s.selectedNodeId)
  const selectedNode    = useCanvasStore((s) => s.selectedNode)
  const selectedNodeIds = useCanvasStore((s) => s.selectedNodeIds)

  // Highlights (render concern)
  const lineageHighlight      = useCanvasStore((s) => s.lineageHighlight)
  const propagationHighlight  = useCanvasStore((s) => s.propagationHighlight)

  // Preview state (render concern)
  const _previewNodeId  = useCanvasStore((s) => s.previewNodeId)
  const _previewVisible = useCanvasStore((s) => s.previewVisible)
  const _previewData    = useCanvasStore((s) => s.previewData)

  // Actions — stable references, never cause re-renders
  const setNodes              = useCanvasStore((s) => s.setNodes)
  const updateNodePositions   = useCanvasStore((s) => s.updateNodePositions)
  const setEdges              = useCanvasStore((s) => s.setEdges)
  const addNode               = useCanvasStore((s) => s.addNode)
  const deleteNode            = useCanvasStore((s) => s.deleteNode)
  const deleteNodeWithBridging = useCanvasStore((s) => s.deleteNodeWithBridging)
  const recompile             = useCanvasStore((s) => s.recompile)
  const setSelectedNode       = useCanvasStore((s) => s.setSelectedNode)
  const setSelectedNodeId     = useCanvasStore((s) => s.setSelectedNodeId)
  const setSelectedNodeIds    = useCanvasStore((s) => s.setSelectedNodeIds)
  const setViewMode           = useCanvasStore((s) => s.setViewMode)
  const setCanvas             = useCanvasStore((s) => s.setCanvas)
  const updateNodeStatus      = useCanvasStore((s) => s.updateNodeStatus)
  const updateNodeStatusBatch = useCanvasStore((s) => s.updateNodeStatusBatch)
  const updateJobProgress     = useCanvasStore((s) => s.updateJobProgress)
  const setAllNodesStatus     = useCanvasStore((s) => s.setAllNodesStatus)
  const setActiveJob          = useCanvasStore((s) => s.setActiveJob)
  const setLineageHighlight   = useCanvasStore((s) => s.setLineageHighlight)
  const setPropagationHighlight = useCanvasStore((s) => s.setPropagationHighlight)
  const setIsDirty            = useCanvasStore((s) => s.setIsDirty)
  const setPreview            = useCanvasStore((s) => s.setPreview)
  const undo                  = useCanvasStore((s) => s.undo)
  const redo                  = useCanvasStore((s) => s.redo)
  const clearNodeStatuses     = useCanvasStore((s) => s.clearNodeStatuses)


  // Schema drift utilities
  const { fetchLiveSchema, detectDrift } = useSchemaDrift()

  // Keep latest nodeStatuses in a ref so nodeTypes useMemo can read it without
  // depending on nodeStatuses (avoids recreating all node types on every WebSocket event).
  const nodeStatusesRef = useRef(nodeStatuses)
  nodeStatusesRef.current = nodeStatuses

  const lastClickedNodeIdRef = useRef<string | null>(null)

  const isExecuteInProgressRef = useRef(false)
  const executeFlowRef = useRef<((flowNodeIds?: string[]) => Promise<void>) | null>(null)
  const nodeProgressBatchRef = useRef<Record<string, { status: string; progress?: number }>>({})
  const nodeProgressFlushTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const clearStatusTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const reactFlowWrapper = useRef<HTMLDivElement>(null)
  const [reactFlowInstance, setReactFlowInstance] = useState<any>(null)
  const [edgeInsertModal, setEdgeInsertModal] = useState<{
    isOpen: boolean;
    edgeId: string;
    sourceNodeId: string;
    targetNodeId: string;
  } | null>(null)
  const [destinationSelectorModal, setDestinationSelectorModal] = useState<{
    isOpen: boolean;
    sourceNodeId: string;
    targetNodeId: string;
    edgeId: string;
  } | null>(null)
  const { isOpen: isConfigOpen, onClose: onConfigClose } = useDisclosure()
  const { isOpen: isSaveModalOpen, onOpen: onOpenSaveModal, onClose: onCloseSaveModal } = useDisclosure()
  const { isOpen: isJoinNodeSelectOpen, onOpen: onOpenJoinNodeSelect, onClose: onCloseJoinNodeSelect } = useDisclosure()
  const [joinNodeSource, setJoinNodeSource] = useState<Node | null>(null) // The node that triggered "Add Join"
  const [canvasName, setCanvasName] = useState<string>('')
  const [validationErrors, setValidationErrors] = useState<string[]>([])
  const [saveLoading, setSaveLoading] = useState(false)
  const [validateLoading, setValidateLoading] = useState(false)
  const [executeLoading, setExecuteLoading] = useState(false)
  const toast = useToast()
  const navigate = useNavigate()

  // Panel collapse and resize state
  const [leftPanelWidth, setLeftPanelWidth] = useState(300)
  const [rightPanelWidth, setRightPanelWidth] = useState(400)
  const [bottomPanelHeight, setBottomPanelHeight] = useState(200) // Reduced default height
  const [isResizing, setIsResizing] = useState<'left' | 'right' | 'bottom' | null>(null)
  const [leftPanelCollapsed, setLeftPanelCollapsed] = useState(false)
  const [rightPanelCollapsed, setRightPanelCollapsed] = useState(false)
  const [bottomPanelCollapsed, setBottomPanelCollapsed] = useState(false)
  const [, setShowAggregatesPanel] = useState(false)

  // Table data panel state
  const [tableDataPanel, setTableDataPanel] = useState<{
    sourceId?: number
    tableName?: string
    schema?: string
    nodeId?: string
    directFilterConditions?: any[]
  } | null>(null)

  // Execution monitor state (current run status from API + WebSocket)
  type ExecutionStatusType = 'idle' | 'pending' | 'running' | 'completed' | 'failed' | 'cancelled'
  const [executionStatus, setExecutionStatus] = useState<ExecutionStatusType>('idle')
  const [jobDetail, setJobDetail] = useState<{
    current_step: string | null
    progress: number
    error_message: string | null
    current_level?: number | null
    total_levels?: number | null
    level_status?: string | null
  }>({ current_step: null, progress: 0, error_message: null })

  // Column definitions menu state
  const [columnMenu, setColumnMenu] = useState<{
    sourceId: number
    tableName: string
    schema?: string
    position: { x: number; y: number }
  } | null>(null)

  // Node context menu state
  const [contextMenu, setContextMenu] = useState<{
    node: Node
    position: { x: number; y: number }
  } | null>(null)
  const [edgeContextMenu, setEdgeContextMenu] = useState<{
    edge: Edge
    position: { x: number; y: number }
  } | null>(null)

  // Direct Filter mode state (no canvas nodes)
  const [directFilterMode, setDirectFilterMode] = useState<{
    sourceId: number
    tableName: string
    schema?: string
  } | null>(null)

  // Table filter state management (session-based, per table)
  const [tableFilters, setTableFilters] = useState<Map<string, {
    sourceId: number
    tableName: string
    schema?: string
    conditions: any[]
    expression?: string
    mode?: 'builder' | 'expression'
  }>>(new Map())

  const getColKey = useCallback((col: any): string => {
    if (col == null) return ''
    if (typeof col === 'string') return col
    return col.technical_name || col.name || col.column_name || col.db_name || String(col)
  }, [])

  const diffColumns = useCallback((prevCols: any[] = [], nextCols: any[] = []) => {
    const prevSet = new Set(prevCols.map(getColKey).filter(Boolean))
    const nextSet = new Set(nextCols.map(getColKey).filter(Boolean))
    const removed: string[] = []
    const added: string[] = []
    prevSet.forEach((name) => { if (!nextSet.has(name)) removed.push(name) })
    nextSet.forEach((name) => { if (!prevSet.has(name)) added.push(name) })
    return { removed, added }
  }, [getColKey])

  /**
   * Engine-delegating wrapper: remove columns BFS downstream.
   * Delegates to pipeline-engine/propagate.ts which uses the pre-built graph.
   * Call signature kept identical to the old inline implementation so all call sites work.
   */
  const propagateRemovedColumnsHard = useCallback(
    (nds: Node[], edgs: Edge[], sourceNodeId: string, removedNames: string[]): Node[] => {
      if (!removedNames.length) return nds
      const graph = buildGraph(nds as RawNode[], edgs as RawEdge[])
      return enginePropagateRemoved(nds as RawNode[], graph, sourceNodeId, removedNames) as Node[]
    },
    []
  )

  const addColumnsToImmediateDownstreamInputs = useCallback(
    (nds: Node[], edgs: Edge[], sourceNodeId: string, addedColumns: any[]): Node[] => {
      if (!addedColumns?.length) return nds
      const graph = buildGraph(nds as RawNode[], edgs as RawEdge[])
      return enginePropagateAdded(
        nds as RawNode[],
        graph,
        sourceNodeId,
        addedColumns
      ) as Node[]
    },
    []
  )


  /**
   * Update immediate downstream nodes' input_metadata and mark them (and their descendants)
   * as schema_outdated without changing any downstream output_metadata.
   */
  const updateImmediateDownstreamInputs = useCallback(
    (nds: Node[], edgs: Edge[], sourceNodeId: string, newOutputMetadata: { columns?: any[]; nodeId?: string }) => {
      if (!newOutputMetadata?.columns?.length) return nds
      const edgesList = edgs && Array.isArray(edgs) ? edgs : []
      const outEdges = edgesList.filter((e: Edge) => e.source === sourceNodeId)
      if (outEdges.length === 0) return nds

      const immediateTargets = new Set<string>()
      outEdges.forEach((e) => immediateTargets.add(e.target))

      // First, update immediate downstream input_metadata
      let updated = nds.map((n) =>
        immediateTargets.has(n.id)
          ? {
              ...n,
              data: {
                ...n.data,
                input_metadata: {
                  ...(n.data.input_metadata || {}),
                  ...newOutputMetadata,
                },
                schema_outdated: true,
              },
            }
          : n
      )

      // Then, mark all recursively downstream nodes as schema_outdated (do not touch schemas)
      const queue: string[] = Array.from(immediateTargets)
      const visited = new Set<string>(queue)

      while (queue.length > 0) {
        const current = queue.shift()!
        const children = edgesList.filter((e) => e.source === current).map((e) => e.target)
        children.forEach((childId) => {
          if (!visited.has(childId)) {
            visited.add(childId)
            queue.push(childId)
          }
        })
      }

      if (visited.size > 0) {
        updated = updated.map((n) =>
          visited.has(n.id)
            ? {
                ...n,
                data: {
                  ...n.data,
                  schema_outdated: true,
                },
              }
            : n
        )
      }

      return updated
    },
    []
  )

  // ──────────────────────────────────────────────────────────────────────────
  // Schema Drift on Canvas Load
  // Runs once per canvas (canvasId) after nodes are populated in the store.
  // For each source node:
  //   1. Fetches live schema and compares with stored output_metadata
  //   2. REMOVED columns  → propagateRemovedColumnsHard (downstream errors on
  //      filters/calculated/compute/aggregate that reference the column)
  //   3. ADDED columns    → addColumnsToImmediateDownstreamInputs + schema_drift badge
  //   4. TYPE changes     → update output_metadata + mark downstream schema_outdated
  //   5. Shows a toast summarising what changed
  // ──────────────────────────────────────────────────────────────────────────
  const driftAlreadyRanRef = useRef<string | null>(null) // tracks last canvasId

  useEffect(() => {
    // Gate: only run once per canvas load, and only when there are nodes
    const keyForCanvas = canvasId ? String(canvasId) : '__new__'
    if (!storeNodes.length) return
    if (driftAlreadyRanRef.current === keyForCanvas) return
    driftAlreadyRanRef.current = keyForCanvas

    // Only run on saved canvases (no point drifting on a brand-new empty canvas)
    if (!canvasId) return

    const runDrift = async () => {
      try {
        const nodes = useCanvasStore.getState().nodes
        const edges = useCanvasStore.getState().edges

        const driftResults = await detectDrift(nodes, false)
        if (!driftResults.length) return

        // ── Step 1: populate output_metadata for brand-new nodes ──────────
        // These are source nodes that were just dragged but never had their
        // schema snapshot saved (output_metadata is null or has no columns).
        let updatedNodes: Node[] = [...nodes]
        for (const result of driftResults) {
          if (!result.liveColumns.length) continue
          updatedNodes = updatedNodes.map((n) => {
            if (n.id !== result.nodeId) return n
            if (n.data?.output_metadata?.columns?.length) return n // already has snapshot
            return {
              ...n,
              data: {
                ...n.data,
                output_metadata: {
                  ...(n.data.output_metadata || {}),
                  columns: result.liveColumns.map((c) => ({
                    name: c.name,
                    column_name: c.name,
                    technical_name: c.name,
                    type: c.type,
                    datatype: c.type,
                    nullable: true,
                  })),
                },
              },
            }
          })
        }

        if (!driftResults.some((r) => r.hasDrift)) {
          // No drift — just persist the snapshot fix and stop
          useCanvasStore.getState().setNodes(updatedNodes)
          return
        }

        // ── Step 2: apply changes for each drifted source node ────────────
        const driftedResults = driftResults.filter((r) => r.hasDrift)
        const toastLines: string[] = []

        for (const result of driftedResults) {
          const { nodeId, addedColumns, removedColumns, typeChanges } = result

          // a) Update source node's output_metadata (merge live schema) and
          //    set the drift badge (shown as ⚠ on the canvas node)
          updatedNodes = updatedNodes.map((n) => {
            if (n.id !== nodeId) return n
            const existingCols: any[] = n.data?.output_metadata?.columns ?? []
            const removedSet = new Set(removedColumns.map((c) => c.name.toLowerCase()))
            const typeMap = new Map(typeChanges.map((c) => [c.name.toLowerCase(), c.newType]))

            // Filter out removed, update types
            let mergedCols = existingCols
              .filter((c: any) => !removedSet.has((c.name || c.column_name || '').toLowerCase()))
              .map((c: any) => {
                const key = (c.name || c.column_name || '').toLowerCase()
                return typeMap.has(key)
                  ? { ...c, type: typeMap.get(key), datatype: typeMap.get(key) }
                  : c
              })

            // Append new columns
            addedColumns.forEach((newCol) => {
              if (!mergedCols.some((c: any) => (c.name || c.column_name || '') === newCol.name)) {
                mergedCols.push({
                  name: newCol.name,
                  column_name: newCol.name,
                  technical_name: newCol.name,
                  type: newCol.type,
                  datatype: newCol.type,
                  nullable: true,
                })
              }
            })

            const driftSummary = [
              ...addedColumns.map((c) => `+ ${c.name} (new column)`),
              ...removedColumns.map((c) => `- ${c.name} (deleted from source)`),
              ...typeChanges.map((c) => `~ ${c.name}: ${c.oldType} → ${c.newType}`),
            ]

            const tableName = n.data?.config?.tableName || n.data?.business_name || 'Source'
            if (addedColumns.length) toastLines.push(`${tableName}: +${addedColumns.length} column(s) added`)
            if (removedColumns.length) toastLines.push(`${tableName}: -${removedColumns.length} column(s) deleted`)
            if (typeChanges.length) toastLines.push(`${tableName}: ${typeChanges.length} type change(s)`)

            return {
              ...n,
              data: {
                ...n.data,
                output_metadata: { ...(n.data.output_metadata || {}), columns: mergedCols },
                schema_drift: { addedColumns, removedColumns, typeChanges, summary: driftSummary },
                schema_outdated: addedColumns.length > 0 || typeChanges.length > 0,
              },
            }
          })

          // b) REMOVED columns — hard propagate: strip from downstream metadata
          //    AND set error badges on nodes that reference them in logic
          if (removedColumns.length > 0) {
            updatedNodes = propagateRemovedColumnsHard(
              updatedNodes,
              edges,
              nodeId,
              removedColumns.map((c) => c.name)
            )
          }

          // c) ADDED columns — add to immediate downstream input_metadata
          //    so the next node can see them and choose to include them
          if (addedColumns.length > 0) {
            const addedAsCols = addedColumns.map((c) => ({
              name: c.name,
              column_name: c.name,
              technical_name: c.name,
              type: c.type,
              datatype: c.type,
              nullable: true,
            }))
            const sourceNode = updatedNodes.find((n) => n.id === nodeId)
            if (sourceNode) {
              updatedNodes = addColumnsToImmediateDownstreamInputs(updatedNodes, edges, nodeId, addedAsCols)
            }
          }

          // d) TYPE changes — mark downstream as schema_outdated (they may need
          //    to re-propagate if they use typed expressions)
          if (typeChanges.length > 0) {
            updatedNodes = updateImmediateDownstreamInputs(
              updatedNodes,
              edges,
              nodeId,
              updatedNodes.find((n: Node) => n.id === nodeId)?.data?.output_metadata || {}
            )
          }
        }

        useCanvasStore.getState().setNodes(updatedNodes)

        // ── Step 3: show toast summary ─────────────────────────────────────
        if (toastLines.length > 0) {
          toast({
            title: '⚠ Schema drift detected',
            description: toastLines.join('\n'),
            status: 'warning',
            duration: 8000,
            isClosable: true,
            position: 'bottom-right',
          })
        }

        console.info(
          `[SchemaDrift] Applied drift for ${driftedResults.length} source node(s):`,
          driftedResults.map((r) => ({
            node: r.nodeId,
            added: r.addedColumns.length,
            removed: r.removedColumns.length,
            typeChanged: r.typeChanges.length,
          }))
        )
      } catch (err) {
        console.warn('[SchemaDrift] Drift detection failed (non-critical):', err)
      }
    }

    // Slight delay so canvas renders first, then run drift in background
    const timer = setTimeout(runDrift, 700)
    return () => clearTimeout(timer)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canvasId, storeNodes.length]) // Re-run if canvas changes or nodes first appear

  /**
   * Recursively recompute downstream nodes' output_metadata based on their configs and
   * current input_metadata, updating their immediate downstream input_metadata and
   * clearing schema_outdated.
   */
  const propagateSchemaRecursively = useCallback(
    (nds: Node[], edgs: Edge[], sourceNodeId: string, sourceOutputMetadata: { columns?: any[]; nodeId?: string }) => {
      if (!sourceOutputMetadata?.columns?.length) return nds
      const edgesList = edgs && Array.isArray(edgs) ? edgs : []
      const schemaTransparentTypes = ['filter', 'projection', 'join', 'calculated', 'aggregate', 'compute', 'destination']

      const queue: string[] = [sourceNodeId]
      const visited = new Set<string>([sourceNodeId])
      let updated = nds

      // Helper to derive columns metadata array from a generic columns list
      const toColumnsMetadata = (cols: any[]) =>
        cols.map((col: any) =>
          typeof col === 'string'
            ? { name: col, technical_name: col, db_name: col, datatype: 'TEXT', nullable: true }
            : {
                name: col.business_name || col.name || col.column_name || col,
                technical_name: col.technical_name || col.db_name || col.name,
                db_name: col.db_name,
                datatype: col.datatype || col.data_type || 'TEXT',
                nullable: col.nullable !== undefined ? col.nullable : true,
              }
        )

      // Breadth-first propagation. When propagating, add new columns to each node's config
      // so they are checked/included (Join checkmark, Projection included, etc.).
      while (queue.length > 0) {
        const currentId = queue.shift()!
        const currentNode = updated.find((n) => n.id === currentId)
        if (!currentNode) continue

        const outEdges = edgesList.filter((e: Edge) => e.source === currentId)
        if (outEdges.length === 0) continue

        const currentAvailableMeta =
          currentId === sourceNodeId
            ? sourceOutputMetadata
            : (currentNode.data?.input_metadata as any) || (currentNode.data?.output_metadata as any)

        if (!currentAvailableMeta?.columns?.length) continue

        const columnsForConfig = toColumnsMetadata(currentAvailableMeta.columns)
        const immediateTargetIds = new Set(outEdges.map((e) => e.target))

        // Process each edge so we have targetHandle for Join (left vs right)
        for (const edge of outEdges) {
          const targetId = edge.target
          const targetHandle = (edge.targetHandle as string) || 'left'
          const targetNode = updated.find((n) => n.id === targetId)
          if (!targetNode) continue

          const targetType = String(targetNode.data?.type || '').toLowerCase()
          const data = targetNode.data as any
          const existingInputCols = (data?.input_metadata?.columns ?? []) as any[]
          const existingInputNames = new Set(
            existingInputCols.map((c: any) => (typeof c === 'string' ? c : c?.name ?? c?.column_name ?? c))
          )
          const mergedInputCols = [...existingInputCols]
          currentAvailableMeta.columns.forEach((col: any) => {
            const name = typeof col === 'string' ? col : (col?.name ?? col?.column_name)
            if (name && !existingInputNames.has(name)) {
              existingInputNames.add(name)
              mergedInputCols.push(col)
            }
          })
          const mergedInputMeta = {
            ...(data?.input_metadata || {}),
            ...currentAvailableMeta,
            columns: mergedInputCols.length > 0 ? mergedInputCols : currentAvailableMeta.columns,
          }

          if (!schemaTransparentTypes.includes(targetType)) {
            updated = updated.map((n) =>
              n.id === targetId
                ? {
                    ...n,
                    data: {
                      ...n.data,
                      input_metadata: mergedInputMeta,
                      schema_outdated: false,
                    },
                  }
                : n
            )
          } else {
            let nextConfig = { ...(data?.config || {}) }
            let nextOutputMeta = { ...currentAvailableMeta, columns: columnsForConfig }

            if (targetType === 'join') {
              const upstreamNames = new Set(
                columnsForConfig.map((c: any) => c?.name ?? c?.column_name ?? c)
              )
              const existingOutputCols = (nextConfig.outputColumns ?? []) as any[]
              const existingKeys = new Set(existingOutputCols.map((c: any) => `${c.source}:${c.column}`))
              const leftCols = new Set(
                existingOutputCols.filter((c: any) => c.source === 'left').map((c: any) => c.column)
              )
              const rightCols = new Set(
                existingOutputCols.filter((c: any) => c.source === 'right').map((c: any) => c.column)
              )
              const toAdd: any[] = []
              columnsForConfig.forEach((col: any) => {
                const name = col?.name ?? col?.column_name ?? col
                if (!name) return
                const key = `${targetHandle}:${name}`
                if (existingKeys.has(key)) return
                existingKeys.add(key)
                const alias = targetHandle === 'left' ? `_L_.${name}` : `_R_.${name}`
                const outputName =
                  targetHandle === 'left' && rightCols.has(name)
                    ? `_L_${name}`
                    : targetHandle === 'right' && leftCols.has(name)
                      ? `_R_${name}`
                      : name
                toAdd.push({
                  source: targetHandle,
                  column: name,
                  alias,
                  outputName,
                  included: true,
                  datatype: col.datatype ?? 'TEXT',
                  nullable: col.nullable !== undefined ? col.nullable : true,
                  isPrimaryKey: col.isPrimaryKey ?? false,
                })
              })
              const mergedOutputCols = [...existingOutputCols, ...toAdd].map((c: any) =>
                upstreamNames.has(c.column)
                  ? { ...c, included: true }
                  : c
              )
              nextConfig = {
                ...nextConfig,
                outputColumns: mergedOutputCols,
              }
              const includedOutputCols = (nextConfig.outputColumns as any[]).filter(
                (c: any) => c.included !== false
              )
              nextOutputMeta = {
                ...currentAvailableMeta,
                columns: includedOutputCols.map((c: any) => {
                  const meta = columnsForConfig.find(
                    (m: any) => (m?.name ?? m?.column_name) === c.column
                  )
                  return meta
                    ? { ...meta, name: c.outputName ?? c.column }
                    : {
                        name: c.outputName ?? c.column,
                        technical_name: c.column,
                        datatype: (c as any).datatype ?? 'TEXT',
                        nullable: (c as any).nullable !== undefined ? (c as any).nullable : true,
                      }
                }),
              }
            } else if (targetType === 'projection') {
              const existingOrder = (nextConfig.columnOrder ?? []) as any[]
              const upstreamNames = new Set(
                columnsForConfig.map((c: any) => c?.name ?? c?.column_name ?? c)
              )
              const existingNames = new Set(
                existingOrder.map((c: any) => (typeof c === 'string' ? c : c?.name ?? c))
              )
              let maxOrder = existingOrder.reduce(
                (m: number, c: any) => Math.max(m, (c?.order ?? 0)),
                0
              )
              const toAdd: any[] = []
              columnsForConfig.forEach((col: any) => {
                const name = col?.name ?? col?.column_name ?? col
                if (!name || existingNames.has(name)) return
                existingNames.add(name)
                toAdd.push({
                  name,
                  order: ++maxOrder,
                  included: true,
                  technical_name: col.technical_name ?? name,
                  isCalculated: false,
                })
              })
              // When propagating, include all columns from upstream (set included: true for any in columnOrder that upstream sends)
              const mergedOrder = [...existingOrder, ...toAdd]
              const normalizedOrder = mergedOrder.map((c: any) => {
                const name = typeof c === 'string' ? c : c?.name ?? c
                if (upstreamNames.has(name)) {
                  return typeof c === 'object' && c !== null
                    ? { ...c, included: true, order: Math.max(0, (c?.order ?? 0)) }
                    : { name, order: 0, included: true, isCalculated: false }
                }
                return c
              })
              const includedNames = normalizedOrder
                .filter((c: any) => c.included !== false && (c.order ?? 0) >= 0)
                .sort((a: any, b: any) => (a.order ?? 0) - (b.order ?? 0))
                .map((c: any) => c.name)
              nextConfig = {
                ...nextConfig,
                columnOrder: normalizedOrder,
                includedColumns: includedNames,
                output_columns: includedNames,
                selectedColumns: includedNames,
              }
              const nameSet = new Set(includedNames)
              const filteredCols = columnsForConfig.filter((c: any) =>
                nameSet.has(c?.name ?? c?.column_name ?? c)
              )
              nextOutputMeta = { ...currentAvailableMeta, columns: filteredCols }
            } else {
              nextOutputMeta = { ...currentAvailableMeta, columns: columnsForConfig }
            }

            updated = updated.map((n) =>
              n.id === targetId
                ? {
                    ...n,
                    data: {
                      ...n.data,
                      config: nextConfig,
                      input_metadata: mergedInputMeta,
                      output_metadata: nextOutputMeta,
                      schema_outdated: false,
                      schema_version:
                        typeof (n.data as any).schema_version === 'number'
                          ? (n.data as any).schema_version + 1
                          : 1,
                    },
                  }
                : n
            )
          }
        }

        immediateTargetIds.forEach((targetId) => {
          if (!visited.has(targetId)) {
            visited.add(targetId)
            queue.push(targetId)
          }
        })
      }

      return updated
    },
    []
  )

  /** BFS: Get all downstream nodes and edges from startNodeId until destination sinks. */
  const getDownstreamPath = useCallback((startNodeId: string, nds: Node[], edgs: Edge[]) => {
    const nodeIds = new Set<string>([startNodeId])
    const edgeIds = new Set<string>()
    const edgesList = edgs && Array.isArray(edgs) ? edgs : []
    const queue = [startNodeId]
    const visited = new Set<string>([startNodeId])

    while (queue.length > 0) {
      const currentId = queue.shift()!
      const outEdges = edgesList.filter((e: Edge) => e.source === currentId)
      for (const e of outEdges) {
        if (e.id) edgeIds.add(e.id)
        if (!visited.has(e.target)) {
          visited.add(e.target)
          nodeIds.add(e.target)
          const targetNode = nds.find((n) => n.id === e.target)
          const isDestination = targetNode?.data?.type === 'destination' || String(targetNode?.data?.type || '').startsWith('destination-')
          if (!isDestination) queue.push(e.target)
        }
      }
    }
    return { nodeIds: Array.from(nodeIds), edgeIds: Array.from(edgeIds) }
  }, [])

  // Helper function to get filter key for a table
  const getFilterKey = useCallback((sourceId: number, tableName: string, schema?: string) => {
    return `${sourceId}_${tableName}_${schema || 'default'}`
  }, [])

  // Helper function to check if a table has a filter
  const hasTableFilter = useCallback((sourceId: number, tableName: string, schema?: string) => {
    const key = getFilterKey(sourceId, tableName, schema)
    return tableFilters.has(key)
  }, [tableFilters, getFilterKey])

  // Helper function to get filter for a table
  const getTableFilter = useCallback((sourceId: number, tableName: string, schema?: string) => {
    const key = getFilterKey(sourceId, tableName, schema)
    return tableFilters.get(key) || null
  }, [tableFilters, getFilterKey])

  // Helper function to set filter for a table
  const setTableFilter = useCallback((sourceId: number, tableName: string, schema: string | undefined, filter: {
    conditions: any[]
    expression?: string
    mode?: 'builder' | 'expression'
  }) => {
    const key = getFilterKey(sourceId, tableName, schema)
    setTableFilters(prev => {
      const newMap = new Map(prev)
      newMap.set(key, {
        sourceId,
        tableName,
        schema,
        ...filter,
      })
      return newMap
    })
  }, [getFilterKey])

  // Helper function to remove filter for a table
  const removeTableFilter = useCallback((sourceId: number, tableName: string, schema?: string) => {
    const key = getFilterKey(sourceId, tableName, schema)
    setTableFilters(prev => {
      const newMap = new Map(prev)
      newMap.delete(key)
      return newMap
    })
  }, [getFilterKey])

  // Load filters from localStorage on mount
  useEffect(() => {
    try {
      const loadedFilters = new Map<string, {
        sourceId: number
        tableName: string
        schema?: string
        conditions: any[]
        expression?: string
        mode?: 'builder' | 'expression'
      }>()

      // Scan localStorage for table filters
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i)
        if (key && key.startsWith('table_filter_')) {
          try {
            const filterData = JSON.parse(localStorage.getItem(key) || '{}')
            // Extract sourceId, tableName, schema from key
            const parts = key.replace('table_filter_', '').split('_')
            if (parts.length >= 2) {
              const sourceId = parseInt(parts[0])
              const schema = parts.length > 2 ? parts.slice(1, -1).join('_') : undefined
              const tableName = parts[parts.length - 1] === 'default'
                ? parts.slice(1, -1).join('_')
                : parts[parts.length - 1]

              if (!isNaN(sourceId) && tableName) {
                const filterKey = getFilterKey(sourceId, tableName, schema === 'default' ? undefined : schema)
                loadedFilters.set(filterKey, {
                  sourceId,
                  tableName,
                  schema: schema === 'default' ? undefined : schema,
                  conditions: filterData.conditions || [],
                  expression: filterData.expression || '',
                  mode: filterData.mode || 'builder',
                })
              }
            }
          } catch (err) {
            console.warn('Failed to parse filter from localStorage:', key, err)
          }
        }
      }

      if (loadedFilters.size > 0) {
        setTableFilters(loadedFilters)
      }
    } catch (err) {
      console.warn('Failed to load filters from localStorage:', err)
    }
  }, [getFilterKey])

  // Normalize initial nodes to ensure they have node identity properties
  // Use initialNodes/initialEdges if provided, otherwise use store nodes/edges
  const nodesToNormalize = useMemo(() => {
    // If initialNodes is explicitly provided (even if empty), use it
    // Otherwise, use store nodes (for backward compatibility)
    if (initialNodes !== undefined) {
      return initialNodes
    }
    return storeNodes || []
  }, [initialNodes, storeNodes])

  const edgesToNormalize = useMemo(() => {
    // If initialEdges is explicitly provided (even if empty), use it
    // Otherwise, use store edges (for backward compatibility)
    if (initialEdges !== undefined) {
      return initialEdges
    }
    return storeEdges || []
  }, [initialEdges, storeEdges])

  const normalizedInitialNodes = useMemo(() => {
    // First, derive input_nodes from edges if missing (for backward compatibility when loading from backend)
    const nodeInputNodesMap = new Map<string, string[]>()
    if (edgesToNormalize && Array.isArray(edgesToNormalize)) {
      edgesToNormalize.forEach((edge) => {
        const targetId = edge.target
        const sourceId = edge.source
        if (!nodeInputNodesMap.has(targetId)) {
          nodeInputNodesMap.set(targetId, [])
        }
        const inputNodes = nodeInputNodesMap.get(targetId)!
        if (!inputNodes.includes(sourceId)) {
          inputNodes.push(sourceId)
        }
      })
    }

    return nodesToNormalize.map((node) => {
      // Generate technical_name if missing (for backward compatibility)
      let technicalName = node.data.technical_name
      if (!technicalName && node.data.node_id) {
        const shortId = node.data.node_id.substring(0, 8)
        const nodeType = (node.data.node_type || node.data.type || 'unknown').toLowerCase()
        const technicalNamePrefixMap: Record<string, string> = {
          'source': 'source',
          'filter': 'filter',
          'projection': 'projection',
          'join': 'join',
          'calculated': 'calculated',
          'destination': 'destination',
          'group': 'group',
          'sort': 'sort',
          'union': 'union',
          'aggregation': 'aggregation',
          'transform': 'transform',
        }
        const prefix = technicalNamePrefixMap[nodeType] || nodeType
        technicalName = `${prefix}_${shortId}`
      }

      // Generate business_name if missing (for backward compatibility)
      let businessName = node.data.business_name
      if (!businessName) {
        const defaultBusinessNameMap: Record<string, string> = {
          'source': 'Source Table',
          'filter': 'Filter',
          'projection': 'Projection',
          'join': 'Join',
          'calculated': 'Calculated Column',
          'destination': 'Destination',
          'group': 'Group',
          'sort': 'Sort',
          'union': 'Union',
          'aggregation': 'Aggregation',
          'transform': 'Transform',
        }
        const nodeType = (node.data.node_type || node.data.type || 'unknown').toLowerCase()
        businessName = defaultBusinessNameMap[nodeType] || node.data.node_name || node.data.label || 'Node'
      }

      // Derive input_nodes from edges if not present in node data (for backward compatibility)
      let inputNodes = node.data.input_nodes || []
      if (inputNodes.length === 0 && nodeInputNodesMap.has(node.id)) {
        inputNodes = nodeInputNodesMap.get(node.id) || []
      }

      return {
        ...node,
        data: {
          ...node.data,
          // Ensure node identity properties exist
          node_id: node.data.node_id || node.id,
          business_name: businessName, // Required, editable
          technical_name: technicalName, // Required, non-editable
          node_name: node.data.node_name || businessName, // Legacy support (editable)
          node_type: node.data.node_type || node.data.type?.toUpperCase() || 'UNKNOWN',
          input_nodes: inputNodes, // Populated from saved data or derived from edges
          output_metadata: node.data.output_metadata || null,
        },
      }
    })
  }, [nodesToNormalize, edgesToNormalize])

  // Normalize initial edges: Ensure join nodes have targetHandle set
  const normalizedInitialEdges = useMemo(() => {
    if (!edgesToNormalize || !Array.isArray(edgesToNormalize)) {
      return []
    }

    return edgesToNormalize.map((edge) => {
      // Check if target is a join node
      const targetNode = normalizedInitialNodes.find((n) => n.id === edge.target)
      if (targetNode?.data?.type === 'join') {
        // If targetHandle is missing, try to infer it from existing edges
        if (!edge.targetHandle) {
          // Find other edges to this join node
          const otherEdgesToJoin = edgesToNormalize.filter(
            (e) => e.target === edge.target && e.id !== edge.id
          )

          // Check if left handle is already taken
          const leftEdgeExists = otherEdgesToJoin.some((e) => e.targetHandle === 'left')

          // Assign targetHandle: first edge gets 'left', second gets 'right'
          if (!leftEdgeExists) {
            edge.targetHandle = 'left'
          } else {
            edge.targetHandle = 'right'
          }
        }
      }
      return edge
    })
  }, [edgesToNormalize, normalizedInitialNodes])

  // Independent flows (connected components) for separate job execution
  const { flows, flowLabels: _flowLabels } = useMemo(() => {
    const nodeList = storeNodes
      .filter((n) => n.id)
      .map((n) => ({ id: n.id, type: n.type, data: n.data }))
    const edgeList = storeEdges
      .filter((e) => e.source && e.target)
      .map((e) => ({ source: e.source, target: e.target }))
    const comps = findConnectedComponents(nodeList, edgeList)
    const labels = getFlowLabels(comps, nodeList)
    // Sort by label so flow order is stable (e.g. flow 1 = tool_user, flow 2 = tool_company, tool_connection)
    const sorted = comps
      .map((c, i) => ({ flow: c, label: labels[i] ?? '' }))
      .sort((a, b) => a.label.localeCompare(b.label))
    return { flows: sorted.map((s) => s.flow), flowLabels: sorted.map((s) => s.label) }
  }, [storeNodes, storeEdges])

  const hasMultipleFlows = flows.length > 1

  const bg = useColorModeValue('gray.50', 'gray.900')
  const toolbarBg = useColorModeValue('white', 'gray.800')

  // Ref used by edge/node insert handlers to cancel in-flight operations (kept for API call safety)
  const syncTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Seed Zustand store from initial nodes/edges on first mount.
  // ReactFlow reads exclusively from storeNodes/storeEdges, so we must populate the store first.
  useEffect(() => {
    if (normalizedInitialNodes.length > 0 && storeNodes.length === 0) {
      setNodes(normalizedInitialNodes)
    }
    if (normalizedInitialEdges.length > 0 && storeEdges.length === 0) {
      // Fix join node edge targetHandles before seeding
      const fixedEdges = normalizedInitialEdges.map((edge) => {
        const targetNode = normalizedInitialNodes.find((n) => n.id === edge.target)
        if (targetNode?.data?.type === 'join' && !edge.targetHandle) {
          const otherEdgesToJoin = normalizedInitialEdges.filter(
            (e) => e.target === edge.target && e.id !== edge.id
          )
          const leftEdgeExists = otherEdgesToJoin.some((e) => e.targetHandle === 'left')
          return { ...edge, targetHandle: leftEdgeExists ? 'right' : 'left' }
        }
        return edge
      })
      setEdges(fixedEdges)
    }
  }, []) // run once on mount

  // Reconcile Filter output_metadata from upstream when loaded (e.g. p1 adds upper_name, Filter was stale)
  // Also: when input is projection, prefer projection's config (output_columns/includedColumns) over output_metadata
  // if config has more columns — fixes mismatch where config panel shows 28 but node shows 24
  const filterMetadataReconciledRef = useRef<string>('')
  useEffect(() => {
    if (!storeNodes?.length || !storeEdges?.length) return
    const edgesList = Array.isArray(storeEdges) ? storeEdges : []
    let updated = storeNodes
    let changed = false
    for (const node of updated) {
      if (node.data?.type !== 'filter') continue
      const inputEdge = edgesList.find((e: Edge) => e.target === node.id)
      if (!inputEdge) continue
      const inputNode = updated.find((n) => n.id === inputEdge.source)
      let inputMeta = inputNode?.data?.output_metadata?.columns
      // When input is projection, prefer config.output_columns/includedColumns if they have more columns
      if (inputNode?.data?.type === 'projection') {
        const configCols = inputNode.data.config?.output_columns || inputNode.data.config?.includedColumns || []
        if (Array.isArray(configCols) && configCols.length > (inputMeta?.length ?? 0)) {
          inputMeta = configCols.map((c: string) => (typeof c === 'string' ? { name: c, technical_name: c } : c))
        }
      }
      if (!inputMeta?.length) continue
      const filterMeta = node.data?.output_metadata?.columns || node.data?.config?.columns || []
      const inputLen = inputMeta.length
      const filterLen = Array.isArray(filterMeta) ? filterMeta.length : 0
      if (inputLen > filterLen) {
        const colsForConfig = inputMeta.map((col: any) =>
          typeof col === 'string'
            ? { name: col, technical_name: col, db_name: col, datatype: 'TEXT', nullable: true }
            : {
                name: col.business_name || col.name || col.column_name || col,
                technical_name: col.technical_name || col.db_name || col.name,
                db_name: col.db_name,
                datatype: col.datatype || col.data_type || 'TEXT',
                nullable: col.nullable !== undefined ? col.nullable : true,
              }
        )
        updated = updated.map((n) =>
          n.id === node.id
            ? {
                ...n,
                data: {
                  ...n.data,
                  output_metadata: { columns: inputMeta, nodeId: inputNode?.data?.node_id || inputNode?.id },
                  config: { ...n.data.config, columns: colsForConfig },
                },
              }
            : n
        )
        changed = true
      }
    }
    if (changed) {
      const key = updated.map((n) => `${n.id}:${(n.data?.output_metadata?.columns?.length ?? 0)}`).join('|')
      if (filterMetadataReconciledRef.current !== key) {
        filterMetadataReconciledRef.current = key
        // Only call setNodes if result is structurally different to prevent update loops
        try {
          if (JSON.stringify(storeNodes) !== JSON.stringify(updated)) setNodes(updated)
        } catch (_) {
          setNodes(updated)
        }
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [storeNodes, storeEdges])

  // Fix edges for join nodes: Ensure targetHandle is set
  const joinEdgeFixAppliedRef = useRef<string>('')
  useEffect(() => {
    if (!storeEdges || storeEdges.length === 0 || !storeNodes || storeNodes.length === 0) return

    // Only fix edges that truly lack a targetHandle on join nodes
    const edgesNeedingFix = storeEdges.filter((edge) => {
      const targetNode = storeNodes.find((n) => n.id === edge.target)
      return targetNode?.data?.type === 'join' && !edge.targetHandle
    })

    if (edgesNeedingFix.length === 0) return

    // Stable key: only based on edge IDs that need fixing
    const fixKey = edgesNeedingFix.map((e) => e.id).join('|')
    if (joinEdgeFixAppliedRef.current === fixKey) return
    joinEdgeFixAppliedRef.current = fixKey

    const fixedEdges = storeEdges.map((edge) => {
      const targetNode = storeNodes.find((n) => n.id === edge.target)
      if (targetNode?.data?.type === 'join' && !edge.targetHandle) {
        const otherEdgesToJoin = storeEdges.filter(
          (e) => e.target === edge.target && e.id !== edge.id
        )
        const leftEdgeExists = otherEdgesToJoin.some((e) => e.targetHandle === 'left')
        return { ...edge, targetHandle: leftEdgeExists ? 'right' : 'left' }
      }
      return edge
    })

    setEdges(fixedEdges)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [storeEdges, storeNodes])

  // Handle edge insertion events
  useEffect(() => {
    const handleEdgeInsert = (event: CustomEvent) => {
      console.log('[EDGE INSERT] Event received:', event.detail)
      const { edgeId, sourceNodeId, targetNodeId } = event.detail

      if (!edgeId || !sourceNodeId || !targetNodeId) {
        console.error('[EDGE INSERT] Missing required fields:', { edgeId, sourceNodeId, targetNodeId })
        toast({
          title: 'Invalid edge data',
          description: 'Missing required edge information.',
          status: 'error',
          duration: 3000,
          isClosable: true,
        })
        return
      }

      // Find the edge
      const edge = storeEdges.find((e) => e.id === edgeId)
      if (!edge) {
        console.error('[EDGE INSERT] Edge not found in edges array:', edgeId, 'Available edges:', storeEdges.map(e => e.id))
        toast({
          title: 'Edge not found',
          description: 'The edge you clicked was not found.',
          status: 'error',
          duration: 3000,
          isClosable: true,
        })
        return
      }

      console.log('[EDGE INSERT] Opening modal for edge:', { edgeId, sourceNodeId, targetNodeId })
      // Open node type selection modal
      setEdgeInsertModal({
        isOpen: true,
        edgeId,
        sourceNodeId,
        targetNodeId,
      })
    }

    console.log('[EDGE INSERT] Registering event listener')
    window.addEventListener('edge-insert', handleEdgeInsert as EventListener)
    return () => {
      console.log('[EDGE INSERT] Removing event listener')
      window.removeEventListener('edge-insert', handleEdgeInsert as EventListener)
    }
  }, [storeEdges, toast])

  // Handle edge destination addition (terminal edges only)
  useEffect(() => {
    const handleAddDestination = (event: Event) => {
      const customEvent = event as CustomEvent
      const { edgeId, sourceNodeId, targetNodeId } = customEvent.detail

      console.log('[EDGE DESTINATION] Add destination event:', { edgeId, sourceNodeId, targetNodeId })

      // Find the edge
      const edge = storeEdges.find((e) => e.id === edgeId)
      if (!edge) {
        toast({
          title: 'Edge not found',
          description: 'The selected edge could not be found.',
          status: 'error',
          duration: 3000,
          isClosable: true,
        })
        return
      }

      // Verify target has no outgoing edges (terminal edge)
      const targetHasOutgoing = storeEdges.some((e) => e.source === targetNodeId)
      if (targetHasOutgoing) {
        toast({
          title: 'Invalid operation',
          description: 'Destination can only be added at the end of a pipeline branch.',
          status: 'error',
          duration: 3000,
          isClosable: true,
        })
        return
      }

      // Check if target is already a destination
      const targetNode = storeNodes.find((n) => n.id === targetNodeId)
      if (targetNode?.data?.type === 'destination') {
        toast({
          title: 'Already a destination',
          description: 'The target node is already a destination.',
          status: 'info',
          duration: 3000,
          isClosable: true,
        })
        return
      }

      // Open destination selector modal
      setDestinationSelectorModal({
        isOpen: true,
        sourceNodeId,
        targetNodeId,
        edgeId,
      })
    }

    console.log('[EDGE DESTINATION] Registering event listener')
    window.addEventListener('edge-add-destination', handleAddDestination as EventListener)
    return () => {
      console.log('[EDGE DESTINATION] Removing event listener')
      window.removeEventListener('edge-add-destination', handleAddDestination as EventListener)
    }
  }, [storeEdges, storeNodes, toast])

  // Handle destination selection and creation
  const handleDestinationSelect = useCallback(async (destinationId: number) => {
    if (!destinationSelectorModal) return

    const { sourceNodeId, targetNodeId } = destinationSelectorModal

    console.log('[EDGE DESTINATION] Creating destination node:', { destinationId, sourceNodeId, targetNodeId })

    try {
      // Get project_id from URL if available
      const urlParams = new URLSearchParams(window.location.search)
      const projectIdParam = urlParams.get('projectId')
      const projectId = projectIdParam ? parseInt(projectIdParam, 10) : null

      // Get destination details first to determine the type
      // Pass project_id if available to filter destinations
      const destResponse = projectId && !isNaN(projectId)
        ? await connectionApi.getDestinations({ project_id: projectId } as Record<string, unknown>)
        : await connectionApi.getDestinations()

      console.log('[EDGE DESTINATION] Full API response:', destResponse)
      console.log('[EDGE DESTINATION] Project ID used:', projectId)

      // Handle different response shapes:
      // - Array of destinations
      // - { destinations: [...] }
      // - Axios-style { data: { destinations: [...] } } (already unwrapped by react-query in some places)
      let destList: any[] = []
      const rawData = destResponse?.data || destResponse

      if (Array.isArray(rawData)) {
        destList = rawData
      } else if (Array.isArray(rawData?.destinations)) {
        destList = rawData.destinations
      } else if (Array.isArray(rawData?.data?.destinations)) {
        destList = rawData.data.destinations
      }

      console.log('[EDGE DESTINATION] Extracted destination list:', destList)
      console.log('[EDGE DESTINATION] Looking for destination ID:', destinationId)

      const destination = destList.find(
        (d: any) => d.id === destinationId || d.destination_id === destinationId
      )

      console.log('[EDGE DESTINATION] Found destination:', destination)

      if (!destination) {
        console.error('[EDGE DESTINATION] Destination not found. Available IDs:', destList.map((d: any) => d.id || d.destination_id))
        toast({
          title: 'Error',
          description: `Selected destination (ID: ${destinationId}) not found. Available destinations: ${destList.length}`,
          status: 'error',
          duration: 5000,
          isClosable: true,
        })
        return
      }

      const destinationName =
        destination.destination_name ||
        destination.name ||
        `Destination ${destination.id || destination.destination_id || ''}`

      const dbType = (destination.db_type || destination.database_type || destination.mode || 'hana')
        .toString()
        .toLowerCase()

      console.log('[EDGE DESTINATION] Destination details:', {
        id: destination.id || destination.destination_id,
        name: destinationName,
        dbType,
        fullDestination: destination
      })

      // Map db_type to node type ID
      const destinationNodeTypeMap: Record<string, string> = {
        hana: 'destination-hana',
        'sap hana': 'destination-hana',
        postgresql: 'destination-postgresql',
        postgres: 'destination-postgresql',
      }

      const destinationNodeType = destinationNodeTypeMap[dbType] || 'destination-hana'

      // Get node type definition for destination
      const nodeTypeDef = getNodeTypeDefinition(destinationNodeType)
      if (!nodeTypeDef) {
        toast({
          title: 'Error',
          description: `Destination node type "${destinationNodeType}" is not recognized.`,
          status: 'error',
          duration: 3000,
          isClosable: true,
        })
        return
      }

      // Get target node (we'll replace it with destination)
      // If sourceNodeId is empty, this is from node context menu (replace current node)
      // If sourceNodeId exists, this is from edge (replace target node)
      // Use latest store state to avoid stale closure
      const targetNode = storeNodes.find((n) => n.id === targetNodeId)
      if (!targetNode) {
        toast({
          title: 'Error',
          description: 'Target node not found.',
          status: 'error',
          duration: 3000,
          isClosable: true,
        })
        return
      }

      // If adding from node context (no sourceNodeId), verify node has no outgoing edges
      if (!sourceNodeId) {
        const hasOutgoing = storeEdges && Array.isArray(storeEdges)
          ? storeEdges.some((e: Edge) => e.source === targetNodeId)
          : false

        if (hasOutgoing) {
          toast({
            title: 'Invalid operation',
            description: 'Destination can only be added at the end of a pipeline branch.',
            status: 'error',
            duration: 3000,
            isClosable: true,
          })
          return
        }
      }

      // Generate new destination node ID
      const generateUUID = () => {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
          const r = Math.random() * 16 | 0
          const v = c === 'x' ? r : (r & 0x3 | 0x8)
          return v.toString(16)
        })
      }
      const destinationNodeId = generateUUID()

      console.log('[EDGE DESTINATION] Creating destination node:', {
        destinationNodeId,
        targetNodeId,
        sourceNodeId,
        destinationNodeType,
        destinationName
      })

      // Create destination node config
      const destinationConfig = {
        ...nodeTypeDef.defaultConfig,
        destinationId,
        connectionType: dbType,
      }

      const isFromNodeContext = !sourceNodeId

      let updatedNodes: Node[]
      let updatedEdges: Edge[]

      if (isFromNodeContext) {
        // Add a NEW destination node after the selected node (keep existing node; connect selected -> destination)
        const newDestinationNode: Node = {
          id: destinationNodeId,
          type: destinationNodeType,
          position: {
            x: (targetNode.position?.x ?? 0) + 280,
            y: targetNode.position?.y ?? 0,
          },
          data: {
            type: 'destination',
            node_type: 'destination',
            label: destinationName,
            business_name: destinationName,
            technical_name: `destination_${destinationNodeId.substring(0, 8)}`,
            config: destinationConfig,
          },
        }
        updatedNodes = [...storeNodes, newDestinationNode]
        updatedEdges = [
          ...storeEdges,
          {
            id: `${targetNodeId}-${destinationNodeId}`,
            source: targetNodeId,
            target: destinationNodeId,
          } as Edge,
        ]
      } else {
        // From edge context: replace the target node with destination
        updatedNodes = storeNodes.map((n) => {
          if (n.id === targetNodeId) {
            return {
              ...n,
              id: destinationNodeId,
              type: destinationNodeType,
              data: {
                ...n.data,
                type: 'destination',
                node_type: 'destination',
                label: destinationName,
                business_name: destinationName,
                technical_name: `destination_${destinationNodeId.substring(0, 8)}`,
                config: destinationConfig,
              },
            } as Node
          }
          return n
        })
        updatedEdges = storeEdges.map((e) => {
          if (e.target === targetNodeId && e.source === sourceNodeId) {
            return { ...e, target: destinationNodeId }
          }
          return e
        })
      }

      console.log('[EDGE DESTINATION] Updated nodes count:', updatedNodes.length)
      console.log('[EDGE DESTINATION] Updated edges count:', updatedEdges.length)

      // Update store only (ReactFlow re-renders from store)
      // Backend save happens on explicit 'Save Pipeline' click
      setNodes(updatedNodes)
      setEdges(updatedEdges)

      // Select the new destination node to show config panel
      const newDestinationNode = updatedNodes.find((n) => n.id === destinationNodeId)
      if (newDestinationNode) {
        setSelectedNode(newDestinationNode)
        setRightPanelCollapsed(false)
      } else {
        console.warn('[EDGE DESTINATION] New destination node not found after update')
      }

      toast({
        title: 'Destination added',
        description: `Destination "${destinationName}" has been added. Save the pipeline to persist.`,
        status: 'success',
        duration: 3000,
        isClosable: true,
      })

      setDestinationSelectorModal(null)
    } catch (error: any) {
      console.error('[EDGE DESTINATION] Error creating destination node:', error)
      let desc: unknown = error?.response?.data?.error ?? error?.response?.data?.detail ?? error?.message ?? 'Failed to create destination node.'
      if (typeof desc !== 'string') desc = JSON.stringify(desc)
      toast({
        title: 'Error',
        description: desc as string,
        status: 'error',
        duration: 5000,
        isClosable: true,
      })
    }
  }, [destinationSelectorModal, canvasId, storeNodes, storeEdges, toast, setNodes, setEdges, setSelectedNode, setRightPanelCollapsed])

  // Handle "Customer Database" selection - write to customer DB (e.g. C00008) with schema + table
  const handleCustomerDatabaseSelect = useCallback(() => {
    if (!destinationSelectorModal) return

    const { sourceNodeId, targetNodeId } = destinationSelectorModal

    const targetNode = storeNodes.find((n) => n.id === targetNodeId)
    if (!targetNode) {
      toast({
        title: 'Error',
        description: 'Target node not found.',
        status: 'error',
        duration: 3000,
        isClosable: true,
      })
      return
    }

    if (!sourceNodeId) {
      const hasOutgoing = storeEdges && Array.isArray(storeEdges)
        ? storeEdges.some((e: Edge) => e.source === targetNodeId)
        : false
      if (hasOutgoing) {
        toast({
          title: 'Invalid operation',
          description: 'Destination can only be added at the end of a pipeline branch.',
          status: 'error',
          duration: 3000,
          isClosable: true,
        })
        return
      }
    }

    const generateUUID = () =>
      'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
        const r = (Math.random() * 16) | 0
        const v = c === 'x' ? r : (r & 0x3) | 0x8
        return v.toString(16)
      })
    const destinationNodeId = generateUUID()

    const nodeTypeDef = getNodeTypeDefinition('destination-postgresql')
    if (!nodeTypeDef) {
      toast({
        title: 'Error',
        description: 'Destination node type is not recognized.',
        status: 'error',
        duration: 3000,
        isClosable: true,
      })
      return
    }

    const destinationConfig = {
      ...nodeTypeDef.defaultConfig,
      destinationType: 'customer_database',
      destinationId: null,
      schema: 'public',
      tableName: 'output_table',
      loadMode: 'insert',
    }

    const isFromNodeContext = !sourceNodeId
    const destinationName = 'Customer Database'

    let updatedNodes: Node[]
    let updatedEdges: Edge[]

    if (isFromNodeContext) {
      const newDestinationNode: Node = {
        id: destinationNodeId,
        type: 'destination-postgresql',
        position: {
          x: (targetNode.position?.x ?? 0) + 280,
          y: targetNode.position?.y ?? 0,
        },
        data: {
          type: 'destination',
          node_type: 'destination',
          label: destinationName,
          business_name: destinationName,
          technical_name: `destination_${destinationNodeId.substring(0, 8)}`,
          config: destinationConfig,
        },
      }
      updatedNodes = [...storeNodes, newDestinationNode]
      updatedEdges = [
        ...storeEdges,
        {
          id: `${targetNodeId}-${destinationNodeId}`,
          source: targetNodeId,
          target: destinationNodeId,
        } as Edge,
      ]
    } else {
      updatedNodes = storeNodes.map((n) => {
        if (n.id === targetNodeId) {
          return {
            ...n,
            id: destinationNodeId,
            type: 'destination-postgresql',
            data: {
              ...n.data,
              type: 'destination',
              node_type: 'destination',
              label: destinationName,
              business_name: destinationName,
              technical_name: `destination_${destinationNodeId.substring(0, 8)}`,
              config: destinationConfig,
            },
          } as Node
        }
        return n
      })
      updatedEdges = storeEdges.map((e) => {
        if (e.target === targetNodeId && e.source === sourceNodeId) {
          return { ...e, target: destinationNodeId }
        }
        return e
      })
    }

    // Update store (ReactFlow re-renders from store)
    setNodes(updatedNodes)
    setEdges(updatedEdges)

    toast({
      title: 'Destination added',
      description: 'Customer Database destination has been added. Save the pipeline to persist.',
      status: 'success',
      duration: 3000,
      isClosable: true,
    })

    setDestinationSelectorModal(null)
  }, [destinationSelectorModal, canvasId, storeNodes, storeEdges, toast, setNodes, setEdges, setSelectedNode, setRightPanelCollapsed])

  // Handle edge context menu node insertion (frontend-only; persist via Save Pipeline)
  const handleEdgeInsertNode = useCallback((nodeType: string) => {
    if (!edgeContextMenu) return

    const { edge } = edgeContextMenu
    const sourceNodeId = edge.source
    const targetNodeId = edge.target

    setEdgeContextMenu(null)

    const nodeTypeDef = getNodeTypeDefinition(nodeType)
    if (!nodeTypeDef) {
      toast({
        title: 'Invalid node type',
        description: `Node type "${nodeType}" is not recognized.`,
        status: 'error',
        duration: 3000,
        isClosable: true,
      })
      return
    }

    const sourceNode = storeNodes.find((n) => n.id === sourceNodeId)
    const targetNode = storeNodes.find((n) => n.id === targetNodeId)
    let position = { x: 0, y: 0 }
    if (sourceNode && targetNode) {
      position = {
        x: (sourceNode.position.x + targetNode.position.x) / 2,
        y: (sourceNode.position.y + targetNode.position.y) / 2,
      }
    }

    const newNodeId = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
      const r = Math.random() * 16 | 0
      const v = c === 'x' ? r : (r & 0x3 | 0x8)
      return v.toString(16)
    })
    const shortId = newNodeId.substring(0, 8)

    const newNode: Node = {
      id: newNodeId,
      type: nodeType,
      position,
      data: {
        type: nodeType,
        id: newNodeId,
        node_id: newNodeId,
        business_name: `New ${nodeType}`,
        technical_name: `${nodeType}_${shortId}`,
        label: `New ${nodeType}`,
        config: { ...nodeTypeDef.defaultConfig },
        input_nodes: [sourceNodeId],
        input_metadata: null,
        output_metadata: null,
        schema_version: 0,
        schema_outdated: false,
      },
    }

    const currentEdges = useCanvasStore.getState().edges
    const oldEdge = currentEdges.find((e) => e.source === sourceNodeId && e.target === targetNodeId)
    const newEdges = currentEdges
      .filter((e) => !(e.source === sourceNodeId && e.target === targetNodeId))
      .concat(
        {
          id: `${sourceNodeId}-${newNodeId}`,
          source: sourceNodeId,
          target: newNodeId,
          sourceHandle: (oldEdge?.sourceHandle as string) || 'output',
          targetHandle: 'input',
          type: 'smoothstep',
        } as Edge,
        {
          id: `${newNodeId}-${targetNodeId}`,
          source: newNodeId,
          target: targetNodeId,
          sourceHandle: 'output',
          targetHandle: (oldEdge?.targetHandle as string) || 'input',
          type: 'smoothstep',
        } as Edge
      )

    addNode(newNode)
    setEdges(newEdges)
    updateNodeStatus(newNodeId, 'success')
    setIsDirty(true)
    toast({
      title: 'Node inserted',
      description: 'Save the pipeline to persist this change.',
      status: 'success',
      duration: 3000,
      isClosable: true,
    })
  }, [edgeContextMenu, storeNodes, toast, addNode, setEdges, updateNodeStatus, setIsDirty])

  // Handle node type selection and insertion (from modal); frontend-only, persist via Save Pipeline
  const handleNodeTypeSelected = useCallback((nodeType: string) => {
    if (!edgeInsertModal) return

    const { sourceNodeId, targetNodeId } = edgeInsertModal
    setEdgeInsertModal(null)

    const currentNodes = storeNodes || []
    const nodeTypeDef = getNodeTypeDefinition(nodeType)
    if (!nodeTypeDef) {
      toast({
        title: 'Invalid node type',
        description: `Node type "${nodeType}" is not recognized.`,
        status: 'error',
        duration: 3000,
        isClosable: true,
      })
      return
    }

    const sourceNode = currentNodes.find((n) => n.id === sourceNodeId)
    const targetNode = currentNodes.find((n) => n.id === targetNodeId)
    let position = { x: 0, y: 0 }
    if (sourceNode && targetNode) {
      position = {
        x: (sourceNode.position.x + targetNode.position.x) / 2,
        y: (sourceNode.position.y + targetNode.position.y) / 2,
      }
    }

    const newNodeId = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
      const r = Math.random() * 16 | 0
      const v = c === 'x' ? r : (r & 0x3 | 0x8)
      return v.toString(16)
    })
    const shortId = newNodeId.substring(0, 8)

    const newNode: Node = {
      id: newNodeId,
      type: nodeType,
      position,
      data: {
        type: nodeType,
        id: newNodeId,
        node_id: newNodeId,
        business_name: `New ${nodeType}`,
        technical_name: `${nodeType}_${shortId}`,
        label: `New ${nodeType}`,
        config: { ...nodeTypeDef.defaultConfig },
        input_nodes: [sourceNodeId],
        input_metadata: null,
        output_metadata: null,
        schema_version: 0,
        schema_outdated: false,
      },
    }

    const currentEdges = useCanvasStore.getState().edges
    const oldEdge = currentEdges.find((e) => e.source === sourceNodeId && e.target === targetNodeId)
    const newEdges = currentEdges
      .filter((e) => !(e.source === sourceNodeId && e.target === targetNodeId))
      .concat(
        {
          id: `${sourceNodeId}-${newNodeId}`,
          source: sourceNodeId,
          target: newNodeId,
          sourceHandle: (oldEdge?.sourceHandle as string) || 'output',
          targetHandle: 'input',
          type: 'smoothstep',
        } as Edge,
        {
          id: `${newNodeId}-${targetNodeId}`,
          source: newNodeId,
          target: targetNodeId,
          sourceHandle: 'output',
          targetHandle: (oldEdge?.targetHandle as string) || 'input',
          type: 'smoothstep',
        } as Edge
      )

    addNode(newNode)
    setEdges(newEdges)
    updateNodeStatus(newNodeId, 'success')
    setIsDirty(true)
    toast({
      title: 'Node inserted',
      description: 'Save the pipeline to persist this change.',
      status: 'success',
      duration: 3000,
      isClosable: true,
    })
  }, [edgeInsertModal, storeNodes, toast, addNode, setEdges, updateNodeStatus, setIsDirty])

  // Initialize canvas state
  useEffect(() => {
    if (canvasId) {
      setCanvas(canvasId, `Canvas ${canvasId}`)
    }
  }, [canvasId, setCanvas])

  // When canvasId changes, clear job state so we never show "triggered" or progress from another canvas.
  useEffect(() => {
    if (!canvasId) return
    setActiveJob(null)
    setExecutionStatus('idle')
    setJobDetail({ current_step: null, progress: 0, error_message: null, current_level: null, total_levels: null, level_status: null })
  }, [canvasId])

  // When canvas loads, restore only an already-running job for this canvas (e.g. after logout and return).
  // Do NOT restore 'pending' jobs: that would show a queued job that may then start and look like
  // "opening the canvas started execution" when the user did not click Execute.
  useEffect(() => {
    if (!canvasId || !storeNodes?.length || activeJobId) return
    let cancelled = false
    const restore = async () => {
      try {
        const res = await migrationApi.list()
        const jobs = (res ?? []) as Array<{
          id: number
          job_id: string
          canvas: number | { id: number }
          status: string
          progress: number
          current_step: string | null
          error_message: string | null
        }>
        const canvasIdNum = Number(canvasId)
        const jobCanvasId = (j: { canvas: number | { id: number } }) =>
          typeof j.canvas === 'object' && j.canvas !== null && 'id' in j.canvas ? j.canvas.id : j.canvas
        const running = jobs.find(
          (j) => (jobCanvasId(j) === canvasId || jobCanvasId(j) === canvasIdNum) && String(j.status).toLowerCase() === 'running'
        )
        if (cancelled || !running) return
        setActiveJob(running.job_id)
        setViewMode('monitor')
        setExecutionStatus((running.status as ExecutionStatusType) || 'running')
        setJobDetail({
          current_step: running.current_step ?? null,
          progress: running.progress ?? 0,
          error_message: running.error_message ?? null,
        })
        const data = await migrationApi.status(running.id) as { node_progress?: Array<{ node_id: string; status: string }>; current_level?: number; total_levels?: number; level_status?: string }
        if (data && !cancelled) {
          if (data.node_progress?.length) {
            const batch: Record<string, 'idle' | 'running' | 'success' | 'error'> = {}
            data.node_progress.forEach((np: { node_id: string; status: string }) => {
              const s = (np.status || '').toLowerCase()
              batch[np.node_id] = s === 'completed' ? 'success' : s === 'failed' ? 'error' : (s === 'running' ? 'running' : 'idle')
            })
            updateNodeStatusBatch(batch)
          }
          setJobDetail((prev) => ({
            ...prev,
            current_level: data.current_level ?? prev.current_level ?? null,
            total_levels: data.total_levels ?? prev.total_levels ?? null,
            level_status: data.level_status ?? prev.level_status ?? null,
          }))
        }
        wsService.subscribeToJobUpdates(running.job_id, {
          onStatus: (d) => {
            if (d?.current_step != null) setJobDetail((prev) => ({ ...prev, current_step: d.current_step ?? prev.current_step }))
            if (d?.progress != null) setJobDetail((prev) => ({ ...prev, progress: d.progress ?? prev.progress }))
            if (d?.current_level != null) setJobDetail((prev) => ({ ...prev, current_level: d.current_level }))
            if (d?.total_levels != null) setJobDetail((prev) => ({ ...prev, total_levels: d.total_levels }))
            if (d?.level_status != null) setJobDetail((prev) => ({ ...prev, level_status: d.level_status }))
            if ((d?.status as string)?.toLowerCase() === 'completed') {
              setExecutionStatus('completed')
              setJobDetail((prev) => ({ ...prev, progress: 100, level_status: 'complete' }))
              setAllNodesStatus('success')
              // Clear node statuses after 5 minutes so old runs don't keep showing green ticks
              if (clearStatusTimeoutRef.current) {
                clearTimeout(clearStatusTimeoutRef.current)
              }
              clearStatusTimeoutRef.current = setTimeout(() => {
                clearNodeStatuses()
                setExecutionStatus('idle')
                setActiveJob('')
              }, 5 * 60 * 1000)
              wsService.unsubscribeFromJobUpdates(running.job_id)
            }
          },
          onNodeProgress: (d) => {
            if (d?.node_id) {
              const s = d.status === 'completed' ? 'success' : d.status === 'failed' ? 'error' : (d.status as any) || 'running'
              updateNodeStatus(d.node_id, s)
              if (d.progress !== undefined) updateJobProgress(d.node_id, d.progress)
            }
            if (d?.current_level != null) setJobDetail((prev) => ({ ...prev, current_level: d.current_level }))
            if (d?.total_levels != null) setJobDetail((prev) => ({ ...prev, total_levels: d.total_levels }))
            if (d?.level_status != null) setJobDetail((prev) => ({ ...prev, level_status: d.level_status }))
          },
          onComplete: (d) => {
            if (d?.node_progress?.length) {
              const batch: Record<string, 'idle' | 'running' | 'success' | 'error'> = {}
              d.node_progress.forEach((np: { node_id: string; status: string }) => {
                const s = (np.status || '').toLowerCase()
                batch[np.node_id] = s === 'completed' ? 'success' : s === 'failed' ? 'error' : (s === 'running' ? 'running' : 'idle')
              })
              updateNodeStatusBatch(batch)
            } else {
              setAllNodesStatus('success')
            }
            setExecutionStatus('completed')
            setJobDetail((prev) => ({
              ...prev,
              progress: 100,
              current_step: 'Completed',
              error_message: null,
              level_status: 'complete',
              current_level: d?.current_level ?? prev.current_level ?? null,
              total_levels: d?.total_levels ?? prev.total_levels ?? null,
            }))
            // Clear node statuses after 5 minutes so old runs don't keep showing green ticks
            if (clearStatusTimeoutRef.current) {
              clearTimeout(clearStatusTimeoutRef.current)
            }
            clearStatusTimeoutRef.current = setTimeout(() => {
              clearNodeStatuses()
              setExecutionStatus('idle')
              setActiveJob('')
            }, 5 * 60 * 1000)
            wsService.unsubscribeFromJobUpdates(running.job_id)
          },
          onError: (d) => {
            setExecutionStatus('failed')
            setJobDetail((prev) => ({ ...prev, error_message: d?.error ?? 'Unknown error' }))
            wsService.unsubscribeFromJobUpdates(running.job_id)
          },
        })
      } catch (_) {
        // ignore
      }
    }
    restore()
    return () => { cancelled = true }
  }, [canvasId, storeNodes?.length, activeJobId, setActiveJob, setViewMode, updateNodeStatusBatch, updateJobProgress, setAllNodesStatus])

  // Poll job status only while job is running/pending; use 12s interval so WebSocket is primary (reduces Django load)
  const POLL_INTERVAL_MS = 12000
  useEffect(() => {
    if (!activeJobId) return
    if (executionStatus !== 'running' && executionStatus !== 'pending') return
    const poll = async () => {
      try {
        const res = await migrationApi.list()
        const jobs = (res ?? []) as Array<{
          id: number
          job_id: string
          status: string
          progress: number
          current_step: string | null
          error_message: string | null
        }>
        const job = jobs.find((j) => j.job_id === activeJobId)
        if (!job) {
          setActiveJob('')
          setExecutionStatus('failed')
          return
        }
        const status = job.status as ExecutionStatusType
        setExecutionStatus(status || 'running')
        setJobDetail((prev) => ({
          ...prev,
          current_step: job.current_step ?? prev.current_step ?? null,
          progress: Math.max(job.progress ?? 0, prev.progress ?? 0),
          error_message: job.error_message ?? prev.error_message ?? null,
        }))
        if (status === 'running' || status === 'pending') {
          try {
            const data = await migrationApi.status(job.id) as { node_progress?: Array<{ node_id: string; status: string }>; status?: string; current_level?: number; total_levels?: number; level_status?: string }
            if (data?.status && !['running', 'pending'].includes((data.status as string).toLowerCase())) {
              setExecutionStatus((data.status as string) as ExecutionStatusType)
              if ((data.status as string).toLowerCase() === 'completed') {
                setJobDetail((prev) => ({ ...prev, progress: 100, level_status: 'complete' }))
              }
            }
            if (data?.node_progress?.length) {
              const batch: Record<string, 'idle' | 'running' | 'success' | 'error'> = {}
              data.node_progress.forEach((np: { node_id: string; status: string }) => {
                const s = (np.status || '').toLowerCase()
                batch[np.node_id] = s === 'completed' ? 'success' : s === 'failed' ? 'error' : (s === 'running' ? 'running' : 'idle')
              })
              updateNodeStatusBatch(batch)
            }
            if (data && (data.current_level != null || data.total_levels != null || data.level_status != null)) {
              setJobDetail((prev) => ({
                ...prev,
                current_level: data.current_level ?? prev.current_level ?? null,
                total_levels: data.total_levels ?? prev.total_levels ?? null,
                level_status: data.level_status ?? prev.level_status ?? null,
              }))
            }
          } catch {
            // e.g. 404 from Django; stop polling by clearing active job
            setActiveJob('')
            setExecutionStatus('failed')
          }
        } else if (status === 'completed') {
          try {
            const data = await migrationApi.status(job.id) as { node_progress?: Array<{ node_id: string; status: string }>; current_level?: number; total_levels?: number; level_status?: string }
            if (data?.node_progress?.length) {
              const batch: Record<string, 'idle' | 'running' | 'success' | 'error'> = {}
              data.node_progress.forEach((np: { node_id: string; status: string }) => {
                const s = (np.status || '').toLowerCase()
                batch[np.node_id] = s === 'completed' ? 'success' : s === 'failed' ? 'error' : (s === 'running' ? 'running' : 'idle')
              })
              updateNodeStatusBatch(batch)
            } else {
              setAllNodesStatus('success')
            }
            setJobDetail((prev) => ({
              ...prev,
              progress: 100,
              current_level: data?.current_level ?? prev.current_level ?? null,
              total_levels: data?.total_levels ?? prev.total_levels ?? null,
              level_status: 'complete',
            }))
            // Clear node statuses after 5 minutes so old runs don't keep showing green ticks
            if (clearStatusTimeoutRef.current) {
              clearTimeout(clearStatusTimeoutRef.current)
            }
            clearStatusTimeoutRef.current = setTimeout(() => {
              clearNodeStatuses()
              setExecutionStatus('idle')
              setActiveJob('')
            }, 5 * 60 * 1000)
          } catch {
            setAllNodesStatus('success')
            setJobDetail((prev) => ({ ...prev, progress: 100, level_status: 'complete' }))
          }
        } else if (status === 'failed') {
          try {
            const data = await migrationApi.status(job.id) as { node_progress?: Array<{ node_id: string; status: string }>; current_level?: number; total_levels?: number; level_status?: string }
            if (data?.node_progress?.length) {
              const batch: Record<string, 'idle' | 'running' | 'success' | 'error'> = {}
              data.node_progress.forEach((np: { node_id: string; status: string }) => {
                const s = (np.status || '').toLowerCase()
                batch[np.node_id] = s === 'completed' ? 'success' : s === 'failed' ? 'error' : (s === 'running' ? 'running' : 'idle')
              })
              updateNodeStatusBatch(batch)
            }
          } catch {
            // ignore
          }
        }
      } catch (_) {
        setActiveJob('')
        setExecutionStatus('failed')
      }
    }
    poll()
    const interval = setInterval(poll, POLL_INTERVAL_MS)
    return () => clearInterval(interval)
  }, [activeJobId, executionStatus, updateNodeStatusBatch, setActiveJob, setAllNodesStatus])

  // Connection validator function
  const isValidConnection = useCallback(
    (connection: Connection) => {
      const sourceNode = storeNodes.find((n) => n.id === connection.source) ?? null
      const targetNode = storeNodes.find((n) => n.id === connection.target) ?? null

      if (!sourceNode || !targetNode) {
        return false
      }

      // Normalize types (backend/canvas may store "Projection", "Join", etc.)
      const rawSourceType = sourceNode.data.type && String(sourceNode.data.type)
      const rawTargetType = targetNode.data.type && String(targetNode.data.type)
      const sourceType = rawSourceType.toLowerCase()
      const targetType = rawTargetType.toLowerCase()

      // CRITICAL: Destinations are terminal - cannot have outgoing edges
      if (sourceType === 'destination') {
        return false
      }

      // Validate node type compatibility
      // Source nodes can connect to any transform node
      // Transform nodes can connect to other transform nodes or destinations
      const validConnections = [
        ['source', 'filter'],
        ['source', 'join'],
        ['source', 'projection'],
        ['source', 'calculated'],
        ['source', 'aggregate'],
        ['source', 'compute'],
        ['filter', 'filter'],
        ['filter', 'join'],
        ['filter', 'projection'],
        ['filter', 'calculated'],
        ['filter', 'aggregate'],
        ['filter', 'compute'],
        ['join', 'filter'],
        ['join', 'projection'],
        ['join', 'calculated'],
        ['join', 'aggregate'],
        ['join', 'compute'],
        ['projection', 'filter'],
        ['projection', 'join'],
        ['projection', 'calculated'],
        ['projection', 'aggregate'],
        ['projection', 'compute'],
        ['calculated', 'filter'],
        ['calculated', 'join'],
        ['calculated', 'projection'],
        ['calculated', 'aggregate'],
        ['calculated', 'compute'],
        ['aggregate', 'filter'],
        ['aggregate', 'join'],
        ['aggregate', 'projection'],
        ['aggregate', 'calculated'],
        ['aggregate', 'compute'],
      ]

      const isTypeCompatible = validConnections.some(
        ([src, tgt]) => src === sourceType && tgt === targetType
      )

      if (!isTypeCompatible) {
        return false
      }

      // CRITICAL FIX: For Join nodes, validate handle-specific connections
      // Join nodes can have TWO inputs (left and right), so we need to check
      // if the specific handle is already occupied, not just if the node has any connection
      if (targetType === 'join') {
        const targetHandle = connection.targetHandle

        // Check existing edges to this join node
        const existingEdgesToJoin = storeEdges.filter((e) => e.target === connection.target)

        // CRITICAL: Prevent connecting the same SOURCE NODE ID to both left and right inputs
        // Validation MUST use node_id comparison ONLY, NOT table names, labels, or lineage
        // Rule: Reject ONLY if left_node_id == right_node_id (same node ID on both handles)
        // Allow joins between different nodes even if they have same table name, schema, etc.
        const leftEdge = existingEdgesToJoin.find((e) => e.targetHandle === 'left')
        const rightEdge = existingEdgesToJoin.find((e) => e.targetHandle === 'right')
        const sourceNodeId = connection.source

        // Log for debugging
        console.log(`[Join Validation] isValidConnection: source=${sourceNodeId}, targetHandle=${targetHandle}`)
        console.log(`[Join Validation] Existing: left=${leftEdge?.source || 'none'}, right=${rightEdge?.source || 'none'}`)

        // Check if the same SOURCE NODE ID is already connected to the OTHER handle
        // Compare node IDs ONLY - ignore all other properties
        if (targetHandle === 'left') {
          // Connecting to left handle - check if same node ID is already on right handle
          if (rightEdge && rightEdge.source === sourceNodeId) {
            console.warn(`[Join Validation] isValidConnection: REJECTED - same node ID '${sourceNodeId}' already on right handle`)
            return false
          }
          // Different node ID or right handle is free - allow
          return true
        } else if (targetHandle === 'right') {
          // Connecting to right handle - check if same node ID is already on left handle
          if (leftEdge && leftEdge.source === sourceNodeId) {
            console.warn(`[Join Validation] isValidConnection: REJECTED - same node ID '${sourceNodeId}' already on left handle`)
            return false
          }
          // Different node ID or left handle is free - allow
          return true
        } else {
          // No targetHandle specified - will be auto-assigned
          // Check if source is already connected to either handle
          if (leftEdge && leftEdge.source === sourceNodeId) {
            // Source already connected to left - can only connect to right if right is free
            if (rightEdge) {
              console.warn(`[Join Validation] isValidConnection: REJECTED - same node ID '${sourceNodeId}' on left, right occupied`)
              return false
            }
            // Right handle is free - allow (will be assigned to right)
            console.log(`[Join Validation] isValidConnection: ALLOWED - node ${sourceNodeId} on left, will assign to right`)
            return true
          }
          if (rightEdge && rightEdge.source === sourceNodeId) {
            // Source already connected to right - can only connect to left if left is free
            if (leftEdge) {
              console.warn(`[Join Validation] isValidConnection: REJECTED - same node ID '${sourceNodeId}' on right, left occupied`)
              return false
            }
            // Left handle is free - allow (will be assigned to left)
            console.log(`[Join Validation] isValidConnection: ALLOWED - node ${sourceNodeId} on right, will assign to left`)
            return true
          }
          // Source not connected to either handle - allow connection
          console.log(`[Join Validation] isValidConnection: ALLOWED - node ${sourceNodeId} not connected to either handle`)
          return true
        }

        // Check if this specific handle is already connected
        const handleAlreadyConnected = existingEdgesToJoin.some(
          (e) => e.targetHandle === targetHandle
        )

        // Reject if this specific handle is already occupied
        if (handleAlreadyConnected) {
          console.warn(`Join node handle '${targetHandle}' is already connected`)
          return false
        }

        // Validate handle ID is valid for Join nodes
        if (targetHandle !== 'left' && targetHandle !== 'right') {
          console.warn(`Invalid handle ID '${targetHandle}' for Join node. Must be 'left' or 'right'`)
          return false
        }

        return true
      }

      // For non-join nodes, check if there's already a connection between these nodes
      // (most nodes only allow one input)
      if (targetType !== 'join') {
        const existingEdge = storeEdges.find(
          (e) => e.source === connection.source && e.target === connection.target
        ) ?? null

        if (existingEdge) {
          return false
        }
      }

      return true
    },
    [storeNodes, storeEdges]
  )

  const onConnect = useCallback(
    (params: Connection) => {
      // Validate connection compatibility
      const sourceNode = storeNodes.find((n) => n.id === params.source) ?? null
      const targetNode = storeNodes.find((n) => n.id === params.target) ?? null

      if (!sourceNode || !targetNode) {
        console.warn('Invalid connection: source or target node not found')
        return
      }

      // Normalize types (backend/canvas may store "Projection", "Join", etc.)
      const rawSourceType = sourceNode.data.type && String(sourceNode.data.type)
      const rawTargetType = targetNode.data.type && String(targetNode.data.type)
      const sourceType = rawSourceType.toLowerCase()
      const targetType = rawTargetType.toLowerCase()

      // Validate node type compatibility
      // Source nodes can connect to any transform node
      // Transform nodes can connect to other transform nodes or destinations
      const validConnections = [
        ['source', 'filter'],
        ['source', 'join'],
        ['source', 'projection'],
        ['source', 'calculated'],
        ['source', 'aggregate'],
        ['source', 'compute'],
        ['filter', 'filter'],
        ['filter', 'join'],
        ['filter', 'projection'],
        ['filter', 'calculated'],
        ['filter', 'aggregate'],
        ['filter', 'compute'],
        ['join', 'filter'],
        ['join', 'projection'],
        ['join', 'calculated'],
        ['join', 'aggregate'],
        ['join', 'compute'],
        ['projection', 'filter'],
        ['projection', 'join'],
        ['projection', 'calculated'],
        ['projection', 'aggregate'],
        ['projection', 'compute'],
        ['calculated', 'filter'],
        ['calculated', 'join'],
        ['calculated', 'projection'],
        ['calculated', 'aggregate'],
        ['calculated', 'compute'],
        ['aggregate', 'filter'],
        ['aggregate', 'join'],
        ['aggregate', 'projection'],
        ['aggregate', 'calculated'],
        ['aggregate', 'compute'],
      ]

      const isValid = validConnections.some(
        ([src, tgt]) => src === sourceType && tgt === targetType
      )

      if (!isValid) {
        console.warn(`Invalid connection: ${sourceType} cannot connect to ${targetType}`)
        // Make the compute-boundary constraint explicit (otherwise this feels "broken" to users)
        if (sourceType === 'compute') {
          toast({
            title: 'Compute is an execution boundary',
            description:
              'You cannot use a Compute node output as input to Join/Projection/Filter/etc. Move the Join/Projection before Compute, or remove Compute.',
            status: 'warning',
            duration: 6000,
            isClosable: true,
          })
        } else if (targetType === 'compute') {
          toast({
            title: 'Connecting into Compute',
            description:
              'Compute nodes run Python on the upstream dataset. This connection must come from a SQL/transform node output.',
            status: 'info',
            duration: 5000,
            isClosable: true,
          })
        }
        return // Don't create invalid connections
      }

      // CRITICAL: For Join nodes, prevent connecting the same SOURCE NODE ID to both left and right inputs
      // Validation MUST use node_id comparison ONLY, NOT table names, table lineage, or any other property
      // Rule: if left_node_id == right_node_id, reject. Otherwise, allow.
      if (targetType === 'join') {
        const existingEdgesToJoin = storeEdges.filter((e) => e.target === params.target)

        const leftEdge = existingEdgesToJoin.find((e) => e.targetHandle === 'left')
        const rightEdge = existingEdgesToJoin.find((e) => e.targetHandle === 'right')

        // CRITICAL: Only check node_id equality, nothing else
        // Rule: Reject ONLY if left_node_id == right_node_id (same node ID on both handles)
        // Allow joins between different nodes even if they have same table name, schema, etc.
        const targetHandle = params.targetHandle
        const sourceNodeId = params.source

        // Log for debugging
        console.log(`[Join Validation] Checking connection: source=${sourceNodeId}, targetHandle=${targetHandle}`)
        console.log(`[Join Validation] Existing edges: left=${leftEdge?.source || 'none'}, right=${rightEdge?.source || 'none'}`)

        // Check if the same SOURCE NODE ID is already connected to the OTHER handle
        // This is the ONLY validation - compare node IDs only, ignore everything else
        if (targetHandle === 'left') {
          // Explicitly connecting to left handle - check if same node ID is on right handle
          if (rightEdge && rightEdge.source === sourceNodeId) {
            console.warn(`[Join Validation] REJECTED: Same node ID '${sourceNodeId}' already connected to right handle`)
            toast({
              title: 'Invalid connection',
              description: `Cannot connect node ${sourceNodeId} to left input: it's already connected to the right input. Use a different node.`,
              status: 'error',
              duration: 5000,
              isClosable: true,
            })
            return
          }
        } else if (targetHandle === 'right') {
          // Explicitly connecting to right handle - check if same node ID is on left handle
          if (leftEdge && leftEdge.source === sourceNodeId) {
            console.warn(`[Join Validation] REJECTED: Same node ID '${sourceNodeId}' already connected to left handle`)
            toast({
              title: 'Invalid connection',
              description: `Cannot connect node ${sourceNodeId} to right input: it's already connected to the left input. Use a different node.`,
              status: 'error',
              duration: 5000,
              isClosable: true,
            })
            return
          }
        } else {
          // No targetHandle specified - will be auto-assigned
          // Check if source is already connected to either handle
          if (leftEdge && leftEdge.source === sourceNodeId) {
            // Source already on left - only allow if right handle is free
            if (rightEdge) {
              console.warn(`[Join Validation] REJECTED: Same node ID '${sourceNodeId}' already on left, right handle occupied`)
              toast({
                title: 'Invalid connection',
                description: `Node ${sourceNodeId} is already connected to the left input. Right input is already occupied.`,
                status: 'error',
                duration: 5000,
                isClosable: true,
              })
              return
            }
            // Right handle is free - allow (will be assigned to right)
            console.log(`[Join Validation] ALLOWED: Node ${sourceNodeId} on left, will assign to right`)
          } else if (rightEdge && rightEdge.source === sourceNodeId) {
            // Source already on right - only allow if left handle is free
            if (leftEdge) {
              console.warn(`[Join Validation] REJECTED: Same node ID '${sourceNodeId}' already on right, left handle occupied`)
              toast({
                title: 'Invalid connection',
                description: `Node ${sourceNodeId} is already connected to the right input. Left input is already occupied.`,
                status: 'error',
                duration: 5000,
                isClosable: true,
              })
              return
            }
            // Left handle is free - allow (will be assigned to left)
            console.log(`[Join Validation] ALLOWED: Node ${sourceNodeId} on right, will assign to left`)
          } else {
            // Source not connected to either handle - allow connection
            console.log(`[Join Validation] ALLOWED: Node ${sourceNodeId} not connected to either handle`)
          }
        }
      }

      // Check for existing connection (same source and target)
      // CRITICAL: For Join nodes, allow multiple sources, but prevent duplicate connections
      // from the same source to the same target
      const existingEdge = storeEdges.find(
        (e) => e.source === params.source && e.target === params.target
      ) ?? null

      if (existingEdge) {
        console.warn('Connection already exists between these nodes')
        return
      }

      // For Join nodes, also check if we're trying to connect to an already-occupied handle
      if (targetType === 'join' && params.targetHandle) {
        const existingEdgeOnHandle = storeEdges.find(
          (e) => e.target === params.target && e.targetHandle === params.targetHandle
        ) ?? null

        if (existingEdgeOnHandle) {
          console.warn(`Join node handle '${params.targetHandle}' is already connected`)
          return
        }
      }

      // For join nodes, validate and set targetHandle based on existing connections
      let targetHandle = params.targetHandle
      if (targetType === 'join') {
        const existingEdgesToJoin = storeEdges.filter((e) => e.target === params.target)
        const leftEdge = existingEdgesToJoin.find((e) => e.targetHandle === 'left')
        const rightEdge = existingEdgesToJoin.find((e) => e.targetHandle === 'right')

        // If targetHandle is explicitly provided, validate it's not already used
        if (targetHandle) {
          if (targetHandle === 'left' && leftEdge) {
            console.warn('Left handle already has a connection. Please use the right handle.')
            return
          }
          if (targetHandle === 'right' && rightEdge) {
            console.warn('Right handle already has a connection. Please use the left handle.')
            return
          }
        } else {
          // Auto-assign handle if not explicitly provided
          if (!leftEdge) {
            // First connection - set as left
            targetHandle = 'left'
          } else if (!rightEdge) {
            // Second connection - set as right
            targetHandle = 'right'
          } else {
            // Both handles already have connections
            console.warn('Join node already has both left and right connections. Cannot add more connections.')
            return
          }
        }
      }

      // Create the connection with proper targetHandle
      const connectionParams: Connection = {
        ...params,
        targetHandle: targetHandle || params.targetHandle,
      }

      // CRITICAL: Ensure targetHandle is set for Join nodes before creating edge
      if (targetType === 'join' && !connectionParams.targetHandle) {
        console.error('Join node connection missing targetHandle - this should not happen')
        return
      }

      // Create the new edge using ReactFlow's addEdge utility
      const newEdgesList = addEdge(connectionParams, storeEdges)
      const newEdge = newEdgesList[newEdgesList.length - 1]

      // CRITICAL: Ensure targetHandle is persisted in the edge object for join nodes
      if (targetType === 'join' && targetHandle && newEdge) {
        newEdge.targetHandle = targetHandle
        if (params.sourceHandle) {
          newEdge.sourceHandle = params.sourceHandle
        }
        console.log('[Join Connection] Created edge with targetHandle:', {
          edgeId: newEdge.id,
          source: newEdge.source,
          target: newEdge.target,
          targetHandle: newEdge.targetHandle,
          sourceHandle: newEdge.sourceHandle,
        })
      }

      // Update Zustand store with the new edge (ReactFlow re-renders from store)
      const finalNewEdge = {
        ...newEdge,
        targetHandle: newEdge.targetHandle || connectionParams.targetHandle,
        sourceHandle: newEdge.sourceHandle || connectionParams.sourceHandle,
      }
      setEdges([...storeEdges, finalNewEdge])

      // Update target node's input_nodes array (node identity model)
      // For filter nodes, only allow single input (replace existing if any)
      const nodesAfterConnect = storeNodes.map((n) => {
        if (n.id === params.target) {
          const currentInputNodes = n.data.input_nodes || []
          if (targetType === 'filter') {
            return { ...n, data: { ...n.data, input_nodes: [params.source] } }
          } else {
            if (!currentInputNodes.includes(params.source)) {
              return { ...n, data: { ...n.data, input_nodes: [...currentInputNodes, params.source] } }
            }
          }
        }
        return n
      })

      // Propagate schema and metadata to target node if it's a transform node
      if (targetType === 'filter' || targetType === 'join' || targetType === 'projection' || targetType === 'calculated' || targetType === 'aggregate') {
        // Update target node with source schema info and output_metadata
        if (sourceNode.data.config || sourceNode.data.output_metadata) {
          const nodesWithMeta = nodesAfterConnect.map((n) => {
            if (n.id === params.target) {
              let outputMetadata = sourceNode.data.output_metadata
              if (!outputMetadata && sourceNode.data.config?.columns) {
                const columns = Array.isArray(sourceNode.data.config.columns)
                  ? sourceNode.data.config.columns.map((col: any) => ({
                    name: typeof col === 'string' ? col : (col.name || col.column_name || col),
                    datatype: typeof col === 'string' ? 'TEXT' : (col.datatype || col.data_type || col.type || 'TEXT'),
                    nullable: typeof col === 'string' ? true : (col.nullable !== undefined ? col.nullable : true),
                  }))
                  : []
                outputMetadata = {
                  columns: columns,
                  nodeId: sourceNode.data.node_id || sourceNode.id,
                }
              }
              return {
                ...n,
                data: {
                  ...n.data,
                  config: {
                    ...n.data.config,
                    sourceId: sourceNode.data.config?.sourceId || n.data.config?.sourceId,
                    tableName: sourceNode.data.config?.tableName || n.data.config?.tableName,
                    schema: sourceNode.data.config?.schema || n.data.config?.schema,
                    ...(targetType === 'filter' && sourceNode.data.config?.columns
                      ? { columns: sourceNode.data.config.columns }
                      : {}),
                    ...(targetType === 'projection' && sourceNode.data.config?.columns
                      ? { columns: sourceNode.data.config.columns }
                      : {}),
                  },
                  ...(targetType === 'filter' && outputMetadata ? { output_metadata: outputMetadata } : {}),
                },
              }
            }
            return n
          })
          setNodes(nodesWithMeta)

          // If target is filter and it's currently selected, trigger panel refresh
          if (targetType === 'filter' && selectedNode?.id === params.target) {
            setTimeout(() => {
              const updatedNode = useCanvasStore.getState().nodes.find((n) => n.id === params.target)
              if (updatedNode) {
                setSelectedNode(updatedNode)
              }
            }, 100)
          }
        } else {
          setNodes(nodesAfterConnect)
        }
      } else {
        setNodes(nodesAfterConnect)
      }
    },
    [storeNodes, storeEdges, setEdges, setNodes, selectedNode, setSelectedNode]
  )

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault()
    event.dataTransfer.dropEffect = 'move'
  }, [])

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault()

      const data = event.dataTransfer.getData('application/reactflow')
      if (!data || !reactFlowInstance) return

      // ENFORCEMENT: Only allow table drops (Source nodes), not generic node drops
      // Generic node drops to empty canvas are DISABLED - nodes must be added via:
      // 1. Edge-based insertion (clicking on edge)
      // 2. Output handle insertion (clicking on node output handle)
      try {
        const parsedData = JSON.parse(data)
        if (parsedData.type === 'table' && parsedData.table) {
          // Table drop is allowed - creates Source node
          const table = parsedData.table
          const position = reactFlowInstance.screenToFlowPosition({
            x: event.clientX,
            y: event.clientY,
          })

          // Generate UUID for node_id (immutable, consistent across environments)
          const generateUUID = () => {
            return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
              const r = Math.random() * 16 | 0
              const v = c === 'x' ? r : (r & 0x3 | 0x8)
              return v.toString(16)
            })
          }

          const nodeId = generateUUID()
          const shortId = nodeId.substring(0, 8)
          const technicalName = `source_${shortId}`
          const businessName = `${table.table_name}${table.schema ? ` (${table.schema})` : ''}`

          // Check if there are saved direct filters for this table
          const filterKey = `table_filter_${parsedData.sourceId}_${table.table_name}_${table.schema || 'default'}`
          let savedFilter = null
          try {
            const saved = localStorage.getItem(filterKey)
            if (saved) {
              savedFilter = JSON.parse(saved)
            }
          } catch (err) {
            console.warn('Failed to load saved filter:', err)
          }

          // Also check in-memory filter state
          const inMemoryFilter = getTableFilter(parsedData.sourceId, table.table_name, table.schema)
          const activeFilter = inMemoryFilter || savedFilter

          const newNode: Node = {
            id: nodeId,
            type: 'source',
            position,
            data: {
              id: nodeId,
              node_id: nodeId, // UUID for persistence (immutable)
              business_name: businessName, // Editable business name (required, shown on canvas)
              technical_name: technicalName, // Non-editable technical name (system-generated, required)
              node_name: businessName, // Legacy support (editable)
              node_type: 'SOURCE', // Enum format (immutable)
              label: businessName, // Legacy support (for display)
              type: 'source',
              status: 'idle',
              config: {
                sourceId: parsedData.sourceId,
                tableName: table.table_name,
                schema: table.schema,
                // Embed filter conditions directly in Source node if available
                ...(activeFilter ? {
                  conditions: activeFilter.conditions || [],
                  expression: activeFilter.expression || '',
                  mode: activeFilter.mode || 'builder',
                  isFiltered: true,
                } : {}),
              },
              input_nodes: [], // Will be populated when edges are created
              input_metadata: null,
              output_metadata: null, // Populated below after live schema fetch
              schema_version: 0,
              schema_outdated: false,
            },
          }

          // Allow multiple source nodes on the same canvas (addNode updates the store)
          // Backend save happens on explicit 'Save Pipeline' click
          addNode(newNode)

          // Fetch live schema in background and enrich node's output_metadata
          // This is the snapshot used for drift detection on subsequent opens
          fetchLiveSchema(parsedData.sourceId, table.table_name, table.schema || undefined).then((liveColumns) => {
            const currentNodes = useCanvasStore.getState().nodes
            const enriched = currentNodes.map((n: Node) =>
              n.id === nodeId
                ? {
                    ...n,
                    data: {
                      ...n.data,
                      output_metadata: {
                        ...(n.data.output_metadata || {}),
                        columns: liveColumns.map((c) => ({
                          // Display/business name
                          name: c.name,
                          business_name: c.name,
                          // Physical DB column name
                          db_name: c.name,
                          column_name: c.name,
                          // Stable technical name prefixed with source node id
                          technical_name: `${nodeId}__${c.name}`,
                          type: c.type,
                          datatype: c.type,
                          nullable: true,
                        })),
                      },
                    },
                  }
                : n
            )
            useCanvasStore.getState().setNodes(enriched)
          }).catch((err) => {
            console.warn('[onDrop] Could not fetch live schema for', table.table_name, err)
          })

          // Select the source node and show filtered data in preview
          setTimeout(() => {
            setSelectedNode(newNode)
            setShowAggregatesPanel(false) // Hide aggregates panel when selecting new node
            if (activeFilter) {
              setTableDataPanel({
                nodeId: nodeId,
                sourceId: parsedData.sourceId,
                tableName: table.table_name,
                schema: table.schema,
                directFilterConditions: activeFilter.conditions || [],
              })
            } else {
              setTableDataPanel({
                nodeId: nodeId,
                sourceId: parsedData.sourceId,
                tableName: table.table_name,
                schema: table.schema,
              })
            }
          }, 100)

          return
        }
      } catch (e) {
        // Not JSON, continue with normal node type handling
      }

      // Generic node type drop - DISABLED
      // Nodes can only be added via:
      // 1. Edge-based insertion (insert between nodes)
      // 2. Output handle insertion (add after node)
      console.warn('Generic node drops to empty canvas are disabled. Use edge-based or output handle insertion.')
      toast({
        title: 'Node insertion method required',
        description: 'Please insert nodes by clicking on an edge or a node\'s output handle.',
        status: 'info',
        duration: 3000,
        isClosable: true,
      })
    },
    [reactFlowInstance, addNode, toast, getTableFilter, setSelectedNode, setShowAggregatesPanel, setTableDataPanel, fetchLiveSchema]
  )


  const handleQuickFilter = useCallback(
    (table: { schema: string; table_name: string }, sourceId: number) => {
      // Open DIRECT_FILTER mode - no canvas nodes created
      setDirectFilterMode({
        sourceId: sourceId,
        tableName: table.table_name,
        schema: table.schema || '',
      })

      // Clear any selected node to show direct filter panel
      setSelectedNode(null)

      // Expand panels if collapsed
      if (rightPanelCollapsed) {
        setRightPanelCollapsed(false)
      }
      if (bottomPanelCollapsed) {
        setBottomPanelCollapsed(false)
      }

      // Check if there's an existing filter for this table
      const existingFilter = getTableFilter(sourceId, table.table_name, table.schema)
      if (existingFilter) {
        // Show filtered data in preview
        setTableDataPanel({
          sourceId: sourceId,
          tableName: table.table_name,
          schema: table.schema || '',
          directFilterConditions: existingFilter.conditions,
        })
      } else {
        // Show unfiltered data
        setTableDataPanel({
          sourceId: sourceId,
          tableName: table.table_name,
          schema: table.schema || '',
        })
      }
    },
    [setDirectFilterMode, setSelectedNode, rightPanelCollapsed, setRightPanelCollapsed, bottomPanelCollapsed, setBottomPanelCollapsed, getTableFilter, setTableDataPanel]
  )

  // Handle table click - show filtered or unfiltered data
  const handleTableClick = useCallback(
    (table: { schema: string; table_name: string }, sourceId: number) => {
      const existingFilter = getTableFilter(sourceId, table.table_name, table.schema)

      if (existingFilter) {
        // Table has filter - show filtered data and open filter panel
        setDirectFilterMode({
          sourceId: sourceId,
          tableName: table.table_name,
          schema: table.schema || '',
        })
        setSelectedNode(null)

        if (rightPanelCollapsed) {
          setRightPanelCollapsed(false)
        }
        if (bottomPanelCollapsed) {
          setBottomPanelCollapsed(false)
        }

        setTableDataPanel({
          sourceId: sourceId,
          tableName: table.table_name,
          schema: table.schema || '',
          directFilterConditions: existingFilter.conditions,
        })
      } else {
        // No filter - show unfiltered data and table details
        setDirectFilterMode(null)
        setTableDataPanel({
          sourceId: sourceId,
          tableName: table.table_name,
          schema: table.schema || '',
        })
      }
    },
    [getTableFilter, setDirectFilterMode, setSelectedNode, rightPanelCollapsed, setRightPanelCollapsed, bottomPanelCollapsed, setBottomPanelCollapsed, setTableDataPanel]
  )

  // Handle preview filtered data from context menu
  const handlePreviewFilteredData = useCallback(
    (table: { schema: string; table_name: string }, sourceId: number) => {
      const existingFilter = getTableFilter(sourceId, table.table_name, table.schema)

      if (existingFilter) {
        // Open direct filter mode to show filter configuration
        setDirectFilterMode({
          sourceId: sourceId,
          tableName: table.table_name,
          schema: table.schema || '',
        })

        // Clear selected node to show filter panel
        setSelectedNode(null)

        // Show filtered data in preview
        setTableDataPanel({
          sourceId: sourceId,
          tableName: table.table_name,
          schema: table.schema || '',
          directFilterConditions: existingFilter.conditions,
        })

        // Expand panels if collapsed
        if (rightPanelCollapsed) {
          setRightPanelCollapsed(false)
        }
        if (bottomPanelCollapsed) {
          setBottomPanelCollapsed(false)
        }
      }
    },
    [getTableFilter, setDirectFilterMode, setSelectedNode, setTableDataPanel, rightPanelCollapsed, setRightPanelCollapsed, bottomPanelCollapsed, setBottomPanelCollapsed]
  )

  const onNodeClick = useCallback(
    (event: React.MouseEvent, node: Node) => {
      setLineageHighlight(null)
      setPropagationHighlight(null)

      // Shift+Click: range selection (file explorer style)
      if (event.shiftKey) {
        const nds = storeNodes || []
        const sorted = [...nds].sort((a, b) => {
          const ya = (a.position?.y ?? 0)
          const yb = (b.position?.y ?? 0)
          if (ya !== yb) return ya - yb
          return (a.position?.x ?? 0) - (b.position?.x ?? 0)
        })
        const lastId = lastClickedNodeIdRef.current
        lastClickedNodeIdRef.current = node.id
        if (lastId && lastId !== node.id) {
          const idxLast = sorted.findIndex((n) => n.id === lastId)
          const idxCur = sorted.findIndex((n) => n.id === node.id)
          if (idxLast >= 0 && idxCur >= 0) {
            const [lo, hi] = idxLast < idxCur ? [idxLast, idxCur] : [idxCur, idxLast]
            const rangeIds = sorted.slice(lo, hi + 1).map((n) => n.id)
            setSelectedNodeIds(rangeIds)
          } else {
            setSelectedNodeIds([...selectedNodeIds, node.id].filter((id, i, arr) => arr.indexOf(id) === i))
          }
        } else {
          setSelectedNodeIds(selectedNodeIds.includes(node.id) ? selectedNodeIds : [...selectedNodeIds, node.id])
        }
      } else {
        // ── Fast path: O(1) selection — only updates selectedNodeId ──────────
        // No propagation, no validation, no API calls.
        // nodesById[id] lookup is O(1); right panel renders from that.
        lastClickedNodeIdRef.current = node.id
        setSelectedNodeId(node.id)
      }

      // Clear direct filter mode when clicking on a node to show node's config panel
      if (directFilterMode) {
        setDirectFilterMode(null)
      }

      // Expand right panel if collapsed (to show config)
      if (rightPanelCollapsed) {
        setRightPanelCollapsed(false)
      }

      // Dismiss any open preview panel — preview is only shown via right-click → "Preview".
      setTableDataPanel(null)
      setBottomPanelCollapsed(true)
    },
    [storeNodes, selectedNodeIds, setSelectedNodeId, setSelectedNodeIds, setLineageHighlight, setPropagationHighlight, setDirectFilterMode, directFilterMode, rightPanelCollapsed, setRightPanelCollapsed, setTableDataPanel, setBottomPanelCollapsed]
  )


  const onNodeContextMenu = useCallback(
    (event: React.MouseEvent, node: Node) => {
      event.preventDefault()

      // Show context menu at click position
      setContextMenu({
        node,
        position: { x: event.clientX, y: event.clientY },
      })
    },
    []
  )

  // Handle selection of second node for join operation
  const handleJoinNodeSelected = useCallback(
    (secondNode: Node) => {
      if (!joinNodeSource || !reactFlowInstance) return

      // Generate UUID for node_id
      const generateUUID = () => {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
          const r = Math.random() * 16 | 0
          const v = c === 'x' ? r : (r & 0x3 | 0x8)
          return v.toString(16)
        })
      }

      const nodeId = generateUUID()
      const shortId = nodeId.substring(0, 8)

      const nodeTypeDef = getNodeTypeDefinition('join')
      if (!nodeTypeDef) return

      // Get source config from first node
      let sourceConfig = joinNodeSource.data.config || {}
        if (['filter', 'join', 'projection', 'calculated', 'aggregate', 'compute'].includes(joinNodeSource.data.type)) {
        const findSourceNode = (currentNodeId: string, visited: Set<string> = new Set()): Node | null => {
          if (visited.has(currentNodeId)) return null
          visited.add(currentNodeId)

          const inputEdge = storeEdges.find((e) => e.target === currentNodeId)
          if (!inputEdge) return null

          const inputNode = storeNodes.find((n) => n.id === inputEdge.source)
          if (!inputNode) return null

          if (inputNode.data.type === 'source') {
            return inputNode
          }

          return findSourceNode(inputNode.id, visited)
        }

        const sourceNode = findSourceNode(joinNodeSource.id)
        if (sourceNode && sourceNode.data.config) {
          sourceConfig = sourceNode.data.config
        }
      }

      // Calculate position between the two nodes
      const node1Pos = joinNodeSource.position
      const node2Pos = secondNode.position
      const newNodePosition = {
        x: (node1Pos.x + node2Pos.x) / 2,
        y: (node1Pos.y + node2Pos.y) / 2,
      }

      const defaultConfig = {
        ...nodeTypeDef.defaultConfig,
        sourceId: sourceConfig.sourceId,
        tableName: sourceConfig.tableName,
        schema: sourceConfig.schema,
        joinType: 'INNER',
        conditions: [],
      }

      const technicalName = `join_${shortId}`
      const defaultBusinessName = 'Join'

          const newNode: Node = {
        id: nodeId,
        type: 'join',
        position: newNodePosition,
        data: {
          id: nodeId,
          node_id: nodeId,
          business_name: defaultBusinessName,
          technical_name: technicalName,
          node_name: defaultBusinessName,
          node_type: 'JOIN',
          label: defaultBusinessName,
          type: 'join',
          status: 'idle',
          config: defaultConfig,
          input_nodes: [],
              input_metadata: null,
              output_metadata: null,
              schema_version: 0,
              schema_outdated: false,
        },
      }

      // addNode updates the store; ReactFlow re-renders from store
      addNode(newNode)

      // Create left edge (from first node to join node)
      const leftEdge: Edge = {
        id: `edge-${joinNodeSource.id}-${nodeId}-left`,
        source: joinNodeSource.id,
        target: nodeId,
        targetHandle: 'left',
        type: 'smoothstep',
        animated: false,
      }

      // Create right edge (from second node to join node)
      const rightEdge: Edge = {
        id: `edge-${secondNode.id}-${nodeId}-right`,
        source: secondNode.id,
        target: nodeId,
        targetHandle: 'right',
        type: 'smoothstep',
        animated: false,
      }

      // Update store with both edges (ReactFlow re-renders from store)
      // Backend save happens on explicit 'Save Pipeline' click
      setEdges([...useCanvasStore.getState().edges, leftEdge, rightEdge])

      // Close modal and reset state
      onCloseJoinNodeSelect()
      setJoinNodeSource(null)

      toast({
        title: 'Join node created',
        description: 'Join node has been created with both connections.',
        status: 'success',
        duration: 3000,
        isClosable: true,
      })
    },
    [joinNodeSource, storeNodes, storeEdges, reactFlowInstance, addNode, setEdges, onCloseJoinNodeSelect, toast]
  )

  const handleContextMenuAction = useCallback(
    (action: string, node: Node) => {
      setSelectedNode(node)

      // Expand panels if collapsed
      if (rightPanelCollapsed) {
        setRightPanelCollapsed(false)
      }
      if (bottomPanelCollapsed) {
        setBottomPanelCollapsed(false)
      }

      // Handle JSON-encoded actions (from submenu items)
      let parsedAction = action
      try {
        const parsed = JSON.parse(action)
        if (parsed.action && parsed.function) {
          parsedAction = parsed.action
        }
      } catch (e) {
        // Not JSON, use as-is
      }

      switch (parsedAction) {
        case 'show-lineage': {
          setContextMenu(null)
          const nds = useCanvasStore.getState().nodes
          const edgs = useCanvasStore.getState().edges
          const path = getDownstreamPath(node.id, nds, edgs)
          setLineageHighlight(path)
          break
        }
        case 'propagate-downstream': {
          setContextMenu(null)
          const nds = useCanvasStore.getState().nodes
          const edgs = useCanvasStore.getState().edges
          const sourceMeta = node.data?.output_metadata
          if (!sourceMeta?.columns?.length) {
            toast({
              title: 'No schema to propagate',
              description: 'This node has no output schema. Save the node configuration first.',
              status: 'warning',
              duration: 4000,
              isClosable: true,
            })
            break
          }
          const path = getDownstreamPath(node.id, nds, edgs)
          if (path.nodeIds.length <= 1) {
            toast({
              title: 'No downstream nodes',
              description: 'There are no nodes downstream of this node to propagate to.',
              status: 'info',
              duration: 3000,
              isClosable: true,
            })
            break
          }
          const updated = propagateSchemaRecursively(nds, edgs, node.id, sourceMeta)
          setNodes(updated)
          setPropagationHighlight(path)
          setLineageHighlight(null)
          setIsDirty(true)
          toast({
            title: 'Smart Propagation',
            description: `Schema updated in ${path.nodeIds.length - 1} downstream node(s).`,
            status: 'success',
            duration: 3000,
            isClosable: true,
          })
          break
        }
        case 'execute-flow':
          if (hasMultipleFlows) {
            const flowContainingNode = flows.find((f) => f.has(node.id))
            if (flowContainingNode && executeFlowRef.current) {
              setContextMenu(null)
              executeFlowRef.current(Array.from(flowContainingNode))
            } else if (!flowContainingNode) {
              toast({ title: 'Could not determine flow', status: 'warning', duration: 3000, isClosable: true })
            }
          }
          break

        case 'preview':
          // Show data preview in bottom panel; sync to store for single source of truth.
          if (node.data.type === 'source' && node.data.config) {
            const config = node.data.config
            if (config.sourceId && config.tableName) {
              const data = { sourceId: config.sourceId, tableName: config.tableName, schema: config.schema }
              setTableDataPanel(data)
              setPreview(node.id, true, data)
            }
          } else if (node.data.type === 'destination' || (node.data.type && String(node.data.type).startsWith('destination-'))) {
            const data = { nodeId: node.id }
            setTableDataPanel(data)
            setPreview(node.id, true, data)
          } else {
            const inputEdge = storeEdges && Array.isArray(storeEdges) ? storeEdges.find((e) => e.target === node.id) : null
            if (inputEdge && storeNodes && Array.isArray(storeNodes)) {
              const inputNode = storeNodes.find((n) => n.id === inputEdge.source)
              if (inputNode && inputNode.data.config) {
                const data = {
                  nodeId: node.id,
                  sourceId: inputNode.data.config.sourceId,
                  tableName: inputNode.data.config.tableName,
                  schema: inputNode.data.config.schema,
                }
                setTableDataPanel(data)
                setPreview(node.id, true, data)
              } else {
                setTableDataPanel({ nodeId: node.id })
                setPreview(node.id, true, { nodeId: node.id })
              }
            } else {
              setTableDataPanel({ nodeId: node.id })
              setPreview(node.id, true, { nodeId: node.id })
            }
          }
          // Always expand bottom panel when explicitly previewing
          setBottomPanelCollapsed(false)
          break

        case 'add-join':
          // For join nodes, open a modal to select the second node
          setJoinNodeSource(node)
          onOpenJoinNodeSelect()
          break

        case 'add-destination':
          // For destination, open the destination selector modal
          // Check if node has outgoing edges - if yes, can't add destination
          const hasOutgoingEdges = storeEdges.some((e: Edge) => e.source === node.id)

          if (hasOutgoingEdges) {
            toast({
              title: 'Invalid operation',
              description: 'Destination can only be added at the end of a pipeline branch. This node has outgoing connections.',
              status: 'error',
              duration: 3000,
              isClosable: true,
            })
            break
          }

          // Check if node is already a destination
          if (node.data?.type === 'destination') {
            toast({
              title: 'Already a destination',
              description: 'This node is already a destination.',
              status: 'info',
              duration: 3000,
              isClosable: true,
            })
            break
          }

          // Open destination selector modal
          // For node-based add, we'll replace the current node with destination
          setDestinationSelectorModal({
            isOpen: true,
            sourceNodeId: '', // No source node when adding from node context
            targetNodeId: node.id, // This node will be replaced
            edgeId: '', // No edge when adding from node context
          })
          break

        case 'add-aggregates':
        case 'addAggregate':
        case 'add-filter':
        case 'add-projection':
        case 'add-compute':
          // Create new transform node connected to selected node
          const nodeTypeMap: Record<string, string> = {
            'add-filter': 'filter',
            'add-projection': 'projection',
            'add-calculated': 'calculated',
            'add-aggregates': 'aggregate',
            'addAggregate': 'aggregate',
            'add-compute': 'compute',
          }

          const newNodeType = nodeTypeMap[parsedAction] || nodeTypeMap[action]
          if (!newNodeType || !reactFlowInstance) return

          // Hide aggregates panel when creating non-aggregate nodes
          if (newNodeType !== 'aggregate') {
            setShowAggregatesPanel(false)
          }


          // Get node position and place new node to the right
          const nodePosition = node.position
          const newNodePosition = {
            x: nodePosition.x + 250,
            y: nodePosition.y,
          }

          // Generate UUID for node_id (immutable, consistent across environments)
          const generateUUID = () => {
            return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
              const r = Math.random() * 16 | 0
              const v = c === 'x' ? r : (r & 0x3 | 0x8)
              return v.toString(16)
            })
          }

          const nodeId = generateUUID()
          const shortId = nodeId.substring(0, 8)

          const nodeTypeDef = getNodeTypeDefinition(newNodeType)

          if (!nodeTypeDef) return

          // Get source config from current node
          // For transformation nodes (filter, join, etc.), we need to traverse to find the actual source
          let sourceConfig = node.data.config || {}

          // If this is a transformation node, find the source node to get sourceId/tableName/schema
          if (['filter', 'join', 'projection', 'calculated', 'aggregate', 'compute'].includes(node.data.type)) {
            const findSourceNode = (currentNodeId: string, visited: Set<string> = new Set()): Node | null => {
              if (visited.has(currentNodeId)) return null
              visited.add(currentNodeId)

              if (!storeEdges || !Array.isArray(storeEdges) || !storeNodes || !Array.isArray(storeNodes)) return null

              const inputEdge = storeEdges.find((e) => e.target === currentNodeId)
              if (!inputEdge) return null

              const inputNode = storeNodes.find((n) => n.id === inputEdge.source)
              if (!inputNode) return null

              if (inputNode.data.type === 'source') {
                return inputNode
              }

              return findSourceNode(inputNode.id, visited)
            }

            const sourceNode = findSourceNode(node.id)
            if (sourceNode && sourceNode.data.config) {
              sourceConfig = sourceNode.data.config
            }
          }

          // For destination nodes, use different default config
          const defaultConfig = newNodeType === 'destination'
            ? {
              ...nodeTypeDef.defaultConfig,
              destinationId: null,
              connectionType: 'hana',
            }
            : {
              ...nodeTypeDef.defaultConfig,
              sourceId: sourceConfig.sourceId,
              tableName: sourceConfig.tableName,
              schema: sourceConfig.schema,
            }

          // Generate technical_name for ALL node types
          const technicalNamePrefixMap: Record<string, string> = {
            'filter': 'filter',
            'join': 'join',
            'projection': 'projection',
            'calculated': 'calculated',
            'aggregate': 'aggregate',
            'destination': 'destination',
          }
          const technicalNamePrefix = technicalNamePrefixMap[newNodeType] || newNodeType.toLowerCase()
          const technicalName = `${technicalNamePrefix}_${shortId}`

          // Generate default business_name based on node type
          const defaultBusinessNameMap: Record<string, string> = {
            'filter': 'Filter',
            'join': 'Join',
            'projection': 'Projection',
            'calculated': 'Calculated Column',
            'aggregate': 'Aggregate',
            'destination': 'Destination',
          }
          const defaultBusinessName = defaultBusinessNameMap[newNodeType] || nodeTypeDef.label || 'Node'

          // Map node types to specification format for node_type enum
          const enumNodeTypeMap: Record<string, string> = {
            'filter': 'FILTER',
            'join': 'JOIN',
            'projection': 'PROJECTION',
            'calculated': 'CALCULATED_COLUMN',
            'aggregate': 'AGGREGATION',
            'destination': 'DESTINATION',
          }

          const newNode: Node = {
            id: nodeId,
            type: newNodeType,
            position: newNodePosition,
            data: {
              id: nodeId,
              node_id: nodeId, // UUID for persistence (immutable)
              business_name: defaultBusinessName, // Editable business name (required, shown on canvas)
              technical_name: technicalName, // Non-editable technical name (system-generated, required)
              node_name: defaultBusinessName, // Legacy support (editable)
              node_type: enumNodeTypeMap[newNodeType] || newNodeType.toUpperCase(), // Enum format (immutable)
              label: defaultBusinessName, // Legacy support (for display)
              type: newNodeType, // Legacy support
              status: 'idle',
              config: defaultConfig,
              input_nodes: [], // Will be populated when edges are created
              input_metadata: null,
              output_metadata: null, // Will be populated when node is saved
              schema_version: 0,
              schema_outdated: false,
            },
          }

          addNode(newNode)

          // Automatically create connection from current node to new node
          const newEdge: Edge = {
            id: `edge-${node.id}-${nodeId}`,
            source: node.id,
            target: nodeId,
            type: 'smoothstep',
            animated: false,
          }

          // Add edge to Zustand store (ReactFlow re-renders from store)
          const { addEdge: storeAddEdge } = useCanvasStore.getState()
          storeAddEdge(newEdge)

          // For filter nodes, also trigger schema propagation and fetch column count
          if (newNodeType === 'filter' && sourceConfig.sourceId && sourceConfig.tableName) {
            // Update the node directly to ensure it has the config
            setNodes(useCanvasStore.getState().nodes.map((n) =>
              n.id === nodeId
                ? {
                  ...n,
                  data: {
                    ...n.data,
                    config: {
                      ...n.data.config,
                      sourceId: sourceConfig.sourceId,
                      tableName: sourceConfig.tableName,
                      schema: sourceConfig.schema,
                    },
                  },
                }
                : n
            ))

            // Fetch column count from source
            setTimeout(async () => {
              try {
                const { sourceApi } = await import('../../services/api')
                const response = await sourceApi.columns(sourceConfig.sourceId, {
                  table_name: sourceConfig.tableName,
                  schema: sourceConfig.schema,
                  page: 1,
                  page_size: 500,
                } as Record<string, unknown>)
                const columns = (response as any)?.columns || (response as any)?.data?.columns || []
                setNodes(useCanvasStore.getState().nodes.map((n) =>
                  n.id === nodeId
                    ? {
                      ...n,
                      data: {
                        ...n.data,
                        config: {
                          ...n.data.config,
                          columnCount: columns.length,
                        },
                      },
                    }
                    : n
                ))
              } catch (err) {
                console.warn('Failed to fetch column count for filter node:', err)
              }
            }, 200)
          }

          // For projection nodes, ensure config is set and trigger column loading
          if (newNodeType === 'projection' && sourceConfig.sourceId && sourceConfig.tableName) {
            setNodes(useCanvasStore.getState().nodes.map((n) =>
              n.id === nodeId
                ? {
                  ...n,
                  data: {
                    ...n.data,
                    config: {
                      ...n.data.config,
                      sourceId: sourceConfig.sourceId,
                      tableName: sourceConfig.tableName,
                      schema: sourceConfig.schema,
                    },
                  },
                }
                : n
            ))
          }

          // Select the new node
          setSelectedNode(newNode)

          // For aggregate nodes, open the aggregates panel immediately
          if (newNodeType === 'aggregate') {
            setShowAggregatesPanel(true)
          }
          break

        case 'edit-filter':
        case 'configure-join':
        case 'configure':
          // These actions just select the node, which will show the config panel
          setSelectedNode(node)
          break

        case 'change-join-type':
        case 'edit-mappings':
          // Select node to show configuration panel
          setSelectedNode(node)
          break

        case 'duplicate':
          // Duplicate the node
          if (!reactFlowInstance) return

          const duplicateId = `${node.data.type}-${Date.now()}`
          const duplicateNode: Node = {
            ...node,
            id: duplicateId,
            position: {
              x: node.position.x + 50,
              y: node.position.y + 50,
            },
            data: {
              ...node.data,
              id: duplicateId,
              label: `${node.data.label} (Copy)`,
            },
          }

          addNode(duplicateNode)
          setSelectedNode(duplicateNode)
          break

        case 'delete':
          // All canvas updates are frontend-only until "Save Pipeline" is clicked
          deleteNodeWithBridging(node.id)
          setSelectedNode(null)
          onConfigClose()
          setContextMenu(null)
          if (tableDataPanel?.nodeId === node.id) {
            setTableDataPanel(null)
          }
          toast({
            title: 'Node deleted',
            description: 'Save the pipeline to persist this change.',
            status: 'success',
            duration: 3000,
            isClosable: true,
          })
          break

        default:
          // For other actions, just select the node
          setSelectedNode(node)
      }
    },
    [
      canvasId,
      storeNodes,
      storeEdges,
      setNodes,
      setEdges,
      setSelectedNode,
      deleteNode,
      deleteNodeWithBridging,
      addNode,
      reactFlowInstance,
      rightPanelCollapsed,
      setRightPanelCollapsed,
      bottomPanelCollapsed,
      setBottomPanelCollapsed,
      tableDataPanel,
      selectedNode,
      setContextMenu,
      setTableDataPanel,
      setPreview,
      setShowAggregatesPanel,
      setIsDirty,
      toast,
      onConfigClose,
      hasMultipleFlows,
      flows,
      getDownstreamPath,
      propagateSchemaRecursively,
      setLineageHighlight,
      setPropagationHighlight,
    ]
  )

  // Close column menu when clicking outside
  useEffect(() => {
    const handleClickOutside = () => {
      if (columnMenu) {
        setColumnMenu(null)
      }
    }

    if (columnMenu) {
      document.addEventListener('click', handleClickOutside)
      return () => document.removeEventListener('click', handleClickOutside)
    }
  }, [columnMenu])

  // Handle panel resizing (left, right, and bottom)
  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing) return

      if (isResizing === 'left') {
        const newWidth = Math.max(200, Math.min(window.innerWidth * 0.5, e.clientX))
        setLeftPanelWidth(newWidth)
      } else if (isResizing === 'right') {
        const newWidth = Math.max(200, Math.min(window.innerWidth * 0.5, window.innerWidth - e.clientX))
        setRightPanelWidth(newWidth)
      } else if (isResizing === 'bottom') {
        // Calculate height from bottom of viewport
        const viewportHeight = window.innerHeight
        const newHeight = Math.max(100, Math.min(Math.floor(viewportHeight * 0.8), viewportHeight - e.clientY))
        setBottomPanelHeight(newHeight)
      }
    }

    const handleMouseUp = () => {
      setIsResizing(null)
    }

    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove)
      document.addEventListener('mouseup', handleMouseUp)
      // Prevent text selection while resizing
      document.body.style.userSelect = 'none'
      document.body.style.cursor = isResizing === 'bottom' ? 'row-resize' : 'col-resize'
      return () => {
        document.removeEventListener('mousemove', handleMouseMove)
        document.removeEventListener('mouseup', handleMouseUp)
        document.body.style.userSelect = ''
        document.body.style.cursor = ''
      }
    }
  }, [isResizing])

  const onSelectionChange = useCallback(
    ({ nodes: selected }: { nodes: Node[]; edges: Edge[] }) => {
      setSelectedNodeIds(selected.map((n) => n.id))
    },
    [setSelectedNodeIds]
  )

  const onPaneClick = useCallback(() => {
    setSelectedNode(null)
    setSelectedNodeIds([])
    onConfigClose()
    setLineageHighlight(null)
    setPropagationHighlight(null)
    setEdgeContextMenu(null)
  }, [setSelectedNode, setSelectedNodeIds, onConfigClose, setLineageHighlight, setPropagationHighlight])

  /** Bulk Refresh Schema: propagate from the most upstream selected node. */
  const handleBulkRefreshSchema = useCallback(() => {
    if (selectedNodeIds.length < 2) return
    const nds = useCanvasStore.getState().nodes
    const edgs = useCanvasStore.getState().edges
    const selectedNodes = nds.filter((n) => selectedNodeIds.includes(n.id))
    if (selectedNodes.length === 0) return

    // Find most upstream: node with no incoming edges from other selected nodes (or fewest total incoming)
    const incomingFromSelected = (nodeId: string) =>
      (edgs || []).filter((e) => e.target === nodeId && selectedNodeIds.includes(e.source)).length
    const candidates = selectedNodes
      .filter((n) => incomingFromSelected(n.id) === 0)
      .sort((a, b) => {
        const inA = (edgs || []).filter((e) => e.target === a.id).length
        const inB = (edgs || []).filter((e) => e.target === b.id).length
        return inA - inB
      })
    const sourceNode = candidates[0] ?? selectedNodes[0]
    const sourceMeta = sourceNode.data?.output_metadata
    if (!sourceMeta?.columns?.length) {
      toast({
        title: 'No schema to propagate',
        description: `"${sourceNode.data?.business_name || sourceNode.id}" has no output schema. Configure it first.`,
        status: 'warning',
        duration: 4000,
        isClosable: true,
      })
      return
    }
    const path = getDownstreamPath(sourceNode.id, nds, edgs)
    if (path.nodeIds.length <= 1) {
      toast({
        title: 'No downstream nodes',
        description: 'There are no nodes downstream of the selected node to propagate to.',
        status: 'info',
        duration: 3000,
        isClosable: true,
      })
      return
    }
    const updated = propagateSchemaRecursively(nds, edgs, sourceNode.id, sourceMeta)
    setNodes(updated)
    setPropagationHighlight(path)
    setLineageHighlight(null)
    setIsDirty(true)
    toast({
      title: 'Bulk Refresh Schema',
      description: `Schema propagated from "${sourceNode.data?.business_name || sourceNode.id}" to ${path.nodeIds.length - 1} downstream node(s).`,
      status: 'success',
      duration: 3000,
      isClosable: true,
    })
  }, [selectedNodeIds, getDownstreamPath, propagateSchemaRecursively, setNodes, setPropagationHighlight, setLineageHighlight, setIsDirty, toast])

  const handleSaveCanvas = useCallback(async (id: number | null, name: string) => {
    setSaveLoading(true)
    try {
      // Single source of truth: always save from Zustand store
      const nodesToSave = storeNodes || []
      const edgesToSave = storeEdges || []

      // CRITICAL: Save joins first (this validates and updates join configs in state)
      const joinNodes = nodesToSave.filter((n) => n.data?.type === 'join')
      if (joinNodes.length > 0) {
        const joinErrors: string[] = []
        for (const joinNode of joinNodes) {
          const joinConfig = joinNode.data?.config || {}
          const joinType = joinConfig.joinType || 'INNER'
          const conditions = joinConfig.conditions || []
          const inputEdges = edgesToSave.filter((e) => e.target === joinNode.id)
          const leftEdge = inputEdges.find((e) => e.targetHandle === 'left')
          const rightEdge = inputEdges.find((e) => e.targetHandle === 'right')

          if (!leftEdge || !rightEdge) {
            joinErrors.push(`Join node "${joinNode.data?.business_name || joinNode.data?.label || joinNode.id}" is missing left or right input connection`)
          }

          // Validate join conditions for non-CROSS joins
          if (joinType !== 'CROSS') {
            const validConditions = conditions.filter((c: any) => c.leftColumn && c.rightColumn)
            if (validConditions.length === 0) {
              joinErrors.push(`Join node "${joinNode.data?.business_name || joinNode.data?.label || joinNode.id}": Join conditions are required for ${joinType} JOIN`)
            }
          }
        }

        if (joinErrors.length > 0) {
          toast({
            title: 'Cannot save pipeline',
            description: joinErrors[0] || 'Please fix Join node configurations before saving',
            status: 'error',
            duration: 5000,
            isClosable: true,
          })
          setValidationErrors(joinErrors)
          return
        }
      }

      // Build config from store state so deleted nodes are not saved
      const nodesWithJoinConfig = nodesToSave.map((node) => {
        if (node.data?.type === 'join') {
          const joinConfig = node.data?.config || {}
          const inputEdges = edgesToSave.filter((e) => e.target === node.id)
          const leftEdge = inputEdges.find((e) => e.targetHandle === 'left')
          const rightEdge = inputEdges.find((e) => e.targetHandle === 'right')
          const leftNodeId = leftEdge?.source || joinConfig.leftNodeId
          const rightNodeId = rightEdge?.source || joinConfig.rightNodeId
          const leftNode = nodesToSave.find((n) => n.id === leftNodeId)
          const rightNode = nodesToSave.find((n) => n.id === rightNodeId)
          const leftTableName = leftNode?.data?.label || leftNode?.data?.business_name || leftNodeId
          const rightTableName = rightNode?.data?.label || rightNode?.data?.business_name || rightNodeId

          // Ensure join config includes all required fields
          // CRITICAL: Preserve existing config but ensure required fields are present
          const enhancedConfig = {
            ...joinConfig,
            joinType: joinConfig.joinType || 'INNER',
            conditions: joinConfig.conditions || [],
            // Store left and right node IDs from edges (critical for execution)
            leftNodeId: leftNodeId,
            rightNodeId: rightNodeId,
            // Store table names for reference
            leftTable: joinConfig.leftTable || leftTableName,
            rightTable: joinConfig.rightTable || rightTableName,
          }

          // Log for debugging
          console.log(`[Save Pipeline] Persisting Join node ${node.id} config:`, {
            joinType: enhancedConfig.joinType,
            conditionsCount: enhancedConfig.conditions.length,
            conditions: enhancedConfig.conditions,
            leftNodeId: enhancedConfig.leftNodeId,
            rightNodeId: enhancedConfig.rightNodeId,
            leftTable: enhancedConfig.leftTable,
            rightTable: enhancedConfig.rightTable,
          })

          // CRITICAL: Warn if conditions are missing for non-CROSS joins
          if (enhancedConfig.joinType !== 'CROSS' && enhancedConfig.conditions.length === 0) {
            console.warn(`[Save Pipeline] WARNING: Join node ${node.id} has joinType=${enhancedConfig.joinType} but no conditions. This will fail validation.`)
          }

          return {
            id: node.id,
            type: node.type,
            position: node.position,
            data: {
              ...node.data,
              config: enhancedConfig,
            },
          }
        }
        // For non-join nodes, return as-is
        return {
          id: node.id,
          type: node.type,
          position: node.position,
          data: node.data,
        }
      })

      const config = {
        nodes: nodesWithJoinConfig,
        edges: edgesToSave.map((edge) => ({
          id: edge.id,
          source: edge.source,
          target: edge.target,
          sourceHandle: edge.sourceHandle,
          targetHandle: edge.targetHandle,
        })),
      }

      if (id) {
        // Validate name is provided
        if (!name || !name.trim()) {
          toast({
            title: 'Name required',
            description: 'Please enter a name for the canvas.',
            status: 'error',
            duration: 3000,
            isClosable: true,
          })
          return
        }

        // Update canvas name if provided
        await canvasApi.update(id, { name: name.trim() })
        // Save configuration
        await canvasApi.saveConfiguration(id, { configuration: config })
        setIsDirty(false)

        // Update canvas name in store
        setCanvas(id, name.trim())

        toast({
          title: 'Canvas saved',
          description: `Canvas "${name.trim()}" has been saved successfully.`,
          status: 'success',
          duration: 3000,
          isClosable: true,
        })
        // Stay on the same canvas page (no redirect to dashboard)
      } else {
        if (!name || name.trim() === '') {
          toast({
            title: 'Name required',
            description: 'Please enter a name for the canvas.',
            status: 'error',
            duration: 3000,
            isClosable: true,
          })
          return
        }
        // Get project_id from URL if available
        const urlParams = new URLSearchParams(window.location.search)
        const projectIdParam = urlParams.get('projectId')
        const projectId = projectIdParam ? parseInt(projectIdParam, 10) : null

        const createPayload: any = {
          name: name.trim(),
          configuration: config,
        }

        // Include project_id if available
        if (projectId && !isNaN(projectId)) {
          createPayload.project_id = projectId
        }

        console.log('[CANVAS CREATE] Creating canvas with payload:', createPayload)
        const createdCanvas = await canvasApi.create(createPayload)
        console.log('[CANVAS CREATE] Response:', createdCanvas)

        // Support both unwrapped body and raw AxiosResponse just in case
        const createdAny: any = createdCanvas as any
        const newCanvasId =
          createdAny?.id ??
          createdAny?.canvas_id ??
          createdAny?.data?.id ??
          createdAny?.data?.canvas_id

        const newCanvasName =
          createdAny?.name ??
          createdAny?.data?.name ??
          name.trim()

        if (newCanvasId) {
          setCanvas(newCanvasId, newCanvasName)
          setIsDirty(false)
          onCloseSaveModal()

          toast({
            title: 'Canvas created',
            description: `Canvas "${newCanvasName}" has been created successfully.`,
            status: 'success',
            duration: 3000,
            isClosable: true,
          })

          // Update URL with new canvas ID and reload the page to show the new canvas
          const currentUrl = new URL(window.location.href)
          currentUrl.searchParams.set('canvasId', newCanvasId.toString())
          if (projectId) {
            currentUrl.searchParams.set('projectId', projectId.toString())
          }
          window.location.href = currentUrl.toString()
        } else {
          // Fallback: treat as saved but stay on page; user can reload from dashboard
          console.warn('[CANVAS CREATE] Canvas created but response had no id; staying on current page.', createdAny)
          setIsDirty(false)
          onCloseSaveModal()
          toast({
            title: 'Canvas created',
            description:
              'Canvas was created on the server, but the response did not include an id. You can open it from the dashboard list.',
            status: 'success',
            duration: 5000,
            isClosable: true,
          })
        }
      }
    } catch (error: any) {
      console.error('Error saving canvas:', error)

      // Handle validation errors
      if (error.response?.data?.details && Array.isArray(error.response.data.details)) {
        const validationErrors = error.response.data.details
        setValidationErrors(validationErrors)
        setViewMode('validate')
        toast({
          title: 'Validation failed',
          description: 'Please fix the validation errors before saving.',
          status: 'error',
          duration: 5000,
          isClosable: true,
        })
      } else {
        let description: any =
          error?.response?.data?.error ||
          error?.response?.data?.detail ||
          error?.message ||
          'Failed to save canvas'

        if (typeof description === 'object') {
          try {
            description = JSON.stringify(description)
          } catch {
            description = 'Failed to save canvas'
          }
        }

        toast({
          title: 'Save failed',
          description,
          status: 'error',
          duration: 5000,
          isClosable: true,
        })
      }
    } finally {
      setSaveLoading(false)
    }
  }, [storeNodes, storeEdges, setCanvas, setIsDirty, onCloseSaveModal, toast, setValidationErrors, setViewMode, navigate])

  const handleSave = useCallback(async () => {
    // Always open save modal to get/confirm canvas name
    // Load current canvas name if canvas exists
      if (canvasId) {
        try {
          const canvasResponse = await canvasApi.get(canvasId)
          const currentName = canvasResponse.name || ''
        setCanvasName(currentName || `Canvas ${new Date().toLocaleString()}`)
      } catch (error) {
        console.error('Error loading canvas name:', error)
        setCanvasName(`Canvas ${new Date().toLocaleString()}`)
      }
    } else {
      // If new canvas, set default name
      setCanvasName(`Canvas ${new Date().toLocaleString()}`)
    }
    onOpenSaveModal()
  }, [canvasId, setCanvasName, onOpenSaveModal])

  const handleValidate = useCallback(async (requireDestination: boolean = true) => {
    setValidateLoading(true)
    const errors: string[] = []

    const sourceNodes = storeNodes.filter((n) => n.data.type === 'source')
    if (sourceNodes.length === 0) {
      errors.push('At least one source node is required')
    }

    // Validate Join nodes
    const joinNodes = storeNodes && Array.isArray(storeNodes) ? storeNodes.filter((n) => n.data?.type === 'join') : []
    for (const joinNode of joinNodes) {
      const joinConfig = joinNode.data?.config || {}
      const joinType = joinConfig.joinType || 'INNER'
      const conditions = joinConfig.conditions || []

      // Check if both inputs are connected
      const inputEdges = storeEdges && Array.isArray(storeEdges) ? storeEdges.filter((e) => e.target === joinNode.id) : []
      const leftEdge = inputEdges.find((e) => e.targetHandle === 'left')
      const rightEdge = inputEdges.find((e) => e.targetHandle === 'right')

      if (!leftEdge || !rightEdge) {
        errors.push(`Join node "${joinNode.data?.business_name || joinNode.data?.label || joinNode.id}" is missing left or right input connection`)
      }

      // Validate join conditions for non-CROSS joins
      if (joinType !== 'CROSS') {
        const validConditions = conditions.filter((c: any) => c.leftColumn && c.rightColumn)
        if (validConditions.length === 0) {
          errors.push(`Join node "${joinNode.data?.business_name || joinNode.data?.label || joinNode.id}": Join conditions are required for ${joinType} JOIN`)
        }
      }
    }

    // Only require destination for execution, not for saving
    if (requireDestination) {
      const destNodes = storeNodes.filter((n) => n.data.type === 'destination')
      if (destNodes.length === 0) {
        errors.push('At least one destination node is required')
      }

      destNodes.forEach((node) => {
        const config = node.data.config || {}
        const isCustomerDb = config.destinationType === 'customer_database'
        const hasConnection = isCustomerDb || config.destinationId
        if (!hasConnection || !config.tableName) {
          errors.push(`Destination node "${node.data.label}" is missing required configuration (${isCustomerDb ? 'schema and table name' : 'destination connection and table name'})`)
        }
      })
    }

    sourceNodes.forEach((node) => {
      const config = node.data.config || {}
      if (!config.sourceId || !config.tableName) {
        errors.push(`Source node "${node.data.label}" is missing required configuration`)
      }
    })

    // Backend validation (DAG, cycles, structure)
    try {
      const payload = {
        nodes: storeNodes.map((n) => ({ id: n.id, type: n.data?.type, data: n.data })),
        edges: storeEdges.map((e) => ({ source: e.source, target: e.target, sourceHandle: e.sourceHandle, targetHandle: e.targetHandle })),
        canvasId,
      }
      const res = await metadataApi.validatePipeline(payload as Record<string, unknown>)
      const backendErrors = ((res as any)?.errors as string[]) || ((res as any)?.data?.errors as string[]) || []
      const combined = [...errors, ...backendErrors]
      setValidationErrors(combined)
      setViewMode('validate')
      if (combined.length === 0) {
        toast({
          title: 'Validation passed',
          description: 'All nodes are configured, connections are valid, and the pipeline has no cycles or structural issues. Ready to save or execute.',
          status: 'success',
          duration: 5000,
          isClosable: true,
        })
      } else {
        toast({
          title: 'Validation failed',
          description: combined[0],
          status: 'error',
          duration: 5000,
          isClosable: true,
        })
      }
    } catch (err: any) {
      const backendErrors = err?.response?.data?.errors as string[] | undefined
      const msg = err?.response?.data?.error ?? err?.message ?? 'Validation request failed'
      const combined = [...errors, ...(backendErrors ?? [msg])]
      setValidationErrors(combined)
      setViewMode('validate')
      if (combined.length > 0) {
        toast({
          title: 'Validation failed',
          description: combined[0],
          status: 'error',
          duration: 5000,
          isClosable: true,
        })
      }
    } finally {
      setValidateLoading(false)
    }
  }, [storeNodes, storeEdges, setViewMode, toast])

  const handleExecute = useCallback(async (flowNodeIds?: string[]) => {
    if (isExecuteInProgressRef.current) return
    if (!canvasId) {
      toast({
        title: 'Canvas not saved',
        description: 'Please save the canvas first',
        status: 'warning',
        duration: 3000,
        isClosable: true,
      })
      return
    }
    isExecuteInProgressRef.current = true
    setExecuteLoading(true)

    // Validate before execution
    const errors: string[] = []

    // Validate Join nodes
    const joinNodes = storeNodes && Array.isArray(storeNodes) ? storeNodes.filter((n) => n.data?.type === 'join') : []
    for (const joinNode of joinNodes) {
      const joinConfig = joinNode.data?.config || {}
      const joinType = joinConfig.joinType || 'INNER'
      const conditions = joinConfig.conditions || []

      // Check if both inputs are connected
      const inputEdges = storeEdges && Array.isArray(storeEdges) ? storeEdges.filter((e) => e.target === joinNode.id) : []
      const leftEdge = inputEdges.find((e) => e.targetHandle === 'left')
      const rightEdge = inputEdges.find((e) => e.targetHandle === 'right')

      if (!leftEdge || !rightEdge) {
        errors.push(`Join node "${joinNode.data?.business_name || joinNode.data?.label || joinNode.id}" is missing left or right input connection`)
      }

      // Validate join conditions for non-CROSS joins
      if (joinType !== 'CROSS') {
        const validConditions = conditions.filter((c: any) => c.leftColumn && c.rightColumn)
        if (validConditions.length === 0) {
          errors.push(`Join node "${joinNode.data?.business_name || joinNode.data?.label || joinNode.id}": Join conditions are required for ${joinType} JOIN`)
        }
      }
    }

    if (errors.length > 0) {
      setValidationErrors(errors)
      toast({
        title: 'Validation failed',
        description: errors[0] || 'Please fix validation errors before executing',
        status: 'error',
        duration: 5000,
        isClosable: true,
      })
      setExecuteLoading(false)
      return
    }

    // Skip blocking server-side validatePipeline: it builds the full execution plan (metadata, SQL
    // compilation, plan persist) and can take 10s–2min. Execute returns 202 quickly; the migration
    // service validates and builds the plan in the background when the job runs. Use the Validate
    // button to pre-check and persist a plan if desired.

    try {
      // CRITICAL: Ensure edges include targetHandle for Join nodes
      const edgesWithHandles = storeEdges && Array.isArray(storeEdges) ? storeEdges.map((edge) => ({
        source: edge.source,
        target: edge.target,
        sourceHandle: edge.sourceHandle,
        targetHandle: edge.targetHandle, // CRITICAL: Include targetHandle for Join nodes
      })) : []

      // When multiple flows: launch one job per flow in parallel so each starts fast (smaller plan)
      const jobIds: string[] = []
      if (hasMultipleFlows && (!flowNodeIds || flowNodeIds.length === 0)) {
        const responses = await Promise.all(
          flows.map((flow) =>
            migrationApi.execute(canvasId, { config: { flow_node_ids: Array.from(flow) } })
          )
        )
        responses.forEach((r) => jobIds.push(r.data.job_id))
      } else {
        const executePayload: Record<string, unknown> = {
          nodes: storeNodes && Array.isArray(storeNodes) ? storeNodes.map((node) => ({
            id: node.id,
            type: node.type,
            data: node.data,
          })) : [],
          edges: edgesWithHandles,
        }
        if (flowNodeIds && flowNodeIds.length > 0) {
          executePayload.config = { flow_node_ids: flowNodeIds }
        }
        const response = await migrationApi.execute(canvasId, executePayload)
        jobIds.push(response.data.job_id)
      }

      const jobId = jobIds[0]
      setActiveJob(jobId)
      setExecutionStatus('running')
      setJobDetail({ current_step: 'Starting...', progress: 0, error_message: null })
      setViewMode('monitor')

      toast({
        title: jobIds.length > 1 ? `${jobIds.length} flows started in parallel` : 'Migration started',
        description: 'Watch the Monitor panel for live progress.',
        status: 'success',
        duration: 5000,
        isClosable: true,
      })

      const flushNodeProgressBatch = () => {
        if (nodeProgressFlushTimeoutRef.current) {
          clearTimeout(nodeProgressFlushTimeoutRef.current)
          nodeProgressFlushTimeoutRef.current = null
        }
        const batch = nodeProgressBatchRef.current
        if (Object.keys(batch).length === 0) return
        const statusBatch: Record<string, 'idle' | 'running' | 'success' | 'error'> = {}
        Object.entries(batch).forEach(([nodeId, { status, progress }]) => {
          statusBatch[nodeId] = status === 'completed' ? 'success' : status === 'failed' ? 'error' : (status as any) || 'running'
          if (progress !== undefined) updateJobProgress(nodeId, progress)
        })
        updateNodeStatusBatch(statusBatch)
        nodeProgressBatchRef.current = {}
      }

      const completedCountRef = { current: 0 }
      const totalJobs = jobIds.length

      const cleanupAll = () => {
        jobIds.forEach((jid) => wsService.unsubscribeFromJobUpdates(jid))
        isExecuteInProgressRef.current = false
      }

      async function catchUpStatusForJob(jid: string) {
        try {
          const res = await migrationApi.list()
          const jobs = (res ?? []) as Array<{ id: number; job_id: string; status: string }>
          const job = jobs.find((j) => j.job_id === jid)
          if (!job || !['running', 'pending', 'completed', 'failed'].includes(String(job.status).toLowerCase())) return
          const data = await migrationApi.status(job.id) as { node_progress?: Array<{ node_id: string; status: string }>; current_level?: number; total_levels?: number; level_status?: string; status?: string; progress?: number; current_step?: string }
          if (!data) return
          if (data.node_progress?.length) {
            const batch: Record<string, 'idle' | 'running' | 'success' | 'error'> = {}
            data.node_progress.forEach((np: { node_id: string; status: string }) => {
              const s = (np.status || '').toLowerCase()
              batch[np.node_id] = s === 'completed' ? 'success' : s === 'failed' ? 'error' : s === 'running' ? 'running' : 'idle'
            })
            updateNodeStatusBatch(batch)
          }
          setJobDetail((prev) => ({
            ...prev,
            current_step: data.current_step ?? prev.current_step ?? null,
            progress: data.progress ?? prev.progress ?? 0,
            current_level: data.current_level ?? prev.current_level ?? null,
            total_levels: data.total_levels ?? prev.total_levels ?? null,
            level_status: data.level_status ?? prev.level_status ?? null,
          }))
          if (data.status && ['completed', 'failed'].includes((data.status as string).toLowerCase())) {
            if ((data.status as string).toLowerCase() === 'completed') {
              completedCountRef.current += 1
              if (completedCountRef.current >= totalJobs) {
                setExecutionStatus('completed')
                setAllNodesStatus('success')
                setJobDetail((prev) => ({ ...prev, progress: 100, level_status: 'complete' }))
                cleanupAll()
              }
            }
          }
        } catch {
          // ignore
        }
      }

      const handleJobComplete = () => {
        completedCountRef.current += 1
        if (completedCountRef.current >= totalJobs) {
          flushNodeProgressBatch()
          setAllNodesStatus('success')
          setExecutionStatus('completed')
          setJobDetail((prev) => ({
            ...prev,
            progress: 100,
            current_step: 'Completed',
            error_message: null,
            level_status: 'complete',
          }))
          cleanupAll()
          toast({ title: 'Migration completed', status: 'success', duration: 5000, isClosable: true })
        }
      }

      jobIds.forEach((jid) => {
        wsService.subscribeToJobUpdates(jid, {
          onJoined: () => catchUpStatusForJob(jid),
          onStatus: (data) => {
            if (data?.current_step != null) setJobDetail((prev) => ({ ...prev, current_step: data.current_step ?? prev.current_step }))
            if (data?.progress != null) setJobDetail((prev) => ({ ...prev, progress: data.progress ?? prev.progress }))
            if (data?.current_level != null) setJobDetail((prev) => ({ ...prev, current_level: data.current_level }))
            if (data?.total_levels != null) setJobDetail((prev) => ({ ...prev, total_levels: data.total_levels }))
            if (data?.level_status != null) setJobDetail((prev) => ({ ...prev, level_status: data.level_status }))
            if ((data?.status as string)?.toLowerCase() === 'completed') {
              handleJobComplete()
            }
          },
          onNodeProgress: (data) => {
            if (data?.node_id) {
              const status = (data.status as string) || 'running'
              const normalized = status === 'completed' ? 'success' : status === 'failed' ? 'error' : (status as 'idle' | 'running')
              updateNodeStatus(data.node_id, normalized)
              if (data.progress !== undefined) updateJobProgress(data.node_id, data.progress)
              nodeProgressBatchRef.current[data.node_id] = { status, progress: data.progress }
              if (!nodeProgressFlushTimeoutRef.current) {
                nodeProgressFlushTimeoutRef.current = setTimeout(flushNodeProgressBatch, 180)
              }
            }
            if (data?.current_step != null) setJobDetail((prev) => ({ ...prev, current_step: data.current_step ?? prev.current_step }))
            if (data?.progress != null) setJobDetail((prev) => ({ ...prev, progress: data.progress ?? prev.progress }))
            if (data?.current_level != null) setJobDetail((prev) => ({ ...prev, current_level: data.current_level }))
            if (data?.total_levels != null) setJobDetail((prev) => ({ ...prev, total_levels: data.total_levels }))
            if (data?.level_status != null) setJobDetail((prev) => ({ ...prev, level_status: data.level_status }))
          },
          onComplete: (data) => {
            if (data?.node_progress?.length) {
              const batch: Record<string, 'idle' | 'running' | 'success' | 'error'> = {}
              data.node_progress.forEach((np: { node_id: string; status: string }) => {
                const s = (np.status || '').toLowerCase()
                batch[np.node_id] = s === 'completed' ? 'success' : s === 'failed' ? 'error' : (s === 'running' ? 'running' : 'idle')
              })
              updateNodeStatusBatch(batch)
            }
            handleJobComplete()
          },
          onError: (data) => {
            setExecutionStatus('failed')
            setJobDetail((prev) => ({ ...prev, error_message: data?.error ?? 'Unknown error' }))
            cleanupAll()
            toast({ title: 'Migration failed', description: data?.error ?? 'Unknown error', status: 'error', duration: 8000, isClosable: true })
          },
        })
      })
    } catch (error: any) {
      isExecuteInProgressRef.current = false
      console.error('Error executing migration:', error)
      const msg = error?.response?.data?.error ?? error?.message ?? 'Failed to start migration'
      toast({
        title: 'Migration not triggered',
        description: msg,
        status: 'error',
        duration: 8000,
        isClosable: true,
      })
    } finally {
      setExecuteLoading(false)
    }
  }, [storeNodes, storeEdges, canvasId, setViewMode, setActiveJob, updateNodeStatus, updateNodeStatusBatch, updateJobProgress, setAllNodesStatus, toast])

  useEffect(() => {
    executeFlowRef.current = handleExecute
  }, [handleExecute])

  const handleDeleteSelected = useCallback(() => {
    const idsToDelete = selectedNodeIds.length > 0 ? selectedNodeIds : (selectedNode ? [selectedNode.id] : [])
    if (idsToDelete.length === 0) return
    if (canvasId) {
      idsToDelete.forEach((id) => {
        const node = storeNodes?.find((n) => n.id === id)
        if (node) handleContextMenuAction('delete', node)
      })
    } else {
      idsToDelete.forEach((id) => deleteNode(id))
    }
    setSelectedNode(null)
    setSelectedNodeIds([])
    onConfigClose()
  }, [selectedNode, selectedNodeIds, storeNodes, canvasId, handleContextMenuAction, deleteNode, setSelectedNode, setSelectedNodeIds, onConfigClose])

  return (
    <Box
      w="100vw"
      h="100vh"
      display="flex"
      bg={bg}
      position="relative"
      overflow="hidden"
      className="data-migration-layout"
    >
      {/* Left Sidebar with Collapse/Resize - Responsive */}
      {!leftPanelCollapsed ? (
        <>
          <Box
            w={`${leftPanelWidth}px`}
            minW="200px"
            maxW="50vw"
            h="100vh"
            position="relative"
            borderRightWidth="1px"
            borderColor={useColorModeValue('gray.200', 'gray.700')}
            display="flex"
            flexDirection="column"
            bg={useColorModeValue('white', 'gray.800')}
            overflow="hidden"
            className="left-sidebar"
            flexShrink={0}
          >
            {/* Collapse Button */}
            <Box position="absolute" right={-12} top="50%" transform="translateY(-50%)" zIndex={20}>
              <IconButton
                aria-label="Collapse left panel"
                icon={<ChevronLeft size={16} />}
                size="xs"
                variant="solid"
                colorScheme="gray"
                onClick={() => setLeftPanelCollapsed(true)}
              />
            </Box>
            <Box flex={1} overflowY="auto" overflowX="hidden">
              <SourceConnectionsSidebar
                selectedSourceId={sourceId}
                onSourceSelect={setSelectedSource}
                onTableDrag={() => {
                  // Table drag is handled in onDrop
                }}
                onQuickFilter={handleQuickFilter}
                onTableClick={handleTableClick}
                onPreviewFilteredData={handlePreviewFilteredData}
                onRemoveFilter={(table, sourceId) => {
                  removeTableFilter(sourceId, table.table_name, table.schema)
                  // Also remove from localStorage
                  const filterKey = `table_filter_${sourceId}_${table.table_name}_${table.schema || 'default'}`
                  try {
                    localStorage.removeItem(filterKey)
                  } catch (err) {
                    console.warn('Failed to remove filter from localStorage:', err)
                  }
                  // Close filter panel if open for this table
                  if (directFilterMode &&
                    directFilterMode.sourceId === sourceId &&
                    directFilterMode.tableName === table.table_name &&
                    directFilterMode.schema === (table.schema || '')) {
                    setDirectFilterMode(null)
                  }
                  // Update preview to show unfiltered data
                  setTableDataPanel({
                    sourceId: sourceId,
                    tableName: table.table_name,
                    schema: table.schema || '',
                  })
                }}
                hasTableFilter={hasTableFilter}
              />
            </Box>
            {/* Resize Handle */}
            <Box
              position="absolute"
              right={0}
              top={0}
              w="4px"
              h="100%"
              cursor="col-resize"
              bg="transparent"
              _hover={{ bg: 'blue.500', opacity: 0.7 }}
              onMouseDown={(e) => {
                e.preventDefault()
                setIsResizing('left')
              }}
              zIndex={10}
            />
          </Box>
        </>
      ) : (
        <Box position="relative" w="0" borderRightWidth="1px" borderColor={useColorModeValue('gray.200', 'gray.700')}>
          <Box position="absolute" left={-12} top="50%" transform="translateY(-50%)" zIndex={20}>
            <IconButton
              aria-label="Expand left panel"
              icon={<ChevronRight size={16} />}
              size="xs"
              variant="solid"
              colorScheme="gray"
              onClick={() => {
                setLeftPanelCollapsed(false)
              }}
            />
          </Box>
        </Box>
      )}

      {/* Middle Section: Canvas + Bottom Panel */}
      <Box
        flex="1 1 auto"
        display="flex"
        flexDirection="column"
        position="relative"
        minH={0}
        overflow="hidden"
        className="canvas-area"
        h="100vh"
      >
        {/* Canvas Area */}
        <Box
          flex={1}
          position="relative"
          ref={reactFlowWrapper}
          minH={0}
          overflow="hidden"
        >
          <ReactFlow
            nodes={useMemo(
              () =>
                (storeNodes || []).map((n) => ({
                  ...n,
                  selected: selectedNodeIds.includes(n.id),
                })),
              [storeNodes, selectedNodeIds]
            )}
            multiSelectionKeyCode={['Control', 'Meta']}
            selectionKeyCode="Shift"
            selectionOnDrag
            selectionMode={SelectionMode.Partial}
            onSelectionChange={onSelectionChange}
            edges={useMemo(() => {
              const defaultStroke = '#94a3b8' // neutral grey – same for all edges so none stand out
              const lineageEdgeIds = new Set(lineageHighlight?.edgeIds ?? [])
              const propagationEdgeIds = new Set(propagationHighlight?.edgeIds ?? [])
              const lineageStroke = '#14b8a6' // teal-500 – lineage highlight
              const propagationStroke = '#22c55e' // green-500 – Smart Propagation success
              return (storeEdges || []).map((e) => {
                const inPropagation = propagationEdgeIds.has(e.id)
                const inLineage = lineageEdgeIds.has(e.id)
                const highlighted = inPropagation || inLineage
                const stroke = inPropagation ? propagationStroke : inLineage ? lineageStroke : ((e.style as any)?.stroke ?? defaultStroke)
                return {
                  ...e,
                  style: {
                    ...(e.style || {}),
                    stroke,
                    strokeWidth: highlighted ? 1.5 : ((e.style as any)?.strokeWidth ?? 1),
                  },
                }
              })
            }, [storeEdges, lineageHighlight, propagationHighlight])}
            onNodesChange={(changes) => {
              const positionOnly = changes.every((c) => c.type === 'position')
              const removeChanges = changes.filter((c): c is { type: 'remove'; id: string } => c.type === 'remove')

              // Remove: eagerly clear from store + clear connected edges
              if (removeChanges.length > 0) {
                const removedIds = new Set(removeChanges.map((c) => c.id))
                const newNodes = storeNodes.filter((n) => !removedIds.has(n.id))
                const newEdges = storeEdges.filter((e) => !removedIds.has(e.source) && !removedIds.has(e.target))
                setNodes(newNodes)
                setEdges(newEdges)
                setIsDirty(true)
                return
              }

              // Position drag: update without creating undo history entry
              const applied = applyNodeChanges(changes, storeNodes)
              if (positionOnly) {
                updateNodePositions(applied)  // no undo entry
              } else {
                setNodes(applied)  // creates undo entry (add, select, etc.)
              }
              if (changes.some((c) => c.type === 'position' || c.type === 'remove' || c.type === 'add')) {
                setIsDirty(true)
              }
            }}
            onNodeDragStop={() => {
              // Commit drag position to undo history now that drag is done
              setNodes([...storeNodes])
              setIsDirty(true)
            }}
            onEdgesChange={(changes) => {
              // Handle edge removal - clear input_nodes when edges are removed
              const removeChanges = changes.filter((c) => c.type === 'remove')
              if (removeChanges.length > 0) {
                const removeIds = new Set(removeChanges.map((c) => c.id))
                const updatedNodes = storeNodes.map((n) => {
                  const removedEdge = storeEdges.find((e) => e.id && removeIds.has(e.id) && e.target === n.id)
                  if (removedEdge) {
                    const currentInputNodes = n.data.input_nodes || []
                    return {
                      ...n,
                      data: {
                        ...n.data,
                        input_nodes: currentInputNodes.filter((id: string) => id !== removedEdge.source),
                        ...(n.data.type === 'filter' ? { output_metadata: null } : {}),
                      },
                    }
                  }
                  return n
                })
                setNodes(updatedNodes)
              }
              // Apply edge changes to store (ReactFlow re-renders from store)
              setEdges(applyEdgeChanges(changes, storeEdges))
              if (changes.some((c) => c.type === 'remove' || c.type === 'add')) {
                setIsDirty(true)
              }
            }}
            onConnect={onConnect}
            onInit={setReactFlowInstance}
            onDrop={onDrop}
            onDragOver={onDragOver}
            onNodeClick={onNodeClick}
            onNodeContextMenu={onNodeContextMenu}
            onPaneClick={onPaneClick}
            onEdgeClick={(_event, edge) => {
              setLineageHighlight(null)
              setPropagationHighlight(null)
              setEdgeContextMenu({
                edge,
                position: { x: _event.clientX, y: _event.clientY },
              })
            }}
            onEdgeContextMenu={(event, edge) => {
              event.preventDefault()
              console.log('[EDGE CLICK] Edge right-clicked via ReactFlow handler:', edge)
              // Show edge context menu at click position
              setEdgeContextMenu({
                edge,
                position: { x: event.clientX, y: event.clientY },
              })
            }}
            nodeTypes={useMemo(() => {
              // Create node types with onNodeNameChange handler
              const handleNodeNameChange = (nodeId: string, newName: string) => {
                // Validation: business_name cannot be blank
                if (!newName || !newName.trim()) {
                  console.warn('Node name cannot be blank')
                  return
                }

                setIsDirty(true)
                const currentNodes = useCanvasStore.getState().nodes
                const updatedNodes = currentNodes.map((n) =>
                  n.id === nodeId
                    ? {
                        ...n,
                        data: {
                          ...n.data,
                          business_name: newName.trim(),
                          node_name: newName.trim(),
                          label: newName.trim(),
                        },
                      }
                    : n
                )
                // Sync to store immediately so the explicit Save action will persist the rename.
                // Do NOT auto-save to backend here; only the main 'Save Pipeline' handler should persist.
                setNodes(updatedNodes)
              }

              // Resolve status from ref (latest nodeStatuses) so we don't need nodeStatuses in deps
              const resolveStatus = (props: any) => {
                const statuses = nodeStatusesRef.current
                return statuses[props.id] ?? statuses[props.data?.node_id] ?? props.data?.status ?? 'idle'
              }

              return {
                ...nodeTypes,
                source: (props: any) => {
                  const SourceNode = nodeTypes.source

                  // Refresh Schema handler — clear cache, re-detect drift, apply changes
                  const handleRefreshSchema = async (nodeId: string) => {
                    const currentNodes = useCanvasStore.getState().nodes
                    const targetNode = currentNodes.find((n: Node) => n.id === nodeId)
                    if (!targetNode) return

                    const connectionId = targetNode.data?.config?.sourceId
                    const tableName = targetNode.data?.config?.tableName
                    const schema = targetNode.data?.config?.schema

                    if (!connectionId || !tableName) return

                    // Clear cache for this table so we get fresh data
                    clearTableSchemaCache(connectionId, tableName, schema)

                    try {
                      toast({ title: 'Refreshing schema…', status: 'info', duration: 2000, isClosable: true, position: 'bottom-right' })

                      const driftResults = await detectDrift(currentNodes, true)
                      const result = driftResults.find((r) => r.nodeId === nodeId)

                      const latestNodes = useCanvasStore.getState().nodes
                      let updatedNodes = [...latestNodes]

                      updatedNodes = updatedNodes.map((n: Node) => {
                        if (n.id !== nodeId) return n

                        // Rebuild merged column list from live schema
                        const mergedCols = (result?.liveColumns ?? []).map((c) => ({
                          name: c.name,
                          column_name: c.name,
                          technical_name: c.name,
                          type: c.type,
                          datatype: c.type,
                          nullable: true,
                        }))

                        return {
                          ...n,
                          data: {
                            ...n.data,
                            output_metadata: {
                              ...(n.data.output_metadata || {}),
                              columns: mergedCols,
                            },
                            schema_drift: null, // Clear drift badge — changes have been applied
                            schema_outdated: false,
                          },
                        }
                      })

                      // Mark downstream nodes as outdated so user knows to re-propagate
                      const { edges: latestEdges } = useCanvasStore.getState()
                      updatedNodes = updateImmediateDownstreamInputs(
                        updatedNodes,
                        latestEdges,
                        nodeId,
                        updatedNodes.find((n: Node) => n.id === nodeId)?.data?.output_metadata || {}
                      )

                      setNodes(updatedNodes)
                      setIsDirty(true)

                      toast({ title: 'Schema refreshed', description: `Schema for ${tableName} updated.`, status: 'success', duration: 3000, isClosable: true, position: 'bottom-right' })
                    } catch (err) {
                      console.error('[RefreshSchema] Failed:', err)
                      toast({ title: 'Refresh failed', status: 'error', duration: 3000, isClosable: true })
                    }
                  }

                  const nodeData = {
                    ...props.data,
                    status: resolveStatus(props),
                    onNodeNameChange: handleNodeNameChange,
                    onRefreshSchema: handleRefreshSchema,
                    business_name: props.data.business_name || props.data.node_name || props.data.label || 'Source Table',
                  }
                  return <SourceNode {...props} data={nodeData} />
                },
                filter: (props: any) => {
                  const FilterNode = nodeTypes.filter
                  const nodeData = {
                    ...props.data,
                    status: resolveStatus(props),
                    onNodeNameChange: handleNodeNameChange,
                    business_name: props.data.business_name || props.data.node_name || props.data.label || 'Filter',
                  }
                  return <FilterNode {...props} data={nodeData} />
                },
                join: (props: any) => {
                  const JoinNode = nodeTypes.join
                  const nodeData = {
                    ...props.data,
                    status: resolveStatus(props),
                    onNodeNameChange: handleNodeNameChange,
                    business_name: props.data.business_name || props.data.node_name || props.data.label || 'Join',
                  }
                  return <JoinNode {...props} data={nodeData} />
                },
                projection: (props: any) => {
                  const ProjectionNode = nodeTypes.projection
                  const nodeData = {
                    ...props.data,
                    status: resolveStatus(props),
                    onNodeNameChange: handleNodeNameChange,
                    business_name: props.data.business_name || props.data.node_name || props.data.label || 'Projection',
                  }
                  return <ProjectionNode {...props} data={nodeData} />
                },
                calculated: (props: any) => {
                  const CalculatedColumnNode = nodeTypes.calculated
                  const nodeData = {
                    ...props.data,
                    status: resolveStatus(props),
                    onNodeNameChange: handleNodeNameChange,
                    business_name: props.data.business_name || props.data.node_name || props.data.label || 'Calculated Column',
                  }
                  return <CalculatedColumnNode {...props} data={nodeData} />
                },
                destination: (props: any) => {
                  const DestinationNode = nodeTypes.destination
                  const nodeData = {
                    ...props.data,
                    status: resolveStatus(props),
                    onNodeNameChange: handleNodeNameChange,
                    business_name: props.data.business_name || props.data.node_name || props.data.label || 'Destination',
                  }
                  return <DestinationNode {...props} data={nodeData} />
                },
                // destination-hana, destination-postgresql, destination-postgres etc. — same editable business name as destination
                'destination-hana': (props: any) => {
                  const DestinationNode = nodeTypes.destination
                  const nodeData = {
                    ...props.data,
                    status: resolveStatus(props),
                    onNodeNameChange: handleNodeNameChange,
                    business_name: props.data.business_name || props.data.node_name || props.data.label || 'Destination',
                  }
                  return <DestinationNode {...props} data={nodeData} />
                },
                'destination-postgresql': (props: any) => {
                  const DestinationNode = nodeTypes.destination
                  const nodeData = {
                    ...props.data,
                    status: resolveStatus(props),
                    onNodeNameChange: handleNodeNameChange,
                    business_name: props.data.business_name || props.data.node_name || props.data.label || 'Destination',
                  }
                  return <DestinationNode {...props} data={nodeData} />
                },
                'destination-postgres': (props: any) => {
                  const DestinationNode = nodeTypes.destination
                  const nodeData = {
                    ...props.data,
                    status: resolveStatus(props),
                    onNodeNameChange: handleNodeNameChange,
                    business_name: props.data.business_name || props.data.node_name || props.data.label || 'Destination',
                  }
                  return <DestinationNode {...props} data={nodeData} />
                },
                transform: (props: any) => {
                  const TransformNode = nodeTypes.transform
                  const nodeData = {
                    ...props.data,
                    status: resolveStatus(props),
                    onNodeNameChange: handleNodeNameChange,
                    business_name: props.data.business_name || props.data.node_name || props.data.label || 'Transform',
                  }
                  return <TransformNode {...props} data={nodeData} />
                },
                aggregate: (props: any) => {
                  const TransformNode = nodeTypes.aggregate
                  const nodeData = {
                    ...props.data,
                    status: resolveStatus(props),
                    onNodeNameChange: handleNodeNameChange,
                    business_name: props.data.business_name || props.data.node_name || props.data.label || 'Aggregate',
                  }
                  return <TransformNode {...props} data={nodeData} />
                },
                compute: (props: any) => {
                  const TransformNode = nodeTypes.compute
                  const nodeData = {
                    ...props.data,
                    status: resolveStatus(props),
                    onNodeNameChange: handleNodeNameChange,
                    business_name: props.data.business_name || props.data.node_name || props.data.label || 'Compute',
                  }
                  return <TransformNode {...props} data={nodeData} />
                },
              }
            }, [canvasId, toast, setIsDirty, setNodes, nodeTypes, detectDrift, clearTableSchemaCache, updateImmediateDownstreamInputs]) as NodeTypes}
            edgeTypes={edgeTypes as EdgeTypes}
            isValidConnection={isValidConnection}
            connectionRadius={20}
            fitView
            defaultEdgeOptions={{
              type: 'smoothstep',
              animated: false,
            }}
          >
            <Controls />
            <Background />
            <MiniMap />

            {/* Top Toolbar */}
            <Panel position="top-center">
              <Box
                bg={toolbarBg}
                p={3}
                borderRadius="lg"
                boxShadow="lg"
                borderWidth="1px"
              >
                <HStack spacing={2}>
                  {/* View Mode Buttons */}
                  <HStack spacing={1} borderRightWidth="1px" pr={2} mr={2}>
                    <Tooltip label="Design Mode">
                      <IconButton
                        aria-label="Design"
                        icon={<Eye />}
                        size="sm"
                        colorScheme={viewMode === 'design' ? 'brand' : 'gray'}
                        variant={viewMode === 'design' ? 'solid' : 'ghost'}
                        onClick={() => setViewMode('design')}
                      />
                    </Tooltip>
                    <Tooltip label={validateLoading ? 'Validating…' : 'Validate: check all nodes are configured, connected correctly, and pipeline has no cycles or structural issues'}>
                      <IconButton
                        aria-label="Validate"
                        icon={validateLoading ? <Spinner size="sm" /> : <CheckCircle />}
                        size="sm"
                        colorScheme={viewMode === 'validate' ? 'brand' : 'gray'}
                        variant={viewMode === 'validate' ? 'solid' : 'ghost'}
                        onClick={() => handleValidate(true)}
                        isDisabled={validateLoading}
                      />
                    </Tooltip>
                    <Tooltip label="Monitor">
                      <IconButton
                        aria-label="Monitor"
                        icon={<BarChart3 />}
                        size="sm"
                        colorScheme={viewMode === 'monitor' ? 'brand' : 'gray'}
                        variant={viewMode === 'monitor' ? 'solid' : 'ghost'}
                        onClick={() => setViewMode('monitor')}
                      />
                    </Tooltip>
                  </HStack>
                  {/* Show when a migration is running so user knows to check Monitor */}
                  {(activeJobId && (executionStatus === 'running' || executionStatus === 'pending')) && (
                    <HStack spacing={2} px={2} py={1} bg="blue.50" borderRadius="md" borderWidth="1px" borderColor="blue.200">
                      <Spinner size="xs" />
                      <Text fontSize="xs" fontWeight="medium" color="blue.700">
                        Migration in progress — {Math.round(jobDetail.progress)}%
                      </Text>
                      <Button size="xs" variant="link" colorScheme="blue" onClick={() => setViewMode('monitor')}>
                        Open Monitor
                      </Button>
                    </HStack>
                  )}

                  {/* Action Buttons */}
                  <HStack spacing={1} borderRightWidth="1px" pr={2} mr={2}>
                    <Tooltip label="Undo">
                      <IconButton
                        aria-label="Undo"
                        icon={<Undo />}
                        size="sm"
                        variant="ghost"
                        onClick={undo}
                        isDisabled={past.length === 0}
                      />
                    </Tooltip>
                    <Tooltip label="Redo">
                      <IconButton
                        aria-label="Redo"
                        icon={<Redo />}
                        size="sm"
                        variant="ghost"
                        onClick={redo}
                        isDisabled={future.length === 0}
                      />
                    </Tooltip>
                  </HStack>

                  {/* Action Buttons */}
                  <Button
                    leftIcon={<Save />}
                    size="sm"
                    colorScheme={isDirty ? "orange" : "blue"}
                    onClick={handleSave}
                    title="Save entire pipeline (nodes, edges, configurations)"
                  >
                    {isDirty ? "Save Pipeline *" : "Save Pipeline"}
                  </Button>
                  <Tooltip label="Check all nodes are configured and pipeline has no issues (sources, joins, destinations, connections, no cycles)">
                    <Button
                      leftIcon={<CheckCircle />}
                      size="sm"
                      colorScheme="purple"
                      isLoading={validateLoading}
                      loadingText="Validating…"
                      onClick={() => handleValidate(true)}
                      isDisabled={validateLoading}
                    >
                      Validate
                    </Button>
                  </Tooltip>
                  <Button
                    leftIcon={<Play />}
                    size="sm"
                    colorScheme="green"
                    isLoading={executeLoading}
                    loadingText="Starting…"
                    onClick={() => handleExecute()}
                    isDisabled={executeLoading}
                    title={hasMultipleFlows ? 'Execute all flows in parallel' : 'Execute pipeline'}
                  >
                    Execute
                  </Button>
                  {selectedNodeIds.length > 1 && (
                    <Tooltip label="Propagate schema from the most upstream selected node to all downstream nodes">
                      <Button
                        leftIcon={<RefreshCw />}
                        size="sm"
                        colorScheme="teal"
                        variant="outline"
                        onClick={handleBulkRefreshSchema}
                      >
                        Bulk Refresh Schema
                      </Button>
                    </Tooltip>
                  )}
                  {(selectedNode || selectedNodeIds.length > 0) && (
                    <Tooltip label={selectedNodeIds.length > 1 ? 'Delete selected nodes' : 'Delete selected node'}>
                      <IconButton
                        aria-label="Delete"
                        icon={<Trash2 />}
                        size="sm"
                        colorScheme="red"
                        onClick={handleDeleteSelected}
                      />
                    </Tooltip>
                  )}
                </HStack>
              </Box>
            </Panel>

            {/* Validation result panel */}
            {viewMode === 'validate' && (
              <Panel position="top-right">
                {validationErrors.length > 0 ? (
                  <Box
                    bg="red.50"
                    borderWidth="1px"
                    borderColor="red.200"
                    borderRadius="lg"
                    p={4}
                    maxW="md"
                    boxShadow="lg"
                  >
                    <HStack mb={2}>
                      <Text fontWeight="semibold" color="red.900">
                        Validation Errors
                      </Text>
                      <IconButton
                        aria-label="Close"
                        icon={<X />}
                        size="xs"
                        variant="ghost"
                        onClick={() => setValidationErrors([])}
                      />
                    </HStack>
                    <VStack align="stretch" spacing={1}>
                      {validationErrors.map((error, idx) => (
                        <Text key={idx} fontSize="sm" color="red.700">
                          • {error}
                        </Text>
                      ))}
                    </VStack>
                  </Box>
                ) : (
                  <Box
                    bg="green.50"
                    borderWidth="1px"
                    borderColor="green.200"
                    borderRadius="lg"
                    p={4}
                    maxW="md"
                    boxShadow="lg"
                  >
                    <HStack>
                      <CheckCircle size={20} color="var(--chakra-colors-green-600)" />
                      <Text fontWeight="semibold" color="green.900">
                        Validation passed
                      </Text>
                    </HStack>
                    <Text fontSize="sm" color="green.700" mt={2}>
                      All nodes are configured, connections are valid, and the pipeline has no cycles or structural issues. Ready to save or execute.
                    </Text>
                  </Box>
                )}
              </Panel>
            )}

            {/* Execution Monitor panel - shows migration in progress / completed / failed */}
            {viewMode === 'monitor' && (
              <Panel position="top-right">
                <Box
                  bg={useColorModeValue('white', 'gray.800')}
                  borderWidth="1px"
                  borderColor={useColorModeValue('gray.200', 'gray.600')}
                  borderRadius="lg"
                  p={4}
                  minW="320px"
                  maxW="md"
                  boxShadow="lg"
                >
                  <HStack mb={3} justifyContent="space-between">
                    <Text fontWeight="semibold" fontSize="md">
                      Migration status
                    </Text>
                    {activeJobId && (
                      <IconButton
                        aria-label="Dismiss"
                        icon={<X />}
                        size="xs"
                        variant="ghost"
                        onClick={() => {
                          setActiveJob(null)
                          setExecutionStatus('idle')
                          setJobDetail({ current_step: null, progress: 0, error_message: null, current_level: null, total_levels: null, level_status: null })
                        }}
                      />
                    )}
                  </HStack>
                  {activeJobId ? (
                    <VStack align="stretch" spacing={3}>
                      <Text fontSize="xs" color={useColorModeValue('gray.600', 'gray.400')} fontFamily="mono">
                        Job: {activeJobId.substring(0, 8)}…
                      </Text>
                      <HStack>
                        {(() => {
                          // Fallback: level_status/progress can indicate completion before executionStatus updates (e.g. WebSocket delay)
                          const isEffectivelyComplete =
                            executionStatus === 'completed' ||
                            (jobDetail.level_status === 'complete' &&
                              (jobDetail.progress >= 100 ||
                                (jobDetail.current_level === jobDetail.total_levels && (jobDetail.total_levels ?? 0) > 0)))
                          if (isEffectivelyComplete) {
                            return (
                              <>
                                <CheckCircle size={18} color="var(--chakra-colors-green-600)" />
                                <Text fontWeight="medium" color="green.600">
                                  Completed
                                </Text>
                              </>
                            )
                          }
                          if (executionStatus === 'running' || executionStatus === 'pending') {
                            return (
                              <>
                                <Spinner size="sm" />
                                <Text fontWeight="medium" color="blue.600">
                                  In progress
                                </Text>
                              </>
                            )
                          }
                          if (executionStatus === 'failed') {
                            return (
                              <>
                                <Box w={3} h={3} borderRadius="full" bg="red.500" />
                                <Text fontWeight="medium" color="red.600">
                                  Failed
                                </Text>
                              </>
                            )
                          }
                          if (executionStatus === 'cancelled') {
                            return (
                              <Text fontWeight="medium" color="gray.600">
                                Cancelled
                              </Text>
                            )
                          }
                          return (
                            <Text fontWeight="medium" color="gray.600">
                              {executionStatus || '—'}
                            </Text>
                          )
                        })()}
                      </HStack>
                      {(jobDetail.total_levels != null && jobDetail.total_levels > 0) && (
                        <Box>
                          <HStack spacing={2} flexWrap="wrap">
                            <Text fontSize="sm" fontWeight="semibold" color={useColorModeValue('gray.800', 'gray.200')}>
                              {((jobDetail.current_level ?? 0) <= 0 && jobDetail.level_status !== 'complete')
                                ? `Level 0 of ${jobDetail.total_levels}`
                                : `Level ${jobDetail.current_level ?? 0} of ${jobDetail.total_levels}`}
                            </Text>
                            <Badge size="sm" colorScheme={jobDetail.level_status === 'complete' ? 'green' : 'blue'} variant="subtle">
                              {jobDetail.level_status === 'complete' ? 'Complete' : (jobDetail.current_level ?? 0) <= 0 ? 'Preparing' : 'Running'}
                            </Badge>
                          </HStack>
                          {((jobDetail.current_level ?? 0) <= 0 && jobDetail.level_status !== 'complete') && (
                            <Text fontSize="xs" mt={1} color={useColorModeValue('gray.500', 'gray.400')}>
                              Preparing pipeline — {jobDetail.total_levels} level{jobDetail.total_levels !== 1 ? 's' : ''}
                            </Text>
                          )}
                          <HStack mt={1.5} spacing={1} flexWrap="wrap">
                            {Array.from({ length: jobDetail.total_levels }, (_, i) => i + 1).map((levelNum) => {
                              const isComplete = levelNum < (jobDetail.current_level ?? 0) || (levelNum === jobDetail.current_level && jobDetail.level_status === 'complete')
                              const isCurrent = levelNum === jobDetail.current_level && jobDetail.level_status !== 'complete'
                              return (
                                <Badge
                                  key={levelNum}
                                  size="sm"
                                  colorScheme={isComplete ? 'green' : isCurrent ? 'blue' : 'gray'}
                                  variant={isComplete || isCurrent ? 'subtle' : 'outline'}
                                  title={isComplete ? `Level ${levelNum} complete` : isCurrent ? `Level ${levelNum} running` : `Level ${levelNum} pending`}
                                >
                                  {isComplete ? `✓ ${levelNum}` : isCurrent ? `● ${levelNum}` : levelNum}
                                </Badge>
                              )
                            })}
                          </HStack>
                        </Box>
                      )}
                      {(jobDetail.current_step != null && jobDetail.current_step !== '') && (
                        <Text fontSize="sm" color={useColorModeValue('gray.700', 'gray.300')}>
                          Step: {jobDetail.current_step}
                        </Text>
                      )}
                      {(executionStatus === 'running' || executionStatus === 'pending' || executionStatus === 'completed') && (
                        <Box w="100%">
                          <Progress value={jobDetail.progress} size="sm" colorScheme="blue" borderRadius="full" hasStripe={executionStatus === 'running'} isAnimated={executionStatus === 'running'} />
                          <Text fontSize="xs" mt={1} color={useColorModeValue('gray.500', 'gray.400')}>
                            {Math.round(jobDetail.progress)}%
                          </Text>
                        </Box>
                      )}
                      {executionStatus === 'failed' && jobDetail.error_message && (
                        <Alert status="error" size="sm" borderRadius="md">
                          <AlertIcon />
                          <Text fontSize="sm" noOfLines={3}>{jobDetail.error_message}</Text>
                        </Alert>
                      )}
                      <RouterLink to="/jobs">
                        <Link as="span" fontSize="sm" color="blue.600">
                          View all jobs →
                        </Link>
                      </RouterLink>
                    </VStack>
                  ) : (
                    <VStack align="stretch" spacing={2}>
                      <Text fontSize="sm" color={useColorModeValue('gray.600', 'gray.400')}>
                        No migration running. Click Execute to start.
                      </Text>
                      <RouterLink to="/jobs">
                        <Link as="span" fontSize="sm" color="blue.600">
                          View all jobs →
                        </Link>
                      </RouterLink>
                    </VStack>
                  )}
                </Box>
              </Panel>
            )}
          </ReactFlow>
        </Box>

        {/* Node Type Selection Modal for Edge Insertion */}
        {edgeInsertModal && (
          <NodeTypeSelectionModal
            isOpen={edgeInsertModal.isOpen}
            onClose={() => setEdgeInsertModal(null)}
            onSelect={handleNodeTypeSelected}
          />
        )}

        {/* Bottom Panel with Collapse/Resize */}
        {tableDataPanel && !bottomPanelCollapsed && (
          <>
            {/* Resize Handle */}
            <Box
              position="relative"
              h="4px"
              cursor="row-resize"
              bg={useColorModeValue('gray.200', 'gray.600')}
              _hover={{ bg: 'blue.500', opacity: 0.7 }}
              onMouseDown={(e) => {
                e.preventDefault()
                setIsResizing('bottom')
              }}
              zIndex={10}
              display="flex"
              alignItems="center"
              justifyContent="center"
            >
              <Box
                position="absolute"
                left="50%"
                top="50%"
                transform="translate(-50%, -50%)"
                zIndex={20}
                onClick={(e) => {
                  e.stopPropagation()
                  setBottomPanelCollapsed(true)
                }}
                cursor="pointer"
                p={1}
                borderRadius="sm"
                _hover={{ bg: 'gray.300' }}
              >
                <ChevronDown size={12} />
              </Box>
            </Box>
            <Box
              h={`${bottomPanelHeight}px`}
              borderTopWidth="1px"
              borderColor={useColorModeValue('gray.200', 'gray.700')}
              position="relative"
              flexShrink={0}
              bg={useColorModeValue('white', 'gray.800')}
              overflow="hidden"
              minH="100px"
              maxH="80vh"
            >
              <TableDataPanel
                sourceId={tableDataPanel.sourceId}
                tableName={tableDataPanel.tableName}
                schema={tableDataPanel.schema}
                nodeId={tableDataPanel.nodeId}
                nodes={storeNodes}
                edges={storeEdges}
                directFilterConditions={tableDataPanel.directFilterConditions}
                canvasId={canvasId}
                onClose={() => {
                  setTableDataPanel(null)
                  setDirectFilterMode(null)
                }}
              />
            </Box>
          </>
        )}
        {tableDataPanel && bottomPanelCollapsed && (
          <Box
            position="absolute"
            bottom={0}
            left="50%"
            transform="translateX(-50%)"
            zIndex={20}
            mb={2}
          >
            <IconButton
              aria-label="Expand bottom panel"
              icon={<ChevronUp size={16} />}
              size="sm"
              variant="solid"
              colorScheme="gray"
              onClick={() => {
                setBottomPanelCollapsed(false)
                setBottomPanelHeight(200) // Reduced default height
              }}
            />
          </Box>
        )}
      </Box>

      {/* Right Panel with Collapse/Resize - Dynamic based on selection - Responsive */}
      {!rightPanelCollapsed ? (
        <Box
          w={`${rightPanelWidth}px`}
          minW="200px"
          maxW="50vw"
          h="100vh"
          position="relative"
          borderLeftWidth="1px"
          borderColor={useColorModeValue('gray.200', 'gray.700')}
          display="flex"
          flexDirection="column"
          bg={useColorModeValue('white', 'gray.800')}
          overflow="hidden"
          className="right-sidebar"
          flexShrink={0}
        >
          {/* Collapse Button */}
          <Box position="absolute" left={-12} top="50%" transform="translateY(-50%)" zIndex={20}>
            <IconButton
              aria-label="Collapse right panel"
              icon={<ChevronRight size={16} />}
              size="xs"
              variant="solid"
              colorScheme="gray"
              onClick={() => setRightPanelCollapsed(true)}
            />
          </Box>
          <Box flex={1} minH={0} overflowY="auto" overflowX="hidden">
            {directFilterMode ? (
              <FilterConfigPanel
                node={null}
                nodes={storeNodes}
                edges={storeEdges}
                directFilterMode={directFilterMode}
                existingFilter={getTableFilter(directFilterMode.sourceId, directFilterMode.tableName, directFilterMode.schema)}
                onUpdate={() => {
                  // No-op for direct filter mode (no node to update)
                }}
                onFilterSaved={(_nodeId, config) => {
                  // In direct filter mode, save filters to state and localStorage
                  if (directFilterMode) {
                    // Update in-memory state
                    setTableFilter(
                      directFilterMode.sourceId,
                      directFilterMode.tableName,
                      directFilterMode.schema,
                      {
                        conditions: config.conditions || [],
                        expression: config.expression || '',
                        mode: config.mode || 'builder',
                      }
                    )

                    // Also save to localStorage for persistence across sessions
                    const filterKey = `table_filter_${directFilterMode.sourceId}_${directFilterMode.tableName}_${directFilterMode.schema || 'default'}`
                    try {
                      localStorage.setItem(filterKey, JSON.stringify({
                        conditions: config.conditions || [],
                        expression: config.expression || '',
                        mode: config.mode || 'builder',
                      }))
                    } catch (err) {
                      console.warn('Failed to save filter to localStorage:', err)
                    }

                    // Update preview with filtered data
                    setTableDataPanel({
                      sourceId: directFilterMode.sourceId,
                      tableName: directFilterMode.tableName,
                      schema: directFilterMode.schema,
                      directFilterConditions: config.conditions || [],
                    })
                  }
                }}
                onDirectFilterPreview={(conditions) => {
                  // Execute filter directly and update preview
                  if (directFilterMode) {
                    setTableDataPanel({
                      sourceId: directFilterMode.sourceId,
                      tableName: directFilterMode.tableName,
                      schema: directFilterMode.schema,
                      directFilterConditions: conditions,
                    })
                    // Expand bottom panel if collapsed
                    if (bottomPanelCollapsed) {
                      setBottomPanelCollapsed(false)
                    }
                  }
                }}
                onCloseDirectFilter={() => {
                  setDirectFilterMode(null)
                  setTableDataPanel(null)
                }}
                onClearDirectFilter={() => {
                  if (directFilterMode) {
                    removeTableFilter(directFilterMode.sourceId, directFilterMode.tableName, directFilterMode.schema)
                    // Clear from localStorage
                    const filterKey = `table_filter_${directFilterMode.sourceId}_${directFilterMode.tableName}_${directFilterMode.schema || 'default'}`
                    try {
                      localStorage.removeItem(filterKey)
                    } catch (err) {
                      console.warn('Failed to remove filter from localStorage:', err)
                    }
                    // Update table data panel to remove filter
                    setTableDataPanel({
                      sourceId: directFilterMode.sourceId,
                      tableName: directFilterMode.tableName,
                      schema: directFilterMode.schema,
                      directFilterConditions: [],
                    })
                  }
                }}
              />
            ) : selectedNode && selectedNode.data.type === 'source' && selectedNode.data.config?.sourceId && selectedNode.data.config?.tableName ? (
              // Check if source node has embedded filter conditions
              selectedNode.data.config?.isFiltered || (selectedNode.data.config?.conditions && selectedNode.data.config.conditions.length > 0) ? (
                // Show filter configuration panel for filtered source nodes
                <FilterConfigPanel
                  node={selectedNode}
                  nodes={storeNodes}
                  edges={storeEdges}
                  onUpdate={(nodeId, updateData) => {
                    setIsDirty(true)
                    const { config: configOverrides, business_name: bn, node_name: nn, label: lb, output_metadata: om, ...configRest } = updateData || {}
                    setNodes(useCanvasStore.getState().nodes.map((n) =>
                      n.id === nodeId
                        ? {
                          ...n,
                          data: {
                            ...n.data,
                            ...(bn !== undefined && { business_name: bn, node_name: nn ?? bn, label: lb ?? bn }),
                            ...(om !== undefined && { output_metadata: om }),
                            config: {
                              ...n.data.config,
                              ...(typeof configOverrides === 'object' && configOverrides ? configOverrides : {}),
                              ...configRest,
                              sourceId: n.data.config?.sourceId,
                              tableName: n.data.config?.tableName,
                              schema: n.data.config?.schema,
                              isFiltered: (configRest.conditions?.length > 0 || (configRest.expression && String(configRest.expression).trim())) || (configOverrides?.conditions?.length > 0 || (configOverrides?.expression && String(configOverrides.expression).trim())) || n.data.config?.isFiltered,
                            },
                          },
                        }
                        : n
                    ))
                  }}
                  onFilterSaved={(nodeId, config) => {
                    // Update source node with new filter conditions
                    const sourceNode = storeNodes.find((n) => n.id === nodeId)
                    if (sourceNode) {
                      const hasFilter = (config.conditions && config.conditions.length > 0) || (config.expression && config.expression.trim())

                      setNodes(useCanvasStore.getState().nodes.map((n) =>
                        n.id === nodeId
                          ? {
                            ...n,
                            data: {
                              ...n.data,
                              label: `${n.data.config?.tableName || n.data.label}${n.data.config?.schema ? ` (${n.data.config.schema})` : ''}`,
                              config: {
                                ...n.data.config,
                                ...config,
                                sourceId: n.data.config?.sourceId,
                                tableName: n.data.config?.tableName,
                                schema: n.data.config?.schema,
                                isFiltered: hasFilter,
                                // Remove filter properties if filter is cleared
                                ...(hasFilter ? {} : { conditions: [], expression: '', mode: 'builder' }),
                              },
                            },
                          }
                          : n
                      ))

                      // Update table filter state
                      if (sourceNode.data.config?.sourceId && sourceNode.data.config?.tableName) {
                        if (hasFilter) {
                          setTableFilter(
                            sourceNode.data.config.sourceId,
                            sourceNode.data.config.tableName,
                            sourceNode.data.config.schema,
                            {
                              conditions: config.conditions || [],
                              expression: config.expression || '',
                              mode: config.mode || 'builder',
                            }
                          )
                        } else {
                          // Clear filter if conditions are empty
                          removeTableFilter(sourceNode.data.config.sourceId, sourceNode.data.config.tableName, sourceNode.data.config.schema)
                        }
                      }

                      // Update preview - show filtered data if filter exists, otherwise show unfiltered
                      if (hasFilter) {
                        setTableDataPanel({
                          nodeId: nodeId,
                          sourceId: sourceNode.data.config?.sourceId,
                          tableName: sourceNode.data.config?.tableName,
                          schema: sourceNode.data.config?.schema,
                          directFilterConditions: config.conditions || [],
                        })
                      } else {
                        setTableDataPanel({
                          nodeId: nodeId,
                          sourceId: sourceNode.data.config?.sourceId,
                          tableName: sourceNode.data.config?.tableName,
                          schema: sourceNode.data.config?.schema,
                        })
                      }
                    }
                  }}
                />
              ) : (
                // Show Business Name + column properties for unfiltered source nodes
                <VStack align="stretch" spacing={4}>
                  <Box>
                    <Text fontSize="xs" fontWeight="medium" mb={1} color={useColorModeValue('gray.600', 'gray.400')}>
                      Business Name
                    </Text>
                    <Input
                      size="sm"
                      value={(() => {
                        const n = (storeNodes || []).find((x) => x.id === selectedNode?.id) ?? selectedNode
                        return n?.data?.business_name || n?.data?.node_name || n?.data?.label || 'Source Table'
                      })()}
                      onChange={(e) => {
                        const val = e.target.value
                        setIsDirty(true)
                        // Use store as source of truth (avoids stale nds for inserted nodes)
                        const currentNodes = useCanvasStore.getState().nodes
                        const updated = currentNodes.map((n) =>
                          n.id === selectedNode.id
                            ? { ...n, data: { ...n.data, business_name: val, node_name: val, label: val } }
                            : n
                        )
                        setNodes(updated)
                      }}
                      placeholder="e.g., Customer Data"
                    />
                  </Box>
                  <ColumnPropertiesPanel
                    sourceId={selectedNode.data.config.sourceId}
                    tableName={selectedNode.data.config.tableName}
                    schema={selectedNode.data.config.schema}
                  />
                </VStack>
              )
            ) : selectedNode && selectedNode.data.type === 'filter' ? (
              <FilterConfigPanel
                node={storeNodes.find((n) => n.id === selectedNode?.id) ?? selectedNode}
                nodes={storeNodes}
                edges={storeEdges}
                onUpdate={(nodeId, updateData) => {
                  setIsDirty(true)
                  const currentNodes = useCanvasStore.getState().nodes
                  const hasSchemaChange = !!updateData.output_metadata?.columns?.length

                  const withUpdate = currentNodes.map((n) =>
                    n.id === nodeId
                      ? {
                          ...n,
                          data: {
                            ...n.data,
                            config: updateData.config || updateData,
                            business_name:
                              updateData.business_name || n.data.business_name || n.data.node_name || n.data.label,
                            technical_name: updateData.technical_name || n.data.technical_name,
                            node_name:
                              updateData.node_name ||
                              updateData.business_name ||
                              n.data.node_name ||
                              n.data.label,
                            output_metadata:
                              updateData.output_metadata !== undefined
                                ? updateData.output_metadata
                                : n.data.output_metadata,
                            label:
                              updateData.business_name ||
                              updateData.node_name ||
                              updateData.displayName ||
                              n.data.label,
                            schema_version:
                              hasSchemaChange && typeof (n.data as any).schema_version === 'number'
                                ? (n.data as any).schema_version + 1
                                : hasSchemaChange
                                  ? 1
                                  : (n.data as any).schema_version,
                          },
                        }
                      : n
                  )

                  const updated = hasSchemaChange && updateData.output_metadata
                    ? updateImmediateDownstreamInputs(withUpdate, storeEdges, nodeId, updateData.output_metadata)
                    : withUpdate

                  setNodes(updated)
                }}
                onFilterSaved={(nodeId, config) => {
                  // Auto-select the filter node and update preview when filter is saved
                  const filterNode = storeNodes && Array.isArray(storeNodes) ? storeNodes.find((n) => n.id === nodeId) : null
                  if (filterNode) {
                    setSelectedNode(filterNode)

                    // Get source info for the filter node
                    let sourceConfig = null
                    if (config.sourceId && config.tableName) {
                      sourceConfig = {
                        sourceId: config.sourceId,
                        tableName: config.tableName,
                        schema: config.schema,
                      }
                    } else {
                      // Fallback: Get source info from connected input node
                      const inputNodeIds = filterNode.data.input_nodes || []
                      let inputNode = null

                      if (inputNodeIds.length > 0 && storeNodes && Array.isArray(storeNodes)) {
                        inputNode = storeNodes.find((n) => n.id === inputNodeIds[0])
                      } else {
                        const inputEdge = storeEdges && Array.isArray(storeEdges) ? storeEdges.find((e: Edge) => e.target === nodeId) : null
                        if (inputEdge && storeNodes && Array.isArray(storeNodes)) {
                          inputNode = storeNodes.find((n) => n.id === inputEdge.source)
                        }
                      }

                      if (inputNode) {
                        if (inputNode.data.type === 'source' && inputNode.data.config) {
                          sourceConfig = {
                            sourceId: inputNode.data.config.sourceId,
                            tableName: inputNode.data.config.tableName,
                            schema: inputNode.data.config.schema,
                          }
                        } else if (inputNode.data.config?.sourceId && inputNode.data.config?.tableName) {
                          sourceConfig = {
                            sourceId: inputNode.data.config.sourceId,
                            tableName: inputNode.data.config.tableName,
                            schema: inputNode.data.config.schema,
                          }
                        }
                      }
                    }

                    // Update preview panel to show filtered results
                    // Pass conditions so preview shows filtered data, not raw data
                    if (sourceConfig) {
                      setTableDataPanel({
                        nodeId: nodeId,
                        sourceId: sourceConfig.sourceId,
                        tableName: sourceConfig.tableName,
                        schema: sourceConfig.schema,
                        // Pass conditions to ensure preview shows filtered data
                        directFilterConditions: config.conditions || [],
                      })
                    } else {
                      setTableDataPanel({
                        nodeId: nodeId,
                      })
                    }

                    // Expand bottom panel if collapsed
                    if (bottomPanelCollapsed) {
                      setBottomPanelCollapsed(false)
                    }
                  }
                }}
              />
            ) : selectedNode && selectedNode.data.type === 'join' ? (
              <JoinConfigPanel
                node={storeNodes.find((n) => n.id === selectedNode?.id) ?? selectedNode}
                nodes={storeNodes}
                edges={storeEdges}
                onUpdate={(nodeId, updateData) => {
                  setIsDirty(true)
                  const currentNodes = useCanvasStore.getState().nodes
                  const nodeBefore = currentNodes.find((n) => n.id === nodeId)
                  const prevCols = (nodeBefore?.data?.output_metadata as any)?.columns ?? []

                  // Merge config, derive output_metadata from outputColumns
                  const newConfig = updateData.config
                    ? { ...(nodeBefore?.data?.config || {}), ...updateData.config }
                    : (nodeBefore?.data?.config || {})

                  let derivedOutputMeta: any = nodeBefore?.data?.output_metadata
                  const outputColsCfg: any[] | undefined = newConfig.outputColumns
                  if (outputColsCfg && outputColsCfg.length > 0) {
                    const includedCols = outputColsCfg
                      .filter((c: any) => c.included !== false)
                      .map((col: any) => ({
                        name: col.outputName || col.column || col.name,
                        column: col.column || col.name,
                        datatype: col.datatype || col.data_type || col.type || 'TEXT',
                        source: col.source,
                        nullable: col.nullable !== undefined ? col.nullable : true,
                      }))
                    derivedOutputMeta = {
                      ...(nodeBefore?.data?.output_metadata || {}),
                      columns: includedCols,
                    }
                  }

                  const nextCols = derivedOutputMeta?.columns ?? prevCols
                  const { removed, added } = diffColumns(prevCols, nextCols)

                  const withUpdate = currentNodes.map((n) =>
                    n.id === nodeId
                      ? {
                          ...n,
                          data: {
                            ...n.data,
                            config: newConfig,
                            business_name: updateData.business_name !== undefined ? updateData.business_name : (n.data.business_name || n.data.node_name || n.data.label),
                            technical_name: updateData.technical_name !== undefined ? updateData.technical_name : n.data.technical_name,
                            node_name: updateData.node_name !== undefined ? updateData.node_name : (updateData.business_name || n.data.node_name || n.data.label),
                            label: updateData.business_name || updateData.node_name || updateData.displayName || n.data.label,
                            output_metadata: derivedOutputMeta,
                            errors: undefined,
                          },
                        }
                      : n
                  )

                  let updated = withUpdate
                  if (removed.length) {
                    updated = propagateRemovedColumnsHard(updated, storeEdges, nodeId, removed)
                  }
                  if (added.length && derivedOutputMeta?.columns?.length) {
                    const addedColumnObjects = (derivedOutputMeta.columns as any[]).filter(
                      (col: any) => added.includes(getColKey(col))
                    )
                    if (addedColumnObjects.length) {
                      updated = addColumnsToImmediateDownstreamInputs(updated, storeEdges, nodeId, addedColumnObjects)
                    }
                  }

                  setNodes(updated)
                }}
              />
            ) : selectedNode && selectedNode.data.type === 'projection' ? (
              <ProjectionConfigPanel
                node={storeNodes.find((n) => n.id === selectedNode?.id) ?? selectedNode}
                onLineageHighlight={setLineageHighlight}
                onPropagateDownstream={
                  selectedNode
                    ? () => handleContextMenuAction('propagate-downstream', selectedNode)
                    : undefined
                }
                onUpdate={(nodeId, updateData) => {
                  setIsDirty(true)
                  const currentNodes = useCanvasStore.getState().nodes
                  const nodeBefore = currentNodes.find((n) => n.id === nodeId)
                  const prevCols = (nodeBefore?.data?.output_metadata as any)?.columns ?? []
                  const nextCols = (updateData.output_metadata as any)?.columns ?? prevCols
                  const { removed, added } = diffColumns(prevCols, nextCols)

                  const withUpdate = currentNodes.map((n) =>
                    n.id === nodeId
                      ? {
                          ...n,
                          data: {
                            ...n.data,
                            config: updateData.config ? { ...n.data.config, ...updateData.config } : (updateData.config === undefined ? n.data.config : updateData),
                            business_name: updateData.business_name !== undefined ? updateData.business_name : (n.data.business_name || n.data.node_name || n.data.label),
                            technical_name: updateData.technical_name !== undefined ? updateData.technical_name : n.data.technical_name,
                            node_name: updateData.node_name !== undefined ? updateData.node_name : (updateData.business_name || n.data.node_name || n.data.label),
                            output_metadata: updateData.output_metadata !== undefined ? updateData.output_metadata : n.data.output_metadata,
                            label: updateData.business_name || updateData.node_name || updateData.displayName || n.data.label,
                            schema_version: (nextCols.length !== prevCols.length || removed.length || added.length) && typeof (n.data as any).schema_version === 'number'
                              ? (n.data as any).schema_version + 1
                              : (n.data as any).schema_version,
                            errors: undefined,
                          },
                        }
                      : n
                  )

                  let updated = withUpdate
                  if (removed.length) {
                    updated = propagateRemovedColumnsHard(updated, storeEdges, nodeId, removed)
                  }
                  if (added.length && updateData.output_metadata?.columns?.length) {
                    const allNewCols = (updateData.output_metadata.columns as any[]) ?? []
                    const addedColumnObjects = allNewCols.filter((col) => added.includes(getColKey(col)))
                    if (addedColumnObjects.length) {
                      updated = addColumnsToImmediateDownstreamInputs(updated, storeEdges, nodeId, addedColumnObjects)
                    }
                  }
                  setNodes(updated)
                }}
              />
            ) : selectedNode && selectedNode.data.type === 'calculated' ? (
              <CalculatedColumnConfigPanel
                node={storeNodes.find((n) => n.id === selectedNode?.id) ?? selectedNode}
                nodes={storeNodes}
                edges={storeEdges}
                onUpdate={(nodeId, config) => {
                  setIsDirty(true)
                  const currentNodes = useCanvasStore.getState().nodes
                  const updated = currentNodes.map((n) =>
                    n.id === nodeId ? { ...n, data: { ...n.data, config } } : n
                  )
                  setNodes(updated)
                }}
              />
            ) : selectedNode && selectedNode.data.type === 'compute' ? (
              <ComputeNodeConfigPanel
                node={storeNodes.find((n) => n.id === selectedNode?.id) ?? selectedNode}
                nodes={storeNodes}
                edges={storeEdges}
                onUpdate={(nodeId, updateData) => {
                  setIsDirty(true)
                  const currentNodes = useCanvasStore.getState().nodes
                  const hasSchemaChange = !!updateData.output_metadata?.columns?.length

                  const withUpdate = currentNodes.map((n) =>
                    n.id === nodeId
                      ? {
                          ...n,
                          data: {
                            ...n.data,
                            config: updateData.config,
                            output_metadata: updateData.output_metadata,
                            business_name: updateData.business_name || n.data.business_name,
                            schema_version:
                              hasSchemaChange && typeof (n.data as any).schema_version === 'number'
                                ? (n.data as any).schema_version + 1
                                : hasSchemaChange
                                  ? 1
                                  : (n.data as any).schema_version,
                          },
                        }
                      : n
                  )

                  const updated = hasSchemaChange && updateData.output_metadata
                    ? updateImmediateDownstreamInputs(withUpdate, storeEdges, nodeId, updateData.output_metadata)
                    : withUpdate

                  setNodes(updated)
                }}
              />
            ) : selectedNode && (selectedNode.data.type === 'destination' || (selectedNode.data.type && String(selectedNode.data.type).startsWith('destination-'))) ? (
              <DestinationConfigPanel
                node={storeNodes?.find((n) => n.id === selectedNode?.id) ?? selectedNode}
                projectId={projectId}
                onUpdate={(nodeId, updateData) => {
                  setIsDirty(true)
                  const currentNodes = useCanvasStore.getState().nodes
                  const updated = currentNodes.map((n) =>
                    n.id === nodeId
                      ? {
                          ...n,
                          data: {
                            ...n.data,
                            // Merge config: spread existing THEN apply overrides so
                            // business-name-only calls (no config key) keep the config
                            // intact, and config-only calls don't clobber other fields.
                            config: updateData.config !== undefined
                              ? { ...n.data.config, ...updateData.config }
                              : n.data.config,
                            business_name:
                              updateData.business_name !== undefined
                                ? updateData.business_name
                                : (n.data.business_name || n.data.node_name || n.data.label),
                            node_name:
                              updateData.node_name !== undefined
                                ? updateData.node_name
                                : (n.data.node_name || n.data.label),
                            label:
                              updateData.label !== undefined
                                ? updateData.label
                                : (n.data.business_name || n.data.node_name || n.data.label),
                          },
                        }
                      : n
                  )
                  setNodes(updated)
                }}
              />
            ) : selectedNode && selectedNode.data.type === 'aggregate' ? (
              <AggregatesConfigPanel
                node={storeNodes.find((n) => n.id === selectedNode?.id) ?? selectedNode}
                nodes={storeNodes}
                edges={storeEdges}
                onUpdate={(nodeId, updateData) => {
                  setIsDirty(true)
                  const currentNodes = useCanvasStore.getState().nodes
                  const hasSchemaChange = !!updateData.output_metadata?.columns?.length

                  const withUpdate = currentNodes.map((n) =>
                    n.id === nodeId
                      ? {
                          ...n,
                          data: {
                            ...n.data,
                            config: updateData.config,
                            output_metadata: updateData.output_metadata,
                            business_name: updateData.business_name || n.data.business_name,
                            schema_version:
                              hasSchemaChange && typeof (n.data as any).schema_version === 'number'
                                ? (n.data as any).schema_version + 1
                                : hasSchemaChange
                                  ? 1
                                  : (n.data as any).schema_version,
                          },
                        }
                      : n
                  )

                  const updated = hasSchemaChange && updateData.output_metadata
                    ? updateImmediateDownstreamInputs(withUpdate, storeEdges, nodeId, updateData.output_metadata)
                    : withUpdate

                  setNodes(updated)
                }}
              />
            ) : selectedSource ? (
              <SourceDetailsPanel source={selectedSource} />
            ) : (
              <Box
                w="100%"
                h="100%"
                bg={useColorModeValue('white', 'gray.800')}
                display="flex"
                alignItems="center"
                justifyContent="center"
              >
                <Text fontSize="sm" color={useColorModeValue('gray.500', 'gray.400')}>
                  Select a source or table node to view details
                </Text>
              </Box>
            )}
          </Box>
          {/* Resize Handle */}
          <Box
            position="absolute"
            left={0}
            top={0}
            w="4px"
            h="100%"
            cursor="col-resize"
            bg="transparent"
            _hover={{ bg: 'blue.500', opacity: 0.7 }}
            onMouseDown={(e) => {
              e.preventDefault()
              setIsResizing('right')
            }}
            zIndex={10}
            userSelect="none"
          />
        </Box>
      ) : (
        <Box position="relative" w="0" borderLeftWidth="1px" borderColor={useColorModeValue('gray.200', 'gray.700')}>
          <Box position="absolute" right={-12} top="50%" transform="translateY(-50%)" zIndex={20}>
            <IconButton
              aria-label="Expand right panel"
              icon={<ChevronLeft size={16} />}
              size="xs"
              variant="solid"
              colorScheme="gray"
              onClick={() => {
                setRightPanelCollapsed(false)
                setRightPanelWidth(400)
              }}
            />
          </Box>
        </Box>
      )}

      {/* Configuration Panel */}
      <NodeConfigurationPanel
        node={selectedNode}
        isOpen={isConfigOpen}
        onClose={() => {
          onConfigClose()
          setSelectedNode(null)
        }}
      />

      {/* Column Definitions Menu */}
      {columnMenu && (
        <ColumnDefinitionsMenu
          sourceId={columnMenu.sourceId}
          tableName={columnMenu.tableName}
          schema={columnMenu.schema}
          isOpen={!!columnMenu}
          onClose={() => setColumnMenu(null)}
          position={columnMenu.position}
        />
      )}

      {/* Node Context Menu */}
      {contextMenu && (
        <NodeContextMenu
          node={contextMenu.node}
          nodes={storeNodes}
          edges={storeEdges}
          position={contextMenu.position}
          isOpen={!!contextMenu}
          onClose={() => setContextMenu(null)}
          onAction={handleContextMenuAction}
          hasMultipleFlows={hasMultipleFlows}
        />
      )}

      {/* Edge Context Menu */}
      {edgeContextMenu && (
        <EdgeContextMenu
          edge={edgeContextMenu.edge}
          position={edgeContextMenu.position}
          isOpen={!!edgeContextMenu}
          onClose={() => setEdgeContextMenu(null)}
          onInsertNode={handleEdgeInsertNode}
        />
      )}

      {/* Destination Selector Modal */}
      {destinationSelectorModal && (
        <DestinationSelectorModal
          isOpen={destinationSelectorModal.isOpen}
          onClose={() => setDestinationSelectorModal(null)}
          onSelect={handleDestinationSelect}
          onSelectCustomerDatabase={handleCustomerDatabaseSelect}
          projectId={projectId ?? undefined}
          onCreateNew={() => {
            // Close the destination selector modal
            setDestinationSelectorModal(null)
            // Navigate to dashboard where destinations can be created
            // Or show a toast with instructions
            toast({
              title: 'Create Destination',
              description: 'Please create a destination from the Dashboard or Project page first, then add it to the pipeline.',
              status: 'info',
              duration: 5000,
              isClosable: true,
            })
            // Optionally navigate to dashboard
            // navigate('/dashboard')
          }}
        />
      )}

      {/* Save Canvas Modal */}
      <Modal isOpen={isSaveModalOpen} onClose={onCloseSaveModal}>
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Save Canvas</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <FormControl isRequired>
              <FormLabel>Canvas Name</FormLabel>
              <Input
                value={canvasName}
                onChange={(e) => setCanvasName(e.target.value)}
                placeholder="Enter canvas name"
                autoFocus
              />
            </FormControl>
          </ModalBody>
          <ModalFooter>
            <Button variant="ghost" mr={3} onClick={onCloseSaveModal} isDisabled={saveLoading}>
              Cancel
            </Button>
            <Button
              colorScheme="blue"
              isLoading={saveLoading}
              loadingText="Saving…"
              onClick={async () => {
                if (!canvasName?.trim()) return
                await handleSaveCanvas(canvasId || null, canvasName)
                onCloseSaveModal()
              }}
              isDisabled={!canvasName || canvasName.trim() === ''}
            >
              {canvasId ? 'Save' : 'Create'}
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>

      {/* Join Node Selection Modal */}
      <Modal isOpen={isJoinNodeSelectOpen} onClose={onCloseJoinNodeSelect}>
        <ModalOverlay />
        <ModalContent maxW="600px">
          <ModalHeader>Select Second Node for Join</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <Text mb={4} fontSize="sm" color={useColorModeValue('gray.600', 'gray.400')}>
              Select the second node to join with <strong>{joinNodeSource?.data?.business_name || joinNodeSource?.data?.label || joinNodeSource?.id}</strong>
            </Text>
            <VStack align="stretch" spacing={2} maxH="400px" overflowY="auto">
              {storeNodes && Array.isArray(storeNodes) && storeNodes
                .filter((n) => {
                  const t = (n.data?.type && String(n.data.type).toLowerCase()) || ''
                  return (
                    n.id !== joinNodeSource?.id &&
                    t !== 'destination' &&
                    t !== 'join' // Prevent joining to existing join nodes to avoid complexity
                  )
                })
                .map((node) => (
                  <Box
                    key={node.id}
                    as="button"
                    p={3}
                    borderWidth="1px"
                    borderRadius="md"
                    borderColor={useColorModeValue('gray.200', 'gray.600')}
                    bg={useColorModeValue('white', 'gray.700')}
                    _hover={{
                      bg: useColorModeValue('gray.50', 'gray.600'),
                      borderColor: useColorModeValue('blue.300', 'blue.500'),
                    }}
                    onClick={() => handleJoinNodeSelected(node)}
                    textAlign="left"
                    transition="all 0.2s"
                  >
                    <HStack justify="space-between">
                      <VStack align="start" spacing={1}>
                        <Text fontWeight="medium" fontSize="sm">
                          {node.data?.business_name || node.data?.label || node.id}
                        </Text>
                        <Text fontSize="xs" color={useColorModeValue('gray.500', 'gray.400')}>
                          Type: {node.data?.type || 'unknown'}
                        </Text>
                      </VStack>
                    </HStack>
                  </Box>
                ))}
              {(!storeNodes || storeNodes.length === 0 || storeNodes.filter((n) => { const t = (n.data?.type && String(n.data.type).toLowerCase()) || ''; return n.id !== joinNodeSource?.id && t !== 'destination' && t !== 'join'; }).length === 0) && (
                <Text fontSize="sm" color={useColorModeValue('gray.500', 'gray.400')} textAlign="center" py={4}>
                  No available nodes to join with. Add more nodes to the canvas first.
                </Text>
              )}
            </VStack>
          </ModalBody>
          <ModalFooter>
            <Button variant="ghost" onClick={onCloseJoinNodeSelect}>
              Cancel
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </Box>
  )
}

