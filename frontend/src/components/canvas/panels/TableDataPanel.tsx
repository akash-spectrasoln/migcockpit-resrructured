/**
 * Table Data Panel Component
 * Displays table data in a scrollable bottom panel
 */
import React, { useState, useEffect, useRef } from 'react'
import {
  Box,
  VStack,
  HStack,
  Text,
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  TableContainer,
  Spinner,
  Alert,
  AlertIcon,
  IconButton,
  useColorModeValue,
  Badge,
  Tooltip,
} from '@chakra-ui/react'
import { X, RefreshCw, Database } from 'lucide-react'

interface TableDataPanelProps {
  sourceId?: number
  tableName?: string
  schema?: string
  nodeId?: string
  nodes?: any[]
  edges?: any[]
  directFilterConditions?: any[] // Conditions for direct filter mode
  canvasId?: number // Required for preview cache
  onClose: () => void
}

interface TableRow {
  [key: string]: any
}

export const TableDataPanel: React.FC<TableDataPanelProps> = ({
  sourceId,
  tableName,
  schema,
  nodeId,
  nodes,
  edges,
  directFilterConditions,
  canvasId,
  onClose,
}) => {
  const [data, setData] = useState<TableRow[]>([])
  const [columns, setColumns] = useState<string[]>([])
  const [columnLineage, setColumnLineage] = useState<Record<string, { origin_type?: string; source_table?: string; origin_branch?: string; is_calculated?: boolean; expression?: string; source_table_left?: string; source_table_right?: string }>>({})
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [page, setPage] = useState(1)
  const [hasMore, setHasMore] = useState(true)
  const pageSize = 50

  // Format lineage for display (tooltip)
  const getLineageLabel = (colName: string): string => {
    const L = columnLineage[colName]
    if (!L) return ''
    if (L.is_calculated) return L.expression ? `Calculated: ${L.expression}` : 'Calculated column'
    if (L.source_table) {
      const side = L.origin_branch ? ` (${L.origin_branch})` : ''
      return `From: ${L.source_table}${side}`
    }
    if (L.origin_type === 'JOIN' && (L.source_table_left || L.source_table_right)) {
      const parts = []
      if (L.source_table_left) parts.push(`Left: ${L.source_table_left}`)
      if (L.source_table_right) parts.push(`Right: ${L.source_table_right}`)
      return parts.length ? parts.join(' · ') : ''
    }
    if (L.origin_type) return `Origin: ${L.origin_type}`
    return ''
  }

  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const headerBg = useColorModeValue('gray.50', 'gray.700')

  // Helper function to normalize columns (handle both string arrays and object arrays)
  const normalizeColumns = (cols: any[]): string[] => {
    if (!cols || !Array.isArray(cols)) return []
    return cols.map((col: any) =>
      typeof col === 'string' ? col : (col?.name || col?.column_name || String(col))
    )
  }

  // Helper function to ensure rows have data in the exact order of columns
  // This is critical for projection nodes where column order matters
  const orderRowsByColumns = (rows: any[], columnOrder: string[]): any[] => {
    if (!rows || !Array.isArray(rows) || !columnOrder || !Array.isArray(columnOrder)) {
      return rows || []
    }
    return rows.map((row: any) => {
      // Create a new object with keys in the exact order of columns
      const orderedRow: any = {}
      for (const col of columnOrder) {
        orderedRow[col] = row[col] !== undefined ? row[col] : null
      }
      return orderedRow
    })
  }

  const executePipelineQuery = async (forceRefresh: boolean = false, targetNodeIdForApi?: string) => {
    const effectiveTargetNodeId = targetNodeIdForApi ?? nodeId
    if (!effectiveTargetNodeId || !nodes || !edges) return

    const requestContext = previewContextRef.current
    setLoading(true)
    setError(null)
    try {
      const { pipelineApi } = await import('../../../services/api')

      const payload: Record<string, unknown> = {
        nodes: nodes && Array.isArray(nodes) ? nodes.map((n: any) => {
          // Ensure type is in data for backend (backend expects node.data.type)
          const nodeType = n.data?.type || n.type || 'unknown'

          // For projection nodes, ensure column metadata is included
          // For join nodes, ensure join config is properly included
          let nodeData = { ...n.data, type: nodeType }
          if (nodeType === 'join') {
            // Ensure join node config is properly formatted for backend
            const config = n.data?.config || {}
            nodeData = {
              ...nodeData,
              config: {
                ...config,
                joinType: config.joinType || 'INNER',
                conditions: config.conditions || [],
                leftNodeId: config.leftNodeId,
                rightNodeId: config.rightNodeId,
                leftTable: config.leftTable,
                rightTable: config.rightTable,
              },
            }
            console.log('[TableDataPanel] Preparing join node for execution:', {
              nodeId: n.id,
              joinType: config.joinType,
              conditionsCount: (config.conditions || []).length,
              leftNodeId: config.leftNodeId,
              rightNodeId: config.rightNodeId,
            })
          } else if (nodeType === 'projection') {
            const config = n.data?.config || {}
            const columnOrder = config.columnOrder || []
            const calculatedColumns = config.calculatedColumns || []

            // DEBUG: Log calculated columns being sent
            if (calculatedColumns.length > 0) {
              console.log('[TableDataPanel] Sending calculated columns:', calculatedColumns.map((cc: any) => ({ name: cc.name, expression: cc.expression })))
            }

            // Build projection metadata with columns array
            if (columnOrder.length > 0) {
              // Map columnOrder to include calculated column expressions
              const columnsWithExpressions = columnOrder.map((col: any) => {
                const colName = col.name || col
                // Check if this is a calculated column
                const calcCol = calculatedColumns.find((cc: any) => cc.name && cc.name.trim() === colName)
                if (calcCol) {
                  // Remove cursor placeholder (|) before sending to backend
                  const cleanedExpression = (calcCol.expression || '').replace(/\|/g, '').trim()
                  return {
                    name: colName,
                    type: col.type || calcCol.dataType || 'TEXT',
                    included: col.included !== undefined ? col.included : true,
                    order: col.order !== undefined ? col.order : -1,
                    isCalculated: true,
                    expression: cleanedExpression,
                  }
                }
                return {
                  name: colName,
                  type: col.type || col.datatype || 'TEXT',
                  included: col.included !== undefined ? col.included : true,
                  order: col.order !== undefined ? col.order : -1,
                  isCalculated: false,
                }
              })

              nodeData = {
                ...nodeData,
                config: {
                  ...config, // Preserve all config properties
                  calculatedColumns: calculatedColumns.map((cc: any) => ({
                    ...cc,
                    expression: (cc.expression || '').replace(/\|/g, '').trim() // Remove cursor placeholder before sending
                  })), // Ensure calculatedColumns is in config for backend
                },
                projection: {
                  mode: config.excludeMode ? 'EXCLUDE' : (config.selectedMode || 'INCLUDE'),
                  columns: columnsWithExpressions,
                },
                calculatedColumns: calculatedColumns.map((cc: any) => ({
                  ...cc,
                  expression: (cc.expression || '').replace(/\|/g, '').trim() // Remove cursor placeholder before sending
                })), // Also include calculated columns separately for backend
              }
            } else {
              // Fallback: build from legacy format if columnOrder doesn't exist
              const includedCols = config.includedColumns || config.output_columns || config.selectedColumns || []
              const excludedCols = config.excludedColumns || []

              if (includedCols.length > 0 || excludedCols.length > 0 || calculatedColumns.length > 0) {
                // Build columns array from included/excluded lists
                const allColumns: any[] = []

                // Add included columns
                includedCols.forEach((colName: string, index: number) => {
                  // Check if this is a calculated column
                  const calcCol = calculatedColumns.find((cc: any) => cc.name && cc.name.trim() === colName)
                  if (calcCol) {
                    // Remove cursor placeholder (|) before sending to backend
                    const cleanedExpression = (calcCol.expression || '').replace(/\|/g, '').trim()
                    allColumns.push({
                      name: colName,
                      type: calcCol.dataType || 'TEXT',
                      included: true,
                      order: index,
                      isCalculated: true,
                      expression: cleanedExpression,
                    })
                  } else {
                    allColumns.push({
                      name: colName,
                      type: 'TEXT', // Default type if not available
                      included: true,
                      order: index,
                      isCalculated: false,
                    })
                  }
                })

                // Add excluded columns
                excludedCols.forEach((colName: string) => {
                  allColumns.push({
                    name: colName,
                    type: 'TEXT', // Default type if not available
                    included: false,
                    order: -1,
                    isCalculated: false,
                  })
                })

                if (allColumns.length > 0) {
                  nodeData = {
                    ...nodeData,
                    config: {
                      ...config, // Preserve all config properties
                      calculatedColumns: calculatedColumns.map((cc: any) => ({
                        ...cc,
                        expression: (cc.expression || '').replace(/\|/g, '').trim() // Remove cursor placeholder before sending
                      })), // Ensure calculatedColumns is in config for backend
                    },
                    projection: {
                      mode: config.excludeMode ? 'EXCLUDE' : (config.selectedMode || 'INCLUDE'),
                      columns: allColumns,
                    },
                    calculatedColumns: calculatedColumns.map((cc: any) => ({
                      ...cc,
                      expression: (cc.expression || '').replace(/\|/g, '').trim() // Remove cursor placeholder before sending
                    })), // Also include calculated columns separately for backend
                  }
                }
              }
            }
          }

          return {
            id: n.id,
            type: nodeType, // Top level for backward compatibility
            data: nodeData,
          }
        }) : [],
        edges: edges && Array.isArray(edges) ? edges.map((e: any) => ({
          source: e.source,
          target: e.target,
          targetHandle: e.targetHandle,
          sourceHandle: e.sourceHandle,
        })) : [],
        targetNodeId: effectiveTargetNodeId,
        page: 1,
        page_size: pageSize,
        canvasId, // Required for preview cache
        useCache: !forceRefresh,
        forceRefresh,
        previewMode: true,  // Use single-query compilation for preview
      }

      const response = await pipelineApi.execute(payload)

      const {
        rows,
        columns: tableColumns,
        has_more,
        from_cache,
        column_lineage: lineage,
        error: backendError,
      } = response as any
      // Ignore stale response if user switched to another node's preview
      if (requestContext !== previewContextRef.current) return

      // Handle backend error in 200 response body
      if (backendError) {
        setError(backendError)
        setData([])
        setColumns([])
        return
      }

      // Normalize columns to ensure they're strings and preserve order
      let normalizedCols = normalizeColumns(tableColumns)
      if (lineage && typeof lineage === 'object') {
        setColumnLineage(lineage)
      } else {
        setColumnLineage({})
      }
      console.log('[TableDataPanel] Received columns from backend (order matters):', normalizedCols)
      console.log('[TableDataPanel] Column count:', normalizedCols.length)

      // CRITICAL: For projection nodes, reorder columns based on saved projection order
      // This ensures preview matches the UI drag-and-drop order, not backend order
      const selectedNode = nodeId && nodes ? nodes.find((n: any) => n.id === nodeId) : null
      const isProjectionNode = selectedNode?.data?.type === 'projection'

      if (isProjectionNode && selectedNode?.data?.config) {
        const projectionConfig = selectedNode.data.config

        // Try to get column order from new format (columnOrder) or legacy format (output_columns)
        let projectionOrder: string[] = []

        if (projectionConfig.columnOrder && Array.isArray(projectionConfig.columnOrder)) {
          // New format: columnOrder array with { name, included, order }
          const includedCols = projectionConfig.columnOrder
            .filter((col: any) => col.included !== false && col.order >= 0)
            .sort((a: any, b: any) => a.order - b.order)
            .map((col: any) => col.name)
          projectionOrder = includedCols
          console.log('[TableDataPanel] Using columnOrder format:', projectionOrder)
        } else if (projectionConfig.output_columns && Array.isArray(projectionConfig.output_columns)) {
          // Legacy format: output_columns array
          projectionOrder = projectionConfig.output_columns
          console.log('[TableDataPanel] Using output_columns format:', projectionOrder)
        } else if (projectionConfig.includedColumns && Array.isArray(projectionConfig.includedColumns)) {
          // Fallback: includedColumns
          projectionOrder = projectionConfig.includedColumns
          console.log('[TableDataPanel] Using includedColumns format:', projectionOrder)
        }

        // If we have a projection order, reorder columns to match it
        if (projectionOrder.length > 0) {
          // Filter projection order to only include columns that exist in backend response
          const validProjectionOrder = projectionOrder.filter(col => normalizedCols.includes(col))

          // Add any backend columns not in projection order (e.g., calculated columns) at the end
          const missingCols = normalizedCols.filter(col => !validProjectionOrder.includes(col))

          // Final order: projection order first, then missing columns
          normalizedCols = [...validProjectionOrder, ...missingCols]

          console.log('[TableDataPanel] Reordered columns based on projection order:', normalizedCols)
          console.log('[TableDataPanel] Projection order had', validProjectionOrder.length, 'valid columns')
          console.log('[TableDataPanel] Added', missingCols.length, 'columns not in projection order')
        } else {
          console.log('[TableDataPanel] No projection order found, using backend order')
        }
      }

      // Set columns first to establish the order
      setColumns(normalizedCols)

      // CRITICAL: Ensure rows have data in the exact same order as columns
      // This is especially important for projection nodes where column order is user-defined
      const orderedRows = orderRowsByColumns(rows || [], normalizedCols)
      console.log('[TableDataPanel] Ordered rows to match column sequence')

      setData(orderedRows)
      setHasMore(has_more || false)
      setPage(1)

      // Log if result came from cache
      if (from_cache) {
        console.log(`Pipeline results loaded from cache for node ${effectiveTargetNodeId}`)
      }
    } catch (err: any) {
      if (requestContext !== previewContextRef.current) return
      const errorMessage = err.response?.data?.error || err.response?.data?.details || err.message || 'Failed to execute pipeline query'
      setError(errorMessage)
      console.error('Error executing pipeline query:', err)
      console.error('Error response data:', err.response?.data)
      console.error('Request payload:', {
        nodes: nodes && Array.isArray(nodes) ? nodes.map((n: any) => ({
          id: n.id,
          type: n.type || n.data?.type,
          dataType: n.data?.type,
        })) : [],
        edges: edges && Array.isArray(edges) ? edges.length : 0,
        targetNodeId: effectiveTargetNodeId,
      })
    } finally {
      setLoading(false)
    }
  }

  // Use refs to track previous values and prevent infinite loops
  const prevExecutionKeyRef = useRef<string>('')
  const prevNodeIdRef = useRef<string | undefined>(undefined)
  const prevNodesRef = useRef<any[] | undefined>(undefined)
  const isExecutingRef = useRef(false)

  // Current preview context: used to ignore stale responses when user switches preview target quickly.
  // Must be unique per node (nodeId) and per source view (sourceId+tableName+schema) so each node shows only its own preview.
  const previewContextRef = useRef<string>('')
  const previewContext = `${nodeId ?? ''}|${sourceId ?? ''}|${tableName ?? ''}|${schema ?? ''}`
  previewContextRef.current = previewContext

  // When the preview target changes, clear panel state immediately so we never show another node's data,
  // and allow a new request to start (reset executing flag so the main effect can run for the new target).
  const prevPreviewContextRef = useRef<string>('')
  useEffect(() => {
    if (prevPreviewContextRef.current !== previewContext) {
      prevPreviewContextRef.current = previewContext
      isExecutingRef.current = false
      setData([])
      setColumns([])
      setColumnLineage({})
      setError(null)
      setPage(1)
      setHasMore(true)
    }
  }, [previewContext])

  useEffect(() => {
    // Prevent infinite loops by checking if this is a meaningful change
    if (isExecutingRef.current) {
      return // Prevent concurrent executions
    }

    const selectedNode = nodeId && nodes ? nodes.find((n: any) => n.id === nodeId) : null
    const currentConditions = selectedNode?.data?.config?.conditions || []
    const isFilterNode = selectedNode?.data?.type === 'filter'
    const nodeChanged = nodeId !== prevNodeIdRef.current
    // Check if nodes array reference changed (indicates node interaction/click)
    const nodesArrayChanged = nodes !== prevNodesRef.current
    prevNodesRef.current = nodes

    // Create a composite key that includes all relevant data for execution
    // This ensures we execute when any relevant piece changes, including node clicks
    // For projection nodes, include columnOrder or output_columns order to detect reordering
    let projectionOrder: string[] = []
    if (selectedNode?.data?.type === 'projection' && selectedNode?.data?.config) {
      const config = selectedNode.data.config
      if (config.columnOrder && Array.isArray(config.columnOrder)) {
        // New format: columnOrder array with { name, included, order }
        projectionOrder = config.columnOrder
          .filter((col: any) => col.included !== false && col.order >= 0)
          .sort((a: any, b: any) => a.order - b.order)
          .map((col: any) => col.name)
      } else {
        // Legacy format: output_columns or includedColumns
        projectionOrder = config.output_columns || config.includedColumns || []
      }
    }
    const executionKey = JSON.stringify({
      nodeId,
      sourceId,
      tableName,
      schema,
      conditions: currentConditions,
      nodeType: selectedNode?.data?.type,
      projectionOrder: projectionOrder, // Include column order for projection nodes
      // Include a hash of nodes array to detect when nodes are updated
      nodesHash: nodes && Array.isArray(nodes) ? nodes.map(n => `${n.id}-${JSON.stringify(n.data)}`).join('|') : '',
    })

    // For filter nodes: always execute when:
    // 1. Execution key changed (conditions/source changed)
    // 2. Node changed (different node clicked)  
    // 3. Nodes array changed (node clicked/interacted) and we have a filter node with conditions
    //    - This handles clicking the same filter node again
    // 4. Filter node is selected with conditions (force execution to show filtered results)
    const hasFilterConditions = isFilterNode && selectedNode?.data?.config?.conditions?.length > 0
    const isFirstTimeFilterNode = isFilterNode && hasFilterConditions &&
      (prevNodeIdRef.current === undefined || prevNodeIdRef.current !== nodeId)

    // Always execute for filter nodes when they're selected and have conditions
    // This ensures clicking a filter node always shows filtered results
    const shouldExecute = executionKey !== prevExecutionKeyRef.current ||
      (isFilterNode && nodeChanged && hasFilterConditions) ||
      isFirstTimeFilterNode ||
      (isFilterNode && hasFilterConditions && nodesArrayChanged && nodeId) ||
      (isFilterNode && hasFilterConditions && nodeId && prevExecutionKeyRef.current === '')

    // Debug logging
    if (isFilterNode) {
      console.log('Filter node detected:', {
        nodeId,
        hasFilterConditions,
        nodeChanged,
        nodesArrayChanged,
        isFirstTimeFilterNode,
        shouldExecute,
        executionKeyChanged: executionKey !== prevExecutionKeyRef.current,
        conditions: selectedNode?.data?.config?.conditions
      })
    }

    if (shouldExecute) {
      prevExecutionKeyRef.current = executionKey
      prevNodeIdRef.current = nodeId
      isExecutingRef.current = true

      // Use setTimeout to allow state updates to complete
      setTimeout(() => {
        if (nodeId && nodes && edges) {
          const node = selectedNode
          const inputEdge = edges && Array.isArray(edges) ? edges.find((e: any) => e.target === nodeId) : null
          const parentNode = inputEdge && nodes && Array.isArray(nodes) ? nodes.find((n: any) => n.id === inputEdge.source) : null
          const parentType = parentNode?.data?.type

          if (node && node.data.type === 'filter') {
            const hasConditions = node.data.config?.conditions?.length > 0

            // If parent is a JOIN / transform/ aggregate node, execute via pipeline so the filter
            // applies on the parent's output (e.g., joined data)
            if (hasConditions && (parentType === 'join' || parentType === 'projection' || parentType === 'calculated' || parentType === 'aggregate' || parentType === 'compute')) {
              setData([])
              setColumns([])
              setColumnLineage({})
              setError(null)
              console.log('Executing pipeline query for filter node on transform parent:', {
                nodeId,
                parentType,
                conditions: node.data.config.conditions,
              })
              executePipelineQuery().finally(() => {
                isExecutingRef.current = false
              })
            } else {
              // Filter directly on a source table (or simple upstream), use filter API
              // Always try to derive source info from props or upstream source node
              let effectiveSourceId = sourceId
              let effectiveTableName = tableName
              let effectiveSchema = schema

              if ((!effectiveSourceId || !effectiveTableName) && parentNode?.data?.config) {
                const cfg = parentNode.data.config
                if (cfg.sourceId && cfg.tableName) {
                  effectiveSourceId = cfg.sourceId
                  effectiveTableName = cfg.tableName
                  effectiveSchema = cfg.schema
                }
              }

              if (hasConditions && effectiveSourceId && effectiveTableName) {
                setData([])
                setColumns([])
                setColumnLineage({})
                setError(null)
                console.log('Executing filter query for filter node:', {
                  nodeId,
                  conditions: node.data.config.conditions,
                  sourceId: effectiveSourceId,
                  tableName: effectiveTableName,
                  schema: effectiveSchema,
                })
                executeFilterQuery(node, 1, false, {
                  sourceId: effectiveSourceId,
                  tableName: effectiveTableName,
                  schema: effectiveSchema || '',
                }).finally(() => {
                  isExecutingRef.current = false
                })
              } else if (effectiveSourceId && effectiveTableName) {
                // Filter node without conditions - show source data
                fetchTableData(1).finally(() => {
                  isExecutingRef.current = false
                })
              } else {
                // No source info available
                setData([])
                setColumns([])
                setColumnLineage({})
                setError('Filter node must be connected to a source node')
                setLoading(false)
                isExecutingRef.current = false
              }
            }
          } else if (node && (node.data.type === 'join' || node.data.type === 'projection' || node.data.type === 'calculated' || node.data.type === 'aggregate' || node.data.type === 'compute')) {
            // For transform nodes (join, projection, calculated, aggregate, compute), execute pipeline query
            executePipelineQuery().finally(() => {
              isExecutingRef.current = false
            })
          } else if (node && (node.data.type === 'destination' || (node.data.type && String(node.data.type).startsWith('destination-')))) {
            // Destination: show data that would be written (output of the node that feeds into destination)
            const destInputEdge = edges && Array.isArray(edges) ? edges.find((e: any) => e.target === nodeId) : null
            const upstreamNodeId = destInputEdge ? destInputEdge.source : null
            if (upstreamNodeId) {
              executePipelineQuery(false, upstreamNodeId).finally(() => {
                isExecutingRef.current = false
              })
            } else {
              setError('Destination has no upstream node to preview.')
              setData([])
              setColumns([])
              setColumnLineage({})
              isExecutingRef.current = false
            }
          } else if (node && node.data.type === 'source') {
            // For source nodes, check if they have embedded filter conditions
            const sourceConfig = node.data.config
            const hasEmbeddedFilter = sourceConfig?.isFiltered || (sourceConfig?.conditions && sourceConfig.conditions.length > 0)

            if (hasEmbeddedFilter && sourceConfig?.conditions && sourceConfig.conditions.length > 0) {
              // Source node has embedded filter - apply it
              setData([])
              setColumns([])
              setColumnLineage({})
              setError(null)
              console.log('Executing filter query for source node with embedded filter:', {
                nodeId,
                conditions: sourceConfig.conditions,
                sourceId: sourceConfig.sourceId || sourceId,
                tableName: sourceConfig.tableName || tableName,
                schema: sourceConfig.schema || schema,
              })

              // Create a temporary filter node structure for executeFilterQuery
              const tempFilterNode = {
                data: {
                  config: {
                    conditions: sourceConfig.conditions,
                    expression: sourceConfig.expression,
                    mode: sourceConfig.mode || 'builder',
                    sourceId: sourceConfig.sourceId || sourceId,
                    tableName: sourceConfig.tableName || tableName,
                    schema: sourceConfig.schema || schema,
                  },
                },
              }

              executeFilterQuery(tempFilterNode, 1, false, {
                sourceId: sourceConfig.sourceId || sourceId,
                tableName: sourceConfig.tableName || tableName,
                schema: sourceConfig.schema || schema,
              }).finally(() => {
                isExecutingRef.current = false
              })
            } else if (sourceId && tableName) {
              // Regular source node without filters
              fetchTableData(1).finally(() => {
                isExecutingRef.current = false
              })
            } else {
              isExecutingRef.current = false
            }
          } else {
            // For other node types, fetch table data only when this is NOT a destination.
            // Destination must always show pipeline output (handled above); never show source table.
            const targetNode = nodeId && nodes ? nodes.find((n: any) => n.id === nodeId) : null
            const targetIsDestination = targetNode && (
              targetNode.data?.type === 'destination' ||
              (targetNode.data?.type && String(targetNode.data.type).startsWith('destination-'))
            )
            if (sourceId && tableName && !targetIsDestination) {
              fetchTableData(1).finally(() => {
                isExecutingRef.current = false
              })
            } else {
              isExecutingRef.current = false
            }
          }
        } else if (sourceId && tableName && !nodeId) {
          // Direct filter mode - execute filter with directFilterConditions
          if (directFilterConditions && directFilterConditions.length > 0) {
            const requestContext = previewContextRef.current
            setData([])
            setColumns([])
            setColumnLineage({})
            setError(null)
            setLoading(true)
              ; (async () => {
                try {
                  const { api } = await import('../../../services/api')
                  const response = await api.post(
                    `/api/api-customer/sources/${sourceId}/filter/`,
                    {
                      table_name: tableName,
                      schema: schema || '',
                      conditions: directFilterConditions,
                      page: 1,
                      page_size: pageSize,
                    }
                  )

                  const { rows, columns: tableColumns, has_more } = response.data
                  if (requestContext !== previewContextRef.current) return
                  const normalizedCols = normalizeColumns(tableColumns)
                  setColumns(normalizedCols)
                  setColumnLineage({})
                  // Ensure rows are ordered to match columns
                  setData(orderRowsByColumns(rows || [], normalizedCols))
                  setHasMore(has_more || false)
                  setPage(1)
                } catch (err: any) {
                  if (requestContext !== previewContextRef.current) return
                  const errorMessage = err.response?.data?.error || err.response?.data?.detail || err.message || 'Failed to execute filter query'
                  setError(errorMessage)
                  console.error('Error executing direct filter query:', err)
                } finally {
                  setLoading(false)
                  isExecutingRef.current = false
                }
              })()
          } else {
            // Direct table data fetch (no node selected) - only if not a filter node
            // Never fetch unfiltered data when a filter node is selected
            if (!nodeId || (nodes && !nodes.find((n: any) => n.id === nodeId && n.data?.type === 'filter'))) {
              fetchTableData(1).finally(() => {
                isExecutingRef.current = false
              })
            } else {
              isExecutingRef.current = false
            }
          }
        } else {
          isExecutingRef.current = false
        }
      }, 0)
    } else {
      isExecutingRef.current = false
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sourceId, tableName, schema, nodeId, nodes, edges, directFilterConditions])

  // SEPARATE EFFECT: Re-sort columns and data when projection order changes (without re-fetching)
  // This ensures the preview table updates immediately when Move buttons are clicked
  useEffect(() => {
    if (!nodeId || !nodes || columns.length === 0 || data.length === 0) return;

    const selectedNode = nodes.find((n: any) => n.id === nodeId);
    if (!selectedNode || selectedNode.data?.type !== 'projection') return;

    const projectionConfig = selectedNode.data?.config;
    if (!projectionConfig) return;

    // Get the current projection order from config
    let projectionOrder: string[] = [];
    if (projectionConfig.columnOrder && Array.isArray(projectionConfig.columnOrder)) {
      const includedCols = projectionConfig.columnOrder
        .filter((col: any) => col.included !== false && col.order >= 0)
        .sort((a: any, b: any) => a.order - b.order)
        .map((col: any) => col.name);
      projectionOrder = includedCols;
    } else if (projectionConfig.output_columns && Array.isArray(projectionConfig.output_columns)) {
      projectionOrder = projectionConfig.output_columns;
    } else if (projectionConfig.includedColumns && Array.isArray(projectionConfig.includedColumns)) {
      projectionOrder = projectionConfig.includedColumns;
    }

    if (projectionOrder.length === 0) return;

    // Check if the current column order matches the projection order
    const currentOrderStr = columns.join(',');

    // Filter projection order to only include columns that exist in current data
    const validProjectionOrder = projectionOrder.filter(col => columns.includes(col));
    const missingCols = columns.filter(col => !validProjectionOrder.includes(col));
    const newOrder = [...validProjectionOrder, ...missingCols];
    const newOrderStr = newOrder.join(',');

    if (currentOrderStr !== newOrderStr) {
      console.log('[TableDataPanel] Projection order changed, re-sorting columns');
      console.log('[TableDataPanel] Old order:', currentOrderStr);
      console.log('[TableDataPanel] New order:', newOrderStr);

      // Update columns to new order
      setColumns(newOrder);

      // Re-order the existing rows to match the new column order
      const reorderedRows = orderRowsByColumns(data, newOrder);
      setData(reorderedRows);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodeId, nodes])

  const executeFilterQuery = async (
    filterNode: any,
    pageNum: number = 1,
    append: boolean = false,
    overrideSource?: { sourceId: number; tableName: string; schema?: string }
  ) => {
    const requestContext = previewContextRef.current
    const effectiveSourceId = overrideSource?.sourceId ?? sourceId
    const effectiveTableName = overrideSource?.tableName ?? tableName
    const effectiveSchema = overrideSource?.schema ?? schema

    if (!effectiveSourceId || !effectiveTableName) {
      setError('Filter node must be connected to a source node')
      setLoading(false)
      return
    }

    // Validate conditions exist and are valid
    if (!filterNode.data.config?.conditions || filterNode.data.config.conditions.length === 0) {
      setError('Filter node has no conditions defined')
      setLoading(false)
      return
    }

    // Clear any existing data before executing filter (unless appending)
    if (!append) {
      setData([])
      setColumns([])
      setColumnLineage({})
    }
    setLoading(true)
    setError(null)
    try {
      const { api } = await import('../../../services/api')

      // Clean column names - remove type information in parentheses if present
      // Get parent node to determine if we should keep table prefix (for joins)
      const inputEdge = edges && Array.isArray(edges) ? edges.find((e: any) => e.target === filterNode.id) : null
      const parentNode = inputEdge && nodes && Array.isArray(nodes) ? nodes.find((n: any) => n.id === inputEdge.source) : null
      const parentType = parentNode?.data?.type

      const cleanedConditions = (filterNode.data.config?.conditions || [])
        .filter((c: any) => c.column && c.operator) // Filter out invalid conditions
        .map((c: any) => {
          let columnName = c.column
          if (typeof columnName === 'string') {
            // Remove type information if present e.g. "col (int)" -> "col"
            if (columnName.includes('(')) {
              columnName = columnName.split('(')[0].trim()
            }

            // For filter nodes after source (not join), remove table prefix
            // For filter nodes after join, keep table prefix (e.g., "table.col")
            const hasTablePrefix = columnName.includes('.')
            if (hasTablePrefix) {
              // Check parent node type - if it's a join, keep the prefix
              // Otherwise, remove it
              if (parentType !== 'join') {
                // Remove table prefix for non-join parents
                const parts = columnName.split('.')
                columnName = parts[parts.length - 1].trim()
              }
              // If parent is join, keep the full "table.column" format
            }
          }

          // Handle BETWEEN operator value format
          let value = c.value
          if (c.operator === 'BETWEEN') {
            if (typeof value === 'string') {
              try {
                value = JSON.parse(value)
              } catch {
                const parts = value.split(',').map((v: string) => v.trim())
                if (parts.length === 2) {
                  value = parts
                }
              }
            }
          }

          return {
            column: columnName,
            operator: c.operator,
            value: value,
            logicalOperator: c.logicalOperator || 'AND',
          }
        })

      // Validate we have at least one valid condition
      if (cleanedConditions.length === 0) {
        setError('No valid filter conditions found')
        setLoading(false)
        return
      }

      // Log request for debugging
      console.log('Sending filter request:', {
        table_name: tableName,
        schema: schema || '',
        conditions: cleanedConditions,
        page: pageNum,
        page_size: pageSize,
      })

      // Backend expects 'conditions' as a flat array, not 'filters' as nested object
      const response = await api.post(
        `/api/api-customer/sources/${effectiveSourceId}/filter/`,
        {
          table_name: effectiveTableName,
          schema: effectiveSchema || '',
          conditions: cleanedConditions,  // Send conditions array directly
          page: pageNum,
          page_size: pageSize,
        }
      )

      const { rows, columns: tableColumns, has_more } = response.data
      if (requestContext !== previewContextRef.current) return
      const normalizedCols = normalizeColumns(tableColumns)

      if (append) {
        // When appending, ensure new rows are also ordered correctly
        const orderedNewRows = orderRowsByColumns(rows || [], normalizedCols)
        setData((prev) => [...prev, ...orderedNewRows])
      } else {
        // Ensure rows are ordered to match columns (important for projection nodes)
        setColumns(normalizedCols)
        setData(orderRowsByColumns(rows || [], normalizedCols))
      }

      setHasMore(has_more || false)
      setPage(pageNum)

      // If no data returned, show a message
      if (!rows || rows.length === 0) {
        if (!append) {
          setError('No data matches the filter conditions')
        }
      }
    } catch (err: any) {
      if (requestContext !== previewContextRef.current) return
      const errorMessage = err.response?.data?.error || err.response?.data?.detail || err.message || 'Failed to execute filter query'
      setError(errorMessage)
      console.error('Error executing filter query:', err)
      if (!append) {
        setData([])
        setColumns([])
        setColumnLineage({})
      }
    } finally {
      setLoading(false)
    }
  }

  const fetchTableData = async (pageNum: number, append = false) => {
    if (!sourceId || !tableName) return

    const requestContext = previewContextRef.current
    setLoading(true)
    setError(null)
    try {
      const { sourceTableApi } = await import('../../../services/api')
      const response = await sourceTableApi.getTableData(sourceId, tableName, schema || '', pageNum, pageSize)

      // sourceTableApi.getTableData already returns the unwrapped body
      const { rows, columns: tableColumns, has_more } = response
      if (requestContext !== previewContextRef.current) return
      const normalizedCols = normalizeColumns(tableColumns)

      if (append) {
        // When appending, ensure new rows are also ordered correctly
        const orderedNewRows = orderRowsByColumns(rows || [], normalizedCols)
        setData((prev) => [...prev, ...orderedNewRows])
      } else {
        // Ensure rows are ordered to match columns (important for projection nodes)
        setColumns(normalizedCols)
        setData(orderRowsByColumns(rows || [], normalizedCols))
        setColumnLineage({}) // Source table API has no lineage
      }

      setHasMore(has_more || false)
      setPage(pageNum)
    } catch (err: any) {
      if (requestContext !== previewContextRef.current) return
      setError(err.response?.data?.error || err.message || 'Failed to fetch table data')
      console.error('Error fetching table data:', err)
    } finally {
      setLoading(false)
    }
  }

  const handleLoadMore = () => {
    if (!loading && hasMore) {
      // Use the appropriate query method based on node type
      if (nodeId && nodes && edges) {
        const filterNode = nodes.find((n: any) => n.id === nodeId)
        if (filterNode && filterNode.data.type === 'filter' &&
          filterNode.data.config?.conditions?.length > 0 && sourceId && tableName) {
          // For filter nodes, execute filter query with next page
          executeFilterQuery(filterNode, page + 1, true)
        } else {
          fetchTableData(page + 1, true)
        }
      } else {
        fetchTableData(page + 1, true)
      }
    }
  }

  const handleRefresh = () => {
    if (nodeId && nodes && edges) {
      const filterNode = nodes.find((n: any) => n.id === nodeId)
      // If it's a filter node with conditions, execute filter query
      if (filterNode && filterNode.data.type === 'filter' &&
        filterNode.data.config?.conditions?.length > 0 && sourceId && tableName) {
        executeFilterQuery(filterNode)
      } else {
        // Force refresh bypasses cache
        executePipelineQuery(true)
      }
    } else {
      fetchTableData(1, false)
    }
  }


  return (
    <Box
      w="100%"
      h="100%"
      bg={bg}
      display="flex"
      flexDirection="column"
      overflow="hidden"
    >
      {/* Header: show which node is being previewed so each node's preview is clearly identified */}
      {(() => {
        const previewNode = nodeId && nodes ? nodes.find((n: any) => n.id === nodeId) : null
        const previewLabel = previewNode?.data?.business_name || previewNode?.data?.label || (nodeId ? `Node ${nodeId.slice(0, 8)}` : null)
        const isDestination = previewNode && (
          previewNode.data?.type === 'destination' ||
          (previewNode.data?.type && String(previewNode.data.type).startsWith('destination-'))
        )
        return (
          <HStack
            p={3}
            bg={headerBg}
            borderBottomWidth="1px"
            borderBottomColor={borderColor}
            justify="space-between"
          >
            <HStack spacing={3}>
              <Database size={18} />
              <VStack align="start" spacing={0}>
                <Text fontWeight="bold" fontSize="sm">
                  {previewLabel ? `Preview: ${previewLabel}` : (tableName || 'Pipeline Results')}
                </Text>
                {tableName && previewLabel && !isDestination && (
                  <Text fontSize="xs" color={useColorModeValue('gray.600', 'gray.400')}>
                    {tableName}{schema ? ` · ${schema}` : ''}
                  </Text>
                )}
                {schema && !previewLabel && (
                  <Badge size="sm" colorScheme="gray">
                    {schema}
                  </Badge>
                )}
              </VStack>
            </HStack>
            <HStack spacing={2}>
              <Tooltip label="Refresh preview (bypasses cache — use after adding columns upstream)">
                <IconButton
                  aria-label="Refresh"
                  icon={<RefreshCw size={16} />}
                  size="sm"
                  variant="ghost"
                  onClick={handleRefresh}
                  isLoading={loading}
                />
              </Tooltip>
              <IconButton
                aria-label="Close"
                icon={<X size={16} />}
                size="sm"
                variant="ghost"
                onClick={onClose}
              />
            </HStack>
          </HStack>
        )
      })()}

      {/* Content */}
      <Box flex={1} overflowY="auto" p={4}>
        {error ? (
          <Alert status="error" mb={4}>
            <AlertIcon />
            <Text fontSize="sm">
              {typeof error === 'string' ? error : (() => {
                try {
                  return JSON.stringify(error)
                } catch {
                  return 'An unexpected error occurred'
                }
              })()}
            </Text>
          </Alert>
        ) : loading && data.length === 0 ? (
          <Box textAlign="center" py={8}>
            <Spinner size="lg" />
            <Text mt={4}>Loading table data...</Text>
          </Box>
        ) : data.length === 0 ? (
          <Box textAlign="center" py={8}>
            <Text color="gray.500">No data available</Text>
            {nodeId && (
              <Text fontSize="sm" color="gray.400" mt={2} maxW="320px" mx="auto">
                The pipeline returned 0 rows. Try relaxing filters, checking join conditions, or previewing an upstream node.
              </Text>
            )}
          </Box>
        ) : (
          <TableContainer>
            <Table size="sm" variant="simple">
              <Thead position="sticky" top={0} bg={headerBg} zIndex={10}>
                <Tr>
                  {columns.map((col) => {
                    const lineageLabel = getLineageLabel(col)
                    return (
                      <Th key={col} fontSize="xs" fontWeight="bold" textTransform="none">
                        {lineageLabel ? (
                          <Tooltip label={lineageLabel} placement="top" hasArrow>
                            <Box as="span" borderBottomWidth="1px" borderStyle="dotted" cursor="help" title={lineageLabel}>
                              {col}
                            </Box>
                          </Tooltip>
                        ) : (
                          col
                        )}
                      </Th>
                    )
                  })}
                </Tr>
              </Thead>
              <Tbody>
                {data.map((row, idx) => (
                  <Tr key={idx} _hover={{ bg: useColorModeValue('gray.50', 'gray.700') }}>
                    {columns.map((col) => (
                      <Td key={col} fontSize="xs" maxW="200px" overflow="hidden" textOverflow="ellipsis">
                        {row[col] !== null && row[col] !== undefined
                          ? String(row[col])
                          : <Text as="span" color="gray.400">NULL</Text>}
                      </Td>
                    ))}
                  </Tr>
                ))}
              </Tbody>
            </Table>
          </TableContainer>
        )}

        {/* Load More Button */}
        {hasMore && !loading && (
          <Box textAlign="center" mt={4}>
            <Text
              as="button"
              color="blue.500"
              cursor="pointer"
              onClick={handleLoadMore}
              _hover={{ textDecoration: 'underline' }}
            >
              Load More ({pageSize} more rows)
            </Text>
          </Box>
        )}

        {loading && data.length > 0 && (
          <Box textAlign="center" mt={4}>
            <Spinner size="sm" />
          </Box>
        )}
      </Box>
    </Box>
  )
}

