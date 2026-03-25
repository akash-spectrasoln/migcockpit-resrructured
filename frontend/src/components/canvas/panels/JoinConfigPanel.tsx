/**
 * Join Configuration Panel Component
 * Allows users to configure join operations between tables
 */
import React, { useState, useEffect, useRef } from 'react'
import {
  Box,
  VStack,
  HStack,
  Grid,
  Text,
  Button,
  Select,
  IconButton,
  Badge,
  Alert,
  AlertIcon,
  Divider,
  useColorModeValue,
  FormControl,
  FormLabel,
  Input,
  InputGroup,
  InputLeftElement,
  Tooltip,
  Checkbox,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
} from '@chakra-ui/react'
import { Plus, X, ArrowLeftRight, ArrowUp, ArrowDown, ArrowUpToLine, ArrowDownToLine, GripVertical, Search, ChevronDown, ChevronUp } from 'lucide-react'
import { Node, Edge } from 'reactflow'
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  DragEndEvent,
} from '@dnd-kit/core'
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'
import { useCanvasStore } from '../../../store/canvasStore'

interface JoinCondition {
  id: string
  leftColumn: string
  rightColumn: string
  operator?: string
}

interface OutputColumn {
  source: 'left' | 'right'
  column: string
  alias: string  // Internal alias-prefixed reference (_L_.column_name or _R_.column_name)
  outputName?: string  // User-renamed output field name (defaults to prefixed name for ambiguous fields)
  included: boolean
  datatype?: string  // Column data type (e.g., 'TEXT', 'INTEGER', 'VARCHAR')
  nullable?: boolean  // Whether column is nullable
  isPrimaryKey?: boolean  // Whether column is a primary key
}

interface JoinConfigPanelProps {
  node: Node | null
  nodes: Node[]
  edges: Edge[]
  onUpdate: (nodeId: string, config: any) => void
}

const joinTypes = [
  { value: 'INNER', label: 'INNER JOIN' },
  { value: 'LEFT', label: 'LEFT JOIN' },
  { value: 'RIGHT', label: 'RIGHT JOIN' },
  { value: 'FULL OUTER', label: 'FULL OUTER JOIN' },
  { value: 'CROSS', label: 'CROSS JOIN' },
]

// Fixed alias convention constants (outside component for reuse)
const LEFT_ALIAS = '_L_'
const RIGHT_ALIAS = '_R_'

// Helper function to format column name with alias
const formatColumnWithAlias = (column: string, source: 'left' | 'right'): string => {
  const alias = source === 'left' ? LEFT_ALIAS : RIGHT_ALIAS
  return `${alias}.${column}`
}

// Helper function to get output name for ambiguous fields (prefix instead of suffix)
const getOutputName = (column: string, source: 'left' | 'right', leftCols: Set<string>, rightCols: Set<string>): string => {
  // Check if column exists in both tables (ambiguous)
  const isAmbiguous = leftCols.has(column) && rightCols.has(column)
  
  if (isAmbiguous) {
    // Prefix with _L_ or _R_ for ambiguous fields
    return source === 'left' ? `_L_${column}` : `_R_${column}`
  }
  
  // For unique fields, return as-is (no prefix/suffix)
  return column
}

// Sortable column item component for column order panel
const SortableColumnItem: React.FC<{
  columnKey: string
  col: OutputColumn
  index: number
  leftColumns: string[]
  rightColumns: string[]
  isSelected: boolean
  onToggleSelect: () => void
}> = ({ columnKey, col, index, leftColumns, rightColumns, isSelected, onToggleSelect }) => {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: columnKey })

  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const leftCols = new Set(leftColumns)
  const rightCols = new Set(rightColumns)
  const displayName = col.outputName || getOutputName(col.column, col.source, leftCols, rightCols)

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  }

  return (
    <HStack
      ref={setNodeRef}
      style={style}
      p={2}
      borderRadius="md"
      borderWidth="1px"
      borderColor={isSelected ? useColorModeValue('blue.400', 'blue.600') : borderColor}
      bg={isSelected ? useColorModeValue('blue.50', 'blue.900') : bg}
      _hover={{
        bg: isSelected ? useColorModeValue('blue.100', 'blue.800') : useColorModeValue('gray.100', 'gray.700'),
      }}
      cursor="pointer"
      onClick={(e) => {
        // Don't trigger selection if clicking on drag handle
        const target = e.target as HTMLElement
        if (target.closest('[data-dnd-handle]')) {
          return
        }
        onToggleSelect()
      }}
    >
      {/* Drag handle */}
      <Box
        {...attributes}
        {...listeners}
        data-dnd-handle
        cursor="grab"
        _active={{ cursor: 'grabbing' }}
        color={useColorModeValue('gray.500', 'gray.400')}
      >
        <GripVertical size={14} />
      </Box>

      {/* Order number */}
      <Text fontSize="xs" fontWeight="medium" minW="30px">
        {index + 1}
      </Text>

      {/* Column name */}
      <Text fontSize="xs" flex="1" minW={0} isTruncated title={displayName}>
        {displayName}
      </Text>

      {/* Source badge */}
      <Badge
        size="sm"
        colorScheme={col.source === 'left' ? 'blue' : 'purple'}
        fontSize="2xs"
      >
        {col.source === 'left' ? 'L' : 'R'}
      </Badge>
    </HStack>
  )
}

export const JoinConfigPanel: React.FC<JoinConfigPanelProps> = ({
  node,
  nodes,
  edges,
  onUpdate,
}) => {
  const [joinType, setJoinType] = useState<string>('INNER')
  const [leftTableNode, setLeftTableNode] = useState<Node | null>(null)
  const [rightTableNode, setRightTableNode] = useState<Node | null>(null)
  const [conditions, setConditions] = useState<JoinCondition[]>([])
  const [leftColumns, setLeftColumns] = useState<string[]>([])
  const [rightColumns, setRightColumns] = useState<string[]>([])
  const [leftColumnTechnicalNames, setLeftColumnTechnicalNames] = useState<string[]>([])
  const [rightColumnTechnicalNames, setRightColumnTechnicalNames] = useState<string[]>([])
  const [leftColumnsMetadata, setLeftColumnsMetadata] = useState<Map<string, { datatype: string; nullable: boolean; isPrimaryKey?: boolean }>>(new Map())
  const [rightColumnsMetadata, setRightColumnsMetadata] = useState<Map<string, { datatype: string; nullable: boolean; isPrimaryKey?: boolean }>>(new Map())
  const [selectedLeftColumns, setSelectedLeftColumns] = useState<Set<string>>(new Set())
  const [selectedRightColumns, setSelectedRightColumns] = useState<Set<string>>(new Set())
  const [outputColumns, setOutputColumns] = useState<OutputColumn[]>([])
  const [outputColumnsOrder, setOutputColumnsOrder] = useState<string[]>([]) // Ordered list of included columns (for reordering)
  const [selectedOutputColumns, setSelectedOutputColumns] = useState<Set<string>>(new Set()) // Selected columns for reordering
  const [error, setError] = useState<string | null>(null)
  const [searchTerm, setSearchTerm] = useState<string>('') // Search for Left/Right columns
  const [orderSearchTerm, setOrderSearchTerm] = useState<string>('') // Search for Order panel
  const [isConditionsPanelOpen, setIsConditionsPanelOpen] = useState<boolean>(false)
  const [isColumnsPanelOpen, setIsColumnsPanelOpen] = useState<boolean>(false) // Left/Right panels
  const [activeTab, setActiveTab] = useState<number>(0) // 0 for Left, 1 for Right
  const isDraggingRef = useRef(false)

  // DnD sensors for reordering included columns
  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: {
        distance: 8,
      },
    }),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  )

  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const headerBg = useColorModeValue('gray.50', 'gray.700')

  // Initialize output columns from config or generate defaults
  const initializeOutputColumns = (leftCols: string[], rightCols: string[], savedOutputColumns?: OutputColumn[]) => {
    const leftColumnNames = new Set(leftCols)
    const rightColumnNames = new Set(rightCols)
    
    // If saved output columns exist, normalize their aliases to use fixed convention
    if (savedOutputColumns && savedOutputColumns.length > 0) {
      let normalizedColumns = savedOutputColumns.map(col => {
        // Recalculate outputName using new prefix logic for ambiguous fields
        const outputName = col.outputName || getOutputName(col.column, col.source, leftColumnNames, rightColumnNames)
        return {
          ...col,
          alias: formatColumnWithAlias(col.column, col.source), // Ensure fixed alias convention (_L_ or _R_)
          outputName: outputName
        }
      })

      // Legacy-heal: some saved configs ended up with only 0/1 included column by default.
      // Join should include all columns unless the user explicitly deselects them.
      const includedCount = normalizedColumns.filter(col => col.included !== false).length
      if (normalizedColumns.length > 1 && includedCount <= 1) {
        normalizedColumns = normalizedColumns.map(col => ({ ...col, included: true }))
      }
      setOutputColumns(normalizedColumns)
      
      // Initialize order from saved columns (only included ones)
      const includedOrder = normalizedColumns
        .filter(col => col.included)
        .map(col => `${col.source}:${col.column}`)
      setOutputColumnsOrder(includedOrder)
      return
    }

    // Generate default output columns with new prefix-based conflict resolution
    const defaultColumns: OutputColumn[] = []
    
    // Include all LEFT table columns with metadata
    leftCols.forEach(col => {
      const outputName = getOutputName(col, 'left', leftColumnNames, rightColumnNames)
      const metadata = leftColumnsMetadata.get(col) || { datatype: 'TEXT', nullable: true, isPrimaryKey: false }
      
      defaultColumns.push({
        source: 'left',
        column: col,
        alias: `${LEFT_ALIAS}.${col}`, // Fixed alias convention (_L_)
        outputName: outputName, // Uses prefix for ambiguous fields
        included: true,
        datatype: metadata.datatype,
        nullable: metadata.nullable,
        isPrimaryKey: metadata.isPrimaryKey,
      })
    })

    // Include RIGHT table columns with metadata (include ALL columns, including join keys)
    // Both _L_cmp_id and _R_cmp_id are needed for downstream (cmp_id_left, cmp_id_right in destinations)
    rightCols.forEach(col => {
      const outputName = getOutputName(col, 'right', leftColumnNames, rightColumnNames)
      const metadata = rightColumnsMetadata.get(col) || { datatype: 'TEXT', nullable: true, isPrimaryKey: false }
      
      defaultColumns.push({
        source: 'right',
        column: col,
        alias: `${RIGHT_ALIAS}.${col}`, // Fixed alias convention (_R_)
        outputName: outputName, // Uses prefix for ambiguous fields
        included: true,
        datatype: metadata.datatype,
        nullable: metadata.nullable,
        isPrimaryKey: metadata.isPrimaryKey,
      })
    })

    setOutputColumns(defaultColumns)
    
    // Initialize order from all included columns
    const includedOrder = defaultColumns
      .filter(col => col.included)
      .map(col => `${col.source}:${col.column}`)
    setOutputColumnsOrder(includedOrder)
  }

  useEffect(() => {
    if (node) {
      const config = node.data.config || {}
      setJoinType(config.joinType || 'INNER')
      setConditions(config.conditions || [])
      
      // Load saved output columns if they exist
      if (config.outputColumns && Array.isArray(config.outputColumns)) {
        setOutputColumns(config.outputColumns)
        // Initialize order from saved columns (only included ones)
        const includedOrder = config.outputColumns
          .filter((col: OutputColumn) => col.included !== false)
          .map((col: OutputColumn) => `${col.source}:${col.column}`)
        setOutputColumnsOrder(includedOrder)
      } else {
        // Reset order if no saved columns
        setOutputColumnsOrder([])
      }

      // Find connected input nodes
      const inputEdges = edges.filter((e) => e.target === node.id)
      
      console.log('Join node edges:', {
        nodeId: node.id,
        edges: inputEdges.map(e => ({
          id: e.id,
          source: e.source,
          target: e.target,
          targetHandle: e.targetHandle,
          sourceHandle: e.sourceHandle
        }))
      })
      
      // For join nodes, we need to identify left and right edges
      // Strategy:
      // 1. First, find edges with explicit targetHandle
      // 2. If not all found, use edge order (first = left, second = right)
      // 3. Make sure we don't assign the same edge to both
      
      let leftEdge = inputEdges.find((e) => e.targetHandle === 'left')
      let rightEdge = inputEdges.find((e) => e.targetHandle === 'right')
      
      // If we have edges without explicit handles, assign them
      const unassignedEdges = inputEdges.filter(e => 
        e.targetHandle !== 'left' && e.targetHandle !== 'right'
      )
      
      // Assign left edge if not found
      if (!leftEdge) {
        if (unassignedEdges.length > 0) {
          // First unassigned edge is left (top handle, connected first)
          leftEdge = unassignedEdges[0]
        } else if (inputEdges.length > 0) {
          // If no unassigned edges, find an edge that's not the right edge
          const candidateLeftEdge = inputEdges.find(e => e !== rightEdge)
          if (candidateLeftEdge) {
            leftEdge = candidateLeftEdge
          }
        }
      }
      
      // Assign right edge if not found
      if (!rightEdge) {
        if (unassignedEdges.length > 1) {
          // Second unassigned edge is right (bottom handle, connected second)
          rightEdge = unassignedEdges[1]
        } else if (unassignedEdges.length === 1 && unassignedEdges[0] !== leftEdge) {
          // Only one unassigned edge and it's not left, so it's right
          rightEdge = unassignedEdges[0]
        } else if (inputEdges.length > 0) {
          // Find an edge that's not the left edge
          const candidateRightEdge = inputEdges.find(e => e !== leftEdge)
          if (candidateRightEdge) {
            rightEdge = candidateRightEdge
          }
        }
      }
      
      console.log('Detected edges:', {
        leftEdge: leftEdge ? { 
          source: leftEdge.source, 
          targetHandle: leftEdge.targetHandle,
          sourceNode: nodes.find(n => n.id === leftEdge.source)?.data.label
        } : null,
        rightEdge: rightEdge ? { 
          source: rightEdge.source, 
          targetHandle: rightEdge.targetHandle,
          sourceNode: nodes.find(n => n.id === rightEdge.source)?.data.label
        } : null,
        totalEdges: inputEdges.length
      })

      if (leftEdge) {
        const leftNode = nodes.find((n) => n.id === leftEdge.source)
        setLeftTableNode(leftNode || null)
        if (leftNode) {
          console.log('Left table node found:', {
            nodeId: leftNode.id,
            label: leftNode.data.label,
            type: leftNode.data.type,
            config: leftNode.data.config
          })
          loadColumns(leftNode, 'left')
        } else {
          console.warn('Left edge found but node not found:', leftEdge.source)
        }
      } else {
        setLeftTableNode(null)
        setLeftColumns([])
        setLeftColumnTechnicalNames([])
        console.log('No left edge found. Input edges:', inputEdges.map(e => ({
          source: e.source,
          target: e.target,
          targetHandle: e.targetHandle
        })))
      }

      if (rightEdge) {
        const rightNode = nodes.find((n) => n.id === rightEdge.source)
        setRightTableNode(rightNode || null)
        if (rightNode) {
          console.log('Right table node found:', {
            nodeId: rightNode.id,
            label: rightNode.data.label,
            type: rightNode.data.type,
            config: rightNode.data.config
          })
          loadColumns(rightNode, 'right')
        } else {
          console.warn('Right edge found but node not found:', rightEdge.source)
        }
      } else {
        setRightTableNode(null)
        setRightColumns([])
        setRightColumnTechnicalNames([])
      }
    }
  }, [node, nodes, edges])

  // CRITICAL: Auto-save Join config to node.data.config when conditions, joinType, or outputColumns change
  // This ensures Save Pipeline can persist Join config without requiring an explicit Save button
  // Only auto-save if we have both inputs connected and valid config
  useEffect(() => {
    if (!node || !leftTableNode || !rightTableNode) {
      return
    }

    // Debounce auto-save to avoid excessive updates
    const timeoutId = setTimeout(() => {
      const validConditions = conditions.filter((c) => c.leftColumn && c.rightColumn)
      
      // Only auto-save if we have valid conditions OR it's a CROSS join
      // Don't auto-save empty conditions for non-CROSS joins (will fail validation anyway)
      if (joinType === 'CROSS' || validConditions.length > 0) {
        const leftTableName = (leftTableNode.data.label || leftTableNode.id) as string
        const rightTableName = (rightTableNode.data.label || rightTableNode.id) as string

        // Build combined output column list respecting the configured order
        const leftColumnNames = new Set(leftColumns)
        const rightColumnNames = new Set(rightColumns)
        const includedOutputColumns = outputColumns.filter(col => col.included)
        const columnMap = new Map<string, OutputColumn>()
        includedOutputColumns.forEach(col => {
          columnMap.set(`${col.source}:${col.column}`, col)
        })
        const orderedColumns: string[] = []
        const processedKeys = new Set<string>()
        outputColumnsOrder.forEach(key => {
          const col = columnMap.get(key)
          if (col) {
            const outputName = col.outputName || getOutputName(col.column, col.source, leftColumnNames, rightColumnNames)
            orderedColumns.push(outputName)
            processedKeys.add(key)
          }
        })
        includedOutputColumns.forEach(col => {
          const key = `${col.source}:${col.column}`
          if (!processedKeys.has(key)) {
            const outputName = col.outputName || getOutputName(col.column, col.source, leftColumnNames, rightColumnNames)
            orderedColumns.push(outputName)
          }
        })
        const joinedColumns = orderedColumns.length > 0 ? orderedColumns : [...leftColumns, ...rightColumns]

        const autoSaveConfig = {
          ...node.data.config,
          joinType,
          conditions: validConditions,
          leftTable: leftTableName,
          rightTable: rightTableName,
          leftNodeId: leftTableNode.id,
          rightNodeId: rightTableNode.id,
          outputColumns: outputColumns.length > 0 ? outputColumns : undefined,
          columns: joinedColumns,
          selectedLeftColumns: Array.from(selectedLeftColumns),
          selectedRightColumns: Array.from(selectedRightColumns),
        }

        // Only update if config actually changed
        const currentConfig = node.data.config || {}
        const configChanged = 
          currentConfig.joinType !== joinType ||
          JSON.stringify(currentConfig.conditions || []) !== JSON.stringify(validConditions) ||
          currentConfig.leftNodeId !== leftTableNode.id ||
          currentConfig.rightNodeId !== rightTableNode.id ||
          JSON.stringify(currentConfig.outputColumns || []) !== JSON.stringify(outputColumns) ||
          JSON.stringify(currentConfig.columns || []) !== JSON.stringify(joinedColumns)

        if (configChanged) {
          console.log('[JoinConfigPanel] Auto-saving config changes to node.data.config')
          onUpdate(node.id, {
            config: autoSaveConfig,
            business_name: node.data.business_name || node.data.node_name || node.data.label || 'Join',
            technical_name: node.data.technical_name,
            node_name: node.data.business_name || node.data.node_name || node.data.label || 'Join',
            label: node.data.business_name || node.data.node_name || node.data.label || 'Join',
          })
        }
      }
    }, 500) // 500ms debounce

    return () => clearTimeout(timeoutId)
  }, [joinType, conditions, leftTableNode, rightTableNode, node, onUpdate, outputColumns, outputColumnsOrder, leftColumns, rightColumns, selectedLeftColumns, selectedRightColumns])

  // Initialize output columns when columns or conditions change
  useEffect(() => {
    if (leftColumns.length > 0 || rightColumns.length > 0) {
      const config = node?.data?.config || {}
      const savedOutputColumns = config.outputColumns
      
      // Only initialize if we don't have saved output columns or if columns changed
      if (!savedOutputColumns || savedOutputColumns.length === 0) {
        initializeOutputColumns(leftColumns, rightColumns, savedOutputColumns)
      } else {
        // Update metadata and add any join-condition columns missing from saved output
        const leftCols = new Set(leftColumns)
        const rightCols = new Set(rightColumns)
        const conds = config.conditions || []
        const existingKeys = new Set(savedOutputColumns.map((c: OutputColumn) => `${c.source}:${c.column}`))
        const toAdd: OutputColumn[] = []

        conds.forEach((cond: { leftColumn?: string; rightColumn?: string }) => {
          if (cond.leftColumn && leftColumns.includes(cond.leftColumn)) {
            const key = `left:${cond.leftColumn}`
            if (!existingKeys.has(key)) {
              const metadata = leftColumnsMetadata.get(cond.leftColumn) || { datatype: 'TEXT', nullable: true, isPrimaryKey: false }
              toAdd.push({
                source: 'left',
                column: cond.leftColumn,
                alias: `${LEFT_ALIAS}.${cond.leftColumn}`,
                outputName: getOutputName(cond.leftColumn, 'left', leftCols, rightCols),
                included: true,
                datatype: metadata.datatype,
                nullable: metadata.nullable,
                isPrimaryKey: metadata.isPrimaryKey,
              })
              existingKeys.add(key)
            }
          }
          if (cond.rightColumn && rightColumns.includes(cond.rightColumn)) {
            const key = `right:${cond.rightColumn}`
            if (!existingKeys.has(key)) {
              const metadata = rightColumnsMetadata.get(cond.rightColumn) || { datatype: 'TEXT', nullable: true, isPrimaryKey: false }
              toAdd.push({
                source: 'right',
                column: cond.rightColumn,
                alias: `${RIGHT_ALIAS}.${cond.rightColumn}`,
                outputName: getOutputName(cond.rightColumn, 'right', leftCols, rightCols),
                included: true,
                datatype: metadata.datatype,
                nullable: metadata.nullable,
                isPrimaryKey: metadata.isPrimaryKey,
              })
              existingKeys.add(key)
            }
          }
        })

        // Add any columns from left/right that are not yet in output.
        // Default include to avoid accidentally collapsing join output to 1 column.
        leftColumns.forEach((col) => {
          const key = `left:${col}`
          if (!existingKeys.has(key)) {
            const metadata = leftColumnsMetadata.get(col) || { datatype: 'TEXT', nullable: true, isPrimaryKey: false }
            toAdd.push({
              source: 'left',
              column: col,
              alias: `${LEFT_ALIAS}.${col}`,
              outputName: getOutputName(col, 'left', leftCols, rightCols),
              included: true,
              datatype: metadata.datatype,
              nullable: metadata.nullable,
              isPrimaryKey: metadata.isPrimaryKey,
            })
            existingKeys.add(key)
          }
        })
        rightColumns.forEach((col) => {
          const key = `right:${col}`
          if (!existingKeys.has(key)) {
            const metadata = rightColumnsMetadata.get(col) || { datatype: 'TEXT', nullable: true, isPrimaryKey: false }
            toAdd.push({
              source: 'right',
              column: col,
              alias: `${RIGHT_ALIAS}.${col}`,
              outputName: getOutputName(col, 'right', leftCols, rightCols),
              included: true,
              datatype: metadata.datatype,
              nullable: metadata.nullable,
              isPrimaryKey: metadata.isPrimaryKey,
            })
            existingKeys.add(key)
          }
        })

        const augmentedColumns = toAdd.length > 0 ? [...savedOutputColumns, ...toAdd] : savedOutputColumns
        const augmentedWithMeta = augmentedColumns.map((col: OutputColumn) => {
          const metadata = col.source === 'left'
            ? leftColumnsMetadata.get(col.column)
            : rightColumnsMetadata.get(col.column)
          if (metadata) {
            return { ...col, datatype: metadata.datatype, nullable: metadata.nullable, isPrimaryKey: metadata.isPrimaryKey }
          }
          return col
        })

        setOutputColumns(prev => {
          if (prev.length === augmentedWithMeta.length && prev.every((p, i) => p.source === augmentedWithMeta[i].source && p.column === augmentedWithMeta[i].column)) {
            return prev
          }
          return augmentedWithMeta
        })

        let includedOrder = savedOutputColumns
          .filter((col: OutputColumn) => col.included !== false)
          .map((col: OutputColumn) => `${col.source}:${col.column}`)
        conds.forEach((cond: { leftColumn?: string; rightColumn?: string }) => {
          if (cond.rightColumn && rightColumns.includes(cond.rightColumn)) {
            const key = `right:${cond.rightColumn}`
            if (!includedOrder.includes(key)) includedOrder = [...includedOrder, key]
          }
          if (cond.leftColumn && leftColumns.includes(cond.leftColumn)) {
            const key = `left:${cond.leftColumn}`
            if (!includedOrder.includes(key)) includedOrder = [...includedOrder, key]
          }
        })
        setOutputColumnsOrder(prevOrder => {
          if (prevOrder.length === includedOrder.length && prevOrder.every((key, idx) => key === includedOrder[idx])) {
            return prevOrder
          }
          return includedOrder
        })
      }
    }
  }, [leftColumns, rightColumns, conditions, node, leftColumnsMetadata, rightColumnsMetadata])

  const loadColumns = async (tableNode: Node, side: 'left' | 'right') => {
    if (!tableNode) return
    const compiledGraph = useCanvasStore.getState().compiledGraph
    if (!compiledGraph) return
    
    // We want the columns that this input node outputs to us
    const compiledInputNode = compiledGraph.nodes[tableNode.id]
    if (!compiledInputNode) return

    // Primary source: compiled pipeline output schema
    const compiledSchema = compiledInputNode.outputSchema || []
    // Fallback: node-carried metadata can be fresher than compiled schema in some UI states
    const metaSchema = ((tableNode as any)?.data?.output_metadata?.columns || []) as any[]
    const configSchema = (((tableNode as any)?.data?.config?.columns || []) as any[])

    // Merge all candidates by display key so join panel doesn't collapse to a single derived column.
    const mergedByKey = new Map<string, any>()
    const pushCols = (arr: any[]) => {
      arr.forEach((col: any) => {
        const key =
          col?.outputName ||
          col?.business_name ||
          col?.name ||
          col?.column_name ||
          col?.column ||
          col?.db_name ||
          col?.technical_name
        if (!key) return
        if (!mergedByKey.has(String(key))) {
          mergedByKey.set(String(key), col)
        }
      })
    }
    pushCols(compiledSchema as any[])
    pushCols(metaSchema)
    pushCols(configSchema)
    const schema = Array.from(mergedByKey.values())

    const columnNames: string[] = []
    const technicalNames: string[] = []
    const metadataMap = new Map<string, { datatype: string; nullable: boolean; isPrimaryKey?: boolean }>()

    schema.forEach((col) => {
      const anyCol: any = col as any
      const displayName = anyCol.outputName || anyCol.business_name || col.column || col.name
      const technicalName = anyCol.technical_name || col.column || col.name

      columnNames.push(displayName)
      technicalNames.push(technicalName)

      const meta = {
        datatype: col.datatype || 'TEXT',
        nullable: col.nullable ?? true,
        isPrimaryKey: false,
      }
      // Index metadata by BOTH technical and display names so lookups work regardless of which was saved
      metadataMap.set(technicalName, meta)
      metadataMap.set(displayName, meta)
    })

    if (side === 'left') {
      setLeftColumns(columnNames)
      setLeftColumnTechnicalNames(technicalNames)
      setLeftColumnsMetadata(metadataMap)
    } else {
      setRightColumns(columnNames)
      setRightColumnTechnicalNames(technicalNames)
      setRightColumnsMetadata(metadataMap)
    }
  }

  const addCondition = () => {
    const newCondition: JoinCondition = {
      id: `condition-${Date.now()}`,
      leftColumn: '',
      rightColumn: '',
      operator: '=',
    }
    setConditions([...conditions, newCondition])
  }

  const removeCondition = (id: string) => {
    setConditions(conditions.filter((c) => c.id !== id))
  }

  const updateCondition = (id: string, updates: Partial<JoinCondition>) => {
    setConditions(
      conditions.map((c) => (c.id === id ? { ...c, ...updates } : c))
    )
  }

  // Output columns management functions
  const updateOutputColumn = (source: 'left' | 'right', column: string, updates: Partial<OutputColumn>) => {
    setOutputColumns(prev => prev.map(col => 
      col.source === source && col.column === column
        ? { ...col, ...updates }
        : col
    ))
  }

  const toggleOutputColumn = (source: 'left' | 'right', column: string) => {
    const currentCol = outputColumns.find(c => c.source === source && c.column === column)
    if (currentCol) {
      const newIncluded = !currentCol.included
      updateOutputColumn(source, column, { included: newIncluded })
      
      // Update order list
      const columnKey = `${source}:${column}`
      if (newIncluded) {
        // Add to order if not already present
        if (!outputColumnsOrder.includes(columnKey)) {
          setOutputColumnsOrder([...outputColumnsOrder, columnKey])
        }
      } else {
        // Remove from order
        setOutputColumnsOrder(outputColumnsOrder.filter(key => key !== columnKey))
      }
    }
  }

  const updateOutputColumnName = (source: 'left' | 'right', column: string, outputName: string) => {
    // Validate: prevent empty names
    if (!outputName || outputName.trim() === '') {
      setError('Output field name cannot be empty')
      return
    }
    
    const trimmedName = outputName.trim()
    
    // Validate: prevent duplicate output names
    const existingCol = outputColumns.find(c => 
      c.outputName === trimmedName && 
      !(c.source === source && c.column === column) &&
      c.included // Only check against included columns
    )
    if (existingCol) {
      setError(`Output field name "${trimmedName}" is already used.`)
      return
    }
    
    setError(null)
    updateOutputColumn(source, column, { outputName: trimmedName })
  }

  // Reordering functions (similar to projection)
  const canMoveSelectedColumns = (direction: 'up' | 'down' | 'top' | 'bottom'): boolean => {
    if (selectedOutputColumns.size === 0) return false

    const selectedArray = Array.from(selectedOutputColumns)
    const includedColumns = outputColumnsOrder

    if (direction === 'up') {
      return selectedArray.some(key => {
        const index = includedColumns.indexOf(key)
        return index > 0
      })
    } else if (direction === 'down') {
      return selectedArray.some(key => {
        const index = includedColumns.indexOf(key)
        return index < includedColumns.length - 1
      })
    } else if (direction === 'top') {
      return selectedArray.some(key => {
        const index = includedColumns.indexOf(key)
        return index > 0
      })
    } else if (direction === 'bottom') {
      return selectedArray.some(key => {
        const index = includedColumns.indexOf(key)
        return index < includedColumns.length - 1
      })
    }

    return false
  }

  const moveSelectedColumns = (direction: 'up' | 'down' | 'top' | 'bottom') => {
    if (selectedOutputColumns.size === 0) return

    let newOrder = [...outputColumnsOrder]

    if (direction === 'up') {
      // Find positions of selected columns
      const selectedIndices = newOrder
        .map((key, index) => ({ key, index }))
        .filter(item => selectedOutputColumns.has(item.key))
        .sort((a, b) => a.index - b.index)

      if (selectedIndices.length === 0) return

      const firstSelectedIndex = selectedIndices[0].index
      if (firstSelectedIndex === 0) return // Cannot move up

      const itemBeforeSelected = newOrder[firstSelectedIndex - 1]
      const selectedItemsInOrder = outputColumnsOrder.filter(key => selectedOutputColumns.has(key))
      const remainingItems = newOrder.filter(key => !selectedOutputColumns.has(key) && key !== itemBeforeSelected)

      const insertPosition = firstSelectedIndex - 1
      newOrder = [
        ...remainingItems.slice(0, insertPosition),
        ...selectedItemsInOrder,
        itemBeforeSelected,
        ...remainingItems.slice(insertPosition)
      ]
    } else if (direction === 'down') {
      // Find positions of selected columns
      const selectedIndices = newOrder
        .map((key, index) => ({ key, index }))
        .filter(item => selectedOutputColumns.has(item.key))
        .sort((a, b) => a.index - b.index)

      if (selectedIndices.length === 0) return

      const lastSelectedIndex = selectedIndices[selectedIndices.length - 1].index
      if (lastSelectedIndex >= newOrder.length - 1) return // Cannot move down

      const firstSelectedIndex = selectedIndices[0].index
      const itemAfterIndex = lastSelectedIndex + 1
      const itemAfter = newOrder[itemAfterIndex]

      newOrder.splice(itemAfterIndex, 1)
      newOrder.splice(firstSelectedIndex, 0, itemAfter)
    } else if (direction === 'top') {
      // Move all selected columns to the top while preserving their relative order
      const selectedInCurrentOrder = outputColumnsOrder.filter(key => selectedOutputColumns.has(key))
      const unselectedColumns = outputColumnsOrder.filter(key => !selectedOutputColumns.has(key))
      newOrder = [...selectedInCurrentOrder, ...unselectedColumns]
    } else if (direction === 'bottom') {
      // Move all selected columns to the bottom while preserving their relative order
      const selectedInCurrentOrder = outputColumnsOrder.filter(key => selectedOutputColumns.has(key))
      const unselectedColumns = outputColumnsOrder.filter(key => !selectedOutputColumns.has(key))
      newOrder = [...unselectedColumns, ...selectedInCurrentOrder]
    }

    setOutputColumnsOrder(newOrder)
  }

  // Drag and drop reordering for output columns
  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event

    if (over && active.id !== over.id) {
      const oldIndex = outputColumnsOrder.indexOf(active.id as string)
      const newIndex = outputColumnsOrder.indexOf(over.id as string)

      if (oldIndex !== -1 && newIndex !== -1) {
        isDraggingRef.current = true
        let newOrder = [...outputColumnsOrder]

        // Handle multi-selection drag if multiple columns are selected
        if (selectedOutputColumns.size > 1 && selectedOutputColumns.has(active.id as string)) {
          const selectedKeys = Array.from(selectedOutputColumns)
          const sortedSelected = selectedKeys.filter(key => outputColumnsOrder.includes(key))
            .sort((a, b) => outputColumnsOrder.indexOf(a) - outputColumnsOrder.indexOf(b))

          const unselectedItems = outputColumnsOrder.filter(key => !selectedKeys.includes(key))
          const targetIndex = unselectedItems.indexOf(over.id as string)

          if (targetIndex !== -1) {
            if (oldIndex < newIndex) {
              newOrder = [
                ...unselectedItems.slice(0, targetIndex + 1),
                ...sortedSelected,
                ...unselectedItems.slice(targetIndex + 1)
              ]
            } else {
              newOrder = [
                ...unselectedItems.slice(0, targetIndex),
                ...sortedSelected,
                ...unselectedItems.slice(targetIndex)
              ]
            }
          } else {
            newOrder = arrayMove(outputColumnsOrder, oldIndex, newIndex)
          }
        } else {
          // Single item drag
          newOrder = arrayMove(outputColumnsOrder, oldIndex, newIndex)
        }

        setOutputColumnsOrder(newOrder)

        // Reset drag flag after a delay
        setTimeout(() => {
          isDraggingRef.current = false
        }, 500)
      } else {
        isDraggingRef.current = false
      }
    } else {
      isDraggingRef.current = false
    }
  }

  const swapTables = () => {
    if (!leftTableNode || !rightTableNode || !node) return

    // 1. Swap the internal state
    const tempNode = leftTableNode
    setLeftTableNode(rightTableNode)
    setRightTableNode(tempNode)

    const tempColumns = leftColumns
    setLeftColumns(rightColumns)
    setRightColumns(tempColumns)

    const tempSelected = selectedLeftColumns
    setSelectedLeftColumns(selectedRightColumns)
    setSelectedRightColumns(tempSelected)

    const tempTechnical = leftColumnTechnicalNames
    setLeftColumnTechnicalNames(rightColumnTechnicalNames)
    setRightColumnTechnicalNames(tempTechnical)

    const tempMeta = leftColumnsMetadata
    setLeftColumnsMetadata(rightColumnsMetadata)
    setRightColumnsMetadata(tempMeta)

    // Swap outputColumns source side so left/right columns stay on their correct side
    const flippedOutputColumns: OutputColumn[] = outputColumns.map((col) => ({
      ...col,
      source: col.source === 'left' ? 'right' : col.source === 'right' ? 'left' : col.source,
    }))
    setOutputColumns(flippedOutputColumns)

    // Swap condition column references
    const swapConditionColumns = (cond: JoinCondition): JoinCondition => ({
      ...cond,
      leftColumn: cond.rightColumn,
      rightColumn: cond.leftColumn,
    })

    const swappedConditions = conditions.map(swapConditionColumns)
    setConditions(swappedConditions)

    // Build swapped auto save config immediately to prevent edges effect from wiping edits
    const validConditions = swappedConditions.filter((c) => c.leftColumn && c.rightColumn)
    const leftTableName = (rightTableNode.data.label || rightTableNode.id) as string
    const rightTableName = (leftTableNode.data.label || leftTableNode.id) as string
    const autoSaveConfig = {
      ...node.data.config,
      joinType,
      conditions: validConditions,
      leftTable: leftTableName,
      rightTable: rightTableName,
      leftNodeId: rightTableNode.id,
      rightNodeId: leftTableNode.id,
      // Persist flipped outputColumns so schema + UI stay consistent after swap
      outputColumns: flippedOutputColumns.length > 0 ? flippedOutputColumns : node.data.config?.outputColumns,
      columns: node.data.config?.columns,
      selectedLeftColumns: Array.from(selectedRightColumns),
      selectedRightColumns: Array.from(selectedLeftColumns),
    }

    onUpdate(node.id, {
      config: autoSaveConfig,
      business_name: node.data.business_name || node.data.node_name || node.data.label || 'Join',
      technical_name: node.data.technical_name,
      node_name: node.data.business_name || node.data.node_name || node.data.label || 'Join',
      label: node.data.business_name || node.data.node_name || node.data.label || 'Join',
    })

    // 2. Actually swap the source connections in React Flow
    const store = useCanvasStore.getState()
    const allEdges = store.edges
    
    // Find the current edges connecting to this join node
    const inputEdges = allEdges.filter(e => e.target === node.id)
    let leftEdge = inputEdges.find(e => e.targetHandle === 'left')
    let rightEdge = inputEdges.find(e => e.targetHandle === 'right')
    
    // Fallback if target handles are lost or unspecified
    if (!leftEdge && inputEdges.length > 0) leftEdge = inputEdges[0]
    if (!rightEdge && inputEdges.length > 1) rightEdge = leftEdge !== inputEdges[0] ? inputEdges[0] : inputEdges[1]
    
    if (leftEdge && rightEdge) {
      // Swap their targetHandles
      const newEdges = allEdges.map(e => {
        if (e.id === leftEdge!.id) return { ...e, targetHandle: 'right' }
        if (e.id === rightEdge!.id) return { ...e, targetHandle: 'left' }
        return e
      })
      store.setEdges(newEdges)
    }
  }


  // NOTE: Live updates were causing nested update loops via store writes.
  // Keep join updates explicit via validation + handleSave/onUpdate only.

  if (!node) {
    return (
      <Box
        w="320px"
        h="100%"
        bg={bg}
        borderLeftWidth="1px"
        borderColor={borderColor}
        display="flex"
        alignItems="center"
        justifyContent="center"
      >
        <Text fontSize="sm" color={useColorModeValue('gray.500', 'gray.400')}>
          Select a join node to configure
        </Text>
      </Box>
    )
  }

  return (
    <Box
      w="100%"
      h="100%"
      minW={0}
      bg={bg}
      borderLeftWidth="1px"
      borderColor={borderColor}
      display="flex"
      flexDirection="column"
      overflow="hidden"
    >
      {/* Header */}
      <Box p={4} borderBottomWidth="1px" borderColor={borderColor} bg={headerBg} w="100%" minW={0}>
        <VStack align="stretch" spacing={3} w="100%" minW={0}>
          <HStack justify="space-between" align="center">
            <Text fontSize="lg" fontWeight="semibold">
              Join Configuration
            </Text>
            {/* Live updates: no per-node Save button */}
          </HStack>
          
          {/* Business Name (Editable) */}
          <Box>
            <Text fontSize="xs" fontWeight="medium" mb={1} color={useColorModeValue('gray.600', 'gray.400')}>
              Business Name
            </Text>
            <Input
              size="sm"
              value={node.data.business_name || node.data.node_name || node.data.label || 'Join'}
              onChange={(e) => {
                onUpdate(node.id, {
                  ...node.data.config,
                  business_name: e.target.value,
                  node_name: e.target.value, // Legacy support
                  label: e.target.value, // Update label for display
                })
              }}
              placeholder="e.g., Join Customer Orders"
            />
          </Box>
          
          {/* Technical Name (Read-only) */}
          {node.data.technical_name && (
            <Box>
              <Text fontSize="xs" fontWeight="medium" mb={1} color={useColorModeValue('gray.600', 'gray.400')}>
                Technical Name
          </Text>
              <Input
                size="sm"
                value={node.data.technical_name}
                isReadOnly
                bg={useColorModeValue('gray.100', 'gray.700')}
                color={useColorModeValue('gray.600', 'gray.400')}
                cursor="not-allowed"
              />
            </Box>
          )}
        </VStack>
      </Box>

      {/* Content */}
      <Box flex={1} overflowY="auto" p={4} w="100%" minW={0}>
        {/* Validation Messages */}
        {(!leftTableNode || !rightTableNode) && (
          <Alert status="warning" mb={4} size="sm">
            <AlertIcon />
            <VStack align="flex-start" spacing={1}>
              <Text fontSize="xs" fontWeight="semibold">
                Both inputs required
              </Text>
              <Text fontSize="2xs">
                {!leftTableNode && !rightTableNode 
                  ? 'Connect both Left and Right input nodes'
                  : !leftTableNode 
                  ? 'Connect a node to the Left input handle'
                  : 'Connect a node to the Right input handle'}
              </Text>
            </VStack>
          </Alert>
        )}
        {error && (
          <Alert status="error" size="sm" mb={4}>
            <AlertIcon />
            <Box>
              <Text fontSize="xs" fontWeight="semibold" mb={1}>
                Validation Error
              </Text>
              <Text fontSize="xs" whiteSpace="pre-wrap">
                {typeof error === 'string'
                  ? error
                  : (() => {
                      try {
                        return JSON.stringify(error)
                      } catch {
                        return 'An unexpected error occurred'
                      }
                    })()}
              </Text>
            </Box>
          </Alert>
        )}

        <VStack align="stretch" spacing={4} w="100%" minW={0}>
          {/* Join Type */}
          <FormControl w="100%" minW={0}>
            <FormLabel fontSize="sm">Join Type</FormLabel>
            <Select
              size="sm"
              value={joinType}
              onChange={(e) => setJoinType(e.target.value)}
              w="100%"
            >
              {joinTypes.map((type) => (
                <option key={type.value} value={type.value}>
                  {type.label}
                </option>
              ))}
            </Select>
          </FormControl>

          <Divider />

          {/* Connected Tables */}
          <Box w="100%" minW={0}>
            <Text fontSize="sm" fontWeight="semibold" mb={2}>
              Connected Tables
            </Text>
            {/* Responsive Grid: side-by-side by default, stacked only on very narrow sidebar */}
            <Grid
              templateColumns={{ base: '1fr auto 1fr' }}
              gap={3}
              alignItems="center"
              w="100%"
              minW={0}
            >
              {/* Left Table */}
              <Box
                p={3}
                bg={useColorModeValue('gray.50', 'gray.700')}
                borderRadius="md"
                w="100%"
                minW={0}
              >
                <Text fontSize="xs" color="gray.500" mb={1}>
                  Left Table ({LEFT_ALIAS})
                </Text>
                <Text
                  fontSize="sm"
                  fontWeight={leftTableNode ? 'medium' : 'normal'}
                  isTruncated
                  title={leftTableNode?.data.label || node.data.config?.leftTable || 'Not connected'}
                >
                  {leftTableNode?.data.label || node.data.config?.leftTable || 'Not connected'}
                </Text>
                {leftTableNode && leftTableNode.data.config?.tableName && (
                  <Text fontSize="xs" color="gray.400" mt={0.5} isTruncated>
                    {leftTableNode.data.config.tableName}
                    {leftTableNode.data.config.schema && ` (${leftTableNode.data.config.schema})`}
                  </Text>
                )}
                {leftColumns.length > 0 && (
                  <Text fontSize="xs" color="green.500" mt={0.5}>
                    {leftColumns.length} column{leftColumns.length !== 1 ? 's' : ''} loaded
                </Text>
                )}
              </Box>
              
              {/* Swap Button - Centered between tables */}
              <Box
                display="flex"
                justifyContent="center"
                alignItems="center"
              >
                <Tooltip label="Swap Left and Right Tables" placement="top">
                  <IconButton
                    aria-label="Swap tables"
                    icon={<ArrowLeftRight size={16} />}
                    size="sm"
                    variant="outline"
                    colorScheme="blue"
                    onClick={swapTables}
                    isDisabled={!leftTableNode || !rightTableNode}
                  />
                </Tooltip>
              </Box>

              {/* Right Table */}
              <Box
                p={3}
                bg={useColorModeValue('gray.50', 'gray.700')}
                borderRadius="md"
                w="100%"
                minW={0}
              >
                <Text fontSize="xs" color="gray.500" mb={1}>
                  Right Table ({RIGHT_ALIAS})
                </Text>
                <Text
                  fontSize="sm"
                  fontWeight={rightTableNode ? 'medium' : 'normal'}
                  isTruncated
                  title={rightTableNode?.data.label || node.data.config?.rightTable || 'Not connected'}
                >
                  {rightTableNode?.data.label || node.data.config?.rightTable || 'Not connected'}
                </Text>
                {rightTableNode && rightTableNode.data.config?.tableName && (
                  <Text fontSize="xs" color="gray.400" mt={0.5} isTruncated>
                    {rightTableNode.data.config.tableName}
                    {rightTableNode.data.config.schema && ` (${rightTableNode.data.config.schema})`}
                  </Text>
                )}
                {rightColumns.length > 0 && (
                  <Text fontSize="xs" color="green.500" mt={0.5}>
                    {rightColumns.length} column{rightColumns.length !== 1 ? 's' : ''} loaded
                </Text>
                )}
              </Box>
            </Grid>
          </Box>

          <Divider />

          {/* Join Conditions */}
          <Box w="100%" minW={0}>
            <Box
              p={2}
              borderWidth="1px"
              borderColor={borderColor}
              borderRadius="md"
              bg={useColorModeValue('gray.50', 'gray.800')}
              cursor="pointer"
              onClick={() => setIsConditionsPanelOpen(!isConditionsPanelOpen)}
              _hover={{
                bg: useColorModeValue('gray.100', 'gray.700'),
              }}
            >
              <HStack justify="space-between" w="100%" minW={0}>
                <HStack spacing={2} flex={1}>
                  <IconButton
                    aria-label={isConditionsPanelOpen ? "Collapse" : "Expand"}
                    icon={isConditionsPanelOpen ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                    size="xs"
                    variant="ghost"
                    onClick={(e) => {
                      e.stopPropagation()
                      setIsConditionsPanelOpen(!isConditionsPanelOpen)
                    }}
                  />
                  <Text fontSize="sm" fontWeight="semibold">
                    Join Conditions
                  </Text>
                  <Badge size="sm" colorScheme="blue">
                    {conditions.filter(c => c.leftColumn && c.rightColumn).length}
                  </Badge>
                </HStack>
                {joinType !== 'CROSS' && (
                  <Button 
                    size="xs" 
                    leftIcon={<Plus size={12} />} 
                    onClick={(e) => {
                      e.stopPropagation()
                      addCondition()
                    }}
                  >
                    Add
                  </Button>
                )}
              </HStack>
            </Box>

            {isConditionsPanelOpen && (
              <Box
                mt={2}
                p={3}
                borderWidth="1px"
                borderColor={borderColor}
                borderRadius="md"
                bg={useColorModeValue('white', 'gray.800')}
              >
                {joinType === 'CROSS' ? (
                  <Alert status="info" size="sm">
                    <AlertIcon />
                    CROSS JOIN does not require conditions
                  </Alert>
                ) : conditions.length === 0 ? (
                  <Box textAlign="center" py={4}>
                    <Text fontSize="sm" color="gray.500" mb={2}>
                      No join conditions defined
                    </Text>
                    <Button size="sm" leftIcon={<Plus />} onClick={addCondition}>
                      Add Condition
                    </Button>
                  </Box>
                ) : (
                  <VStack align="stretch" spacing={2} w="100%" minW={0}>
                    {/* Render saved conditions */}
                    {conditions.map((condition, index) => (
                      <Box
                        key={condition.id}
                        p={3}
                        borderWidth="1px"
                        borderColor={borderColor}
                        borderRadius="md"
                        bg={useColorModeValue('white', 'gray.800')}
                        w="100%"
                        minW={0}
                      >
                        <HStack justify="space-between" mb={2}>
                          <Text fontSize="xs" fontWeight="semibold">
                            Condition {index + 1}
                          </Text>
                          <IconButton
                            aria-label="Remove condition"
                            icon={<X size={12} />}
                            size="xs"
                            variant="ghost"
                            colorScheme="red"
                            onClick={() => removeCondition(condition.id)}
                          />
                        </HStack>

                        <VStack align="stretch" spacing={2} w="100%" minW={0}>
                          <FormControl w="100%" minW={0}>
                            <FormLabel fontSize="xs">Left Column ({LEFT_ALIAS})</FormLabel>
                            <Select
                              size="sm"
                              value={(() => {
                                const i = leftColumnTechnicalNames.indexOf(condition.leftColumn)
                                if (i >= 0) return condition.leftColumn
                                const j = leftColumns.indexOf(condition.leftColumn)
                                if (j >= 0) return leftColumnTechnicalNames[j] ?? condition.leftColumn
                                return condition.leftColumn
                              })()}
                              onChange={(e) =>
                                updateCondition(condition.id, { leftColumn: e.target.value })
                              }
                              placeholder="Select column"
                              w="100%"
                            >
                              {leftColumns.map((col, idx) => (
                                <option key={leftColumnTechnicalNames[idx] ?? col} value={leftColumnTechnicalNames[idx] ?? col}>
                                  {formatColumnWithAlias(col, 'left')}
                                </option>
                              ))}
                            </Select>
                          </FormControl>

                          <FormControl w="100%" minW={0}>
                            <FormLabel fontSize="xs">Operator</FormLabel>
                            <Select
                              size="sm"
                              value={condition.operator || '='}
                              onChange={(e) =>
                                updateCondition(condition.id, { operator: e.target.value })
                              }
                              w="100%"
                            >
                              <option value="=">=</option>
                              <option value="!=">!=</option>
                              <option value=">">&gt;</option>
                              <option value="<">&lt;</option>
                              <option value=">=">&gt;=</option>
                              <option value="<=">&lt;=</option>
                            </Select>
                          </FormControl>

                          <FormControl w="100%" minW={0}>
                            <FormLabel fontSize="xs">Right Column ({RIGHT_ALIAS})</FormLabel>
                            <Select
                              size="sm"
                              value={(() => {
                                const i = rightColumnTechnicalNames.indexOf(condition.rightColumn)
                                if (i >= 0) return condition.rightColumn
                                const j = rightColumns.indexOf(condition.rightColumn)
                                if (j >= 0) return rightColumnTechnicalNames[j] ?? condition.rightColumn
                                return condition.rightColumn
                              })()}
                              onChange={(e) =>
                                updateCondition(condition.id, { rightColumn: e.target.value })
                              }
                              placeholder={rightColumns.length > 0 ? "Select column" : "No columns available"}
                              isDisabled={rightColumns.length === 0}
                              w="100%"
                            >
                              {rightColumns.length === 0 ? (
                                <option value="">{rightTableNode ? "Loading columns..." : "Connect right table first"}</option>
                              ) : (
                                rightColumns.map((col, idx) => (
                                <option key={rightColumnTechnicalNames[idx] ?? col} value={rightColumnTechnicalNames[idx] ?? col}>
                                  {formatColumnWithAlias(col, 'right')}
                                </option>
                                ))
                              )}
                            </Select>
                          </FormControl>
                        </VStack>
                      </Box>
                    ))}
                  </VStack>
                )}
              </Box>
            )}
          </Box>

          <Divider />

          {/* Output Columns */}
          <Box w="100%" minW={0}>
            <Box
              p={2}
              borderWidth="1px"
              borderColor={borderColor}
              borderRadius="md"
              bg={useColorModeValue('gray.50', 'gray.800')}
              cursor="pointer"
              onClick={() => setIsColumnsPanelOpen(!isColumnsPanelOpen)}
              _hover={{
                bg: useColorModeValue('gray.100', 'gray.700'),
              }}
              mb={2}
            >
              <HStack justify="space-between" w="100%" minW={0}>
                <HStack spacing={2} flex={1}>
                  <IconButton
                    aria-label={isColumnsPanelOpen ? "Collapse" : "Expand"}
                    icon={isColumnsPanelOpen ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                    size="xs"
                    variant="ghost"
                    onClick={(e) => {
                      e.stopPropagation()
                      setIsColumnsPanelOpen(!isColumnsPanelOpen)
                    }}
                  />
                  <Text fontSize="sm" fontWeight="semibold">
                    Output Columns
                  </Text>
                  <Badge size="sm" colorScheme="green">
                    {outputColumns.filter(c => c.included).length} / {outputColumns.length}
                  </Badge>
                </HStack>
                {/* Save button removed — auto-save handles persisting to Zustand store */}
              </HStack>
            </Box>

            {isColumnsPanelOpen && (
              <Box w="100%" minW={0}>
                {outputColumns.length === 0 ? (
                  <Alert status="info" size="sm" mb={3}>
                    <AlertIcon />
                    <Text fontSize="xs">
                      Connect both tables to configure output columns
                    </Text>
                  </Alert>
                ) : (
                  <Grid templateColumns="1fr 1fr" gap={3} w="100%" minW={0}>
                    {/* Left Side: Tabs for Left/Right */}
                    <Box w="100%" minW={0}>
                      <Tabs index={activeTab} onChange={setActiveTab} colorScheme="blue" size="sm">
                        <TabList>
                          <Tab fontSize="xs">
                            Left ({LEFT_ALIAS})
                            <Badge size="sm" colorScheme="blue" ml={2}>
                              {outputColumns.filter(c => c.source === 'left' && c.included).length} / {outputColumns.filter(c => c.source === 'left').length}
                            </Badge>
                          </Tab>
                          <Tab fontSize="xs">
                            Right ({RIGHT_ALIAS})
                            <Badge size="sm" colorScheme="purple" ml={2}>
                              {outputColumns.filter(c => c.source === 'right' && c.included).length} / {outputColumns.filter(c => c.source === 'right').length}
                            </Badge>
                          </Tab>
                        </TabList>
                        
                        {/* Search Input for Left/Right */}
                        <Box p={2} borderBottomWidth="1px" borderColor={borderColor}>
                          <InputGroup size="sm">
                            <InputLeftElement pointerEvents="none">
                              <Search size={14} />
                            </InputLeftElement>
                            <Input
                              placeholder="Search columns..."
                              value={searchTerm}
                              onChange={(e) => setSearchTerm(e.target.value)}
                            />
                          </InputGroup>
                        </Box>

                        <TabPanels>
                          {/* Left Tab Panel */}
                          <TabPanel p={2}>
                            <Box
                              borderWidth="1px"
                              borderColor={borderColor}
                              borderRadius="md"
                              bg={useColorModeValue('gray.50', 'gray.800')}
                              display="flex"
                              flexDirection="column"
                              maxH="400px"
                            >
                              <Box flex={1} overflowY="auto" p={2}>
                                <VStack align="stretch" spacing={2} w="100%" minW={0}>
                                  {outputColumns
                                    .filter(c => c.source === 'left')
                                    .filter(c => {
                                      if (!searchTerm) return true
                                      const searchLower = searchTerm.toLowerCase()
                                      return c.column.toLowerCase().includes(searchLower) ||
                                             (c.outputName || getOutputName(c.column, 'left', new Set(leftColumns), new Set(rightColumns))).toLowerCase().includes(searchLower)
                                    })
                                    .map((col) => (
                                      <HStack key={`left-${col.column}`} spacing={2} w="100%" minW={0} align="center">
                                        <Checkbox
                                          isChecked={col.included}
                                          onChange={() => toggleOutputColumn('left', col.column)}
                                          size="sm"
                                        />
                                        <Text fontSize="xs" flex="1" minW={0} isTruncated title={formatColumnWithAlias(col.column, 'left')}>
                                          {formatColumnWithAlias(col.column, 'left')}
                                        </Text>
                                        <Text fontSize="xs" color="gray.500" flexShrink={0}>
                                          →
                                        </Text>
                                        <Input
                                          size="xs"
                                          value={col.outputName || getOutputName(col.column, 'left', new Set(leftColumns), new Set(rightColumns))}
                                          onChange={(e) => {
                                            const newName = e.target.value
                                            if (newName !== col.outputName) {
                                              updateOutputColumnName('left', col.column, newName)
                                            }
                                          }}
                                          onBlur={(e) => {
                                            const newName = e.target.value.trim()
                                            if (!newName) {
                                              const defaultName = getOutputName(col.column, 'left', new Set(leftColumns), new Set(rightColumns))
                                              updateOutputColumn('left', col.column, { outputName: defaultName })
                                            }
                                          }}
                                          placeholder={getOutputName(col.column, 'left', new Set(leftColumns), new Set(rightColumns))}
                                          flex="1"
                                          minW={0}
                                          fontSize="xs"
                                          bg={useColorModeValue('white', 'gray.700')}
                                        />
                                      </HStack>
                                    ))}
                                </VStack>
                              </Box>
                            </Box>
                          </TabPanel>

                          {/* Right Tab Panel */}
                          <TabPanel p={2}>
                            <Box
                              borderWidth="1px"
                              borderColor={borderColor}
                              borderRadius="md"
                              bg={useColorModeValue('gray.50', 'gray.800')}
                              display="flex"
                              flexDirection="column"
                              maxH="400px"
                            >
                              <Box flex={1} overflowY="auto" p={2}>
                                <VStack align="stretch" spacing={2} w="100%" minW={0}>
                                  {outputColumns
                                    .filter(c => c.source === 'right')
                                    .filter(c => {
                                      if (!searchTerm) return true
                                      const searchLower = searchTerm.toLowerCase()
                                      return c.column.toLowerCase().includes(searchLower) ||
                                             (c.outputName || getOutputName(c.column, 'right', new Set(leftColumns), new Set(rightColumns))).toLowerCase().includes(searchLower)
                                    })
                                    .map((col) => (
                                      <HStack key={`right-${col.column}`} spacing={2} w="100%" minW={0} align="center">
                                        <Checkbox
                                          isChecked={col.included}
                                          onChange={() => toggleOutputColumn('right', col.column)}
                                          size="sm"
                                        />
                                        <Text fontSize="xs" flex="1" minW={0} isTruncated title={formatColumnWithAlias(col.column, 'right')}>
                                          {formatColumnWithAlias(col.column, 'right')}
                                        </Text>
                                        <Text fontSize="xs" color="gray.500" flexShrink={0}>
                                          →
                                        </Text>
                                        <Input
                                          size="xs"
                                          value={col.outputName || getOutputName(col.column, 'right', new Set(leftColumns), new Set(rightColumns))}
                                          onChange={(e) => {
                                            const newName = e.target.value
                                            if (newName !== col.outputName) {
                                              updateOutputColumnName('right', col.column, newName)
                                            }
                                          }}
                                          onBlur={(e) => {
                                            const newName = e.target.value.trim()
                                            if (!newName) {
                                              const defaultName = getOutputName(col.column, 'right', new Set(leftColumns), new Set(rightColumns))
                                              updateOutputColumn('right', col.column, { outputName: defaultName })
                                            }
                                          }}
                                          placeholder={getOutputName(col.column, 'right', new Set(leftColumns), new Set(rightColumns))}
                                          flex="1"
                                          minW={0}
                                          fontSize="xs"
                                          bg={useColorModeValue('white', 'gray.700')}
                                        />
                                      </HStack>
                                    ))}
                                </VStack>
                              </Box>
                            </Box>
                          </TabPanel>
                        </TabPanels>
                      </Tabs>
                    </Box>

                    {/* Right Side: Selected Columns (Order) */}
                    <Box w="100%" minW={0}>
                      <Box
                        borderWidth="1px"
                        borderColor={borderColor}
                        borderRadius="md"
                        bg={useColorModeValue('gray.50', 'gray.800')}
                        display="flex"
                        flexDirection="column"
                        maxH="500px"
                      >
                        {/* Header */}
                        <Box
                          p={2}
                          borderBottomWidth="1px"
                          borderColor={borderColor}
                          bg={useColorModeValue('green.50', 'green.900')}
                        >
                          <HStack justify="space-between" w="100%" minW={0} mb={selectedOutputColumns.size > 0 ? 2 : 0}>
                            <HStack spacing={2} flex={1}>
                              <Text fontSize="sm" fontWeight="semibold">
                                Selected Columns
                              </Text>
                              <Badge size="sm" colorScheme="green">
                                {outputColumnsOrder.length}
                              </Badge>
                            </HStack>
                            {/* Selection toolbar */}
                            {selectedOutputColumns.size > 0 && (
                              <HStack spacing={1} flexWrap="wrap">
                                <Tooltip label="Move Up">
                                  <IconButton
                                    icon={<ArrowUp size={12} />}
                                    size="xs"
                                    colorScheme="blue"
                                    aria-label="Move up"
                                    onClick={() => moveSelectedColumns('up')}
                                    isDisabled={!canMoveSelectedColumns('up')}
                                  />
                                </Tooltip>
                                <Tooltip label="Move Down">
                                  <IconButton
                                    icon={<ArrowDown size={12} />}
                                    size="xs"
                                    colorScheme="blue"
                                    aria-label="Move down"
                                    onClick={() => moveSelectedColumns('down')}
                                    isDisabled={!canMoveSelectedColumns('down')}
                                  />
                                </Tooltip>
                                <Tooltip label="Move to Top">
                                  <IconButton
                                    icon={<ArrowUpToLine size={12} />}
                                    size="xs"
                                    colorScheme="blue"
                                    aria-label="Move to top"
                                    onClick={() => moveSelectedColumns('top')}
                                    isDisabled={!canMoveSelectedColumns('top')}
                                  />
                                </Tooltip>
                                <Tooltip label="Move to Bottom">
                                  <IconButton
                                    icon={<ArrowDownToLine size={12} />}
                                    size="xs"
                                    colorScheme="blue"
                                    aria-label="Move to bottom"
                                    onClick={() => moveSelectedColumns('bottom')}
                                    isDisabled={!canMoveSelectedColumns('bottom')}
                                  />
                                </Tooltip>
                              </HStack>
                            )}
                          </HStack>
                        </Box>

                        {/* Search Input for Order */}
                        <Box p={2} borderBottomWidth="1px" borderColor={borderColor}>
                          <InputGroup size="sm">
                            <InputLeftElement pointerEvents="none">
                              <Search size={14} />
                            </InputLeftElement>
                            <Input
                              placeholder="Search order columns..."
                              value={orderSearchTerm}
                              onChange={(e) => setOrderSearchTerm(e.target.value)}
                            />
                          </InputGroup>
                        </Box>

                        <Box flex={1} overflowY="auto" p={2}>
                          {outputColumnsOrder.length === 0 ? (
                            <Text fontSize="xs" color={useColorModeValue('gray.500', 'gray.400')} textAlign="center" py={4}>
                              Select columns from Left/Right tables
                            </Text>
                          ) : (
                            <DndContext
                              sensors={sensors}
                              collisionDetection={closestCenter}
                              onDragEnd={handleDragEnd}
                            >
                              <SortableContext
                                items={outputColumnsOrder}
                                strategy={verticalListSortingStrategy}
                              >
                                <VStack align="stretch" spacing={1} w="100%" minW={0}>
                                  {outputColumnsOrder
                                    .filter(columnKey => {
                                      if (!orderSearchTerm) return true
                                      const [source, column] = columnKey.split(':')
                                      const col = outputColumns.find(c => c.source === source && c.column === column)
                                      if (!col) return false
                                      const searchLower = orderSearchTerm.toLowerCase()
                                      const displayName = col.outputName || getOutputName(col.column, col.source as 'left' | 'right', new Set(leftColumns), new Set(rightColumns))
                                      return column.toLowerCase().includes(searchLower) || displayName.toLowerCase().includes(searchLower)
                                    })
                                    .map((columnKey) => {
                                      const [source, column] = columnKey.split(':')
                                      const col = outputColumns.find(c => c.source === source && c.column === column)
                                      if (!col || !col.included) return null

                                      // Recalculate index after filtering
                                      const filteredOrder = outputColumnsOrder.filter(key => {
                                        if (!orderSearchTerm) return true
                                        const [s, c] = key.split(':')
                                        const co = outputColumns.find(oc => oc.source === s && oc.column === c)
                                        if (!co) return false
                                        const searchLower = orderSearchTerm.toLowerCase()
                                        const displayName = co.outputName || getOutputName(co.column, co.source as 'left' | 'right', new Set(leftColumns), new Set(rightColumns))
                                        return c.toLowerCase().includes(searchLower) || displayName.toLowerCase().includes(searchLower)
                                      })
                                      const actualIndex = filteredOrder.indexOf(columnKey)

                                      return (
                                        <SortableColumnItem
                                          key={columnKey}
                                          columnKey={columnKey}
                                          col={col}
                                          index={actualIndex}
                                          leftColumns={leftColumns}
                                          rightColumns={rightColumns}
                                          isSelected={selectedOutputColumns.has(columnKey)}
                                          onToggleSelect={() => {
                                            const newSelected = new Set(selectedOutputColumns)
                                            if (selectedOutputColumns.has(columnKey)) {
                                              newSelected.delete(columnKey)
                                            } else {
                                              newSelected.add(columnKey)
                                            }
                                            setSelectedOutputColumns(newSelected)
                                          }}
                                        />
                                      )
                                    })}
                                </VStack>
                              </SortableContext>
                            </DndContext>
                          )}
                        </Box>
                      </Box>
                    </Box>
                  </Grid>
                )}
              </Box>
            )}
          </Box>
        </VStack>
      </Box>

    </Box>
  )
}

