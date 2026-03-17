/**
 * Projection Configuration Panel Component
 * Simplified to EXCLUDE-only mode: users mark columns to exclude, remaining columns are included
 */
import React, { useState, useEffect, useMemo, useRef } from 'react'
import {
  Box,
  VStack,
  HStack,
  Text,
  Button,
  Input,
  InputGroup,
  InputLeftElement,
  Alert,
  AlertIcon,
  useColorModeValue,
  Badge,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
  FormControl,
  FormLabel,
  Textarea,
  Select,
  IconButton,
  Tooltip,
  Accordion,
  AccordionItem,
  AccordionButton,
  AccordionPanel,
  AccordionIcon,
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalBody,
  ModalCloseButton,
  ModalFooter,
  useDisclosure,
  Code as ChakraCode,
  Portal,
} from '@chakra-ui/react'
import { Search, GripVertical, Lock, Plus, Minus, X, Code, CheckCircle, AlertCircle, Type, Hash, CheckSquare, Calendar, Clock, Braces, ArrowUp, ArrowDown, ArrowUpToLine, ArrowDownToLine, GitBranch, RefreshCw } from 'lucide-react'
import { Node, Edge } from 'reactflow'
import { useCanvasStore } from '../../../store/canvasStore'
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

interface ProjectionConfigPanelProps {
  node: Node | null
    onUpdate: (nodeId: string, config: any) => void
  /** When user asks to show lineage for a column, highlight the path from projection back to sources */
  onLineageHighlight?: (path: { nodeIds: string[]; edgeIds: string[] } | null) => void
  /** When user asks to propagate schema changes downstream from this projection */
  onPropagateDownstream?: () => void
}

interface ColumnMetadata {
  /** Human-friendly business name used for display. */
  name: string
  /** Explicit business name; falls back to `name` when not provided. */
  business_name?: string
  /** Stable identifier for lineage and config; use when persisting. */
  technical_name?: string
  /** Actual DB column name; used for fetch / pushdown. */
  db_name?: string
  datatype?: string
  nullable?: boolean
  isPrimaryKey?: boolean
}

interface CalculatedColumn {
  id: string
  name: string
  expression: string
  dataType: string
}

interface ValidationResult {
  success: boolean
  errors: string[]
  inferred_type?: string
}

interface CalculatedColumnWithValidation extends CalculatedColumn {
  validation?: ValidationResult
  isValidating?: boolean
  tests?: ExpressionTest[]
  testResults?: ExpressionTestResult[]
  isRunningTests?: boolean
}

interface ExpressionTest {
  inputRow: Record<string, any>
  expected: any
  description: string
}

interface ExpressionTestResult {
  test: ExpressionTest
  passed: boolean
  actual?: any
  error?: string
  diff?: string
  debug_steps?: Array<{
    stage: string
    input: any
    output: any
    args?: any[]
    depth?: number
    error?: string
  }>
}

const dataTypes = [
  'STRING', 'INTEGER', 'DECIMAL', 'DATE', 'DATETIME', 'BOOLEAN'
]

/**
 * Get column type icon and tooltip based on datatype
 * Returns icon component, tooltip text, and color scheme
 */
const getColumnTypeIcon = (datatype?: string): { icon: React.ComponentType<any>, tooltip: string, color: string } => {
  if (!datatype) {
    return { icon: Type, tooltip: 'Type: Unknown', color: 'gray' }
  }

  const normalizedType = datatype.toUpperCase()
  const originalType = datatype // Keep original for tooltip

  // String/Text types - varchar, string, text, char
  if (normalizedType.includes('VARCHAR') || normalizedType.includes('STRING') ||
    normalizedType.includes('TEXT') || normalizedType.includes('CHAR')) {
    return { icon: Type, tooltip: `Type: ${originalType}`, color: 'blue' }
  }

  // Numeric types - int, number, float, decimal, numeric, real, double, bigint, smallint
  if (normalizedType.includes('INT') || normalizedType.includes('NUMBER') ||
    normalizedType.includes('FLOAT') || normalizedType.includes('DECIMAL') ||
    normalizedType.includes('NUMERIC') || normalizedType.includes('REAL') ||
    normalizedType.includes('DOUBLE') || normalizedType.includes('BIGINT') ||
    normalizedType.includes('SMALLINT')) {
    return { icon: Hash, tooltip: `Type: ${originalType}`, color: 'green' }
  }

  // Boolean types
  if (normalizedType.includes('BOOL')) {
    return { icon: CheckSquare, tooltip: `Type: ${originalType}`, color: 'purple' }
  }

  // Date types (but not datetime/timestamp)
  if (normalizedType.includes('DATE') && !normalizedType.includes('TIME') &&
    !normalizedType.includes('DATETIME') && !normalizedType.includes('TIMESTAMP')) {
    return { icon: Calendar, tooltip: `Type: ${originalType}`, color: 'orange' }
  }

  // Timestamp/DateTime types
  if (normalizedType.includes('TIMESTAMP') || normalizedType.includes('DATETIME') ||
    (normalizedType.includes('TIME') && !normalizedType.includes('DATE'))) {
    return { icon: Clock, tooltip: `Type: ${originalType}`, color: 'teal' }
  }

  // Variant/JSON/Object types
  if (normalizedType.includes('VARIANT') || normalizedType.includes('JSON') ||
    normalizedType.includes('OBJECT') || normalizedType.includes('ARRAY')) {
    return { icon: Braces, tooltip: `Type: ${originalType}`, color: 'pink' }
  }

  // Default to text if we can't determine
  return { icon: Type, tooltip: `Type: ${originalType}`, color: 'gray' }
}

interface FunctionDefinition {
  name: string
  description: string
  signature: string
  parameters: string[]
  example: string
}

const functions: FunctionDefinition[] = [
  {
    name: 'CONCAT',
    description: 'Concatenate strings together',
    signature: 'CONCAT(arg1, arg2, ...)',
    parameters: ['arg1: string', 'arg2: string', '...: additional strings'],
    example: 'CONCAT(status, is_history)'
  },
  {
    name: 'SUBSTRING',
    description: 'Extract substring from a string',
    signature: 'SUBSTRING(string, start, length)',
    parameters: ['string: source string', 'start: start position (1-based)', 'length: number of characters'],
    example: "SUBSTRING(name, 1, 5)"
  },
  {
    name: 'UPPER',
    description: 'Convert string to uppercase',
    signature: 'UPPER(string)',
    parameters: ['string: source string'],
    example: 'UPPER(status)'
  },
  {
    name: 'LOWER',
    description: 'Convert string to lowercase',
    signature: 'LOWER(string)',
    parameters: ['string: source string'],
    example: 'LOWER(status)'
  },
  {
    name: 'TRIM',
    description: 'Remove leading and trailing spaces',
    signature: 'TRIM(string)',
    parameters: ['string: source string'],
    example: 'TRIM(name)'
  },
  {
    name: 'COALESCE',
    description: 'Return first non-null value',
    signature: 'COALESCE(val1, val2, ...)',
    parameters: ['val1: first value', 'val2: second value', '...: additional values'],
    example: "COALESCE(status, 'unknown')"
  },
  {
    name: 'CAST',
    description: 'Convert value to specified data type',
    signature: 'CAST(value AS type)',
    parameters: ['value: value to convert', 'type: target data type (STRING, INTEGER, DECIMAL, DATE, etc.)'],
    example: "CAST(price AS STRING)"
  },
  {
    name: 'IF',
    description: 'Conditional expression',
    signature: 'IF(condition, true_value, false_value)',
    parameters: ['condition: boolean expression', 'true_value: value if true', 'false_value: value if false'],
    example: "IF(status = 'active', 'Yes', 'No')"
  },
  {
    name: 'CASE',
    description: 'Case statement for multiple conditions',
    signature: 'CASE WHEN condition THEN value ... ELSE default END',
    parameters: ['condition: boolean expression', 'value: return value', '...: additional WHEN clauses', 'default: default value'],
    example: "CASE WHEN status = 'A' THEN 'Active' ELSE 'Inactive' END"
  },
  {
    name: 'DATEADD',
    description: 'Add interval to date',
    signature: 'DATEADD(interval, amount, date)',
    parameters: ['interval: DAY, MONTH, YEAR', 'amount: number to add', 'date: date column'],
    example: "DATEADD('DAY', 7, created_date)"
  },
  {
    name: 'DATEDIFF',
    description: 'Calculate difference between dates',
    signature: 'DATEDIFF(interval, date1, date2)',
    parameters: ['interval: DAY, MONTH, YEAR', 'date1: first date', 'date2: second date'],
    example: "DATEDIFF('DAY', start_date, end_date)"
  },
]

const JOIN_LEFT_PREFIX = 'left_'
const JOIN_RIGHT_PREFIX = 'right_'

/** Get output column names from node metadata (output_metadata.columns, config.columns, config.output_columns). */
function getNodeOutputColumnNames (node: Node | undefined): Set<string> {
  if (!node?.data) return new Set()
  const data = node.data as any
  const names: string[] = []
  if (data.output_metadata?.columns && Array.isArray(data.output_metadata.columns)) {
    data.output_metadata.columns.forEach((c: any) => {
      names.push(typeof c === 'string' ? c : (c?.name ?? c?.column_name ?? ''))
    })
  }
  const config = data.config || {}
  if (config.columns && Array.isArray(config.columns)) {
    config.columns.forEach((c: any) => {
      const n = typeof c === 'string' ? c : (c?.name ?? c?.column_name ?? '')
      if (n) names.push(n)
    })
  }
  ;(config.output_columns || config.selectedColumns || config.includedColumns || []).forEach((n: string) => {
    if (n) names.push(n)
  })
  return new Set(names.filter(Boolean))
}

/** Compute lineage path for a specific column using node metadata: from projection backward, following only the branch that supplies this column (at joins, use saved metadata to pick left or right). */
function getLineagePathForColumn (
  columnName: string,
  startNodeId: string,
  nodes: Node[],
  edges: Edge[]
): { nodeIds: string[]; edgeIds: string[] } {
  const nodeIds: string[] = [startNodeId]
  const edgeIds: string[] = []
  const edgeSet = new Set<string>()
  const nodeSet = new Set<string>([startNodeId])

  function addPath(fromNodeId: string, colName: string): void {
    const incomingEdges = (edges || []).filter((e: Edge) => e.target === fromNodeId)
    if (incomingEdges.length === 0) return
    const targetNode = nodes.find((n: Node) => n.id === fromNodeId)
    const isJoin = targetNode?.data?.type === 'join'

    if (isJoin && incomingEdges.length >= 2) {
      const leftEdge = incomingEdges.find((e: Edge) => (e as any).targetHandle === 'left')
      const rightEdge = incomingEdges.find((e: Edge) => (e as any).targetHandle === 'right')
      const leftNode = leftEdge ? nodes.find((n: Node) => n.id === leftEdge.source) : undefined
      const rightNode = rightEdge ? nodes.find((n: Node) => n.id === rightEdge.source) : undefined
      const leftColumns = getNodeOutputColumnNames(leftNode)
      const rightColumns = getNodeOutputColumnNames(rightNode)
      const unprefixed = colName.startsWith(JOIN_LEFT_PREFIX)
        ? colName.slice(JOIN_LEFT_PREFIX.length)
        : colName.startsWith(JOIN_RIGHT_PREFIX)
          ? colName.slice(JOIN_RIGHT_PREFIX.length)
          : colName
      const fromLeftByPrefix = colName.startsWith(JOIN_LEFT_PREFIX)
      const fromRightByPrefix = colName.startsWith(JOIN_RIGHT_PREFIX)
      const fromLeftByMeta = leftColumns.has(unprefixed) || leftColumns.has(colName)
      const fromRightByMeta = rightColumns.has(unprefixed) || rightColumns.has(colName)
      const followLeft = fromLeftByPrefix || (!fromRightByPrefix && fromLeftByMeta && !fromRightByMeta)
      const followRight = fromRightByPrefix || (!fromLeftByPrefix && fromRightByMeta && !fromLeftByMeta)

      if (followLeft && leftEdge) {
        if (!edgeSet.has(leftEdge.id)) {
          edgeSet.add(leftEdge.id)
          edgeIds.push(leftEdge.id)
        }
        if (!nodeSet.has(leftEdge.source)) {
          nodeSet.add(leftEdge.source)
          nodeIds.push(leftEdge.source)
          addPath(leftEdge.source, unprefixed)
        }
      }
      if (followRight && rightEdge) {
        if (!edgeSet.has(rightEdge.id)) {
          edgeSet.add(rightEdge.id)
          edgeIds.push(rightEdge.id)
        }
        if (!nodeSet.has(rightEdge.source)) {
          nodeSet.add(rightEdge.source)
          nodeIds.push(rightEdge.source)
          addPath(rightEdge.source, unprefixed)
        }
      }
      if (!followLeft && !followRight) {
        if (leftEdge && !edgeSet.has(leftEdge.id)) {
          edgeSet.add(leftEdge.id)
          edgeIds.push(leftEdge.id)
          if (!nodeSet.has(leftEdge.source)) {
            nodeSet.add(leftEdge.source)
            nodeIds.push(leftEdge.source)
            addPath(leftEdge.source, unprefixed)
          }
        }
        if (rightEdge && !edgeSet.has(rightEdge.id)) {
          edgeSet.add(rightEdge.id)
          edgeIds.push(rightEdge.id)
          if (!nodeSet.has(rightEdge.source)) {
            nodeSet.add(rightEdge.source)
            nodeIds.push(rightEdge.source)
            addPath(rightEdge.source, unprefixed)
          }
        }
      }
    } else {
      const singleEdge = incomingEdges[0]
      if (singleEdge && !edgeSet.has(singleEdge.id)) {
        edgeSet.add(singleEdge.id)
        edgeIds.push(singleEdge.id)
        if (!nodeSet.has(singleEdge.source)) {
          nodeSet.add(singleEdge.source)
          nodeIds.push(singleEdge.source)
          addPath(singleEdge.source, colName)
        }
      }
    }
  }

  addPath(startNodeId, columnName)
  return { nodeIds, edgeIds }
}

export const ProjectionConfigPanel: React.FC<ProjectionConfigPanelProps> = ({
  node,
  onUpdate,
  onLineageHighlight,
  onPropagateDownstream,
}) => {
  const [availableColumns, setAvailableColumns] = useState<ColumnMetadata[]>([])
  const compiledGraph = useCanvasStore((s) => s.compiledGraph)
  const storeNodes = useCanvasStore((s) => s.nodes)
  const storeEdges = useCanvasStore((s) => s.edges)
  const [columnContextMenu, setColumnContextMenu] = useState<{ x: number; y: number; columnName: string } | null>(null)
  const columnContextMenuRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    if (!columnContextMenu) return
    const close = (e: MouseEvent) => {
      const el = e.target as HTMLElement
      if (columnContextMenuRef.current && !columnContextMenuRef.current.contains(el)) {
        setColumnContextMenu(null)
      }
    }
    document.addEventListener('click', close, true)
    return () => document.removeEventListener('click', close, true)
  }, [columnContextMenu])
  const [excludedColumns, setExcludedColumns] = useState<string[]>([]) // Columns to exclude
  const [includedColumnsOrder, setIncludedColumnsOrder] = useState<string[]>([]) // Ordered list of included columns (for reordering)
  const [businessName, setBusinessName] = useState<string>('')
  const [searchTerm, setSearchTerm] = useState('')
  const [loading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState(0) // 0 = Projection, 1 = Calculated Columns
  const [activeFieldTab, setActiveFieldTab] = useState<'included' | 'excluded' | 'all'>('all') // Field view tab - default to 'all'
  const [calculatedColumns, setCalculatedColumns] = useState<CalculatedColumnWithValidation[]>([])
  const [functionSearch, setFunctionSearch] = useState('')
  const [debugColumn, setDebugColumn] = useState<CalculatedColumnWithValidation | null>(null)
  const [selectedFields, setSelectedFields] = useState<Set<string>>(new Set())
  const [renderKey, setRenderKey] = useState(0) // Used to force re-renders when needed
  /** Source column name -> output (display) name; only entries where user renamed */
  const [columnOutputNames, setColumnOutputNames] = useState<Record<string, string>>({})
  /** When set, show inline input to rename this field (source column name) */
  const [editingFieldName, setEditingFieldName] = useState<string | null>(null)
  const { isOpen: isDebugOpen, onOpen: onOpenDebug, onClose: onCloseDebug } = useDisclosure()

  // Function to check if selected fields can be moved in a specific direction
  const canMoveSelectedFields = (direction: 'up' | 'down' | 'top' | 'bottom'): boolean => {
    if (selectedFields.size === 0) return false;

    const selectedFieldArray = Array.from(selectedFields);
    const includedFieldNames = includedColumns;

    if (direction === 'up') {
      // Can move up if at least one selected field is not at the top
      return selectedFieldArray.some(field => {
        const index = includedFieldNames.indexOf(field);
        return index > 0;
      });
    } else if (direction === 'down') {
      // Can move down if at least one selected field is not at the bottom
      return selectedFieldArray.some(field => {
        const index = includedFieldNames.indexOf(field);
        return index < includedFieldNames.length - 1;
      });
    } else if (direction === 'top') {
      // Can move to top if at least one selected field is not already at the top
      return selectedFieldArray.some(field => {
        const index = includedFieldNames.indexOf(field);
        return index > 0;
      });
    } else if (direction === 'bottom') {
      // Can move to bottom if at least one selected field is not already at the bottom
      return selectedFieldArray.some(field => {
        const index = includedFieldNames.indexOf(field);
        return index < includedFieldNames.length - 1;
      });
    }

    return false;
  };

  // Function to move selected fields
  const moveSelectedFields = (direction: 'up' | 'down' | 'top' | 'bottom') => {
    if (selectedFields.size === 0 || !node) return;

    let newOrder = [...includedColumns];

    if (direction === 'up') {
      // Move the entire selected block up by one position while preserving relative order
      newOrder = [...includedColumns];

      // Find positions of selected fields
      const selectedIndices = newOrder
        .map((name, index) => ({ name, index }))
        .filter(item => selectedFields.has(item.name))
        .sort((a, b) => a.index - b.index); // Sort by current position

      if (selectedIndices.length === 0) return;

      // Check if the first selected field is already at the top
      const firstSelectedIndex = selectedIndices[0].index;
      if (firstSelectedIndex === 0) return; // Cannot move up

      // Create a new array by removing the block and the item before it, then reinserting them in new positions
      const itemBeforeSelected = newOrder[firstSelectedIndex - 1];

      // Get selected items in their current order
      const selectedItemsInOrder = includedColumns.filter(field => selectedFields.has(field));

      // Create a copy excluding the selected items and the item before them
      const remainingItems = newOrder.filter(name => !selectedFields.has(name) && name !== itemBeforeSelected);

      // Insert selected items at the position where the "before" item was
      const insertPosition = firstSelectedIndex - 1;
      newOrder = [
        ...remainingItems.slice(0, insertPosition),
        ...selectedItemsInOrder,
        itemBeforeSelected,
        ...remainingItems.slice(insertPosition)
      ];
    } else if (direction === 'down') {
      // Move the entire selected block down by one position while preserving relative order
      // Strategy: swap the item immediately AFTER the block with the FIRST item of the block
      newOrder = [...includedColumns];

      // Find positions of selected fields (sorted by index)
      const selectedIndices = newOrder
        .map((name, index) => ({ name, index }))
        .filter(item => selectedFields.has(item.name))
        .sort((a, b) => a.index - b.index);

      if (selectedIndices.length === 0) return;

      // Check if the last selected field is already at the bottom
      const lastSelectedIndex = selectedIndices[selectedIndices.length - 1].index;
      if (lastSelectedIndex >= newOrder.length - 1) return; // Cannot move down

      // Get the first selected index
      const firstSelectedIndex = selectedIndices[0].index;

      // The item immediately after the selected block
      const itemAfterIndex = lastSelectedIndex + 1;
      const itemAfter = newOrder[itemAfterIndex];

      // Remove the item that's after the block
      newOrder.splice(itemAfterIndex, 1);
      // Insert it before the first selected item (effectively moving selected block down by 1)
      newOrder.splice(firstSelectedIndex, 0, itemAfter);
    } else if (direction === 'top') {
      // Move all selected fields to the top while preserving their relative order
      // Get selected items in their current order (not original order)
      const selectedInCurrentOrder = includedColumns.filter(field => selectedFields.has(field));
      const unselectedFields = includedColumns.filter(field => !selectedFields.has(field));
      newOrder = [...selectedInCurrentOrder, ...unselectedFields];
    } else if (direction === 'bottom') {
      // Move all selected fields to the bottom while preserving their relative order
      // Get selected items in their current order (not original order)
      const selectedInCurrentOrder = includedColumns.filter(field => selectedFields.has(field));
      const unselectedFields = includedColumns.filter(field => !selectedFields.has(field));
      newOrder = [...unselectedFields, ...selectedInCurrentOrder];
    }

    // Update state and persist changes
    setIncludedColumnsOrder(newOrder);

    // Update node config immediately with new order and column metadata
    const orderedProjectedColumns = newOrder;

    // Create column metadata with order numbers (0, 1, 2, ...); include outputName when renamed
    const columnsWithOrder = orderedProjectedColumns.map((colName, index) => {
      const colMeta = availableColumns.find(c => c.name === colName)
      const outputName = columnOutputNames[colName]?.trim()
      return {
        name: colName,
        type: colMeta?.datatype || 'TEXT',
        included: true,
        order: index,
        datatype: colMeta?.datatype || 'TEXT',
        nullable: colMeta?.nullable !== undefined ? colMeta.nullable : true,
        isPrimaryKey: colMeta?.isPrimaryKey || false,
        ...(outputName && outputName !== colName ? { outputName } : {}),
      }
    })

    // Add excluded columns with order = -1 (not in output order)
    const excludedColumnsWithOrder = excludedColumns.map((colName) => {
      const colMeta = availableColumns.find(c => c.name === colName)
      return {
        name: colName,
        type: colMeta?.datatype || 'TEXT',
        included: false,
        order: -1,
        datatype: colMeta?.datatype || 'TEXT',
        nullable: colMeta?.nullable !== undefined ? colMeta.nullable : true,
        isPrimaryKey: colMeta?.isPrimaryKey || false,
      }
    })

    // Build output metadata with new order (use output names for display/downstream)
    const outputMetadata = {
      columns: [
        // Projected columns in new order (use renamed output name when set)
        ...orderedProjectedColumns.map((colName) => {
          const colMeta = availableColumns.find(c => c.name === colName)
          const outputName = columnOutputNames[colName]?.trim() || colName
          return {
            name: outputName,
            business_name: outputName,
            technical_name: colMeta?.technical_name ?? colMeta?.db_name ?? colName,
            db_name: colMeta?.db_name,
            datatype: colMeta?.datatype || 'TEXT',
            nullable: colMeta?.nullable !== undefined ? colMeta.nullable : true,
            isPrimaryKey: colMeta?.isPrimaryKey || false,
          }
        }),
        // Calculated columns (if any)
        ...calculatedColumns
          .filter((c) => c.name.trim() && c.expression.trim())
          .map((calcCol) => ({
            name: calcCol.name.trim(),
            business_name: calcCol.name.trim(),
            technical_name: calcCol.name.trim(),
            datatype: calcCol.dataType || 'TEXT',
            nullable: true,
            isPrimaryKey: false,
          })),
      ],
      nodeId: node.data.node_id || node.id,
    }

    const outputNamesInOrder = [...orderedProjectedColumns.map(c => columnOutputNames[c]?.trim() || c), ...calculatedColumns.filter(c => c.name.trim() && c.expression.trim()).map(c => c.name.trim())]
    // Update node config immediately with new order and column metadata
    const updatedConfig = {
      ...node.data.config,
      excludedColumns: excludedColumns,
      includedColumns: orderedProjectedColumns, // Store ordered included columns (source names)
      output_columns: orderedProjectedColumns, // Explicit output columns in order (source names for backend)
      selectedColumns: orderedProjectedColumns, // Legacy support
      columns: outputNamesInOrder, // All output columns (output names for downstream)
      // Store column metadata with order for persistence
      columnOrder: columnsWithOrder.concat(excludedColumnsWithOrder), // All columns with order metadata (includes outputName when renamed)
    };

    // Update the config hash ref BEFORE calling onUpdate to prevent useEffect from resetting
    const newConfigHash = JSON.stringify({
      output_columns: orderedProjectedColumns,
      includedColumns: orderedProjectedColumns,
      excludedColumns: excludedColumns,
      calculatedColumns: calculatedColumns,
    });
    lastConfigHashRef.current = newConfigHash;

    // Persist immediately via onUpdate
    onUpdate(node.id, {
      config: updatedConfig,
      output_metadata: outputMetadata,
      // Preserve other node properties
      business_name: businessName || node.data.business_name || node.data.node_name || node.data.label || 'Projection',
      technical_name: node.data.technical_name,
      node_name: businessName || node.data.node_name || node.data.label || 'Projection',
    });

    // Clear selection after moving
    setSelectedFields(new Set());

    // Force a re-render to ensure UI updates properly
    setRenderKey(prev => prev + 1);
  };

  // Debounce timer ref for auto-validation
  const validationTimersRef = useRef<{ [key: string]: ReturnType<typeof setTimeout> }>({})

  // Validation function for calculated column expressions
  // Aggregate function names that are NOT allowed in Calculated Columns
  const aggregateFunctionNames = ['SUM', 'AVG', 'MIN', 'MAX', 'COUNT']

  const validateExpression = async (columnId: string) => {
    const column = calculatedColumns.find(c => c.id === columnId)
    if (!column || !column.expression.trim()) {
      return
    }

    // Check for aggregate functions in Calculated Columns (not allowed)
    const upperExpr = column.expression.toUpperCase()
    const foundAggregates = aggregateFunctionNames.filter(aggFunc => 
      upperExpr.includes(`${aggFunc}(`) || upperExpr.includes(`${aggFunc} `)
    )
    
    if (foundAggregates.length > 0) {
      setCalculatedColumns(prev =>
        prev.map(c => c.id === columnId ? {
          ...c,
          isValidating: false,
          validation: {
            success: false,
            errors: [`Aggregate functions (${foundAggregates.join(', ')}) are not allowed in Calculated Columns. Use the "Add Aggregates" option from the right-click menu for group-level functions.`]
          }
        } : c)
      )
      return
    }

    // Clear any existing timer for this column
    if (validationTimersRef.current[columnId]) {
      clearTimeout(validationTimersRef.current[columnId])
    }

    // Mark as validating
    setCalculatedColumns(prev =>
      prev.map(c => c.id === columnId ? { ...c, isValidating: true } : c)
    )

    try {
      const { api } = await import('../../../services/api')

      // Prepare available columns for validation
      const availableCols = availableColumns.map(col => ({
        name: col.name,
        datatype: col.datatype || 'TEXT',
      }))

      const response = await api.post('/api/validate-expression/', {
        expression: column.expression,
        expected_data_type: column.dataType,
        available_columns: availableCols,
      })

      const validationResult: ValidationResult = response.data

      // Check for auto-fix suggestions (optional feature)
      // If user selected STRING type and used '+' operator, suggest CONCAT
      if (!validationResult.success && column.dataType === 'STRING') {
        const expression = column.expression
        // Check if expression contains '+' but not CONCAT
        if (expression.includes('+') && !expression.toUpperCase().includes('CONCAT')) {
          // Extract potential operands for suggestion
          const plusMatch = expression.match(/(\w+)\s*\+\s*(\w+)/)
          if (plusMatch) {
            const [, left, right] = plusMatch
            // Add suggestion to errors if not already present
            if (!validationResult.errors.some(e => e.includes('CONCAT'))) {
              validationResult.errors.push(
                `Did you mean CONCAT(${left}, ${right})? Use CONCAT() for string concatenation.`
              )
            }
          }
        }
      }

      // Update validation state
      setCalculatedColumns(prev =>
        prev.map(c =>
          c.id === columnId
            ? {
              ...c,
              validation: validationResult,
              isValidating: false,
            }
            : c
        )
      )
    } catch (err: any) {
      const errorMessage = err.response?.data?.error || err.message || 'Validation failed'
      setCalculatedColumns(prev =>
        prev.map(c =>
          c.id === columnId
            ? {
              ...c,
              validation: {
                success: false,
                errors: [errorMessage],
              },
              isValidating: false,
            }
            : c
        )
      )
    }
  }

  // Check if all calculated columns are valid
  const allCalculatedColumnsValid = useMemo(() => {
    if (calculatedColumns.length === 0) return true

    return calculatedColumns.every(col => {
      // If expression is empty, it's not valid (but we don't block save if user hasn't validated)
      if (!col.expression.trim()) return true // Allow empty expressions (user might not have started)

      // If validation was run and failed, it's invalid
      if (col.validation && !col.validation.success) return false

      // If tests were run and any failed, it's invalid
      if (col.testResults && col.testResults.length > 0) {
        const hasFailures = col.testResults.some(result => !result.passed)
        if (hasFailures) return false

        // Check if any nested stage in test results has errors
        const hasNestedStageErrors = col.testResults.some(result => {
          // Check if any debug step has an error
          if (result.debug_steps && result.debug_steps.length > 0) {
            return result.debug_steps.some(step => step.error)
          }
          return false
        })
        if (hasNestedStageErrors) return false
      }

      // Otherwise, consider it valid (or not yet validated/tested)
      return true
    })
  }, [calculatedColumns])

  // Ref to track if we're in the middle of a drag operation (prevents useEffect from resetting order)
  const isDraggingRef = useRef(false)
  const lastNodeIdRef = useRef<string | null>(null)
  const lastConfigHashRef = useRef<string>('')
  // Ref to track last processed calculated columns to prevent infinite loops
  const lastProcessedCalculatedColumnsRef = useRef<string>('')
  // Additional refs to prevent infinite loops in calculated columns auto-add
  const isProcessingCalculatedColumnsRef = useRef(false)
  const lastProcessedCalculatedColumnsHashRef = useRef<string>('')
  // STATE-001: Ref to track if save is in progress (prevents recursive saves)
  const isSavingRef = useRef(false)
  // Ref to track last includedColumnsOrder hash to prevent unnecessary updates
  const lastIncludedColumnsOrderHashRef = useRef<string>('')
  // Ref to avoid re-running loadColumns when only store reference changed (prevents update loop)
  const lastLoadedSourceKeyRef = useRef<string>('')

  // DnD sensors for reordering included columns
  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  )

  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const headerBg = useColorModeValue('gray.50', 'gray.700')

  useEffect(() => {
    if (!node) return

    // Create a hash of the config to detect actual changes
    const config = node.data.config || {}
    const configHash = JSON.stringify({
      output_columns: config.output_columns,
      includedColumns: config.includedColumns,
      excludedColumns: config.excludedColumns,
      calculatedColumns: config.calculatedColumns,
    })

    // Only reload if:
    // 1. Node ID changed (different node selected)
    // 2. Config actually changed (not just a reference change) AND it's not our own update
    // 3. We're not in the middle of a drag operation
    const nodeIdChanged = lastNodeIdRef.current !== node.id
    const configChanged = lastConfigHashRef.current !== configHash

    // Check if this is our own update by comparing the new hash with what we last saved
    // If the new config hash matches what we set in handleDragEnd, don't reload
    // Also check if the output_columns in the new config match what we just saved
    const isOurOwnUpdate = !nodeIdChanged && lastConfigHashRef.current && configHash === lastConfigHashRef.current
    const outputColumnsMatch = !nodeIdChanged && lastConfigHashRef.current &&
      JSON.parse(lastConfigHashRef.current).output_columns?.join(',') === config.output_columns?.join(',')
    const isDefinitelyOurUpdate = isOurOwnUpdate || outputColumnsMatch

    // Also check if the config hash is empty (initial load) or if we're dragging
    const shouldReload = (nodeIdChanged || (configChanged && !isDefinitelyOurUpdate)) && !isDraggingRef.current

    console.log('[ProjectionConfig] useEffect check:', {
      nodeIdChanged,
      configChanged,
      isOurOwnUpdate,
      outputColumnsMatch,
      isDefinitelyOurUpdate,
      isDragging: isDraggingRef.current,
      shouldReload,
      currentHash: lastConfigHashRef.current?.substring(0, 50),
      newHash: configHash.substring(0, 50),
      savedOutputColumns: config.output_columns,
      lastSavedOutputColumns: lastConfigHashRef.current ? JSON.parse(lastConfigHashRef.current).output_columns : null,
    })

    if (shouldReload) {
      lastNodeIdRef.current = node.id
      lastConfigHashRef.current = configHash
      lastLoadedSourceKeyRef.current = '' // Reset so we load columns for the new node
      // Reset calculated columns ref when node changes
      const currentCalculatedColumns = config.calculatedColumns || []
      const currentCalculatedNames = currentCalculatedColumns
        .filter((c: any) => c.name && c.expression)
        .map((c: any) => c.name.trim())
        .sort()
      lastProcessedCalculatedColumnsRef.current = JSON.stringify(currentCalculatedNames)
      // Also reset the hash refs for the auto-add useEffect
      lastProcessedCalculatedColumnsHashRef.current = JSON.stringify(currentCalculatedNames)
      lastIncludedColumnsOrderHashRef.current = '' // Reset order hash to allow re-processing
      isProcessingCalculatedColumnsRef.current = false // Reset processing flag

      // Load saved state - always use EXCLUDE mode
      setExcludedColumns(config.excludedColumns || [])

      // Load included columns order from saved config
      // First try columnOrder (new format with order metadata), then fall back to legacy formats
      let savedOrder: string[] = []
      if (config.columnOrder && Array.isArray(config.columnOrder)) {
        // New format: columnOrder array with { name, included, order, outputName? }
        const includedCols = config.columnOrder
          .filter((col: any) => col.included !== false && col.order >= 0)
          .sort((a: any, b: any) => a.order - b.order)
          .map((col: any) => col.name)
        savedOrder = includedCols
        // Load renames: source name -> output name
        const renames: Record<string, string> = {}
        config.columnOrder.forEach((col: any) => {
          if (col.name && col.outputName && col.outputName.trim() !== '' && col.outputName !== col.name) {
            renames[col.name] = col.outputName.trim()
          }
        })
        setColumnOutputNames(renames)
        console.log('[ProjectionConfig] useEffect: Loaded order from columnOrder:', savedOrder, 'renames:', renames)
      } else {
        // Legacy format: prefer output_columns (explicit UI order), then includedColumns, then selectedColumns
        savedOrder = config.output_columns || config.includedColumns || config.selectedColumns || []
        setColumnOutputNames({}) // No renames in legacy format
        console.log('[ProjectionConfig] useEffect: Loaded order from legacy format:', savedOrder)
      }

      if (savedOrder.length > 0) {
        setIncludedColumnsOrder(savedOrder)
        console.log('[ProjectionConfig] useEffect: Set includedColumnsOrder to:', savedOrder)
      } else {
        // Only reset to empty if we don't have a saved order AND we don't have local state
        if (includedColumnsOrder.length === 0) {
          setIncludedColumnsOrder([])
        } else {
          console.log('[ProjectionConfig] useEffect: Preserving local includedColumnsOrder:', includedColumnsOrder)
        }
      }
      setBusinessName(node.data.business_name || node.data.node_name || node.data.label || '')
      // Load calculated columns
      const loadedCalculatedColumns = config.calculatedColumns || []
      setCalculatedColumns(loadedCalculatedColumns)
      
      // Ensure calculated columns are in includedColumnsOrder if they exist
      if (loadedCalculatedColumns.length > 0 && savedOrder.length > 0) {
        const validCalculatedNames = loadedCalculatedColumns
          .filter((c: any) => c.name && c.expression)
          .map((c: any) => c.name.trim())
        const missingCalculated = validCalculatedNames.filter((name: string) => !savedOrder.includes(name))
        if (missingCalculated.length > 0) {
          // Add missing calculated columns to the end
          const updatedOrder = [...savedOrder, ...missingCalculated]
          setIncludedColumnsOrder(updatedOrder)
          console.log('[ProjectionConfig] Added missing calculated columns to order:', missingCalculated)
        }
      }
    }

    // Load columns from compiledGraph (preferred — uses compiled input schema)
    if (node && compiledGraph) {
      const compiledNode = compiledGraph.nodes[node.id]
      // Only trust compiled input schema when it's non-empty; otherwise fall back to upstream metadata
      if (compiledNode && Array.isArray(compiledNode.inputSchema) && compiledNode.inputSchema.length > 0) {
        const newCols: ColumnMetadata[] = compiledNode.inputSchema.map((col) => ({
          // UI should always show the business/display name, never the technical_name
          name: col.outputName || col.column || col.name,
          business_name: col.outputName || col.column || col.name,
          // Preserve stable technical identifier separately
          technical_name: col.technical_name || col.column || col.name,
          db_name: col.column || col.name,
          datatype: col.datatype || 'TEXT',
          nullable: col.nullable ?? true,
          isPrimaryKey: false,
        }))

        setAvailableColumns((prev) => {
          if (
            prev.length === newCols.length &&
            prev.every((p, i) => p.name === newCols[i].name && p.datatype === newCols[i].datatype)
          ) {
            return prev
          }
          return newCols
        })
        setError(null)
        return
      }
    }

    // Fallback: derive columns directly from upstream node's output_metadata
    if (node && storeNodes.length && storeEdges.length) {
      const incoming = storeEdges.filter((e) => e.target === node.id)
      const upstreamId = incoming[0]?.source
      const upstream = upstreamId ? storeNodes.find((n) => n.id === upstreamId) : undefined
      const cols = (upstream?.data as any)?.output_metadata?.columns || []

      if (cols && cols.length) {
        const newCols: ColumnMetadata[] = cols.map((c: any) => ({
          name: String(c.name || c.column_name || c.technical_name || c.db_name || ''),
          business_name: String(c.business_name || c.name || c.column_name || c.technical_name || c.db_name || ''),
          technical_name: String(c.technical_name || c.db_name || c.column_name || c.name || ''),
          db_name: String(c.db_name || c.column_name || c.name || ''),
          datatype: String(c.datatype || c.type || c.data_type || 'TEXT'),
          nullable: c.nullable ?? true,
          isPrimaryKey: Boolean(c.isPrimaryKey || c.is_primary_key),
        })).filter((c) => c.name)

        setAvailableColumns((prev) => {
          if (
            prev.length === newCols.length &&
            prev.every((p, i) => p.name === newCols[i].name && p.datatype === newCols[i].datatype)
          ) {
            return prev
          }
          return newCols
        })
        setError(null)
        return
      }
    }

    if (node) {
      setError('No input node found. Please connect a source or transform node.')
    }
  }, [
    node?.id,
    node?.data?.config?.output_columns,
    node?.data?.config?.includedColumns,
    node?.data?.config?.excludedColumns,
    node?.data?.config?.calculatedColumns,
    compiledGraph,
    storeNodes,
    storeEdges,
  ])

  /**
   * EFFECT-001: Effects may READ state but MUST NOT WRITE state.
   * This effect only updates LOCAL state (includedColumnsOrder), never calls onUpdate().
   * Calculated columns are added to projection ONLY on explicit save action.
   */
  // Create a stable reference to valid calculated columns (prevents infinite loops)
  const savedCalculatedColumns = useMemo(() => {
    return calculatedColumns.filter(
      (c) => {
        // Only include columns with both name and expression
        return c.name.trim() && c.expression.trim()
      }
    )
  }, [calculatedColumns])

  // EFFECT-001 COMPLIANT: Only updates LOCAL state, never calls onUpdate()
  useEffect(() => {
    if (!node) return
    if (isDraggingRef.current) return // Don't update during drag operations
    if (availableColumns.length === 0) return // Wait for source columns to load
    if (isProcessingCalculatedColumnsRef.current) return // Prevent recursive calls

    // Use the stable SAVED columns reference
    const validCalculatedColumns = savedCalculatedColumns
    const calculatedColumnNames = validCalculatedColumns.map(c => c.name.trim())
    
    // Create a hash of calculated columns to detect actual changes
    const calculatedColumnsHash = JSON.stringify(calculatedColumnNames.sort())
    
    // Skip if we've already processed these calculated columns
    if (lastProcessedCalculatedColumnsHashRef.current === calculatedColumnsHash) {
      return
    }

    // CRITICAL FIX: Separate source columns from calculated columns
    // Source columns come first, calculated columns ALWAYS at the end
    const currentSourceColumns = includedColumnsOrder.filter(
      col => !calculatedColumnNames.includes(col)
    )

    // Get ALL calculated columns (both existing and new) - they should all be at the end
    const allCalculatedColumns = calculatedColumnNames.filter(
      name => calculatedColumnNames.includes(name) // All valid calculated columns
    )

    // CRITICAL FIX: Always ensure calculated columns are at the end
    // Build new order: source columns first, then ALL calculated columns at end
    const newOrder = [...currentSourceColumns, ...allCalculatedColumns]
    
    // Check if order needs updating (calculated columns might be in wrong position)
    const currentOrderHash = JSON.stringify(includedColumnsOrder)
    const newOrderHash = JSON.stringify(newOrder)
    
    // Also check if we've already set this exact order
    if (lastIncludedColumnsOrderHashRef.current === newOrderHash) {
      // Already processed this exact order, just mark calculated columns as processed
      lastProcessedCalculatedColumnsHashRef.current = calculatedColumnsHash
      return
    }
    
    // Only update if order actually changed
    if (currentOrderHash !== newOrderHash) {
      // Mark as processed BEFORE updating to prevent re-triggering
      lastProcessedCalculatedColumnsHashRef.current = calculatedColumnsHash
      lastIncludedColumnsOrderHashRef.current = newOrderHash
      
      // Use setTimeout to break the update cycle and prevent infinite loops
      setTimeout(() => {
        setIncludedColumnsOrder(newOrder)
      }, 0)
    } else {
      // Order is already correct, just mark as processed
      lastProcessedCalculatedColumnsHashRef.current = calculatedColumnsHash
      lastIncludedColumnsOrderHashRef.current = newOrderHash
    }
    // NOTE: Node update happens ONLY in handleSave() (explicit user action)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [savedCalculatedColumns, node?.id])

  /**
   * EFFECT-001 COMPLIANT: Auto-include all columns when node is first created.
   * Only updates LOCAL state, never calls onUpdate().
   * Node update happens ONLY on explicit save action.
   */
  useEffect(() => {
    if (!node) return
    if (availableColumns.length === 0) return

    const hasSavedOrder = (node.data.config?.output_columns && node.data.config.output_columns.length > 0) ||
      includedColumnsOrder.length > 0
    const hasExclusions = excludedColumns.length > 0

    if (hasSavedOrder || hasExclusions) return

    const allNames = availableColumns.map(c => c.name)
    // STATE-002: Only update LOCAL state, never call onUpdate() in effect
    setIncludedColumnsOrder(allNames)
    setExcludedColumns([])
    // NOTE: Node update happens ONLY in handleSave() (explicit user action)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [availableColumns, node?.id])


  // Toggle column exclusion (checked = include, unchecked = exclude)
  const toggleExcludeColumn = (columnName: string) => {
    const column = availableColumns.find(c => c.name === columnName)
    // Prevent excluding primary key columns
    if (column?.isPrimaryKey) {
      setError('Primary key columns cannot be excluded')
      setTimeout(() => setError(null), 3000)
      return
    }

    const isCurrentlyExcluded = excludedColumns.includes(columnName)

    let newIncluded: string[]
    let newExcluded: string[]

    if (isCurrentlyExcluded) {
      // Including: remove from excluded, append to the end of included order
      newExcluded = excludedColumns.filter(c => c !== columnName)
      newIncluded = [...includedColumnsOrder, columnName]

      setExcludedColumns(newExcluded)
      setIncludedColumnsOrder(newIncluded)
    } else {
      // Excluding: add to excluded, remove from included order
      newExcluded = [...excludedColumns, columnName]
      newIncluded = includedColumnsOrder.filter(c => c !== columnName)

      setExcludedColumns(newExcluded)
      setIncludedColumnsOrder(newIncluded)
    }

    // Immediately persist the change (including columnOrder so TableDataPanel sends correct columns)
    if (node) {
      const config = node.data.config || {}
      const existingColumnOrder = config.columnOrder || []
      const orderByName = new Map(
        existingColumnOrder.map((c: any) => [typeof c === 'string' ? c : c.name, c])
      )
      const calculatedNames = calculatedColumns.filter(c => c.name?.trim()).map(c => c.name.trim())
      const columnsWithOrder = newIncluded.map((colName, index) => {
        const existing = orderByName.get(colName)
        const colMeta = availableColumns.find(c => c.name === colName)
        const isCalc = calculatedNames.includes(colName)
        return typeof existing === 'object' && existing
          ? { ...existing, included: true, order: index, isCalculated: isCalc }
          : { name: colName, type: colMeta?.datatype || 'TEXT', included: true, order: index, isCalculated: isCalc }
      })
      const excludedWithOrder = newExcluded.map((colName) => {
        const existing = orderByName.get(colName)
        const colMeta = availableColumns.find(c => c.name === colName)
        return typeof existing === 'object' && existing
          ? { ...existing, included: false, order: -1, isCalculated: false }
          : { name: colName, type: colMeta?.datatype || 'TEXT', included: false, order: -1, isCalculated: false }
      })

      const updatedConfig = {
        ...config,
        excludedColumns: newExcluded,
        includedColumns: newIncluded,
        output_columns: newIncluded,
        selectedColumns: newIncluded,
        columnOrder: columnsWithOrder.concat(excludedWithOrder),
      }

      const outputMetadata = {
        columns: newIncluded.map((colName) => {
          const colMeta = availableColumns.find(c => c.name === colName)
          const businessName = colMeta?.business_name || colName
          const technicalName = colMeta?.technical_name ?? colMeta?.db_name ?? colName
          return {
            name: businessName,
            business_name: businessName,
            technical_name: technicalName,
            db_name: colMeta?.db_name,
            datatype: colMeta?.datatype || 'TEXT',
            nullable: colMeta?.nullable !== undefined ? colMeta.nullable : true,
            isPrimaryKey: colMeta?.isPrimaryKey || false,
          }
        }),
        nodeId: node.data.node_id || node.id,
      }

      onUpdate(node.id, {
        config: updatedConfig,
        output_metadata: outputMetadata,
        business_name: businessName || node.data.business_name || node.data.node_name || node.data.label || 'Projection',
        technical_name: node.data.technical_name,
        node_name: businessName || node.data.business_name || node.data.node_name || node.data.label || 'Projection',
      })
    }
  }

  // Bulk include selected excluded columns (Smart Propagation)
  const includeSelectedExcludedColumns = () => {
    const toInclude = Array.from(selectedFields).filter(
      (colName) =>
        excludedColumns.includes(colName) &&
        !availableColumns.find((c) => c.name === colName)?.isPrimaryKey
    )
    if (toInclude.length === 0 || !node) return

    const newExcluded = excludedColumns.filter((c) => !toInclude.includes(c))
    const newIncluded = [...includedColumnsOrder, ...toInclude]

    setExcludedColumns(newExcluded)
    setIncludedColumnsOrder(newIncluded)
    setSelectedFields(new Set())

    const config = node.data.config || {}
    const existingColumnOrder = config.columnOrder || []
    const orderByName = new Map(
      existingColumnOrder.map((c: any) => [typeof c === 'string' ? c : c.name, c])
    )
    const calculatedNames = calculatedColumns.filter((c) => c.name?.trim()).map((c) => c.name.trim())
    const columnsWithOrder = newIncluded.map((colName, index) => {
      const existing = orderByName.get(colName)
      const colMeta = availableColumns.find((c) => c.name === colName)
      const isCalc = calculatedNames.includes(colName)
      return typeof existing === 'object' && existing
        ? { ...existing, included: true, order: index, isCalculated: isCalc }
        : { name: colName, type: colMeta?.datatype || 'TEXT', included: true, order: index, isCalculated: isCalc }
    })
    const excludedWithOrder = newExcluded.map((colName) => {
      const existing = orderByName.get(colName)
      const colMeta = availableColumns.find((c) => c.name === colName)
      return typeof existing === 'object' && existing
        ? { ...existing, included: false, order: -1, isCalculated: false }
        : { name: colName, type: colMeta?.datatype || 'TEXT', included: false, order: -1, isCalculated: false }
    })

    const updatedConfig = {
      ...config,
      excludedColumns: newExcluded,
      includedColumns: newIncluded,
      output_columns: newIncluded,
      selectedColumns: newIncluded,
      columnOrder: columnsWithOrder.concat(excludedWithOrder),
    }

    const outputMetadata = {
      columns: newIncluded.map((colName) => {
        const colMeta = availableColumns.find((c) => c.name === colName)
        const businessName = colMeta?.business_name || colName
        const technicalName = colMeta?.technical_name ?? colMeta?.db_name ?? colName
        return {
          name: businessName,
          business_name: businessName,
          technical_name: technicalName,
          db_name: colMeta?.db_name,
          datatype: colMeta?.datatype || 'TEXT',
          nullable: colMeta?.nullable !== undefined ? colMeta.nullable : true,
          isPrimaryKey: colMeta?.isPrimaryKey || false,
        }
      }),
      nodeId: node.data.node_id || node.id,
    }

    onUpdate(node.id, {
      config: updatedConfig,
      output_metadata: outputMetadata,
      business_name: businessName || node.data.business_name || node.data.node_name || node.data.label || 'Projection',
      technical_name: node.data.technical_name,
      node_name: businessName || node.data.business_name || node.data.node_name || node.data.label || 'Projection',
    })
  }

  // Bulk exclude selected included columns (respects primary keys; cannot exclude calculated columns)
  const excludeSelectedIncludedColumns = () => {
    const calcNames = new Set(calculatedColumns.filter((c) => c.name?.trim()).map((c) => c.name.trim()))
    const toExclude = Array.from(selectedFields).filter(
      (colName) =>
        includedColumnsOrder.includes(colName) &&
        !availableColumns.find((c) => c.name === colName)?.isPrimaryKey &&
        !calcNames.has(colName)
    )
    if (toExclude.length === 0 || !node) return

    const newExcluded = [...excludedColumns, ...toExclude]
    const newIncluded = includedColumnsOrder.filter((c) => !toExclude.includes(c))

    setExcludedColumns(newExcluded)
    setIncludedColumnsOrder(newIncluded)
    setSelectedFields(new Set())

    const config = node.data.config || {}
    const existingColumnOrder = config.columnOrder || []
    const orderByName = new Map(
      existingColumnOrder.map((c: any) => [typeof c === 'string' ? c : c.name, c])
    )
    const calculatedNames = calculatedColumns.filter((c) => c.name?.trim()).map((c) => c.name.trim())
    const columnsWithOrder = newIncluded.map((colName, index) => {
      const existing = orderByName.get(colName)
      const colMeta = availableColumns.find((c) => c.name === colName)
      const isCalc = calculatedNames.includes(colName)
      return typeof existing === 'object' && existing
        ? { ...existing, included: true, order: index, isCalculated: isCalc }
        : { name: colName, type: colMeta?.datatype || 'TEXT', included: true, order: index, isCalculated: isCalc }
    })
    const excludedWithOrder = newExcluded.map((colName) => {
      const existing = orderByName.get(colName)
      const colMeta = availableColumns.find((c) => c.name === colName)
      return typeof existing === 'object' && existing
        ? { ...existing, included: false, order: -1, isCalculated: false }
        : { name: colName, type: colMeta?.datatype || 'TEXT', included: false, order: -1, isCalculated: false }
    })

    const updatedConfig = {
      ...config,
      excludedColumns: newExcluded,
      includedColumns: newIncluded,
      output_columns: newIncluded,
      selectedColumns: newIncluded,
      columnOrder: columnsWithOrder.concat(excludedWithOrder),
    }

    const outputMetadata = {
      columns: newIncluded.map((colName) => {
        const colMeta = availableColumns.find((c) => c.name === colName)
        const businessName = colMeta?.business_name || colName
        const technicalName = colMeta?.technical_name ?? colMeta?.db_name ?? colName
        return {
          name: businessName,
          business_name: businessName,
          technical_name: technicalName,
          db_name: colMeta?.db_name,
          datatype: colMeta?.datatype || 'TEXT',
          nullable: colMeta?.nullable !== undefined ? colMeta.nullable : true,
          isPrimaryKey: colMeta?.isPrimaryKey || false,
        }
      }),
      nodeId: node.data.node_id || node.id,
    }

    onUpdate(node.id, {
      config: updatedConfig,
      output_metadata: outputMetadata,
      business_name: businessName || node.data.business_name || node.data.node_name || node.data.label || 'Projection',
      technical_name: node.data.technical_name,
      node_name: businessName || node.data.business_name || node.data.node_name || node.data.label || 'Projection',
    })
  }

  // Compute included columns (all columns minus excluded)
  // CRITICAL: Use includedColumnsOrder as the source of truth when it exists
  // This includes both source columns and calculated columns in the unified sequence
  const includedColumns = useMemo(() => {
    // Get valid calculated column names
    const validCalculatedColumns = calculatedColumns.filter(
      (c) => c.name.trim() && c.expression.trim()
    )
    const calculatedColumnNames = validCalculatedColumns.map(c => c.name.trim())

    // First, get all available source column names (excluding excluded columns)
    const allSourceIncluded = availableColumns
      .filter(col => !excludedColumns.includes(col.name))
      .map(col => col.name)

    // Combine source columns and calculated columns
    const allIncluded = [...allSourceIncluded, ...calculatedColumnNames]

    // If we have a saved order, use it as the primary source
    if (includedColumnsOrder.length > 0) {
      // Filter the saved order to only include columns that are still available (source) or valid calculated
      const ordered = includedColumnsOrder.filter(col => 
        allSourceIncluded.includes(col) || calculatedColumnNames.includes(col)
      )
      // Add any new columns that weren't in the saved order (append to end)
      const newColumns = allIncluded.filter(col => !ordered.includes(col))
      // Return: saved order first, then new columns
      return [...ordered, ...newColumns]
    }

    // No saved order - return all included columns (source first, then calculated)
    return allIncluded
  }, [availableColumns, excludedColumns, includedColumnsOrder, calculatedColumns])

  // Drag and drop reordering for included columns
  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event

    if (over && active.id !== over.id && node) {
      // Use includedColumns (which is the current displayed order) as the source
      // This ensures we're working with the actual displayed list
      const oldIndex = includedColumns.indexOf(active.id as string)
      const newIndex = includedColumns.indexOf(over.id as string)

      console.log(`[ProjectionConfig] Drag ended - active: ${active.id}, over: ${over.id}`)
      console.log(`[ProjectionConfig] Old index: ${oldIndex}, New index: ${newIndex}`)
      console.log(`[ProjectionConfig] Current includedColumns: ${includedColumns.join(', ')}`)

      if (oldIndex !== -1 && newIndex !== -1) {
        // Handle multi-selection drag if multiple fields are selected
        let newOrder = [...includedColumns];

        if (selectedFields.size > 1 && selectedFields.has(active.id as string)) {
          // Multi-selection drag: move the selected group while preserving relative order
          const selectedFieldNames = Array.from(selectedFields);

          // Find the new position relative to other selected fields
          const sortedSelected = selectedFieldNames.filter(field => includedColumns.includes(field))
            .sort((a, b) => includedColumns.indexOf(a) - includedColumns.indexOf(b));

          // Remove all selected items from the array
          const unselectedItems = includedColumns.filter(field => !selectedFieldNames.includes(field));

          // Find where to insert the selected items
          const targetIndex = unselectedItems.indexOf(over.id as string);
          if (targetIndex !== -1) {
            // Insert selected items at the target position
            if (oldIndex < newIndex) {
              // Moving down - insert after the target
              newOrder = [
                ...unselectedItems.slice(0, targetIndex + 1),
                ...sortedSelected,
                ...unselectedItems.slice(targetIndex + 1)
              ];
            } else {
              // Moving up - insert before the target
              newOrder = [
                ...unselectedItems.slice(0, targetIndex),
                ...sortedSelected,
                ...unselectedItems.slice(targetIndex)
              ];
            }
          } else {
            // Fallback: move to the new index
            const selectedItems = sortedSelected;
            const unselectedItems = includedColumns.filter(field => !selectedFieldNames.includes(field));

            // Remove selected items from their original positions
            const tempOrder = [...unselectedItems];

            // Insert at the new position
            if (newIndex < tempOrder.length) {
              tempOrder.splice(newIndex, 0, ...selectedItems);
            } else {
              tempOrder.push(...selectedItems);
            }

            newOrder = tempOrder;
          }

          console.log(`[ProjectionConfig] Multi-selection drag: ${sortedSelected.join(', ')} moved to index ${newIndex}`);
        } else {
          // Single item drag
          newOrder = arrayMove(includedColumns, oldIndex, newIndex);
          console.log(`[ProjectionConfig] Single item drag: ${active.id} from index ${oldIndex} to ${newIndex}`);
        }
        console.log(`[ProjectionConfig] Reordering from index ${oldIndex} to ${newIndex}`)
        console.log(`[ProjectionConfig] Old order: ${includedColumns.join(', ')}`)
        console.log(`[ProjectionConfig] New order: ${newOrder.join(', ')}`)

        // Update state immediately - this will trigger a re-render
        setIncludedColumnsOrder(newOrder)

        // Immediately persist the new order to node config and update output_metadata
        // Use the new order directly (it's already filtered to only included columns)
        const orderedProjectedColumns = newOrder

        // Create column metadata with order numbers (0, 1, 2, ...)
        const columnsWithOrder = orderedProjectedColumns.map((colName, index) => {
          const colMeta = availableColumns.find(c => c.name === colName)
          return {
            name: colName,
            type: colMeta?.datatype || 'TEXT',
            included: true,
            order: index,
            datatype: colMeta?.datatype || 'TEXT',
            nullable: colMeta?.nullable !== undefined ? colMeta.nullable : true,
            isPrimaryKey: colMeta?.isPrimaryKey || false,
          }
        })

        // Add excluded columns with order = -1 (not in output order)
        const excludedColumnsWithOrder = excludedColumns.map((colName) => {
          const colMeta = availableColumns.find(c => c.name === colName)
          return {
            name: colName,
            type: colMeta?.datatype || 'TEXT',
            included: false,
            order: -1,
            datatype: colMeta?.datatype || 'TEXT',
            nullable: colMeta?.nullable !== undefined ? colMeta.nullable : true,
            isPrimaryKey: colMeta?.isPrimaryKey || false,
          }
        })

        // Build output metadata with new order
        const outputMetadata = {
          columns: [
            // Projected columns in new order
            ...orderedProjectedColumns.map((colName) => {
              const colMeta = availableColumns.find(c => c.name === colName)
              return {
                name: colMeta?.business_name || colName,
                business_name: colMeta?.business_name || colName,
                technical_name: colMeta?.technical_name ?? colMeta?.db_name ?? colName,
                db_name: colMeta?.db_name,
                datatype: colMeta?.datatype || 'TEXT',
                nullable: colMeta?.nullable !== undefined ? colMeta.nullable : true,
                isPrimaryKey: colMeta?.isPrimaryKey || false,
              }
            }),
            // Calculated columns (if any)
            ...calculatedColumns
              .filter((c) => c.name.trim() && c.expression.trim())
              .map((calcCol) => ({
                name: calcCol.name.trim(),
                business_name: calcCol.name.trim(),
                technical_name: calcCol.name.trim(),
                datatype: calcCol.dataType || 'TEXT',
                nullable: true,
                isPrimaryKey: false,
              })),
          ],
          nodeId: node.data.node_id || node.id,
        }

        // Update node config immediately with new order and column metadata
        const updatedConfig = {
          ...node.data.config,
          excludedColumns: excludedColumns,
          includedColumns: orderedProjectedColumns, // Store ordered included columns
          output_columns: orderedProjectedColumns, // Explicit output columns in order
          selectedColumns: orderedProjectedColumns, // Legacy support
          columns: [...orderedProjectedColumns, ...calculatedColumns.filter(c => c.name.trim() && c.expression.trim()).map(c => c.name.trim())], // All output columns
          // Store column metadata with order for persistence
          columnOrder: columnsWithOrder.concat(excludedColumnsWithOrder), // All columns with order metadata
        }

        // Update the config hash ref BEFORE calling onUpdate to prevent useEffect from resetting
        // This hash represents what we're about to save, so when the node updates and useEffect runs,
        // it will see this hash and know not to reset the state
        const newConfigHash = JSON.stringify({
          output_columns: orderedProjectedColumns,
          includedColumns: orderedProjectedColumns,
          excludedColumns: excludedColumns,
          calculatedColumns: calculatedColumns,
        })
        lastConfigHashRef.current = newConfigHash
        console.log(`[ProjectionConfig] Updated config hash BEFORE onUpdate to prevent reset`)
        console.log(`[ProjectionConfig] New hash: ${newConfigHash.substring(0, 100)}...`)
        console.log(`[ProjectionConfig] Preserving excludedColumns: ${excludedColumns.join(', ')}`)
        console.log(`[ProjectionConfig] Preserving includedColumnsOrder: ${orderedProjectedColumns.join(', ')}`)

        // Persist immediately via onUpdate
        onUpdate(node.id, {
          config: updatedConfig,
          output_metadata: outputMetadata,
          // Preserve other node properties
          business_name: businessName || node.data.business_name || node.data.node_name || node.data.label || 'Projection',
          technical_name: node.data.technical_name,
          node_name: businessName || node.data.node_name || node.data.label || 'Projection',
        })

        console.log(`[ProjectionConfig] Column order persisted: ${orderedProjectedColumns.join(', ')}`)
        console.log(`[ProjectionConfig] Excluded columns persisted: ${excludedColumns.join(', ')}`)

        // Force a re-render to ensure UI updates properly
        setRenderKey(prev => prev + 1);

        // Reset drag flag after a delay to allow state to settle
        // Keep the flag true longer to prevent useEffect from running during the update
        setTimeout(() => {
          isDraggingRef.current = false
          console.log('[ProjectionConfig] Drag operation complete - unblocking useEffect')
          // After unblocking, verify the hash is still correct
          const currentConfig = node?.data?.config || {}
          const currentHash = JSON.stringify({
            output_columns: currentConfig.output_columns,
            includedColumns: currentConfig.includedColumns,
            excludedColumns: currentConfig.excludedColumns,
            calculatedColumns: currentConfig.calculatedColumns,
          })
          if (currentHash === newConfigHash) {
            console.log('[ProjectionConfig] Hash verification: Config matches after update ✓')
          } else {
            console.warn('[ProjectionConfig] Hash verification: Config mismatch after update!')
            console.warn('[ProjectionConfig]   Expected:', newConfigHash.substring(0, 100))
            console.warn('[ProjectionConfig]   Got:', currentHash.substring(0, 100))
          }
        }, 500) // Increased delay to ensure node update completes
      } else {
        // No valid drag - reset flag immediately
        isDraggingRef.current = false
        console.log('[ProjectionConfig] Invalid drag indices - resetting flag')
      }
    } else {
      // No valid drag target - reset flag immediately
      isDraggingRef.current = false
      console.log('[ProjectionConfig] No valid drag target - resetting flag')
    }
  }

  // Refs to track cursor position in expression textareas
  const expressionRefs = useRef<{ [key: string]: HTMLTextAreaElement | null }>({})

  // Generate default tests for CONCAT expressions
  const generateDefaultTests = (expression: string, availableColumns: string[]): ExpressionTest[] => {
    const tests: ExpressionTest[] = []
    const upperExpr = expression.toUpperCase()

    // Extract column names from expression
    // Handle both regular identifiers and bracketed column names like [execution order]
    const bracketedMatches = expression.match(/\[([^\]]+)\]/g) || []
    const bracketedColumns = bracketedMatches.map(m => m.slice(1, -1)) // Remove brackets
    const regularMatches = expression.match(/\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g) || []
    const allColumnMatches = [...bracketedColumns, ...regularMatches]
    const usedColumns = allColumnMatches.filter(col => availableColumns.includes(col))
    const testCol = usedColumns[0] || 'test_col'

    // UPPER function tests (only if not nested in SUBSTRING)
    if (upperExpr.includes('UPPER') && !upperExpr.includes('LOWER') && !upperExpr.includes('SUBSTRING')) {
      tests.push({
        inputRow: { [testCol]: 'abc' },
        expected: 'ABC',
        description: 'UPPER converts to uppercase'
      })
      tests.push({
        inputRow: { [testCol]: null },
        expected: '',
        description: 'UPPER handles NULL safely'
      })
      tests.push({
        inputRow: { [testCol]: 'A1b' },
        expected: 'A1B',
        description: 'UPPER converts mixed case'
      })
    }

    // LOWER function tests (only if not nested in SUBSTRING)
    if (upperExpr.includes('LOWER') && !upperExpr.includes('UPPER') && !upperExpr.includes('SUBSTRING')) {
      tests.push({
        inputRow: { [testCol]: 'ABC' },
        expected: 'abc',
        description: 'LOWER converts to lowercase'
      })
      tests.push({
        inputRow: { [testCol]: null },
        expected: '',
        description: 'LOWER handles NULL safely'
      })
      tests.push({
        inputRow: { [testCol]: 'A1B' },
        expected: 'a1b',
        description: 'LOWER converts mixed case'
      })
    }

    // SUBSTRING function tests
    if (upperExpr.includes('SUBSTRING')) {
      // Check if SUBSTRING has nested function (e.g., SUBSTRING(UPPER(...), ...))
      const hasNestedFunction = /SUBSTRING\s*\(\s*(UPPER|LOWER|CONCAT)\s*\(/i.test(expression)

      if (hasNestedFunction) {
        // Nested function tests - test the composition
        if (upperExpr.includes('UPPER')) {
          // SUBSTRING(UPPER(...), ...) tests
          tests.push({
            inputRow: { [testCol]: 'abcde' },
            expected: 'BCDE',  // UPPER('abcde') = 'ABCDE', SUBSTRING('ABCDE', 2, 4) = 'BCDE'
            description: 'SUBSTRING(UPPER(...)) nested composition'
          })
          tests.push({
            inputRow: { [testCol]: 'Hello' },
            expected: 'ELL',  // UPPER('Hello') = 'HELLO', SUBSTRING('HELLO', 2, 3) = 'ELL'
            description: 'SUBSTRING(UPPER(...)) with length'
          })
        }
      } else {
        // Simple SUBSTRING tests - extract start and length from expression if possible
        const substringMatch = expression.match(/SUBSTRING\s*\(\s*\w+\s*,\s*(\d+)\s*(?:,\s*(\d+))?\)/i)
        const startPos = substringMatch ? parseInt(substringMatch[1]) : 1
        const length = substringMatch && substringMatch[2] ? parseInt(substringMatch[2]) : null

        // Generate tests based on actual parameters in expression
        if (startPos === 1 && length === 3) {
          // SUBSTRING(table, 1, 3) - first 3 characters
          tests.push({
            inputRow: { [testCol]: 'abc' },
            expected: 'abc',
            description: 'SUBSTRING extracts first 3 characters'
          })
          tests.push({
            inputRow: { [testCol]: 'Hello' },
            expected: 'Hel',
            description: 'SUBSTRING extracts first 3 characters from longer string'
          })
          tests.push({
            inputRow: { [testCol]: null },
            expected: '',
            description: 'SUBSTRING handles NULL safely'
          })
        } else {
          // Generic SUBSTRING tests
          tests.push({
            inputRow: { [testCol]: 'Hello World' },
            expected: startPos === 7 ? 'World' : 'Hello World'.substring(startPos - 1, length ? startPos - 1 + length : undefined),
            description: `SUBSTRING extracts from position ${startPos}${length ? ` with length ${length}` : ''}`
          })
          tests.push({
            inputRow: { [testCol]: null },
            expected: '',
            description: 'SUBSTRING handles NULL safely'
          })
          tests.push({
            inputRow: { [testCol]: 'ABCDEF' },
            expected: 'ABCDEF'.substring(startPos - 1, length ? startPos - 1 + length : undefined),
            description: `SUBSTRING with length parameter (pos=${startPos}${length ? `, len=${length}` : ''})`
          })
        }
      }
    }

    // CONCAT function tests
    if (upperExpr.includes('CONCAT')) {
      // Extract column names from expression (simple regex-based extraction)
      const columnMatches = expression.match(/\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g) || []
      const usedColumns = columnMatches.filter(col => availableColumns.includes(col))

      if (usedColumns.length >= 2) {
        // Test 1: Simple concatenation with non-null values
        tests.push({
          inputRow: Object.fromEntries(usedColumns.map(col => [col, 'X'])),
          expected: 'X'.repeat(usedColumns.length),
          description: `Simple CONCAT with ${usedColumns.length} non-null values`
        })

        // Test 2: CONCAT with one NULL
        if (usedColumns.length >= 2) {
          const testRow: Record<string, any> = {}
          testRow[usedColumns[0]] = 'A'
          for (let i = 1; i < usedColumns.length; i++) {
            testRow[usedColumns[i]] = null
          }
          tests.push({
            inputRow: testRow,
            expected: 'A', // With COALESCE, NULL becomes ''
            description: `CONCAT with first value non-null, rest NULL`
          })
        }

        // Test 3: CONCAT with all NULL
        tests.push({
          inputRow: Object.fromEntries(usedColumns.map(col => [col, null])),
          expected: '', // With COALESCE, all NULLs become ''
          description: `CONCAT with all NULL values`
        })

        // Test 4: CONCAT with mixed values
        if (usedColumns.length >= 2) {
          const testRow: Record<string, any> = {}
          testRow[usedColumns[0]] = 'table'
          testRow[usedColumns[1]] = 'selected'
          for (let i = 2; i < usedColumns.length; i++) {
            testRow[usedColumns[i]] = null
          }
          tests.push({
            inputRow: testRow,
            expected: 'tableselected', // Concatenated non-null values
            description: `CONCAT with mixed non-null and NULL values`
          })
        }
      } else {
        // Fallback tests if we can't extract columns
        tests.push({
          inputRow: { test_col: 'X' },
          expected: 'XY',
          description: 'Simple CONCAT test'
        })
        tests.push({
          inputRow: { test_col: 'A', test_col2: null },
          expected: 'A',
          description: 'CONCAT with NULL'
        })
        tests.push({
          inputRow: { test_col: null, test_col2: null },
          expected: '',
          description: 'CONCAT with all NULL'
        })
        tests.push({
          inputRow: { test_col: 'table', test_col2: 'selected' },
          expected: 'tableselected',
          description: 'CONCAT with mixed values'
        })
      }
    }

    return tests
  }

  // Run expression tests
  const runExpressionTests = async (columnId: string) => {
    const column = calculatedColumns.find(c => c.id === columnId)
    if (!column || !column.expression.trim()) {
      return
    }

    // Mark as running tests
    setCalculatedColumns(prev =>
      prev.map(c =>
        c.id === columnId
          ? { ...c, isRunningTests: true, testResults: undefined }
          : c
      )
    )

    // Get or generate tests
    let tests = column.tests || []
    if (tests.length === 0) {
      // Generate default tests
      tests = generateDefaultTests(column.expression, includedColumns)
      // Save generated tests
      setCalculatedColumns(prev =>
        prev.map(c =>
          c.id === columnId
            ? { ...c, tests }
            : c
        )
      )
    }

    // If no tests could be generated, show message via a placeholder result and return
    if (tests.length === 0) {
      setCalculatedColumns(prev =>
        prev.map(c =>
          c.id === columnId
            ? {
              ...c,
              isRunningTests: false,
              testResults: [{
                test: {
                  inputRow: {},
                  expected: null,
                  description: 'No auto-generated tests available for this expression type. Supported: UPPER, LOWER, CONCAT, SUBSTRING.'
                },
                passed: false,
                actual: null,
                error: 'No tests available'
              }]
            }
            : c
        )
      )
      return
    }

    // Execute tests using backend API
    try {
      const { api } = await import('../../../services/api')

      // Prepare available columns for backend
      const availableCols = availableColumns.map(col => ({
        name: col.name,
        datatype: col.datatype || 'TEXT',
      }))

      // Prepare test cases for backend
      const testCases = tests.map(t => ({
        input: t.inputRow,
        expected: t.expected,
        description: t.description
      }))

      // Call backend API
      const response = await api.post('/api/test-expression/', {
        expression: column.expression,
        available_columns: availableCols,
        test_cases: testCases,
      })

      const backendResults = response.data.results || []

      // Check if backend results look suspicious (e.g., returning column names instead of evaluated values)
      // If backend returns column names as results, fall back to frontend evaluation
      const hasSuspiciousResults = backendResults.some((result: any) => {
        const actual = String(result.actual || '')
        // Check if actual value matches a column name (suspicious - suggests backend didn't evaluate)
        const columnNames = availableColumns.map(c => c.name.toLowerCase())
        return columnNames.includes(actual.toLowerCase()) && actual !== String(result.test?.expected || '')
      })

      if (hasSuspiciousResults) {
        console.warn('[Test] Backend returned suspicious results (column names instead of evaluated values), using frontend fallback')
        throw new Error('Backend returned invalid results')
      }

      // Map backend results to frontend format
      // Backend returns test.input, but frontend expects test.inputRow
      const testResults: ExpressionTestResult[] = backendResults.map((result: any) => ({
        test: {
          inputRow: result.test?.input || result.test?.inputRow || {},
          expected: result.test?.expected,
          description: result.test?.description || 'Test case'
        },
        passed: result.passed,
        actual: result.actual,
        error: result.error,
        diff: result.passed ? undefined : (result.error || `Expected: "${result.test?.expected}", Got: "${result.actual}"`),
        debug_steps: result.debug_steps || []  // Include nested evaluation steps
      }))

      // Update column with test results
      setCalculatedColumns(prev =>
        prev.map(c =>
          c.id === columnId
            ? { ...c, isRunningTests: false, testResults }
            : c
        )
      )

    } catch (apiError: any) {
      console.error('Error calling test API:', apiError)

      // Fallback: Simple frontend evaluation (for development/offline)
      const testResults: ExpressionTestResult[] = tests.map(test => {
        try {
          let actual: any = null
          let error: string | undefined = undefined

          try {
            // Replace column references with values from inputRow
            // Process column replacements first, before function replacements
            let evalExpression = column.expression
            const functionNames = ['UPPER', 'LOWER', 'CONCAT', 'SUBSTRING', 'TRIM', 'COALESCE', 'CAST', 'IF', 'CASE', 'DATEADD', 'DATEDIFF']
            
            // Replace column names with their test values
            // Process in reverse order of length to avoid partial matches
            const sortedKeys = Object.keys(test.inputRow).sort((a, b) => b.length - a.length)
            for (const key of sortedKeys) {
              // Only replace if it's not a function name
              if (!functionNames.includes(key.toUpperCase())) {
                const value = test.inputRow[key]
                // Escape special regex characters in the column name
                const escapedKey = key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
                
                // Replace bracketed column names first: [column name]
                const bracketedRegex = new RegExp(`\\[${escapedKey}\\]`, 'gi')
                const replacement = value === null || value === undefined ? '""' : JSON.stringify(String(value))
                const beforeBracketReplace = evalExpression
                evalExpression = evalExpression.replace(bracketedRegex, replacement)
                
                // Then replace unbracketed column names (whole word match)
                const regex = new RegExp(`\\b${escapedKey}\\b`, 'gi')
                const beforeReplace = evalExpression
                evalExpression = evalExpression.replace(regex, replacement)
                
                if (beforeBracketReplace !== evalExpression || beforeReplace !== evalExpression) {
                  console.log(`[Test] Replaced column "${key}" with ${replacement} in expression: ${beforeReplace} -> ${evalExpression}`)
                }
              }
            }
            
            console.log(`[Test] Expression after column replacement: "${evalExpression}"`)

            // Now replace function calls (after column values are in place)
            // Replace UPPER function calls
            evalExpression = evalExpression.replace(/UPPER\s*\(([^)]+)\)/gi, (_match, arg) => {
              let cleanArg = arg.trim()
              // Remove surrounding quotes if present
              if ((cleanArg.startsWith('"') && cleanArg.endsWith('"')) || 
                  (cleanArg.startsWith("'") && cleanArg.endsWith("'"))) {
                cleanArg = cleanArg.slice(1, -1)
              }
              // Handle null/empty
              if (cleanArg === 'null' || cleanArg === 'NULL' || cleanArg === '""' || cleanArg === "''" || cleanArg === '') {
                return '""'
              }
              // Convert to uppercase and return as JSON string
              return JSON.stringify(cleanArg.toUpperCase())
            })

            // Replace LOWER function calls
            evalExpression = evalExpression.replace(/LOWER\s*\(([^)]+)\)/gi, (_match, arg) => {
              let cleanArg = arg.trim()
              if ((cleanArg.startsWith('"') && cleanArg.endsWith('"')) || 
                  (cleanArg.startsWith("'") && cleanArg.endsWith("'"))) {
                cleanArg = cleanArg.slice(1, -1)
              }
              if (cleanArg === 'null' || cleanArg === 'NULL' || cleanArg === '""' || cleanArg === "''" || cleanArg === '') {
                return '""'
              }
              return JSON.stringify(cleanArg.toLowerCase())
            })

            // Replace CONCAT function calls
            evalExpression = evalExpression.replace(/CONCAT\s*\(([^)]+)\)/gi, (_match, args) => {
              const argList = args.split(',').map((a: string) => {
                let arg = a.trim()
                // Remove quotes if present
                if ((arg.startsWith('"') && arg.endsWith('"')) || 
                    (arg.startsWith("'") && arg.endsWith("'"))) {
                  arg = arg.slice(1, -1)
                }
                // Handle null
                if (arg === 'null' || arg === 'NULL' || arg === '""' || arg === "''") {
                  return '""'
                }
                return JSON.stringify(arg)
              })
              // Join with + operator for concatenation
              return argList.join(' + ')
            })

            // Evaluate the expression
            // The expression should now be valid JavaScript like: "ABC" or "" + ""
            actual = eval(evalExpression) || ''

            // Normalize
            if (actual === null || actual === undefined) {
              actual = ''
            }
            actual = String(actual)

          } catch (e: any) {
            error = e.message || 'Evaluation error'
          }

          const passed = error === undefined && String(actual) === String(test.expected)
          const diff = !passed && error === undefined
            ? `Expected: "${test.expected}", Got: "${actual}"`
            : undefined

          return {
            test,
            passed,
            actual,
            error,
            diff
          }
        } catch (e: any) {
          return {
            test,
            passed: false,
            error: e.message || 'Test execution failed'
          }
        }
      })

      // Update column with test results
      setCalculatedColumns(prev =>
        prev.map(c =>
          c.id === columnId
            ? { ...c, isRunningTests: false, testResults }
            : c
        )
      )
    }
  }

  // Calculated columns handlers
  const addCalculatedColumn = () => {
    const newColumn: CalculatedColumnWithValidation = {
      id: `calc-${Date.now()}-${Math.random()}`,
      name: '',
      expression: '',
      dataType: 'STRING',
      tests: [],
    }
    setCalculatedColumns([...calculatedColumns, newColumn])
  }

  const removeCalculatedColumn = (id: string) => {
    const columnToRemove = calculatedColumns.find(c => c.id === id)
    if (columnToRemove && columnToRemove.name.trim()) {
      // Remove from includedColumnsOrder if it exists
      const columnName = columnToRemove.name.trim()
      setIncludedColumnsOrder(prev => prev.filter(col => col !== columnName))
      
      // Reset the processed ref to allow re-processing
      lastProcessedCalculatedColumnsRef.current = ''
    }
    
    // Remove from calculated columns state
    setCalculatedColumns(calculatedColumns.filter((c) => c.id !== id))
    
    // Clean up ref
    delete expressionRefs.current[id]
    
    // Update node config to remove the calculated column
    if (node) {
      const currentConfig = node.data.config || {}
      const updatedCalculatedColumns = (currentConfig.calculatedColumns || []).filter((c: any) => c.id !== id)
      onUpdate(node.id, {
        config: {
          ...currentConfig,
          calculatedColumns: updatedCalculatedColumns,
        },
      })
    }
  }

  // Simplified: No cursor placeholder system - just update the column
  const updateCalculatedColumn = (id: string, updates: Partial<CalculatedColumnWithValidation>) => {
    setCalculatedColumns(
      calculatedColumns.map((c) => {
        if (c.id === id) {
          return { ...c, ...updates }
        }
        return c
      })
    )
  }

  const insertColumnIntoExpression = (columnName: string, columnId: string) => {
    // If column name contains spaces or special characters, wrap it in brackets for SQL compatibility
    const formattedColumnName = columnName.includes(' ') || /[^a-zA-Z0-9_]/.test(columnName)
      ? `[${columnName}]`  // Use brackets for column names with spaces
      : columnName
    const column = calculatedColumns.find((c) => c.id === columnId)
    if (!column) return

    const textarea = expressionRefs.current[columnId]
    const currentExpression = column.expression || ''

    // Simple: Insert at cursor position
    if (textarea) {
      const cursorPos = textarea.selectionStart || currentExpression.length
      const textBefore = currentExpression.substring(0, cursorPos)
      const textAfter = currentExpression.substring(cursorPos)

      // Check if cursor is inside function parentheses
      const lastOpenParen = textBefore.lastIndexOf('(')
      const lastCloseParen = textBefore.lastIndexOf(')')

      let newExpression: string
      let newCursorPos: number

      if (lastOpenParen > lastCloseParen) {
        // Cursor is inside function parentheses
        const contentBeforeCursor = textBefore.substring(lastOpenParen + 1).trim()
        if (contentBeforeCursor.length > 0) {
          // Add comma and formatted column name
          newExpression = `${textBefore}, ${formattedColumnName}${textAfter}`
          newCursorPos = cursorPos + formattedColumnName.length + 2 // +2 for ", "
        } else {
          // First argument, no comma needed
          newExpression = `${textBefore}${formattedColumnName}${textAfter}`
          newCursorPos = cursorPos + formattedColumnName.length
        }
      } else {
        // Cursor is outside function parentheses, insert at cursor
        newExpression = `${textBefore}${formattedColumnName}${textAfter}`
        newCursorPos = cursorPos + formattedColumnName.length
      }

      updateCalculatedColumn(columnId, { expression: newExpression })

      // Restore cursor position after state update
      setTimeout(() => {
        const updatedTextarea = expressionRefs.current[columnId]
        if (updatedTextarea) {
          updatedTextarea.focus()
          updatedTextarea.setSelectionRange(newCursorPos, newCursorPos)
        }
      }, 0)
    } else {
      // Fallback: append to end if textarea ref not available
      const newExpression = currentExpression
        ? `${currentExpression}, ${formattedColumnName}`
        : formattedColumnName
      updateCalculatedColumn(columnId, { expression: newExpression })
    }
  }

  const insertFunctionIntoExpression = (funcName: string, columnId: string) => {
    const column = calculatedColumns.find((c) => c.id === columnId)
    if (!column) return

    const textarea = expressionRefs.current[columnId]
    const currentExpression = column.expression || ''

    // Simple: Insert function template at cursor position
    if (textarea) {
      const cursorPos = textarea.selectionStart || currentExpression.length
      const textBefore = currentExpression.substring(0, cursorPos)
      const textAfter = currentExpression.substring(cursorPos)

      // Check function signature to determine if single or multi-argument
      const func = functions.find(f => f.name.toUpperCase() === funcName.toUpperCase())
      const isMultiArg = func && func.parameters && func.parameters.length > 1
      
      // Insert function template
      const template = isMultiArg 
        ? `${funcName}(, )`  // Multi-argument template
        : `${funcName}()`     // Single-argument template
      
      const newExpression = `${textBefore}${template}${textAfter}`
      const cursorPosAfterInsert = textBefore.length + funcName.length + 1 // Position inside parentheses

      updateCalculatedColumn(columnId, { expression: newExpression })

      // Restore cursor position inside parentheses
      setTimeout(() => {
        const updatedTextarea = expressionRefs.current[columnId]
        if (updatedTextarea) {
          updatedTextarea.focus()
          updatedTextarea.setSelectionRange(cursorPosAfterInsert, cursorPosAfterInsert)
        }
      }, 0)
    } else {
      // Fallback: insert at end if textarea ref not available
      const template = `${funcName}()`
      const newExpression = currentExpression
        ? `${currentExpression}${template}`
        : template
      updateCalculatedColumn(columnId, { expression: newExpression })
    }
  }

  const filteredFunctions = useMemo(() => {
    return functions.filter((f) =>
      f.name.toLowerCase().includes(functionSearch.toLowerCase()) ||
      f.description.toLowerCase().includes(functionSearch.toLowerCase())
    )
  }, [functionSearch])

  // Filter columns by search term - filters within the active tab
  // CRITICAL: All tabs preserve the original display_order from availableColumns, EXCEPT for the included tab which uses the custom order
  // Calculated columns are included in the projection list and can be reordered
  const filteredColumns = useMemo(() => {
    const searchLower = searchTerm.toLowerCase()
    const baseFilter = searchLower.length > 0
      ? (col: ColumnMetadata) => col.name.toLowerCase().includes(searchLower)
      : () => true

    // Get valid calculated columns as ColumnMetadata
    const validCalculatedColumns = calculatedColumns.filter(
      (c) => c.name.trim() && c.expression.trim()
    )
    const calculatedColumnsMetadata: ColumnMetadata[] = validCalculatedColumns.map(c => ({
      name: c.name.trim(),
      datatype: c.dataType || 'TEXT',
      nullable: true,
      isPrimaryKey: false,
    }))

    // Create a map of column names to their original index in availableColumns (display_order)
    const originalOrderMap = new Map<string, number>()
    availableColumns.forEach((col, index) => {
      originalOrderMap.set(col.name, index)
    })
    // Calculated columns come after source columns in original order
    calculatedColumnsMetadata.forEach((col, index) => {
      originalOrderMap.set(col.name, availableColumns.length + index)
    })

    // Helper function to get original order index
    const getOriginalOrder = (col: ColumnMetadata): number => {
      return originalOrderMap.get(col.name) ?? Infinity
    }

    // Combine source and calculated columns for "all" view
    const allColumns = [...availableColumns, ...calculatedColumnsMetadata]

    // Filter based on active field tab
    switch (activeFieldTab) {
      case 'included':
        // Filter to included columns only, preserve CUSTOM display order (user drag-and-drop order)
        // This includes both source columns and calculated columns in the unified sequence
        const includedFiltered = includedColumns
          .filter(colName => {
            // Check if it's a source column
            const sourceCol = availableColumns.find(c => c.name === colName)
            if (sourceCol) return baseFilter(sourceCol)
            // Check if it's a calculated column
            const calcCol = calculatedColumnsMetadata.find(c => c.name === colName)
            if (calcCol) return baseFilter(calcCol)
            return false
          })
        // Return them in the user-defined order (includes calculated columns)
        return includedFiltered.map(colName => {
          const sourceCol = availableColumns.find(c => c.name === colName)
          if (sourceCol) return sourceCol
          const calcCol = calculatedColumnsMetadata.find(c => c.name === colName)
          return calcCol!
        })
      case 'excluded':
        // Filter to excluded columns only, preserve original display_order
        // Only source columns can be excluded (calculated columns are always included)
        return availableColumns
          .filter(col => excludedColumns.includes(col.name))
          .filter(baseFilter)
          .sort((a, b) => getOriginalOrder(a) - getOriginalOrder(b))
      case 'all':
        // Show all columns (source + calculated), strictly sorted by original display_order
        // This remains static and never changes based on reordering
        return allColumns
          .filter(baseFilter)
          .sort((a, b) => getOriginalOrder(a) - getOriginalOrder(b))
      default:
        return allColumns.filter(baseFilter)
    }
  }, [availableColumns, searchTerm, activeFieldTab, includedColumns, excludedColumns, calculatedColumns])

  // Calculate counts for tab labels
  // Included count includes both source columns and calculated columns
  const countIncluded = useMemo(() => includedColumns.length, [includedColumns])
  const countExcluded = useMemo(() => excludedColumns.length, [excludedColumns])
  // Total count includes source columns and calculated columns with name and expression
  const countTotal = useMemo(() => {
    const validCalculatedColumns = calculatedColumns.filter(
      (c) => c.name.trim() && c.expression.trim()
    )
    return availableColumns.length + validCalculatedColumns.length
  }, [availableColumns, calculatedColumns])

  // Get primary key columns
  const primaryKeyColumns = useMemo(() => {
    return availableColumns.filter(col => col.isPrimaryKey).map(col => col.name)
  }, [availableColumns])

  const _handleSave = () => {
    if (!node) return

    // STATE-001 GUARD: Prevent execution if save already in progress
    if (isSavingRef.current) {
      console.log('[ProjectionConfig] SAVE BLOCKED: Save already in progress')
      return
    }

    // Mark save as in progress
    isSavingRef.current = true

    // DEBUGGING: OnCalculatedColumnSave
    console.log('[ProjectionConfig] ===== SAVE START =====')
    console.log('[ProjectionConfig] Calculated Columns:', calculatedColumns.filter(c => c.name.trim() && c.expression.trim()))
    console.log('[ProjectionConfig] Projection Columns (includedColumnsOrder):', includedColumnsOrder)

    // Simplified: Just validate basic requirements, let backend handle SQL generation

    // Validation
    if (availableColumns.length === 0) {
      setError('No columns available. Please ensure the input node has column metadata.')
      isSavingRef.current = false // Reset flag on error
      return
    }

    // ✅ FIX: Allow empty includedColumns (means SELECT * / all columns)
    // Backend will default to all columns if none are explicitly selected
    // Only block if we're in EXCLUDE mode and excluded all columns
    if (excludedColumns.length > 0 && excludedColumns.length >= availableColumns.length) {
      setError('Cannot exclude all columns. At least one column must remain.')
      isSavingRef.current = false // Reset flag on error
      return
    }

    // Cannot exclude all primary key columns
    const remainingPrimaryKeys = primaryKeyColumns.filter(pk => !excludedColumns.includes(pk))
    if (remainingPrimaryKeys.length === 0 && primaryKeyColumns.length > 0) {
      setError('Cannot exclude all primary key columns. At least one primary key must remain.')
      return
    }

    // Get valid calculated columns (just name and expression required)
    // Backend will handle validation and SQL generation
    const validCalculatedColumns = calculatedColumns
      .filter((c) => {
        // Must have name and expression
        return c.name.trim() && c.expression.trim()
      })
      .map(c => {
        // Clean expression (remove any accidental placeholders)
        const cleanedExpression = (c.expression || '').trim()
        return {
          ...c,
          expression: cleanedExpression,
        }
      })
    const calculatedColumnNames = validCalculatedColumns.map(c => c.name.trim())

    // CRITICAL FIX: Ensure calculated columns are ALWAYS at the end
    // Separate source columns from calculated columns, then append calculated at end
    const sourceColumnsInOrder = includedColumnsOrder.length > 0
      ? includedColumnsOrder.filter(col => {
          const meta = availableColumns.find(c => (c.technical_name ?? c.name) === col || c.name === col)
          const displayName = meta?.name ?? col
          return meta != null && !excludedColumns.includes(displayName) && !calculatedColumnNames.includes(col)
        })
      : includedColumns.filter(col => !excludedColumns.includes(col) && !calculatedColumnNames.includes(col))
    
    // Build orderedOutputColumns: source columns first, then calculated columns at end
    const orderedOutputColumns = [...sourceColumnsInOrder, ...calculatedColumnNames]
    // Persist technical names for output_columns when available (rename-safe)
    const outputColumnsForSave = orderedOutputColumns.map((colName) => {
      const meta = availableColumns.find(c => c.name === colName || (c.technical_name ?? c.name) === colName)
      return (meta?.technical_name ?? meta?.name ?? colName)
    })

    // Validate calculated column names don't conflict with source columns
    const sourceColumnNames = availableColumns.map(c => c.name)
    const conflictingNames = calculatedColumnNames.filter(name =>
      sourceColumnNames.includes(name)
    )

    if (conflictingNames.length > 0) {
      setError(`Calculated column name(s) conflict with existing source columns: ${conflictingNames.join(', ')}`)
      isSavingRef.current = false // Reset flag on error
      return
    }

    // Build output metadata (schema) for downstream nodes; use output names when renamed
    // CRITICAL: Columns are in the order specified by orderedOutputColumns
    const outputMetadata = {
      columns: orderedOutputColumns.map((colName) => {
        // Check if it's a calculated column
        const calcCol = validCalculatedColumns.find(c => c.name.trim() === colName)
        if (calcCol) {
          const businessName = calcCol.name.trim()
          return {
            name: businessName,
            business_name: businessName,
            technical_name: businessName,
            datatype: calcCol.dataType || 'TEXT',
            nullable: true,
            isPrimaryKey: false,
          }
        }
        // It's a source column (colName may be technical_name from saved config)
        const colMeta = availableColumns.find(c => (c.technical_name ?? c.name) === colName || c.name === colName)
        const outputName = columnOutputNames[(colMeta?.name ?? colName)]?.trim() || (colMeta?.name ?? colName)
        const businessName = outputName
        return {
          name: businessName,
          business_name: businessName,
          technical_name: colMeta?.technical_name ?? colMeta?.name ?? colName,
          db_name: colMeta?.db_name,
          datatype: colMeta?.datatype || 'TEXT',
          nullable: colMeta?.nullable !== undefined ? colMeta.nullable : true,
          isPrimaryKey: colMeta?.isPrimaryKey || false,
        }
      }),
      nodeId: node.data.node_id || node.id,
    }

    // Generate technical_name if not exists
    const technicalName = node.data.technical_name || (node.data.node_id ? `projection_${node.data.node_id.substring(0, 8)}` : undefined)

    // Create column metadata with order numbers (0, 1, 2, ...) for persistence; include outputName when renamed
    // Include both source columns and calculated columns in the unified sequence
    const columnsWithOrderSave = orderedOutputColumns.map((colName, index) => {
      // Check if it's a calculated column
      const calcCol = validCalculatedColumns.find(c => c.name.trim() === colName)
      if (calcCol) {
        return {
          name: colName,
          type: calcCol.dataType || 'TEXT',
          included: true,
          order: index,
          isCalculated: true,
        }
      }
      // It's a source column (colName may be technical_name)
      const colMeta = availableColumns.find(c => (c.technical_name ?? c.name) === colName || c.name === colName)
      const sourceName = colMeta?.name ?? colName
      const outputName = columnOutputNames[sourceName]?.trim()
      return {
        name: colMeta?.name ?? colName,
        type: colMeta?.datatype || 'TEXT',
        included: true,
        order: index,
        isCalculated: false,
        ...(outputName && outputName !== sourceName ? { outputName } : {}),
      }
    })

    // Add excluded columns with order = -1 (not in output order)
    const excludedColumnsWithOrderSave = excludedColumns.map((colName) => {
      const colMeta = availableColumns.find(c => c.name === colName)
      return {
        name: colName,
        type: colMeta?.datatype || 'TEXT',
        included: false,
        order: -1,
        isCalculated: false, // Excluded columns are always source columns
      }
    })

    // Output names in order for downstream (renamed when columnOutputNames set)
    const outputNamesForDownstream = orderedOutputColumns.map(c => {
      const calcCol = validCalculatedColumns.find(cc => cc.name.trim() === c)
      if (calcCol) return c
      const colMeta = availableColumns.find(ac => (ac.technical_name ?? ac.name) === c || ac.name === c)
      const sourceName = colMeta?.name ?? c
      return columnOutputNames[sourceName]?.trim() || c
    })
    const config = {
      ...node.data.config,
      excludedColumns: excludedColumns,
      includedColumns: outputColumnsForSave, // Store technical names for rename-safety
      output_columns: outputColumnsForSave, // Explicit output columns in order (source names for backend)
      calculatedColumns: validCalculatedColumns, // Store calculated columns
      // Legacy support
      selectedColumns: outputColumnsForSave,
      selectedMode: 'INCLUDE',
      excludeMode: false,
      columns: outputNamesForDownstream, // All output columns (output names for downstream)
      // Store column metadata with order for persistence
      columnOrder: columnsWithOrderSave.concat(excludedColumnsWithOrderSave), // All columns with order metadata (includes outputName when renamed)
    }

    // CHANGE DETECTION: Check if new state differs from previous state
    const currentConfigHash = JSON.stringify({
      output_columns: node.data.config?.output_columns || [],
      includedColumns: node.data.config?.includedColumns || [],
      excludedColumns: node.data.config?.excludedColumns || [],
      calculatedColumns: node.data.config?.calculatedColumns || [],
      business_name: node.data.business_name,
    })
    const newConfigHash = JSON.stringify({
      output_columns: orderedOutputColumns,
      includedColumns: orderedOutputColumns,
      excludedColumns: excludedColumns,
      calculatedColumns: validCalculatedColumns,
      business_name: businessName.trim() || node.data.business_name || node.data.node_name || node.data.label || 'Projection',
    })

    // CHANGE DETECTION: Skip update if no change
    if (currentConfigHash === newConfigHash) {
      console.log('[ProjectionConfig] SAVE SKIPPED: No changes detected')
      isSavingRef.current = false
      return
    }

    // DEBUGGING: OnNodeUpdate
    console.log('[ProjectionConfig] NODE UPDATE TRIGGERED')
    console.log('[ProjectionConfig] New Config:', {
      output_columns: orderedOutputColumns,
      calculatedColumns: validCalculatedColumns.map(c => ({ name: c.name, expression: c.expression })),
    })

    // ACTION: REGISTER_CALCULATED_COLUMN
    // Update calculated columns state (local state only, no node update yet)
    setCalculatedColumns(prev =>
      prev.map(c => {
        const validCol = validCalculatedColumns.find(vc => vc.id === c.id)
        if (validCol) {
          return {
            ...c,
            ...validCol,
          }
        }
        return c
      })
    )

    // ACTION: ADD_TO_PROJECTION
    // STATE-001: Update node ONLY in explicit user action (OnSave)
    onUpdate(node.id, {
      config: config,
      // Node identity properties
      business_name: businessName.trim() || node.data.business_name || node.data.node_name || node.data.label || 'Projection',
      technical_name: technicalName,
      node_name: businessName.trim() || node.data.node_name || node.data.label || 'Projection', // Legacy
      output_metadata: outputMetadata,
    })
    setError(null)

    // POST-ACTION: REFRESH_PREVIEW_ONCE
    // Note: Preview refresh happens via TableDataPanel when node config changes
    // This is a post-action, not triggered by state change
    console.log('[ProjectionConfig] PREVIEW REFRESH (post-action)')

    // Reset save flag
    isSavingRef.current = false

    // DEBUGGING: Save complete
    console.log('[ProjectionConfig] ===== SAVE END =====')
  }

  // Live state updates: push config + output_metadata to store whenever panel state changes (no per-node Save button).
  const applyLiveConfigRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const lastLiveAppliedHashRef = useRef<string>('')
  useEffect(() => {
    if (!node || availableColumns.length === 0) return
    if (excludedColumns.length >= availableColumns.length) return
    const primaryKeyColumns = availableColumns.filter(col => col.isPrimaryKey).map(col => col.name)
    const remainingPrimaryKeys = primaryKeyColumns.filter(pk => !excludedColumns.includes(pk))
    if (primaryKeyColumns.length > 0 && remainingPrimaryKeys.length === 0) return

    if (applyLiveConfigRef.current) clearTimeout(applyLiveConfigRef.current)
    applyLiveConfigRef.current = setTimeout(() => {
      applyLiveConfigRef.current = null
      const validCalculatedColumns = calculatedColumns
        .filter(c => c.name.trim() && c.expression.trim())
        .map(c => ({ ...c, expression: (c.expression || '').trim() }))
      const calculatedColumnNames = validCalculatedColumns.map(c => c.name.trim())
      const sourceColumnsInOrder = includedColumnsOrder.length > 0
        ? includedColumnsOrder.filter(col => {
            const meta = availableColumns.find(c => (c.technical_name ?? c.name) === col || c.name === col)
            const displayName = meta?.name ?? col
            return meta != null && !excludedColumns.includes(displayName) && !calculatedColumnNames.includes(col)
          })
        : availableColumns.filter(c => !excludedColumns.includes(c.name) && !calculatedColumnNames.includes(c.name)).map(c => c.name)
      const orderedOutputColumns = [...sourceColumnsInOrder, ...calculatedColumnNames]
      const outputColumnsForSave = orderedOutputColumns.map((colName) => {
        const meta = availableColumns.find(c => c.name === colName || (c.technical_name ?? c.name) === colName)
        return (meta?.technical_name ?? meta?.name ?? colName)
      })
      const outputMetadata = {
        columns: orderedOutputColumns.map((colName) => {
          const calcCol = validCalculatedColumns.find(c => c.name.trim() === colName)
          const outputName = columnOutputNames[colName]?.trim() || colName
          if (calcCol) {
            return {
              name: outputName,
              business_name: outputName,
              technical_name: outputName,
              datatype: calcCol.dataType || 'TEXT',
              nullable: true,
              isPrimaryKey: false,
            }
          }
        const colMeta = availableColumns.find(c => (c.technical_name ?? c.name) === colName || c.name === colName)
        return {
            name: outputName,
            business_name: outputName,
            technical_name: colMeta?.technical_name ?? colMeta?.db_name ?? colName,
            db_name: colMeta?.db_name,
            datatype: colMeta?.datatype || 'TEXT',
            nullable: colMeta?.nullable !== undefined ? colMeta.nullable : true,
            isPrimaryKey: colMeta?.isPrimaryKey || false,
          }
        }),
        nodeId: node.data.node_id || node.id,
      }
      const columnsWithOrderSave = orderedOutputColumns.map((colName, index) => {
        const colMeta = availableColumns.find(c => c.name === colName || (c.technical_name ?? c.name) === colName)
        const outputName = columnOutputNames[colName]?.trim() || colName
        return {
          name: colName,
          type: colMeta?.datatype || 'TEXT',
          included: true,
          order: index,
          isCalculated: calculatedColumnNames.includes(colName),
          ...(outputName !== colName ? { outputName } : {}),
        }
      })
      const excludedColumnsWithOrderSave = excludedColumns.map((colName) => ({
        name: colName,
        type: 'TEXT',
        included: false,
        order: -1,
        isCalculated: false,
      }))
      const config = {
        excludedColumns,
        includedColumns: outputColumnsForSave,
        output_columns: outputColumnsForSave,
        calculatedColumns: validCalculatedColumns,
        selectedColumns: outputColumnsForSave,
        selectedMode: 'INCLUDE',
        excludeMode: false,
        columns: orderedOutputColumns.map(colName => {
          const meta = availableColumns.find(c => c.name === colName || (c.technical_name ?? c.name) === colName)
          return (columnOutputNames[colName]?.trim() || meta?.name) ?? colName
        }),
        columnOrder: columnsWithOrderSave.concat(excludedColumnsWithOrderSave),
      }
      const technicalNameForEffect =
        node.data.technical_name ||
        (node.data.node_id ? `projection_${node.data.node_id.substring(0, 8)}` : undefined)

      // Change detection guard to prevent infinite update loops:
      // only call onUpdate if the computed payload actually differs.
      const nextHash = JSON.stringify({
        nodeId: node.id,
        business_name: businessName.trim() || node.data.business_name || node.data.node_name || node.data.label || 'Projection',
        technical_name: technicalNameForEffect,
        config,
        output_metadata: outputMetadata,
      })
      if (nextHash === lastLiveAppliedHashRef.current) return
      lastLiveAppliedHashRef.current = nextHash

      onUpdate(node.id, {
        config,
        business_name: businessName.trim() || node.data.business_name || node.data.node_name || node.data.label || 'Projection',
        technical_name: technicalNameForEffect,
        node_name: businessName.trim() || node.data.node_name || node.data.label || 'Projection',
        output_metadata: outputMetadata,
      })
    }, 150)
    return () => {
      if (applyLiveConfigRef.current) clearTimeout(applyLiveConfigRef.current)
    }
  }, [
    node?.id,
    node?.data?.node_id,
    excludedColumns,
    includedColumnsOrder,
    calculatedColumns,
    columnOutputNames,
    businessName,
    availableColumns,
    onUpdate,
  ])

  // Function to handle field selection
  const toggleFieldSelection = (fieldName: string, event?: React.MouseEvent) => {
    const newSelectedFields = new Set(selectedFields);

    if (event?.ctrlKey || event?.metaKey) {
      // Ctrl/Cmd + click: toggle individual field
      if (newSelectedFields.has(fieldName)) {
        newSelectedFields.delete(fieldName);
      } else {
        newSelectedFields.add(fieldName);
      }
    } else if (event?.shiftKey && selectedFields.size > 0) {
      // Shift + click: select range from last selected field
      const allFields = filteredColumns.map(col => col.name);
      const lastSelectedField = Array.from(selectedFields).pop();

      if (lastSelectedField) {
        const startIndex = allFields.indexOf(lastSelectedField);
        const endIndex = allFields.indexOf(fieldName);

        if (startIndex !== -1 && endIndex !== -1) {
          const start = Math.min(startIndex, endIndex);
          const end = Math.max(startIndex, endIndex);

          for (let i = start; i <= end; i++) {
            newSelectedFields.add(allFields[i]);
          }
        }
      }
    } else {
      // Regular click: select only this field
      if (newSelectedFields.has(fieldName)) {
        newSelectedFields.clear();
      } else {
        newSelectedFields.clear();
        newSelectedFields.add(fieldName);
      }
    }

    setSelectedFields(newSelectedFields);
  };

  // Get display (output) name for a source column (renamed or same).
  // Optional overrides allow us to persist using the *new* map immediately, instead of stale state.
  const getOutputName = (sourceName: string, overrides?: Record<string, string>) =>
    overrides?.[sourceName]?.trim() || columnOutputNames[sourceName]?.trim() || sourceName

  // Persist column renames to node config (columnOrder.outputName, output_metadata with output names)
  const persistColumnRenames = (overrideOutputNames?: Record<string, string>) => {
    if (!node) return
    const effectiveOutputNames = overrideOutputNames ?? columnOutputNames
    const config = node.data.config || {}
    const includedOrder = includedColumnsOrder.filter(
      c => !excludedColumns.includes(c) && !calculatedColumns.some(calc => calc.name.trim() === c)
    )
    const calculatedNames = calculatedColumns.filter(c => c.name.trim() && c.expression.trim()).map(c => c.name.trim())
    const sourceOrder = [...includedOrder, ...calculatedNames]

    const columnsWithOrder = sourceOrder.map((colName, index) => {
      const colMeta = availableColumns.find(c => c.name === colName || (c.technical_name ?? c.name) === colName)
      const outputName = effectiveOutputNames[colName]?.trim()
      return {
        name: colName,
        type: colMeta?.datatype || 'TEXT',
        included: true,
        order: index,
        isCalculated: calculatedNames.includes(colName),
        ...(outputName && outputName !== colName ? { outputName } : {}),
      }
    })
    const excludedWithOrder = excludedColumns.map((colName) => {
      const colMeta = availableColumns.find(c => c.name === colName)
      return {
        name: colName,
        type: colMeta?.datatype || 'TEXT',
        included: false,
        order: -1,
        isCalculated: false,
      }
    })
    const outputNamesInOrder = sourceOrder.map(s => getOutputName(s, effectiveOutputNames))
    const outputMetadata = {
      columns: sourceOrder.map((colName) => {
        const calcCol = calculatedColumns.find(c => c.name.trim() === colName)
        const outputName = getOutputName(colName, effectiveOutputNames)
        if (calcCol) {
          return {
            name: outputName,
            business_name: outputName,
            technical_name: outputName,
            db_name: undefined,
            datatype: calcCol.dataType || 'TEXT',
            nullable: true,
            isPrimaryKey: false,
          }
        }
        const colMeta = availableColumns.find(c => c.name === colName || (c.technical_name ?? c.name) === colName)
        return {
          name: outputName,
          business_name: outputName,
          technical_name: colMeta?.technical_name ?? colMeta?.db_name ?? colName,
          db_name: colMeta?.db_name,
          datatype: colMeta?.datatype || 'TEXT',
          nullable: colMeta?.nullable !== undefined ? colMeta.nullable : true,
          isPrimaryKey: colMeta?.isPrimaryKey || false,
        }
      }),
      nodeId: node.data.node_id || node.id,
    }
    onUpdate(node.id, {
      config: {
        ...config,
        columnOrder: columnsWithOrder.concat(excludedWithOrder),
        output_columns: config.output_columns, // Keep source names for backend selection
        columns: outputNamesInOrder, // Downstream see output names
      },
      output_metadata: outputMetadata,
    })
  }

  const handleRenameField = (sourceName: string, newOutputName: string) => {
    const trimmed = newOutputName.trim()
    if (!trimmed) {
      setEditingFieldName(null)
      return
    }
    const existingOutputNames = new Set(
      availableColumns.map(c => c.name).filter(n => n !== sourceName).map(n => getOutputName(n))
    )
    calculatedColumns.filter(c => c.name.trim()).forEach(c => existingOutputNames.add(c.name.trim()))
    if (existingOutputNames.has(trimmed)) {
      setError(`A column named "${trimmed}" already exists`)
      return
    }
    setError(null)

    // Build the next output-name map explicitly so we can persist it immediately (avoid async state lag).
    let nextOutputNames: Record<string, string>
    if (trimmed !== sourceName) {
      nextOutputNames = { ...columnOutputNames, [sourceName]: trimmed }
    } else {
      const without = { ...columnOutputNames }
      delete without[sourceName]
      nextOutputNames = without
    }

    setColumnOutputNames(nextOutputNames)
    setEditingFieldName(null)
    persistColumnRenames(nextOutputNames)
  }

  // Sortable column item component for included columns (unified list)
  const SortableColumnItem: React.FC<{
    col: string
    isPrimaryKey: boolean
    isIncluded: boolean
    onToggleInclude: (colName: string) => void
    onContextMenuColumn?: (e: React.MouseEvent, columnName: string) => void
    displayName: string
    isEditing: boolean
    onStartEdit: () => void
    onRename: (newName: string) => void
    onCancelEdit: () => void
  }> = ({ col, isPrimaryKey, isIncluded, onToggleInclude, onContextMenuColumn, displayName, isEditing, onStartEdit, onRename, onCancelEdit }) => {
    const colMeta = availableColumns.find(c => c.name === col)
    const typeInfo = getColumnTypeIcon(colMeta?.datatype)
    const TypeIcon = typeInfo.icon

    const {
      attributes,
      listeners,
      setNodeRef,
      transform,
      transition,
      isDragging,
    } = useSortable({ id: col, disabled: isPrimaryKey || !isIncluded }) // Disable drag for primary keys or excluded columns

    const isFieldSelected = selectedFields.has(col);
    const style = {
      transform: CSS.Transform.toString(transform),
      transition,
      opacity: isDragging ? 0.5 : 1,
    }

    return (
      <HStack
        ref={setNodeRef}
        style={style}
        p="4px"
        borderRadius="4px"
        mb="1px"
        borderWidth="1px"
        borderColor={isFieldSelected ? 'blue.400' : (isIncluded ? borderColor : useColorModeValue('gray.200', 'gray.600'))}
        bg={isFieldSelected ? useColorModeValue('blue.100', 'blue.900') : useColorModeValue('gray.50', 'gray.700')}
        _hover={{
          bg: isFieldSelected ? useColorModeValue('blue.100', 'blue.900') : useColorModeValue('gray.100', 'gray.600')
        }}
        opacity={isIncluded ? (isPrimaryKey ? 0.7 : 1) : 0.6}
        onClick={(e) => {
          // If this is part of a double-click, let onDoubleClick handle it (avoid toggling selection twice)
          if ((e.detail ?? 1) > 1) {
            return
          }
          // Don't trigger field selection if clicking on Select dropdown or its children
          const target = e.target as HTMLElement
          if (target.closest('select') || target.closest('[role="combobox"]') || target.tagName === 'SELECT' || target.closest('input')) {
            return
          }
          toggleFieldSelection(col, e)
        }}
        onDoubleClick={(e) => {
          const target = e.target as HTMLElement
          if (target.closest('select') || target.closest('input')) return
          e.stopPropagation()
          onStartEdit()
        }}
        onContextMenu={(e) => {
          e.preventDefault()
          onContextMenuColumn?.(e, col)
        }}
        cursor="pointer"
        title={isFieldSelected ? 'Click to deselect, Ctrl+click to toggle individual, Shift+click for range. Right-click for lineage. Double-click to rename.' : 'Click to select. Double-click to rename field. Right-click to show column lineage.'}
      >
        {/* Drag handle (only for included, non-primary-key columns) */}
        {isIncluded && !isPrimaryKey ? (
          <Box
            {...attributes}
            {...listeners}
            cursor="grab"
            _active={{ cursor: 'grabbing' }}
            color={useColorModeValue('gray.500', 'gray.400')}
          >
            <GripVertical size={16} />
          </Box>
        ) : (
          // Placeholder for excluded columns to maintain alignment
          <Box w="16px" />
        )}

        {/* Type icon with tooltip */}
        <Tooltip label={typeInfo.tooltip} placement="left" hasArrow>
          <Box color={`${typeInfo.color}.500`}>
            <TypeIcon size={16} />
          </Box>
        </Tooltip>

        {/* Column name: editable on double-click */}
        {isEditing ? (
          <Input
            size="sm"
            flex={1}
            defaultValue={displayName}
            autoFocus
            onClick={(e) => e.stopPropagation()}
            onBlur={(e) => onRename(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.currentTarget.blur()
              } else if (e.key === 'Escape') {
                onCancelEdit()
              }
            }}
            _focus={{ boxShadow: 'outline' }}
          />
        ) : (
          <Text fontSize="sm" flex={1} fontWeight={isIncluded ? 'normal' : 'light'}>
            {displayName}
          </Text>
        )}

        {/* Dropdown selector for include/exclude status */}
        <Select
          value={isIncluded ? 'true' : 'false'}
          onChange={(e) => {
            const newValue = e.target.value === 'true'
            if (newValue !== isIncluded && !isPrimaryKey) {
              onToggleInclude(col)
            }
          }}
          onClick={(e) => {
            // Stop event propagation to prevent parent HStack onClick from interfering
            e.stopPropagation()
          }}
          onMouseDown={(e) => {
            // Stop event propagation on mousedown as well
            e.stopPropagation()
          }}
          isDisabled={isPrimaryKey}
          size="sm"
          w="120px"
          borderRadius="6px"
          p="4px"
          fontSize="xs"
          cursor={isPrimaryKey ? 'not-allowed' : 'pointer'}
          opacity={isPrimaryKey ? 0.5 : 1}
          bg="#ffffff"
        >
          <option value="true">Included</option>
          <option value="false">Excluded</option>
        </Select>

        {/* Primary key badge */}
        {isPrimaryKey && (
          <Badge size="xs" colorScheme="yellow">
            <Lock size={10} style={{ display: 'inline', marginRight: '2px' }} />
            PK
          </Badge>
        )}
      </HStack>
    )
  }


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
          Select a projection node to configure
        </Text>
      </Box>
    )
  }

  return (
    <Box
      w="100%"
      h="100%"
      bg={bg}
      borderLeftWidth="1px"
      borderColor={borderColor}
      display="flex"
      flexDirection="column"
      overflow="hidden"
    >
      {/* Column lineage context menu */}
      {columnContextMenu && onLineageHighlight && (
        <Portal>
          <Box
            ref={columnContextMenuRef}
            position="fixed"
            left={columnContextMenu.x}
            top={columnContextMenu.y}
            zIndex={9999}
            bg={bg}
            borderWidth="1px"
            borderColor={borderColor}
            borderRadius="md"
            boxShadow="lg"
            py={1}
            minW="180px"
          >
            <Button
              size="sm"
              variant="ghost"
              w="100%"
              justifyContent="flex-start"
              leftIcon={<GitBranch size={16} />}
              onClick={() => {
                if (node) {
                  const path = getLineagePathForColumn(columnContextMenu.columnName, node.id, useCanvasStore.getState().nodes, useCanvasStore.getState().edges)
                  onLineageHighlight(path)
                }
                setColumnContextMenu(null)
              }}
            >
              Show lineage
            </Button>
            <Button
              size="sm"
              variant="ghost"
              w="100%"
              justifyContent="flex-start"
              onClick={() => {
                onLineageHighlight(null)
                setColumnContextMenu(null)
              }}
            >
              Clear lineage
            </Button>
            {onPropagateDownstream && (
              <>
                <Box borderTopWidth="1px" borderColor={borderColor} my={1} />
                <Button
                  size="sm"
                  variant="ghost"
                  w="100%"
                  justifyContent="flex-start"
                  leftIcon={<RefreshCw size={14} />}
                  onClick={() => {
                    // If there are selected excluded columns, include them all first,
                    // otherwise, include just this one column if it is excluded.
                    const selectedExcluded = Array.from(selectedFields).filter((c) =>
                      excludedColumns.includes(c)
                    )
                    if (selectedExcluded.length > 1) {
                      includeSelectedExcludedColumns()
                    } else if (
                      columnContextMenu.columnName &&
                      excludedColumns.includes(columnContextMenu.columnName)
                    ) {
                      toggleExcludeColumn(columnContextMenu.columnName)
                    }
                    onPropagateDownstream()
                    setColumnContextMenu(null)
                  }}
                >
                  Propagate Changes Downstream
                </Button>
              </>
            )}
            {excludedColumns.includes(columnContextMenu.columnName) &&
              !availableColumns.find((c) => c.name === columnContextMenu.columnName)?.isPrimaryKey && (
              <>
                <Box borderTopWidth="1px" borderColor={borderColor} my={1} />
                <Button
                  size="sm"
                  variant="ghost"
                  w="100%"
                  justifyContent="flex-start"
                  leftIcon={<Plus size={14} />}
                  onClick={() => {
                    toggleExcludeColumn(columnContextMenu.columnName)
                    setColumnContextMenu(null)
                  }}
                >
                  Include in projection
                </Button>
                {Array.from(selectedFields).filter((c) => excludedColumns.includes(c)).length > 1 && (
                  <Button
                    size="sm"
                    variant="ghost"
                    w="100%"
                    justifyContent="flex-start"
                    leftIcon={<Plus size={14} />}
                    onClick={() => {
                      includeSelectedExcludedColumns()
                      setColumnContextMenu(null)
                    }}
                  >
                    Include selected in projection
                  </Button>
                )}
              </>
            )}
          </Box>
        </Portal>
      )}
      {/* Header - Fixed */}
      <Box
        p={4}
        borderBottomWidth="1px"
        borderColor={borderColor}
        bg={headerBg}
        flexShrink={0}
      >
        <VStack align="stretch" spacing={3}>
          <HStack justify="space-between" align="center">
            <Text fontSize="lg" fontWeight="semibold">
              Projection Configuration
            </Text>
            {/* Live updates: no per-node Save button; state is applied immediately via useEffect */}
          </HStack>

          {/* Business Name (Editable) */}
          <Box>
            <Text fontSize="xs" fontWeight="medium" mb={1} color={useColorModeValue('gray.600', 'gray.400')}>
              Business Name
            </Text>
            <Input
              size="sm"
              value={businessName}
              onChange={(e) => setBusinessName(e.target.value)}
              placeholder="e.g., Projection_1"
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

          {/* Info about unified mode */}
          <Text fontSize="xs" color={useColorModeValue('gray.600', 'gray.400')}>
            Check columns to include. Uncheck to exclude. Drag included columns to reorder.
          </Text>
        </VStack>
      </Box>

      {/* Content with Tabs - Scrollable */}
      <Box flex={1} display="flex" flexDirection="column" overflow="hidden" minH={0}>
        <Tabs index={activeTab} onChange={setActiveTab} flex={1} display="flex" flexDirection="column" overflow="hidden" minH={0}>
          <TabList px={4} pt={2} flexShrink={0}>
            <Tab fontSize="sm">Projection</Tab>
            <Tab fontSize="sm">Calculated Columns</Tab>
          </TabList>

          <TabPanels flex={1} overflowY="auto" overflowX="hidden" display="flex" flexDirection="column" minH={0}>
            {/* Projection Tab */}
            <TabPanel
              flex={1}
              overflowY="auto"
              overflowX="hidden"
              display="flex"
              flexDirection="column"
              p={4}
              minH={0}
            >
              {error && (
                <Alert status="error" size="sm" mb={4} flexShrink={0}>
                  <AlertIcon />
                  <Text fontSize="sm">
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
                </Alert>
              )}

              <VStack
                align="stretch"
                spacing={4}
                flex={1}
                overflow="hidden"
                display="flex"
                flexDirection="column"
              >
                {/* Search */}
                <InputGroup size="sm" flexShrink={0}>
                  <InputLeftElement pointerEvents="none">
                    <Search size={14} />
                  </InputLeftElement>
                  <Input
                    placeholder="Search columns..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                  />
                </InputGroup>

                {/* Field View Navbar */}
                <Box flexShrink={0} mb={2}>
                  <HStack spacing={0} borderWidth="1px" borderColor={borderColor} borderRadius="md" overflow="hidden">
                    <Button
                      size="sm"
                      variant={activeFieldTab === 'included' ? 'solid' : 'ghost'}
                      colorScheme={activeFieldTab === 'included' ? 'blue' : 'gray'}
                      borderRadius={0}
                      flex={1}
                      onClick={() => setActiveFieldTab('included')}
                      fontSize="xs"
                      fontWeight={activeFieldTab === 'included' ? 'semibold' : 'normal'}
                    >
                      Included ({countIncluded})
                    </Button>
                    <Button
                      size="sm"
                      variant={activeFieldTab === 'excluded' ? 'solid' : 'ghost'}
                      colorScheme={activeFieldTab === 'excluded' ? 'blue' : 'gray'}
                      borderRadius={0}
                      flex={1}
                      onClick={() => setActiveFieldTab('excluded')}
                      fontSize="xs"
                      fontWeight={activeFieldTab === 'excluded' ? 'semibold' : 'normal'}
                    >
                      Excluded ({countExcluded})
                    </Button>
                    <Button
                      size="sm"
                      variant={activeFieldTab === 'all' ? 'solid' : 'ghost'}
                      colorScheme={activeFieldTab === 'all' ? 'blue' : 'gray'}
                      borderRadius={0}
                      flex={1}
                      onClick={() => setActiveFieldTab('all')}
                      fontSize="xs"
                      fontWeight={activeFieldTab === 'all' ? 'semibold' : 'normal'}
                    >
                      All Fields ({countTotal})
                    </Button>
                  </HStack>
                </Box>

                {/* Selection toolbar - Included tab: move/reorder */}
                {selectedFields.size > 0 && activeFieldTab === 'included' && (
                  <Box flexShrink={0} mb={2} p={2} bg={useColorModeValue('blue.50', 'blue.900')} borderRadius="md" borderWidth="1px" borderColor={useColorModeValue('blue.200', 'blue.700')}>
                    <HStack justify="space-between" align="center">
                      <Text fontSize="xs" fontWeight="medium" color={useColorModeValue('blue.700', 'blue.300')}>
                        {selectedFields.size} field{selectedFields.size !== 1 ? 's' : ''} selected
                      </Text>
                      <HStack spacing={1}>
                        <Tooltip label="Move selected fields up" placement="top">
                          <IconButton
                            icon={<ArrowUp size={14} />}
                            size="xs"
                            colorScheme="blue"
                            aria-label="Move up"
                            onClick={() => moveSelectedFields('up')}
                            isDisabled={!canMoveSelectedFields('up')}
                          />
                        </Tooltip>
                        <Tooltip label="Move selected fields down" placement="top">
                          <IconButton
                            icon={<ArrowDown size={14} />}
                            size="xs"
                            colorScheme="blue"
                            aria-label="Move down"
                            onClick={() => moveSelectedFields('down')}
                            isDisabled={!canMoveSelectedFields('down')}
                          />
                        </Tooltip>
                        <Tooltip label="Move selected fields to top" placement="top">
                          <IconButton
                            icon={<ArrowUpToLine size={14} />}
                            size="xs"
                            colorScheme="blue"
                            aria-label="Move to top"
                            onClick={() => moveSelectedFields('top')}
                            isDisabled={!canMoveSelectedFields('top')}
                          />
                        </Tooltip>
                        <Tooltip label="Move selected fields to bottom" placement="top">
                          <IconButton
                            icon={<ArrowDownToLine size={14} />}
                            size="xs"
                            colorScheme="blue"
                            aria-label="Move to bottom"
                            onClick={() => moveSelectedFields('bottom')}
                            isDisabled={!canMoveSelectedFields('bottom')}
                          />
                        </Tooltip>
                        <Tooltip label="Exclude selected fields from projection" placement="top">
                          <IconButton
                            icon={<Minus size={14} />}
                            size="xs"
                            colorScheme="red"
                            aria-label="Exclude selected"
                            onClick={() => {
                              excludeSelectedIncludedColumns()
                              setSelectedFields(new Set())
                            }}
                          />
                        </Tooltip>
                        <Tooltip label="Clear selection" placement="top">
                          <IconButton
                            icon={<X size={14} />}
                            size="xs"
                            colorScheme="gray"
                            aria-label="Clear selection"
                            onClick={() => setSelectedFields(new Set())}
                          />
                        </Tooltip>
                      </HStack>
                    </HStack>
                  </Box>
                )}

                {/* Selection toolbar - Excluded tab: Include selected */}
                {selectedFields.size > 0 && activeFieldTab === 'excluded' && (
                  <Box flexShrink={0} mb={2} p={2} bg={useColorModeValue('green.50', 'green.900')} borderRadius="md" borderWidth="1px" borderColor={useColorModeValue('green.200', 'green.700')}>
                    <HStack justify="space-between" align="center">
                      <Text fontSize="xs" fontWeight="medium" color={useColorModeValue('green.700', 'green.300')}>
                        {selectedFields.size} excluded field{selectedFields.size !== 1 ? 's' : ''} selected
                      </Text>
                      <HStack spacing={1}>
                        <Button
                          size="xs"
                          colorScheme="green"
                          leftIcon={<Plus size={12} />}
                          onClick={includeSelectedExcludedColumns}
                        >
                          Include selected
                        </Button>
                        <Tooltip label="Clear selection" placement="top">
                          <IconButton
                            icon={<X size={14} />}
                            size="xs"
                            colorScheme="gray"
                            aria-label="Clear selection"
                            onClick={() => setSelectedFields(new Set())}
                          />
                        </Tooltip>
                      </HStack>
                    </HStack>
                  </Box>
                )}

                {/* Select All / Select None */}
                <HStack spacing={2} flexShrink={0} mb={2}>
                  <Button
                    size="xs"
                    variant="outline"
                    onClick={() => {
                      const cols =
                        activeFieldTab === 'included'
                          ? includedColumns
                          : activeFieldTab === 'excluded'
                            ? excludedColumns
                            : filteredColumns.map((c) => c.name)
                      setSelectedFields(new Set(cols))
                    }}
                  >
                    Select all
                  </Button>
                  <Button
                    size="xs"
                    variant="outline"
                    onClick={() => setSelectedFields(new Set())}
                  >
                    Select none
                  </Button>
                </HStack>

                {/* Column List - Filtered by Active Tab */}
                <Box flex={1} display="flex" flexDirection="column" overflow="hidden" minH={0}>
                  {loading ? (
                    <Text fontSize="sm" color="gray.500">
                      Loading columns...
                    </Text>
                  ) : filteredColumns.length === 0 ? (
                    <Text fontSize="sm" color="gray.500">
                      {searchTerm ? 'No columns match your search' : `No ${activeFieldTab === 'included' ? 'included' : activeFieldTab === 'excluded' ? 'excluded' : ''} columns available`}
                    </Text>
                  ) : activeFieldTab === 'included' && includedColumns.length === 0 ? (
                    <Text fontSize="sm" color="gray.500">
                      No columns included. Check columns to include them.
                    </Text>
                  ) : (
                    <Box flex={1} overflowY="auto" minH={0}>
                      {activeFieldTab === 'included' ? (
                        // Included tab: Enable drag-and-drop and manual reordering controls
                        <DndContext
                          sensors={sensors}
                          collisionDetection={closestCenter}
                          onDragStart={() => {
                            isDraggingRef.current = true
                            console.log('[ProjectionConfig] Drag started - blocking useEffect')
                          }}
                          onDragEnd={handleDragEnd}
                        >
                          <SortableContext
                            items={includedColumns}
                            strategy={verticalListSortingStrategy}
                            key={`sortable-${includedColumns.join('-')}-${renderKey}`}
                          >
                            <VStack align="stretch" spacing={2} key={`vstack-${filteredColumns.map(c => c.name).join('-')}-${renderKey}`}>
                              {filteredColumns.map((col, displayIndex) => {
                                const isIncluded = includedColumns.includes(col.name)
                                const colIndex = isIncluded ? includedColumns.indexOf(col.name) : -1
                                return (
                                  <SortableColumnItem
                                    key={`${col.name}-${displayIndex}-${colIndex}-${renderKey}`}
                                    col={col.name}
                                    isPrimaryKey={col.isPrimaryKey || false}
                                    isIncluded={isIncluded}
                                    onToggleInclude={toggleExcludeColumn}
                                    onContextMenuColumn={onLineageHighlight ? (e, columnName) => setColumnContextMenu({ x: e.clientX, y: e.clientY, columnName }) : undefined}
                                    displayName={getOutputName(col.name)}
                                    isEditing={editingFieldName === col.name}
                                    onStartEdit={() => setEditingFieldName(col.name)}
                                    onRename={(newName) => handleRenameField(col.name, newName)}
                                    onCancelEdit={() => setEditingFieldName(null)}
                                  />
                                )
                              })}
                            </VStack>
                          </SortableContext>
                        </DndContext>
                      ) : (
                        // All Fields or Excluded tab: No drag-and-drop, strictly original order
                        <VStack align="stretch" spacing={2} key={`${activeFieldTab}-vstack-${filteredColumns.map(c => c.name).join('-')}-${renderKey}`}>
                          {filteredColumns.map((col, displayIndex) => {
                            const isIncluded = includedColumns.includes(col.name)
                            const colIndex = isIncluded ? includedColumns.indexOf(col.name) : -1
                            return (
                              <SortableColumnItem
                                key={`${activeFieldTab}-${col.name}-${displayIndex}-${colIndex}-${renderKey}`}
                                col={col.name}
                                isPrimaryKey={col.isPrimaryKey || false}
                                isIncluded={isIncluded}
                                onToggleInclude={toggleExcludeColumn}
                                onContextMenuColumn={onLineageHighlight ? (e, columnName) => setColumnContextMenu({ x: e.clientX, y: e.clientY, columnName }) : undefined}
                                displayName={getOutputName(col.name)}
                                isEditing={editingFieldName === col.name}
                                onStartEdit={() => setEditingFieldName(col.name)}
                                onRename={(newName) => handleRenameField(col.name, newName)}
                                onCancelEdit={() => setEditingFieldName(null)}
                              />
                            )
                          })}
                        </VStack>
                      )}
                    </Box>
                  )}
                </Box>
              </VStack>
            </TabPanel>

            {/* Calculated Columns Tab */}
            <TabPanel flex={1} overflowY="auto" p={4}>
              {error && (
                <Alert status="error" size="sm" mb={4}>
                  <AlertIcon />
                  <Text fontSize="sm">
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
                </Alert>
              )}

              <VStack align="stretch" spacing={4}>
                {calculatedColumns.length === 0 ? (
                  <Box textAlign="center" py={8}>
                    <Text fontSize="sm" color="gray.500" mb={4}>
                      No calculated columns defined
                    </Text>
                    <Button leftIcon={<Plus />} size="sm" onClick={addCalculatedColumn}>
                      Add Calculated Column
                    </Button>
                  </Box>
                ) : (
                  calculatedColumns.map((column, index) => (
                    <Box
                      key={column.id}
                      p={3}
                      borderWidth="1px"
                      borderColor={
                        column.validation
                          ? column.validation.success
                            ? 'green.300'
                            : 'red.300'
                          : borderColor
                      }
                      borderRadius="md"
                    >
                      <HStack justify="space-between" mb={2}>
                        <HStack spacing={2}>
                          <Text fontSize="sm" fontWeight="semibold">
                            Column {index + 1}
                          </Text>
                          {column.validation && (
                            column.validation.success ? (
                              <CheckCircle size={14} color="green" />
                            ) : (
                              <AlertCircle size={14} color="red" />
                            )
                          )}
                        </HStack>
                        <HStack spacing={1}>
                          <IconButton
                            aria-label="Remove column"
                            icon={<X size={12} />}
                            size="xs"
                            variant="ghost"
                            colorScheme="red"
                            onClick={() => removeCalculatedColumn(column.id)}
                          />
                        </HStack>
                      </HStack>

                      <VStack align="stretch" spacing={2}>
                        <FormControl>
                          <FormLabel fontSize="xs">Column Name</FormLabel>
                          <Input
                            size="sm"
                            value={column.name}
                            onChange={(e) =>
                              updateCalculatedColumn(column.id, { name: e.target.value })
                            }
                            placeholder="Enter column name"
                          />
                        </FormControl>

                        <FormControl isInvalid={column.validation && !column.validation.success}>
                          <HStack justify="space-between" mb={1}>
                            <FormLabel fontSize="xs" mb={0}>Expression</FormLabel>
                            <HStack spacing={2}>
                              <Button
                                size="xs"
                                colorScheme="blue"
                                variant="outline"
                                onClick={() => validateExpression(column.id)}
                                isLoading={column.isValidating}
                                leftIcon={column.validation?.success ? <CheckCircle size={12} /> : undefined}
                              >
                                Validate Expression
                              </Button>
                              {column.expression.trim() && (
                                <Button
                                  size="xs"
                                  colorScheme="purple"
                                  variant="outline"
                                  onClick={() => runExpressionTests(column.id)}
                                  isLoading={column.isRunningTests}
                                  leftIcon={<Code size={12} />}
                                >
                                  Run Tests
                                </Button>
                              )}
                            </HStack>
                          </HStack>
                          <Textarea
                            ref={(el) => {
                              expressionRefs.current[column.id] = el
                            }}
                            size="sm"
                            value={column.expression}
                            onChange={(e) => {
                              const newExpression = e.target.value
                              
                              // ON MANUAL INPUT BEHAVIOR (per specification)
                              // Manual typing must not auto-register calculated columns
                              // Update expression
                              updateCalculatedColumn(column.id, { 
                                expression: newExpression,
                              })
                              
                              // VALIDATION RULE VAL-003: Editor must not close on validation failure
                              // Clear validation when expression changes (non-destructive)
                              setCalculatedColumns(prev =>
                                prev.map(c => {
                                  if (c.id === column.id) {
                                    // Auto-generate tests if expression contains supported functions and no tests exist
                                    const upperExpr = newExpression.toUpperCase()
                                    const hasFunction = upperExpr.includes('CONCAT') || upperExpr.includes('UPPER') ||
                                      upperExpr.includes('LOWER') || upperExpr.includes('SUBSTRING')
                                    const needsTests = hasFunction && (!c.tests || c.tests.length === 0)
                                    return {
                                      ...c,
                                      validation: undefined, // Clear validation but keep editor open
                                      isValidating: false,
                                      testResults: undefined, // Clear test results when expression changes
                                      tests: needsTests ? generateDefaultTests(newExpression, includedColumns) : c.tests
                                    }
                                  }
                                  return c
                                })
                              )
                            }}
                            placeholder="e.g., CONCAT(firstname, lastname) - CONCAT(arg1, arg2, ...)"
                            rows={3}
                            borderColor={
                              column.validation
                                ? column.validation.success
                                  ? 'green.300'
                                  : 'red.300'
                                : undefined
                            }
                            borderWidth={column.validation ? '2px' : '1px'}
                          />
                          {column.validation && (
                            <Box mt={1}>
                              {column.validation.success ? (
                                <VStack align="start" spacing={1}>
                                  <HStack spacing={1} color="green.500" fontSize="xs">
                                    <CheckCircle size={12} />
                                    <Text>
                                      Expression is valid
                                      {column.validation.inferred_type && ` (returns ${column.validation.inferred_type})`}
                                    </Text>
                                  </HStack>
                                  {/* Show function signature if expression contains a function */}
                                  {(() => {
                                    const upperExpr = column.expression.toUpperCase()
                                    const funcMatch = functions.find(f => upperExpr.includes(f.name))
                                    if (funcMatch) {
                                      return (
                                        <Text fontSize="xs" color="gray.600" fontFamily="mono" pl={4}>
                                          {funcMatch.signature}
                                        </Text>
                                      )
                                    }
                                    return null
                                  })()}
                                </VStack>
                              ) : (
                                <VStack align="start" spacing={0.5} mt={1}>
                                  {column.validation.errors.map((err, idx) => (
                                    <HStack key={idx} spacing={1} color="red.500" fontSize="xs">
                                      <AlertCircle size={12} />
                                      <Text>{err}</Text>
                                    </HStack>
                                  ))}
                                </VStack>
                              )}
                            </Box>
                          )}
                        </FormControl>

                        {/* Expression Test Results - Collapsible Accordion */}
                        {column.testResults && column.testResults.length > 0 && (
                          <Accordion allowToggle defaultIndex={[]}>
                            <AccordionItem border="none">
                              <AccordionButton
                                px={2}
                                py={2}
                                borderRadius="md"
                                bg="gray.50"
                                _hover={{ bg: 'gray.100' }}
                                _expanded={{ bg: 'gray.100' }}
                              >
                                <HStack flex={1} justify="space-between">
                                  <HStack spacing={2}>
                                    <Text fontSize="xs" fontWeight="semibold">
                                      Test Results ({column.testResults.length} tests)
                                    </Text>
                                    <Badge
                                      colorScheme={column.testResults.every(r => r.passed) ? 'green' : 'red'}
                                      fontSize="xs"
                                    >
                                      {column.testResults.filter(r => r.passed).length}/{column.testResults.length} passed
                                    </Badge>
                                  </HStack>
                                  <AccordionIcon />
                                </HStack>
                              </AccordionButton>
                              <AccordionPanel px={2} pb={2}>
                                <VStack align="stretch" spacing={1}>
                                  {column.testResults.map((result, idx) => (
                                    <Box
                                      key={idx}
                                      px={2}
                                      py={1}
                                      fontSize="xs"
                                      fontFamily="mono"
                                      bg={result.passed ? 'green.50' : 'red.50'}
                                      borderLeftWidth="3px"
                                      borderLeftColor={result.passed ? 'green.400' : 'red.400'}
                                      borderRadius="sm"
                                    >
                                      <HStack spacing={2}>
                                        <Text
                                          fontWeight="semibold"
                                          color={result.passed ? 'green.700' : 'red.700'}
                                        >
                                          {result.passed ? '[PASS]' : '[FAIL]'}
                                        </Text>
                                        <Text flex={1} fontSize="xs">
                                          {result.test?.description || 'Test case'}
                                        </Text>
                                      </HStack>
                                      {!result.passed && (
                                        <Text mt={1} color="red.600" fontSize="xs">
                                          {result.error
                                            ? `Error: ${result.error}`
                                            : `Expected: ${JSON.stringify(result.test?.expected)}, Got: ${JSON.stringify(result.actual)}`
                                          }
                                        </Text>
                                      )}
                                      {result.passed && (
                                        <VStack align="stretch" spacing={1} mt={1}>
                                          <Text color="green.600" fontSize="xs" fontFamily="mono">
                                            {(() => {
                                              const inputRow = result.test?.inputRow || {}
                                              const inputStr = Object.entries(inputRow)
                                                .map(([k, v]) => `${k}=${v === null ? 'NULL' : JSON.stringify(v)}`)
                                                .join(', ')
                                              return `${inputStr} → ${JSON.stringify(result.actual)}`
                                            })()}
                                          </Text>
                                          {/* Show nested evaluation steps if available */}
                                          {result.debug_steps && result.debug_steps.length > 0 && (
                                            <Box mt={2} pl={2} borderLeftWidth="2px" borderLeftColor="green.300">
                                              <Text fontSize="xs" fontWeight="semibold" color="gray.600" mb={1}>
                                                Nested Evaluation Steps:
                                              </Text>
                                              <VStack align="stretch" spacing={0.5}>
                                                {result.debug_steps.map((step, stepIdx) => (
                                                  <HStack key={stepIdx} spacing={2} fontSize="xs" fontFamily="mono">
                                                    <Text color="blue.600" minW="80px">
                                                      {step.stage}:
                                                    </Text>
                                                    <Text color="gray.700">
                                                      {Array.isArray(step.input)
                                                        ? `[${step.input.map(v => JSON.stringify(v)).join(', ')}]`
                                                        : JSON.stringify(step.input)}
                                                    </Text>
                                                    <Text color="gray.500">→</Text>
                                                    <Text color="green.700" fontWeight="medium">
                                                      {JSON.stringify(step.output)}
                                                    </Text>
                                                  </HStack>
                                                ))}
                                              </VStack>
                                            </Box>
                                          )}
                                        </VStack>
                                      )}
                                    </Box>
                                  ))}
                                  <Button
                                    size="xs"
                                    variant="outline"
                                    colorScheme="blue"
                                    leftIcon={<Code size={12} />}
                                    onClick={() => {
                                      setDebugColumn(column)
                                      onOpenDebug()
                                    }}
                                    mt={2}
                                  >
                                    View Runtime Logs
                                  </Button>
                                </VStack>
                              </AccordionPanel>
                            </AccordionItem>
                          </Accordion>
                        )}

                        <FormControl>
                          <FormLabel fontSize="xs">Data Type</FormLabel>
                          <Select
                            size="sm"
                            value={column.dataType}
                            onChange={(e) =>
                              updateCalculatedColumn(column.id, { dataType: e.target.value })
                            }
                          >
                            {dataTypes.map((type) => (
                              <option key={type} value={type}>
                                {type}
                              </option>
                            ))}
                          </Select>
                        </FormControl>

                        {/* Quick Insert: Columns */}
                        {includedColumns.length > 0 && (
                          <Box>
                            <Text fontSize="xs" color="gray.500" mb={1}>
                              Insert Column:
                            </Text>
                            <HStack spacing={1} flexWrap="wrap" maxH="120px" overflowY="auto">
                              {includedColumns.map((col) => (
                                <Button
                                  key={col}
                                  size="xs"
                                  variant="outline"
                                  onClick={() => insertColumnIntoExpression(col, column.id)}
                                >
                                  {col}
                                </Button>
                              ))}
                            </HStack>
                          </Box>
                        )}

                        {/* Quick Insert: Functions */}
                        <Box>
                          <Text fontSize="xs" color="gray.500" mb={1}>
                            Insert Function:
                          </Text>
                          <Input
                            size="xs"
                            placeholder="Search functions..."
                            value={functionSearch}
                            onChange={(e) => setFunctionSearch(e.target.value)}
                            mb={1}
                          />
                          <HStack spacing={1} flexWrap="wrap" maxH="100px" overflowY="auto">
                            {filteredFunctions.slice(0, 6).map((func) => (
                              <Tooltip
                                key={func.name}
                                label={
                                  <VStack align="start" spacing={1} fontSize="xs">
                                    <Text fontWeight="bold">{func.signature}</Text>
                                    <Text>{func.description}</Text>
                                    {func.parameters.length > 0 && (
                                      <Box>
                                        <Text fontWeight="semibold" mb={1}>Parameters:</Text>
                                        {func.parameters.map((param, idx) => (
                                          <Text key={idx} pl={2}>• {param}</Text>
                                        ))}
                                      </Box>
                                    )}
                                    <Text fontWeight="semibold" mt={1}>Example:</Text>
                                    <Text fontFamily="mono" bg="blackAlpha.300" px={1} borderRadius="sm">
                                      {func.example}
                                    </Text>
                                  </VStack>
                                }
                                placement="top"
                                hasArrow
                                bg="gray.800"
                                color="white"
                                p={3}
                                borderRadius="md"
                                maxW="300px"
                              >
                                <Button
                                  size="xs"
                                  variant="outline"
                                  leftIcon={<Code size={10} />}
                                  onClick={() => insertFunctionIntoExpression(func.name, column.id)}
                                >
                                  {func.name}
                                </Button>
                              </Tooltip>
                            ))}
                          </HStack>
                        </Box>
                      </VStack>
                    </Box>
                  ))
                )}

                <Button leftIcon={<Plus />} size="sm" variant="outline" onClick={addCalculatedColumn}>
                  Add Calculated Column
                </Button>
              </VStack>
            </TabPanel>
          </TabPanels>
        </Tabs>
      </Box>

      {/* Footer - Show validation message only (Save button moved to header) */}
      {!allCalculatedColumnsValid && (
        <Box
          p={3}
          borderTopWidth="1px"
          borderColor={borderColor}
          bg="red.50"
          flexShrink={0}
        >
          <Text fontSize="xs" color="red.600" textAlign="center" fontWeight="medium">
            ⚠️ Some calculated columns have invalid expressions. Please validate them before saving.
          </Text>
        </Box>
      )}

      {/* Debug Modal for Runtime Logs */}
      <Modal isOpen={isDebugOpen} onClose={onCloseDebug} size="xl">
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>
            Runtime Logs: {debugColumn?.name || 'Calculated Column'}
          </ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            {debugColumn && (
              <VStack align="stretch" spacing={4}>
                <Box>
                  <Text fontSize="sm" fontWeight="semibold" mb={2}>Original Expression</Text>
                  <ChakraCode p={2} display="block" whiteSpace="pre-wrap" fontSize="xs">
                    {debugColumn.expression}
                  </ChakraCode>
                </Box>

                <Box>
                  <Text fontSize="sm" fontWeight="semibold" mb={2}>Transformed SQL</Text>
                  <ChakraCode p={2} display="block" whiteSpace="pre-wrap" fontSize="xs" colorScheme="blue">
                    {(() => {
                      // Simulate the transformation that happens in backend
                      let sql = debugColumn.expression
                      // This is a simplified version - actual transformation happens in backend
                      // In production, this would come from backend logs or API response
                      return `(${sql}) AS "${debugColumn.name}"`
                    })()}
                  </ChakraCode>
                  <Text fontSize="xs" color="gray.500" mt={1}>
                    Note: Check backend logs for actual SQL transformation with COALESCE wrapping
                  </Text>
                </Box>

                {debugColumn.testResults && debugColumn.testResults.length > 0 && (
                  <Box>
                    <Text fontSize="sm" fontWeight="semibold" mb={2}>Test Results Summary</Text>
                    <VStack align="stretch" spacing={2}>
                      {debugColumn.testResults.map((result, idx) => (
                        <Box key={idx} p={2} bg={result.passed ? 'green.50' : 'red.50'} borderRadius="sm">
                          <Text fontSize="xs" fontWeight="semibold">
                            {result.passed ? '✓ PASS' : '✗ FAIL'}
                          </Text>
                          <Text fontSize="xs" mt={1}>
                            Input: {(() => {
                              const inputRow = result.test?.inputRow || {}
                              return Object.entries(inputRow)
                                .map(([k, v]) => `${k}=${v === null ? 'NULL' : JSON.stringify(v)}`)
                                .join(', ')
                            })()}
                          </Text>
                          <Text fontSize="xs">
                            Expected: {JSON.stringify(result.test?.expected)}
                          </Text>
                          <Text fontSize="xs">
                            Got: {JSON.stringify(result.actual)}
                          </Text>
                          {result.error && (
                            <Text fontSize="xs" color="red.600" mt={1}>
                              Error: {result.error}
                            </Text>
                          )}
                        </Box>
                      ))}
                    </VStack>
                  </Box>
                )}

                <Box>
                  <Text fontSize="sm" fontWeight="semibold" mb={2}>Debugging Tips</Text>
                  <VStack align="stretch" spacing={2} fontSize="xs">
                    <Text>• Check backend logs for detailed SQL transformation</Text>
                    <Text>• Verify input columns are not NULL in source data</Text>
                    <Text>• Ensure COALESCE wrapping is applied to string functions</Text>
                    <Text>• Check that calculated column name matches in SQL SELECT clause</Text>
                  </VStack>
                </Box>
              </VStack>
            )}
          </ModalBody>
          <ModalFooter>
            <Button size="sm" onClick={onCloseDebug}>Close</Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </Box>
  )
}

