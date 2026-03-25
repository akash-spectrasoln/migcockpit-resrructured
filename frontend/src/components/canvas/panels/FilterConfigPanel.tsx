/**
 * Enhanced Filter Configuration Panel Component
 * Provides a sophisticated expression editor with function picker, validation, and SQL-like interface
 */
import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react'
import {
  Box,
  VStack,
  HStack,
  Text,
  Button,
  Select,
  Input,
  InputGroup,
  InputLeftElement,
  IconButton,
  Badge,
  Alert,
  AlertIcon,
  useColorModeValue,
  FormControl,
  FormLabel,
  Tooltip,
  Textarea,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
} from '@chakra-ui/react'
import { Plus, X, Code as CodeIcon, Search, CheckCircle, XCircle, Save } from 'lucide-react'
import { Node, Edge } from 'reactflow'
import { useCanvasStore } from '../../../store/canvasStore'
import { validationApi } from '../../../lib/axios/api-client'
import { 
  TypedFilterCondition, 
  ColumnMetadata, 
  parseFilterValue, 
  getInputTypeForColumn 
} from '../../../types/filterTypes'

interface FilterCondition extends TypedFilterCondition {
  expression?: string  // For expression mode
}

interface FunctionDefinition {
  name: string
  category: 'string' | 'numeric' | 'date' | 'logical'
  description: string
  syntax: string
  returnType: string
}

/** Check if column type is boolean (case-insensitive; includes BIT for SQL Server) */
function isBooleanColumnType(datatype: string | undefined): boolean {
  if (!datatype) return false
  const upper = String(datatype).toUpperCase()
  return upper === 'BOOLEAN' || upper === 'BOOL' || upper === 'BIT'
}

const operators = [
  { value: '=', label: 'Equals (=)' },
  { value: '!=', label: 'Not Equals (!=)' },
  { value: '>', label: 'Greater Than (>)' },
  { value: '<', label: 'Less Than (<)' },
  { value: '>=', label: 'Greater or Equal (>=)' },
  { value: '<=', label: 'Less or Equal (<=)' },
  { value: 'LIKE', label: 'Contains (LIKE)' },
  { value: 'ILIKE', label: 'Contains (Case Insensitive)' },
  { value: 'IN', label: 'In List (IN)' },
  { value: 'NOT IN', label: 'Not In List (NOT IN)' },
  { value: 'BETWEEN', label: 'Between (BETWEEN)' },
  { value: 'IS NULL', label: 'Is Null' },
  { value: 'IS NOT NULL', label: 'Is Not Null' },
]

const functions: FunctionDefinition[] = [
  // String functions
  { name: 'UPPER', category: 'string', description: 'Convert string to uppercase', syntax: 'UPPER(column)', returnType: 'STRING' },
  { name: 'LOWER', category: 'string', description: 'Convert string to lowercase', syntax: 'LOWER(column)', returnType: 'STRING' },
  { name: 'SUBSTRING', category: 'string', description: 'Extract substring', syntax: 'SUBSTRING(column, start, length)', returnType: 'STRING' },
  { name: 'TRIM', category: 'string', description: 'Remove leading/trailing spaces', syntax: 'TRIM(column)', returnType: 'STRING' },
  { name: 'LENGTH', category: 'string', description: 'Get string length', syntax: 'LENGTH(column)', returnType: 'INTEGER' },
  { name: 'CONCAT', category: 'string', description: 'Concatenate strings', syntax: 'CONCAT(col1, col2, ...)', returnType: 'STRING' },
  { name: 'REPLACE', category: 'string', description: 'Replace substring', syntax: 'REPLACE(column, old, new)', returnType: 'STRING' },

  // Numeric functions
  { name: 'ABS', category: 'numeric', description: 'Absolute value', syntax: 'ABS(column)', returnType: 'NUMERIC' },
  { name: 'ROUND', category: 'numeric', description: 'Round to decimal places', syntax: 'ROUND(column, decimals)', returnType: 'NUMERIC' },
  { name: 'FLOOR', category: 'numeric', description: 'Round down', syntax: 'FLOOR(column)', returnType: 'NUMERIC' },
  { name: 'CEIL', category: 'numeric', description: 'Round up', syntax: 'CEIL(column)', returnType: 'NUMERIC' },
  { name: 'SUM', category: 'numeric', description: 'Sum values', syntax: 'SUM(column)', returnType: 'NUMERIC' },
  { name: 'AVG', category: 'numeric', description: 'Average values', syntax: 'AVG(column)', returnType: 'NUMERIC' },
  { name: 'MAX', category: 'numeric', description: 'Maximum value', syntax: 'MAX(column)', returnType: 'NUMERIC' },
  { name: 'MIN', category: 'numeric', description: 'Minimum value', syntax: 'MIN(column)', returnType: 'NUMERIC' },

  // Date functions
  { name: 'CURRENT_DATE', category: 'date', description: 'Current date', syntax: 'CURRENT_DATE', returnType: 'DATE' },
  { name: 'CURRENT_TIMESTAMP', category: 'date', description: 'Current timestamp', syntax: 'CURRENT_TIMESTAMP', returnType: 'TIMESTAMP' },
  { name: 'DATEADD', category: 'date', description: 'Add to date', syntax: 'DATEADD(unit, value, date)', returnType: 'DATE' },
  { name: 'DATEDIFF', category: 'date', description: 'Date difference', syntax: 'DATEDIFF(unit, date1, date2)', returnType: 'INTEGER' },
  { name: 'YEAR', category: 'date', description: 'Extract year', syntax: 'YEAR(date)', returnType: 'INTEGER' },
  { name: 'MONTH', category: 'date', description: 'Extract month', syntax: 'MONTH(date)', returnType: 'INTEGER' },
  { name: 'DAY', category: 'date', description: 'Extract day', syntax: 'DAY(date)', returnType: 'INTEGER' },

  // Logical functions
  { name: 'IF', category: 'logical', description: 'Conditional expression', syntax: 'IF(condition, true_value, false_value)', returnType: 'ANY' },
  { name: 'CASE', category: 'logical', description: 'Case statement', syntax: 'CASE WHEN ... THEN ... ELSE ... END', returnType: 'ANY' },
  { name: 'COALESCE', category: 'logical', description: 'Return first non-null value', syntax: 'COALESCE(val1, val2, ...)', returnType: 'ANY' },
  { name: 'NULLIF', category: 'logical', description: 'Return NULL if equal', syntax: 'NULLIF(val1, val2)', returnType: 'ANY' },
]

const valueOperators = ['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'ILIKE', 'IN', 'NOT IN', 'BETWEEN']
const noValueOperators = ['IS NULL', 'IS NOT NULL']

/** Operators valid for boolean columns - LIKE/ILIKE/BETWEEN etc. don't apply */
const booleanOperators = [
  { value: '=', label: 'Equals (=)' },
  { value: '!=', label: 'Not Equals (!=)' },
  { value: 'IS NULL', label: 'Is Null' },
  { value: 'IS NOT NULL', label: 'Is Not Null' },
]

/** Get operators for a column based on its datatype */
function getOperatorsForColumn(columnName: string, columns: ColumnMetadata[]): typeof operators {
  const col = columns.find((c) => (c.technical_name ?? c.name) === columnName || c.name === columnName)
  if (isBooleanColumnType(col?.datatype)) return booleanOperators
  return operators
}

interface FilterConfigPanelProps {
  node: Node | null
  nodes: Node[]
  edges: Edge[]
  onUpdate: (nodeId: string, config: any) => void
  onFilterSaved?: (nodeId: string, config: any) => void // Callback when filter is saved
  onClose?: () => void // Callback to close the panel after saving
  directFilterMode?: {
    sourceId: number
    tableName: string
    schema?: string
    isRepository?: boolean
  } | null
  existingFilter?: {
    conditions: any[]
    expression?: string
    mode?: 'builder' | 'expression'
  } | null
  onDirectFilterPreview?: (conditions: any[]) => void // Callback for direct filter preview
  onCloseDirectFilter?: () => void // Callback to close direct filter mode
  onClearDirectFilter?: () => void // Callback to clear direct filter (remove from storage)
}

export const FilterConfigPanel: React.FC<FilterConfigPanelProps> = ({
  node,
  nodes,
  edges,
  onUpdate,
  onFilterSaved,
  onClose,
  directFilterMode,
  existingFilter,
  onDirectFilterPreview,
  onCloseDirectFilter,
  onClearDirectFilter,
}) => {
  const [mode, setMode] = useState<'builder' | 'expression'>('builder')
  const [conditions, setConditions] = useState<FilterCondition[]>([]) // Saved conditions
  const [editingConditions, setEditingConditions] = useState<FilterCondition[]>([]) // Conditions being edited
  const [expression, setExpression] = useState('')
  const [availableColumns, setAvailableColumns] = useState<ColumnMetadata[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [validationError, setValidationError] = useState<string | null>(null)
  const [validationSuccess, setValidationSuccess] = useState<string | null>(null)
  const [previewCount, setPreviewCount] = useState<number | null>(null)
  const [functionSearch, setFunctionSearch] = useState('')
  const [selectedFunctionCategory, setSelectedFunctionCategory] = useState<string>('all')
  const compiledGraph = useCanvasStore((s) => s.compiledGraph)
  const [hasUpstreamMetadata, setHasUpstreamMetadata] = useState(false) // Track if upstream metadata is loaded
  const [upstreamNodeId, setUpstreamNodeId] = useState<string | null>(null) // Track upstream node ID

  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const headerBg = useColorModeValue('gray.50', 'gray.700')
  const textColor = useColorModeValue('gray.800', 'white')

  // Track the last node ID to detect when we switch to a different node
  const [lastNodeId, setLastNodeId] = useState<string | null>(null)

  // ✅ FIX: Stabilize inputNode lookup with useMemo to prevent re-render loops
  const inputNodeId = useMemo(() => {
    if (!node || directFilterMode || node.data.type !== 'filter') return null

    const inputNodeIds = node.data.input_nodes || []
    if (inputNodeIds.length > 0) return inputNodeIds[0]

    const inputEdge = edges?.find((e) => e.target === node.id)
    return inputEdge?.source || null
  }, [node?.id, node?.data.input_nodes, node?.data.type, edges?.length, directFilterMode])

  const inputNode = useMemo(() => {
    if (!inputNodeId || !nodes) return null
    return nodes.find((n) => n.id === inputNodeId) || null
  }, [inputNodeId, nodes?.length])

  // ✅ FIX: Separate effect for direct filter mode (runs once)
  useEffect(() => {
    if (!directFilterMode) return

    loadAvailableColumnsForDirectFilter()

    if (existingFilter) {
      setConditions(existingFilter.conditions || [])
      setExpression(existingFilter.expression || '')
      setMode(existingFilter.mode || 'builder')
    } else {
      const filterKey = `table_filter_${directFilterMode.sourceId}_${directFilterMode.tableName}_${directFilterMode.schema || 'default'}`
      try {
        const saved = localStorage.getItem(filterKey)
        if (saved) {
          const savedFilter = JSON.parse(saved)
          setConditions(savedFilter.conditions || [])
          setExpression(savedFilter.expression || '')
          setMode(savedFilter.mode || 'builder')
        } else {
          setConditions([])
          setExpression('')
          setMode('builder')
        }
      } catch (err) {
        console.warn('Failed to load saved filter:', err)
        setConditions([])
        setExpression('')
        setMode('builder')
      }
    }
  }, [directFilterMode?.sourceId, directFilterMode?.tableName, directFilterMode?.schema])

  // ✅ FIX: Separate effect for loading node config (runs only when node changes)
  useEffect(() => {
    if (directFilterMode || !node) {
      setLastNodeId(null)
      if (!directFilterMode) {
        setConditions([])
        setExpression('')
      }
      return
    }

    const config = node.data.config || {}
    const savedConditions = config.conditions || config.filterConditions || []
    const savedExpression = config.expression || config.filterExpression || ''
    const savedMode = config.mode || config.filterMode || 'builder'

    // Only update if node changed
    if (node.id !== lastNodeId) {
      const normalizedConditions = Array.isArray(savedConditions)
        ? savedConditions.map((c: any) => ({
          id: c.id || `condition-${Date.now()}-${Math.random()}`,
          column: c.column || '',
          operator: c.operator || '=',
          value: c.value,
          logicalOperator: c.logicalOperator || 'AND',
        }))
        : []

      setConditions(normalizedConditions)
      setEditingConditions([]) // Clear any editing conditions when loading saved config
      setExpression(savedExpression || '')
      setMode(savedMode || 'builder')
      setLastNodeId(node.id)
    }
  }, [node?.id, directFilterMode])

  // ✅ FIX: Fast O(1) schema fetching from compiled DAG, bypassing recursive lookups or backend API calls
  useEffect(() => {
    if (!node || node.data.type !== 'filter' || directFilterMode) {
      setUpstreamNodeId(null)
      setHasUpstreamMetadata(false)
      return
    }

    if (!inputNodeId) {
      setUpstreamNodeId(null)
      setHasUpstreamMetadata(false)
      setAvailableColumns([])
      return
    }

    setUpstreamNodeId(inputNodeId)

    // Read directly from the heavily optimized compiledGraph in zustand store
    const compiledInputSchema =
      (compiledGraph && compiledGraph.nodes[node.id]?.inputSchema) || []

    // Fallback: compiledGraph can be stale for a moment; prefer inputNode's output_metadata if it's larger.
    const inputMetaCols = (inputNode as any)?.data?.output_metadata?.columns || []
    const chosenSchema =
      Array.isArray(inputMetaCols) && inputMetaCols.length > compiledInputSchema.length
        ? inputMetaCols
        : compiledInputSchema

    if (Array.isArray(chosenSchema) && chosenSchema.length > 0) {
      setHasUpstreamMetadata(true)
      setAvailableColumns(
        chosenSchema.map((col: any) => {
          const anyCol: any = col as any
          const displayName =
            anyCol.outputName ||
            anyCol.business_name ||
            anyCol.name ||
            anyCol.column_name ||
            anyCol.column ||
            String(anyCol)
          const technicalName =
            anyCol.technical_name || anyCol.db_name || anyCol.column || anyCol.name || displayName

          return {
            name: displayName,
            business_name: anyCol.business_name || displayName,
            technical_name: technicalName,
            datatype: String(anyCol.datatype || anyCol.data_type || anyCol.type || 'TEXT').toUpperCase(),
            nullable: anyCol.nullable ?? true,
          }
        })
      )
      setError(null)
      return
    }
    
    // Fallback: If no compiled graph schema yet, clear them
    setHasUpstreamMetadata(false)
    setAvailableColumns([])
  }, [inputNodeId, node?.id, compiledGraph, directFilterMode, inputNode])

  // ✅ FIX: Use useCallback with stable ID generation to prevent re-creating functions
  const conditionIdRef = useRef(0)
  const addCondition = useCallback(() => {
    conditionIdRef.current += 1
    setConditions((prev) => [
      ...prev,
      {
        id: `condition-${conditionIdRef.current}`,
        column: '',
        operator: '=',
        value: '',
        logicalOperator: prev.length > 0 ? 'AND' : undefined,
      },
    ])
  }, [])

  const removeCondition = useCallback((id: string) => {
    setConditions((prev) => prev.filter((c) => c.id !== id))
  }, [])

  const updateCondition = useCallback((id: string, updates: Partial<FilterCondition>, isEditing: boolean = false) => {
    const updateFn = (prev: FilterCondition[]) =>
      prev.map((c) => {
        if (c.id === id) {
          const cleanedUpdates = { ...updates }
          
          // Clean column name
          if (updates.column) {
            cleanedUpdates.column = updates.column.includes('(')
              ? updates.column.split('(')[0].trim()
              : updates.column.trim()
            
            // Find column metadata and store type
            const columnMeta = availableColumns.find(col => (col.technical_name ?? col.name) === cleanedUpdates.column || col.name === cleanedUpdates.column)
            if (columnMeta) {
              cleanedUpdates._columnType = columnMeta.datatype
            } else {
              // ✅ DEFENSIVE: Column not found, log warning and use TEXT as fallback
              console.warn(`Column metadata not found for: ${cleanedUpdates.column}`)
              cleanedUpdates._columnType = 'TEXT' as any
            }
          }
          
          // Type-safe value parsing when value changes
          if (updates.value !== undefined) {
            try {
              // Try to find column type if not already set
              let typeToUse = cleanedUpdates._columnType || c._columnType
              
              // If still no type, try to find it from the column name (if column is set)
              if (!typeToUse && c.column) {
                const columnMeta = availableColumns.find(col => (col.technical_name ?? col.name) === c.column || col.name === c.column)
                if (columnMeta) {
                  typeToUse = columnMeta.datatype
                  cleanedUpdates._columnType = columnMeta.datatype
                }
              }
              
              if (typeToUse) {
                cleanedUpdates.value = parseFilterValue(updates.value, c.operator || updates.operator || '=', typeToUse)
              } else {
                // Only warn if we have a column set but no type - otherwise it's expected (user typing value before selecting column)
                if (c.column || cleanedUpdates.column) {
                  console.warn(`No column type available for parsing column "${c.column || cleanedUpdates.column}", storing raw value`)
                }
                cleanedUpdates.value = updates.value
              }
            } catch (error: any) {
              // If parsing fails, store raw value and set validation error
              console.warn(`Value parsing failed: ${error.message}`)
              cleanedUpdates.value = updates.value
              setValidationError(error.message)
            }
          }
          
          return { ...c, ...cleanedUpdates }
        }
        return c
      })
    
    if (isEditing) {
      setEditingConditions(updateFn)
    } else {
      setConditions(updateFn)
    }
    
    // Clear validation error if no error occurred
    if (!updates.value) {
      setValidationError(null)
    }
  }, [availableColumns])

  const saveCondition = useCallback((condition: FilterCondition) => {
    // Validate condition before saving
    if (!condition.column || !condition.operator) {
      setValidationError('Column and operator are required')
      return
    }
    
    if (valueOperators.includes(condition.operator) && !condition.value && condition.value !== null) {
      setValidationError('Value is required for this operator')
      return
    }

    // Validate BETWEEN operator
    if (condition.operator === 'BETWEEN') {
      let value = condition.value
      if (typeof value === 'string') {
        try {
          value = JSON.parse(value)
        } catch {
          const parts = value.split(',').map((v: string) => v.trim())
          if (parts.length !== 2) {
            setValidationError(`BETWEEN operator requires exactly two values for condition: ${condition.column}`)
            return
          }
          value = parts
        }
      }
      if (!Array.isArray(value) || value.length !== 2) {
        setValidationError(`BETWEEN operator requires exactly two values [min, max] for condition: ${condition.column}`)
        return
      }
    }

    // Move from editing to saved
    const savedCondition: FilterCondition = {
      ...condition,
      logicalOperator: condition.logicalOperator || (conditions.length > 0 ? 'AND' : undefined),
    }
    setConditions((prev) => [...prev, savedCondition])
    setEditingConditions((prev) => prev.filter((c) => c.id !== condition.id))
    setValidationError(null)
  }, [conditions.length])

  const cancelCondition = useCallback((id: string) => {
    // Remove from editing conditions (don't save)
    setEditingConditions((prev) => prev.filter((c) => c.id !== id))
    setValidationError(null)
  }, [])

  const validateExpression = (expr: string): string | null => {
    if (!expr.trim()) {
      return 'Expression cannot be empty'
    }

    // Basic validation - check for balanced parentheses
    let parenCount = 0
    for (const char of expr) {
      if (char === '(') parenCount++
      if (char === ')') parenCount--
      if (parenCount < 0) return 'Unmatched closing parenthesis'
    }
    if (parenCount !== 0) return 'Unmatched opening parenthesis'

    // Check for valid column names
    const columnNames = availableColumns.map(c => c.name)
    const words = expr.split(/\s+/)
    for (const word of words) {
      const cleanWord = word.replace(/[(),]/g, '')
      if (cleanWord && !columnNames.includes(cleanWord) &&
        !functions.find(f => f.name === cleanWord.toUpperCase()) &&
        !['AND', 'OR', 'NOT', '=', '!=', '>', '<', '>=', '<=', 'LIKE', 'ILIKE', 'IN', 'NOT', 'IN', 'BETWEEN', 'IS', 'NULL'].includes(cleanWord.toUpperCase()) &&
        !/^[\d.]+$/.test(cleanWord) && !/^['"].*['"]$/.test(cleanWord)) {
        // This is a simplified check - full validation would be more complex
      }
    }

    return null
  }

  const insertColumn = (columnName: string) => {
    if (mode === 'expression') {
      setExpression(prev => prev + (prev ? ' ' : '') + columnName)
    }
  }

  const insertFunction = (func: FunctionDefinition) => {
    if (mode === 'expression') {
      // Insert function template
      const template = func.syntax.replace(/column/g, '')
      setExpression(prev => prev + (prev ? ' ' : '') + template)
    }
  }

  const insertOperator = (op: string) => {
    if (mode === 'expression') {
      setExpression(prev => prev + ' ' + op + ' ')
    }
  }

  const handleSave = (closeAfterSave: boolean = false) => {
    // Handle direct filter mode
    if (directFilterMode) {
      setValidationError(null)

      if (mode === 'builder') {
        const validConditions = conditions.filter(
          (c) => c.column && c.operator && (noValueOperators.includes(c.operator) || c.value !== '')
        )

        if (validConditions.length === 0) {
          setValidationError('At least one valid condition is required')
          return
        }

        // Validate BETWEEN operator
        for (const condition of validConditions) {
          if (condition.operator === 'BETWEEN') {
            let value = condition.value
            if (typeof value === 'string') {
              try {
                value = JSON.parse(value)
              } catch {
                const parts = value.split(',').map((v: string) => v.trim())
                if (parts.length !== 2) {
                  setValidationError(`BETWEEN operator requires exactly two values for condition: ${condition.column}`)
                  return
                }
                value = parts
              }
            }
            if (!Array.isArray(value) || value.length !== 2) {
              setValidationError(`BETWEEN operator requires exactly two values [min, max] for condition: ${condition.column}`)
              return
            }
          }
        }

        // Validate and format conditions with type-safe values
        const formattedConditions = validConditions.map(c => {
          // Find column metadata for type validation
          const columnMeta = availableColumns.find(col => (col.technical_name ?? col.name) === c.column || col.name === c.column)
          
          let parsedValue = c.value
          if (columnMeta && c.value !== null && c.value !== undefined) {
            try {
              // Ensure value is properly typed
              parsedValue = parseFilterValue(c.value, c.operator, columnMeta.datatype)
            } catch (error: any) {
              setValidationError(`Invalid value for column "${c.column}" (${columnMeta.datatype}): ${error.message}`)
              throw error
            }
          }
          
          return {
            id: c.id,
            column: c.column.trim(),
            operator: c.operator,
            value: parsedValue,
            logicalOperator: c.logicalOperator,
          }
        })

        const config = {
          conditions: formattedConditions,
          mode: 'builder',
          sourceId: directFilterMode.sourceId,
          tableName: directFilterMode.tableName,
          schema: directFilterMode.schema,
        }

        if (onFilterSaved) {
          onFilterSaved('', config) // Pass empty string for nodeId in direct filter mode
        }
      } else {
        const exprError = validateExpression(expression)
        if (exprError) {
          setValidationError(exprError)
          return
        }

        const config = {
          expression: expression,
          mode: 'expression',
          sourceId: directFilterMode.sourceId,
          tableName: directFilterMode.tableName,
          schema: directFilterMode.schema,
        }

        if (onFilterSaved) {
          onFilterSaved('', config)
        }
      }
      return
    }

    if (!node) return

    setValidationError(null)

    if (mode === 'builder') {
      // Combine saved and editing conditions (only saved ones are used for final save)
      const validConditions = conditions.filter(
        (c) => c.column && c.operator && (noValueOperators.includes(c.operator) || c.value !== '')
      )
      
      // Warn if there are unsaved editing conditions
      if (editingConditions.length > 0) {
        setValidationError(`You have ${editingConditions.length} unsaved condition(s). Please save or cancel them before saving the filter.`)
        return
      }

      if (validConditions.length === 0) {
        setValidationError('At least one valid condition is required')
        return
      }

      // Validate BETWEEN operator
      for (const condition of validConditions) {
        if (condition.operator === 'BETWEEN') {
          let value = condition.value
          if (typeof value === 'string') {
            try {
              value = JSON.parse(value)
            } catch {
              const parts = value.split(',').map((v: string) => v.trim())
              if (parts.length !== 2) {
                setValidationError(`BETWEEN operator requires exactly two values for condition: ${condition.column}`)
                return
              }
              value = parts
            }
          }
          if (!Array.isArray(value) || value.length !== 2) {
            setValidationError(`BETWEEN operator requires exactly two values [min, max] for condition: ${condition.column}`)
            return
          }
        }
      }

      // Ensure conditions array is properly formatted and type-safe
      const formattedConditions = validConditions.map(c => {
        // Find column metadata for type validation (match by technical_name or name for rename-safety)
        const columnMeta = availableColumns.find(col => (col.technical_name ?? col.name) === c.column || col.name === c.column)
        
        let parsedValue = c.value
        if (columnMeta && c.value !== null && c.value !== undefined) {
          try {
            // Ensure value is properly typed (handles BETWEEN, IN, etc.)
            parsedValue = parseFilterValue(c.value, c.operator, columnMeta.datatype)
          } catch (error: any) {
            setValidationError(`Invalid value for column "${c.column}" (${columnMeta.datatype}): ${error.message}`)
            throw error
          }
        }
        
        // Ensure all required fields are present
        const condition: any = {
          id: c.id || `condition-${Date.now()}-${Math.random()}`,
          column: (c.column || '').trim(),
          operator: c.operator || '=',
          value: parsedValue,
          logicalOperator: c.logicalOperator || 'AND',
        }

        return condition
      }).filter(c => c.column && c.operator) // Filter out any invalid conditions

      if (formattedConditions.length === 0) {
        setValidationError('At least one valid condition is required')
        return
      }

      // ✅ FIX: Use availableColumns which should have correct types loaded from source
      let outputMetadata = null
      if (availableColumns && availableColumns.length > 0) {
        // Use the columns we already loaded with proper types
        outputMetadata = {
          columns: availableColumns.map(col => ({
            name: col.business_name || col.name,
            business_name: col.business_name || col.name,
            technical_name: col.technical_name ?? col.db_name ?? col.name,
            db_name: col.db_name,
            datatype: col.datatype,
            nullable: col.nullable
          })),
          nodeId: node.data.node_id || node.id,
        }
      } else {
        // Fallback: try to get from input node if availableColumns not loaded
        const inputNodeIds = node.data.input_nodes || []
        let inputNode = null
        if (inputNodeIds.length > 0 && nodes && Array.isArray(nodes)) {
          inputNode = nodes.find((n) => n.id === inputNodeIds[0])
        } else {
          const inputEdge = edges.find((e) => e.target === node.id)
          if (inputEdge && nodes && Array.isArray(nodes)) {
            inputNode = nodes.find((n) => n.id === inputEdge.source)
          }
        }

        if (inputNode?.data.output_metadata?.columns) {
          outputMetadata = {
            columns: inputNode.data.output_metadata.columns.map((col: any) => ({
              name: typeof col === 'string' ? col : (col.business_name || col.name || col.column_name || col),
              business_name: typeof col === 'string' ? col : (col.business_name || col.name || col.column_name || col),
              technical_name: typeof col === 'string'
                ? col
                : (col.technical_name || col.db_name || col.name || col.column_name || col),
              db_name: typeof col === 'string' ? undefined : col.db_name,
              datatype: typeof col === 'string' ? 'TEXT' : ((col.datatype || col.data_type || col.type || 'TEXT').toUpperCase()),
              nullable: typeof col === 'string' ? true : (col.nullable !== undefined ? col.nullable : true),
            })),
            nodeId: node.data.node_id || node.id,
          }
        }
      }

      // Build expression from conditions (for expression mode compatibility)
      const expressionFromConditions = formattedConditions
        .map((c, idx) => {
          const prefix = idx > 0 ? ` ${c.logicalOperator || 'AND'} ` : ''
          if (noValueOperators.includes(c.operator)) {
            return `${prefix}${c.column} ${c.operator}`
          } else if (c.operator === 'IN' || c.operator === 'NOT IN') {
            const values = Array.isArray(c.value) ? c.value : [c.value]
            return `${prefix}${c.column} ${c.operator} (${values.map((v: any) => typeof v === 'string' ? `'${v}'` : v).join(', ')})`
          } else if (c.operator === 'BETWEEN') {
            const values = Array.isArray(c.value) ? c.value : [c.value]
            return `${prefix}${c.column} BETWEEN ${values[0]} AND ${values[1]}`
          } else {
            const value = typeof c.value === 'string' ? `'${c.value}'` : c.value
            return `${prefix}${c.column} ${c.operator} ${value}`
          }
        })
        .join('')

      const config = {
        ...node.data.config,
        conditions: formattedConditions,
        expression: expressionFromConditions, // Store expression for backend
        mode: 'builder',
        sourceId: node.data.config?.sourceId,
        tableName: node.data.config?.tableName,
        schema: node.data.config?.schema,
        // For source nodes, set isFiltered flag
        ...(node.data.type === 'source' ? { isFiltered: true } : {}),
      }

      // Update node with output_metadata and business_name
      onUpdate(node.id, {
        config: config,
        business_name: node.data.business_name || node.data.label || 'Filter',
        output_metadata: outputMetadata,
      })
      setError(null)
      setValidationError(null)

      // Notify parent that filter was saved - this will auto-select the node and update preview
      if (onFilterSaved) {
        onFilterSaved(node.id, config)
      }

      // Fetch metadata from source-table API only when input is a SOURCE node.
      // When input is Projection/Filter/Join, we already have correct output_metadata (e.g. 16 cols);
      // the source API would return wrong count (e.g. 19 from tool_user) and overwrite.
      let saveInputNode: Node | null = null
      const inputNodeIds = node.data.input_nodes || []
      if (inputNodeIds.length > 0 && nodes) {
        saveInputNode = nodes.find((n) => n.id === inputNodeIds[0]) || null
      } else {
        const inputEdge = edges?.find((e) => e.target === node.id)
        if (inputEdge && nodes) saveInputNode = nodes.find((n) => n.id === inputEdge.source) || null
      }
      const inputIsSource = saveInputNode?.data?.type === 'source'

      if (config.sourceId && config.tableName && inputIsSource) {
        fetchFilterMetadata(config, {
          sourceId: config.sourceId,
          tableName: config.tableName,
          schema: config.schema,
        }).then((metadata) => {
          if (metadata && metadata.columns) {
            onUpdate(node.id, {
              config: config,
              business_name: node.data.business_name || node.data.label || 'Filter',
              output_metadata: metadata,
            })
          }
          if (closeAfterSave && onClose) onClose()
        }).catch((err) => {
          console.error('Failed to fetch filter metadata:', err)
          if (closeAfterSave && onClose) onClose()
        })
      } else {
        // Close panel if no metadata fetch needed
        if (closeAfterSave && onClose) {
          onClose()
        }
      }
    } else {
      // Validate expression
      const exprError = validateExpression(expression)
      if (exprError) {
        setValidationError(exprError)
        return
      }

      // Get input node to inherit output_metadata (same columns as input)
      const inputNodeIds = node.data.input_nodes || []
      let inputNode = null
      if (inputNodeIds.length > 0 && nodes && Array.isArray(nodes)) {
        inputNode = nodes.find((n) => n.id === inputNodeIds[0])
      } else {
        // Fallback to edge traversal
        const inputEdge = edges.find((e) => e.target === node.id)
        if (inputEdge && nodes && Array.isArray(nodes)) {
          inputNode = nodes.find((n) => n.id === inputEdge.source)
        }
      }

      // Build output_metadata (same columns as input, per specification)
      let outputMetadata = null
      if (inputNode) {
        if (inputNode.data.output_metadata && inputNode.data.output_metadata.columns) {
          // Use input node's output_metadata
          outputMetadata = {
            columns: inputNode.data.output_metadata.columns,
            nodeId: node.data.node_id || node.id,
          }
        } else if (inputNode.data.config?.columns) {
          // Fallback: use config.columns
          const columns = Array.isArray(inputNode.data.config.columns)
            ? inputNode.data.config.columns.map((col: any) => ({
              name: typeof col === 'string' ? col : (col.name || col.column_name || col),
              datatype: typeof col === 'string' ? 'TEXT' : (col.datatype || col.data_type || 'TEXT'),
              nullable: typeof col === 'string' ? true : (col.nullable !== undefined ? col.nullable : true),
            }))
            : []
          outputMetadata = {
            columns: columns,
            nodeId: node.data.node_id || node.id,
          }
        }
      }

      const config = {
        ...node.data.config,
        expression: expression,
        mode: 'expression',
        // Preserve source info if available
        sourceId: node.data.config?.sourceId,
        tableName: node.data.config?.tableName,
        schema: node.data.config?.schema,
      }

      // Update node with output_metadata and business_name
      onUpdate(node.id, {
        config: config,
        business_name: node.data.business_name || node.data.label || 'Filter',
        output_metadata: outputMetadata,
      })
      setError(null)
      setValidationError(null)

      // Notify parent that filter was saved - this will auto-select the node and update preview
      if (onFilterSaved) {
        onFilterSaved(node.id, config)
      }
      
      // Close panel if requested
      if (closeAfterSave && onClose) {
        onClose()
      }
    }
  }

  // Live updates: debounced push to store when filter conditions/expression/mode change (no per-node Save button).
  const liveUpdateTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  useEffect(() => {
    if (directFilterMode || !node) return
    if (liveUpdateTimerRef.current) clearTimeout(liveUpdateTimerRef.current)
    liveUpdateTimerRef.current = setTimeout(() => {
      liveUpdateTimerRef.current = null
      handleSave(false)
    }, 300)
    return () => {
      if (liveUpdateTimerRef.current) clearTimeout(liveUpdateTimerRef.current)
    }
  }, [conditions, expression, mode, node?.id, directFilterMode])

  const fetchFilterMetadata = async (config: any, sourceConfig: any): Promise<any> => {
    if (!node || !sourceConfig?.sourceId || !sourceConfig?.tableName) return null

    try {
      if (config.mode === 'builder' && config.conditions?.length > 0) {
        // Clean column names and ensure type-safe values
        const cleanedConditions = config.conditions.map((c: any) => {
          let columnName = c.column.trim()

          // Remove type information if present e.g. "col (int)" -> "col"
          if (columnName.includes('(')) {
            columnName = columnName.split('(')[0].trim()
          }

          // Remove table prefix if present e.g. "table.col" -> "col"
          if (columnName.includes('.')) {
            const parts = columnName.split('.')
            columnName = parts[parts.length - 1].trim()
          }

          // Find column metadata for type-safe value parsing
          const columnMeta = availableColumns.find(col => (col.technical_name ?? col.name) === c.column || col.name === c.column)
          let parsedValue = c.value
          
          if (columnMeta && c.value !== null && c.value !== undefined) {
            try {
              parsedValue = parseFilterValue(c.value, c.operator, columnMeta.datatype)
            } catch (error: any) {
              console.warn(`Metadata fetch: Failed to parse value for ${c.column}:`, error.message)
              parsedValue = c.value // Fallback to raw value
            }
          }

          return {
            column: columnName,
            operator: c.operator,
            value: parsedValue,
            logicalOperator: c.logicalOperator || 'AND',
          }
        })

        // Call filter execution API to get metadata
        const { api } = await import('../../../services/api')
        const response = sourceConfig.sourceId === -1
          ? await api.post('/api/api-customer/repository/filter/', {
              table_name: sourceConfig.tableName,
              schema: sourceConfig.schema || 'repository',
              conditions: cleanedConditions,  // Send conditions array directly
              page: 1,
              page_size: 1,  // Just get metadata
            })
          : await api.post(
              `/api/api-customer/sources/${sourceConfig.sourceId}/filter/`,
              {
                table_name: sourceConfig.tableName,
                schema: sourceConfig.schema || '',
                conditions: cleanedConditions,  // Send conditions array directly
                page: 1,
                page_size: 1,  // Just get metadata
              }
            )

        const columns = response.data.columns || []
        const columnCount = columns.length

        // ✅ FIX: Extract FULL column metadata with types, not just names!
        const columnMetadata = columns.map((col: any) => {
          if (typeof col === 'string') {
            // If it's just a string, create object with default type
            return {
              name: col,
              datatype: 'TEXT',
              nullable: true
            }
          } else {
            // Normalize column metadata structure
            return {
              name: col.name || col.column_name || 'unknown',
              datatype: (col.datatype || col.data_type || col.type || 'TEXT').toUpperCase(),
              nullable: col.nullable !== undefined ? col.nullable : true
            }
          }
        })

        // ✅ FIX: Store full metadata in output_metadata for downstream nodes
        const outputMetadata = {
          columns: columnMetadata,
          nodeId: node.data.node_id || node.id,
        }

        // Update node with full metadata including types
        onUpdate(node.id, {
          config: config,
          columnCount: columnCount,
          output_metadata: outputMetadata,  // ✅ Full metadata with types!
          business_name: node.data.business_name || node.data.label || 'Filter',
        })

        return outputMetadata
      }
    } catch (err: any) {
      console.warn('Failed to fetch filter metadata:', err)
      // Don't show error - just continue without metadata
      return null
    }
    return null
  }

  const handlePreview = async () => {
    // Handle direct filter mode
    if (directFilterMode) {
      setLoading(true)
      setError(null)
      setValidationError(null)

      try {
        if (mode === 'builder') {
          const validConditions = conditions.filter(
            (c) => c.column && c.operator && (noValueOperators.includes(c.operator) || c.value !== '')
          )

          if (validConditions.length === 0) {
            setValidationError('At least one valid condition is required')
            setLoading(false)
            return
          }

          // Clean column names and prepare type-safe conditions for backend
          const cleanedConditions = validConditions.map((c: any) => {
            let columnName = c.column.trim()

            // Handle table.column format - preserve it for joined tables
            if (columnName.includes('.')) {
              const parts = columnName.split('.')
              const tablePart = parts[0].trim()
              const colPart = parts[parts.length - 1].trim()
              // Remove type info from table part if present
              const tableClean = tablePart.includes('(') ? tablePart.split('(')[0].trim() : tablePart
              columnName = `${tableClean}.${colPart}`
            } else {
              // Remove type information if present e.g. "col (int)" -> "col"
              if (columnName.includes('(')) {
                columnName = columnName.split('(')[0].trim()
              }
            }

            // Find column metadata for type-safe value parsing
            const columnMeta = availableColumns.find(col => (col.technical_name ?? col.name) === c.column || col.name === c.column)
            let parsedValue = c.value
            
            if (columnMeta && c.value !== null && c.value !== undefined) {
              try {
                parsedValue = parseFilterValue(c.value, c.operator, columnMeta.datatype)
              } catch (error: any) {
                console.warn(`Preview: Failed to parse value for ${c.column}:`, error.message)
                parsedValue = c.value // Fallback to raw value
              }
            }

            return {
              column: columnName,
              operator: c.operator,
              value: parsedValue,
              logicalOperator: c.logicalOperator || 'AND',
            }
          })

          // Call onDirectFilterPreview with cleaned conditions so the main preview grid
          // executes the filtered query instead of showing full table data
          if (onDirectFilterPreview) {
            onDirectFilterPreview(cleanedConditions)
          }
        } else {
          // Expression-mode direct preview is not yet wired to the main preview grid.
          // For now, just validate the expression and show an error if invalid.
          const exprError = validateExpression(expression)
          if (exprError) {
            setValidationError(exprError)
          } else {
            setValidationError('Expression-mode direct preview is not yet supported for data grid.')
          }
        }
      } catch (err: any) {
        setError(err.message || 'Failed to preview filter')
      } finally {
        setLoading(false)
      }
      return
    }

    if (!node) return

    setLoading(true)
    setError(null)
    setValidationError(null)

    try {
      // Build filter specification
      let filterSpec: any = {}

      if (mode === 'builder') {
        const validConditions = conditions.filter(
          (c) => c.column && c.operator && (noValueOperators.includes(c.operator) || c.value !== '')
        )

        if (validConditions.length === 0) {
          setValidationError('At least one valid condition is required')
          return
        }

        // Clean column names and prepare type-safe conditions for backend
        const cleanedConditions = validConditions.map((c: any) => {
          let columnName = c.column.trim()

          // Remove type information if present e.g. "col (int)" -> "col"
          if (columnName.includes('(')) {
            columnName = columnName.split('(')[0].trim()
          }

          // Remove table prefix if present e.g. "table.col" -> "col"
          if (columnName.includes('.')) {
            const parts = columnName.split('.')
            columnName = parts[parts.length - 1].trim()
          }

          // Find column metadata for type-safe value parsing
          const columnMeta = availableColumns.find(col => (col.technical_name ?? col.name) === c.column || col.name === c.column)
          let parsedValue = c.value
          
          if (columnMeta && c.value !== null && c.value !== undefined) {
            try {
              parsedValue = parseFilterValue(c.value, c.operator, columnMeta.datatype)
            } catch (error: any) {
              console.warn(`Preview: Failed to parse value for ${c.column}:`, error.message)
              parsedValue = c.value // Fallback to raw value
            }
          }

          return {
            column: columnName,
            operator: c.operator,
            value: parsedValue,
            logicalOperator: c.logicalOperator || 'AND',
          }
        })

        // Backend expects conditions array, not nested filterSpec
        filterSpec = cleanedConditions
      } else {
        // For expression mode, we'd need to parse the expression
        // This is a simplified version
        filterSpec = {
          type: 'expression',
          expression: expression
        }
      }

      // Find source node to get sourceId and table info
      let sourceConfig: any = null

      // Try to get from filter node's config first
      if (node.data.config?.sourceId && node.data.config?.tableName) {
        sourceConfig = {
          sourceId: node.data.config.sourceId,
          tableName: node.data.config.tableName,
          schema: node.data.config.schema,
        }
      } else {
        // Try to get from connected input node via input_nodes array (preferred) or edge traversal
        const inputNodeIds = node.data.input_nodes || []
        let inputNode = null

        if (inputNodeIds.length > 0 && nodes && Array.isArray(nodes)) {
          inputNode = nodes.find((n) => n.id === inputNodeIds[0])
        } else {
          // Fallback: check edges
          const inputEdge = edges && Array.isArray(edges) ? edges.find((e) => e.target === node.id) : null
          if (inputEdge && nodes && Array.isArray(nodes)) {
            inputNode = nodes.find((n) => n.id === inputEdge.source)
          }
        }

        if (inputNode) {
          // For source nodes, get config directly
          if (inputNode.data.type === 'source' && inputNode.data.config) {
            sourceConfig = {
              sourceId: inputNode.data.config.sourceId,
              tableName: inputNode.data.config.tableName,
              schema: inputNode.data.config.schema,
            }
          } else if (inputNode.data.config?.sourceId && inputNode.data.config?.tableName) {
            // For transform nodes, try to get source info from their config
            sourceConfig = {
              sourceId: inputNode.data.config.sourceId,
              tableName: inputNode.data.config.tableName,
              schema: inputNode.data.config.schema,
            }
          }
        }
      }

      if (!sourceConfig || !sourceConfig.sourceId || !sourceConfig.tableName) {
        setError('Filter node must be connected to a source node or have source configuration')
        setLoading(false)
        return
      }

      // Call filter execution API to preview filtered data
      const { api } = await import('../../../services/api')
      const response = sourceConfig.sourceId === -1
        ? await api.post('/api/api-customer/repository/filter/', {
            table_name: sourceConfig.tableName,
            schema: sourceConfig.schema || 'repository',
            conditions: filterSpec,  // Send conditions array directly
            page: 1,
            page_size: 100,  // Get preview rows (limited to 100 for preview)
          })
        : await api.post(
            `/api/api-customer/sources/${sourceConfig.sourceId}/filter/`,
            {
              table_name: sourceConfig.tableName,
              schema: sourceConfig.schema || '',
              conditions: filterSpec,  // Send conditions array directly
              page: 1,
              page_size: 100,  // Get preview rows (limited to 100 for preview)
            }
          )

      const total = response.data.total || response.data.filtered_count || 0
      const columns = response.data.columns || []
      const rows = response.data.rows || []

      setPreviewCount(total)

      // Update node with metadata
      onUpdate(node.id, {
        config: {
          ...node.data.config,
          columnCount: columns.length,
        },
      })

      // If onFilterSaved callback exists, trigger it to update preview panel with filtered data
      if (onFilterSaved && rows.length > 0) {
        // Temporarily save the filter config to trigger preview update
        const tempConfig = {
          ...node.data.config,
          conditions: mode === 'builder' ? filterSpec : undefined,
          expression: mode === 'expression' ? expression : undefined,
          mode: mode,
          sourceId: sourceConfig.sourceId,
          tableName: sourceConfig.tableName,
          schema: sourceConfig.schema,
        }
        onFilterSaved(node.id, tempConfig)
      }
    } catch (err: any) {
      setError(err.response?.data?.error || err.message || 'Failed to preview filter')
    } finally {
      setLoading(false)
    }
  }

  const filteredFunctions = useMemo(() => {
    let filtered = functions

    if (functionSearch) {
      const searchLower = functionSearch.toLowerCase()
      filtered = filtered.filter(f =>
        f.name.toLowerCase().includes(searchLower) ||
        f.description.toLowerCase().includes(searchLower) ||
        f.syntax.toLowerCase().includes(searchLower)
      )
    }

    if (selectedFunctionCategory !== 'all') {
      filtered = filtered.filter(f => f.category === selectedFunctionCategory)
    }

    return filtered
  }, [functionSearch, selectedFunctionCategory])

  if (!node && !directFilterMode) {
    return (
      <Box
        w="400px"
        h="100%"
        bg={bg}
        borderLeftWidth="1px"
        borderColor={borderColor}
        display="flex"
        flexDirection="column"
        overflow="hidden"
      >
        <Box p={4} borderBottomWidth="1px" borderColor={borderColor} bg={headerBg}>
          <Text fontSize="lg" fontWeight="semibold" color={textColor}>
            Filter Properties
          </Text>
          <Text fontSize="xs" color="gray.500" mt={1}>
            Configure filter conditions for selected node
          </Text>
        </Box>
        <Box flex={1} display="flex" alignItems="center" justifyContent="center" p={8}>
          <VStack spacing={4}>
            <Text fontSize="sm" color={useColorModeValue('gray.500', 'gray.400')} textAlign="center">
              No filter node selected
            </Text>
            <Text fontSize="xs" color={useColorModeValue('gray.400', 'gray.500')} textAlign="center">
              Drag a Filter node from the palette onto the canvas, then click it to configure filter conditions
            </Text>
          </VStack>
        </Box>
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
      {/* Header */}
      <Box p={4} borderBottomWidth="1px" borderColor={borderColor} bg={headerBg}>
        <VStack align="stretch" spacing={3}>
          <HStack justify="space-between" align="center">
            {!directFilterMode && node && node.data.type === 'filter' ? (
              <Text fontSize="lg" fontWeight="semibold" color={textColor}>
                Filter Configuration
              </Text>
            ) : (
              <Text fontSize="lg" fontWeight="semibold" color={textColor}>
                {directFilterMode
                  ? `Filter: ${directFilterMode.tableName}`
                  : node?.data.type === 'source' && node.data.config?.isFiltered
                    ? `Filter: ${node.data.config?.tableName || node.data.label}`
                    : 'Filter Properties'}
              </Text>
            )}
            {/* Live updates: no per-node Save button; Clear only for filtered source */}
            {!directFilterMode && node && (
              <HStack spacing={2}>
                {node.data.type === 'source' && node.data.config?.isFiltered && (
                  <Button
                    leftIcon={<X size={14} />}
                    colorScheme="red"
                    size="sm"
                    variant="ghost"
                    onClick={() => {
                      setConditions([])
                      setEditingConditions([])
                      setExpression('')
                      setValidationError(null)
                      setPreviewCount(null)
                      if (node) {
                        const config = {
                          ...node.data.config,
                          conditions: [],
                          expression: '',
                          mode: 'builder',
                          isFiltered: false,
                        }
                        onUpdate(node.id, config)
                        if (onFilterSaved) onFilterSaved(node.id, config)
                      }
                    }}
                  >
                    Clear
                  </Button>
                )}
              </HStack>
            )}
            {/* Direct Filter Mode Buttons - Top Right */}
            {directFilterMode && (
              <HStack spacing={2}>
                <Button
                  leftIcon={<X size={14} />}
                  colorScheme="red"
                  size="sm"
                  variant="ghost"
                  onClick={() => {
                    // Clear all conditions
                    setConditions([])
                    setEditingConditions([])
                    setExpression('')
                    setValidationError(null)
                    setPreviewCount(null)
                    // Clear the filter from storage
                    if (onClearDirectFilter && directFilterMode) {
                      onClearDirectFilter()
                    } else if (onFilterSaved && directFilterMode) {
                      // Fallback: save empty filter
                      onFilterSaved('', {
                        conditions: [],
                        expression: '',
                        mode: 'builder',
                        sourceId: directFilterMode.sourceId,
                        tableName: directFilterMode.tableName,
                        schema: directFilterMode.schema,
                      })
                    }
                  }}
                >
                  Clear
                </Button>
                {onCloseDirectFilter && (
                  <Button
                    leftIcon={<X size={14} />}
                    colorScheme="gray"
                    size="sm"
                    variant="outline"
                    onClick={onCloseDirectFilter}
                  >
                    Close
                  </Button>
                )}
              </HStack>
            )}
          </HStack>
          
          {!directFilterMode && node && (node.data.type === 'filter' || node.data.type === 'source') ? (
            <>
              {/* Business Name (Editable) - for filter and source nodes */}
              <Box>
                <Text fontSize="xs" fontWeight="medium" mb={1} color={useColorModeValue('gray.600', 'gray.400')}>
                  Business Name
                </Text>
                <Input
                  size="sm"
                  value={node.data.business_name || node.data.node_name || node.data.label || (node.data.type === 'source' ? 'Source Table' : 'Filter')}
                  onChange={(e) => {
                    const val = e.target.value
                    onUpdate(node.id, {
                      config: node.data.config || {},
                      business_name: val,
                      node_name: val,
                      label: val,
                    })
                  }}
                  placeholder={node.data.type === 'source' ? 'e.g., Customer Data' : 'e.g., Filter Active Records'}
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
            </>
          ) : (
            !directFilterMode && node && (
              <Badge colorScheme={node?.data.type === 'source' ? 'blue' : 'purple'} alignSelf="flex-start">
                {node?.data.type === 'source' && node.data.config?.isFiltered
                  ? 'Filtered Source Node'
                  : 'Filter Node'}
              </Badge>
            )
          )}

          {/* Check for input node and show warning if missing or metadata unavailable */}
          {!directFilterMode && node && node.data.type === 'filter' && (
            (() => {
              // Check for input node via input_nodes array or edges
              const inputNodeIds = node.data.input_nodes || []
              const hasInputNodeFromArray = inputNodeIds.length > 0
              const hasInputEdge = edges && Array.isArray(edges) ? edges.some((e) => e.target === node.id) : false
              const hasInputConnection = hasInputNodeFromArray || hasInputEdge

              // Show warning only if no input connection OR metadata is missing
              if (!hasInputConnection || !hasUpstreamMetadata) {
                return (
                  <Alert status="warning" size="sm">
                    <AlertIcon />
                    <Text fontSize="xs">
                      {!hasInputConnection
                        ? 'Filter requires an input source. Please connect an upstream node.'
                        : 'Upstream node metadata is not available. Please save the upstream node first.'}
                    </Text>
                  </Alert>
                )
              }
              return null
            })()
          )}


          {/* Display current filter conditions summary - show both saved and editing conditions */}
          {((conditions.length > 0 || editingConditions.length > 0) || (node?.data.config?.conditions && node.data.config.conditions.length > 0) || (mode === 'expression' && expression.trim())) && (
            <Box mt={2} p={2} bg={useColorModeValue('purple.50', 'purple.900')} borderRadius="md">
              <Text fontSize="xs" fontWeight="semibold" color={useColorModeValue('purple.700', 'purple.200')} mb={1}>
                Active Conditions:
              </Text>
              {mode === 'builder' && (conditions.length > 0 || editingConditions.length > 0) && (
                <VStack align="stretch" spacing={1}>
                  {[...conditions, ...editingConditions].slice(0, 3).map((cond: any, idx: number) => {
                    const valueDisplay = Array.isArray(cond.value)
                      ? `[${cond.value.join(', ')}]`
                      : typeof cond.value === 'string' && cond.value.length > 30
                        ? cond.value.substring(0, 30) + '...'
                        : cond.value || '(empty)'
                    return (
                      <Text key={cond.id || idx} fontSize="2xs" color={useColorModeValue('purple.600', 'purple.300')}>
                        {idx > 0 && (cond.logicalOperator || 'AND')} • {cond.column} {cond.operator} {valueDisplay}
                      </Text>
                    )
                  })}
                  {(conditions.length + editingConditions.length) > 3 && (
                    <Text fontSize="2xs" color={useColorModeValue('purple.500', 'purple.400')} fontStyle="italic">
                      + {(conditions.length + editingConditions.length) - 3} more condition(s)
                    </Text>
                  )}
                </VStack>
              )}
              {mode === 'expression' && expression.trim() && (
                <Text fontSize="2xs" color={useColorModeValue('purple.600', 'purple.300')} fontFamily="mono">
                  {expression.length > 100 ? expression.substring(0, 100) + '...' : expression}
                </Text>
              )}
            </Box>
          )}
        </VStack>
      </Box>

      {/* Mode Tabs */}
      <Tabs 
        index={mode === 'builder' ? 0 : 1} 
        onChange={(idx) => setMode(idx === 0 ? 'builder' : 'expression')} 
        colorScheme="purple"
        display="flex"
        flexDirection="column"
        flex={1}
        minH={0}
        overflow="hidden"
      >
        <TabList px={4} pt={2} flexShrink={0}>
          <Tab fontSize="sm">Builder</Tab>
          <Tab fontSize="sm">Expression</Tab>
        </TabList>

        <TabPanels flex={1} minH={0} overflow="hidden" display="flex" flexDirection="column">
          {/* Builder Mode */}
          <TabPanel p={4} overflowY="auto" flex={1} minH={0}>
            {error && (
              <Alert status="error" size="sm" mb={4}>
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
            )}

            {validationError && (
              <Alert status="warning" size="sm" mb={4}>
                <AlertIcon />
                {validationError}
              </Alert>
            )}

            {previewCount !== null && (
              <Alert status="info" size="sm" mb={4}>
                <AlertIcon />
                <Text fontSize="sm">
                  Filtered rows: <strong>{previewCount.toLocaleString()}</strong>
                </Text>
              </Alert>
            )}

            <VStack align="stretch" spacing={4}>
              {conditions.length === 0 && editingConditions.length === 0 ? (
                <Box textAlign="center" py={8}>
                  <Text fontSize="sm" color="gray.500" mb={4}>
                    No filter conditions defined
                  </Text>
                  <Button
                    leftIcon={<Plus />}
                    size="sm"
                    colorScheme="blue"
                    onClick={addCondition}
                  >
                    Add Condition
                  </Button>
                </Box>
              ) : (
                <>
                  {/* Render saved conditions */}
                  {conditions.map((condition, index) => (
                    <Box key={condition.id}>
                      {index > 0 && (
                        <HStack mb={2}>
                          <Select
                            size="sm"
                            value={condition.logicalOperator || 'AND'}
                            onChange={(e) =>
                              updateCondition(condition.id, {
                                logicalOperator: e.target.value as 'AND' | 'OR',
                              }, false) // Saved condition
                            }
                            w="80px"
                          >
                            <option value="AND">AND</option>
                            <option value="OR">OR</option>
                          </Select>
                          <Text fontSize="xs" color="gray.500">
                            (previous condition)
                          </Text>
                        </HStack>
                      )}

                      <Box
                        p={3}
                        borderWidth="1px"
                        borderColor={borderColor}
                        borderRadius="md"
                      >
                        <VStack align="stretch" spacing={2}>
                          <HStack justify="space-between">
                            <Text fontSize="sm" fontWeight="semibold">
                              Condition {index + 1}
                            </Text>
                            <IconButton
                              aria-label="Remove condition"
                              icon={<X size={14} />}
                              size="xs"
                              variant="ghost"
                              colorScheme="red"
                              onClick={() => removeCondition(condition.id)}
                            />
                          </HStack>

                      <FormControl>
                        <FormLabel fontSize="xs">Column</FormLabel>
                        <Select
                          size="sm"
                          value={(() => {
                            const key = (c: any) => c.technical_name ?? c.name
                            const col = availableColumns.find(c => key(c) === condition.column || c.name === condition.column)
                            return col ? key(col) : condition.column
                          })()}
                          onChange={(e) => {
                            const selectedTechnicalName = e.target.value
                            const columnMeta = availableColumns.find(c => (c.technical_name ?? c.name) === selectedTechnicalName || c.name === selectedTechnicalName)
                            const isBoolean = isBooleanColumnType(columnMeta?.datatype)
                            const validOps = getOperatorsForColumn(selectedTechnicalName, availableColumns)
                            const currentValid = validOps.some((op) => op.value === condition.operator)
                            updateCondition(condition.id, {
                              column: selectedTechnicalName,
                              _columnType: columnMeta?.datatype,
                              value: isBoolean ? null : '',
                              ...(isBoolean && !currentValid ? { operator: '=' } : {}),
                            }, false) // Saved condition
                          }}
                          placeholder="Select column"
                        >
                          {availableColumns.map((col) => {
                            const columnName = col.name.includes('(')
                              ? col.name.split('(')[0].trim()
                              : col.name
                            const optionValue = col.technical_name ?? col.name
                            const displayType = col.datatype || 'unknown type'
                            return (
                              <option key={optionValue} value={optionValue}>
                                {columnName} ({displayType})
                              </option>
                            )
                          })}
                        </Select>
                      </FormControl>

                          <FormControl>
                            <FormLabel fontSize="xs">Operator</FormLabel>
                            <Select
                              size="sm"
                              value={(() => {
                                const ops = getOperatorsForColumn(condition.column, availableColumns)
                                return ops.some((op) => op.value === condition.operator) ? condition.operator : (ops[0]?.value ?? '=')
                              })()}
                              onChange={(e) =>
                                updateCondition(condition.id, {
                                  operator: e.target.value,
                                  value: noValueOperators.includes(e.target.value) ? null : '',
                                }, false) // Saved condition
                              }
                            >
                              {getOperatorsForColumn(condition.column, availableColumns).map((op) => (
                                <option key={op.value} value={op.value}>
                                  {op.label}
                                </option>
                              ))}
                            </Select>
                          </FormControl>

                          {valueOperators.includes(condition.operator) && (
                            <FormControl>
                              <FormLabel fontSize="xs">
                                Value
                                {condition.operator === 'BETWEEN' && (
                                  <Text as="span" fontSize="xs" color="gray.500" ml={1}>
                                    (min, max or JSON array)
                                  </Text>
                                )}
                                {(condition.operator === 'IN' || condition.operator === 'NOT IN') && (
                                  <Text as="span" fontSize="xs" color="gray.500" ml={1}>
                                    (comma-separated)
                                  </Text>
                                )}
                              </FormLabel>
                              {condition.operator === 'BETWEEN' ? (
                                <Input
                                  size="sm"
                                  value={Array.isArray(condition.value)
                                    ? JSON.stringify(condition.value)
                                    : typeof condition.value === 'string' && condition.value.includes(',')
                                      ? condition.value
                                      : condition.value || ''}
                                  onChange={(e) => {
                                    const value = e.target.value
                                    // Try to parse as JSON array first
                                    try {
                                      const parsed = JSON.parse(value)
                                      if (Array.isArray(parsed) && parsed.length === 2) {
                                        updateCondition(condition.id, { value: parsed })
                                      } else {
                                        updateCondition(condition.id, { value })
                                      }
                                    } catch {
                                      // If not JSON, treat as comma-separated
                                      const parts = value.split(',').map((v: string) => v.trim())
                                      if (parts.length === 2) {
                                        updateCondition(condition.id, { value: parts })
                                      } else {
                                        updateCondition(condition.id, { value })
                                      }
                                    }
                                  }}
                                  placeholder="min, max or [min, max]"
                                />
                              ) : condition.operator === 'IN' || condition.operator === 'NOT IN' ? (
                                <Textarea
                                  size="sm"
                                  value={Array.isArray(condition.value) ? condition.value.join(', ') : condition.value || ''}
                                  onChange={(e) => {
                                    const value = e.target.value
                                    const values = value.split(',').map((v) => v.trim()).filter((v) => v)
                                    updateCondition(condition.id, { value: values.length > 1 ? values : value })
                                  }}
                                  placeholder="Enter values separated by commas"
                                  rows={2}
                                />
                              ) : isBooleanColumnType(condition._columnType) ? (
                                <Select
                                  size="sm"
                                  value={
                                    condition.value !== null && condition.value !== undefined 
                                      ? String(condition.value).toLowerCase()
                                      : ''
                                  }
                                  onChange={(e) => {
                                    const rawValue = e.target.value
                                    // Convert string to boolean
                                    const boolValue = rawValue === 'true' ? true : rawValue === 'false' ? false : rawValue
                                    updateCondition(condition.id, { value: boolValue }, false) // Saved condition
                                  }}
                                  placeholder="Select value"
                                >
                                  <option value="">-- Select --</option>
                                  <option value="true">True</option>
                                  <option value="false">False</option>
                                </Select>
                              ) : (
                                <Input
                                  size="sm"
                                  type={condition._columnType ? getInputTypeForColumn(condition._columnType) : 'text'}
                                  value={
                                    condition.value !== null && condition.value !== undefined 
                                      ? (Array.isArray(condition.value) 
                                          ? condition.value.join(', ') 
                                          : typeof condition.value === 'object' 
                                            ? JSON.stringify(condition.value)
                                            : String(condition.value))
                                      : ''
                                  }
                                  onChange={(e) => {
                                    const rawValue = e.target.value
                                    updateCondition(condition.id, { value: rawValue }, false) // Saved condition
                                  }}
                                  placeholder={
                                    condition._columnType?.includes('INT') || condition._columnType?.includes('NUMERIC')
                                      ? 'Enter number'
                                      : condition._columnType?.includes('DATE')
                                        ? 'YYYY-MM-DD'
                                        : 'Enter value'
                                  }
                                  step={condition._columnType?.includes('NUMERIC') || condition._columnType?.includes('FLOAT') ? '0.01' : undefined}
                                />
                              )}
                            </FormControl>
                          )}
                        </VStack>
                      </Box>
                    </Box>
                  ))}

                  {/* Render editing conditions */}
                  {editingConditions.map((condition) => (
                    <Box key={condition.id}>
                      {conditions.length > 0 && (
                        <HStack mb={2}>
                          <Select
                            size="sm"
                            value={condition.logicalOperator || 'AND'}
                            onChange={(e) =>
                              updateCondition(condition.id, {
                                logicalOperator: e.target.value as 'AND' | 'OR',
                              }, true) // Editing condition
                            }
                            w="80px"
                          >
                            <option value="AND">AND</option>
                            <option value="OR">OR</option>
                          </Select>
                          <Text fontSize="xs" color="gray.500">
                            (previous condition)
                          </Text>
                        </HStack>
                      )}

                      <Box
                        p={3}
                        borderWidth="2px"
                        borderColor={useColorModeValue('blue.300', 'blue.600')}
                        borderRadius="md"
                        bg={useColorModeValue('blue.50', 'blue.900')}
                      >
                        <VStack align="stretch" spacing={2}>
                          <HStack justify="space-between">
                            <Badge colorScheme="blue" fontSize="xs">
                              Editing
                            </Badge>
                            <HStack spacing={1}>
                              <Tooltip label="Save condition">
                                <IconButton
                                  aria-label="Save condition"
                                  icon={<Save size={12} />}
                                  size="xs"
                                  colorScheme="green"
                                  onClick={() => saveCondition(condition)}
                                  isDisabled={
                                    !condition.column ||
                                    !condition.operator ||
                                    (valueOperators.includes(condition.operator) && !condition.value && condition.value !== null)
                                  }
                                />
                              </Tooltip>
                              <Tooltip label="Cancel">
                                <IconButton
                                  aria-label="Cancel editing"
                                  icon={<X size={12} />}
                                  size="xs"
                                  variant="ghost"
                                  colorScheme="red"
                                  onClick={() => cancelCondition(condition.id)}
                                />
                              </Tooltip>
                            </HStack>
                          </HStack>

                          <FormControl>
                            <FormLabel fontSize="xs">Column</FormLabel>
                            <Select
                              size="sm"
                              value={(() => {
                                const key = (c: any) => c.technical_name ?? c.name
                                const col = availableColumns.find(c => key(c) === condition.column || c.name === condition.column)
                                return col ? key(col) : condition.column
                              })()}
                              onChange={(e) => {
                                const selectedTechnicalName = e.target.value
                                const columnMeta = availableColumns.find(c => (c.technical_name ?? c.name) === selectedTechnicalName || c.name === selectedTechnicalName)
                                const isBoolean = isBooleanColumnType(columnMeta?.datatype)
                                const validOps = getOperatorsForColumn(selectedTechnicalName, availableColumns)
                                const currentValid = validOps.some((op) => op.value === condition.operator)
                                updateCondition(condition.id, {
                                  column: selectedTechnicalName,
                                  _columnType: columnMeta?.datatype,
                                  value: isBoolean ? null : '',
                                  ...(isBoolean && !currentValid ? { operator: '=' } : {}),
                                }, true) // Editing condition
                              }}
                              placeholder="Select column"
                            >
                              {availableColumns.map((col) => {
                                const columnName = col.name.includes('(')
                                  ? col.name.split('(')[0].trim()
                                  : col.name
                                const optionValue = col.technical_name ?? col.name
                                const displayType = col.datatype || 'unknown type'
                                return (
                                  <option key={optionValue} value={optionValue}>
                                    {columnName} ({displayType})
                                  </option>
                                )
                              })}
                            </Select>
                          </FormControl>

                          <FormControl>
                            <FormLabel fontSize="xs">Operator</FormLabel>
                            <Select
                              size="sm"
                              value={(() => {
                                const ops = getOperatorsForColumn(condition.column, availableColumns)
                                return ops.some((op) => op.value === condition.operator) ? condition.operator : (ops[0]?.value ?? '=')
                              })()}
                              onChange={(e) =>
                                updateCondition(condition.id, {
                                  operator: e.target.value,
                                  value: noValueOperators.includes(e.target.value) ? null : '',
                                }, true) // Editing condition
                              }
                            >
                              {getOperatorsForColumn(condition.column, availableColumns).map((op) => (
                                <option key={op.value} value={op.value}>
                                  {op.label}
                                </option>
                              ))}
                            </Select>
                          </FormControl>

                          {valueOperators.includes(condition.operator) && (
                            <FormControl>
                              <FormLabel fontSize="xs">
                                Value
                                {condition.operator === 'BETWEEN' && (
                                  <Text as="span" fontSize="xs" color="gray.500" ml={1}>
                                    (min, max or JSON array)
                                  </Text>
                                )}
                                {(condition.operator === 'IN' || condition.operator === 'NOT IN') && (
                                  <Text as="span" fontSize="xs" color="gray.500" ml={1}>
                                    (comma-separated)
                                  </Text>
                                )}
                              </FormLabel>
                              {condition.operator === 'BETWEEN' ? (
                                <Input
                                  size="sm"
                                  value={Array.isArray(condition.value)
                                    ? JSON.stringify(condition.value)
                                    : typeof condition.value === 'string' && condition.value.includes(',')
                                      ? condition.value
                                      : condition.value || ''}
                                  onChange={(e) => {
                                    const value = e.target.value
                                    try {
                                      const parsed = JSON.parse(value)
                                      if (Array.isArray(parsed) && parsed.length === 2) {
                                        updateCondition(condition.id, { value: parsed }, true)
                                      } else {
                                        updateCondition(condition.id, { value }, true)
                                      }
                                    } catch {
                                      const parts = value.split(',').map((v: string) => v.trim())
                                      if (parts.length === 2) {
                                        updateCondition(condition.id, { value: parts }, true)
                                      } else {
                                        updateCondition(condition.id, { value }, true)
                                      }
                                    }
                                  }}
                                  placeholder="min, max or [min, max]"
                                />
                              ) : condition.operator === 'IN' || condition.operator === 'NOT IN' ? (
                                <Textarea
                                  size="sm"
                                  value={Array.isArray(condition.value) ? condition.value.join(', ') : condition.value || ''}
                                  onChange={(e) => {
                                    const value = e.target.value
                                    const values = value.split(',').map((v) => v.trim()).filter((v) => v)
                                    updateCondition(condition.id, { value: values.length > 1 ? values : value }, true)
                                  }}
                                  placeholder="Enter values separated by commas"
                                  rows={2}
                                />
                              ) : isBooleanColumnType(condition._columnType) ? (
                                <Select
                                  size="sm"
                                  value={
                                    condition.value !== null && condition.value !== undefined 
                                      ? String(condition.value).toLowerCase()
                                      : ''
                                  }
                                  onChange={(e) => {
                                    const rawValue = e.target.value
                                    // Convert string to boolean
                                    const boolValue = rawValue === 'true' ? true : rawValue === 'false' ? false : rawValue
                                    updateCondition(condition.id, { value: boolValue }, true) // Editing condition
                                  }}
                                  placeholder="Select value"
                                >
                                  <option value="">-- Select --</option>
                                  <option value="true">True</option>
                                  <option value="false">False</option>
                                </Select>
                              ) : (
                                <Input
                                  size="sm"
                                  type={condition._columnType ? getInputTypeForColumn(condition._columnType) : 'text'}
                                  value={
                                    condition.value !== null && condition.value !== undefined 
                                      ? (Array.isArray(condition.value) 
                                          ? condition.value.join(', ') 
                                          : typeof condition.value === 'object' 
                                            ? JSON.stringify(condition.value)
                                            : String(condition.value))
                                      : ''
                                  }
                                  onChange={(e) => {
                                    const rawValue = e.target.value
                                    updateCondition(condition.id, { value: rawValue }, true) // Editing condition
                                  }}
                                  placeholder={
                                    condition._columnType?.includes('INT') || condition._columnType?.includes('NUMERIC')
                                      ? 'Enter number'
                                      : condition._columnType?.includes('DATE')
                                        ? 'YYYY-MM-DD'
                                        : 'Enter value'
                                  }
                                  step={condition._columnType?.includes('NUMERIC') || condition._columnType?.includes('FLOAT') ? '0.01' : undefined}
                                />
                              )}
                            </FormControl>
                          )}
                        </VStack>
                      </Box>
                    </Box>
                  ))}

                  <Button
                    leftIcon={<Plus />}
                    size="sm"
                    variant="outline"
                    onClick={addCondition}
                  >
                    Add Condition
                  </Button>
                </>
              )}
            </VStack>
          </TabPanel>

          {/* Expression Mode */}
          <TabPanel p={4} display="flex" flexDirection="column" flex={1} minH={0} overflowY="auto">
            <VStack align="stretch" spacing={4} flex={1} minH={0}>
              {validationError && (
                <Alert status="warning" size="sm">
                  <AlertIcon />
                  {validationError}
                </Alert>
              )}
              {validationSuccess && (
                <Alert status="success" size="sm">
                  <AlertIcon />
                  {validationSuccess}
                </Alert>
              )}

              <FormControl>
                <FormLabel fontSize="sm" fontWeight="semibold">Expression Editor</FormLabel>
                <Textarea
                  value={expression}
                  onChange={(e) => {
                    setExpression(e.target.value)
                    setValidationError(null)
                    setValidationSuccess(null)
                  }}
                  placeholder="e.g., name = 'John' AND age > 25 OR status IN ('active', 'pending')"
                  rows={6}
                  fontFamily="mono"
                  fontSize="sm"
                  // Allow editing even if `input_nodes` is not populated on the filter node.
                  // Upstream metadata availability is handled separately via warnings/validation.
                  isDisabled={false}
                />
                <Text fontSize="xs" color="gray.500" mt={1}>
                  Use column names, operators (=, !=, {'>'}, {'<'}, {'>='}, {'<='}, LIKE, IN, BETWEEN), and logical operators (AND, OR, NOT)
                </Text>
              </FormControl>

              {/* Validate Expression Button */}
              <HStack spacing={2}>
                <Button
                  size="sm"
                  colorScheme="blue"
                  variant="outline"
                  isLoading={loading}
                  onClick={async () => {
                    if (!expression.trim()) {
                      setValidationError('Expression cannot be empty')
                      setValidationSuccess(null)
                      return
                    }

                    // Fast client-side checks first
                    const exprError = validateExpression(expression)
                    if (exprError) {
                      setValidationError(exprError)
                      setValidationSuccess(null)
                      return
                    }

                    // Authoritative backend validation (Python + SQL parser via EXPLAIN)
                    setLoading(true)
                    try {
                      const payload = {
                        expression: expression,
                        available_columns: availableColumns.map((col) => ({
                          name: col.business_name || col.name,
                          datatype: col.datatype || 'TEXT',
                          technical_name: col.technical_name ?? col.name,
                        })),
                        sql_validation: true,
                      }
                      const result: any = await validationApi.validateExpression(payload)
                      if (result?.success) {
                        setValidationError(null)
                        setError(null)
                        setValidationSuccess('Expression is valid.')
                      } else {
                        const errs = Array.isArray(result?.errors) ? result.errors : ['Expression validation failed']
                        setValidationError(errs.join(' | '))
                        setValidationSuccess(null)
                      }
                    } catch (err: any) {
                      const msg = err?.response?.data?.errors?.join?.(' | ')
                        || err?.response?.data?.error
                        || err?.message
                        || 'Backend expression validation failed'
                      setValidationError(msg)
                      setValidationSuccess(null)
                    } finally {
                      setLoading(false)
                    }
                  }}
                  isDisabled={!expression.trim()}
                >
                  Validate Expression
                </Button>
              </HStack>

              {/* Column Picker */}
              <Box>
                <FormLabel fontSize="sm" fontWeight="semibold">Available Columns</FormLabel>
                <InputGroup size="sm" mb={2}>
                  <InputLeftElement pointerEvents="none">
                    <Search size={14} />
                  </InputLeftElement>
                  <Input
                    placeholder="Search columns..."
                    onChange={(_e) => {
                      // Filter columns as user types
                    }}
                  />
                </InputGroup>
                <Box
                  maxH="150px"
                  overflowY="auto"
                  borderWidth="1px"
                  borderColor={borderColor}
                  borderRadius="md"
                  p={2}
                >
                  <VStack align="stretch" spacing={1}>
                    {availableColumns.map((col) => (
                      <Button
                        key={col.name}
                        size="xs"
                        variant="ghost"
                        justifyContent="flex-start"
                        onClick={() => insertColumn(col.name)}
                        leftIcon={<CodeIcon size={12} />}
                      >
                        <Text fontSize="xs">{col.name}</Text>
                        <Badge ml={2} size="xs" colorScheme="gray">
                          {col.datatype}
                        </Badge>
                      </Button>
                    ))}
                  </VStack>
                </Box>
              </Box>

              {/* Function Picker */}
              <Box>
                <FormLabel fontSize="sm" fontWeight="semibold">Functions</FormLabel>
                <HStack mb={2} spacing={2}>
                  <Select
                    size="sm"
                    value={selectedFunctionCategory}
                    onChange={(e) => setSelectedFunctionCategory(e.target.value)}
                    w="150px"
                  >
                    <option value="all">All Categories</option>
                    <option value="string">String</option>
                    <option value="numeric">Numeric</option>
                    <option value="date">Date</option>
                    <option value="logical">Logical</option>
                  </Select>
                  <InputGroup size="sm" flex={1}>
                    <InputLeftElement pointerEvents="none">
                      <Search size={14} />
                    </InputLeftElement>
                    <Input
                      placeholder="Search functions..."
                      value={functionSearch}
                      onChange={(e) => setFunctionSearch(e.target.value)}
                    />
                  </InputGroup>
                </HStack>
                <Box
                  maxH="200px"
                  overflowY="auto"
                  borderWidth="1px"
                  borderColor={borderColor}
                  borderRadius="md"
                  p={2}
                >
                  <VStack align="stretch" spacing={1}>
                    {filteredFunctions.map((func) => (
                      <Tooltip key={func.name} label={func.description} placement="left">
                        <Button
                          size="xs"
                          variant="outline"
                          justifyContent="flex-start"
                          onClick={() => insertFunction(func)}
                          leftIcon={<CodeIcon size={12} />}
                        >
                          <VStack align="start" spacing={0} flex={1}>
                            <Text fontSize="xs" fontWeight="semibold">
                              {func.name}
                            </Text>
                            <Text fontSize="2xs" color="gray.500">
                              {func.syntax}
                            </Text>
                          </VStack>
                          <Badge ml={2} size="xs" colorScheme={
                            func.category === 'string' ? 'green' :
                              func.category === 'numeric' ? 'blue' :
                                func.category === 'date' ? 'purple' : 'orange'
                          }>
                            {func.category}
                          </Badge>
                        </Button>
                      </Tooltip>
                    ))}
                  </VStack>
                </Box>
              </Box>

              {/* Quick Operators */}
              <Box>
                <FormLabel fontSize="sm" fontWeight="semibold">Quick Operators</FormLabel>
                <HStack spacing={1} flexWrap="wrap">
                  {['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'IN', 'BETWEEN', 'AND', 'OR', 'NOT'].map((op) => (
                    <Button
                      key={op}
                      size="xs"
                      variant="outline"
                      onClick={() => insertOperator(op)}
                    >
                      {op}
                    </Button>
                  ))}
                </HStack>
              </Box>
            </VStack>
          </TabPanel>
        </TabPanels>
      </Tabs>

      {/* Footer */}
      <Box p={4} borderTopWidth="1px" borderColor={borderColor} bg={headerBg}>
        <VStack align="stretch" spacing={2}>
          <Button
            leftIcon={<Search />}
            size="sm"
            colorScheme="blue"
            variant="outline"
            onClick={handlePreview}
            isLoading={loading}
              isDisabled={
                loading ||
                (mode === 'expression' && !expression.trim()) ||
                (mode === 'builder' && conditions.length === 0 && editingConditions.length === 0) ||
              (!directFilterMode && node && node.data.type === 'filter'
                  ? (!upstreamNodeId || !hasUpstreamMetadata || (mode === 'builder' && conditions.length === 0 && editingConditions.length === 0) || (mode === 'expression' && !expression.trim()))
                : false)
            }
            w="100%"
          >
            Preview Data
          </Button>
          {!directFilterMode && node && node.data.type === 'filter' && (
            <>
              {(!upstreamNodeId || !hasUpstreamMetadata) && (
                <Text fontSize="xs" color="orange.500">
                  {!upstreamNodeId
                    ? 'Connect an input node to enable validation and preview'
                    : 'Upstream node metadata is not available. Please save the upstream node first.'}
                </Text>
              )}
              {upstreamNodeId && hasUpstreamMetadata && (
                <Text fontSize="xs" color="green.500">
                  ✓ Connected to upstream node. Ready to configure filter.
                </Text>
              )}
            </>
          )}
        </VStack>
      </Box>
    </Box>
  )
}


