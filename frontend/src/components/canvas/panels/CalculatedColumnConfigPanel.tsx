/**
 * Calculated Column Configuration Panel Component
 * Allows users to create calculated columns with expressions.
 * All changes are saved to Zustand live (no Save button).
 */
import React, { useState, useEffect, useRef } from 'react'
import {
  Box,
  VStack,
  HStack,
  Text,
  Button,
  Input,
  IconButton,
  Alert,
  AlertIcon,
  useColorModeValue,
  FormControl,
  FormLabel,
  Textarea,
  Select,
} from '@chakra-ui/react'
import { Plus, X, CheckCircle, Code } from 'lucide-react'
import { Node, Edge } from 'reactflow'

interface CalculatedColumn {
  id: string
  name: string
  expression: string
  dataType: string
}

interface CalculatedColumnConfigPanelProps {
  node: Node | null
  nodes: Node[]
  edges: Edge[]
  onUpdate: (nodeId: string, config: any) => void
}

const dataTypes = [
  'STRING', 'INTEGER', 'DECIMAL', 'DATE', 'DATETIME', 'BOOLEAN'
]

const functions = [
  { name: 'CONCAT', description: 'Concatenate strings' },
  { name: 'SUBSTRING', description: 'Extract substring' },
  { name: 'UPPER', description: 'Convert to uppercase' },
  { name: 'LOWER', description: 'Convert to lowercase' },
  { name: 'SUM', description: 'Sum values' },
  { name: 'AVG', description: 'Average values' },
  { name: 'COUNT', description: 'Count rows' },
  { name: 'MAX', description: 'Maximum value' },
  { name: 'MIN', description: 'Minimum value' },
  { name: 'IF', description: 'Conditional expression' },
  { name: 'CASE', description: 'Case statement' },
  { name: 'DATEADD', description: 'Add to date' },
  { name: 'DATEDIFF', description: 'Date difference' },
]

export const CalculatedColumnConfigPanel: React.FC<CalculatedColumnConfigPanelProps> = ({
  node,
  nodes,
  edges,
  onUpdate,
}) => {
  const [calculatedColumns, setCalculatedColumns] = useState<CalculatedColumn[]>([])
  const [availableColumns, setAvailableColumns] = useState<string[]>([])
  const [functionSearch, setFunctionSearch] = useState('')
  const [error, setError] = useState<string | null>(null)

  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const headerBg = useColorModeValue('gray.50', 'gray.700')

  useEffect(() => {
    if (node) {
      const config = node.data.config || {}
      setCalculatedColumns(config.calculatedColumns || [])

      // Load columns from input node
      const inputEdge = edges.find((e) => e.target === node.id)
      if (inputEdge) {
        const inputNode = nodes.find((n) => n.id === inputEdge.source)
        if (inputNode) {
          loadColumns(inputNode)
        }
      }
    }
  }, [node, nodes, edges])

  const loadColumns = async (inputNode: Node) => {
    // ✅ SPECIAL HANDLING FOR JOIN NODES: Use outputColumns with resolved outputName
    if (inputNode.data.type === 'join') {
      // First, try outputColumns (has resolved outputName with _l/_r suffixes)
      if (inputNode.data.config?.outputColumns && Array.isArray(inputNode.data.config.outputColumns)) {
        const outputColumns = inputNode.data.config.outputColumns
        const includedColumns = outputColumns.filter((col: any) => col.included !== false)
        
        if (includedColumns.length > 0) {
          const columnNames = includedColumns.map((col: any) => col.outputName || col.column)
          setAvailableColumns(columnNames)
          return
        }
      }
      
      // Fallback: If outputColumns not available, try to build from left and right tables
      if (inputNode.data.config?.leftNodeId && inputNode.data.config?.rightNodeId && nodes) {
        console.log('[CalculatedColumnConfig] Join outputColumns not available, building from left/right nodes')
        const leftNodeId = inputNode.data.config.leftNodeId
        const rightNodeId = inputNode.data.config.rightNodeId
        
        const leftNode = nodes.find((n: any) => n.id === leftNodeId)
        const rightNode = nodes.find((n: any) => n.id === rightNodeId)
        
        if (leftNode && rightNode) {
          // Load columns from both nodes recursively
          const loadColumnsFromNode = async (node: Node): Promise<string[]> => {
            if (node.data.type === 'source' && node.data.config?.sourceId && node.data.config?.tableName) {
              try {
                const { sourceTableApi } = await import('../../../services/api')
                const response = await sourceTableApi.getColumns(
                  node.data.config.sourceId,
                  node.data.config.tableName,
                  node.data.config.schema
                )
                return (response.data.columns || []).map((col: any) => col.name || col.column_name)
              } catch (err) {
                return []
              }
            } else if (node.data.output_metadata?.columns) {
              return node.data.output_metadata.columns.map((col: any) => 
                typeof col === 'string' ? col : (col.name || col.column_name || col)
              )
            } else if (node.data.config?.columns) {
              return node.data.config.columns.map((col: any) => 
                typeof col === 'string' ? col : (col.name || col.column_name || col)
              )
            }
            return []
          }
          
          try {
            const leftColNames = await loadColumnsFromNode(leftNode)
            const rightColNames = await loadColumnsFromNode(rightNode)
            
            const leftColSet = new Set(leftColNames)
            const rightColSet = new Set(rightColNames)
            
            const combinedColumnNames: string[] = []
            
            // Add left columns
            leftColNames.forEach((colName) => {
              if (rightColSet.has(colName)) {
                combinedColumnNames.push(`${colName}_l`)
              } else {
                combinedColumnNames.push(colName)
              }
            })
            
            // Add right columns
            rightColNames.forEach((colName) => {
              if (leftColSet.has(colName)) {
                combinedColumnNames.push(`${colName}_r`)
              } else {
                combinedColumnNames.push(colName)
              }
            })
            
            if (combinedColumnNames.length > 0) {
              console.log('[CalculatedColumnConfig] Built columns from left/right nodes with conflict resolution:', combinedColumnNames)
              setAvailableColumns(combinedColumnNames)
              return
            }
          } catch (err: any) {
            console.warn('[CalculatedColumnConfig] Error building columns from left/right nodes:', err)
            // Continue to other fallbacks
          }
        }
      }
    }
    
    // Handle output_metadata
    if (inputNode.data.output_metadata?.columns) {
      const columns = inputNode.data.output_metadata.columns
      const columnNames = columns.map((col: any) => 
        typeof col === 'string' ? col : (col.name || col.column_name || col)
      )
      setAvailableColumns(columnNames)
      return
    }
    
    // Handle config.columns
    if (inputNode.data.config?.columns && Array.isArray(inputNode.data.config.columns)) {
      const columnNames = inputNode.data.config.columns.map((col: any) => 
        typeof col === 'string' ? col : (col.name || col.column_name || col)
      )
      setAvailableColumns(columnNames)
      return
    }
    
    // Handle source node
    if (inputNode.data.type === 'source' && inputNode.data.config) {
      const config = inputNode.data.config
      if (config.sourceId && config.tableName) {
        try {
          const { sourceTableApi } = await import('../../../services/api')
          const response = await sourceTableApi.getColumns(
            config.sourceId,
            config.tableName,
            config.schema
          )
          const columns = response.data.columns || []
          const columnNames = columns.map((col: any) => col.name || col.column_name)
          setAvailableColumns(columnNames)
        } catch (err: any) {
          console.error('Error loading columns:', err)
        }
      }
    }
  }

  const addCalculatedColumn = () => {
    const newColumn: CalculatedColumn = {
      id: `calc-${Date.now()}`,
      name: '',
      expression: '',
      dataType: 'STRING',
    }
    setCalculatedColumns([...calculatedColumns, newColumn])
  }

  const removeCalculatedColumn = (id: string) => {
    setCalculatedColumns(calculatedColumns.filter((c) => c.id !== id))
  }

  const updateCalculatedColumn = (id: string, updates: Partial<CalculatedColumn>) => {
    setCalculatedColumns(
      calculatedColumns.map((c) => (c.id === id ? { ...c, ...updates } : c))
    )
  }

  const insertColumnIntoExpression = (columnName: string, columnId: string) => {
    const column = calculatedColumns.find((c) => c.id === columnId)
    if (column) {
      const newExpression = column.expression
        ? `${column.expression} + ${columnName}`
        : columnName
      updateCalculatedColumn(columnId, { expression: newExpression })
    }
  }

  const insertFunctionIntoExpression = (funcName: string, columnId: string) => {
    const column = calculatedColumns.find((c) => c.id === columnId)
    if (column) {
      const newExpression = column.expression
        ? `${column.expression} + ${funcName}()`
        : `${funcName}()`
      updateCalculatedColumn(columnId, { expression: newExpression })
    }
  }

  // Live updates: push config to Zustand when calculated columns change (no Save button).
  const lastPushedHashRef = useRef<string>('')
  useEffect(() => {
    if (!node) return
    const validColumns = calculatedColumns.filter((c) => c.name.trim() && c.expression.trim())
    const config = { ...node.data.config, calculatedColumns: validColumns }
    const hash = JSON.stringify(validColumns)
    if (lastPushedHashRef.current === hash) return
    lastPushedHashRef.current = hash
    onUpdate(node.id, {
      config,
      business_name: node.data.business_name || node.data.node_name || node.data.label || 'Calculated Column',
      technical_name: node.data.technical_name,
      node_name: node.data.business_name || node.data.node_name || node.data.label || 'Calculated Column',
      label: node.data.business_name || node.data.node_name || node.data.label || 'Calculated Column',
    })
  }, [node?.id, calculatedColumns, onUpdate])

  const filteredFunctions = functions.filter((f) =>
    f.name.toLowerCase().includes(functionSearch.toLowerCase()) ||
    f.description.toLowerCase().includes(functionSearch.toLowerCase())
  )

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
          Select a calculated column node to configure
        </Text>
      </Box>
    )
  }

  return (
    <Box
      w="320px"
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
          <Text fontSize="lg" fontWeight="semibold">
            Calculated Columns
          </Text>
          
          {/* Business Name (Editable) */}
          <Box>
            <Text fontSize="xs" fontWeight="medium" mb={1} color={useColorModeValue('gray.600', 'gray.400')}>
              Business Name
            </Text>
            <Input
              size="sm"
              value={node.data.business_name || node.data.node_name || node.data.label || 'Calculated Column'}
              onChange={(e) => {
                onUpdate(node.id, {
                  ...node.data.config,
                  business_name: e.target.value,
                  node_name: e.target.value, // Legacy support
                  label: e.target.value, // Update label for display
                })
              }}
              placeholder="e.g., Calculated Column 1"
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
      <Box flex={1} overflowY="auto" p={4}>
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
                borderColor={borderColor}
                borderRadius="md"
              >
                <HStack justify="space-between" mb={2}>
                  <Text fontSize="sm" fontWeight="semibold">
                    Column {index + 1}
                  </Text>
                  <IconButton
                    aria-label="Remove column"
                    icon={<X size={12} />}
                    size="xs"
                    variant="ghost"
                    colorScheme="red"
                    onClick={() => removeCalculatedColumn(column.id)}
                  />
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

                  <FormControl>
                    <FormLabel fontSize="xs">Expression</FormLabel>
                    <Textarea
                      size="sm"
                      value={column.expression}
                      onChange={(e) =>
                        updateCalculatedColumn(column.id, { expression: e.target.value })
                      }
                      placeholder="e.g., column1 + column2"
                      rows={3}
                    />
                  </FormControl>

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
                  {availableColumns.length > 0 && (
                    <Box>
                      <Text fontSize="xs" color="gray.500" mb={1}>
                        Insert Column:
                      </Text>
                      <HStack spacing={1} flexWrap="wrap" maxH="120px" overflowY="auto">
                        {availableColumns.map((col) => (
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
                        <Button
                          key={func.name}
                          size="xs"
                          variant="outline"
                          leftIcon={<Code size={10} />}
                          onClick={() => insertFunctionIntoExpression(func.name, column.id)}
                          title={func.description}
                        >
                          {func.name}
                        </Button>
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
      </Box>

    </Box>
  )
}

