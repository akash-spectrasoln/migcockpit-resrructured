/**
 * Aggregates Configuration Panel Component
 * Dedicated section for configuring group-level aggregate functions
 * Accessed via right-click context menu, not mixed with projection or calculated columns
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
  Badge,
  useColorModeValue,
  FormControl,
  FormLabel,
  Select,
} from '@chakra-ui/react'
import { Plus, X } from 'lucide-react'
import { Node, Edge } from 'reactflow'
import { ColumnMetadata } from '../../../types/filterTypes'

interface AggregateColumn {
  id: string
  function: 'SUM' | 'AVG' | 'MIN' | 'MAX' | 'COUNT'
  column: string
  alias: string
  groupBy: string[]  // Each aggregate can have its own group-by columns
}

interface AggregatesConfigPanelProps {
  node: Node | null
  nodes: Node[]
  edges: Edge[]
  onUpdate: (nodeId: string, config: any) => void
}

const aggregateFunctions = [
  { name: 'SUM', description: 'Sum numeric values' },
  { name: 'AVG', description: 'Calculate average of numeric values' },
  { name: 'MIN', description: 'Get minimum value' },
  { name: 'MAX', description: 'Get maximum value' },
  { name: 'COUNT', description: 'Count rows' },
]

/** Auto group-by: all columns except the selected measure column */
function getAutoGroupByColumns(columns: ColumnMetadata[]): string[] {
  return columns.map(c => c.name)
}

export const AggregatesConfigPanel: React.FC<AggregatesConfigPanelProps> = ({
  node,
  nodes,
  edges,
  onUpdate,
}) => {
  const [aggregateColumns, setAggregateColumns] = useState<AggregateColumn[]>([])
  const [availableColumns, setAvailableColumns] = useState<ColumnMetadata[]>([])
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [businessName, setBusinessName] = useState<string>('')
  const [technicalName, setTechnicalName] = useState<string>('')

  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const headerBg = useColorModeValue('gray.50', 'gray.700')
  const groupByTextColor = useColorModeValue('gray.600', 'gray.400')
  const groupByBg = useColorModeValue('gray.50', 'gray.700')

  const previousNodeId = React.useRef<string | null>(null)

  useEffect(() => {
    if (!node) return

    // Only initialize state if the node ID has changed
    if (previousNodeId.current !== node.id) {
      const config = node.data.config || {}
      const cols = config.aggregateColumns || []
      setAggregateColumns(cols)
      setBusinessName(node.data.business_name || node.data.label || '')
      setTechnicalName(node.data.technical_name || node.id || '')
      previousNodeId.current = node.id

      // Load columns only when switching nodes or if not loaded yet
      const inputNodeIds = node.data.input_nodes || []
      if (inputNodeIds.length > 0 && nodes) {
        const inputNode = nodes.find((n) => n.id === inputNodeIds[0])
        if (inputNode) {
          loadColumns(inputNode)
        }
      } else if (edges && nodes) {
        const inputEdge = edges.find((e) => e.target === node.id)
        if (inputEdge) {
          const inputNode = nodes.find((n) => n.id === inputEdge.source)
          if (inputNode) {
            loadColumns(inputNode)
          }
        }
      }
    }
  }, [node?.id, nodes, edges])

  // When availableColumns loads, auto-fill empty groupBy with non-numeric columns
  useEffect(() => {
    if (availableColumns.length === 0 || aggregateColumns.length === 0) return
    const autoGb = getAutoGroupByColumns(availableColumns)
    const needsUpdate = aggregateColumns.some(a => a.groupBy.length === 0 && autoGb.length > 0)
    if (needsUpdate) {
      setAggregateColumns(prev =>
        prev.map(a => (a.groupBy.length === 0 ? { ...a, groupBy: [...autoGb] } : a))
      )
    }
  }, [availableColumns, aggregateColumns])

  const loadColumns = async (inputNode: Node): Promise<void> => {
    setLoading(true)
    setError(null)

    try {
      // ✅ SPECIAL HANDLING FOR JOIN NODES: Use outputColumns with resolved outputName
      if (inputNode.data.type === 'join' && inputNode.data.config?.outputColumns) {
        const outputColumns = inputNode.data.config.outputColumns
        const includedColumns = outputColumns.filter((col: any) => col.included !== false)
        
        if (includedColumns.length > 0) {
          const columnMetadata: ColumnMetadata[] = includedColumns.map((col: any) => {
            const outputName = col.outputName || col.column
            return {
              name: outputName, // Use resolved outputName (e.g., "src_config_id_l")
              datatype: col.datatype || col.data_type || col.type || 'TEXT',
              nullable: col.nullable !== undefined ? col.nullable : true,
            }
          })
          
          if (columnMetadata.length > 0) {
            console.log('[AggregatesConfig] Using columns from join outputColumns (with resolved names):', columnMetadata)
            setAvailableColumns(columnMetadata)
            setLoading(false)
            return
          }
        }
      }
      
      // Handle direct source node
      if (inputNode.data.type === 'source' && inputNode.data.config) {
        const config = inputNode.data.config
        if (config.sourceId && config.tableName) {
          const { sourceTableApi } = await import('../../../services/api')
          const response = await sourceTableApi.getColumns(
            config.sourceId,
            config.tableName,
            config.schema
          )
          const columns = response.data.columns || []
          const columnMetadata: ColumnMetadata[] = columns.map((col: any) => ({
            name: col.name || col.column_name,
            datatype: col.data_type || col.datatype || 'TEXT',
            nullable: col.nullable !== undefined ? col.nullable : true,
          }))
          setAvailableColumns(columnMetadata)
          setLoading(false)
          return
        }
      }

      // Handle transform nodes - get columns from output_metadata
      if (inputNode.data.output_metadata && inputNode.data.output_metadata.columns) {
        const columns = inputNode.data.output_metadata.columns.map((col: any) => {
          if (typeof col === 'string') {
            const business_name = col
            return {
              name: business_name,
              business_name,
              technical_name: col,
              db_name: undefined,
              datatype: 'TEXT',
              nullable: true,
            } as ColumnMetadata
          }
          const business_name = col.business_name || col.name || col.column_name || col
          const technical_name = col.technical_name || col.db_name || col.name || col.column_name || business_name
          const db_name = col.db_name
          return {
            name: business_name,
            business_name,
            technical_name,
            db_name,
            datatype: (col.datatype || col.data_type || col.type || 'TEXT') as any,
            nullable: col.nullable !== undefined ? col.nullable : true,
          } as ColumnMetadata
        })
        setAvailableColumns(columns)
        setLoading(false)
        return
      }

      // Fallback: use config.columns if available
      if (inputNode.data.config?.columns) {
        const columns = Array.isArray(inputNode.data.config.columns)
          ? inputNode.data.config.columns.map((col: any) => {
            if (typeof col === 'string') {
              const business_name = col
              return {
                name: business_name,
                business_name,
                technical_name: col,
                db_name: undefined,
                datatype: 'TEXT',
                nullable: true,
              } as ColumnMetadata
            }
            const business_name = col.business_name || col.name || col.column_name || col
            const technical_name = col.technical_name || col.db_name || col.name || col.column_name || business_name
            const db_name = col.db_name
            return {
              name: business_name,
              business_name,
              technical_name,
              db_name,
              datatype: (col.datatype || col.data_type || col.type || 'TEXT') as any,
              nullable: col.nullable !== undefined ? col.nullable : true,
            } as ColumnMetadata
          })
          : []
        setAvailableColumns(columns)
        setLoading(false)
        return
      }

      setError('Could not determine available columns. Please ensure the input node is properly configured.')
    } catch (err: any) {
      console.error('[AggregatesConfig] Error loading columns:', err)
      setError(err.message || 'Failed to load columns')
    } finally {
      setLoading(false)
    }
  }

  // Filter columns to only numeric types (for SUM, AVG, MIN, MAX)
  const numericColumns = availableColumns.filter(col => {
    const datatype = (col.datatype || 'TEXT').toUpperCase()
    return ['INTEGER', 'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'BIGINT', 'SMALLINT', 'TINYINT'].includes(datatype)
  })

  // All columns (for COUNT which can count any column)
  const allColumns = availableColumns

  const autoGroupBy = getAutoGroupByColumns(availableColumns)

  const addAggregate = () => {
    const newAggregate: AggregateColumn = {
      id: `agg-${Date.now()}`,
      function: 'SUM',
      column: '',
      alias: '',
      groupBy: [...autoGroupBy],
    }
    setAggregateColumns([...aggregateColumns, newAggregate])
  }

  const removeAggregate = (id: string) => {
    setAggregateColumns(aggregateColumns.filter((a) => a.id !== id))
  }

  const updateAggregate = (id: string, updates: Partial<AggregateColumn>) => {
    setAggregateColumns(
      aggregateColumns.map((a) => (a.id === id ? { ...a, ...updates } : a))
    )
  }

  // Live updates: push config and output_metadata when aggregates or business name change (no per-node Save button).
  const liveApplyRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const lastLiveHashRef = useRef<string>('')
  useEffect(() => {
    if (!node || aggregateColumns.length === 0) return
    const invalidAggregates = aggregateColumns.filter(a => {
      const needsColumn = a.function !== 'COUNT'
      const hasGroupBy = (a.groupBy.length > 0 ? a.groupBy : autoGroupBy).length > 0
      return (needsColumn && !a.column) || !a.alias.trim() || !hasGroupBy
    })
    if (invalidAggregates.length > 0) return

    if (liveApplyRef.current) clearTimeout(liveApplyRef.current)
    liveApplyRef.current = setTimeout(() => {
      liveApplyRef.current = null

      const allGroupByColumns = new Set<string>()
      aggregateColumns.forEach(agg => {
        const effectiveGb = agg.groupBy.length > 0 ? agg.groupBy : autoGroupBy
        effectiveGb.forEach(col => allGroupByColumns.add(col))
      })
      const outputColumns = [
        ...Array.from(allGroupByColumns).map(colName => {
          const col = availableColumns.find(c => c.name === colName)
          return {
            name: col?.business_name || colName,
            business_name: col?.business_name || colName,
            technical_name: col?.technical_name ?? col?.db_name ?? colName,
            db_name: col?.db_name,
            datatype: col?.datatype || 'TEXT',
            nullable: col?.nullable !== undefined ? col.nullable : true,
          }
        }),
        ...aggregateColumns.map(agg => ({
          name: agg.alias,
          business_name: agg.alias,
          technical_name: agg.alias,
          db_name: undefined,
          datatype: agg.function === 'COUNT' ? 'INTEGER' : 'NUMERIC',
          nullable: false,
        })),
      ]
      const outputMetadata = { columns: outputColumns, nodeId: node.data.node_id || node.id }
      const aggregateColumnsToSave = aggregateColumns.map(agg => ({
        ...agg,
        groupBy: agg.groupBy.length > 0 ? agg.groupBy : autoGroupBy,
      }))
      const config = { ...(node.data?.config || {}), aggregateColumns: aggregateColumnsToSave }

      const nextHash = JSON.stringify({
        nodeId: node.id,
        business_name: businessName || 'Aggregate',
        technical_name: technicalName || node.id,
        config,
        output_metadata: outputMetadata,
      })
      if (nextHash === lastLiveHashRef.current) return
      lastLiveHashRef.current = nextHash

      onUpdate(node.id, {
        config,
        business_name: businessName || 'Aggregate',
        technical_name: technicalName || node.id,
        output_metadata: outputMetadata,
      })
    }, 150)

    return () => {
      if (liveApplyRef.current) clearTimeout(liveApplyRef.current)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [node?.id, aggregateColumns, businessName, technicalName, availableColumns, autoGroupBy])

  return (
    <Box bg={bg} h="100%" display="flex" flexDirection="column">
      {/* Header */}
      <Box bg={headerBg} p={4} borderBottomWidth="1px" borderColor={borderColor}>
        <HStack justify="space-between" align="center" mb={2}>
          <Text fontSize="lg" fontWeight="semibold">
            Aggregates
          </Text>
          {/* Live updates: no per-node Save button */}
        </HStack>
        <VStack align="stretch" spacing={2} mb={2}>
          <HStack spacing={2}>
            <FormControl size="sm">
              <FormLabel fontSize="xs">Business Name</FormLabel>
              <Input
                size="sm"
                value={businessName}
                onChange={(e) => setBusinessName(e.target.value)}
                placeholder="e.g., Sales Aggregation"
              />
            </FormControl>
            <FormControl size="sm">
              <FormLabel fontSize="xs">Technical Name</FormLabel>
              <Input
                size="sm"
                value={technicalName}
                onChange={(e) => setTechnicalName(e.target.value)}
                placeholder="Auto-generated"
                isReadOnly
                bg={useColorModeValue('gray.100', 'gray.700')}
              />
            </FormControl>
          </HStack>
        </VStack>
        <Text fontSize="sm" color="gray.600">
          Group-level aggregate functions. Configure aggregates for data grouping.
        </Text>
      </Box>

      {/* Content */}
      <Box flex={1} overflowY="auto" p={4}>
        {error && (
          <Alert status="error" size="sm" mb={4}>
            <AlertIcon />
            <Text fontSize="xs">
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

        {loading ? (
          <Box textAlign="center" py={8}>
            <Text fontSize="sm" color="gray.500">
              Loading columns...
            </Text>
          </Box>
        ) : availableColumns.length === 0 ? (
          <Box textAlign="center" py={8}>
            <Text fontSize="sm" color="gray.500" mb={4}>
              No columns available. Please ensure the input node is properly configured.
            </Text>
          </Box>
        ) : (
          <VStack align="stretch" spacing={6}>

            {/* ── Schema drift / column removal errors ─────────────────── */}
            {(() => {
              const configErrors: any[] = Array.isArray((node as any)?.data?.config_errors)
                ? (node as any).data.config_errors
                : []
              const flatErrors: string[] = Array.isArray((node as any)?.data?.errors)
                ? (node as any).data.errors.filter((e: string) => e.includes('not found'))
                : []
              if (configErrors.length === 0 && flatErrors.length === 0) return null
              return (
                <Alert status="error" size="sm" borderRadius="md">
                  <AlertIcon />
                  <Box w="100%">
                    <Text fontSize="xs" fontWeight="semibold" mb={1}>
                      ❌ Schema errors — columns removed upstream
                    </Text>
                    {configErrors.length > 0
                      ? configErrors.map((e: any, i: number) => (
                          <Text key={i} fontSize="xs" mt={0.5}>
                            • {e.message}
                          </Text>
                        ))
                      : flatErrors.map((e: string, i: number) => (
                          <Text key={i} fontSize="xs" mt={0.5}>
                            • {e}
                          </Text>
                        ))}
                    <Text fontSize="xs" color="red.600" mt={1} fontStyle="italic">
                      Fix the expressions or re-include the removed column upstream.
                    </Text>
                  </Box>
                </Alert>
              )
            })()}

            <Box>
              <Text fontSize="sm" fontWeight="semibold" mb={3}>
                Aggregations
              </Text>
              {aggregateColumns.length === 0 ? (
                <Box textAlign="center" py={4}>
                  <Text fontSize="sm" color="gray.500" mb={3}>
                    No aggregates defined
                  </Text>
                  <Button leftIcon={<Plus />} size="sm" onClick={addAggregate}>
                    Add Aggregate
                  </Button>
                </Box>
              ) : (
                <VStack align="stretch" spacing={3}>
                  {aggregateColumns.map((agg, index) => {
                    // For COUNT, show all columns; for others, show only numeric columns
                    const columnOptions = agg.function === 'COUNT' ? allColumns : numericColumns

                    // Per-agg error from config_errors
                    const aggConfigErrors: any[] = Array.isArray((node as any)?.data?.config_errors)
                      ? (node as any).data.config_errors.filter((e: any) => e.aggId === agg.id)
                      : []
                    const aggHasError = aggConfigErrors.length > 0

                    return (
                      <Box
                        key={agg.id}
                        p={3}
                        borderWidth={aggHasError ? '2px' : '1px'}
                        borderColor={aggHasError ? 'red.400' : borderColor}
                        borderRadius="md"
                        bg={aggHasError ? useColorModeValue('red.50', 'red.900') : undefined}
                      >
                        <HStack justify="space-between" mb={2}>
                          <HStack spacing={2}>
                            <Text fontSize="sm" fontWeight="semibold">
                              Aggregate {index + 1}
                            </Text>
                            {aggHasError && (
                              <Badge colorScheme="red" fontSize="2xs">
                                ❌ Error
                              </Badge>
                            )}
                          </HStack>
                          <HStack spacing={1}>
                            <IconButton
                              aria-label="Remove aggregate"
                              icon={<X size={12} />}
                              size="xs"
                              variant="ghost"
                              colorScheme="red"
                              onClick={() => removeAggregate(agg.id)}
                            />
                          </HStack>
                        </HStack>

                        {/* Per-agg error messages */}
                        {aggHasError && (
                          <Box mb={2}>
                            {aggConfigErrors.map((e: any, ei: number) => (
                              <Text key={ei} fontSize="xs" color="red.600" fontWeight="medium">
                                ❌ {e.message}
                              </Text>
                            ))}
                          </Box>
                        )}

                        <VStack align="stretch" spacing={2}>
                          <FormControl isRequired>
                            <FormLabel fontSize="xs">Aggregate Function</FormLabel>
                            <Select
                              size="sm"
                              value={agg.function}
                              onChange={(e) => {
                                updateAggregate(agg.id, {
                                  function: e.target.value as AggregateColumn['function'],
                                  column: '', // Reset column when function changes
                                })
                              }}
                            >
                              {aggregateFunctions.map((func) => (
                                <option key={func.name} value={func.name}>
                                  {func.name} - {func.description}
                                </option>
                              ))}
                            </Select>
                          </FormControl>
                          <FormControl isRequired={agg.function !== 'COUNT'}>
                            <FormLabel fontSize="xs">
                              Column {agg.function !== 'COUNT' && '(Numeric Only)'}
                              {agg.function === 'COUNT' && ' (Optional - leave empty or select * to count all rows)'}
                            </FormLabel>
                            {/* Keep showing the previously selected column even if it was removed upstream,
                                so the user can see which column the aggregate was using. */}
                            <Select
                              size="sm"
                              value={agg.column || ''}
                              onChange={(e) => {
                                updateAggregate(agg.id, { column: e.target.value || '' })
                              }}
                              placeholder={agg.function === 'COUNT' ? 'Select column or leave empty for COUNT(*)' : 'Select column'}
                            >
                              {agg.function === 'COUNT' && (
                                <option value="">* (Count All Rows)</option>
                              )}
                              {agg.column &&
                                !columnOptions.some((col) => col.name === agg.column) && (
                                  <option value={agg.column} disabled>
                                    {agg.column} (removed upstream)
                                  </option>
                                )}
                              {columnOptions.length === 0 ? (
                                <option disabled>
                                  {agg.function === 'COUNT'
                                    ? 'No columns available'
                                    : 'No numeric columns available'}
                                </option>
                              ) : (
                                columnOptions.map((col) => (
                                  <option key={col.name} value={col.name}>
                                    {col.name} ({col.datatype || 'TEXT'})
                                  </option>
                                ))
                              )}
                            </Select>
                            {agg.function !== 'COUNT' && numericColumns.length === 0 && (
                              <Text fontSize="xs" color="orange.500" mt={1}>
                                No numeric columns available. Only numeric columns can be used with {agg.function}.
                              </Text>
                            )}
                            {agg.function === 'COUNT' && (
                              <Text fontSize="xs" color="gray.500" mt={1}>
                                Leave empty or select * to count all rows, or select a column to count non-null values.
                              </Text>
                            )}
                          </FormControl>
                          <FormControl isRequired>
                            <FormLabel fontSize="xs">Alias</FormLabel>
                            <Input
                              size="sm"
                              value={agg.alias}
                              onChange={(e) => {
                                updateAggregate(agg.id, { alias: e.target.value })
                              }}
                              placeholder="e.g., total_amount"
                            />
                          </FormControl>
                          <FormControl>
                            <FormLabel fontSize="xs">Group By (auto)</FormLabel>
                            <Box
                              fontSize="xs"
                              color={groupByTextColor}
                              p={2}
                              bg={groupByBg}
                              borderRadius="md"
                            >
                              {autoGroupBy.length > 0 ? (
                                <Text>
                                  All non-numeric columns: {autoGroupBy.join(', ')}
                                </Text>
                              ) : (
                                <Text>
                                  No group-by (all columns are numeric — result will be a single row)
                                </Text>
                              )}
                            </Box>
                          </FormControl>
                        </VStack>
                      </Box>
                    )
                  })}
                  <Button leftIcon={<Plus />} size="sm" onClick={addAggregate}>
                    Add Aggregate
                  </Button>
                </VStack>
              )}
            </Box>
          </VStack>
        )}
      </Box>


    </Box>
  )
}
