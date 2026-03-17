/**
 * Column Properties Panel Component
 * Displays table columns with search, pagination, grouping, and management features
 */
import React, { useState, useEffect, useMemo } from 'react'
import {
  Box,
  VStack,
  HStack,
  Text,
  Input,
  InputGroup,
  InputLeftElement,
  InputRightElement,
  IconButton,
  Badge,
  Button,
  Spinner,
  Alert,
  AlertIcon,
  Divider,
  Checkbox,
  Tooltip,
  Menu,
  MenuButton,
  MenuList,
  MenuItem,
  useColorModeValue,
  Collapse,
  Select,
} from '@chakra-ui/react'
import { Search, X, Edit, Code, Filter, ChevronDown, ChevronRight, MoreVertical } from 'lucide-react'
import { sourceTableApi } from '../../../services/api'

interface ColumnDefinition {
  name: string
  data_type: string
  nullable: boolean
  default_value?: string | null
  max_length?: number | null
}

interface ColumnPropertiesPanelProps {
  sourceId: number
  tableName: string
  schema?: string
}

interface ColumnGroup {
  type: string
  columns: ColumnDefinition[]
}

export const ColumnPropertiesPanel: React.FC<ColumnPropertiesPanelProps> = ({
  sourceId,
  tableName,
  schema,
}) => {
  const [columns, setColumns] = useState<ColumnDefinition[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedColumns, setSelectedColumns] = useState<Set<string>>(new Set())
  const [mappedColumns, setMappedColumns] = useState<Map<string, string>>(new Map())
  const [renamedColumns, setRenamedColumns] = useState<Map<string, string>>(new Map())
  const [typeFilter, setTypeFilter] = useState<string>('all')
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set(['string', 'number', 'date']))
  const [page, setPage] = useState(1)
  const [hasMore, setHasMore] = useState(false)
  const [totalColumns, setTotalColumns] = useState(0)
  const pageSize = 100

  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const headerBg = useColorModeValue('gray.50', 'gray.700')
  const hoverBg = useColorModeValue('gray.50', 'gray.700')

  // Load columns on mount
  useEffect(() => {
    setPage(1)
    fetchColumns(1, false)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sourceId, tableName, schema])

  // Refetch when search or type filter changes (debounced)
  useEffect(() => {
    const timeoutId = setTimeout(() => {
      setPage(1)
      fetchColumns(1, false)
    }, 300) // Debounce search

    return () => clearTimeout(timeoutId)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchTerm, typeFilter])

  const fetchColumns = async (pageNum: number, append: boolean) => {
    setLoading(true)
    setError(null)
    try {
      // First try to get from stored table fields
      const { api } = await import('../../../services/api')
      const tableResponse = await api.get(
        `/api/api-customer/sources/${sourceId}/selected-tables/`,
        {
          params: {
            table_name: tableName,
            schema: schema || '',
          },
        }
      )

      const tables = tableResponse.data.tables || []
      const table = tables.find(
        (t: any) => t.table_name === tableName && (t.schema === schema || (!t.schema && !schema))
      )

      if (table?.table_fields) {
        const fields = typeof table.table_fields === 'string' 
          ? JSON.parse(table.table_fields) 
          : table.table_fields
        
        if (append) {
          setColumns((prev) => [...prev, ...fields])
        } else {
          setColumns(fields)
          setTotalColumns(fields.length)
        }
        setHasMore(false) // All columns loaded from local DB
        return
      }
      
      // If not in local DB, fetch from API with pagination
      // api is already imported above
      const response = await api.get(
        `/api/api-customer/sources/${sourceId}/columns/`,
        {
          params: {
            table_name: tableName,
            schema: schema || '',
            page: pageNum,
            page_size: pageSize,
            search: searchTerm || undefined,
            type_filter: typeFilter !== 'all' ? typeFilter : undefined,
          },
        }
      )
      
      const fetchedColumns = response.data.columns || []
      const total = response.data.total || fetchedColumns.length
      const hasMoreData = response.data.has_more || false
      
      if (append) {
        setColumns((prev) => [...prev, ...fetchedColumns])
      } else {
        setColumns(fetchedColumns)
        setTotalColumns(total)
      }
      setHasMore(hasMoreData)
    } catch (err: any) {
      setError(err.response?.data?.error || err.message || 'Failed to fetch columns')
      console.error('Error fetching columns:', err)
    } finally {
      setLoading(false)
    }
  }

  const loadMoreColumns = () => {
    if (!loading && hasMore) {
      fetchColumns(page + 1, true)
      setPage(page + 1)
    }
  }

  const getColumnTypeCategory = (dataType: string): string => {
    const type = dataType.toLowerCase()
    if (type.includes('int') || type.includes('number') || type.includes('decimal') || type.includes('float') || type.includes('double')) {
      return 'number'
    }
    if (type.includes('char') || type.includes('text') || type.includes('varchar') || type.includes('string')) {
      return 'string'
    }
    if (type.includes('date') || type.includes('time') || type.includes('timestamp')) {
      return 'date'
    }
    if (type.includes('bool') || type.includes('bit')) {
      return 'boolean'
    }
    return 'other'
  }

  // Group columns by data type
  const groupedColumns = useMemo(() => {
    const groups: Map<string, ColumnDefinition[]> = new Map()
    
    columns.forEach((col) => {
      const type = getColumnTypeCategory(col.data_type)
      if (!groups.has(type)) {
        groups.set(type, [])
      }
      groups.get(type)!.push(col)
    })
    
    return Array.from(groups.entries()).map(([type, cols]) => ({
      type,
      columns: cols,
    }))
  }, [columns])

  // Filter columns based on search and type filter
  const filteredGroupedColumns = useMemo(() => {
    let filtered = groupedColumns

    // Apply type filter
    if (typeFilter !== 'all') {
      filtered = filtered.filter((group) => group.type === typeFilter)
    }

    // Apply search filter
    if (searchTerm) {
      filtered = filtered.map((group) => ({
        ...group,
        columns: group.columns.filter((col) =>
          col.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
          col.data_type.toLowerCase().includes(searchTerm.toLowerCase())
        ),
      })).filter((group) => group.columns.length > 0)
    }

    return filtered
  }, [groupedColumns, searchTerm, typeFilter])

  const getTypeColor = (dataType: string) => {
    const category = getColumnTypeCategory(dataType)
    const colors: Record<string, string> = {
      number: 'blue',
      string: 'green',
      date: 'purple',
      boolean: 'orange',
      other: 'gray',
    }
    return colors[category] || 'gray'
  }

  const toggleGroup = (type: string) => {
    const newExpanded = new Set(expandedGroups)
    if (newExpanded.has(type)) {
      newExpanded.delete(type)
    } else {
      newExpanded.add(type)
    }
    setExpandedGroups(newExpanded)
  }

  const toggleColumnSelection = (columnName: string) => {
    const newSelected = new Set(selectedColumns)
    if (newSelected.has(columnName)) {
      newSelected.delete(columnName)
    } else {
      newSelected.add(columnName)
    }
    setSelectedColumns(newSelected)
  }

  const handleRenameColumn = (columnName: string, newName: string) => {
    const newRenamed = new Map(renamedColumns)
    if (newName.trim()) {
      newRenamed.set(columnName, newName.trim())
    } else {
      newRenamed.delete(columnName)
    }
    setRenamedColumns(newRenamed)
  }

  const handleMapColumn = (columnName: string, mappedName: string) => {
    const newMapped = new Map(mappedColumns)
    if (mappedName.trim()) {
      newMapped.set(columnName, mappedName.trim())
    } else {
      newMapped.delete(columnName)
    }
    setMappedColumns(newMapped)
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
        <VStack align="stretch" spacing={2}>
          <Text fontSize="lg" fontWeight="semibold">
            Column Properties
          </Text>
          <Text fontSize="xs" color="gray.500">
            {tableName}
            {schema && ` (${schema})`}
          </Text>
          <Text fontSize="xs" color="gray.500">
            {totalColumns} column{totalColumns !== 1 ? 's' : ''}
          </Text>
        </VStack>
      </Box>

      {/* Search and Filters */}
      <Box p={3} borderBottomWidth="1px" borderColor={borderColor}>
        <VStack align="stretch" spacing={2}>
          <InputGroup size="sm">
            <InputLeftElement pointerEvents="none">
              <Search size={14} />
            </InputLeftElement>
            <Input
              placeholder="Search columns..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />
            {searchTerm && (
              <InputRightElement>
                <IconButton
                  aria-label="Clear search"
                  icon={<X size={12} />}
                  size="xs"
                  variant="ghost"
                  onClick={() => setSearchTerm('')}
                />
              </InputRightElement>
            )}
          </InputGroup>
          <Select
            size="sm"
            value={typeFilter}
            onChange={(e) => setTypeFilter(e.target.value)}
          >
            <option value="all">All Types</option>
            <option value="string">String</option>
            <option value="number">Number</option>
            <option value="date">Date/Time</option>
            <option value="boolean">Boolean</option>
            <option value="other">Other</option>
          </Select>
        </VStack>
      </Box>

      {/* Content */}
      <Box flex={1} overflowY="auto" p={3}>
        {loading && columns.length === 0 ? (
          <Box textAlign="center" py={8}>
            <Spinner size="md" />
            <Text mt={4} fontSize="sm" color="gray.500">
              Loading columns...
            </Text>
          </Box>
        ) : error ? (
          <Alert status="error" size="sm">
            <AlertIcon />
            {error}
          </Alert>
        ) : filteredGroupedColumns.length === 0 ? (
          <Box textAlign="center" py={8}>
            <Text fontSize="sm" color="gray.500">
              No columns found
            </Text>
          </Box>
        ) : (
          <VStack align="stretch" spacing={3}>
            {filteredGroupedColumns.map((group) => (
              <Box key={group.type}>
                <Button
                  size="sm"
                  variant="ghost"
                  w="100%"
                  justifyContent="space-between"
                  onClick={() => toggleGroup(group.type)}
                  fontWeight="semibold"
                  textTransform="capitalize"
                >
                  <HStack spacing={2}>
                    {expandedGroups.has(group.type) ? (
                      <ChevronDown size={14} />
                    ) : (
                      <ChevronRight size={14} />
                    )}
                    <Text>{group.type}</Text>
                    <Badge size="sm" colorScheme={getTypeColor(group.type)}>
                      {group.columns.length}
                    </Badge>
                  </HStack>
                </Button>
                <Collapse in={expandedGroups.has(group.type)} animateOpacity>
                  <VStack align="stretch" spacing={1} mt={2} pl={4}>
                    {group.columns.map((col, idx) => (
                      <Box
                        key={idx}
                        p={2}
                        borderRadius="md"
                        _hover={{ bg: hoverBg }}
                        borderWidth="1px"
                        borderColor={selectedColumns.has(col.name) ? 'blue.300' : 'transparent'}
                      >
                        <HStack justify="space-between" mb={1}>
                          <HStack spacing={2} flex={1}>
                            <Checkbox
                              size="sm"
                              isChecked={selectedColumns.has(col.name)}
                              onChange={() => toggleColumnSelection(col.name)}
                            />
                            <VStack align="start" spacing={0} flex={1} minW={0}>
                              <Text
                                fontSize="sm"
                                fontWeight={selectedColumns.has(col.name) ? 'semibold' : 'normal'}
                                isTruncated
                                title={col.name}
                              >
                                {renamedColumns.get(col.name) || col.name}
                              </Text>
                              <HStack spacing={1}>
                                <Badge size="xs" colorScheme={getTypeColor(col.data_type)}>
                                  {col.data_type}
                                </Badge>
                                {!col.nullable && (
                                  <Badge size="xs" colorScheme="red">
                                    NOT NULL
                                  </Badge>
                                )}
                              </HStack>
                            </VStack>
                          </HStack>
                          <Menu>
                            <MenuButton
                              as={IconButton}
                              icon={<MoreVertical size={14} />}
                              size="xs"
                              variant="ghost"
                              aria-label="Column options"
                            />
                            <MenuList>
                              <MenuItem icon={<Edit size={14} />} onClick={() => {
                                const newName = prompt('Rename column:', renamedColumns.get(col.name) || col.name)
                                if (newName !== null) {
                                  handleRenameColumn(col.name, newName)
                                }
                              }}>
                                Rename
                              </MenuItem>
                              <MenuItem icon={<Code size={14} />} onClick={() => {
                                // TODO: Open expression editor
                                alert('Expression editor coming soon')
                              }}>
                                Expression
                              </MenuItem>
                              <MenuItem icon={<Filter size={14} />} onClick={() => {
                                // TODO: Open filter options
                                alert('Filter options coming soon')
                              }}>
                                Filter
                              </MenuItem>
                            </MenuList>
                          </Menu>
                        </HStack>
                        {(col.default_value || col.max_length) && (
                          <HStack spacing={2} fontSize="xs" color="gray.500" mt={1} pl={6}>
                            {col.default_value && (
                              <Text>
                                Default: <Text as="span" fontWeight="medium">{col.default_value}</Text>
                              </Text>
                            )}
                            {col.max_length && (
                              <Text>
                                Max: <Text as="span" fontWeight="medium">{col.max_length}</Text>
                              </Text>
                            )}
                          </HStack>
                        )}
                      </Box>
                    ))}
                  </VStack>
                </Collapse>
              </Box>
            ))}
          </VStack>
        )}

        {/* Load More Button */}
        {hasMore && !loading && (
          <Box textAlign="center" mt={4}>
            <Button size="sm" variant="outline" onClick={loadMoreColumns}>
              Load More ({pageSize} more columns)
            </Button>
          </Box>
        )}

        {loading && columns.length > 0 && (
          <Box textAlign="center" mt={4}>
            <Spinner size="sm" />
          </Box>
        )}
      </Box>
    </Box>
  )
}

