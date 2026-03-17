/**
 * Column Definitions Context Menu Component
 * Shows column definitions when right-clicking on a table node
 */
import React, { useState, useEffect } from 'react'
import {
  Menu,
  MenuButton,
  MenuList,
  MenuItem,
  Portal,
  Box,
  VStack,
  HStack,
  Text,
  Badge,
  Spinner,
  useColorModeValue,
  Divider,
  Tooltip,
} from '@chakra-ui/react'
import { Database, Info } from 'lucide-react'
import { api } from '../../../services/api'

interface ColumnDefinition {
  name: string
  data_type: string
  nullable: boolean
  default_value?: string | null
  max_length?: number | null
}

interface ColumnDefinitionsMenuProps {
  sourceId: number
  tableName: string
  schema?: string
  isOpen: boolean
  onClose: () => void
  position: { x: number; y: number }
}

export const ColumnDefinitionsMenu: React.FC<ColumnDefinitionsMenuProps> = ({
  sourceId,
  tableName,
  schema,
  isOpen,
  onClose,
  position,
}) => {
  const [columns, setColumns] = useState<ColumnDefinition[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')

  useEffect(() => {
    if (isOpen) {
      fetchColumnDefinitions()
    }
  }, [isOpen, sourceId, tableName, schema])

  const fetchColumnDefinitions = async () => {
    setLoading(true)
    setError(null)
    try {
      // First try to get from stored table fields
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
        // Use stored fields from local database
        const fields = typeof table.table_fields === 'string' 
          ? JSON.parse(table.table_fields) 
          : table.table_fields
        setColumns(fields)
        setLoading(false)
        return
      }
      
      // If not in local DB, fetch from API (which will try local DB first, then source)
      const { sourceTableApi } = await import('../../../services/api')
      const response = await sourceTableApi.getColumns(sourceId, tableName, schema)
      setColumns(response.data.columns || [])
    } catch (err: any) {
      setError(err.response?.data?.error || err.message || 'Failed to fetch column definitions')
      console.error('Error fetching column definitions:', err)
    } finally {
      setLoading(false)
    }
  }

  if (!isOpen) return null

  const getTypeColor = (dataType: string) => {
    const type = dataType.toLowerCase()
    if (type.includes('int') || type.includes('number')) return 'blue'
    if (type.includes('char') || type.includes('text') || type.includes('varchar')) return 'green'
    if (type.includes('date') || type.includes('time')) return 'purple'
    if (type.includes('bool')) return 'orange'
    return 'gray'
  }

  return (
    <Portal>
      <Box
        position="fixed"
        left={`${position.x}px`}
        top={`${position.y}px`}
        bg={bg}
        borderWidth="1px"
        borderColor={borderColor}
        borderRadius="md"
        boxShadow="lg"
        zIndex={2000}
        minW="400px"
        maxW="600px"
        maxH="500px"
        overflowY="auto"
      >
        {/* Header */}
        <Box p={3} borderBottomWidth="1px" borderBottomColor={borderColor}>
          <HStack spacing={2}>
            <Info size={16} />
            <VStack align="start" spacing={0}>
              <Text fontWeight="bold" fontSize="sm">
                Column Definitions
              </Text>
              <Text fontSize="xs" color="gray.500">
                {tableName}
                {schema && ` (${schema})`}
              </Text>
            </VStack>
          </HStack>
        </Box>

        {/* Content */}
        <Box p={3}>
          {loading ? (
            <Box textAlign="center" py={4}>
              <Spinner size="sm" />
              <Text mt={2} fontSize="xs" color="gray.500">
                Loading column definitions...
              </Text>
            </Box>
          ) : error ? (
            <Text fontSize="sm" color="red.500">
              {error}
            </Text>
          ) : columns.length === 0 ? (
            <Text fontSize="sm" color="gray.500">
              No column definitions available
            </Text>
          ) : (
            <VStack align="stretch" spacing={2}>
              {columns.map((col, idx) => (
                <Box
                  key={idx}
                  p={2}
                  borderRadius="md"
                  _hover={{ bg: useColorModeValue('gray.50', 'gray.700') }}
                >
                  <HStack justify="space-between" mb={1}>
                    <Text fontWeight="medium" fontSize="sm">
                      {col.name}
                    </Text>
                    <HStack spacing={1}>
                      <Badge size="sm" colorScheme={getTypeColor(col.data_type)}>
                        {col.data_type}
                      </Badge>
                      {!col.nullable && (
                        <Badge size="sm" colorScheme="red">
                          NOT NULL
                        </Badge>
                      )}
                    </HStack>
                  </HStack>
                  {(col.default_value || col.max_length) && (
                    <HStack spacing={2} fontSize="xs" color="gray.500" mt={1}>
                      {col.default_value && (
                        <Text>
                          Default: <Text as="span" fontWeight="medium">{col.default_value}</Text>
                        </Text>
                      )}
                      {col.max_length && (
                        <Text>
                          Max Length: <Text as="span" fontWeight="medium">{col.max_length}</Text>
                        </Text>
                      )}
                    </HStack>
                  )}
                  {idx < columns.length - 1 && <Divider mt={2} />}
                </Box>
              ))}
            </VStack>
          )}
        </Box>
      </Box>
    </Portal>
  )
}

