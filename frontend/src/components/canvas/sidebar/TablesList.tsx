/**
 * TablesList - Component for displaying tables with infinite scroll and search
 * Supports cursor-based pagination and server-side search
 */
import React, { useState, useEffect, useCallback, useRef } from 'react'
import {
  Box,
  VStack,
  HStack,
  Text,
  Spinner,
  Input,
  InputGroup,
  InputLeftElement,
  InputRightElement,
  IconButton,
  Badge,
  useToast,
  Tooltip,
  Menu,
  MenuButton,
  MenuList,
  MenuItem,
} from '@chakra-ui/react'
import { useColorModeValue } from '../../../hooks/useColorModeValue'
import { Search, X, Database, CheckCircle2, Clock, Filter, MoreVertical, Eye, RefreshCw } from 'lucide-react'
import { api, sourceTableApi } from '../../../services/api'

interface Table {
  schema: string
  table_name: string
  last_synced?: string
  added_on?: string
  is_synced?: boolean
}

interface TablesListProps {
  sourceId: number
  onTableDrag?: (table: Table) => void
  onQuickFilter?: (table: Table, sourceId: number) => void
  onTableClick?: (table: Table, sourceId: number) => void
  onRemoveFilter?: (table: Table, sourceId: number) => void
  onPreviewFilteredData?: (table: Table, sourceId: number) => void
  hasTableFilter?: (sourceId: number, tableName: string, schema?: string) => boolean
  forceRefresh?: boolean // If true, force refresh from FastAPI service
}

export const TablesList: React.FC<TablesListProps> = ({ 
  sourceId, 
  onTableDrag, 
  onQuickFilter, 
  onTableClick,
  onRemoveFilter,
  onPreviewFilteredData,
  hasTableFilter,
  forceRefresh = false 
}) => {
  const [tables, setTables] = useState<Table[]>([])
  const [loading, setLoading] = useState(false)
  const [hasMore, setHasMore] = useState(false)
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [searchLoading, setSearchLoading] = useState(false)
  const [searchTimeout, setSearchTimeout] = useState<ReturnType<typeof setTimeout> | null>(null)
  const [contextMenu, setContextMenu] = useState<{
    table: Table
    position: { x: number; y: number }
  } | null>(null)
  const scrollContainerRef = useRef<HTMLDivElement>(null)
  const toast = useToast()
  
  // Close context menu when clicking outside
  useEffect(() => {
    const handleClickOutside = () => {
      if (contextMenu) {
        setContextMenu(null)
      }
    }
    
    if (contextMenu) {
      document.addEventListener('click', handleClickOutside)
      return () => {
        document.removeEventListener('click', handleClickOutside)
      }
    }
  }, [contextMenu])

  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const hoverBg = useColorModeValue('gray.50', 'gray.700')

  // Safely coerce to string for rendering (avoids "Objects are not valid as React child")
  const safeStr = (v: unknown): string =>
    typeof v === 'string' ? v : v != null ? String(v) : ''

  // Fetch tables function
  const fetchTables = useCallback(
    async (cursor: string | null = null, search: string = '', append: boolean = false, forceRefreshParam: boolean = false) => {
      if (loading || searchLoading) return

      if (search) {
        setSearchLoading(true)
      } else {
        setLoading(true)
      }

      try {
        const params: any = {
          source_id: sourceId,
          limit: 100,
        }
        if (cursor) {
          params.cursor = cursor
        }
        if (search) {
          params.search = search
        }
        // Only force refresh when explicitly requested (Fetch Tables button clicked)
        if (forceRefreshParam) {
          params.force_refresh = 'true'
        }

        const response = await api.get(`/api/api-customer/sources/${sourceId}/tables/`, {
          params,
        })

        const data = response.data
        const newTables = data.tables || []

        if (append) {
          setTables((prev) => [...prev, ...newTables])
        } else {
          setTables(newTables)
        }

        setNextCursor(data.next_cursor || null)
        setHasMore(data.has_more || false)
      } catch (error: any) {
        console.error('Failed to fetch tables:', error)

        let description: any =
          error?.response?.data?.error ||
          error?.response?.data?.detail ||
          error?.message ||
          'Failed to fetch tables'

        if (typeof description === 'object') {
          try {
            description = JSON.stringify(description)
          } catch {
            description = 'Failed to fetch tables'
          }
        }

        toast({
          title: 'Error',
          description,
          status: 'error',
          duration: 3000,
          isClosable: true,
        })
      } finally {
        setLoading(false)
        setSearchLoading(false)
      }
    },
    [sourceId, loading, searchLoading, toast]
  )

  // Initial load - use cached tables (no force refresh)
  useEffect(() => {
    if (sourceId) {
      fetchTables(null, '', false, false)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sourceId])

  // When forceRefresh prop changes to true, fetch from FastAPI
  useEffect(() => {
    if (sourceId && forceRefresh) {
      fetchTables(null, searchTerm, false, true)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [forceRefresh])

  // Search debounce — skip on initial mount (we already called fetchTables in the effect above)
  const isFirstSearchRender = useRef(true)
  useEffect(() => {
    if (isFirstSearchRender.current) {
      isFirstSearchRender.current = false
      return
    }
    if (searchTimeout) {
      clearTimeout(searchTimeout)
    }

    const timeout = setTimeout(() => {
      if (sourceId) {
        fetchTables(null, searchTerm, false, false)
      }
    }, 400) // 400ms debounce (was 1000ms — snappier search)

    setSearchTimeout(timeout)

    return () => {
      if (searchTimeout) {
        clearTimeout(searchTimeout)
      }
    }
  }, [searchTerm, sourceId])

  // Infinite scroll handler
  const handleScroll = useCallback(() => {
    const container = scrollContainerRef.current
    if (!container || loading || searchLoading || !hasMore) return

    const { scrollTop, scrollHeight, clientHeight } = container
    // Load more when user is 100px from bottom
    if (scrollHeight - scrollTop - clientHeight < 100) {
      if (nextCursor) {
        fetchTables(nextCursor, searchTerm, true)
      }
    }
  }, [loading, searchLoading, hasMore, nextCursor, searchTerm, fetchTables])

  useEffect(() => {
    const container = scrollContainerRef.current
    if (container) {
      container.addEventListener('scroll', handleScroll)
      return () => container.removeEventListener('scroll', handleScroll)
    }
  }, [handleScroll])

  // Drag handler
  const handleDragStart = (e: React.DragEvent, table: Table) => {
    e.dataTransfer.setData('application/reactflow', JSON.stringify({
      type: 'table',
      table: table,
      sourceId: sourceId,
    }))
    if (onTableDrag) {
      onTableDrag(table)
    }
  }

  return (
    <Box>
      {/* Search Box */}
      <InputGroup size="sm" mb={2}>
        <InputLeftElement pointerEvents="none">
          <Search size={14} />
        </InputLeftElement>
        <Input
          placeholder="Search tables..."
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

      {/* Tables List */}
      <Box
        ref={scrollContainerRef}
        maxH="400px"
        overflowY="auto"
        borderWidth="1px"
        borderColor={borderColor}
        borderRadius="md"
        p={2}
      >
        {tables.length === 0 && !loading && !searchLoading ? (
          <Box p={4} textAlign="center">
            <Text fontSize="sm" color="gray.500">
              {searchTerm ? 'No tables found' : 'No tables available'}
            </Text>
          </Box>
        ) : (
          <VStack align="stretch" spacing={1}>
            {tables.map((table, index) => {
              const isFiltered = hasTableFilter ? hasTableFilter(sourceId, table.table_name, table.schema) : false
              
              return (
              <Box
                key={`${safeStr(table.schema)}.${safeStr(table.table_name)}-${index}`}
                p={2}
                borderRadius="md"
                  cursor="pointer"
                draggable
                onDragStart={(e) => handleDragStart(e, table)}
                  onClick={(e) => {
                    // Only trigger on left click, not on right click or when clicking buttons
                    if (e.button === 0 && !(e.target as HTMLElement).closest('button, [role="button"]')) {
                      if (onTableClick) {
                        onTableClick(table, sourceId)
                      }
                    }
                  }}
                  onContextMenu={(e) => {
                    // Only show context menu for filtered tables
                    if (isFiltered) {
                      e.preventDefault()
                      e.stopPropagation()
                      setContextMenu({
                        table,
                        position: { x: e.clientX, y: e.clientY },
                      })
                    }
                  }}
                  bg={isFiltered ? useColorModeValue('blue.50', 'blue.900') : 'transparent'}
                  _hover={{ bg: isFiltered ? useColorModeValue('blue.100', 'blue.800') : hoverBg, borderColor: borderColor }}
                borderWidth="1px"
                  borderColor={isFiltered ? useColorModeValue('blue.200', 'blue.700') : 'transparent'}
                  position="relative"
              >
                <HStack spacing={2} justify="space-between">
                  <HStack spacing={2} flex={1} minW={0}>
                    <Database size={14} />
                    <VStack align="start" spacing={0} flex={1} minW={0}>
                        <HStack spacing={1} align="center">
                      <Text fontSize="xs" fontWeight="medium" isTruncated title={safeStr(table.table_name)}>
                        {safeStr(table.table_name)}
                      </Text>
                          {isFiltered && (
                            <Tooltip label="This table has an active filter">
                              <Badge 
                                size="xs" 
                                colorScheme="blue" 
                                borderRadius="full"
                                px={1.5}
                                title="Filtered"
                              >
                                <HStack spacing={0.5}>
                                  <Filter size={8} fill="currentColor" />
                                </HStack>
                              </Badge>
                            </Tooltip>
                          )}
                        </HStack>
                      <HStack spacing={1} mt={0.5}>
                        {table.schema && (
                          <Badge size="xs" colorScheme="gray">
                            {safeStr(table.schema)}
                          </Badge>
                        )}
                        {table.is_synced && table.last_synced && (
                          <Badge size="xs" colorScheme="green" title={`Synced: ${new Date(table.last_synced).toLocaleString()}`}>
                            <HStack spacing={1}>
                              <CheckCircle2 size={10} />
                              <Text fontSize="2xs">Synced</Text>
                            </HStack>
                          </Badge>
                        )}
                        {!table.is_synced && (
                          <Badge size="xs" colorScheme="yellow" title="Not yet synced">
                            <HStack spacing={1}>
                              <Clock size={10} />
                              <Text fontSize="2xs">Pending</Text>
                            </HStack>
                          </Badge>
                        )}
                      </HStack>
                    </VStack>
                  </HStack>

                    <HStack spacing={1}>
                      {/* Refresh Schema Icon */}
                      <Tooltip label="Refresh Schema">
                        <IconButton
                          aria-label="Refresh Schema"
                          icon={<RefreshCw size={14} />}
                          size="xs"
                          variant="ghost"
                          colorScheme="green"
                          onClick={async (e) => {
                            e.stopPropagation()
                            try {
                              toast({ title: 'Refreshing Schema...', status: 'info', duration: 2000, position: 'bottom-right' })
                              await sourceTableApi.getColumns(sourceId, table.table_name, table.schema, true)
                              toast({ title: 'Schema Refreshed', status: 'success', duration: 2000, position: 'bottom-right' })
                            } catch (error) {
                              toast({ title: 'Failed to refresh schema', status: 'error', duration: 3000, position: 'bottom-right' })
                            }
                          }}
                        />
                      </Tooltip>
                    
                      {/* Quick Filter Icon */}
                      <Tooltip label="Quick Filter">
                        <IconButton
                          aria-label="Quick Filter"
                          icon={<Filter size={14} />}
                          size="xs"
                          variant="ghost"
                          colorScheme="blue"
                          onClick={(e) => {
                            e.stopPropagation()
                            if (onQuickFilter) {
                              onQuickFilter(table, sourceId)
                            }
                          }}
                        />
                      </Tooltip>
                      
                      {/* Context Menu for Remove Filter */}
                      {isFiltered && onRemoveFilter && (
                        <Menu>
                          <MenuButton
                            as={IconButton}
                            aria-label="Table options"
                            icon={<MoreVertical size={12} />}
                            size="xs"
                            variant="ghost"
                            colorScheme="gray"
                            onClick={(e) => {
                              e.stopPropagation()
                            }}
                          />
                          <MenuList>
                            <MenuItem
                              icon={<X size={14} />}
                              onClick={(e) => {
                                e.stopPropagation()
                                if (onRemoveFilter) {
                                  onRemoveFilter(table, sourceId)
                                }
                              }}
                            >
                              Remove Filter
                            </MenuItem>
                          </MenuList>
                        </Menu>
                      )}
                    </HStack>
                </HStack>
              </Box>
              )
            })}

            {/* Loading indicator */}
            {(loading || searchLoading) && (
              <Box p={4} textAlign="center">
                <Spinner size="sm" />
              </Box>
            )}

            {/* End of list indicator */}
            {!hasMore && tables.length > 0 && (
              <Box p={2} textAlign="center">
                <Text fontSize="xs" color="gray.500">
                  {tables.length} table{tables.length !== 1 ? 's' : ''} shown
                </Text>
              </Box>
            )}
          </VStack>
        )}
      </Box>

      {/* Right-click Context Menu for Filtered Tables */}
      {contextMenu && (
        <Box
          position="fixed"
          left={contextMenu.position.x}
          top={contextMenu.position.y}
          zIndex={1000}
        >
          <Menu
            isOpen={!!contextMenu}
            onClose={() => setContextMenu(null)}
            placement="bottom-start"
          >
            <MenuButton
              as={Box}
              visibility="hidden"
              pointerEvents="none"
              w={0}
              h={0}
            />
            <MenuList>
              <MenuItem
                icon={<Eye size={14} />}
                onClick={() => {
                  if (onPreviewFilteredData) {
                    onPreviewFilteredData(contextMenu.table, sourceId)
                  }
                  setContextMenu(null)
                }}
              >
                Preview Filtered Data
              </MenuItem>
              <MenuItem
                icon={<X size={14} />}
                onClick={() => {
                  if (onRemoveFilter) {
                    onRemoveFilter(contextMenu.table, sourceId)
                  }
                  setContextMenu(null)
                }}
              >
                Remove Filter
              </MenuItem>
            </MenuList>
          </Menu>
        </Box>
      )}
    </Box>
  )
}

