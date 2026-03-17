/**
 * SourceConnectionsSidebar - Left sidebar showing source connections
 * Displays source connections, allows selection, and shows tables when "Fetch Tables" is clicked
 */
import React, { useState, useEffect, useRef } from 'react'
import {
  Box,
  VStack,
  HStack,
  Text,
  Button,
  Spinner,
  Badge,
  Collapse,
  Input,
  InputGroup,
  InputLeftElement,
  InputRightElement,
  IconButton,
  Tooltip,
  AlertDialog,
  AlertDialogOverlay,
  AlertDialogContent,
  AlertDialogHeader,
  AlertDialogBody,
  AlertDialogFooter,
} from '@chakra-ui/react'
import { useColorModeValue } from '../../../hooks/useColorModeValue'
import { Database, ChevronDown, ChevronRight, Search, X, Trash2, RefreshCw } from 'lucide-react'
import { connectionApi } from '../../../services/api'
import { TablesList } from './TablesList'

interface Source {
  source_id: number
  source_name: string
  db_type: string
  hostname?: string
  port?: number
  database?: string
  schema?: string
  created_on?: string
  is_active: boolean
}

interface SourceConnectionsSidebarProps {
  selectedSourceId?: number
  onSourceSelect?: (source: Source) => void
  onTableDrag?: (table: { schema: string; table_name: string }) => void
  onQuickFilter?: (table: { schema: string; table_name: string }, sourceId: number) => void
  onTableClick?: (table: { schema: string; table_name: string }, sourceId: number) => void
  onRemoveFilter?: (table: { schema: string; table_name: string }, sourceId: number) => void
  onPreviewFilteredData?: (table: { schema: string; table_name: string }, sourceId: number) => void
  hasTableFilter?: (sourceId: number, tableName: string, schema?: string) => boolean
}

export const SourceConnectionsSidebar: React.FC<SourceConnectionsSidebarProps> = ({
  selectedSourceId,
  onSourceSelect,
  onTableDrag,
  onQuickFilter,
  onTableClick,
  onRemoveFilter,
  onPreviewFilteredData,
  hasTableFilter,
}) => {
  const [sources, setSources] = useState<Source[]>([])
  const [loading, setLoading] = useState(false)
  const [expandedSourceId, setExpandedSourceId] = useState<number | null>(selectedSourceId || null)
  const [selectedSource, setSelectedSource] = useState<Source | null>(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [forceRefreshSourceId, setForceRefreshSourceId] = useState<number | null>(null)
  const [sourceToDelete, setSourceToDelete] = useState<Source | null>(null)
  const cancelRef = useRef<HTMLButtonElement | null>(null)

  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const hoverBg = useColorModeValue('gray.50', 'gray.700')

  // Load sources on mount
  useEffect(() => {
    loadSources()
  }, [])

  // Set selected source when selectedSourceId changes
  useEffect(() => {
    if (selectedSourceId && sources.length > 0) {
      const source = sources.find(s => s.source_id === selectedSourceId)
      if (source) {
        setSelectedSource(source)
        setExpandedSourceId(selectedSourceId)
        if (onSourceSelect) {
          onSourceSelect(source)
        }
      }
    }
  }, [selectedSourceId, sources])

  const loadSources = async () => {
    setLoading(true)
    try {
      const data = await connectionApi.sources()
      if (data && data.sources) {
        setSources(data.sources)
      }
    } catch (error) {
      console.error('Failed to load sources:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleSourceClick = (source: Source) => {
    if (expandedSourceId === source.source_id) {
      setExpandedSourceId(null)
      setSelectedSource(null)
    } else {
      setExpandedSourceId(source.source_id)
      setSelectedSource(source)
      if (onSourceSelect) {
        onSourceSelect(source)
      }
    }
  }

  const handleFetchTables = (e: React.MouseEvent, source: Source) => {
    e.stopPropagation()
    // Expand the source if not already expanded
    if (expandedSourceId !== source.source_id) {
      setExpandedSourceId(source.source_id)
      setSelectedSource(source)
      if (onSourceSelect) {
        onSourceSelect(source)
      }
    }
    // Trigger force refresh to fetch from FastAPI service
    setForceRefreshSourceId(source.source_id)
    // Reset after a short delay to allow the effect to trigger
    setTimeout(() => {
      setForceRefreshSourceId(null)
    }, 100)
  }

  const handleDeleteSource = async () => {
    if (!sourceToDelete) return
    try {
      await connectionApi.deleteSource(sourceToDelete.source_id)
      setSources(prev => prev.filter(s => s.source_id !== sourceToDelete.source_id))
      if (selectedSource?.source_id === sourceToDelete.source_id) {
        setSelectedSource(null)
        setExpandedSourceId(null)
      }
    } catch (error) {
      console.error('Failed to delete source:', error)
    } finally {
      setSourceToDelete(null)
    }
  }

  // Filter sources based on search term
  const filteredSources = sources.filter(source =>
    source.source_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    source.db_type?.toLowerCase().includes(searchTerm.toLowerCase())
  )

  if (loading) {
    return (
      <Box
        w="280px"
        h="100%"
        bg={bg}
        borderRightWidth="1px"
        borderColor={borderColor}
        display="flex"
        alignItems="center"
        justifyContent="center"
      >
        <Spinner size="md" />
      </Box>
    )
  }

  return (
    <Box
      w="280px"
      h="100%"
      bg={bg}
      borderRightWidth="1px"
      borderColor={borderColor}
      display="flex"
      flexDirection="column"
      overflow="hidden"
    >
      {/* Header */}
      <Box p={4} borderBottomWidth="1px" borderColor={borderColor}>
        <Text fontSize="lg" fontWeight="semibold" mb={2}>
          Source Connections
        </Text>
        <InputGroup size="sm">
          <InputLeftElement pointerEvents="none">
            <Search size={16} />
          </InputLeftElement>
          <Input
            placeholder="Search sources..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
          {searchTerm && (
            <InputRightElement>
              <IconButton
                aria-label="Clear search"
                icon={<X size={14} />}
                size="xs"
                variant="ghost"
                onClick={() => setSearchTerm('')}
              />
            </InputRightElement>
          )}
        </InputGroup>
      </Box>

      {/* Sources List */}
      <Box flex={1} overflowY="auto">
        <VStack align="stretch" spacing={0} p={2}>
          {filteredSources.length === 0 ? (
            <Box p={4} textAlign="center">
              <Text fontSize="sm" color="gray.500">
                {searchTerm ? 'No sources found' : 'No source connections'}
              </Text>
            </Box>
          ) : (
            filteredSources.map((source) => {
              const isExpanded = expandedSourceId === source.source_id
              const isSelected = selectedSource?.source_id === source.source_id

              return (
                <Box key={source.source_id}>
                  <Box
                    p={3}
                    cursor="pointer"
                    bg={isSelected ? hoverBg : 'transparent'}
                    _hover={{ bg: hoverBg }}
                    onClick={() => handleSourceClick(source)}
                    borderRadius="md"
                    mb={1}
                  >
                    <HStack spacing={2} justify="space-between">
                      <HStack spacing={2} flex={1}>
                        {isExpanded ? (
                          <ChevronDown size={16} />
                        ) : (
                          <ChevronRight size={16} />
                        )}
                        <Database size={16} />
                        <VStack align="start" spacing={0} flex={1} minW={0}>
                          <Text
                            fontSize="sm"
                            fontWeight={isSelected ? 'semibold' : 'normal'}
                            isTruncated
                            title={source.source_name}
                          >
                            {source.source_name}
                          </Text>
                          {source.db_type && (
                            <Badge size="sm" colorScheme="blue" mt={1}>
                              {source.db_type.toUpperCase()}
                            </Badge>
                          )}
                        </VStack>
                      </HStack>
                      <HStack spacing={1}>
                        <Tooltip label="Refresh all tables & schemas" hasArrow>
                          <IconButton
                            aria-label="Refresh entire schema"
                            icon={<RefreshCw size={14} />}
                            size="xs"
                            variant="ghost"
                            colorScheme="green"
                            onClick={(e) => {
                              handleFetchTables(e, source)
                            }}
                          />
                        </Tooltip>
                        <Tooltip label="Delete source" hasArrow>
                          <IconButton
                            aria-label="Delete source"
                            icon={<Trash2 size={14} />}
                            size="xs"
                            variant="ghost"
                            colorScheme="red"
                            onClick={(e) => {
                              e.stopPropagation()
                              setSourceToDelete(source)
                            }}
                          />
                        </Tooltip>
                      </HStack>
                    </HStack>
                  </Box>

                  {/* Tables List (shown when expanded) */}
                  <Collapse in={isExpanded} animateOpacity>
                    <Box pl={6} pr={2} pb={2}>
                      <Button
                        size="xs"
                        colorScheme="blue"
                        variant="outline"
                        w="100%"
                        mb={2}
                        onClick={(e) => {
                          e.stopPropagation()
                          handleFetchTables(e, source)
                        }}
                      >
                        Fetch New Tables
                      </Button>
                      {isExpanded && (
                        <TablesList
                          sourceId={source.source_id}
                          onTableDrag={onTableDrag}
                          onQuickFilter={onQuickFilter}
                          onTableClick={onTableClick}
                          onRemoveFilter={onRemoveFilter}
                          onPreviewFilteredData={onPreviewFilteredData}
                          hasTableFilter={hasTableFilter}
                          forceRefresh={forceRefreshSourceId === source.source_id}
                        />
                      )}
                    </Box>
                  </Collapse>
                </Box>
              )
            })
          )}
        </VStack>
      </Box>
      {/* Delete source confirmation dialog */}
      <AlertDialog
        isOpen={!!sourceToDelete}
        leastDestructiveRef={cancelRef}
        onClose={() => setSourceToDelete(null)}
      >
        <AlertDialogOverlay>
          <AlertDialogContent>
            <AlertDialogHeader fontSize="lg" fontWeight="bold">
              Delete Source
            </AlertDialogHeader>
            <AlertDialogBody>
              Are you sure you want to delete source{' '}
              <Text as="span" fontWeight="semibold">
                {sourceToDelete?.source_name}
              </Text>
              ? This action cannot be undone.
            </AlertDialogBody>
            <AlertDialogFooter>
              <Button ref={cancelRef} onClick={() => setSourceToDelete(null)}>
                Cancel
              </Button>
              <Button colorScheme="red" onClick={handleDeleteSource} ml={3}>
                Delete Source
              </Button>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialogOverlay>
      </AlertDialog>
    </Box>
  )
}

