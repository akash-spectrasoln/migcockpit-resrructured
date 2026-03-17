/**
 * Destination Selector Modal Component
 * Modal for selecting a destination when adding a destination node at the end of a pipeline
 */
import React, { useState, useEffect } from 'react'
import {
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalBody,
  ModalFooter,
  ModalCloseButton,
  Button,
  VStack,
  HStack,
  Text,
  Input,
  InputGroup,
  InputLeftElement,
  Box,
  useColorModeValue,
  Spinner,
  Alert,
  AlertIcon,
} from '@chakra-ui/react'
import { Search, Database, Plus, Server } from 'lucide-react'
import { connectionApi } from '../../../services/api'

interface Destination {
  id: number
  name?: string
  destination_name?: string
  db_type?: string
  mode?: string
  hostname?: string
  database?: string
}

interface DestinationSelectorModalProps {
  isOpen: boolean
  onClose: () => void
  onSelect: (destinationId: number) => void
  /** When user chooses "Customer Database" - write to same DB as customer (e.g. C00008) with schema + table */
  onSelectCustomerDatabase?: () => void
  onCreateNew?: () => void // Optional callback for creating new destination
  projectId?: number | null // When on a project canvas, load project-specific destinations
}

export const DestinationSelectorModal: React.FC<DestinationSelectorModalProps> = ({
  isOpen,
  onClose,
  onSelect,
  onSelectCustomerDatabase,
  onCreateNew,
  projectId,
}) => {
  const [destinations, setDestinations] = useState<Destination[]>([])
  const [filteredDestinations, setFilteredDestinations] = useState<Destination[]>([])
  const [searchQuery, setSearchQuery] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [selectedDestinationId, setSelectedDestinationId] = useState<number | null>(null)

  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const hoverBg = useColorModeValue('gray.50', 'gray.700')
  const textColor = useColorModeValue('gray.800', 'white')

  // Load destinations when modal opens
  useEffect(() => {
    if (isOpen) {
      loadDestinations()
    } else {
      // Reset state when modal closes
      setSearchQuery('')
      setSelectedDestinationId(null)
      setError(null)
    }
  }, [isOpen])

  // Filter destinations based on search query
  useEffect(() => {
    if (!searchQuery.trim()) {
      setFilteredDestinations(destinations)
    } else {
      const query = searchQuery.toLowerCase()
      setFilteredDestinations(
        destinations.filter(
          (dest) =>
            (dest.name || dest.destination_name || '').toLowerCase().includes(query) ||
            (dest.db_type || dest.mode || '').toLowerCase().includes(query) ||
            dest.hostname?.toLowerCase().includes(query) ||
            dest.database?.toLowerCase().includes(query)
        )
      )
    }
  }, [searchQuery, destinations])

  const loadDestinations = async () => {
    setLoading(true)
    setError(null)
    try {
      const response = projectId != null && !isNaN(projectId)
        ? await connectionApi.getDestinations(projectId)
        : await connectionApi.getDestinations()
      // Handle both array response and object with destinations property
      let dests = Array.isArray(response.data) 
        ? response.data 
        : (response.data?.destinations || [])
      
      // Map destination_name to name and determine db_type
      dests = dests.map((dest: any) => ({
        id: dest.id || dest.destination_id,
        name: dest.name || dest.destination_name || `Destination ${dest.id || dest.destination_id}`,
        db_type: dest.db_type || (dest.mode === 'multiple_containers' ? 'hana' : 'hana') || 'hana',
        hostname: dest.hostname,
        database: dest.database || dest.tenant_db_name || dest.system_db_name,
      }))
      
      setDestinations(dests)
      setFilteredDestinations(dests)
    } catch (err: any) {
      console.error('Error loading destinations:', err)
      setError(err.response?.data?.error || 'Failed to load destinations')
      // Set empty array on error so "Create New" option is still available
      setDestinations([])
      setFilteredDestinations([])
    } finally {
      setLoading(false)
    }
  }

  const handleSelect = () => {
    if (selectedDestinationId) {
      onSelect(selectedDestinationId)
      onClose()
    }
  }

  return (
    <Modal isOpen={isOpen} onClose={onClose} size="lg" isCentered>
      <ModalOverlay />
      <ModalContent bg={bg}>
        <ModalHeader>
          <HStack spacing={2}>
            <Database size={20} />
            <Text>Select Destination</Text>
          </HStack>
        </ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          <VStack align="stretch" spacing={4}>
            {/* Search Input */}
            <InputGroup>
              <InputLeftElement pointerEvents="none">
                <Search size={16} />
              </InputLeftElement>
              <Input
                placeholder="Search destinations..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
            </InputGroup>

            {/* Error Message */}
            {error && (
              <Alert status="error" size="sm">
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

            {/* Loading State */}
            {loading && (
              <Box textAlign="center" py={8}>
                <Spinner size="lg" />
                <Text mt={4} color="gray.500">
                  Loading destinations...
                </Text>
              </Box>
            )}

            {/* Destinations List */}
            {!loading && !error && (
              <Box
                maxH="400px"
                overflowY="auto"
                borderWidth="1px"
                borderColor={borderColor}
                borderRadius="md"
              >
                {filteredDestinations.length === 0 ? (
                  <Box p={4} textAlign="center" color="gray.500">
                    {searchQuery ? 'No destinations found matching your search' : 'No destinations available'}
                  </Box>
                ) : (
                  <VStack align="stretch" spacing={0}>
                    {filteredDestinations.map((dest) => (
                      <Box
                        key={dest.id}
                        as="button"
                        w="100%"
                        px={4}
                        py={3}
                        textAlign="left"
                        onClick={() => setSelectedDestinationId(dest.id)}
                        bg={selectedDestinationId === dest.id ? 'blue.50' : 'transparent'}
                        borderLeftWidth={selectedDestinationId === dest.id ? '3px' : '0px'}
                        borderLeftColor={selectedDestinationId === dest.id ? 'blue.500' : 'transparent'}
                        _hover={{ bg: hoverBg }}
                        cursor="pointer"
                        transition="all 0.2s"
                      >
                        <VStack align="stretch" spacing={1}>
                          <HStack justify="space-between">
                            <Text fontWeight="semibold" color={textColor}>
                              {dest.name || dest.destination_name || `Destination ${dest.id}`}
                            </Text>
                            <Text fontSize="xs" color="gray.500" px={2} py={1} bg="gray.100" borderRadius="md">
                              {(dest.db_type || dest.mode || 'HANA').toUpperCase()}
                            </Text>
                          </HStack>
                          {(dest.hostname || dest.database) && (
                            <Text fontSize="xs" color="gray.500">
                              {dest.hostname && dest.database
                                ? `${dest.hostname} / ${dest.database}`
                                : dest.hostname || dest.database}
                            </Text>
                          )}
                        </VStack>
                      </Box>
                    ))}
                  </VStack>
                )}
                {/* Customer Database Option - write to customer DB (e.g. C00008) with schema + table */}
                {onSelectCustomerDatabase && (
                  <Box
                    as="button"
                    w="100%"
                    px={4}
                    py={3}
                    mt={2}
                    textAlign="left"
                    onClick={() => {
                      onSelectCustomerDatabase()
                      onClose()
                    }}
                    bg="blue.50"
                    _dark={{ bg: 'blue.900' }}
                    borderWidth="1px"
                    borderColor="blue.200"
                    _dark={{ borderColor: 'blue.700' }}
                    borderRadius="md"
                    _hover={{ bg: 'blue.100', _dark: { bg: 'blue.800' } }}
                    cursor="pointer"
                    transition="all 0.2s"
                  >
                    <HStack spacing={2}>
                      <Server size={20} color="var(--chakra-colors-blue-600)" />
                      <VStack align="stretch" spacing={0}>
                        <Text fontWeight="semibold" color="blue.700" _dark={{ color: 'blue.300' }}>
                          Customer Database
                        </Text>
                        <Text fontSize="xs" color="gray.600" _dark={{ color: 'gray.400' }}>
                          Write to customer DB (e.g. C00008) — specify schema and table name
                        </Text>
                      </VStack>
                    </HStack>
                  </Box>
                )}
                {/* Create New Destination Option */}
                {onCreateNew && (
                  <Box
                    as="button"
                    w="100%"
                    px={4}
                    py={3}
                    mt={2}
                    textAlign="left"
                    onClick={onCreateNew}
                    bg="green.50"
                    borderWidth="1px"
                    borderColor="green.200"
                    borderRadius="md"
                    _hover={{ bg: 'green.100' }}
                    cursor="pointer"
                    transition="all 0.2s"
                  >
                    <HStack spacing={2}>
                      <Plus size={20} color="green.700" />
                      <Text fontWeight="semibold" color="green.700">
                        Create New Destination
                      </Text>
                    </HStack>
                  </Box>
                )}
              </Box>
            )}
          </VStack>
        </ModalBody>
        <ModalFooter>
          <HStack spacing={2}>
            <Button variant="ghost" onClick={onClose}>
              Cancel
            </Button>
            {onCreateNew && destinations.length === 0 && (
              <Button
                colorScheme="green"
                onClick={onCreateNew}
                leftIcon={<Plus size={16} />}
              >
                Create New Destination
              </Button>
            )}
            <Button
              colorScheme="green"
              onClick={handleSelect}
              isDisabled={!selectedDestinationId || loading}
            >
              Add Destination
            </Button>
          </HStack>
        </ModalFooter>
      </ModalContent>
    </Modal>
  )
}
