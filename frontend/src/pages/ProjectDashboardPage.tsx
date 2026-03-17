/**
 * Project Dashboard Page - Shows project-specific sources, destinations, and canvases
 * Replaces the current dashboard when working within a project context
 */
import React from 'react'
import {
  Box,
  Flex,
  Heading,
  Text,
  Button,
  HStack,
  VStack,
  SimpleGrid,
  Card,
  CardHeader,
  CardBody,
  Badge,
  Spinner,
  useColorModeValue,
  useDisclosure,
  Drawer,
  DrawerOverlay,
  DrawerContent,
  DrawerHeader,
  DrawerBody,
  DrawerCloseButton,
  FormControl,
  FormLabel,
  Input,
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalBody,
  ModalCloseButton,
  Grid,
  GridItem,
  Radio,
  RadioGroup,
  Stack,
  Select,
} from '@chakra-ui/react'
import { useParams, useNavigate } from 'react-router-dom'
import { Database, ArrowRight, Plus, ArrowLeft, Edit } from 'lucide-react'
import { useAuthStore } from '../store/authStore'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { connectionApi, api, canvasApi, projectApi } from '../services/api'
import { useCanvases, useDeleteCanvas } from '../hooks/useCanvas'

type DatabaseType = 'postgresql' | 'mysql' | 'sqlserver' | 'oracle'
type DestinationType = 'hana' | 'postgresql' | 'mysql' | 'sqlserver' | 'oracle'

interface SourceType {
  value: DatabaseType
  label: string
  defaultPort: number
}

interface DestinationTypeConfig {
  value: DestinationType
  label: string
  defaultPort: number
}

const destinationTypes: DestinationTypeConfig[] = [
  { value: 'hana', label: 'SAP HANA', defaultPort: 30015 },
  { value: 'postgresql', label: 'PostgreSQL', defaultPort: 5432 },
]

const sourceTypes: SourceType[] = [
  { value: 'postgresql', label: 'PostgreSQL', defaultPort: 5432 },
  { value: 'mysql', label: 'MySQL', defaultPort: 3306 },
  { value: 'sqlserver', label: 'SQL Server', defaultPort: 1433 },
  { value: 'oracle', label: 'Oracle', defaultPort: 1521 },
]

export const ProjectDashboardPage: React.FC = () => {
  const { projectId } = useParams<{ projectId: string }>()
  const navigate = useNavigate()
  const { logout } = useAuthStore()
  const queryClient = useQueryClient()
  const projectIdNum = projectId ? parseInt(projectId, 10) : null

  const bg = useColorModeValue('gray.50', 'gray.900')
  const cardBg = useColorModeValue('white', 'gray.800')
  const textColor = useColorModeValue('gray.800', 'white')
  const subtextColor = useColorModeValue('gray.600', 'gray.300')

  // Fetch project details
  const { data: projectData, isLoading: isProjectLoading } = useQuery({
    queryKey: ['project', projectIdNum],
    queryFn: () => projectApi.get(projectIdNum!),
    enabled: !!projectIdNum,
  })

  // Fetch project-specific canvases
  const { data: canvasesData, isLoading: isCanvasesLoading } = useQuery({
    queryKey: ['canvases', projectIdNum],
    queryFn: () => canvasApi.list(projectIdNum!),
    enabled: !!projectIdNum,
  })

  // Fetch sources filtered by project_id
  const { data: sourcesData, isLoading: isSourcesLoading } = useQuery({
    queryKey: ['sources', projectIdNum],
    queryFn: async () => {
      const response = await connectionApi.getSources(projectIdNum!)
      return response.data
    },
    enabled: !!projectIdNum,
  })

  // Fetch destinations filtered by project_id
  const { data: destinationsData, isLoading: isDestinationsLoading } = useQuery({
    queryKey: ['destinations', projectIdNum],
    queryFn: () => connectionApi.getDestinations(projectIdNum!),
    enabled: !!projectIdNum,
  })

  const canvases = canvasesData?.data || []
  const deleteCanvasMutation = useDeleteCanvas()
  const sources = sourcesData?.sources || (sourcesData?.data?.sources) || (Array.isArray(sourcesData?.data) ? sourcesData.data : []) || []

  // Source creation state (same as DashboardPageChakra)
  const {
    isOpen: isSourceTypeModalOpen,
    onOpen: onOpenSourceTypeModal,
    onClose: onCloseSourceTypeModal,
  } = useDisclosure()

  const {
    isOpen: isSourceDrawerOpen,
    onOpen: onOpenSourceDrawer,
    onClose: onCloseSourceDrawer,
  } = useDisclosure()

  const [selectedSourceType, setSelectedSourceType] = React.useState<DatabaseType | null>(null)
  const [sourceName, setSourceName] = React.useState('')
  const [host, setHost] = React.useState('localhost')
  const [port, setPort] = React.useState<number>(5432)
  const [user, setUser] = React.useState('')
  const [password, setPassword] = React.useState('')
  const [schema, setSchema] = React.useState('')
  const [database, setDatabase] = React.useState('')
  const [serviceName, setServiceName] = React.useState('')
  const [isCreatingSource, setIsCreatingSource] = React.useState(false)
  const [createError, setCreateError] = React.useState<string | null>(null)
  const [editingSourceId, setEditingSourceId] = React.useState<number | null>(null)

  // Destination creation state
  const {
    isOpen: isDestinationTypeModalOpen,
    onOpen: onOpenDestinationTypeModal,
    onClose: onCloseDestinationTypeModal,
  } = useDisclosure()

  const {
    isOpen: isDestinationDrawerOpen,
    onOpen: onOpenDestinationDrawer,
    onClose: onCloseDestinationDrawer,
  } = useDisclosure()

  const [selectedDestinationType, setSelectedDestinationType] = React.useState<DestinationType | null>(null)
  const [destinationName, setDestinationName] = React.useState('')
  const [destHost, setDestHost] = React.useState('localhost')
  
  // HANA-specific fields
  const [instanceNumber, setInstanceNumber] = React.useState('')
  const [hanaMode, setHanaMode] = React.useState('single_container')
  const [hanaDbType, setHanaDbType] = React.useState('tenant_database')
  const [tenantDbName, setTenantDbName] = React.useState('')
  const [systemDbName, setSystemDbName] = React.useState('')
  const [s4Schema, setS4Schema] = React.useState('')
  
  // PostgreSQL/MySQL/SQL Server/Oracle fields
  const [destPort, setDestPort] = React.useState('')
  const [destDatabase, setDestDatabase] = React.useState('')
  const [destUser, setDestUser] = React.useState('')
  const [destPassword, setDestPassword] = React.useState('')
  
  // Common fields
  const [destSchema, setDestSchema] = React.useState('')
  const [isCreatingDestination, setIsCreatingDestination] = React.useState(false)
  const [createDestError, setCreateDestError] = React.useState<string | null>(null)

  const handleOpenCanvas = (canvasId?: number, sourceId?: number) => {
    if (canvasId) {
      navigate(`/canvas?canvasId=${canvasId}&projectId=${projectId}`)
    } else if (sourceId) {
      navigate(`/canvas?sourceId=${sourceId}&projectId=${projectId}`)
    } else {
      navigate(`/canvas?projectId=${projectId}`)
    }
  }

  const resetSourceForm = () => {
    setSourceName('')
    setHost('localhost')
    setPort(5432)
    setUser('')
    setPassword('')
    setSchema('')
    setDatabase('')
    setServiceName('')
    setSelectedSourceType(null)
    setCreateError(null)
    setEditingSourceId(null)
  }

  const handleCreateSource = async () => {
    if (!selectedSourceType) {
      setCreateError('Please select a source type first')
      return
    }

    setIsCreatingSource(true)
    setCreateError(null)

    if (!sourceName || !host || !port || !user || !password) {
      setCreateError('Please fill in all required fields')
      setIsCreatingSource(false)
      return
    }

    try {
      const payload: any = {
        source_name: sourceName,
        db_type: selectedSourceType,
        hostname: host,
        port: port,
        user: user,
        password: password,
        project_id: projectIdNum, // Include project_id
      }

      if (schema) payload.schema = schema
      if (database) payload.database = database
      if (selectedSourceType === 'oracle' && serviceName) {
        payload.service_name = serviceName
      }

      await api.post('/api/sources-connection/', payload)
      queryClient.invalidateQueries({ queryKey: ['sources'] })

      // Reset form
      resetSourceForm()

      setTimeout(() => {
        onCloseSourceDrawer()
      }, 500)
    } catch (err: any) {
      let msg: any =
        err.response?.data?.error ||
        err.response?.data?.detail ||
        err.message ||
        'Failed to create source'

      if (typeof msg === 'object') {
        try {
          msg = JSON.stringify(msg)
        } catch {
          msg = 'Failed to create source'
        }
      }

      setCreateError(msg)
    } finally {
      setIsCreatingSource(false)
    }
  }

  const handleEditSourceClick = async (sourceId: number) => {
    try {
      setCreateError(null)
      setIsCreatingSource(true)
      setEditingSourceId(sourceId)

      const response = await api.get(`/api/api-customer/sources/${sourceId}/edit/`)
      const src = response.data?.source || {}

      setSourceName(src.source_name || '')
      setHost(src.hostname || 'localhost')
      setPort(Number(src.port) || 5432)
      setUser(src.user || '')
      setPassword(src.password || '')
      setSchema(src.schema || '')
      setDatabase(src.database || '')
      setServiceName(src.service_name || '')
      if (src.db_type) {
        setSelectedSourceType(src.db_type as DatabaseType)
      }

      onOpenSourceDrawer()
    } catch (err: any) {
      let msg: any =
        err.response?.data?.error ||
        err.response?.data?.detail ||
        err.message ||
        'Failed to load source for editing'

      if (typeof msg === 'object') {
        try {
          msg = JSON.stringify(msg)
        } catch {
          msg = 'Failed to load source for editing'
        }
      }

      setCreateError(msg)
      setEditingSourceId(null)
    } finally {
      setIsCreatingSource(false)
    }
  }

  const handleSaveSource = async () => {
    if (!editingSourceId) {
      return
    }

    setIsCreatingSource(true)
    setCreateError(null)

    if (!sourceName || !host || !port || !user || !password) {
      setCreateError('Please fill in all required fields (Source Name, Host, Port, Username, Password)')
      setIsCreatingSource(false)
      return
    }

    try {
      const payload: any = {
        source_name: sourceName,
        hostname: host,
        port: port,
        user: user,
        password: password,
      }

      if (schema) payload.schema = schema
      if (database) payload.database = database
      if (selectedSourceType === 'oracle' && serviceName) {
        payload.service_name = serviceName
      }
      if (selectedSourceType) {
        payload.db_type = selectedSourceType
      }

      await api.put(`/api/api-customer/sources/${editingSourceId}/edit/`, payload)
      queryClient.invalidateQueries({ queryKey: ['sources'] })

      resetSourceForm()
      setTimeout(() => {
        onCloseSourceDrawer()
      }, 500)
    } catch (err: any) {
      let msg: any =
        err.response?.data?.error ||
        err.response?.data?.detail ||
        err.message ||
        'Failed to update source'

      if (typeof msg === 'object') {
        try {
          msg = JSON.stringify(msg)
        } catch {
          msg = 'Failed to update source'
        }
      }

      setCreateError(msg)
    } finally {
      setIsCreatingSource(false)
    }
  }

  const handleCreateDestination = async () => {
    if (!selectedDestinationType) {
      setCreateDestError('Please select a destination type first')
      return
    }

    setIsCreatingDestination(true)
    setCreateDestError(null)

    // Validate based on destination type
    if (selectedDestinationType === 'hana') {
      if (!destinationName || !destHost || !instanceNumber || !destSchema || !s4Schema) {
        setCreateDestError('Please fill in all required fields')
        setIsCreatingDestination(false)
        return
      }
    } else if (selectedDestinationType === 'postgresql' || selectedDestinationType === 'mysql' || 
               selectedDestinationType === 'sqlserver' || selectedDestinationType === 'oracle') {
      if (!destinationName || !destHost || !destPort || !destDatabase || !destUser || !destPassword || !destSchema) {
        setCreateDestError('Please fill in all required fields')
        setIsCreatingDestination(false)
        return
      }
    } else {
      setCreateDestError('Please select a destination type')
      setIsCreatingDestination(false)
      return
    }

    try {
      const payload: any = {
        destination_name: destinationName,
        db_type: selectedDestinationType,
        hostname: destHost,
        destination_schema_name: destSchema,
        project_id: projectIdNum, // Include project_id
      }

      if (selectedDestinationType === 'hana') {
        // HANA-specific fields
        payload.instance_number = parseInt(instanceNumber, 10)
        payload.mode = hanaMode
        payload.s4_schema_name = s4Schema
        
        if (hanaMode === 'multiple_containers') {
          payload.database_type = hanaDbType
          if (hanaDbType === 'tenant_database') payload.tenant_db_name = tenantDbName
          if (hanaDbType === 'system_database') payload.system_db_name = systemDbName
        }
      } else {
        // PostgreSQL/MySQL/SQL Server/Oracle fields
        payload.port = parseInt(destPort, 10)
        payload.database = destDatabase
        payload.user = destUser
        payload.password = destPassword
      }

      await connectionApi.createDestination(payload)
      queryClient.invalidateQueries({ queryKey: ['destinations'] })
      
      // Reset form
      setDestinationName('')
      setDestHost('localhost')
      setInstanceNumber('')
      setHanaMode('single_container')
      setHanaDbType('tenant_database')
      setTenantDbName('')
      setSystemDbName('')
      setDestSchema('')
      setS4Schema('')
      setDestPort('')
      setDestDatabase('')
      setDestUser('')
      setDestPassword('')
      setSelectedDestinationType(null)
      setCreateDestError(null)
      
      setTimeout(() => {
        onCloseDestinationDrawer()
      }, 500)
    } catch (err: any) {
      const data = err.response?.data
      let msg = data?.error || err.message || 'Failed to create destination'
      if (data && typeof data === 'object' && !data.error && Object.keys(data).length) {
        msg = Object.entries(data)
          .map(([k, v]) => `${k}: ${Array.isArray(v) ? v.join(', ') : String(v)}`)
          .join('; ')
      }
      setCreateDestError(msg)
    } finally {
      setIsCreatingDestination(false)
    }
  }

  if (isProjectLoading) {
    return (
      <Box w="100%" h="100vh" bg={bg} display="flex" justify="center" align="center">
        <Spinner size="xl" />
      </Box>
    )
  }

  const project = projectData?.data

  if (!project) {
    return (
      <Box w="100%" h="100vh" bg={bg} display="flex" justify="center" align="center">
        <VStack spacing={4}>
          <Text color={textColor}>Project not found</Text>
          <Button onClick={() => navigate('/projects')}>Back to Projects</Button>
        </VStack>
      </Box>
    )
  }

  return (
    <Box w="100%" h="100vh" bg={bg} display="flex" flexDirection="column">
      {/* Header */}
      <Box
        px={8}
        py={4}
        borderBottomWidth="1px"
        borderColor={useColorModeValue('gray.200', 'gray.700')}
        bg={useColorModeValue('white', 'gray.800')}
      >
        <Flex justify="space-between" align="center">
          <VStack align="flex-start" spacing={0}>
            <HStack spacing={2}>
              <Button
                variant="ghost"
                size="sm"
                leftIcon={<ArrowLeft size={16} />}
                onClick={() => navigate('/projects')}
              >
                Projects
              </Button>
              <Text color={subtextColor}>/</Text>
              <Heading size="md" color={textColor}>
                {project.project_name}
              </Heading>
            </HStack>
            {project.description && (
              <Text fontSize="sm" color={subtextColor}>
                {project.description}
              </Text>
            )}
          </VStack>
          <HStack spacing={3}>
            <Button variant="ghost" leftIcon={<ArrowRight />} onClick={() => handleOpenCanvas()}>
              New Canvas
            </Button>
            <Button variant="outline" onClick={logout}>
              Logout
            </Button>
          </HStack>
        </Flex>
      </Box>

      {/* Content */}
      <Box flex={1} px={8} py={6} overflowY="auto">
        <SimpleGrid columns={{ base: 1, md: 2, lg: 3 }} spacing={6}>
          {/* Sources Card */}
          <Card bg={cardBg} borderWidth="1px" borderColor={useColorModeValue('gray.200', 'gray.700')}>
            <CardHeader>
              <HStack justify="space-between" align="center">
                <HStack>
                  <Database />
                  <Heading size="sm" color={textColor}>
                    Source Connections
                  </Heading>
                </HStack>
                <Button
                  size="xs"
                  colorScheme="brand"
                  leftIcon={<Plus size={14} />}
                  onClick={onOpenSourceTypeModal}
                >
                  Add Source
                </Button>
              </HStack>
            </CardHeader>
            <CardBody>
              {isSourcesLoading ? (
                <HStack spacing={3}>
                  <Spinner size="sm" />
                  <Text color={subtextColor}>Loading sources...</Text>
                </HStack>
              ) : sources.length > 0 ? (
                <VStack align="stretch" spacing={3}>
                  {sources.map((src: any) => {
                    const id = src.source_id || src.id
                    return (
                      <Flex
                        key={id}
                        align="center"
                        justify="space-between"
                        borderWidth="1px"
                        borderRadius="md"
                        px={3}
                        py={3}
                        borderColor={useColorModeValue('gray.200', 'gray.700')}
                        _hover={{
                          borderColor: useColorModeValue('brand.400', 'brand.500'),
                          bg: useColorModeValue('brand.50', 'brand.900'),
                        }}
                        transition="all 0.2s"
                        w="100%"
                      >
                        <Box
                          as="button"
                          onClick={() => handleOpenCanvas(undefined, id)}
                          flex="1"
                          textAlign="left"
                        >
                          <Flex align="center" justify="flex-start">
                            <VStack align="flex-start" spacing={1}>
                              <Text fontSize="sm" fontWeight="medium" color={textColor}>
                                {src.source_name || src.name}
                              </Text>
                              <HStack spacing={2}>
                                <Badge colorScheme="blue" fontSize="xs">
                                  {src.db_type || 'Database'}
                                </Badge>
                                {src.hostname && (
                                  <Text fontSize="xs" color={subtextColor}>
                                    {src.hostname}:{src.port}
                                  </Text>
                                )}
                              </HStack>
                            </VStack>
                          </Flex>
                        </Box>
                        <HStack spacing={2} pl={3}>
                          <Button
                            size="xs"
                            variant="outline"
                            onClick={() => handleEditSourceClick(id)}
                          >
                            Edit
                          </Button>
                          <ArrowRight size={16} color={useColorModeValue('gray.600', 'gray.400')} />
                        </HStack>
                      </Flex>
                    )
                  })}
                </VStack>
              ) : (
                <VStack spacing={4} py={8}>
                  <Database size={48} color={useColorModeValue('gray.400', 'gray.600')} />
                  <Text fontSize="sm" color={subtextColor} textAlign="center">
                    No sources configured yet.
                  </Text>
                  <Button
                    size="sm"
                    colorScheme="brand"
                    leftIcon={<Plus size={16} />}
                    onClick={onOpenSourceTypeModal}
                  >
                    Connect Your First Source
                  </Button>
                </VStack>
              )}
            </CardBody>
          </Card>

          {/* Destinations Card */}
          <Card bg={cardBg} borderWidth="1px" borderColor={useColorModeValue('gray.200', 'gray.700')}>
            <CardHeader>
              <HStack justify="space-between" align="center">
                <HStack>
                  <Database />
                  <Heading size="sm" color={textColor}>
                    Destination Connections
                  </Heading>
                </HStack>
                <Button
                  size="xs"
                  colorScheme="brand"
                  leftIcon={<Plus size={14} />}
                  onClick={onOpenDestinationTypeModal}
                >
                  Add Destination
                </Button>
              </HStack>
            </CardHeader>
            <CardBody>
              {isDestinationsLoading ? (
                <HStack spacing={3}>
                  <Spinner size="sm" />
                  <Text color={subtextColor}>Loading destinations...</Text>
                </HStack>
              ) : destinationsData?.data?.length ? (
                <VStack align="stretch" spacing={3}>
                  {destinationsData.data.map((dest: any) => (
                    <Flex
                      key={dest.id}
                      align="center"
                      justify="space-between"
                      borderWidth="1px"
                      borderRadius="md"
                      px={3}
                      py={2}
                      borderColor={useColorModeValue('gray.200', 'gray.700')}
                    >
                      <VStack align="flex-start" spacing={0}>
                        <Text fontSize="sm" fontWeight="medium" color={textColor}>
                          {dest.destination_name || dest.name}
                        </Text>
                        <Text fontSize="xs" color={subtextColor}>
                          {dest.database_type || 'HANA'}
                        </Text>
                      </VStack>
                      <Button size="xs" variant="outline" onClick={() => handleOpenCanvas()}>
                        Use in Canvas
                      </Button>
                    </Flex>
                  ))}
                </VStack>
              ) : (
                <VStack spacing={4} py={8}>
                  <Database size={48} color={useColorModeValue('gray.400', 'gray.600')} />
                  <Text fontSize="sm" color={subtextColor} textAlign="center">
                    No destinations configured yet.
                  </Text>
                  <Button
                    size="sm"
                    colorScheme="brand"
                    leftIcon={<Plus size={16} />}
                    onClick={onOpenDestinationTypeModal}
                  >
                    Connect Your First Destination
                  </Button>
                </VStack>
              )}
            </CardBody>
          </Card>

          {/* Canvases Card */}
          <Card bg={cardBg} borderWidth="1px" borderColor={useColorModeValue('gray.200', 'gray.700')}>
            <CardHeader>
              <HStack justify="space-between" align="center">
                <HStack>
                  <ArrowRight />
                  <Heading size="sm" color={textColor}>
                    Canvases
                  </Heading>
                </HStack>
                <Button
                  size="sm"
                  colorScheme="brand"
                  leftIcon={<Plus size={16} />}
                  onClick={() => handleOpenCanvas()}
                >
                  New Canvas
                </Button>
              </HStack>
            </CardHeader>
            <CardBody>
              {isCanvasesLoading ? (
                <HStack spacing={3}>
                  <Spinner size="sm" />
                  <Text color={subtextColor}>Loading canvases...</Text>
                </HStack>
              ) : canvases.length > 0 ? (
                <VStack align="stretch" spacing={3}>
                  {canvases.map((canvas: any) => (
                    <Box
                      key={canvas.id}
                      as="button"
                      onClick={() => handleOpenCanvas(canvas.id)}
                      borderWidth="1px"
                      borderRadius="md"
                      px={3}
                      py={3}
                      borderColor={useColorModeValue('gray.200', 'gray.700')}
                      _hover={{
                        borderColor: useColorModeValue('brand.400', 'brand.500'),
                        bg: useColorModeValue('brand.50', 'brand.900'),
                        cursor: 'pointer',
                      }}
                      transition="all 0.2s"
                      textAlign="left"
                      w="100%"
                    >
                      <Flex align="center" justify="space-between">
                        <VStack align="flex-start" spacing={0}>
                          <Text fontSize="sm" fontWeight="medium" color={textColor}>
                            {canvas.name}
                          </Text>
                          <Text fontSize="xs" color={subtextColor}>
                            {canvas.created_on ? new Date(canvas.created_on).toLocaleDateString() : 'No date'}
                          </Text>
                        </VStack>
                        <Button
                          size="xs"
                          variant="outline"
                          onClick={(e) => {
                            e.stopPropagation()
                            handleOpenCanvas(canvas.id)
                          }}
                        >
                          Open
                        </Button>
                      </Flex>
                    </Box>
                  ))}
                </VStack>
              ) : (
                <VStack spacing={4} py={8}>
                  <ArrowRight size={48} color={useColorModeValue('gray.400', 'gray.600')} />
                  <Text fontSize="sm" color={subtextColor} textAlign="center">
                    No canvases created yet.
                  </Text>
                  <Button
                    size="sm"
                    colorScheme="brand"
                    leftIcon={<Plus size={16} />}
                    onClick={() => handleOpenCanvas()}
                  >
                    Create Your First Canvas
                  </Button>
                </VStack>
              )}
            </CardBody>
          </Card>
        </SimpleGrid>
      </Box>

      {/* Source Type Selection Modal - Same as DashboardPageChakra */}
      <Modal isOpen={isSourceTypeModalOpen} onClose={onCloseSourceTypeModal} size="lg">
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Select Source Type</ModalHeader>
          <ModalCloseButton />
          <ModalBody pb={6}>
            <Grid templateColumns="repeat(2, 1fr)" gap={4}>
              {sourceTypes.map((type) => (
                <GridItem key={type.value}>
                  <Box
                    as="button"
                    onClick={() => {
                      setSelectedSourceType(type.value)
                      setPort(type.defaultPort)
                      onCloseSourceTypeModal()
                      onOpenSourceDrawer()
                    }}
                    w="100%"
                    p={4}
                    borderWidth="2px"
                    borderRadius="md"
                    borderColor={useColorModeValue('gray.200', 'gray.700')}
                    _hover={{
                      borderColor: 'brand.400',
                      bg: useColorModeValue('brand.50', 'brand.900'),
                    }}
                    transition="all 0.2s"
                  >
                    <VStack spacing={2}>
                      <Database size={32} />
                      <Text fontWeight="semibold" fontSize="md">
                        {type.label}
                      </Text>
                      <Text fontSize="xs" color={subtextColor}>
                        Default Port: {type.defaultPort}
                      </Text>
                    </VStack>
                  </Box>
                </GridItem>
              ))}
            </Grid>
          </ModalBody>
        </ModalContent>
      </Modal>

      {/* Source Connection Setup Drawer - Same as DashboardPageChakra */}
      <Drawer
        isOpen={isSourceDrawerOpen}
        placement="right"
        onClose={onCloseSourceDrawer}
        size="sm"
      >
        <DrawerOverlay />
        <DrawerContent>
          <DrawerCloseButton />
          <DrawerHeader>
            {editingSourceId
              ? 'Edit Source Connection'
              : selectedSourceType
                ? `${sourceTypes.find(t => t.value === selectedSourceType)?.label} Source Connection`
                : 'Source Connection'}
          </DrawerHeader>
          <DrawerBody>
            <VStack align="stretch" spacing={4}>
              <FormControl isRequired>
                <FormLabel>Source Name</FormLabel>
                <Input
                  value={sourceName}
                  onChange={(e) => setSourceName(e.target.value)}
                  placeholder="Enter a name for this connection"
                />
              </FormControl>

              <FormControl isRequired>
                <FormLabel>Host</FormLabel>
                <Input
                  value={host}
                  onChange={(e) => setHost(e.target.value)}
                  placeholder="localhost"
                />
              </FormControl>

              <FormControl isRequired>
                <FormLabel>Port</FormLabel>
                <Input
                  type="number"
                  value={port}
                  onChange={(e) => setPort(Number(e.target.value) || 0)}
                  placeholder="5432"
                />
              </FormControl>

              <FormControl isRequired>
                <FormLabel>Username</FormLabel>
                <Input
                  value={user}
                  onChange={(e) => setUser(e.target.value)}
                  placeholder="Database username"
                />
              </FormControl>

              <FormControl isRequired>
                <FormLabel>Password</FormLabel>
                <Input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="Database password"
                />
              </FormControl>

              {selectedSourceType !== 'oracle' && (
                <>
                  <FormControl>
                    <FormLabel>Database</FormLabel>
                    <Input
                      value={database}
                      onChange={(e) => setDatabase(e.target.value)}
                      placeholder="Database name (optional)"
                    />
                  </FormControl>

                  <FormControl>
                    <FormLabel>Schema</FormLabel>
                    <Input
                      value={schema}
                      onChange={(e) => setSchema(e.target.value)}
                      placeholder="Schema name (optional)"
                    />
                  </FormControl>
                </>
              )}

              {selectedSourceType === 'oracle' && (
                <FormControl>
                  <FormLabel>Service Name</FormLabel>
                  <Input
                    value={serviceName}
                    onChange={(e) => setServiceName(e.target.value)}
                    placeholder="Oracle service name (optional)"
                  />
                </FormControl>
              )}

              {createError && (
                <Box
                  p={3}
                  borderRadius="md"
                  bg="red.50"
                  borderWidth="1px"
                  borderColor="red.200"
                  fontSize="sm"
                  color="red.700"
                >
                  <Text fontWeight="semibold" mb={1}>
                    Error:
                  </Text>
                  <Text>{createError}</Text>
                </Box>
              )}

              <HStack justify="flex-end" pt={2}>
                <Button variant="ghost" onClick={onCloseSourceDrawer}>
                  Cancel
                </Button>
                <Button
                  colorScheme="brand"
                  onClick={editingSourceId ? handleSaveSource : handleCreateSource}
                  isLoading={isCreatingSource}
                  isDisabled={!selectedSourceType && !editingSourceId}
                >
                  {editingSourceId ? 'Save' : 'Connect'}
                </Button>
              </HStack>
            </VStack>
          </DrawerBody>
        </DrawerContent>
      </Drawer>

      {/* Destination Type Selection Modal - Same as DashboardPageChakra */}
      <Modal isOpen={isDestinationTypeModalOpen} onClose={onCloseDestinationTypeModal} size="lg">
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Select Destination Type</ModalHeader>
          <ModalCloseButton />
          <ModalBody pb={6}>
            <Grid templateColumns="repeat(2, 1fr)" gap={4}>
              {destinationTypes.map((type) => (
                <GridItem key={type.value}>
                  <Box
                    as="button"
                    onClick={() => {
                      setSelectedDestinationType(type.value)
                      onCloseDestinationTypeModal()
                      onOpenDestinationDrawer()
                    }}
                    w="100%"
                    p={4}
                    borderWidth="2px"
                    borderRadius="md"
                    borderColor={useColorModeValue('gray.200', 'gray.700')}
                    _hover={{
                      borderColor: 'brand.400',
                      bg: useColorModeValue('brand.50', 'brand.900'),
                    }}
                    transition="all 0.2s"
                  >
                    <VStack spacing={2}>
                      <Database size={32} />
                      <Text fontWeight="semibold" fontSize="md">
                        {type.label}
                      </Text>
                      <Text fontSize="xs" color={subtextColor}>
                        Default Port: {type.defaultPort}
                      </Text>
                    </VStack>
                  </Box>
                </GridItem>
              ))}
            </Grid>
          </ModalBody>
        </ModalContent>
      </Modal>

      {/* Destination Connection Setup Drawer - Same as DashboardPageChakra */}
      <Drawer
        isOpen={isDestinationDrawerOpen}
        placement="right"
        onClose={onCloseDestinationDrawer}
        size="sm"
      >
        <DrawerOverlay />
        <DrawerContent>
          <DrawerCloseButton />
          <DrawerHeader>
            {selectedDestinationType
              ? `${destinationTypes.find(t => t.value === selectedDestinationType)?.label} Destination Connection`
              : 'Destination Connection'}
          </DrawerHeader>
          <DrawerBody>
            <VStack align="stretch" spacing={4}>
              <FormControl isRequired>
                <FormLabel>Destination Name</FormLabel>
                <Input
                  value={destinationName}
                  onChange={(e) => setDestinationName(e.target.value)}
                  placeholder="Enter a name for this connection"
                />
              </FormControl>

              <FormControl isRequired>
                <FormLabel>Host</FormLabel>
                <Input
                  value={destHost}
                  onChange={(e) => setDestHost(e.target.value)}
                  placeholder="e.g. localhost or 10.0.0.1"
                />
              </FormControl>

              {/* HANA-specific fields */}
              {selectedDestinationType === 'hana' && (
                <>
                  <FormControl isRequired>
                    <FormLabel>Instance Number</FormLabel>
                    <Input
                      type="number"
                      value={instanceNumber}
                      onChange={(e) => setInstanceNumber(e.target.value)}
                      placeholder="e.g. 00"
                      maxLength={2}
                    />
                  </FormControl>

                  <FormControl isRequired>
                    <FormLabel>Mode</FormLabel>
                    <RadioGroup value={hanaMode} onChange={(val: any) => setHanaMode(val)}>
                      <Stack direction="row">
                        <Radio value="single_container">Single Container</Radio>
                        <Radio value="multiple_containers">Multiple Containers</Radio>
                      </Stack>
                    </RadioGroup>
                  </FormControl>

                  {hanaMode === 'multiple_containers' && (
                    <>
                      <FormControl isRequired>
                        <FormLabel>Database Type</FormLabel>
                        <Select value={hanaDbType} onChange={(e: any) => setHanaDbType(e.target.value)}>
                          <option value="tenant_database">Tenant Database</option>
                          <option value="system_database">System Database</option>
                        </Select>
                      </FormControl>

                      {hanaDbType === 'tenant_database' && (
                        <FormControl isRequired>
                          <FormLabel>Tenant Database Name</FormLabel>
                          <Input
                            value={tenantDbName}
                            onChange={(e) => setTenantDbName(e.target.value)}
                            placeholder="Enter tenant DB name"
                          />
                        </FormControl>
                      )}

                      {hanaDbType === 'system_database' && (
                        <FormControl isRequired>
                          <FormLabel>System Database Name</FormLabel>
                          <Input
                            value={systemDbName}
                            onChange={(e) => setSystemDbName(e.target.value)}
                            placeholder="Enter system DB name"
                          />
                        </FormControl>
                      )}
                    </>
                  )}

                  <FormControl isRequired>
                    <FormLabel>S4 Schema Name</FormLabel>
                    <Input
                      value={s4Schema}
                      onChange={(e) => setS4Schema(e.target.value)}
                      placeholder="S4 Schema name"
                    />
                  </FormControl>
                </>
              )}

              {/* PostgreSQL/MySQL/SQL Server/Oracle fields */}
              {(selectedDestinationType === 'postgresql' || selectedDestinationType === 'mysql' || 
                selectedDestinationType === 'sqlserver' || selectedDestinationType === 'oracle') && (
                <>
                  <FormControl isRequired>
                    <FormLabel>Port</FormLabel>
                    <Input
                      type="number"
                      value={destPort}
                      onChange={(e) => setDestPort(e.target.value)}
                      placeholder={`e.g. ${destinationTypes.find(t => t.value === selectedDestinationType)?.defaultPort || 5432}`}
                    />
                  </FormControl>

                  <FormControl isRequired>
                    <FormLabel>Database</FormLabel>
                    <Input
                      value={destDatabase}
                      onChange={(e) => setDestDatabase(e.target.value)}
                      placeholder="Database name"
                    />
                  </FormControl>

                  <FormControl isRequired>
                    <FormLabel>Username</FormLabel>
                    <Input
                      value={destUser}
                      onChange={(e) => setDestUser(e.target.value)}
                      placeholder="Database username"
                    />
                  </FormControl>

                  <FormControl isRequired>
                    <FormLabel>Password</FormLabel>
                    <Input
                      type="password"
                      value={destPassword}
                      onChange={(e) => setDestPassword(e.target.value)}
                      placeholder="Database password"
                    />
                  </FormControl>
                </>
              )}

              <FormControl isRequired>
                <FormLabel>Destination Schema Name</FormLabel>
                <Input
                  value={destSchema}
                  onChange={(e) => setDestSchema(e.target.value)}
                  placeholder="Schema for destination tables"
                />
              </FormControl>

              {createDestError && (
                <Box
                  p={3}
                  borderRadius="md"
                  bg="red.50"
                  borderWidth="1px"
                  borderColor="red.200"
                  fontSize="sm"
                  color="red.700"
                >
                  <Text fontWeight="semibold" mb={1}>
                    Error:
                  </Text>
                  <Text>{createDestError}</Text>
                </Box>
              )}

              <HStack justify="flex-end" pt={2}>
                <Button variant="ghost" onClick={onCloseDestinationDrawer}>
                  Cancel
                </Button>
                <Button
                  colorScheme="brand"
                  onClick={handleCreateDestination}
                  isLoading={isCreatingDestination}
                  isDisabled={!selectedDestinationType}
                >
                  Connect
                </Button>
              </HStack>
            </VStack>
          </DrawerBody>
        </DrawerContent>
      </Drawer>
    </Box>
  )
}

