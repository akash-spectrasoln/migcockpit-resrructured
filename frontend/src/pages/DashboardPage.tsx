/**
 * Dashboard Page - Chakra UI Version
 * After login, user lands here to manage source/destination connections
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
import { useColorModeValue } from '../hooks/useColorModeValue'
import { Database, ArrowRight, Plus, CheckCircle2, XCircle, Lock, Clock } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import { useAuthStore } from '../store/authStore'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { connectionApi, api, canvasApi } from '../services/api'
import { useCanvases, useDeleteCanvas } from '../hooks/useCanvas'
import { DestinationSelectorModal } from '../components/canvas/interactions/DestinationSelectorModal'

type DatabaseType = 'postgresql' | 'mysql' | 'sqlserver' | 'oracle'

interface SourceType {
  value: DatabaseType
  label: string
  defaultPort: number
  icon: string
}

type DestinationType = 'hana' | 'postgresql' | 'mysql' | 'sqlserver' | 'oracle'

interface DestinationTypeConfig {
  value: DestinationType
  label: string
  defaultPort: number
  icon: string
}

const destinationTypes: DestinationTypeConfig[] = [
  { value: 'hana', label: 'SAP HANA', defaultPort: 30015, icon: 'Database' },
  { value: 'postgresql', label: 'PostgreSQL', defaultPort: 5432, icon: 'Database' },
  // { value: 'mysql', label: 'MySQL', defaultPort: 3306, icon: 'Database' },
  // { value: 'sqlserver', label: 'SQL Server', defaultPort: 1433, icon: 'Database' },
  // { value: 'oracle', label: 'Oracle', defaultPort: 1521, icon: 'Database' },
]

const sourceTypes: SourceType[] = [
  { value: 'postgresql', label: 'PostgreSQL', defaultPort: 5432, icon: 'Database' },
  { value: 'mysql', label: 'MySQL', defaultPort: 3306, icon: 'Database' },
  { value: 'sqlserver', label: 'SQL Server', defaultPort: 1433, icon: 'Database' },
  { value: 'oracle', label: 'Oracle', defaultPort: 1521, icon: 'Database' },
]

export const DashboardPage: React.FC = () => {
  const navigate = useNavigate()
  const { logout } = useAuthStore()
  const queryClient = useQueryClient()

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

  // Destination State
  const {
    isOpen: isDestinationSelectorModalOpen,
    onOpen: onOpenDestinationSelectorModal,
    onClose: onCloseDestinationSelectorModal,
  } = useDisclosure()

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
  const [destPort, setDestPort] = React.useState<number>(30015)

  // HANA Specific State
  const [instanceNumber, setInstanceNumber] = React.useState('')
  const [hanaMode, setHanaMode] = React.useState('single_container')
  const [hanaDbType, setHanaDbType] = React.useState('tenant_database')
  const [tenantDbName, setTenantDbName] = React.useState('')
  const [systemDbName, setSystemDbName] = React.useState('')
  const [destSchema, setDestSchema] = React.useState('')
  const [s4Schema, setS4Schema] = React.useState('')

  // PostgreSQL/MySQL/SQL Server/Oracle fields
  const [destDatabase, setDestDatabase] = React.useState('')
  const [destUser, setDestUser] = React.useState('')
  const [destPassword, setDestPassword] = React.useState('')

  const [isCreatingDestination, setIsCreatingDestination] = React.useState(false)
  const [createDestError, setCreateDestError] = React.useState<string | null>(null)

  const bg = useColorModeValue('gray.50', 'gray.900')
  const cardBg = useColorModeValue('white', 'gray.800')
  const textColor = useColorModeValue('gray.800', 'white')
  const subtextColor = useColorModeValue('gray.600', 'gray.300')

  const { data: sourcesData } = useQuery({
    queryKey: ['sources'],
    queryFn: async () => {
      // connectionApi.sources already returns unwrapped data
      return connectionApi.sources()
    },
  })

  const { data: destinationsData } = useQuery({
    queryKey: ['destinations'],
    queryFn: () => connectionApi.destinations(),
  })

  const { data: canvasesData } = useCanvases()
  // canvasApi.list() already returns the unwrapped list of canvases
  const canvases = Array.isArray(canvasesData)
    ? canvasesData
    : (canvasesData as any)?.results || []
  const deleteCanvasMutation = useDeleteCanvas()

  // Handle both response structures: { sources: [...] } or { data: { sources: [...] } }
  const sources = sourcesData?.sources || (sourcesData?.data?.sources) || (Array.isArray(sourcesData?.data) ? sourcesData.data : []) || []

  const handleOpenCanvas = (sourceId?: number) => {
    if (sourceId) {
      navigate(`/canvas?sourceId=${sourceId}`)
    } else {
      navigate('/canvas')
    }
  }

  const handleSelectSourceType = (sourceType: DatabaseType) => {
    setSelectedSourceType(sourceType)
    const typeConfig = sourceTypes.find(t => t.value === sourceType)
    if (typeConfig) {
      setPort(typeConfig.defaultPort)
    }
    onCloseSourceTypeModal()
    onOpenSourceDrawer()
  }

  const resetForm = () => {
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
  }

  const handleCreateSource = async () => {
    if (!selectedSourceType) {
      setCreateError('Please select a source type first')
      return
    }

    setIsCreatingSource(true)
    setCreateError(null)

    // Validate required fields
    if (!sourceName || !host || !port || !user || !password) {
      setCreateError('Please fill in all required fields (Source Name, Host, Port, Username, Password)')
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
      }

      // Add optional fields based on database type
      if (schema) {
        payload.schema = schema
      }
      if (database) {
        payload.database = database
      }
      if (selectedSourceType === 'oracle' && serviceName) {
        payload.service_name = serviceName
      }

      console.log(`Creating ${selectedSourceType} source with:`, {
        ...payload,
        password: '***',
      })

      const response = await api.post('/api/sources-connection/', payload)

      console.log('Source created successfully:', response.data)

      // Refresh sources list
      queryClient.invalidateQueries({ queryKey: ['sources'] })

      // Reset form
      resetForm()

      // Close drawer after a short delay to show success
      setTimeout(() => {
        onCloseSourceDrawer()
      }, 500)
    } catch (err: any) {
      console.error(`Failed to create ${selectedSourceType} source:`, err)
      console.error('Error response:', err.response?.data)
      const msg =
        err.response?.data?.error ||
        err.response?.data?.detail ||
        (err.response?.data ? JSON.stringify(err.response.data) : null) ||
        err.message ||
        `Failed to create ${selectedSourceType} source. Please check the console for details.`
      setCreateError(msg)
    } finally {
      setIsCreatingSource(false)
    }
  }

  const handleSelectDestinationType = (type: DestinationType) => {
    setSelectedDestinationType(type)
    const typeConfig = destinationTypes.find(t => t.value === type)
    if (typeConfig) {
      setDestPort(typeConfig.defaultPort)
    }
    // Reset form fields when switching types
    setInstanceNumber('')
    setHanaMode('single_container')
    setHanaDbType('tenant_database')
    setTenantDbName('')
    setSystemDbName('')
    setDestSchema('')
    setS4Schema('')
    setDestDatabase('')
    setDestUser('')
    setDestPassword('')
    onCloseDestinationTypeModal()
    onOpenDestinationDrawer()
  }

  const resetDestinationForm = () => {
    setDestinationName('')
    setDestHost('localhost')
    setDestPort(30015)
    setInstanceNumber('')
    setHanaMode('single_container')
    setHanaDbType('tenant_database')
    setTenantDbName('')
    setSystemDbName('')
    setDestSchema('')
    setS4Schema('')
    setDestDatabase('')
    setDestUser('')
    setDestPassword('')
    setSelectedDestinationType(null)
    setCreateDestError(null)
  }

  const handleCreateDestination = async () => {
    if (!selectedDestinationType) {
      setCreateDestError('Please select a destination type first')
      return
    }

    setIsCreatingDestination(true)
    setCreateDestError(null)

    // Validate required fields based on destination type
    if (selectedDestinationType === 'hana') {
      if (!destinationName || !destHost || !instanceNumber || !destSchema || !s4Schema) {
        setCreateDestError('Please fill in all required fields for HANA destination')
        setIsCreatingDestination(false)
        return
      }
    } else {
      // PostgreSQL/MySQL/SQL Server/Oracle
      if (!destinationName || !destHost || !destPort || !destDatabase || !destUser || !destPassword) {
        setCreateDestError('Please fill in all required fields (Name, Host, Port, Database, Username, Password)')
        setIsCreatingDestination(false)
        return
      }
    }

    try {
      const payload: any = {
        destination_name: destinationName,
        db_type: selectedDestinationType,
        hostname: destHost,
      }

      if (selectedDestinationType === 'hana') {
        // HANA-specific fields
        payload.instance_number = parseInt(instanceNumber, 10)
        payload.mode = hanaMode
        payload.destination_schema_name = destSchema
        payload.s4_schema_name = s4Schema

        if (hanaMode === 'multiple_containers') {
          payload.database_type = hanaDbType
          if (hanaDbType === 'tenant_database') payload.tenant_db_name = tenantDbName
          if (hanaDbType === 'system_database') payload.system_db_name = systemDbName
        }
      } else {
        // PostgreSQL/MySQL/SQL Server/Oracle fields
        payload.port = destPort ? parseInt(String(destPort), 10) : undefined
        payload.database = destDatabase
        payload.user = destUser
        payload.password = destPassword
      }

      console.log(`Creating ${selectedDestinationType} destination with:`, {
        ...payload,
        password: payload.password ? '***' : undefined,
      })

      const response = await connectionApi.createDestination(payload)

      console.log('Destination created successfully:', response.data)

      queryClient.invalidateQueries({ queryKey: ['destinations'] })

      resetDestinationForm()

      setTimeout(() => {
        onCloseDestinationDrawer()
      }, 500)
    } catch (err: any) {
      console.error(`Failed to create ${selectedDestinationType} destination:`, err)
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

  // Calculate status and last activity
  const hasSource = sources.length > 0
  const hasDestination = destinationsData?.data?.length > 0
  const canDesignFlow = hasSource && hasDestination

  // Get last activity time (most recent modified_on from sources or destinations)
  const getLastActivity = () => {
    // Ensure sources is an array
    const sourcesArray = Array.isArray(sources) ? sources : []
    const sourceTimes = sourcesArray
      .map((s: any) => s.modified_on || s.created_on)
      .filter(Boolean)
      .map((d: string) => new Date(d).getTime())
    
    // Handle different destinations data structures
    let destinationsArray: any[] = []
    if (destinationsData?.data) {
      if (Array.isArray(destinationsData.data)) {
        destinationsArray = destinationsData.data
      } else if (destinationsData.data.destinations && Array.isArray(destinationsData.data.destinations)) {
        destinationsArray = destinationsData.data.destinations
      }
    }
    
    const destTimes = destinationsArray
      .map((d: any) => d.modified_on || d.created_on)
      .filter(Boolean)
      .map((d: string) => new Date(d).getTime())
    
    const allTimes = [...sourceTimes, ...destTimes]
    if (allTimes.length === 0) return null
    const latest = new Date(Math.max(...allTimes))
    const now = new Date()
    const diffHours = Math.floor((now.getTime() - latest.getTime()) / (1000 * 60 * 60))
    if (diffHours < 1) return 'Just now'
    if (diffHours === 1) return '1 hour ago'
    if (diffHours < 24) return `${diffHours} hours ago`
    const diffDays = Math.floor(diffHours / 24)
    if (diffDays === 1) return '1 day ago'
    return `${diffDays} days ago`
  }

  const lastActivity = getLastActivity()

  return (
    <Box w="100%" h="100vh" bg={bg} display="flex" flexDirection="column">
      {/* Header / Status Bar */}
      <Box
        px={8}
        py={4}
        borderBottomWidth="1px"
        borderColor={useColorModeValue('gray.200', 'gray.700')}
        bg={useColorModeValue('white', 'gray.800')}
      >
        <Flex justify="space-between" align="center">
          <VStack align="flex-start" spacing={2}>
            <Heading size="lg" color={textColor}>
              Migration Cockpit
            </Heading>
            <HStack spacing={6}>
              <HStack spacing={2}>
                {hasSource ? (
                  <>
                    <CheckCircle2 size={16} color="green" />
                    <Text fontSize="sm" color={textColor}>
                      {sources.length} Source{sources.length !== 1 ? 's' : ''} Connected
                    </Text>
                  </>
                ) : (
                  <>
                    <XCircle size={16} color="red" />
                    <Text fontSize="sm" color={textColor}>
                      Source Not Connected
                    </Text>
                  </>
                )}
              </HStack>
              <HStack spacing={2}>
                {hasDestination ? (
                  <>
                    <CheckCircle2 size={16} color="green" />
                    <Text fontSize="sm" color={textColor}>
                      {(destinationsData?.data || []).length} Destination{(destinationsData?.data || []).length !== 1 ? 's' : ''} Connected
                    </Text>
                  </>
                ) : (
                  <>
                    <XCircle size={16} color="red" />
                    <Text fontSize="sm" color={textColor}>
                      Destination Not Connected
                    </Text>
                  </>
                )}
              </HStack>
              {lastActivity && (
                <HStack spacing={2}>
                  <Clock size={14} color={useColorModeValue('gray.500', 'gray.400')} />
                  <Text fontSize="sm" color={subtextColor}>
                    Last Activity: {lastActivity}
                  </Text>
                </HStack>
              )}
            </HStack>
          </VStack>
          <HStack spacing={3}>
            <Button
              variant="outline"
              onClick={onOpenSourceTypeModal}
              leftIcon={<Plus size={16} />}
            >
              Add Source
            </Button>
            {!hasDestination && (
              <Button
                colorScheme="brand"
                onClick={onOpenDestinationSelectorModal}
                leftIcon={<Plus size={16} />}
              >
                Add Destination
              </Button>
            )}
            <Button
              colorScheme="brand"
              onClick={() => handleOpenCanvas()}
              leftIcon={<ArrowRight size={16} />}
            >
              Go to Canvas →
            </Button>
            <Button variant="ghost" onClick={logout}>
              Logout
            </Button>
          </HStack>
        </Flex>
      </Box>

      {/* Content */}
      <Box flex={1} px={8} py={6} overflowY="auto">
        <VStack align="stretch" spacing={6}>
          {/* Guided Setup Flow */}
          <Box>
            <Heading size="sm" color={textColor} mb={4}>
              Setup Flow
            </Heading>
            <SimpleGrid columns={{ base: 1, md: 3 }} spacing={4}>
              {/* Step 1: Source */}
              <Card
                bg={cardBg}
                borderWidth="1px"
                borderColor={
                  hasSource
                    ? useColorModeValue('green.200', 'green.700')
                    : useColorModeValue('gray.200', 'gray.700')
                }
              >
                <CardBody>
                  <VStack align="stretch" spacing={3}>
                    <HStack justify="space-between" align="center">
                      <HStack>
                        <Box
                          w={8}
                          h={8}
                          borderRadius="full"
                          bg={hasSource ? 'green.500' : 'gray.300'}
                          color="white"
                          display="flex"
                          alignItems="center"
                          justifyContent="center"
                          fontSize="sm"
                          fontWeight="bold"
                        >
                          1
                        </Box>
                        <Heading size="sm" color={textColor}>
                          Source
                        </Heading>
                      </HStack>
                      {hasSource ? (
                        <CheckCircle2 size={20} color="green" />
                      ) : (
                        <XCircle size={20} color="gray" />
                      )}
                    </HStack>
                    {hasSource ? (
                      <VStack align="stretch" spacing={2}>
                        <VStack align="flex-start" spacing={1} maxH="120px" overflowY="auto">
                          {sources.map((src: any) => (
                            <HStack key={src.source_id || src.id} justify="space-between" w="100%">
                              <Text fontSize="sm" color={textColor}>
                                ✓ {src.source_name || src.name} ({src.db_type || 'Database'})
                              </Text>
                            </HStack>
                          ))}
                        </VStack>
                        <Button
                          size="sm"
                          variant="outline"
                          leftIcon={<Plus size={14} />}
                          onClick={onOpenSourceTypeModal}
                          mt={1}
                        >
                          Add Another Source
                        </Button>
                      </VStack>
                    ) : (
                      <VStack align="stretch" spacing={2}>
                        <Text fontSize="sm" color={subtextColor}>
                          Connect a source database to begin
                        </Text>
                        <Button
                          size="sm"
                          colorScheme="brand"
                          leftIcon={<Plus size={14} />}
                          onClick={onOpenSourceTypeModal}
                        >
                          Connect Source
                        </Button>
                      </VStack>
                    )}
                  </VStack>
                </CardBody>
              </Card>

              {/* Arrow */}
              <Box display={{ base: 'none', md: 'flex' }} alignItems="center" justifyContent="center">
                <ArrowRight size={24} color={useColorModeValue('gray.400', 'gray.500')} />
              </Box>

              {/* Step 2: Destination */}
              <Card
                bg={cardBg}
                borderWidth="1px"
                borderColor={
                  !hasSource
                    ? useColorModeValue('gray.200', 'gray.700')
                    : hasDestination
                    ? useColorModeValue('green.200', 'green.700')
                    : useColorModeValue('yellow.200', 'yellow.700')
                }
                opacity={!hasSource ? 0.6 : 1}
              >
                <CardBody>
                  <VStack align="stretch" spacing={3}>
                    <HStack justify="space-between" align="center">
                      <HStack>
                        <Box
                          w={8}
                          h={8}
                          borderRadius="full"
                          bg={
                            !hasSource
                              ? 'gray.300'
                              : hasDestination
                              ? 'green.500'
                              : 'yellow.500'
                          }
                          color="white"
                          display="flex"
                          alignItems="center"
                          justifyContent="center"
                          fontSize="sm"
                          fontWeight="bold"
                        >
                          2
                        </Box>
                        <Heading size="sm" color={textColor}>
                          Destination
                        </Heading>
                      </HStack>
                      {!hasSource ? (
                        <Lock size={20} color="gray" />
                      ) : hasDestination ? (
                        <CheckCircle2 size={20} color="green" />
                      ) : (
                        <XCircle size={20} color="orange" />
                      )}
                    </HStack>
                    {!hasSource ? (
                      <VStack align="stretch" spacing={2}>
                        <Text fontSize="sm" color={subtextColor}>
                          🔒 Locked - Complete Step 1 first
                        </Text>
                      </VStack>
                    ) : hasDestination ? (
                      <VStack align="stretch" spacing={2}>
                        <VStack align="flex-start" spacing={1} maxH="120px" overflowY="auto">
                          {(destinationsData?.data || []).map((dest: any) => (
                            <Text key={dest.id} fontSize="sm" color={textColor}>
                              ✓ {dest.destination_name || dest.name} ({dest.database_type || dest.db_type || 'HANA'})
                            </Text>
                          ))}
                        </VStack>
                        <Button
                          size="sm"
                          variant="outline"
                          leftIcon={<Plus size={14} />}
                          onClick={onOpenDestinationSelectorModal}
                          mt={1}
                        >
                          Add Another Destination
                        </Button>
                      </VStack>
                    ) : (
                      <VStack align="stretch" spacing={2}>
                        <Text fontSize="sm" color={subtextColor}>
                          Required
                        </Text>
                        <Button
                          size="sm"
                          colorScheme="brand"
                          leftIcon={<Plus size={14} />}
                          onClick={onOpenDestinationSelectorModal}
                        >
                          Add Now
                        </Button>
                      </VStack>
                    )}
                  </VStack>
                </CardBody>
              </Card>

              {/* Arrow */}
              <Box display={{ base: 'none', md: 'flex' }} alignItems="center" justifyContent="center">
                <ArrowRight
                  size={24}
                  color={
                    canDesignFlow
                      ? useColorModeValue('gray.400', 'gray.500')
                      : useColorModeValue('gray.300', 'gray.600')
                  }
                />
              </Box>

              {/* Step 3: Design Flow */}
              <Card
                bg={cardBg}
                borderWidth="1px"
                borderColor={
                  !canDesignFlow
                    ? useColorModeValue('gray.200', 'gray.700')
                    : useColorModeValue('green.200', 'green.700')
                }
                opacity={!canDesignFlow ? 0.6 : 1}
              >
                <CardBody>
                  <VStack align="stretch" spacing={3}>
                    <HStack justify="space-between" align="center">
                      <HStack>
                        <Box
                          w={8}
                          h={8}
                          borderRadius="full"
                          bg={canDesignFlow ? 'green.500' : 'gray.300'}
                          color="white"
                          display="flex"
                          alignItems="center"
                          justifyContent="center"
                          fontSize="sm"
                          fontWeight="bold"
                        >
                          3
                        </Box>
                        <Heading size="sm" color={textColor}>
                          Design Flow
                        </Heading>
                      </HStack>
                      {!canDesignFlow ? (
                        <Lock size={20} color="gray" />
                      ) : (
                        <CheckCircle2 size={20} color="green" />
                      )}
                    </HStack>
                    {!canDesignFlow ? (
                      <VStack align="stretch" spacing={2}>
                        <Text fontSize="sm" color={subtextColor}>
                          🔒 Locked - Complete Steps 1 & 2
                        </Text>
                      </VStack>
                    ) : (
                      <VStack align="stretch" spacing={2}>
                        <Text fontSize="sm" color={textColor}>
                          Ready to design your pipeline
                        </Text>
                        <Button
                          size="sm"
                          colorScheme="brand"
                          leftIcon={<ArrowRight size={14} />}
                          onClick={() => handleOpenCanvas()}
                        >
                          Open Canvas
                        </Button>
                      </VStack>
                    )}
                  </VStack>
                </CardBody>
              </Card>
            </SimpleGrid>
          </Box>

          {/* Saved Canvases Section */}
          <Box>
            <Heading size="sm" color={textColor} mb={4}>
              Your Canvases
            </Heading>
              <Card bg={cardBg} borderWidth="1px" borderColor={useColorModeValue('gray.200', 'gray.700')}>
                <CardBody>
                {canvases.length > 0 ? (
                  <VStack align="stretch" spacing={2}>
                    {canvases.map((canvas: any) => (
                      <Flex
                        key={canvas.id}
                        align="center"
                        justify="space-between"
                        borderWidth="1px"
                        borderRadius="md"
                        px={4}
                        py={3}
                        borderColor={useColorModeValue('gray.200', 'gray.700')}
                        _hover={{
                          borderColor: useColorModeValue('brand.400', 'brand.500'),
                          bg: useColorModeValue('brand.50', 'brand.900'),
                        }}
                        transition="all 0.2s"
                      >
                        <VStack align="flex-start" spacing={0} flex="1">
                          <Text fontSize="sm" fontWeight="medium" color={textColor}>
                            {canvas.name}
                          </Text>
                          <Text fontSize="xs" color={subtextColor}>
                            Updated: {canvas.created_on ? new Date(canvas.created_on).toLocaleDateString('en-US', {
                              month: 'short',
                              day: 'numeric',
                              year: 'numeric'
                            }) : 'No date'}
                          </Text>
                        </VStack>
                        <HStack spacing={2}>
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() => navigate(`/canvas?canvasId=${canvas.id}`)}
                          >
                            Open
                          </Button>
                          <Button
                            size="sm"
                            colorScheme="red"
                            variant="ghost"
                            isLoading={deleteCanvasMutation.isPending}
                            onClick={(e) => {
                              e.stopPropagation()
                              if (window.confirm(`Delete canvas "${canvas.name}"? This cannot be undone.`)) {
                                deleteCanvasMutation.mutate(canvas.id)
                              }
                            }}
                          >
                            Delete
                          </Button>
                        </HStack>
                      </Flex>
                    ))}
                    <Button
                      variant="ghost"
                      leftIcon={<Plus size={16} />}
                      onClick={() => handleOpenCanvas()}
                      mt={2}
                    >
                      + New Canvas
                    </Button>
                  </VStack>
                ) : (
                  <VStack spacing={3} py={6}>
                    <Text fontSize="sm" color={subtextColor} textAlign="center">
                      No canvases saved yet.
                    </Text>
                    <Button
                      size="sm"
                      variant="outline"
                      leftIcon={<Plus size={16} />}
                      onClick={() => handleOpenCanvas()}
                    >
                      Create New Canvas
                    </Button>
                  </VStack>
                )}
              </CardBody>
            </Card>
          </Box>
        </VStack>
      </Box>

      {/* Source Type Selection Modal */}
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
                    onClick={() => handleSelectSourceType(type.value)}
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

      {/* Source Connection Setup Drawer */}
      <Drawer
        isOpen={isSourceDrawerOpen}
        placement="right"
        onClose={() => {
          onCloseSourceDrawer()
          resetForm()
        }}
        size="sm"
      >
        <DrawerOverlay />
        <DrawerContent>
          <DrawerCloseButton />
          <DrawerHeader>
            {selectedSourceType
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
                <FormControl>
                  <FormLabel>Database</FormLabel>
                  <Input
                    value={database}
                    onChange={(e) => setDatabase(e.target.value)}
                    placeholder="Database name (optional)"
                  />
                </FormControl>
              )}

              {selectedSourceType !== 'oracle' && (
                <FormControl>
                  <FormLabel>Schema</FormLabel>
                  <Input
                    value={schema}
                    onChange={(e) => setSchema(e.target.value)}
                    placeholder="Schema name (optional, e.g., public, GENERAL)"
                  />
                </FormControl>
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
                <Button
                  variant="ghost"
                  onClick={() => {
                    onCloseSourceDrawer()
                    resetForm()
                  }}
                >
                  Cancel
                </Button>
                <Button
                  colorScheme="brand"
                  onClick={handleCreateSource}
                  isLoading={isCreatingSource}
                  isDisabled={!selectedSourceType}
                >
                  Connect
                </Button>
              </HStack>
            </VStack>
          </DrawerBody>
        </DrawerContent>
      </Drawer>

      {/* Destination Selector Modal - Shows existing destinations or option to create new */}
      <DestinationSelectorModal
        isOpen={isDestinationSelectorModalOpen}
        onClose={onCloseDestinationSelectorModal}
        onSelect={(destinationId: number) => {
          // If a destination is selected, we could show it's already connected
          // For now, just close the modal and refresh destinations
          onCloseDestinationSelectorModal()
          queryClient.invalidateQueries({ queryKey: ['destinations'] })
        }}
        onCreateNew={() => {
          // Close selector modal and open type selection modal
          onCloseDestinationSelectorModal()
          onOpenDestinationTypeModal()
        }}
      />

      {/* Destination Type Selection Modal */}
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
                    onClick={() => handleSelectDestinationType(type.value)}
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

      {/* Destination Connection Setup Drawer */}
      <Drawer
        isOpen={isDestinationDrawerOpen}
        placement="right"
        onClose={() => {
          onCloseDestinationDrawer()
          resetDestinationForm()
        }}
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
                    <FormLabel>Destination Schema Name</FormLabel>
                    <Input
                      value={destSchema}
                      onChange={(e) => setDestSchema(e.target.value)}
                      placeholder="Schema for destination tables"
                    />
                  </FormControl>

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
                      onChange={(e) => setDestPort(Number(e.target.value) || 0)}
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
                <Button
                  variant="ghost"
                  onClick={() => {
                    onCloseDestinationDrawer()
                    resetDestinationForm()
                  }}
                >
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
    </Box >
  )
}