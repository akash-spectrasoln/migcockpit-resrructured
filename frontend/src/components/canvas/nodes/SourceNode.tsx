/**
 * Source Node Configuration Component
 * Handles database type selection, credential entry, connection testing, and table selection
 */
import React, { useState, useEffect } from 'react'
import {
  VStack,
  HStack,
  FormControl,
  FormLabel,
  Input,
  Select,
  Button,
  Text,
  Divider,
  Alert,
  AlertIcon,
  AlertTitle,
  AlertDescription,
  Box,
  Badge,
  Spinner,
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  Checkbox,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
  Accordion,
  AccordionItem,
  AccordionButton,
  AccordionPanel,
  AccordionIcon,
  Code,
  IconButton,
} from '@chakra-ui/react'
import { useColorModeValue } from '../../../hooks/useColorModeValue'
import { Database, CheckCircle, XCircle, RefreshCw, Plus, Trash2 } from 'lucide-react'
import { api, connectionApi, metadataApi } from '../../../services/api'

interface SourceNodeConfigProps {
  nodeId: string
  initialConfig?: any
  onSave: (config: any) => void
  onCancel: () => void
}

type DatabaseType = 'mysql' | 'oracle' | 'sqlserver' | 'postgresql'

interface ConnectionCredentials {
  hostname: string
  port: number
  user: string
  password: string
  database?: string
  schema?: string
  serviceName?: string // For Oracle
}

interface TableInfo {
  name: string
  schema?: string
  type?: string
  selected: boolean
}

interface FilterRule {
  id: string
  column: string
  operator: 'equals' | 'not_equals' | 'greater_than' | 'less_than' | 'contains' | 'starts_with' | 'ends_with'
  value: string
}

interface JoinConfig {
  id: string
  type: 'inner' | 'left' | 'right' | 'full'
  leftTable: string
  rightTable: string
  leftColumn: string
  rightColumn: string
}

export const SourceNodeConfig: React.FC<SourceNodeConfigProps> = ({
  nodeId,
  initialConfig,
  onSave,
  onCancel,
}) => {
  const [step, setStep] = useState<'type' | 'credentials' | 'tables' | 'transformations'>('type')
  const [dbType, setDbType] = useState<DatabaseType | ''>('')
  const [credentials, setCredentials] = useState<ConnectionCredentials>({
    hostname: '',
    port: 0,
    user: '',
    password: '',
    database: '',
    schema: '',
  })
  const [testingConnection, setTestingConnection] = useState(false)
  const [connectionError, setConnectionError] = useState('')
  const [connectionSuccess, setConnectionSuccess] = useState(false)
  const [sourceId, setSourceId] = useState<number | null>(null)
  const [tables, setTables] = useState<TableInfo[]>([])
  const [loadingTables, setLoadingTables] = useState(false)
  const [filters, setFilters] = useState<FilterRule[]>([])
  const [joins, setJoins] = useState<JoinConfig[]>([])

  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const textColor = useColorModeValue('gray.800', 'white')

  const databaseTypes: { value: DatabaseType; label: string; defaultPort: number }[] = [
    { value: 'mysql', label: 'MySQL', defaultPort: 3306 },
    { value: 'oracle', label: 'Oracle', defaultPort: 1521 },
    { value: 'sqlserver', label: 'SQL Server', defaultPort: 1433 },
    { value: 'postgresql', label: 'PostgreSQL', defaultPort: 5432 },
  ]

  useEffect(() => {
    if (initialConfig) {
      if (initialConfig.dbType) setDbType(initialConfig.dbType)
      if (initialConfig.credentials) setCredentials(initialConfig.credentials)
      if (initialConfig.sourceId) setSourceId(initialConfig.sourceId)
      if (initialConfig.tables) setTables(initialConfig.tables)
      if (initialConfig.filters) setFilters(initialConfig.filters)
      if (initialConfig.joins) setJoins(initialConfig.joins)
      if (initialConfig.sourceId) {
        setStep('tables')
        setConnectionSuccess(true)
      }
    }
  }, [initialConfig])

  const handleDbTypeSelect = (type: DatabaseType) => {
    setDbType(type)
    const defaultPort = databaseTypes.find((dt) => dt.value === type)?.defaultPort || 0
    setCredentials((prev) => ({ ...prev, port: defaultPort }))
    setStep('credentials')
  }

  const handleCredentialsChange = (field: keyof ConnectionCredentials, value: any) => {
    setCredentials((prev) => ({ ...prev, [field]: value }))
    setConnectionError('')
    setConnectionSuccess(false)
  }

  const testConnection = async () => {
    if (!dbType) return

    setTestingConnection(true)
    setConnectionError('')
    setConnectionSuccess(false)

    try {
      // Create source connection
      const connectionData = {
        source_name: `${dbType}_${Date.now()}`,
        db_type: dbType,
        hostname: credentials.hostname,
        port: credentials.port,
        user: credentials.user,
        password: credentials.password,
        database: credentials.database || '',
        schema: credentials.schema || '',
        service_name: credentials.serviceName || '',
      }

      const response = await api.post('/api/sources-connection/', connectionData)
      
      // The backend returns the source ID in the response
      if (response.data.id) {
        setSourceId(response.data.id)
        setConnectionSuccess(true)
        setStep('tables')
        // Load tables after a short delay to ensure the source is fully created
        setTimeout(() => {
          loadTables(response.data.id)
        }, 500)
      } else {
        // If no ID, try to get it from the sources list
        // This is a fallback in case the response structure is different
        setConnectionError('Connection created but no ID returned. Please refresh and try again.')
        setConnectionSuccess(true) // Still mark as successful
        setStep('tables')
      }
    } catch (error: any) {
      console.error('Connection error:', error)
      setConnectionError(
        error.response?.data?.error || 
        error.response?.data?.details || 
        error.message || 
        'Failed to connect to database'
      )
    } finally {
      setTestingConnection(false)
    }
  }

  const loadTables = async (sourceIdToLoad: number) => {
    setLoadingTables(true)
    try {
      const response = await metadataApi.getTables(sourceIdToLoad)
      // Handle different response structures
      const tablesData = response.data.tables || response.data || []
      const tableList: TableInfo[] = tablesData.map((table: any) => ({
        name: table.name || table.table_name || table,
        schema: table.schema || table.schema_name,
        type: table.type || 'table',
        selected: false,
      }))
      setTables(tableList)
    } catch (error: any) {
      console.error('Failed to load tables:', error)
      setConnectionError('Failed to load tables: ' + (error.message || 'Unknown error'))
    } finally {
      setLoadingTables(false)
    }
  }

  const toggleTableSelection = (tableName: string) => {
    setTables((prev) =>
      prev.map((table) =>
        table.name === tableName ? { ...table, selected: !table.selected } : table
      )
    )
  }

  const addFilter = () => {
    const newFilter: FilterRule = {
      id: `filter_${Date.now()}`,
      column: '',
      operator: 'equals',
      value: '',
    }
    setFilters((prev) => [...prev, newFilter])
  }

  const removeFilter = (id: string) => {
    setFilters((prev) => prev.filter((f) => f.id !== id))
  }

  const updateFilter = (id: string, field: keyof FilterRule, value: any) => {
    setFilters((prev) =>
      prev.map((filter) => (filter.id === id ? { ...filter, [field]: value } : filter))
    )
  }

  const addJoin = () => {
    const newJoin: JoinConfig = {
      id: `join_${Date.now()}`,
      type: 'inner',
      leftTable: '',
      rightTable: '',
      leftColumn: '',
      rightColumn: '',
    }
    setJoins((prev) => [...prev, newJoin])
  }

  const removeJoin = (id: string) => {
    setJoins((prev) => prev.filter((j) => j.id !== id))
  }

  const updateJoin = (id: string, field: keyof JoinConfig, value: any) => {
    setJoins((prev) =>
      prev.map((join) => (join.id === id ? { ...join, [field]: value } : join))
    )
  }

  const handleSave = () => {
    const config = {
      dbType,
      credentials,
      sourceId,
      tables: tables.filter((t) => t.selected),
      filters,
      joins,
    }
    onSave(config)
  }

  const selectedTables = tables.filter((t) => t.selected)

  return (
    <VStack spacing={4} align="stretch" p={4} bg={bg} borderRadius="lg" borderWidth="1px" borderColor={borderColor}>
      {/* Step Indicator */}
      <HStack spacing={2} mb={4}>
        {['type', 'credentials', 'tables', 'transformations'].map((s, idx) => (
          <React.Fragment key={s}>
            <Box
              px={3}
              py={1}
              borderRadius="md"
              bg={step === s ? 'brand.500' : 'gray.200'}
              color={step === s ? 'white' : 'gray.600'}
              fontSize="xs"
              fontWeight="semibold"
            >
              {idx + 1}. {s.charAt(0).toUpperCase() + s.slice(1)}
            </Box>
            {idx < 3 && <Text color="gray.400">→</Text>}
          </React.Fragment>
        ))}
      </HStack>

      {/* Step 1: Database Type Selection */}
      {step === 'type' && (
        <VStack spacing={4} align="stretch">
          <Text fontSize="lg" fontWeight="semibold" color={textColor}>
            Select Database Type
          </Text>
          <VStack spacing={3}>
            {databaseTypes.map((db) => (
              <Button
                key={db.value}
                leftIcon={<Database />}
                variant={dbType === db.value ? 'solid' : 'outline'}
                colorScheme={dbType === db.value ? 'brand' : 'gray'}
                onClick={() => handleDbTypeSelect(db.value)}
                w="100%"
                justifyContent="flex-start"
                h="60px"
              >
                <VStack align="flex-start" spacing={0}>
                  <Text fontWeight="semibold">{db.label}</Text>
                  <Text fontSize="xs" color="gray.500">
                    Default port: {db.defaultPort}
                  </Text>
                </VStack>
              </Button>
            ))}
          </VStack>
        </VStack>
      )}

      {/* Step 2: Credentials */}
      {step === 'credentials' && (
        <VStack spacing={4} align="stretch">
          <HStack justify="space-between">
            <Text fontSize="lg" fontWeight="semibold" color={textColor}>
              Connection Credentials
            </Text>
            <Badge colorScheme="blue" fontSize="sm">
              {databaseTypes.find((dt) => dt.value === dbType)?.label}
            </Badge>
          </HStack>

          <FormControl isRequired>
            <FormLabel>Hostname</FormLabel>
            <Input
              value={credentials.hostname}
              onChange={(e) => handleCredentialsChange('hostname', e.target.value)}
              placeholder="localhost or IP address"
            />
          </FormControl>

          <FormControl isRequired>
            <FormLabel>Port</FormLabel>
            <Input
              type="number"
              value={credentials.port || ''}
              onChange={(e) => handleCredentialsChange('port', parseInt(e.target.value) || 0)}
              placeholder={databaseTypes.find((dt) => dt.value === dbType)?.defaultPort.toString()}
            />
          </FormControl>

          <FormControl isRequired>
            <FormLabel>Username</FormLabel>
            <Input
              value={credentials.user}
              onChange={(e) => handleCredentialsChange('user', e.target.value)}
              placeholder="Database username"
            />
          </FormControl>

          <FormControl isRequired>
            <FormLabel>Password</FormLabel>
            <Input
              type="password"
              value={credentials.password}
              onChange={(e) => handleCredentialsChange('password', e.target.value)}
              placeholder="Database password"
            />
          </FormControl>

          {dbType === 'mysql' || dbType === 'postgresql' ? (
            <FormControl>
              <FormLabel>Database Name</FormLabel>
              <Input
                value={credentials.database || ''}
                onChange={(e) => handleCredentialsChange('database', e.target.value)}
                placeholder="Database name"
              />
            </FormControl>
          ) : null}

          {(dbType === 'oracle' || dbType === 'postgresql' || dbType === 'sqlserver') && (
            <FormControl>
              <FormLabel>Schema</FormLabel>
              <Input
                value={credentials.schema || ''}
                onChange={(e) => handleCredentialsChange('schema', e.target.value)}
                placeholder="Schema name (optional)"
              />
            </FormControl>
          )}

          {dbType === 'oracle' && (
            <FormControl>
              <FormLabel>Service Name / SID</FormLabel>
              <Input
                value={credentials.serviceName || ''}
                onChange={(e) => handleCredentialsChange('serviceName', e.target.value)}
                placeholder="Service name or SID"
              />
            </FormControl>
          )}

          {connectionError && (
            <Alert status="error">
              <AlertIcon />
              <AlertTitle>Connection Failed</AlertTitle>
              <AlertDescription>
                {typeof connectionError === 'string' ? connectionError : (() => {
                  try {
                    return JSON.stringify(connectionError)
                  } catch {
                    return 'An unexpected error occurred while testing the connection'
                  }
                })()}
              </AlertDescription>
            </Alert>
          )}

          {connectionSuccess && (
            <Alert status="success">
              <AlertIcon />
              <AlertTitle>Connection Successful!</AlertTitle>
              <AlertDescription>Successfully connected to the database.</AlertDescription>
            </Alert>
          )}

          <HStack spacing={2}>
            <Button variant="outline" onClick={() => setStep('type')} flex={1}>
              Back
            </Button>
            <Button
              colorScheme="brand"
              onClick={testConnection}
              isLoading={testingConnection}
              loadingText="Testing..."
              leftIcon={connectionSuccess ? <CheckCircle /> : <RefreshCw />}
              flex={1}
            >
              {connectionSuccess ? 'Reconnect' : 'Test Connection'}
            </Button>
          </HStack>
        </VStack>
      )}

      {/* Step 3: Table Selection */}
      {step === 'tables' && (
        <VStack spacing={4} align="stretch">
          <HStack justify="space-between">
            <Text fontSize="lg" fontWeight="semibold" color={textColor}>
              Select Tables
            </Text>
            <Button
              size="sm"
              leftIcon={<RefreshCw />}
              onClick={() => sourceId && loadTables(sourceId)}
              isLoading={loadingTables}
            >
              Refresh
            </Button>
          </HStack>

          {loadingTables ? (
            <Box textAlign="center" py={8}>
              <Spinner size="xl" color="brand.500" />
              <Text mt={4} color="gray.500">
                Loading tables...
              </Text>
            </Box>
          ) : tables.length === 0 ? (
            <Alert status="info">
              <AlertIcon />
              <AlertDescription>No tables found. Please check your connection.</AlertDescription>
            </Alert>
          ) : (
            <Box maxH="400px" overflowY="auto" borderWidth="1px" borderRadius="md" p={2}>
              <Table size="sm" variant="simple">
                <Thead>
                  <Tr>
                    <Th w="50px">
                      <Checkbox />
                    </Th>
                    <Th>Table Name</Th>
                    <Th>Schema</Th>
                    <Th>Type</Th>
                  </Tr>
                </Thead>
                <Tbody>
                  {tables.map((table) => (
                    <Tr key={`${table.schema || ''}.${table.name}`}>
                      <Td>
                        <Checkbox
                          isChecked={table.selected}
                          onChange={() => toggleTableSelection(table.name)}
                        />
                      </Td>
                      <Td fontWeight="medium">{table.name}</Td>
                      <Td color="gray.500">{table.schema || '-'}</Td>
                      <Td>
                        <Badge size="sm">{table.type || 'table'}</Badge>
                      </Td>
                    </Tr>
                  ))}
                </Tbody>
              </Table>
            </Box>
          )}

          {selectedTables.length > 0 && (
            <Alert status="success">
              <AlertIcon />
              <AlertDescription>
                {selectedTables.length} table(s) selected
              </AlertDescription>
            </Alert>
          )}

          <HStack spacing={2}>
            <Button variant="outline" onClick={() => setStep('credentials')} flex={1}>
              Back
            </Button>
            <Button
              colorScheme="brand"
              onClick={() => setStep('transformations')}
              isDisabled={selectedTables.length === 0}
              flex={1}
            >
              Configure Transformations ({selectedTables.length})
            </Button>
          </HStack>
        </VStack>
      )}

      {/* Step 4: Transformations (Filters & Joins) */}
      {step === 'transformations' && (
        <VStack spacing={4} align="stretch">
          <Text fontSize="lg" fontWeight="semibold" color={textColor}>
            Apply Transformations
          </Text>

          <Tabs>
            <TabList>
              <Tab>Filters</Tab>
              <Tab>Joins</Tab>
            </TabList>

            <TabPanels>
              {/* Filters Tab */}
              <TabPanel>
                <VStack spacing={4} align="stretch">
                  <HStack justify="space-between">
                    <Text fontSize="md" fontWeight="medium">
                      Filter Rules
                    </Text>
                    <Button size="sm" leftIcon={<Plus />} onClick={addFilter}>
                      Add Filter
                    </Button>
                  </HStack>

                  {filters.length === 0 ? (
                    <Alert status="info">
                      <AlertIcon />
                      <AlertDescription>
                        No filters applied. Click "Add Filter" to create filter rules.
                      </AlertDescription>
                    </Alert>
                  ) : (
                    <VStack spacing={3} align="stretch">
                      {filters.map((filter) => (
                        <Box
                          key={filter.id}
                          p={4}
                          borderWidth="1px"
                          borderRadius="md"
                          bg={useColorModeValue('gray.50', 'gray.700')}
                        >
                          <HStack spacing={2} mb={3}>
                            <FormControl flex={2}>
                              <FormLabel fontSize="xs">Column</FormLabel>
                              <Input
                                size="sm"
                                value={filter.column}
                                onChange={(e) => updateFilter(filter.id, 'column', e.target.value)}
                                placeholder="Column name"
                              />
                            </FormControl>
                            <FormControl flex={2}>
                              <FormLabel fontSize="xs">Operator</FormLabel>
                              <Select
                                size="sm"
                                value={filter.operator}
                                onChange={(e) =>
                                  updateFilter(filter.id, 'operator', e.target.value as FilterRule['operator'])
                                }
                              >
                                <option value="equals">Equals</option>
                                <option value="not_equals">Not Equals</option>
                                <option value="greater_than">Greater Than</option>
                                <option value="less_than">Less Than</option>
                                <option value="contains">Contains</option>
                                <option value="starts_with">Starts With</option>
                                <option value="ends_with">Ends With</option>
                              </Select>
                            </FormControl>
                            <FormControl flex={2}>
                              <FormLabel fontSize="xs">Value</FormLabel>
                              <Input
                                size="sm"
                                value={filter.value}
                                onChange={(e) => updateFilter(filter.id, 'value', e.target.value)}
                                placeholder="Filter value"
                              />
                            </FormControl>
                            <IconButton
                              aria-label="Remove filter"
                              icon={<Trash2 />}
                              size="sm"
                              colorScheme="red"
                              variant="ghost"
                              onClick={() => removeFilter(filter.id)}
                            />
                          </HStack>
                        </Box>
                      ))}
                    </VStack>
                  )}
                </VStack>
              </TabPanel>

              {/* Joins Tab */}
              <TabPanel>
                <VStack spacing={4} align="stretch">
                  <HStack justify="space-between">
                    <Text fontSize="md" fontWeight="medium">
                      Table Joins
                    </Text>
                    <Button size="sm" leftIcon={<Plus />} onClick={addJoin}>
                      Add Join
                    </Button>
                  </HStack>

                  {joins.length === 0 ? (
                    <Alert status="info">
                      <AlertIcon />
                      <AlertDescription>
                        No joins configured. Click "Add Join" to join multiple tables.
                      </AlertDescription>
                    </Alert>
                  ) : (
                    <VStack spacing={3} align="stretch">
                      {joins.map((join) => (
                        <Box
                          key={join.id}
                          p={4}
                          borderWidth="1px"
                          borderRadius="md"
                          bg={useColorModeValue('gray.50', 'gray.700')}
                        >
                          <HStack spacing={2} mb={3}>
                            <FormControl flex={1}>
                              <FormLabel fontSize="xs">Join Type</FormLabel>
                              <Select
                                size="sm"
                                value={join.type}
                                onChange={(e) =>
                                  updateJoin(join.id, 'type', e.target.value as JoinConfig['type'])
                                }
                              >
                                <option value="inner">INNER JOIN</option>
                                <option value="left">LEFT JOIN</option>
                                <option value="right">RIGHT JOIN</option>
                                <option value="full">FULL OUTER JOIN</option>
                              </Select>
                            </FormControl>
                            <IconButton
                              aria-label="Remove join"
                              icon={<Trash2 />}
                              size="sm"
                              colorScheme="red"
                              variant="ghost"
                              onClick={() => removeJoin(join.id)}
                            />
                          </HStack>
                          <HStack spacing={2}>
                            <FormControl flex={1}>
                              <FormLabel fontSize="xs">Left Table</FormLabel>
                              <Select
                                size="sm"
                                value={join.leftTable}
                                onChange={(e) => updateJoin(join.id, 'leftTable', e.target.value)}
                              >
                                <option value="">Select table...</option>
                                {selectedTables.map((table) => (
                                  <option key={table.name} value={table.name}>
                                    {table.name}
                                  </option>
                                ))}
                              </Select>
                            </FormControl>
                            <FormControl flex={1}>
                              <FormLabel fontSize="xs">Left Column</FormLabel>
                              <Input
                                size="sm"
                                value={join.leftColumn}
                                onChange={(e) => updateJoin(join.id, 'leftColumn', e.target.value)}
                                placeholder="Column name"
                              />
                            </FormControl>
                            <Text>=</Text>
                            <FormControl flex={1}>
                              <FormLabel fontSize="xs">Right Column</FormLabel>
                              <Input
                                size="sm"
                                value={join.rightColumn}
                                onChange={(e) => updateJoin(join.id, 'rightColumn', e.target.value)}
                                placeholder="Column name"
                              />
                            </FormControl>
                            <FormControl flex={1}>
                              <FormLabel fontSize="xs">Right Table</FormLabel>
                              <Select
                                size="sm"
                                value={join.rightTable}
                                onChange={(e) => updateJoin(join.id, 'rightTable', e.target.value)}
                              >
                                <option value="">Select table...</option>
                                {selectedTables.map((table) => (
                                  <option key={table.name} value={table.name}>
                                    {table.name}
                                  </option>
                                ))}
                              </Select>
                            </FormControl>
                          </HStack>
                        </Box>
                      ))}
                    </VStack>
                  )}
                </VStack>
              </TabPanel>
            </TabPanels>
          </Tabs>

          <Divider />

          <HStack spacing={2}>
            <Button variant="outline" onClick={() => setStep('tables')} flex={1}>
              Back
            </Button>
            <Button colorScheme="brand" onClick={handleSave} flex={1}>
              Save Configuration
            </Button>
            <Button variant="ghost" onClick={onCancel}>
              Cancel
            </Button>
          </HStack>
        </VStack>
      )}
    </VStack>
  )
}

