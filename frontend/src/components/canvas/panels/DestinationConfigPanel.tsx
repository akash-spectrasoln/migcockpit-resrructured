/**
 * Destination Configuration Panel
 * Inline right-sidebar panel for configuring destination nodes (like Join/Projection).
 * Reads and writes configuration from Zustand store only — no local form state,
 * so values persist when switching between nodes (same behavior as Projection, Join, Filter).
 *
 * - Destination connection (select)
 * - All destination details (host, port, database, schema, db_type, etc.) when selected
 * - Option to create new destination (opens Dashboard/Project in new tab)
 * - Load mode: Insert / Upsert / Replace
 * - For Upsert or Replace: list tables from destination so user can pick existing table
 * - For Insert: input to enter new table name (table will be created)
 */
import React, { useState, useEffect, useCallback } from 'react'
import { useCanvasStore } from '../../../store/canvasStore'
import {
  Box,
  VStack,
  HStack,
  FormControl,
  FormLabel,
  Select,
  Input,
  Button,
  Text,
  useColorModeValue,
  useToast,
  Spinner,
  Alert,
  AlertIcon,
  Divider,
  SimpleGrid,
  Badge,
} from '@chakra-ui/react'
import { Plus, RefreshCw, Server } from 'lucide-react'
import { Node } from 'reactflow'
import { connectionApi } from '../../../services/api'
import { ClientRoutes } from '../../../constants/client-routes'

/** Shape of a destination as returned by the API (no password). */
interface DestinationDetail {
  id?: number
  destination_id?: number
  name?: string
  destination_name?: string
  db_type?: string
  database_type?: string
  hostname?: string
  port?: number | string
  database?: string
  user?: string
  schema?: string
  destination_schema_name?: string
  tenant_db_name?: string
  system_db_name?: string
  s4_schema_name?: string
  mode?: string
  project_id?: number | null
  created_on?: string
  modified_on?: string
  is_active?: boolean
}

interface DestinationConfigPanelProps {
  node: Node | null
  projectId?: number | null
  onUpdate: (nodeId: string, updateData: { config?: Record<string, any>; business_name?: string; node_name?: string; label?: string }) => void
  /** Optional: custom handler when user clicks "Create new destination". If not set, opens Dashboard/Project in new tab. */
  onCreateNewDestination?: () => void
}

const LOAD_MODES = [
  { value: 'insert', label: 'Insert (create new table)' },
  { value: 'upsert', label: 'Upsert (existing table)' },
  { value: 'replace', label: 'Replace (existing table)' },
  { value: 'drop_and_reload', label: 'Drop and reload' },
] as const

function DetailRow({ label, value }: { label: string; value?: string | number | null }) {
  if (value === undefined || value === null || value === '') return null
  return (
    <HStack justify="space-between" spacing={2}>
      <Text as="span" color="gray.500">{label}</Text>
      <Text as="span" noOfLines={1} title={String(value)}>{String(value)}</Text>
    </HStack>
  )
}

export const DestinationConfigPanel: React.FC<DestinationConfigPanelProps> = ({
  node,
  projectId,
  onUpdate,
  onCreateNewDestination,
}) => {
  const nodeId = node?.id ?? null
  // Single source of truth: read node from Zustand so config persists when switching nodes
  const liveNode = useCanvasStore((s) => (nodeId ? s.nodesById[nodeId] : null))
  const displayNode = liveNode ?? node
  const config = displayNode?.data?.config ?? {}
  const updateNode = useCanvasStore((s) => s.updateNode)

  const [destinations, setDestinations] = useState<{ value: string; label: string }[]>([])
  const [destinationList, setDestinationList] = useState<DestinationDetail[]>([])
  const [tables, setTables] = useState<{ value: string; label: string }[]>([])
  const [loadingDestinations, setLoadingDestinations] = useState(false)
  const [loadingTables, setLoadingTables] = useState(false)
  const toast = useToast()

  // Form values derived from store — no local state; switching nodes shows the correct node's config
  const destinationId = config.destinationId != null ? String(config.destinationId) : ''
  const destinationType = config.destinationType === 'customer_database' ? 'customer_database' : 'remote'
  const loadMode = config.loadMode ?? 'insert'
  const tableName = config.tableName ?? ''
  const schema = config.schema ?? 'public'
  const businessName =
    config.businessName ??
    displayNode?.data?.business_name ??
    displayNode?.data?.node_name ??
    displayNode?.data?.label ??
    ''

  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const headerBg = useColorModeValue('gray.50', 'gray.800')
  const detailBg = useColorModeValue('gray.50', 'gray.900')
  const detailBorder = useColorModeValue('gray.200', 'gray.600')

  const selectedDestination = destinationId
    ? (destinationList.find(
        (d) => String(d.id ?? d.destination_id ?? '') === destinationId
      ) ?? null)
    : null
  const displaySchema = selectedDestination?.schema ?? selectedDestination?.destination_schema_name

  const loadDestinations = useCallback(() => {
    const urlParams = new URLSearchParams(window.location.search)
    const projectIdFromUrl = urlParams.get('projectId')
    const pid = projectId ?? (projectIdFromUrl ? parseInt(projectIdFromUrl, 10) : null)
    setLoadingDestinations(true)
    connectionApi.getDestinations(pid && !isNaN(pid) ? pid : undefined)
      .then((res: any) => {
        const raw = res?.data || res
        const list: DestinationDetail[] = Array.isArray(raw) ? raw : (raw?.destinations ?? raw?.data?.destinations ?? [])
        setDestinationList(list)
        const options = list.map((d: DestinationDetail) => ({
          value: String(d.id ?? d.destination_id ?? ''),
          label: `${d.name ?? d.destination_name ?? 'Destination'} (${(d.db_type ?? d.database_type ?? 'N/A')})`,
        }))
        setDestinations(options)
      })
      .catch(() => {
        setDestinations([])
        setDestinationList([])
      })
      .finally(() => setLoadingDestinations(false))
  }, [projectId])

  // Load destinations on mount / projectId change
  useEffect(() => {
    loadDestinations()
  }, [loadDestinations])

  const handleCreateNewDestination = () => {
    if (onCreateNewDestination) {
      onCreateNewDestination()
      return
    }
    const pid = projectId ?? new URLSearchParams(window.location.search).get('projectId')
    const url = pid
      ? ClientRoutes.dashboard.projectDashboard(pid)
      : ClientRoutes.dashboard.root
    window.open(url, '_blank', 'noopener,noreferrer')
    toast({
      title: 'Open destination page',
      description: 'Add a new destination in the new tab, then click Refresh in this panel to see it in the list.',
      status: 'info',
      duration: 6000,
      isClosable: true,
    })
  }

  // Update store on any config or business name change — single source of truth
  const handleConfigChange = useCallback(
    (updates: Partial<{
      destinationId: string | number | null
      destinationType: 'remote' | 'customer_database'
      loadMode: string
      tableName: string
      schema: string
      businessName: string
    }>) => {
      if (!nodeId || !displayNode) return
      const nextType = updates.destinationType ?? config.destinationType ?? 'remote'
      const isCustomerDb = nextType === 'customer_database'
      const nextDestinationId =
        isCustomerDb ? null : (updates.destinationId !== undefined
          ? (typeof updates.destinationId === 'string' ? parseInt(updates.destinationId, 10) || null : updates.destinationId)
          : (config.destinationId != null ? config.destinationId : null))
      const nextConfig = {
        ...config,
        destinationType: nextType,
        destinationId: nextDestinationId,
        loadMode: updates.loadMode ?? config.loadMode ?? 'insert',
        tableName: (updates.tableName ?? config.tableName ?? '').trim(),
        schema: (updates.schema ?? config.schema ?? 'public').trim(),
        ...(updates.businessName !== undefined && { businessName: updates.businessName }),
      }
      const name = updates.businessName ?? businessName
      updateNode(nodeId, {
        data: {
          ...displayNode.data,
          config: nextConfig,
          business_name: name,
          node_name: name,
          label: name,
        },
      })
      onUpdate?.(nodeId, {
        config: nextConfig,
        business_name: name,
        node_name: name,
        label: name,
      })
    },
    [nodeId, displayNode, config, businessName, updateNode, onUpdate]
  )

  // Load tables when destination is selected and load mode is upsert/replace
  const loadTables = useCallback(async (destId: number) => {
    setLoadingTables(true)
    setTables([])
    try {
      const res = await connectionApi.getDestinationTables(destId)
      const raw = res?.data || res
      const list = raw?.tables ?? (Array.isArray(raw) ? raw : [])
      const options = list.map((t: any) => {
        const name = t.table_name ?? t.name ?? t
        return { value: typeof name === 'string' ? name : String(name), label: typeof name === 'string' ? name : String(name) }
      })
      setTables(options)
    } catch {
      setTables([])
    } finally {
      setLoadingTables(false)
    }
  }, [])

  useEffect(() => {
    if (destinationType === 'customer_database') {
      setTables([])
      return
    }
    if (loadMode !== 'upsert' && loadMode !== 'replace' && loadMode !== 'drop_and_reload') {
      setTables([])
      return
    }
    const destId = destinationId ? parseInt(destinationId, 10) : NaN
    if (!destinationId || isNaN(destId)) {
      setTables([])
      return
    }
    loadTables(destId)
  }, [loadMode, destinationId, destinationType, loadTables])

  if (!node || !displayNode) return null

  const isCustomerDb = destinationType === 'customer_database'
  const isUpsertOrReplaceOrDropReload = loadMode === 'upsert' || loadMode === 'replace' || loadMode === 'drop_and_reload'

  return (
    <Box w="100%" h="100%" display="flex" flexDirection="column" overflow="hidden" bg={useColorModeValue('white', 'gray.800')}>
      <Box p={4} borderBottomWidth="1px" borderColor={borderColor} bg={headerBg}>
        <VStack align="stretch" spacing={1}>
          <Text fontSize="lg" fontWeight="semibold">
            Destination
          </Text>
          <Text fontSize="xs" color="gray.500">
            Configure target connection and table. For Insert enter a new table name; for Upsert/Replace select an existing table.
          </Text>
        </VStack>
      </Box>
      <Box flex={1} overflowY="auto" p={4}>
        <VStack align="stretch" spacing={4}>
          {/* Business Name (Editable) — stored in config + data; persists in Zustand */}
          <Box>
            <Text fontSize="xs" fontWeight="medium" mb={1} color={useColorModeValue('gray.600', 'gray.400')}>
              Business Name
            </Text>
            <Input
              size="sm"
              value={businessName}
              onChange={(e) => handleConfigChange({ businessName: e.target.value })}
              placeholder="e.g., Customer Output"
            />
          </Box>

          <FormControl>
            <FormLabel fontSize="sm">Destination Type</FormLabel>
            <Select
              value={destinationType}
              onChange={(e) => handleConfigChange({ destinationType: e.target.value as 'remote' | 'customer_database' })}
            >
              <option value="remote">Remote Destination</option>
              <option value="customer_database">Customer Database</option>
            </Select>
            <Text fontSize="xs" color="gray.500" mt={1}>
              {destinationType === 'customer_database'
                ? 'Write to the same database as your customer (e.g. C00008) — specify schema and table.'
                : 'Use a configured remote destination connection.'}
            </Text>
          </FormControl>

          {destinationType === 'remote' && (
            <>
          <FormControl isRequired>
            <FormLabel fontSize="sm">Destination Connection</FormLabel>
            <Select
              placeholder="Select destination"
              value={destinationId}
              onChange={(e) => handleConfigChange({ destinationId: e.target.value })}
              isDisabled={loadingDestinations}
            >
              {destinations.map((opt) => (
                <option key={opt.value} value={opt.value}>{opt.label}</option>
              ))}
            </Select>
            <HStack mt={2} spacing={2} flexWrap="wrap">
              <Button
                size="xs"
                variant="outline"
                leftIcon={<RefreshCw size={12} />}
                onClick={loadDestinations}
                isDisabled={loadingDestinations}
              >
                Refresh list
              </Button>
              <Button
                size="xs"
                colorScheme="green"
                variant="outline"
                leftIcon={<Plus size={12} />}
                onClick={handleCreateNewDestination}
              >
                Create new destination
              </Button>
            </HStack>
            {loadingDestinations && <Spinner size="sm" mt={2} />}
          </FormControl>

          {!selectedDestination && destinationId && !loadingDestinations && (
            <Alert status="info" size="sm">
              <AlertIcon />
              <Text fontSize="xs">Select a destination above or refresh the list to see full connection details.</Text>
            </Alert>
          )}

          {selectedDestination && (
            <Box
              p={3}
              borderRadius="md"
              bg={detailBg}
              borderWidth="1px"
              borderColor={detailBorder}
              w="100%"
            >
              <HStack spacing={2} mb={2}>
                <Server size={14} />
                <Text fontSize="sm" fontWeight="semibold">
                  Full destination details
                </Text>
                <Badge colorScheme={selectedDestination.db_type === 'postgresql' || selectedDestination.db_type === 'postgres' ? 'blue' : 'purple'}>
                  {selectedDestination.db_type ?? selectedDestination.database_type ?? 'N/A'}
                </Badge>
              </HStack>
              <VStack align="stretch" spacing={2} fontSize="xs">
                <SimpleGrid columns={2} spacingX={3} spacingY={1}>
                  <DetailRow label="Name" value={selectedDestination.name ?? selectedDestination.destination_name} />
                  <DetailRow label="Host" value={selectedDestination.hostname} />
                  <DetailRow label="Port" value={selectedDestination.port != null ? String(selectedDestination.port) : undefined} />
                  <DetailRow label="Database" value={selectedDestination.database ?? selectedDestination.tenant_db_name ?? selectedDestination.system_db_name} />
                  <DetailRow label="Schema (connection)" value={displaySchema ?? selectedDestination.s4_schema_name} />
                  <DetailRow label="User" value={selectedDestination.user} />
                </SimpleGrid>
                {(selectedDestination.tenant_db_name != null || selectedDestination.system_db_name != null || selectedDestination.s4_schema_name != null) && (
                  <>
                    <Divider />
                    <Text fontWeight="medium" color="gray.500">Extra (HANA)</Text>
                    <SimpleGrid columns={2} spacingX={3} spacingY={1}>
                      {selectedDestination.tenant_db_name != null && <DetailRow label="Tenant DB" value={selectedDestination.tenant_db_name} />}
                      {selectedDestination.system_db_name != null && <DetailRow label="System DB" value={selectedDestination.system_db_name} />}
                      {selectedDestination.s4_schema_name != null && <DetailRow label="S4 Schema" value={selectedDestination.s4_schema_name} />}
                    </SimpleGrid>
                  </>
                )}
                <DetailRow label="ID" value={selectedDestination.id ?? selectedDestination.destination_id} />
              </VStack>

              <Divider my={2} />

              <Box>
                <Text fontSize="xs" fontWeight="semibold" color="green.600" _dark={{ color: 'green.400' }} mb={1}>
                  Where to find your new table
                </Text>
                <Text fontSize="xs" color="gray.600" _dark={{ color: 'gray.400' }}>
                  In your DB client (e.g. pgAdmin, DBeaver), connect using <strong>Host</strong> and <strong>Port</strong> above, then open:
                </Text>
                <Box mt={1} p={2} borderRadius="md" bg={useColorModeValue('white', 'gray.800')} borderWidth="1px" borderColor={detailBorder}>
                  <Text fontSize="xs" fontFamily="mono" wordBreak="break-all">
                    Database → <strong>{selectedDestination.database ?? selectedDestination.tenant_db_name ?? selectedDestination.system_db_name ?? '—'}</strong>
                  </Text>
                  <Text fontSize="xs" fontFamily="mono" mt={0.5} wordBreak="break-all">
                    Schema → <strong>{schema?.trim() || displaySchema || 'public'}</strong>
                  </Text>
                  {tableName?.trim() && (
                    <Text fontSize="xs" fontFamily="mono" mt={0.5} wordBreak="break-all">
                      Table → <strong>{tableName.trim()}</strong>
                    </Text>
                  )}
                </Box>
                {loadMode === 'insert' && !tableName?.trim() && (
                  <Text fontSize="xs" color="orange.600" _dark={{ color: 'orange.400' }} mt={1}>
                    Enter a table name below and save so the new table is created here.
                  </Text>
                )}
              </Box>
            </Box>
          )}
            </>
          )}

          {destinationType === 'customer_database' && (
            <Alert status="info" size="sm">
              <AlertIcon />
              <Text fontSize="xs">
                Data will be written to your customer database. Enter schema and table name below.
              </Text>
            </Alert>
          )}

          <FormControl isRequired>
            <FormLabel fontSize="sm">Load Mode</FormLabel>
            <Select
              value={loadMode}
              onChange={(e) => handleConfigChange({ loadMode: e.target.value })}
            >
              {LOAD_MODES.map((m) => (
                <option key={m.value} value={m.value}>{m.label}</option>
              ))}
            </Select>
          </FormControl>

          {isUpsertOrReplaceOrDropReload ? (
            <FormControl isRequired>
              <FormLabel fontSize="sm">
                {loadMode === 'drop_and_reload' ? 'Table to drop and reload' : 'Existing Table'}
              </FormLabel>
              {!isCustomerDb && tables.length > 0 ? (
                <Select
                  placeholder={loadingTables ? 'Loading tables...' : loadMode === 'drop_and_reload' ? 'Select table to drop and reload' : 'Select table'}
                  value={tableName}
                  onChange={(e) => handleConfigChange({ tableName: e.target.value })}
                  isDisabled={loadingTables || !destinationId}
                >
                  {tables.map((opt) => (
                    <option key={opt.value} value={opt.value}>{opt.label}</option>
                  ))}
                </Select>
              ) : (
                <Input
                  placeholder={loadingTables && !isCustomerDb ? 'Loading...' : loadMode === 'drop_and_reload' ? 'Enter table name to drop and reload' : 'Enter existing table name'}
                  value={tableName}
                  onChange={(e) => handleConfigChange({ tableName: e.target.value })}
                  isDisabled={!isCustomerDb && (loadingTables || !destinationId)}
                />
              )}
              {loadMode === 'drop_and_reload' && (
                <Text fontSize="xs" color="orange.600" _dark={{ color: 'orange.400' }} mt={1}>
                  Table will be dropped (if it exists) and recreated with pipeline data.
                </Text>
              )}
              {loadingTables && <Spinner size="sm" mt={2} />}
              {!loadingTables && destinationId && tables.length === 0 && !isCustomerDb && (
                <Alert status="info" size="sm" mt={2}>
                  <AlertIcon />
                  <Text fontSize="xs">No tables returned for this destination. Enter the existing table name above.</Text>
                </Alert>
              )}
            </FormControl>
          ) : (
            <FormControl isRequired>
              <FormLabel fontSize="sm">New Table Name</FormLabel>
              <Input
                placeholder="Enter new table name (table will be created)"
                value={tableName}
                onChange={(e) => handleConfigChange({ tableName: e.target.value })}
              />
            </FormControl>
          )}

          <FormControl>
            <FormLabel fontSize="sm">Schema</FormLabel>
            <Input
              placeholder="e.g. public"
              value={schema}
              onChange={(e) => handleConfigChange({ schema: e.target.value })}
            />
          </FormControl>

          {/* Live updates: no per-node Save button */}
        </VStack>
      </Box>
    </Box>
  )
}
