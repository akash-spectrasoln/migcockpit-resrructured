/**
 * Node Types - Chakra UI Version
 * Custom React Flow nodes styled with Chakra UI
 */
import React from 'react'
import { Handle, Position, NodeProps } from 'reactflow'
import { Box, HStack, VStack, Text, Badge, Icon, Tooltip } from '@chakra-ui/react'
import { useColorModeValue } from '../../../hooks/useColorModeValue'
import { Database, Settings, ArrowRight, Filter, GitMerge, Columns, Calculator, BarChart3, Code2, Check, AlertTriangle, RefreshCw } from 'lucide-react'
import { FilterNode } from './FilterNode'
import { JoinNode } from './JoinNode'
import { ProjectionNode } from './ProjectionNode'
import { CalculatedColumnNode } from './CalculatedColumnNode'
import { EditableNodeName } from './EditableNodeName'
// Type definitions (moved from legacy NodeTypes.tsx)
export interface BaseNodeData {
  id: string
  label: string
  node_id?: string
  business_name?: string
  technical_name?: string
  node_name?: string
  node_type?: string
  type: 'source' | 'transform' | 'destination' | 'filter' | 'join' | 'projection' | 'calculated'
  config?: any
  status?: 'idle' | 'running' | 'success' | 'error'
  onNodeNameChange?: (nodeId: string, newName: string) => void
}

export interface SourceNodeData extends BaseNodeData {
  type: 'source'
  sourceId?: number
  sourceName?: string
  connectionType?: 'mysql' | 'oracle' | 'sqlserver' | 'postgresql'
  tableName?: string
  schema?: string
  rowCount?: number
  /** Set by drift detection — lists changes since last canvas save */
  schema_drift?: {
    addedColumns: Array<{ name: string; type: string }>
    removedColumns: Array<{ name: string; type: string }>
    typeChanges: Array<{ name: string; oldType: string; newType: string }>
    summary: string[]
  } | null
  /** Callback to force-refresh schema for this node */
  onRefreshSchema?: (nodeId: string) => void
  config?: {
    sourceId?: number
    tableName?: string
    schema?: string
    isFiltered?: boolean
    conditions?: Array<{
      id: string
      column: string
      operator: string
      value: any
      logicalOperator?: 'AND' | 'OR'
    }>
    expression?: string
    mode?: 'builder' | 'expression'
    columnCount?: number
  }
}

export interface TransformNodeData extends BaseNodeData {
  type: 'transform'
  transformType?: 'map' | 'filter' | 'aggregate' | 'clean'
  rules?: any[]
}

export interface DestinationNodeData extends BaseNodeData {
  type: 'destination'
  destinationId?: number
  destinationName?: string
  connectionType?: 'hana'
}

const nodeIcons = {
  source: Database,
  transform: Settings,
  destination: ArrowRight,
  filter: Filter,
  join: GitMerge,
  projection: Columns,
  calculated: Calculator,
  aggregate: BarChart3,
  compute: Code2,
}

const nodeColors = {
  source: 'blue',
  transform: 'purple',
  destination: 'green',
  filter: 'purple',
  join: 'purple',
  projection: 'purple',
  calculated: 'purple',
  aggregate: 'purple',
  compute: 'purple',
}

const statusColors = {
  idle: 'gray',
  running: 'yellow',
  success: 'green',
  error: 'red',
}

// Deduplicate structured schema errors by (column, type, location)
const dedupeSchemaErrors = (errors: any[] | undefined): any[] => {
  if (!Array.isArray(errors)) return []
  const map = new Map<string, any>()
  for (const e of errors) {
    const key = `${e.column ?? ''}|${e.type ?? ''}|${e.location ?? ''}`
    if (!map.has(key)) {
      map.set(key, e)
    }
  }
  return Array.from(map.values())
}

interface NodeWrapperProps {
  nodeType: 'source' | 'transform' | 'destination'
  data: BaseNodeData
  selected: boolean
  children: React.ReactNode
}

const NodeWrapper: React.FC<NodeWrapperProps> = ({ nodeType, data, selected, children }) => {
  const color = nodeColors[nodeType]
  const bg = useColorModeValue(`${color}.50`, `${color}.900`)
  const borderColor = useColorModeValue(`${color}.300`, `${color}.700`)
  const selectedBorder = useColorModeValue(`${color}.500`, `${color}.400`)
  const status = data?.status || 'idle'
  const successBorder = useColorModeValue('green.400', 'green.500')
  const errorBorder = useColorModeValue('red.400', 'red.500')
  const runningBorder = useColorModeValue('yellow.400', 'yellow.500')
  const resolvedBorder =
    status === 'success' ? successBorder
    : status === 'error' ? errorBorder
    : status === 'running' ? runningBorder
    : selected ? selectedBorder : borderColor
  const successBadgeBg = useColorModeValue('green.50', 'green.900')
  const successBadgeBorder = useColorModeValue('green.200', 'green.700')

  return (
    <Box
      bg={bg}
      borderWidth={status !== 'idle' ? '2px' : '1.5px'}
      borderColor={resolvedBorder}
      borderRadius="md"
      p={2}
      minW="140px"
      maxW="180px"
      boxShadow={
        selected ? 'lg'
          : status === 'success'
            ? '0 0 8px 2px rgba(72, 187, 120, 0.12)'
            : status === 'running'
              ? '0 0 6px 1px rgba(214, 158, 46, 0.15)'
              : 'sm'
      }
      position="relative"
      transition="border-color 0.35s ease-in-out, border-width 0.35s ease-in-out, box-shadow 0.35s ease-in-out"
      className={status === 'success' ? 'node-complete-glow' : undefined}
    >
      {status === 'success' && (
        <Tooltip label="Transformation completed" placement="top" hasArrow>
          <Box
            position="absolute"
            top={0.5}
            right={0.5}
            display="flex"
            alignItems="center"
            justifyContent="center"
            className="node-complete-badge"
            bg={successBadgeBg}
            color="green.600"
            p={1}
            borderRadius="full"
            borderWidth="1.5px"
            borderColor={successBadgeBorder}
            boxShadow="sm"
            zIndex={2}
          >
            <Icon as={Check} w={3.5} h={3.5} />
          </Box>
        </Tooltip>
      )}
      {children}
    </Box>
  )
}

export const SourceNode: React.FC<NodeProps<SourceNodeData>> = ({ data, selected }) => {
  const SourceIcon = nodeIcons.source
  const color = nodeColors.source
  const statusColor = statusColors[data.status || 'idle']
  const textColor = useColorModeValue('gray.800', 'white')
  const subtextColor = useColorModeValue('gray.600', 'gray.400')

  // Get table name from config or data
  const tableName = data.tableName || data.config?.tableName || data.label
  const schema = data.schema || data.config?.schema
  const isFiltered = data.config?.isFiltered || (data.config?.conditions && data.config.conditions.length > 0)
  // Use business_name if available, otherwise fall back to tableName or label
  const displayName = data.business_name || tableName || data.label || 'Source Table'

  return (
    <NodeWrapper nodeType="source" data={data} selected={selected}>
      <VStack align="flex-start" spacing={1}>
        <HStack spacing={1.5} w="100%">
          <Icon as={SourceIcon} color={`${color}.500`} w={4} h={4} />
          <VStack align="flex-start" spacing={0} flex={1} minW={0}>
            <HStack spacing={1} align="center" w="100%">
              {data.onNodeNameChange ? (
                <EditableNodeName
                  value={displayName}
                  onChange={(newName) => {
                    if (data.onNodeNameChange) {
                      data.onNodeNameChange(data.id, newName)
                    }
                  }}
                  fontSize="xs"
                  fontWeight="semibold"
                  color={textColor}
                  isTruncated
                  flex={1}
                />
              ) : (
                <Text fontSize="xs" fontWeight="semibold" color={textColor} isTruncated flex={1}>
                  {displayName}
                </Text>
              )}
              {isFiltered && (
                <Tooltip label="Filtered Source" placement="top">
                  <Box>
                    <Filter size={10} color={`var(--chakra-colors-${color}-500)`} fill={`var(--chakra-colors-${color}-500)`} />
                  </Box>
                </Tooltip>
              )}
              {(data as any).schema_drift && (
                <Tooltip
                  label={
                    <Box maxW="240px">
                      <Text fontWeight="semibold" mb={1}>Schema drift detected:</Text>
                      {((data as any).schema_drift.summary || []).map((line: string, i: number) => (
                        <Text key={i} fontSize="xs">{line}</Text>
                      ))}
                    </Box>
                  }
                  placement="top"
                  hasArrow
                >
                  <Box display="inline-flex" alignItems="center">
                    <AlertTriangle size={12} color="var(--chakra-colors-orange-400)" fill="var(--chakra-colors-orange-50)" />
                  </Box>
                </Tooltip>
              )}
            </HStack>
            {schema && (
              <Text fontSize="2xs" color={subtextColor} isTruncated w="100%">
                {schema}
              </Text>
            )}
            {isFiltered && (
              <Text fontSize="2xs" color={`${color}.600`} fontWeight="medium" isTruncated w="100%">
                Filtered Source
              </Text>
            )}
          </VStack>
        </HStack>

        {data.connectionType && (
          <Badge colorScheme={color} size="sm" fontSize="2xs" px={1.5} py={0.5}>
            {data.connectionType.toUpperCase()}
          </Badge>
        )}

        {data.config?.columnCount !== undefined && (
          <VStack align="flex-start" spacing={0} w="100%">
            <Text fontSize="2xs" color={textColor} fontWeight="medium">
              Columns: {data.config.columnCount}
            </Text>
          </VStack>
        )}

        <HStack spacing={1}>
          <Box
            w={1.5}
            h={1.5}
            borderRadius="full"
            bg={`${statusColor}.400`}
            animation={data.status === 'running' ? 'pulse 2s infinite' : 'none'}
          />
          <Text fontSize="2xs" color={subtextColor} textTransform="capitalize">
            {data.status || 'idle'}
          </Text>
          {/* Refresh Schema inline button — only shown when drift is detected */}
          {(data as any).schema_drift && (data as any).onRefreshSchema && (
            <Tooltip label="Refresh node schema (apply changes)" placement="bottom" hasArrow>
              <Box
                as="button"
                display="inline-flex"
                alignItems="center"
                ml={1}
                p={0.5}
                borderRadius="sm"
                _hover={{ bg: 'orange.100' }}
                cursor="pointer"
                onClick={(e: React.MouseEvent) => {
                  e.stopPropagation()
                  ;(data as any).onRefreshSchema(data.id)
                }}
              >
                <RefreshCw size={10} color="var(--chakra-colors-orange-500)" />
              </Box>
            </Tooltip>
          )}
        </HStack>
      </VStack>

      <Handle
        type="source"
        position={Position.Right}
        style={{
          width: 12,
          height: 12,
          background: `var(--chakra-colors-${color}-500)`,
          border: '2px solid white',
        }}
      />
    </NodeWrapper>
  )
}

export const TransformNode: React.FC<NodeProps<TransformNodeData>> = ({ data, selected }) => {
  const TransformIcon = nodeIcons.transform
  const color = nodeColors.transform
  const textColor = useColorModeValue('gray.800', 'white')
  const subtextColor = useColorModeValue('gray.600', 'gray.400')
  const displayName = data.business_name || data.label || 'Transform'

  // Errors set by propagateRemovedColumnsHard (flat string array) or structured config_errors
  const nodeErrors: string[] = (data as any).errors && Array.isArray((data as any).errors)
    ? (data as any).errors
    : []
  const rawConfigErrors: any[] =
    (data as any).config_errors && Array.isArray((data as any).config_errors)
      ? (data as any).config_errors
      : []
  const schemaErrors = dedupeSchemaErrors(rawConfigErrors)
  const errorColumns = Array.from(
    new Set(
      schemaErrors
        .map((e: any) => e.column)
        .filter((c: any) => typeof c === 'string' && c.trim().length > 0)
    )
  )
  const hasSchemaErrors = schemaErrors.length > 0
  const hasErrors = hasSchemaErrors || nodeErrors.length > 0

  const baseStatus = (data.status as keyof typeof statusColors) || 'idle'
  const derivedStatus: keyof typeof statusColors = hasErrors ? 'error' : baseStatus
  const statusColor = statusColors[derivedStatus]
  const nodeDataWithStatus = { ...data, status: derivedStatus }

  return (
    <NodeWrapper nodeType="transform" data={nodeDataWithStatus} selected={selected}>
      <VStack align="flex-start" spacing={2}>
        <HStack spacing={2} w="100%">
          <Icon as={TransformIcon} color={`${color}.500`} w={5} h={5} />
          <Box flex={1} minW={0}>
            {data.onNodeNameChange ? (
              <EditableNodeName
                value={displayName}
                onChange={(newName) => {
                  if (data.onNodeNameChange) {
                    data.onNodeNameChange(data.id, newName)
                  }
                }}
                fontSize="sm"
                fontWeight="semibold"
                color={textColor}
              />
            ) : (
              <Text fontSize="sm" fontWeight="semibold" color={textColor} isTruncated>
                {displayName}
              </Text>
            )}
          </Box>
          {hasErrors && (
            <Tooltip
              label={
                <Box maxW="280px">
                  <Text fontWeight="semibold" mb={1}>
                    Schema Errors
                  </Text>
                  {(hasSchemaErrors ? schemaErrors : nodeErrors.map((e) => ({ message: e }))).map(
                    (e: any, i: number) => (
                      <Text key={i} fontSize="xs" whiteSpace="pre-wrap">
                        {e.message || e}
                      </Text>
                    )
                  )}
                </Box>
              }
              placement="top"
              hasArrow
            >
              <Box display="inline-flex" alignItems="center" flexShrink={0}>
                <AlertTriangle size={13} color="var(--chakra-colors-red-500)" fill="var(--chakra-colors-red-100)" />
              </Box>
            </Tooltip>
          )}
        </HStack>

        {(data as any).transformType && (
          <Badge colorScheme={color} size="sm">
            {(data as any).transformType}
          </Badge>
        )}

        {hasErrors && (
          <Text fontSize="2xs" color="red.500" fontWeight="medium" noOfLines={1}>
            {errorColumns.length > 0
              ? errorColumns.length <= 3
                ? `⚠ ${errorColumns.join(', ')}`
                : `⚠ ${errorColumns.length} schema errors`
              : '⚠ Schema error'}
          </Text>
        )}

        <HStack spacing={2}>
          <Box
            w={2}
            h={2}
            borderRadius="full"
            bg={`${statusColor}.400`}
            animation={data.status === 'running' ? 'pulse 2s infinite' : 'none'}
          />
          <Text fontSize="xs" color={subtextColor} textTransform="capitalize">
            {derivedStatus || 'idle'}
          </Text>
        </HStack>
      </VStack>

      <Handle
        type="target"
        position={Position.Left}
        style={{
          width: 12,
          height: 12,
          background: `var(--chakra-colors-${color}-500)`,
          border: '2px solid white',
        }}
      />
      <Handle
        type="source"
        position={Position.Right}
        style={{
          width: 12,
          height: 12,
          background: `var(--chakra-colors-${color}-500)`,
          border: '2px solid white',
        }}
      />
    </NodeWrapper>
  )
}

export const DestinationNode: React.FC<NodeProps<DestinationNodeData>> = ({ data, selected }) => {
  const DestinationIcon = nodeIcons.destination
  const color = nodeColors.destination
  const statusColor = statusColors[data.status || 'idle']
  const textColor = useColorModeValue('gray.800', 'white')
  const subtextColor = useColorModeValue('gray.600', 'gray.400')
  const displayName = data.business_name || data.label || 'Destination'

  return (
    <NodeWrapper nodeType="destination" data={data} selected={selected}>
      <VStack align="flex-start" spacing={2}>
        <HStack spacing={2}>
          <Icon as={DestinationIcon} color={`${color}.500`} w={5} h={5} />
          {data.onNodeNameChange ? (
            <EditableNodeName
              value={displayName}
              onChange={(newName) => {
                if (data.onNodeNameChange) {
                  data.onNodeNameChange(data.id, newName)
                }
              }}
              fontSize="sm"
              fontWeight="semibold"
              color={textColor}
            />
          ) : (
            <Text fontSize="sm" fontWeight="semibold" color={textColor}>
              {displayName}
            </Text>
          )}
        </HStack>

        {data.destinationName && (
          <Text fontSize="xs" color={subtextColor}>
            {data.destinationName}
          </Text>
        )}

        {data.connectionType && (
          <Badge colorScheme={color} size="sm">
            {data.connectionType.toUpperCase()}
          </Badge>
        )}

        <HStack spacing={2}>
          <Box
            w={2}
            h={2}
            borderRadius="full"
            bg={`${statusColor}.400`}
            animation={data.status === 'running' ? 'pulse 2s infinite' : 'none'}
          />
          <Text fontSize="xs" color={subtextColor} textTransform="capitalize">
            {data.status || 'idle'}
          </Text>
        </HStack>
      </VStack>

      <Handle
        type="target"
        position={Position.Left}
        style={{
          width: 12,
          height: 12,
          background: `var(--chakra-colors-${color}-500)`,
          border: '2px solid white',
        }}
      />
    </NodeWrapper>
  )
}

export const nodeTypes = {
  source: SourceNode,
  transform: TransformNode,
  destination: DestinationNode,
  filter: FilterNode,
  join: JoinNode,
  projection: ProjectionNode,
  calculated: CalculatedColumnNode,
  aggregate: TransformNode,
  compute: TransformNode,
}

