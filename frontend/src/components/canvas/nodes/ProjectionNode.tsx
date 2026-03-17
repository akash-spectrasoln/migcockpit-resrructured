/**
 * Projection Node Component
 * Visual representation of a projection/column selection node
 */
import React from 'react'
import { Handle, Position, NodeProps } from 'reactflow'
import { Box, HStack, VStack, Text, Badge, Icon, Tooltip } from '@chakra-ui/react'
import { useColorModeValue } from '../../../hooks/useColorModeValue'
import { Columns, Check } from 'lucide-react'
import { EditableNodeName } from './EditableNodeName'

export interface ProjectionNodeData {
  id: string
  label: string
  node_id?: string
  node_name?: string
  business_name?: string
  technical_name?: string
  node_type?: string
  type: 'projection'
  status?: 'idle' | 'running' | 'success' | 'error'
  schema_outdated?: boolean
  output_metadata?: { columns?: Array<{ name?: string; [key: string]: unknown }> }
  config?: {
    selectedColumns?: string[]
    excludedColumns?: string[]
    includedColumns?: string[]
    columnMappings?: Array<{
      source: string
      target: string
    }>
  }
  onNodeNameChange?: (nodeId: string, newName: string) => void
}

const statusColors = {
  idle: 'gray',
  running: 'yellow',
  success: 'green',
  error: 'red',
}

export const ProjectionNode: React.FC<NodeProps<ProjectionNodeData>> = ({ data, selected, id }) => {
  const color = 'purple'
  const status = data.status || 'idle'
  const statusColor = statusColors[status]
  const bg = useColorModeValue(`${color}.50`, `${color}.900`)
  const borderColor = useColorModeValue(`${color}.300`, `${color}.700`)
  const selectedBorder = useColorModeValue(`${color}.500`, `${color}.400`)
  const successBorder = useColorModeValue('green.400', 'green.500')
  const errorBorder = useColorModeValue('red.400', 'red.500')
  const runningBorder = useColorModeValue('yellow.400', 'yellow.500')
  const errors = (data as any).errors as any[] | undefined
  const hasError = Array.isArray(errors) && errors.length > 0
  const resolvedBorder =
    hasError || status === 'error' ? errorBorder
    : status === 'success' ? successBorder
    : status === 'running' ? runningBorder
    : selected ? selectedBorder : borderColor
  const textColor = useColorModeValue('gray.800', 'white')
  const subtextColor = useColorModeValue('gray.600', 'gray.400')
  const successBadgeBg = useColorModeValue('green.50', 'green.900')
  const successBadgeBorder = useColorModeValue('green.200', 'green.700')

  // Prefer includedColumns count (matches right-hand Projection panel "Included"),
  // then fall back to output_columns / selectedColumns / columnOrder, then metadata.
  const cfg: any = data.config || {}
  const columnOrder = cfg.columnOrder
  const orderCount =
    Array.isArray(columnOrder)
      ? columnOrder.filter(
          (c: any) => c && c.included !== false && (c.order ?? 0) >= 0
        ).length
      : undefined
  const configCount =
    data.config?.includedColumns?.length ??
    cfg.output_columns?.length ??
    data.config?.selectedColumns?.length ??
    orderCount
  const cols = data.output_metadata?.columns
  const metadataCount = cols && Array.isArray(cols) ? cols.length : undefined
  const columnCount = configCount ?? metadataCount ?? 0

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
      <VStack align="flex-start" spacing={1}>
        <HStack spacing={1.5} w="100%">
          <Icon as={Columns} color={`${color}.500`} w={4} h={4} />
          <EditableNodeName
            value={data.business_name || data.node_name || data.label || 'Projection'}
            onChange={(newName) => {
              if (data.onNodeNameChange) {
                data.onNodeNameChange(id, newName)
              }
            }}
            fontSize="xs"
            fontWeight="semibold"
            color={textColor}
            isTruncated
            flex={1}
          />
        </HStack>

        {hasError && (
          <Text fontSize="8px" color="red.400">
            ❌ {typeof errors?.[0] === 'string' ? errors[0] : errors?.[0]?.message ?? 'Schema error'}
          </Text>
        )}

        {columnCount > 0 && (
          <Badge colorScheme={color} size="sm" fontSize="2xs" px={1.5} py={0.5}>
            {columnCount} col{columnCount !== 1 ? 's' : ''}
          </Badge>
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
        </HStack>
      </VStack>

      <Handle
        type="target"
        position={Position.Left}
        id="input"
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
        id="output"
        style={{
          width: 12,
          height: 12,
          background: `var(--chakra-colors-${color}-500)`,
          border: '2px solid white',
        }}
      />
    </Box>
  )
}

