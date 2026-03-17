/**
 * Filter Node Component
 * Visual representation of a filter transformation node
 */
import React from 'react'
import { Handle, Position, NodeProps } from 'reactflow'
import { Box, HStack, VStack, Text, Badge, Icon, Tooltip } from '@chakra-ui/react'
import { useColorModeValue } from '../../../hooks/useColorModeValue'
import { Filter, Check } from 'lucide-react'
import { EditableNodeName } from './EditableNodeName'

export interface FilterNodeData {
  id: string
  label: string
  node_id?: string
  business_name?: string
  technical_name?: string
  node_type?: string
  type: 'filter'
  status?: 'idle' | 'running' | 'success' | 'error'
  schema_outdated?: boolean
  output_metadata?: { columns?: Array<{ name?: string; [key: string]: unknown }> }
  config?: {
    expression?: string
    conditions?: Array<{
      id: string
      column: string
      operator: string
      value: any
      logicalOperator?: 'AND' | 'OR'
    }>
    filteredRowCount?: number
    columnCount?: number
    sourceId?: number
    tableName?: string
    schema?: string
  }
  onNodeNameChange?: (nodeId: string, newName: string) => void
}

const statusColors = {
  idle: 'gray',
  running: 'yellow',
  success: 'green',
  error: 'red',
}

export const FilterNode: React.FC<NodeProps<FilterNodeData>> = ({ data, selected }) => {
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

  const conditionCount = data.config?.conditions?.length || 0
  // Filter is schema-transparent: use output_metadata from input (projection) over config.columnCount
  // config.columnCount can be wrong when from source-table API (e.g. 19) instead of projection output (16)
  const cols = data.output_metadata?.columns
  const columnCount =
    (cols && Array.isArray(cols) ? cols.length : undefined) ?? data.config?.columnCount

  return (
    <Box
      bg={bg}
      borderWidth={status !== 'idle' ? '2px' : '1.5px'}
      borderColor={resolvedBorder}
      borderRadius="md"
      p={1.5}
      minW="120px"
      maxW="160px"
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
      <VStack align="flex-start" spacing={0.5}>
        <HStack spacing={1} w="100%">
          <Icon as={Filter} color={`${color}.500`} w={3.5} h={3.5} />
          <EditableNodeName
            value={data.business_name || data.label || 'Filter'}
            onChange={(newName) => {
              if (data.onNodeNameChange) {
                data.onNodeNameChange(data.id, newName)
              }
            }}
            fontSize="2xs"
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

        {conditionCount > 0 && (
          <Text fontSize="2xs" color={subtextColor}>
            {conditionCount} condition{conditionCount !== 1 ? 's' : ''}
          </Text>
        )}

        {columnCount !== undefined && (
          <Text fontSize="2xs" color={textColor} fontWeight="medium">
            Columns: {columnCount}
          </Text>
        )}

        {conditionCount === 0 && columnCount === undefined && (
          <Text fontSize="2xs" color={subtextColor} fontStyle="italic">
            No conditions
          </Text>
        )}
      </VStack>

      {/* Input handle - larger and more visible */}
      <Handle
        type="target"
        position={Position.Left}
        style={{
          width: 14,
          height: 14,
          background: `var(--chakra-colors-${color}-500)`,
          border: '3px solid white',
          borderRadius: '50%',
        }}
      />

      {/* Output handle - larger and more visible */}
      <Handle
        type="source"
        position={Position.Right}
        style={{
          width: 14,
          height: 14,
          background: `var(--chakra-colors-${color}-500)`,
          border: '3px solid white',
          borderRadius: '50%',
        }}
      />
    </Box>
  )
}

