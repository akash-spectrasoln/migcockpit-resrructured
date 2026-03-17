/**
 * Calculated Column Node Component
 * Visual representation of a calculated column transformation node
 */
import React from 'react'
import { Handle, Position, NodeProps } from 'reactflow'
import { Box, HStack, VStack, Text, Badge, Icon, Tooltip } from '@chakra-ui/react'
import { useColorModeValue } from '../../../hooks/useColorModeValue'
import { Calculator, Check } from 'lucide-react'
import { EditableNodeName } from './EditableNodeName'

export interface CalculatedColumnNodeData {
  id: string
  label: string
  node_id?: string
  business_name?: string
  technical_name?: string
  node_name?: string
  node_type?: string
  type: 'calculated'
  status?: 'idle' | 'running' | 'success' | 'error'
  config?: {
    calculatedColumns?: Array<{
      name: string
      expression: string
      dataType?: string
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

export const CalculatedColumnNode: React.FC<NodeProps<CalculatedColumnNodeData>> = ({ data, selected }) => {
  const color = 'purple'
  const status = data.status || 'idle'
  const statusColor = statusColors[status]
  const bg = useColorModeValue(`${color}.50`, `${color}.900`)
  const borderColor = useColorModeValue(`${color}.300`, `${color}.700`)
  const selectedBorder = useColorModeValue(`${color}.500`, `${color}.400`)
  const successBorder = useColorModeValue('green.400', 'green.500')
  const errorBorder = useColorModeValue('red.400', 'red.500')
  const runningBorder = useColorModeValue('yellow.400', 'yellow.500')
  const resolvedBorder =
    status === 'success' ? successBorder
    : status === 'error' ? errorBorder
    : status === 'running' ? runningBorder
    : selected ? selectedBorder : borderColor
  const textColor = useColorModeValue('gray.800', 'white')
  const subtextColor = useColorModeValue('gray.600', 'gray.400')
  const successBadgeBg = useColorModeValue('green.50', 'green.900')
  const successBadgeBorder = useColorModeValue('green.200', 'green.700')

  const columnCount = data.config?.calculatedColumns?.length || 0
  const displayName = data.business_name || data.label || 'Calculated Column'

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
          <Icon as={Calculator} color={`${color}.500`} w={4} h={4} />
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
        </HStack>

        {columnCount > 0 && (
          <Badge colorScheme={color} size="sm" fontSize="2xs" px={1.5} py={0.5}>
            {columnCount} calc{columnCount !== 1 ? 's' : ''}
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
    </Box>
  )
}

