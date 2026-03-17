/**
 * Edge Context Menu Component
 * Context menu that appears on right-click/click of an edge with options to insert nodes
 */
import React, { useEffect, useRef } from 'react'
import {
  Portal,
  Box,
  VStack,
  HStack,
  Text,
  Divider,
  Icon,
  useColorModeValue,
} from '@chakra-ui/react'
import {
  Filter,
  GitMerge,
  Columns,
  Calculator,
  BarChart3,
  Code2,
} from 'lucide-react'
import { Edge } from 'reactflow'

interface EdgeContextMenuProps {
  edge: Edge
  position: { x: number; y: number }
  isOpen: boolean
  onClose: () => void
  onInsertNode: (nodeType: string) => void
}

interface MenuOption {
  id: string
  label: string
  icon: React.ElementType
  color?: string
  divider?: boolean
}

export const EdgeContextMenu: React.FC<EdgeContextMenuProps> = ({
  edge,
  position,
  isOpen,
  onClose,
  onInsertNode,
}) => {
  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const hoverBg = useColorModeValue('gray.50', 'gray.700')
  const textColor = useColorModeValue('gray.800', 'white')
  const menuRef = useRef<HTMLDivElement>(null)

  // Close menu when clicking outside
  useEffect(() => {
    if (!isOpen) return

    const handleClickOutside = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        onClose()
      }
    }

    // Add event listener after a short delay to avoid immediate closure
    const timeoutId = setTimeout(() => {
      document.addEventListener('mousedown', handleClickOutside)
    }, 100)

    return () => {
      clearTimeout(timeoutId)
      document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [isOpen, onClose])

  if (!isOpen) return null

  const menuOptions: MenuOption[] = [
    { id: 'insert-filter', label: 'Insert Filter', icon: Filter, color: 'purple' },
    { id: 'insert-projection', label: 'Insert Projection', icon: Columns, color: 'purple' },
    { id: 'insert-join', label: 'Insert Join', icon: GitMerge, color: 'purple' },
    { id: 'insert-calculated', label: 'Insert Calculated Column', icon: Calculator, color: 'purple' },
    { id: 'insert-aggregate', label: 'Insert Aggregate', icon: BarChart3, color: 'purple' },
    { id: 'insert-compute', label: 'Insert Compute', icon: Code2, color: 'purple' },
  ]

  const handleAction = (actionId: string) => {
    // Extract node type from action ID (e.g., 'insert-filter' -> 'filter')
    const nodeType = actionId.replace('insert-', '')
    onInsertNode(nodeType)
    onClose()
  }

  return (
    <Portal>
      <Box
        ref={menuRef}
        position="fixed"
        left={`${position.x}px`}
        top={`${position.y}px`}
        bg={bg}
        borderWidth="1px"
        borderColor={borderColor}
        borderRadius="md"
        boxShadow="xl"
        zIndex={2000}
        minW="200px"
        py={1}
      >
        <VStack align="stretch" spacing={0}>
          <Box px={4} py={2} borderBottomWidth="1px" borderColor={borderColor}>
            <Text fontSize="xs" color="gray.500" fontWeight="semibold">
              Insert Node Between
            </Text>
          </Box>
          {menuOptions.map((option, idx) => {
            if (option.divider) {
              return <Divider key={`divider-${idx}`} my={1} />
            }

            const IconComponent = option.icon

            return (
              <Box
                key={option.id}
                as="button"
                w="100%"
                px={4}
                py={2}
                textAlign="left"
                onClick={() => handleAction(option.id)}
                _hover={{ bg: hoverBg }}
                cursor="pointer"
                transition="background 0.2s"
              >
                <HStack spacing={3}>
                  <Icon
                    as={IconComponent}
                    w={4}
                    h={4}
                    color={
                      option.color === 'red'
                        ? 'red.500'
                        : option.color === 'blue'
                          ? 'blue.500'
                          : option.color === 'purple'
                            ? 'purple.500'
                            : option.color === 'green'
                              ? 'green.500'
                              : 'gray.500'
                    }
                  />
                  <Text
                    fontSize="sm"
                    color={
                      option.color === 'red'
                        ? 'red.600'
                        : option.color === 'green'
                          ? 'green.600'
                          : textColor
                    }
                    fontWeight={option.color === 'red' || option.color === 'green' ? 'medium' : 'normal'}
                  >
                    {option.label}
                  </Text>
                </HStack>
              </Box>
            )
          })}
        </VStack>
      </Box>
    </Portal>
  )
}
