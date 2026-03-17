/**
 * Node Context Menu Component
 * Functional context menu that appears on right-click with actionable options
 */
import React, { useState } from 'react'
import {
  Portal,
  Box,
  VStack,
  HStack,
  Text,
  Divider,
  Icon,
  useColorModeValue,
  Popover,
  PopoverTrigger,
  PopoverContent,
  PopoverBody,
} from '@chakra-ui/react'
import {
  Filter,
  GitMerge,
  Columns,
  Calculator,
  Eye,
  Trash2,
  Settings,
  Copy,
  X,
  Edit,
  ArrowRight,
  BarChart3,
  Code2,
  Play,
} from 'lucide-react'
import { Node, Edge } from 'reactflow'

interface NodeContextMenuProps {
  node: Node
  nodes: Node[]
  edges: Edge[]
  position: { x: number; y: number }
  isOpen: boolean
  onClose: () => void
  onAction: (action: string, node: Node) => void
  /** When true, show "Execute this flow" option to run only the flow containing this node */
  hasMultipleFlows?: boolean
}

interface SubMenuItem {
  id: string
  label: string
  function?: string
  action?: string
}

interface MenuOption {
  id: string
  label: string
  icon: React.ElementType
  color?: string
  divider?: boolean
  disabled?: boolean
  hasSubmenu?: boolean
  submenu?: SubMenuItem[]
}

export const NodeContextMenu: React.FC<NodeContextMenuProps> = ({
  node,
  position,
  isOpen,
  onClose,
  onAction,
  hasMultipleFlows = false,
}) => {
  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const hoverBg = useColorModeValue('gray.50', 'gray.700')
  const textColor = useColorModeValue('gray.800', 'white')
  const [openSubmenu, setOpenSubmenu] = useState<string | null>(null)

  if (!isOpen) return null

  const getMenuOptions = (): MenuOption[] => {
    const nodeType = node.data.type
    const baseOptions: MenuOption[] = []

    if (hasMultipleFlows) {
      baseOptions.push(
        { id: 'execute-flow', label: 'Execute this flow', icon: Play, color: 'green' },
        { id: 'divider-execute', label: '', icon: X, divider: true },
      )
    }

    if (nodeType === 'source') {
      // Table/Source node options
      return [
        ...baseOptions,
        { id: 'preview', label: 'Preview Data', icon: Eye, color: 'blue' },
        { id: 'divider1', label: '', icon: X, divider: true },
        { id: 'add-filter', label: 'Add Filter', icon: Filter, color: 'purple' },
        { id: 'add-join', label: 'Add Join', icon: GitMerge, color: 'purple' },
        { id: 'add-projection', label: 'Add Projection', icon: Columns, color: 'purple' },
        { id: 'addAggregate', label: 'Add Aggregates', icon: BarChart3, color: 'purple' },
        { id: 'add-compute', label: 'Add Compute', icon: Code2, color: 'purple' },
        { id: 'divider2', label: '', icon: X, divider: true },
        { id: 'add-destination', label: 'Add Destination', icon: ArrowRight, color: 'green' },
        { id: 'divider3', label: '', icon: X, divider: true },
        { id: 'delete', label: 'Delete Node', icon: Trash2, color: 'red' },
      ]
    } else if (nodeType === 'filter') {
      // Filter node options
      return [
        ...baseOptions,
        { id: 'edit-filter', label: 'Edit Filter', icon: Edit, color: 'blue' },
        { id: 'preview', label: 'Preview Data', icon: Eye, color: 'blue' },
        { id: 'duplicate', label: 'Duplicate Filter', icon: Copy, color: 'gray' },
        { id: 'divider1', label: '', icon: X, divider: true },
        { id: 'add-filter', label: 'Add Filter', icon: Filter, color: 'purple' },
        { id: 'add-join', label: 'Add Join', icon: GitMerge, color: 'purple' },
        { id: 'add-projection', label: 'Add Projection', icon: Columns, color: 'purple' },
        { id: 'addAggregate', label: 'Add Aggregates', icon: BarChart3, color: 'purple' },
        { id: 'add-compute', label: 'Add Compute', icon: Code2, color: 'purple' },
        { id: 'divider2', label: '', icon: X, divider: true },
        { id: 'add-destination', label: 'Add Destination', icon: ArrowRight, color: 'green' },
        { id: 'divider3', label: '', icon: X, divider: true },
        { id: 'delete', label: 'Remove Filter', icon: Trash2, color: 'red' },
      ]
    } else if (nodeType === 'join') {
      // Join node options
      return [
        ...baseOptions,
        { id: 'configure-join', label: 'Configure Join', icon: Settings, color: 'blue' },
        { id: 'change-join-type', label: 'Change Join Type', icon: GitMerge, color: 'blue' },
        { id: 'edit-mappings', label: 'Edit Mappings', icon: Edit, color: 'blue' },
        { id: 'preview', label: 'Preview Data', icon: Eye, color: 'blue' },
        { id: 'divider1', label: '', icon: X, divider: true },
        { id: 'add-filter', label: 'Add Filter', icon: Filter, color: 'purple' },
        { id: 'add-projection', label: 'Add Projection', icon: Columns, color: 'purple' },
        { id: 'addAggregate', label: 'Add Aggregates', icon: BarChart3, color: 'purple' },
        { id: 'add-compute', label: 'Add Compute', icon: Code2, color: 'purple' },
        { id: 'divider2', label: '', icon: X, divider: true },
        { id: 'add-destination', label: 'Add Destination', icon: ArrowRight, color: 'green' },
        { id: 'divider3', label: '', icon: X, divider: true },
        { id: 'delete', label: 'Remove Join', icon: Trash2, color: 'red' },
      ]
    } else if (nodeType === 'projection') {
      // Projection node options (matching XML specification)
      // Note: "Add Calculated Column" and "Add Aggregated Column" are specific to projection nodes
      // "Add Aggregates" is removed to avoid duplication with "Add Aggregated Column"
      return [
        ...baseOptions,
        { id: 'configure', label: 'Configure Projection', icon: Settings, color: 'blue' },
        { id: 'preview', label: 'Preview Data', icon: Eye, color: 'blue' },
        { id: 'divider1', label: '', icon: X, divider: true },
        { id: 'add-calculated-column', label: 'Add Calculated Column', icon: Calculator, color: 'purple' },
        { id: 'addAggregate', label: 'Add Aggregates', icon: BarChart3, color: 'purple' },
        { id: 'add-compute', label: 'Add Compute', icon: Code2, color: 'purple' },
        { id: 'divider2', label: '', icon: X, divider: true },
        { id: 'add-filter', label: 'Add Filter', icon: Filter, color: 'purple' },
        { id: 'add-join', label: 'Add Join', icon: GitMerge, color: 'purple' },
        { id: 'divider3', label: '', icon: X, divider: true },
        { id: 'add-destination', label: 'Add Destination', icon: ArrowRight, color: 'green' },
        { id: 'divider4', label: '', icon: X, divider: true },
        { id: 'delete', label: 'Delete Node', icon: Trash2, color: 'red' },
      ]
    } else if (nodeType === 'calculated') {
      // Calculated column node options
      return [
        ...baseOptions,
        { id: 'configure', label: 'Configure Calculated Column', icon: Settings, color: 'blue' },
        { id: 'preview', label: 'Preview Data', icon: Eye, color: 'blue' },
        { id: 'divider1', label: '', icon: X, divider: true },
        { id: 'add-filter', label: 'Add Filter', icon: Filter, color: 'purple' },
        { id: 'add-join', label: 'Add Join', icon: GitMerge, color: 'purple' },
        { id: 'add-projection', label: 'Add Projection', icon: Columns, color: 'purple' },
        { id: 'addAggregate', label: 'Add Aggregates', icon: BarChart3, color: 'purple' },
        { id: 'add-compute', label: 'Add Compute', icon: Code2, color: 'purple' },
        { id: 'divider2', label: '', icon: X, divider: true },
        { id: 'add-destination', label: 'Add Destination', icon: ArrowRight, color: 'green' },
        { id: 'divider3', label: '', icon: X, divider: true },
        { id: 'delete', label: 'Delete Node', icon: Trash2, color: 'red' },
      ]
    } else if (nodeType === 'aggregate') {
      // Aggregate node options
      return [
        ...baseOptions,
        { id: 'configure', label: 'Configure Aggregates', icon: Settings, color: 'blue' },
        { id: 'preview', label: 'Preview Data', icon: Eye, color: 'blue' },
        { id: 'divider1', label: '', icon: X, divider: true },
        { id: 'add-filter', label: 'Add Filter', icon: Filter, color: 'purple' },
        { id: 'add-join', label: 'Add Join', icon: GitMerge, color: 'purple' },
        { id: 'add-projection', label: 'Add Projection', icon: Columns, color: 'purple' },
        { id: 'add-compute', label: 'Add Compute', icon: Code2, color: 'purple' },
        { id: 'divider2', label: '', icon: X, divider: true },
        { id: 'add-destination', label: 'Add Destination', icon: ArrowRight, color: 'green' },
        { id: 'divider3', label: '', icon: X, divider: true },
        { id: 'delete', label: 'Delete Node', icon: Trash2, color: 'red' },
      ]
    } else if (nodeType === 'compute') {
      // Compute node options
      return [
        ...baseOptions,
        { id: 'configure', label: 'Configure Compute', icon: Settings, color: 'blue' },
        { id: 'preview', label: 'Preview Data', icon: Eye, color: 'blue' },
        { id: 'divider1', label: '', icon: X, divider: true },
        { id: 'add-filter', label: 'Add Filter', icon: Filter, color: 'purple' },
        { id: 'add-join', label: 'Add Join', icon: GitMerge, color: 'purple' },
        { id: 'add-projection', label: 'Add Projection', icon: Columns, color: 'purple' },
        { id: 'addAggregate', label: 'Add Aggregates', icon: BarChart3, color: 'purple' },
        { id: 'divider2', label: '', icon: X, divider: true },
        { id: 'add-destination', label: 'Add Destination', icon: ArrowRight, color: 'green' },
        { id: 'divider3', label: '', icon: X, divider: true },
        { id: 'delete', label: 'Delete Node', icon: Trash2, color: 'red' },
      ]
    } else if (nodeType === 'destination') {
      // Destination node options
      return [
        ...baseOptions,
        { id: 'configure', label: 'Configure Destination', icon: Settings, color: 'blue' },
        { id: 'preview', label: 'Preview Data', icon: Eye, color: 'blue' },
        { id: 'divider1', label: '', icon: X, divider: true },
        { id: 'delete', label: 'Delete Node', icon: Trash2, color: 'red' },
      ]
    }

    // Default options for unknown node types
    return [
      ...baseOptions,
      { id: 'configure', label: 'Configure', icon: Settings, color: 'blue' },
      { id: 'preview', label: 'Preview Data', icon: Eye, color: 'blue' },
      { id: 'divider1', label: '', icon: X, divider: true },
      { id: 'delete', label: 'Delete Node', icon: Trash2, color: 'red' },
    ]
  }

  const menuOptions = getMenuOptions()

  const handleAction = (actionId: string, functionName?: string) => {
    // Pass function name if it's an aggregate action
    const actionData = functionName ? { action: actionId, function: functionName } : actionId
    onAction(typeof actionData === 'string' ? actionData : JSON.stringify(actionData), node)
    onClose()
  }

  return (
    <Portal>
      {/* Click-away overlay: close menu on any left-click outside */}
      <Box
        position="fixed"
        top={0}
        left={0}
        right={0}
        bottom={0}
        zIndex={1999}
        bg="transparent"
        onClick={onClose}
      />
      <Box
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
        onClick={(e) => e.stopPropagation()}
      >
        <VStack align="stretch" spacing={0}>
          {menuOptions.map((option, idx) => {
            if (option.divider) {
              return <Divider key={`divider-${idx}`} my={1} />
            }

            const IconComponent = option.icon

            // Handle submenu items
            if (option.hasSubmenu && option.submenu) {
              return (
                <Popover
                  key={option.id}
                  isOpen={openSubmenu === option.id}
                  onOpen={() => setOpenSubmenu(option.id)}
                  onClose={() => setOpenSubmenu(null)}
                  placement="right-start"
                  closeOnBlur={true}
                >
                  <PopoverTrigger>
                    <Box
                      as="button"
                      w="100%"
                      px={4}
                      py={2}
                      textAlign="left"
                      disabled={option.disabled}
                      _hover={!option.disabled ? { bg: hoverBg } : {}}
                      _disabled={{ opacity: 0.5, cursor: 'not-allowed' }}
                      cursor={option.disabled ? 'not-allowed' : 'pointer'}
                      transition="background 0.2s"
                    >
                      <HStack spacing={3} justify="space-between">
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
                                  : option.color === 'green'
                                    ? 'green.500'
                                    : option.color === 'purple'
                                      ? 'purple.500'
                                      : 'gray.500'
                            }
                          />
                          <Text fontSize="sm" color={textColor}>
                            {option.label}
                          </Text>
                        </HStack>
                        <Text fontSize="xs" color="gray.500">›</Text>
                      </HStack>
                    </Box>
                  </PopoverTrigger>
                  <PopoverContent
                    bg={bg}
                    borderColor={borderColor}
                    boxShadow="xl"
                    minW="180px"
                  >
                    <PopoverBody p={0}>
                      <VStack align="stretch" spacing={0}>
                        {option.submenu.map((subItem) => (
                          <Box
                            key={subItem.id}
                            as="button"
                            w="100%"
                            px={4}
                            py={2}
                            textAlign="left"
                            onClick={() => {
                              handleAction(subItem.id, subItem.function || '')
                              setOpenSubmenu(null)
                            }}
                            _hover={{ bg: hoverBg }}
                            cursor="pointer"
                            transition="background 0.2s"
                          >
                            <Text fontSize="sm" color={textColor}>
                              {subItem.label}
                            </Text>
                          </Box>
                        ))}
                      </VStack>
                    </PopoverBody>
                  </PopoverContent>
                </Popover>
              )
            }

            return (
              <Box
                key={option.id}
                as="button"
                w="100%"
                px={4}
                py={2}
                textAlign="left"
                onClick={() => !option.disabled && handleAction(option.id)}
                disabled={option.disabled}
                _hover={!option.disabled ? { bg: hoverBg } : {}}
                _disabled={{ opacity: 0.5, cursor: 'not-allowed' }}
                cursor={option.disabled ? 'not-allowed' : 'pointer'}
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
                          : option.color === 'green'
                            ? 'green.500'
                            : option.color === 'purple'
                              ? 'purple.500'
                              : 'gray.500'
                    }
                  />
                  <Text
                    fontSize="sm"
                    color={
                      option.color === 'red'
                        ? 'red.600'
                        : textColor
                    }
                    fontWeight={option.color === 'red' ? 'medium' : 'normal'}
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

