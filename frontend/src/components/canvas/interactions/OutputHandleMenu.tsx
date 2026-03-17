/**
 * Output Handle Menu Component
 * Shows a menu when hovering over a node's output handle to add a next node
 */
import React, { useState } from 'react'
import {
  Menu,
  MenuButton,
  MenuList,
  MenuItem,
  IconButton,
  Tooltip,
} from '@chakra-ui/react'
import { Plus } from 'lucide-react'

interface OutputHandleMenuProps {
  nodeId: string
  nodeType: string
  onAddNode: (nodeType: string) => void
  disabled?: boolean
}

export const OutputHandleMenu: React.FC<OutputHandleMenuProps> = ({
  nodeId,
  nodeType,
  onAddNode,
  disabled = false,
}) => {
  const [isOpen, setIsOpen] = useState(false)

  // Node types that can be added after any node (except Source which is special)
  const availableNodeTypes = [
    { id: 'filter', label: 'Filter' },
    { id: 'projection', label: 'Projection' },
    { id: 'join', label: 'Join' },
    { id: 'calculated', label: 'Calculated Column' },
    { id: 'aggregate', label: 'Aggregate' },
    { id: 'compute', label: 'Compute' },
    { id: 'destination', label: 'Destination' },
  ]

  if (disabled) {
    return null
  }

  return (
    <Menu isOpen={isOpen} onOpen={() => setIsOpen(true)} onClose={() => setIsOpen(false)}>
      <Tooltip label="Add next node" placement="right">
        <MenuButton
          as={IconButton}
          icon={<Plus size={16} />}
          size="xs"
          variant="ghost"
          colorScheme="blue"
          borderRadius="full"
          position="absolute"
          right="-20px"
          top="50%"
          transform="translateY(-50%)"
          zIndex={1000}
          onMouseEnter={() => setIsOpen(true)}
          onMouseLeave={() => setIsOpen(false)}
          aria-label="Add next node"
        />
      </Tooltip>
      <MenuList onMouseEnter={() => setIsOpen(true)} onMouseLeave={() => setIsOpen(false)}>
        {availableNodeTypes.map((type) => (
          <MenuItem
            key={type.id}
            onClick={() => {
              onAddNode(type.id)
              setIsOpen(false)
            }}
          >
            {type.label}
          </MenuItem>
        ))}
      </MenuList>
    </Menu>
  )
}
