/**
 * Edge Insert Button Component
 * Shows a button on edge hover to insert a node between two nodes
 */
import React from 'react'
import { Box, Button, Icon } from '@chakra-ui/react'
import { Plus } from 'lucide-react'

interface EdgeInsertButtonProps {
  x: number
  y: number
  onInsert: () => void
}

export const EdgeInsertButton: React.FC<EdgeInsertButtonProps> = ({ x, y, onInsert }) => {
  return (
    <Box
      position="absolute"
      left={`${x}px`}
      top={`${y}px`}
      transform="translate(-50%, -50%)"
      zIndex={1000}
      pointerEvents="all"
    >
      <Button
        size="sm"
        colorScheme="blue"
        leftIcon={<Icon as={Plus} boxSize={4} />}
        onClick={onInsert}
        borderRadius="full"
        boxShadow="md"
        _hover={{
          transform: 'scale(1.1)',
        }}
      >
        Insert node
      </Button>
    </Box>
  )
}
