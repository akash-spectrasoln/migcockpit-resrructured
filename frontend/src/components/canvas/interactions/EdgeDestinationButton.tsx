/**
 * Edge Destination Button Component
 * Shows a button on edge hover to add a destination at the end of a pipeline branch
 */
import React from 'react'
import { Box, Button, Icon, Text } from '@chakra-ui/react'
import { Database } from 'lucide-react'

interface EdgeDestinationButtonProps {
  x: number
  y: number
  onAddDestination: () => void
}

export const EdgeDestinationButton: React.FC<EdgeDestinationButtonProps> = ({ x, y, onAddDestination }) => {
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
        colorScheme="green"
        leftIcon={<Icon as={Database} boxSize={4} />}
        onClick={onAddDestination}
        borderRadius="md"
        boxShadow="lg"
        _hover={{
          transform: 'scale(1.05)',
          boxShadow: 'xl',
        }}
        transition="all 0.2s"
      >
        <Text as="span" mr={1}>➕</Text>
        Add Destination
      </Button>
    </Box>
  )
}
