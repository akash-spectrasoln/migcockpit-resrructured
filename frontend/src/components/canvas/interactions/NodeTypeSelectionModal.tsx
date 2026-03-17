/**
 * Node Type Selection Modal
 * Modal for selecting node type when inserting a node between edges
 */
import React from 'react'
import {
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalBody,
  ModalFooter,
  ModalCloseButton,
  Button,
  VStack,
  Text,
  SimpleGrid,
} from '@chakra-ui/react'
import { Filter, Columns, GitMerge, Calculator, BarChart3, Code2, ArrowRight } from 'lucide-react'
import { useColorModeValue } from '../../../hooks/useColorModeValue'

interface NodeTypeOption {
  id: string
  label: string
  icon: React.ComponentType<any>
  description: string
}

const nodeTypeOptions: NodeTypeOption[] = [
  { id: 'filter', label: 'Filter', icon: Filter, description: 'Filter rows based on conditions' },
  { id: 'projection', label: 'Projection', icon: Columns, description: 'Select and rename columns' },
  { id: 'join', label: 'Join', icon: GitMerge, description: 'Join two data streams' },
  { id: 'calculated', label: 'Calculated Column', icon: Calculator, description: 'Add calculated columns' },
  { id: 'aggregate', label: 'Aggregate', icon: BarChart3, description: 'Group and aggregate data' },
  { id: 'compute', label: 'Compute', icon: Code2, description: 'Custom Python transformation' },
  { id: 'destination', label: 'Destination', icon: ArrowRight, description: 'Output to destination' },
]

interface NodeTypeSelectionModalProps {
  isOpen: boolean
  onClose: () => void
  onSelect: (nodeType: string) => void
}

export const NodeTypeSelectionModal: React.FC<NodeTypeSelectionModalProps> = ({
  isOpen,
  onClose,
  onSelect,
}) => {
  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const hoverBg = useColorModeValue('gray.50', 'gray.700')

  return (
    <Modal isOpen={isOpen} onClose={onClose} size="lg">
      <ModalOverlay />
      <ModalContent bg={bg}>
        <ModalHeader>Select Node Type</ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          <Text fontSize="sm" color="gray.500" mb={4}>
            Choose the type of node to insert between the connected nodes
          </Text>
          <SimpleGrid columns={2} spacing={3}>
            {nodeTypeOptions.map((option) => {
              const Icon = option.icon
              return (
                <Button
                  key={option.id}
                  leftIcon={<Icon size={20} />}
                  variant="outline"
                  onClick={() => {
                    onSelect(option.id)
                    onClose()
                  }}
                  h="auto"
                  py={4}
                  flexDirection="column"
                  alignItems="flex-start"
                  borderColor={borderColor}
                  _hover={{
                    bg: hoverBg,
                    borderColor: 'blue.500',
                  }}
                >
                  <VStack align="flex-start" spacing={1}>
                    <Text fontWeight="semibold">{option.label}</Text>
                    <Text fontSize="xs" color="gray.500">
                      {option.description}
                    </Text>
                  </VStack>
                </Button>
              )
            })}
          </SimpleGrid>
        </ModalBody>
        <ModalFooter>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  )
}
