/**
 * Node Palette - Chakra UI Version
 * Left-side panel with draggable nodes grouped by category
 */
import React from 'react'
import {
  Box,
  VStack,
  Accordion,
  AccordionItem,
  AccordionButton,
  AccordionPanel,
  AccordionIcon,
  HStack,
  Text,
  Badge,
  Icon,
} from '@chakra-ui/react'
import { useColorModeValue } from '../../../hooks/useColorModeValue'
import { motion } from 'framer-motion'
import { Database, Settings, ArrowRight } from 'lucide-react'
import { getNodeTypesByCategory, NodeCategory } from '../../../types/nodeRegistry'

interface NodePaletteProps {
  onDragStart: (nodeType: string, event: React.DragEvent) => void
}

const categoryIcons = {
  source: Database,
  transform: Settings,
  destination: ArrowRight,
}

const categoryColors = {
  source: 'blue',
  transform: 'purple',
  destination: 'green',
}

const MotionBox = motion.create(Box)

export const NodePalette: React.FC<NodePaletteProps> = ({ onDragStart }) => {
  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const categories: NodeCategory[] = ['source', 'transform', 'destination']

  return (
    <Box
      w="280px"
      h="100%"
      bg={bg}
      borderRightWidth="1px"
      borderColor={borderColor}
      overflowY="auto"
    >
      <Box p={4} borderBottomWidth="1px" borderColor={borderColor}>
        <Text fontSize="lg" fontWeight="semibold" color={useColorModeValue('gray.800', 'white')}>
          Node Palette
        </Text>
        <Text fontSize="xs" color={useColorModeValue('gray.600', 'gray.400')} mt={1}>
          Drag nodes to canvas
        </Text>
      </Box>

      <Box p={4}>
        <Accordion defaultIndex={[0, 1, 2]} allowMultiple>
          {categories.map((category) => {
            const nodeTypes = getNodeTypesByCategory(category)
            const CategoryIcon = categoryIcons[category]
            const color = categoryColors[category]

            return (
              <AccordionItem key={category} border="none" mb={4}>
                <AccordionButton
                  px={3}
                  py={2}
                  borderRadius="md"
                  _hover={{ bg: useColorModeValue('gray.50', 'gray.700') }}
                  _expanded={{ bg: useColorModeValue('gray.100', 'gray.700') }}
                >
                  <HStack flex="1" spacing={2}>
                    <Icon as={CategoryIcon} color={`${color}.500`} w={4} h={4} />
                    <Text
                      fontSize="sm"
                      fontWeight="semibold"
                      textTransform="capitalize"
                      color={useColorModeValue('gray.700', 'gray.300')}
                    >
                      {category}
                    </Text>
                    <Badge colorScheme={color} ml="auto">
                      {nodeTypes.length}
                    </Badge>
                  </HStack>
                  <AccordionIcon />
                </AccordionButton>

                <AccordionPanel px={3} pb={2}>
                  <VStack spacing={2} align="stretch">
                    {nodeTypes.map((nodeType) => (
                      <Box
                        key={nodeType.id}
                        as={MotionBox}
                        draggable
                        onDragStart={(e: React.DragEvent) => onDragStart(nodeType.id, e)}
                        p={3}
                        bg={useColorModeValue(`${color}.50`, `${color}.900`)}
                        borderWidth="1px"
                        borderColor={useColorModeValue(`${color}.300`, `${color}.700`)}
                        borderRadius="lg"
                        cursor="grab"
                        _hover={{
                          bg: useColorModeValue(`${color}.100`, `${color}.800`),
                          transform: 'translateY(-2px)',
                          boxShadow: 'md',
                        }}
                        _active={{ cursor: 'grabbing' }}
                        whileHover={{ scale: 1.02 }}
                        whileTap={{ scale: 0.98 }}
                      >
                        <Text fontSize="sm" fontWeight="medium" color={useColorModeValue('gray.900', 'white')}>
                          {nodeType.label}
                        </Text>
                        <Text fontSize="xs" color={useColorModeValue('gray.600', 'gray.400')} mt={1}>
                          {nodeType.description}
                        </Text>
                      </Box>
                    ))}
                  </VStack>
                </AccordionPanel>
              </AccordionItem>
            )
          })}
        </Accordion>
      </Box>
    </Box>
  )
}

