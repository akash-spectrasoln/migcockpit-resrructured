/**
 * SourceDetailsPanel - Right panel showing source connection details
 */
import React from 'react'
import {
  Box,
  VStack,
  HStack,
  Text,
  Badge,
  Divider,
  Heading,
  Code,
} from '@chakra-ui/react'
import { useColorModeValue } from '../../../hooks/useColorModeValue'
import { Database, Server, User, Key, Globe } from 'lucide-react'

interface Source {
  source_id: number
  source_name: string
  db_type: string
  hostname?: string
  port?: number
  database?: string
  schema?: string
  user?: string
  service_name?: string
  created_on?: string
  is_active: boolean
}

interface SourceDetailsPanelProps {
  source: Source | null
}

export const SourceDetailsPanel: React.FC<SourceDetailsPanelProps> = ({ source }) => {
  const bg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const textColor = useColorModeValue('gray.600', 'gray.400')

  if (!source) {
    return (
      <Box
        w="320px"
        h="100%"
        bg={bg}
        borderLeftWidth="1px"
        borderColor={borderColor}
        display="flex"
        alignItems="center"
        justifyContent="center"
      >
        <Text fontSize="sm" color={textColor}>
          Select a source connection to view details
        </Text>
      </Box>
    )
  }

  return (
    <Box
      w="320px"
      h="100%"
      bg={bg}
      borderLeftWidth="1px"
      borderColor={borderColor}
      overflowY="auto"
    >
      <Box p={4} borderBottomWidth="1px" borderColor={borderColor}>
        <Heading size="md" mb={2}>
          Connection Details
        </Heading>
        <HStack spacing={2} mb={2}>
          <Badge colorScheme="blue" fontSize="sm">
            {source.db_type?.toUpperCase() || 'UNKNOWN'}
          </Badge>
          {source.is_active ? (
            <Badge colorScheme="green" fontSize="sm">
              Active
            </Badge>
          ) : (
            <Badge colorScheme="gray" fontSize="sm">
              Inactive
            </Badge>
          )}
        </HStack>
        <Text fontSize="lg" fontWeight="semibold" mt={2}>
          {source.source_name}
        </Text>
      </Box>

      <VStack align="stretch" spacing={4} p={4}>
        {/* Connection Information */}
        <Box>
          <Text fontSize="sm" fontWeight="semibold" mb={2} color={textColor}>
            Connection Information
          </Text>
          <VStack align="stretch" spacing={2}>
            {source.hostname && (
              <HStack spacing={2}>
                <Server size={14} />
                <Text fontSize="sm" flex={1}>
                  Host:
                </Text>
                <Code fontSize="xs">{source.hostname}</Code>
              </HStack>
            )}
            {source.port && (
              <HStack spacing={2}>
                <Globe size={14} />
                <Text fontSize="sm" flex={1}>
                  Port:
                </Text>
                <Code fontSize="xs">{source.port}</Code>
              </HStack>
            )}
            {source.database && (
              <HStack spacing={2}>
                <Database size={14} />
                <Text fontSize="sm" flex={1}>
                  Database:
                </Text>
                <Code fontSize="xs">{source.database}</Code>
              </HStack>
            )}
            {source.schema && (
              <HStack spacing={2}>
                <Database size={14} />
                <Text fontSize="sm" flex={1}>
                  Schema:
                </Text>
                <Code fontSize="xs">{source.schema}</Code>
              </HStack>
            )}
            {source.user && (
              <HStack spacing={2}>
                <User size={14} />
                <Text fontSize="sm" flex={1}>
                  User:
                </Text>
                <Code fontSize="xs">{source.user}</Code>
              </HStack>
            )}
            {source.service_name && (
              <HStack spacing={2}>
                <Key size={14} />
                <Text fontSize="sm" flex={1}>
                  Service Name:
                </Text>
                <Code fontSize="xs">{source.service_name}</Code>
              </HStack>
            )}
          </VStack>
        </Box>

        <Divider />

        {/* Metadata */}
        <Box>
          <Text fontSize="sm" fontWeight="semibold" mb={2} color={textColor}>
            Metadata
          </Text>
          <VStack align="stretch" spacing={2}>
            {source.created_on && (
              <HStack spacing={2}>
                <Text fontSize="sm" flex={1} color={textColor}>
                  Created:
                </Text>
                <Text fontSize="sm">
                  {new Date(source.created_on).toLocaleDateString()}
                </Text>
              </HStack>
            )}
            <HStack spacing={2}>
              <Text fontSize="sm" flex={1} color={textColor}>
                Connection ID:
              </Text>
              <Code fontSize="xs">{source.source_id}</Code>
            </HStack>
          </VStack>
        </Box>
      </VStack>
    </Box>
  )
}

