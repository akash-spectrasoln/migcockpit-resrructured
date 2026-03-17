/**
 * Node Configuration Panel - Chakra UI Version
 * Right-side drawer/panel for configuring selected nodes
 */
import React, { useEffect, useState } from 'react'
import {
  Drawer,
  DrawerBody,
  DrawerHeader,
  DrawerOverlay,
  DrawerContent,
  DrawerCloseButton,
  VStack,
  FormControl,
  FormLabel,
  Input,
  Select,
  Textarea,
  Checkbox,
  Button,
  ButtonGroup,
  Text,
  Divider,
  Alert,
  AlertIcon,
  Code,
  Box,
} from '@chakra-ui/react'
import { useColorModeValue } from '../../../hooks/useColorModeValue'
import { motion, AnimatePresence } from 'framer-motion'
import { Node } from 'reactflow'
import { useCanvasStore } from '../../../store/canvasStore'
import { getNodeTypeDefinition, ConfigField } from '../../../types/nodeRegistry'
import { connectionApi } from '../../../services/api'
import { SourceNodeConfig } from '../nodes/SourceNode'

interface NodeConfigPanelProps {
  node: Node | null
  isOpen: boolean
  onClose: () => void
}

export const NodeConfigurationPanel: React.FC<NodeConfigPanelProps> = ({
  node,
  isOpen,
  onClose,
}) => {
  const { updateNode } = useCanvasStore()
  const [config, setConfig] = useState<Record<string, any>>({})
  const [errors, setErrors] = useState<Record<string, string>>({})
  const [loading, setLoading] = useState(false)
  const [sourceOptions, setSourceOptions] = useState<{ value: string; label: string }[]>([])
  const [destinationOptions, setDestinationOptions] = useState<{ value: string; label: string }[]>([])

  const bg = useColorModeValue('white', 'gray.800')

  useEffect(() => {
    if (node) {
      setConfig(node.data.config || {})
      
      // Load connection options if needed
      const nodeType = node.type || 'source'
      if (nodeType.startsWith('source-') || nodeType === 'source') {
        loadSourceConnections()
      } else if (nodeType.startsWith('destination-') || nodeType === 'destination') {
        loadDestinationConnections()
      }
    }
  }, [node])

  const loadSourceConnections = async () => {
    try {
      const response = await connectionApi.sources()
      // API returns {customer_id, customer_name, sources: [...]}
      const sources = response.sources || response.data?.sources || response.data || []
      const options = sources.map((source: any) => ({
        value: source.source_id?.toString() || source.id?.toString() || '',
        label: `${source.source_name || source.name || 'Unknown'} (${source.db_type || 'N/A'})`,
      }))
      setSourceOptions(options)
    } catch (error) {
      console.error('Failed to load source connections:', error)
    }
  }

  const loadDestinationConnections = async () => {
    try {
      // Get project_id from URL if available so we can load project-specific destinations
      const urlParams = new URLSearchParams(window.location.search)
      const projectIdParam = urlParams.get('projectId')
      const projectId = projectIdParam ? parseInt(projectIdParam, 10) : null

      const response = projectId && !isNaN(projectId)
        ? await connectionApi.getDestinations(projectId)
        : await connectionApi.getDestinations()

      const rawData = response?.data || response
      let destinations: any[] = []

      if (Array.isArray(rawData)) {
        destinations = rawData
      } else if (Array.isArray(rawData?.destinations)) {
        destinations = rawData.destinations
      } else if (Array.isArray(rawData?.data?.destinations)) {
        destinations = rawData.data.destinations
      }

      const options = destinations.map((dest: any) => {
        const id = dest.id || dest.destination_id
        const name =
          dest.destination_name ||
          dest.name ||
          `Destination ${id ?? ''}`
        const dbType = (dest.db_type || dest.database_type || 'HANA')
          .toString()
          .toUpperCase()

        return {
          value: id ? id.toString() : '',
          label: `${name} (${dbType})`,
        }
      })

      setDestinationOptions(options)
    } catch (error) {
      console.error('Failed to load destination connections:', error)
      setDestinationOptions([])
    }
  }

  if (!node) return null

  // Use specialized source node config if it's a source node
  const isSourceNode = node.type === 'source' || node.type?.startsWith('source')
  
  if (isSourceNode) {
    return (
      <Drawer isOpen={isOpen} placement="right" onClose={onClose} size="lg">
        <DrawerOverlay />
        <DrawerContent bg={bg}>
          <DrawerHeader borderBottomWidth="1px">
            <VStack align="flex-start" spacing={1}>
              <Text fontSize="lg" fontWeight="semibold">
                Source Node Configuration
              </Text>
              <Text fontSize="xs" color="gray.500">
                Configure database connection and select tables
              </Text>
            </VStack>
          </DrawerHeader>
          <DrawerCloseButton />
          <DrawerBody>
            <SourceNodeConfig
              nodeId={node.id}
              initialConfig={node.data.config}
              onSave={(config) => {
                updateNode(node.id, {
                  data: {
                    ...node.data,
                    config,
                    label: config.tables?.length 
                      ? `${config.tables.length} table(s) from ${config.dbType}` 
                      : node.data.label || 'Source',
                    connectionType: config.dbType,
                    sourceId: config.sourceId,
                  },
                })
                onClose()
              }}
              onCancel={onClose}
            />
          </DrawerBody>
        </DrawerContent>
      </Drawer>
    )
  }

  const nodeTypeDef = getNodeTypeDefinition(node.type || '')
  if (!nodeTypeDef) {
    return (
      <Drawer isOpen={isOpen} placement="right" onClose={onClose} size="md">
        <DrawerOverlay />
        <DrawerContent bg={bg}>
          <DrawerHeader>Node Configuration</DrawerHeader>
          <DrawerCloseButton />
          <DrawerBody>
            <Text color="gray.500">Unknown node type: {node.type}</Text>
          </DrawerBody>
        </DrawerContent>
      </Drawer>
    )
  }

  const handleFieldChange = (fieldName: string, value: any) => {
    const newConfig = { ...config, [fieldName]: value }
    setConfig(newConfig)
    
    // Validate field
    const field = nodeTypeDef.configSchema.find((f) => f.name === fieldName)
    if (field?.validation) {
      const error = field.validation(value)
      setErrors((prev) => ({
        ...prev,
        [fieldName]: error || '',
      }))
    } else {
      setErrors((prev) => {
        const newErrors = { ...prev }
        delete newErrors[fieldName]
        return newErrors
      })
    }
  }

  const handleSave = () => {
    // Validate all required fields
    const newErrors: Record<string, string> = {}
    nodeTypeDef.configSchema.forEach((field) => {
      if (field.required && !config[field.name]) {
        newErrors[field.name] = `${field.label} is required`
      }
    })

    if (Object.keys(newErrors).length > 0) {
      setErrors(newErrors)
      return
    }

    // Update node with new config
    updateNode(node.id, {
      data: {
        ...node.data,
        config,
        label: config.label || node.data.label || nodeTypeDef.label,
      },
    })

    onClose()
  }

  const renderField = (field: ConfigField) => {
    const value = config[field.name] ?? field.defaultValue ?? ''
    const error = errors[field.name]
    const options = field.name === 'sourceId' 
      ? sourceOptions 
      : field.name === 'destinationId'
      ? destinationOptions
      : field.options || []

    const baseProps = {
      value: value,
      onChange: (e: any) => {
        const newValue = field.type === 'number' 
          ? (e.target.value ? Number(e.target.value) : null)
          : field.type === 'checkbox'
          ? e.target.checked
          : e.target.value
        handleFieldChange(field.name, newValue)
      },
      isInvalid: !!error,
      focusBorderColor: 'brand.500',
    }

    switch (field.type) {
      case 'text':
        return <Input {...baseProps} placeholder={field.placeholder} />
      
      case 'number':
        return <Input {...baseProps} type="number" placeholder={field.placeholder} />
      
      case 'select':
        return (
          <Select {...baseProps} placeholder="Select...">
            {options.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </Select>
        )
      
      case 'textarea':
        return <Textarea {...baseProps} placeholder={field.placeholder} rows={3} />
      
      case 'checkbox':
        return <Checkbox {...baseProps} isChecked={value} />
      
      case 'json':
        return (
          <Box>
            <Textarea
              {...baseProps}
              value={typeof value === 'string' ? value : JSON.stringify(value, null, 2)}
              onChange={(e) => {
                try {
                  const parsed = JSON.parse(e.target.value)
                  handleFieldChange(field.name, parsed)
                } catch {
                  handleFieldChange(field.name, e.target.value)
                }
              }}
              placeholder={field.placeholder}
              rows={6}
              fontFamily="mono"
              fontSize="sm"
            />
          </Box>
        )
      
      default:
        return null
    }
  }

  return (
    <Drawer isOpen={isOpen} placement="right" onClose={onClose} size="md">
      <DrawerOverlay />
      <DrawerContent bg={bg}>
        <DrawerHeader borderBottomWidth="1px">
          <VStack align="flex-start" spacing={1}>
            <Text fontSize="lg" fontWeight="semibold">
              {nodeTypeDef.label}
            </Text>
            <Text fontSize="xs" color="gray.500">
              {nodeTypeDef.description}
            </Text>
          </VStack>
        </DrawerHeader>
        <DrawerCloseButton />

        <DrawerBody>
          <AnimatePresence>
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
            >
              <VStack spacing={4} align="stretch">
                {nodeTypeDef.configSchema.map((field) => (
                  <FormControl key={field.name} isRequired={field.required} isInvalid={!!errors[field.name]}>
                    <FormLabel fontSize="sm" fontWeight="medium">
                      {field.label}
                    </FormLabel>
                    {renderField(field)}
                    {errors[field.name] && (
                      <Text fontSize="xs" color="red.500" mt={1}>
                        {errors[field.name]}
                      </Text>
                    )}
                  </FormControl>
                ))}
              </VStack>
            </motion.div>
          </AnimatePresence>
        </DrawerBody>

        <Box p={4} borderTopWidth="1px">
          <ButtonGroup spacing={2} w="100%">
            <Button variant="outline" onClick={onClose} flex={1}>
              Cancel
            </Button>
            <Button colorScheme="brand" onClick={handleSave} flex={1}>
              Save
            </Button>
          </ButtonGroup>
        </Box>
      </DrawerContent>
    </Drawer>
  )
}

