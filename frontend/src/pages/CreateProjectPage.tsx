/**
 * Create Project Page - Form to create a new project
 */
import React, { useState } from 'react'
import {
  Box,
  Flex,
  Heading,
  Text,
  Button,
  VStack,
  FormControl,
  FormLabel,
  Input,
  Textarea,
  useColorModeValue,
  HStack,
  Alert,
  AlertIcon,
} from '@chakra-ui/react'
import { useNavigate } from 'react-router-dom'
import { useMutation } from '@tanstack/react-query'
import { projectApi } from '../services/api'
import { ArrowLeft } from 'lucide-react'

export const CreateProjectPage: React.FC = () => {
  const navigate = useNavigate()
  const [projectName, setProjectName] = useState('')
  const [description, setDescription] = useState('')
  const [error, setError] = useState<string | null>(null)

  const bg = useColorModeValue('gray.50', 'gray.900')
  const cardBg = useColorModeValue('white', 'gray.800')
  const textColor = useColorModeValue('gray.800', 'white')
  const subtextColor = useColorModeValue('gray.600', 'gray.300')

  const createMutation = useMutation({
    mutationFn: (data: { project_name: string; description?: string }) =>
      projectApi.create(data),
    onSuccess: (response) => {
      const projectId = response.data.id
      navigate(`/projects/${projectId}/dashboard`)
    },
    onError: (err: any) => {
      setError(
        err.response?.data?.error ||
        err.response?.data?.detail ||
        err.message ||
        'Failed to create project'
      )
    },
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)

    if (!projectName.trim()) {
      setError('Project name is required')
      return
    }

    createMutation.mutate({
      project_name: projectName.trim(),
      description: description.trim() || undefined,
    })
  }

  return (
    <Box w="100%" h="100vh" bg={bg} display="flex" flexDirection="column">
      {/* Header */}
      <Box
        px={8}
        py={4}
        borderBottomWidth="1px"
        borderColor={useColorModeValue('gray.200', 'gray.700')}
        bg={useColorModeValue('white', 'gray.800')}
      >
        <Flex align="center" spacing={4}>
          <Button
            variant="ghost"
            leftIcon={<ArrowLeft size={16} />}
            onClick={() => navigate('/projects')}
            mr={4}
          >
            Back to Projects
          </Button>
          <VStack align="flex-start" spacing={0}>
            <Heading size="md" color={textColor}>
              Create New Project
            </Heading>
            <Text fontSize="sm" color={subtextColor}>
              Organize your migration work into a project
            </Text>
          </VStack>
        </Flex>
      </Box>

      {/* Content */}
      <Box flex={1} px={8} py={6} overflowY="auto">
        <Flex justify="center" align="flex-start" minH="100%">
          <Box w="100%" maxW="600px" mt={8}>
            <Box
              bg={cardBg}
              borderWidth="1px"
              borderColor={useColorModeValue('gray.200', 'gray.700')}
              borderRadius="md"
              p={6}
            >
              <form onSubmit={handleSubmit}>
                <VStack align="stretch" spacing={6}>
                  <FormControl isRequired>
                    <FormLabel color={textColor}>Project Name</FormLabel>
                    <Input
                      value={projectName}
                      onChange={(e) => setProjectName(e.target.value)}
                      placeholder="e.g., Customer Migration 2024"
                      size="lg"
                    />
                  </FormControl>

                  <FormControl>
                    <FormLabel color={textColor}>Description</FormLabel>
                    <Textarea
                      value={description}
                      onChange={(e) => setDescription(e.target.value)}
                      placeholder="Describe what this project is for..."
                      rows={4}
                    />
                  </FormControl>

                  {error && (
                    <Alert status="error">
                      <AlertIcon />
                      {error}
                    </Alert>
                  )}

                  <HStack justify="flex-end" pt={4}>
                    <Button
                      variant="outline"
                      onClick={() => navigate('/projects')}
                      isDisabled={createMutation.isPending}
                    >
                      Cancel
                    </Button>
                    <Button
                      type="submit"
                      colorScheme="brand"
                      isLoading={createMutation.isPending}
                      loadingText="Creating..."
                    >
                      Create Project
                    </Button>
                  </HStack>
                </VStack>
              </form>
            </Box>
          </Box>
        </Flex>
      </Box>
    </Box>
  )
}

