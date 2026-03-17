/**
 * Projects List Page - Shows all projects for the customer
 */
import React from 'react'
import {
  Box,
  Flex,
  Heading,
  Text,
  Button,
  VStack,
  SimpleGrid,
  Card,
  CardHeader,
  CardBody,
  Badge,
  Spinner,
  useColorModeValue,
  HStack,
} from '@chakra-ui/react'
import { useNavigate } from 'react-router-dom'
import { useAuthStore } from '../store/authStore'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { projectApi } from '../services/api'
import { Plus, Calendar, FolderOpen } from 'lucide-react'

export const ProjectsListPage: React.FC = () => {
  const navigate = useNavigate()
  const { logout } = useAuthStore()
  const queryClient = useQueryClient()

  const bg = useColorModeValue('gray.50', 'gray.900')
  const cardBg = useColorModeValue('white', 'gray.800')
  const textColor = useColorModeValue('gray.800', 'white')
  const subtextColor = useColorModeValue('gray.600', 'gray.300')

  const { data: projectsData, isLoading } = useQuery({
    queryKey: ['projects'],
    queryFn: () => projectApi.list(),
  })

  const deleteMutation = useMutation({
    mutationFn: (id: number) => projectApi.delete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['projects'] })
    },
  })

  const projects = projectsData?.data || []

  const handleCreateProject = () => {
    navigate('/projects/new')
  }

  const handleOpenProject = (projectId: number) => {
    navigate(`/projects/${projectId}/dashboard`)
  }

  const handleDeleteProject = (e: React.MouseEvent, projectId: number, projectName: string) => {
    e.stopPropagation()
    if (window.confirm(`Are you sure you want to delete "${projectName}"? This will soft-delete the project.`)) {
      deleteMutation.mutate(projectId)
    }
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
        <Flex justify="space-between" align="center">
          <VStack align="flex-start" spacing={0}>
            <Heading size="md" color={textColor}>
              Projects
            </Heading>
            <Text fontSize="sm" color={subtextColor}>
              Organize your migration work into projects
            </Text>
          </VStack>
          <HStack spacing={3}>
            <Button
              colorScheme="brand"
              leftIcon={<Plus size={16} />}
              onClick={handleCreateProject}
            >
              Create New Project
            </Button>
            <Button variant="outline" onClick={logout}>
              Logout
            </Button>
          </HStack>
        </Flex>
      </Box>

      {/* Content */}
      <Box flex={1} px={8} py={6} overflowY="auto">
        {isLoading ? (
          <Flex justify="center" align="center" h="400px">
            <VStack spacing={4}>
              <Spinner size="xl" />
              <Text color={subtextColor}>Loading projects...</Text>
            </VStack>
          </Flex>
        ) : projects.length > 0 ? (
          <SimpleGrid columns={{ base: 1, md: 2, lg: 3 }} spacing={6}>
            {projects.map((project: any) => (
              <Card
                key={project.id}
                bg={cardBg}
                borderWidth="1px"
                borderColor={useColorModeValue('gray.200', 'gray.700')}
                cursor="pointer"
                _hover={{
                  borderColor: useColorModeValue('brand.400', 'brand.500'),
                  shadow: 'md',
                  transform: 'translateY(-2px)',
                }}
                transition="all 0.2s"
                onClick={() => handleOpenProject(project.id)}
              >
                <CardHeader>
                  <Flex justify="space-between" align="start">
                    <VStack align="flex-start" spacing={1} flex={1}>
                      <Heading size="sm" color={textColor}>
                        {project.project_name}
                      </Heading>
                      {project.description && (
                        <Text fontSize="sm" color={subtextColor} noOfLines={2}>
                          {project.description}
                        </Text>
                      )}
                    </VStack>
                    <Button
                      size="xs"
                      variant="ghost"
                      colorScheme="red"
                      onClick={(e) => handleDeleteProject(e, project.id, project.project_name)}
                    >
                      Delete
                    </Button>
                  </Flex>
                </CardHeader>
                <CardBody>
                  <VStack align="stretch" spacing={3}>
                    <HStack justify="space-between">
                      <HStack spacing={2}>
                        <FolderOpen size={16} color={useColorModeValue('gray.600', 'gray.400')} />
                        <Text fontSize="sm" color={subtextColor}>
                          {project.canvas_count || 0} Canvas{project.canvas_count !== 1 ? 'es' : ''}
                        </Text>
                      </HStack>
                      {project.created_on && (
                        <HStack spacing={1}>
                          <Calendar size={14} color={useColorModeValue('gray.600', 'gray.400')} />
                          <Text fontSize="xs" color={subtextColor}>
                            {new Date(project.created_on).toLocaleDateString()}
                          </Text>
                        </HStack>
                      )}
                    </HStack>
                    <Badge
                      colorScheme={project.is_active ? 'green' : 'gray'}
                      fontSize="xs"
                      w="fit-content"
                    >
                      {project.is_active ? 'Active' : 'Inactive'}
                    </Badge>
                  </VStack>
                </CardBody>
              </Card>
            ))}
          </SimpleGrid>
        ) : (
          <Flex justify="center" align="center" h="400px">
            <VStack spacing={6}>
              <FolderOpen size={64} color={useColorModeValue('gray.400', 'gray.600')} />
              <VStack spacing={2}>
                <Heading size="md" color={textColor}>
                  No Projects Yet
                </Heading>
                <Text fontSize="sm" color={subtextColor} textAlign="center">
                  Create your first project to organize your migration work
                </Text>
              </VStack>
              <Button
                colorScheme="brand"
                leftIcon={<Plus size={16} />}
                onClick={handleCreateProject}
              >
                Create Your First Project
              </Button>
            </VStack>
          </Flex>
        )}
      </Box>
    </Box>
  )
}

