/**
 * Migration Jobs Monitoring Page
 * Lists, filters, and monitors migration jobs with real-time updates.
 * Uses Chakra UI to align with the rest of the app.
 */
import React, { useEffect, useState, useRef } from 'react'
import { useNavigate, Link as RouterLink } from 'react-router-dom'
import {
  Box,
  Flex,
  Heading,
  Text,
  Button,
  HStack,
  VStack,
  IconButton,
  useDisclosure,
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  TableContainer,
  Badge,
  Spinner,
  Input,
  Select,
  Progress,
  useToast,
  Collapse,
  FormControl,
  FormLabel,
  Divider,
  AlertDialog,
  AlertDialogBody,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogContent,
  AlertDialogOverlay,
} from '@chakra-ui/react'
import { useColorModeValue } from '../hooks/useColorModeValue'
import { migrationApi } from '../services/api'
import { useAuthStore } from '../store/authStore'
import { useCanvasStore } from '../store/canvasStore'
import { wsService, JobUpdateMessage } from '../services/websocket'
import { ClientRoutes } from '../constants/client-routes'
import { LogOut, Filter, RefreshCw, Square, Eye, X, ArrowLeft, List } from 'lucide-react'

interface MigrationJob {
  id: number
  job_id: string
  canvas: {
    id: number
    name: string
  }
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled'
  progress: number
  current_step: string | null
  error_message: string | null
  created_on: string
  started_on: string | null
  completed_on: string | null
  stats: Record<string, any> | null
}

const statusColorMap: Record<string, string> = {
  completed: 'green',
  running: 'blue',
  pending: 'yellow',
  failed: 'red',
  cancelled: 'gray',
}

export const JobsPage: React.FC = () => {
  const navigate = useNavigate()
  const toast = useToast()
  const { isAuthenticated, logout, checkAuth } = useAuthStore()
  const [jobs, setJobs] = useState<MigrationJob[]>([])
  const [selectedJob, setSelectedJob] = useState<MigrationJob | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [filters, setFilters] = useState({
    status: '',
    search: '',
    dateFrom: '',
    dateTo: '',
  })
  const [jobLogs, setJobLogs] = useState<any[]>([])
  const [loadingLogs, setLoadingLogs] = useState(false)
  const { updateNodeStatus, updateJobProgress } = useCanvasStore()
  const wsConnectedRef = useRef<Set<string>>(new Set())
  const { isOpen: isFiltersOpen, onToggle: onToggleFilters } = useDisclosure({ defaultIsOpen: false })
  const { isOpen: isCancelOpen, onOpen: onCancelOpen, onClose: onCancelClose } = useDisclosure()
  const [jobToCancel, setJobToCancel] = useState<MigrationJob | null>(null)
  const cancelRef = useRef<HTMLButtonElement>(null)

  const bg = useColorModeValue('gray.50', 'gray.900')
  const headerBg = useColorModeValue('white', 'gray.800')
  const cardBg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const textColor = useColorModeValue('gray.800', 'white')
  const subtextColor = useColorModeValue('gray.600', 'gray.300')
  const hoverBg = useColorModeValue('gray.50', 'gray.700')
  const selectedBg = useColorModeValue('blue.50', 'blue.900')
  const tableHeaderBg = useColorModeValue('gray.50', 'gray.800')

  useEffect(() => {
    const init = async () => {
      await checkAuth()
      if (!isAuthenticated) {
        navigate('/login')
        return
      }
      loadJobs()
    }
    init()
  }, [isAuthenticated, checkAuth, navigate])

  useEffect(() => {
    const runningJobs = jobs.filter(
      (job) =>
        (job.status === 'running' || job.status === 'pending') &&
        !wsConnectedRef.current.has(job.job_id)
    )

    runningJobs.forEach((job) => {
      wsConnectedRef.current.add(job.job_id)
      wsService.subscribeToJobUpdates(job.job_id, {
        onStatus: (data: JobUpdateMessage) => {
          setJobs((prevJobs) =>
            prevJobs.map((j) =>
              j.job_id === job.job_id
                ? {
                    ...j,
                    status: (data.status as any) || j.status,
                    progress: data.progress ?? j.progress,
                    current_step: data.current_step || j.current_step,
                  }
                : j
            )
          )
        },
        onNodeProgress: (data: JobUpdateMessage) => {
          if (data.node_id) {
            const status = data.status === 'completed' ? 'success' : data.status === 'failed' ? 'error' : (data.status as any) || 'running'
            updateNodeStatus(data.node_id, status)
            if (data.progress !== undefined) {
              updateJobProgress(data.node_id, data.progress)
            }
          }
        },
        onComplete: (data: JobUpdateMessage) => {
          setJobs((prevJobs) =>
            prevJobs.map((j) =>
              j.job_id === job.job_id
                ? {
                    ...j,
                    status: 'completed',
                    progress: 100,
                    current_step: 'Completed',
                    stats: data.stats || j.stats,
                  }
                : j
            )
          )
          wsConnectedRef.current.delete(job.job_id)
          wsService.unsubscribeFromJobUpdates(job.job_id)
        },
        onError: (data: JobUpdateMessage) => {
          setJobs((prevJobs) =>
            prevJobs.map((j) =>
              j.job_id === job.job_id
                ? { ...j, status: 'failed', error_message: data.error || 'Unknown error' }
                : j
            )
          )
          wsConnectedRef.current.delete(job.job_id)
          wsService.unsubscribeFromJobUpdates(job.job_id)
        },
        onCancelled: () => {
          setJobs((prevJobs) =>
            prevJobs.map((j) =>
              j.job_id === job.job_id ? { ...j, status: 'cancelled' } : j
            )
          )
          wsConnectedRef.current.delete(job.job_id)
          wsService.unsubscribeFromJobUpdates(job.job_id)
        },
      })
    })

    const completedJobs = jobs.filter(
      (job) =>
        (job.status === 'completed' || job.status === 'failed' || job.status === 'cancelled') &&
        wsConnectedRef.current.has(job.job_id)
    )
    completedJobs.forEach((job) => {
      wsConnectedRef.current.delete(job.job_id)
      wsService.unsubscribeFromJobUpdates(job.job_id)
    })

    return () => {
      jobs.forEach((job) => {
        if (wsConnectedRef.current.has(job.job_id)) {
          wsService.unsubscribeFromJobUpdates(job.job_id)
        }
      })
      wsConnectedRef.current.clear()
    }
  }, [jobs, updateNodeStatus, updateJobProgress])

  useEffect(() => {
    const interval = setInterval(() => {
      const hasRunningJobs = jobs.some(
        (job) =>
          (job.status === 'running' || job.status === 'pending') &&
          !wsConnectedRef.current.has(job.job_id)
      )
      if (hasRunningJobs) loadJobs(true)
    }, 5000)
    return () => clearInterval(interval)
  }, [jobs])

  const loadJobs = async (silent = false) => {
    if (!silent) setLoading(true)
    else setRefreshing(true)
    try {
      const response = await migrationApi.getAll()
      setJobs(response.data)
    } catch (error) {
      console.error('Error loading jobs:', error)
      toast({ title: 'Failed to load jobs', status: 'error', isClosable: true })
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }

  const loadJobLogs = async (jobId: number) => {
    setLoadingLogs(true)
    try {
      const response = await migrationApi.getLogs(jobId)
      setJobLogs(response.data)
    } catch (error) {
      console.error('Error loading logs:', error)
    } finally {
      setLoadingLogs(false)
    }
  }

  const handleJobSelect = async (job: MigrationJob) => {
    setSelectedJob(job)
    if (job.status === 'running' || job.status === 'pending') {
      loadJobLogs(job.id)
    }
  }

  const handleCancelJobClick = (job: MigrationJob) => {
    setJobToCancel(job)
    onCancelOpen()
  }

  const handleCancelJobConfirm = async () => {
    if (!jobToCancel) return
    try {
      await migrationApi.cancel(jobToCancel.id)
      loadJobs()
      onCancelClose()
      setJobToCancel(null)
      toast({ title: 'Job cancelled', status: 'info', isClosable: true })
    } catch (error) {
      console.error('Error cancelling job:', error)
      toast({ title: 'Failed to cancel job', status: 'error', isClosable: true })
    }
  }

  const filteredJobs = jobs.filter((job) => {
    if (filters.status && job.status !== filters.status) return false
    if (filters.search) {
      const searchLower = filters.search.toLowerCase()
      if (
        !job.job_id.toLowerCase().includes(searchLower) &&
        !job.canvas.name.toLowerCase().includes(searchLower)
      )
        return false
    }
    if (filters.dateFrom && new Date(job.created_on) < new Date(filters.dateFrom)) return false
    if (filters.dateTo && new Date(job.created_on) > new Date(filters.dateTo)) return false
    return true
  })

  const handleLogout = () => {
    logout()
    navigate('/login')
  }

  if (loading) {
    return (
      <Box w="100%" h="100vh" bg={bg} display="flex" alignItems="center" justifyContent="center">
        <VStack spacing={4}>
          <Spinner size="xl" colorScheme="brand" />
          <Text color={subtextColor}>Loading jobs...</Text>
        </VStack>
      </Box>
    )
  }

  return (
    <Box w="100%" h="100vh" bg={bg} display="flex" flexDirection="column" overflow="hidden">
      {/* Header - aligned with DashboardLayout */}
      <Box
        px={8}
        py={4}
        borderBottomWidth="1px"
        borderColor={borderColor}
        bg={headerBg}
        flexShrink={0}
      >
        <Flex justify="space-between" align="center">
          <HStack spacing={4}>
            <RouterLink to={ClientRoutes.dashboard.canvas}>
              <Button
                leftIcon={<ArrowLeft size={18} />}
                variant="ghost"
                size="sm"
                color={textColor}
              >
                Back to Canvas
              </Button>
            </RouterLink>
            <VStack align="flex-start" spacing={0}>
              <Heading size="lg" color={textColor}>
                Migration Jobs
              </Heading>
              <Text fontSize="sm" color={subtextColor}>
                Monitor and manage pipeline executions
              </Text>
            </VStack>
          </HStack>
          <HStack spacing={3}>
            <RouterLink to={ClientRoutes.dashboard.root}>
              <Button leftIcon={<List size={18} />} variant="ghost" size="sm">
                Dashboard
              </Button>
            </RouterLink>
            <Button
              leftIcon={<LogOut size={18} />}
              variant="outline"
              size="sm"
              onClick={handleLogout}
            >
              Logout
            </Button>
          </HStack>
        </Flex>
      </Box>

      <Flex flex={1} overflow="hidden">
        {/* Main content */}
        <Box flex={1} display="flex" flexDirection="column" overflow="hidden" minW={0}>
          {/* Toolbar */}
          <Box
            px={8}
            py={3}
            borderBottomWidth="1px"
            borderColor={borderColor}
            bg={headerBg}
            flexShrink={0}
          >
            <Flex justify="space-between" align="center" wrap="wrap" gap={3}>
              <HStack spacing={3}>
                <Button
                  leftIcon={<Filter size={16} />}
                  size="sm"
                  variant={isFiltersOpen ? 'solid' : 'outline'}
                  colorScheme={isFiltersOpen ? 'brand' : 'gray'}
                  onClick={onToggleFilters}
                >
                  Filters
                </Button>
                <Button
                  leftIcon={<RefreshCw size={16} />}
                  size="sm"
                  variant="outline"
                  onClick={() => loadJobs()}
                  isLoading={refreshing}
                  loadingText="Refreshing"
                >
                  Refresh
                </Button>
              </HStack>
              <Text fontSize="sm" color={subtextColor} fontWeight="medium">
                {filteredJobs.length} job{filteredJobs.length !== 1 ? 's' : ''}
              </Text>
            </Flex>
          </Box>

          {/* Filters */}
          <Collapse in={isFiltersOpen} animateOpacity>
            <Box
              px={8}
              py={4}
              borderBottomWidth="1px"
              borderColor={borderColor}
              bg={cardBg}
            >
              <Flex gap={6} wrap="wrap" align="flex-end">
                <FormControl w="40" minW="120px">
                  <FormLabel fontSize="xs" color={subtextColor}>Status</FormLabel>
                  <Select
                    size="sm"
                    value={filters.status}
                    onChange={(e) => setFilters({ ...filters, status: e.target.value })}
                    placeholder="All"
                    bg={useColorModeValue('white', 'gray.700')}
                  >
                    <option value="pending">Pending</option>
                    <option value="running">Running</option>
                    <option value="completed">Completed</option>
                    <option value="failed">Failed</option>
                    <option value="cancelled">Cancelled</option>
                  </Select>
                </FormControl>
                <FormControl w="56" minW="180px">
                  <FormLabel fontSize="xs" color={subtextColor}>Search</FormLabel>
                  <Input
                    size="sm"
                    value={filters.search}
                    onChange={(e) => setFilters({ ...filters, search: e.target.value })}
                    placeholder="Job ID or Canvas name"
                    bg={useColorModeValue('white', 'gray.700')}
                  />
                </FormControl>
                <FormControl w="40" minW="140px">
                  <FormLabel fontSize="xs" color={subtextColor}>From date</FormLabel>
                  <Input
                    size="sm"
                    type="date"
                    value={filters.dateFrom}
                    onChange={(e) => setFilters({ ...filters, dateFrom: e.target.value })}
                    bg={useColorModeValue('white', 'gray.700')}
                  />
                </FormControl>
                <FormControl w="40" minW="140px">
                  <FormLabel fontSize="xs" color={subtextColor}>To date</FormLabel>
                  <Input
                    size="sm"
                    type="date"
                    value={filters.dateTo}
                    onChange={(e) => setFilters({ ...filters, dateTo: e.target.value })}
                    bg={useColorModeValue('white', 'gray.700')}
                  />
                </FormControl>
              </Flex>
            </Box>
          </Collapse>

          {/* Jobs table */}
          <TableContainer flex={1} overflowY="auto" px={8} py={4}>
            <Table size="sm" variant="simple">
              <Thead bg={tableHeaderBg} position="sticky" top={0} zIndex={1}>
                <Tr>
                  <Th color={subtextColor} fontWeight="semibold" textTransform="uppercase" fontSize="xs">Job ID</Th>
                  <Th color={subtextColor} fontWeight="semibold" textTransform="uppercase" fontSize="xs">Canvas</Th>
                  <Th color={subtextColor} fontWeight="semibold" textTransform="uppercase" fontSize="xs">Status</Th>
                  <Th color={subtextColor} fontWeight="semibold" textTransform="uppercase" fontSize="xs">Progress</Th>
                  <Th color={subtextColor} fontWeight="semibold" textTransform="uppercase" fontSize="xs">Created</Th>
                  <Th color={subtextColor} fontWeight="semibold" textTransform="uppercase" fontSize="xs">Actions</Th>
                </Tr>
              </Thead>
              <Tbody>
                {filteredJobs.map((job) => (
                  <Tr
                    key={job.id}
                    onClick={() => handleJobSelect(job)}
                    cursor="pointer"
                    bg={selectedJob?.id === job.id ? selectedBg : 'transparent'}
                    _hover={{ bg: selectedJob?.id === job.id ? selectedBg : hoverBg }}
                  >
                    <Td fontWeight="medium" color={textColor} fontFamily="mono" fontSize="sm">
                      {job.job_id.substring(0, 8)}…
                    </Td>
                    <Td fontSize="sm" color={subtextColor}>{job.canvas.name}</Td>
                    <Td>
                      <Badge
                        colorScheme={statusColorMap[job.status] || 'gray'}
                        variant="subtle"
                        fontSize="xs"
                        textTransform="capitalize"
                      >
                        {job.status}
                      </Badge>
                    </Td>
                    <Td w="140px">
                      <HStack spacing={2}>
                        <Progress
                          value={job.progress}
                          size="sm"
                          flex={1}
                          colorScheme="blue"
                          borderRadius="full"
                          hasStripe={job.status === 'running' || job.status === 'pending'}
                          isAnimated={job.status === 'running' || job.status === 'pending'}
                        />
                        <Text fontSize="xs" color={subtextColor} w="8" textAlign="right">
                          {Math.round(job.progress)}%
                        </Text>
                      </HStack>
                    </Td>
                    <Td fontSize="sm" color={subtextColor} whiteSpace="nowrap">
                      {new Date(job.created_on).toLocaleString()}
                    </Td>
                    <Td onClick={(e) => e.stopPropagation()}>
                      <HStack spacing={1}>
                        <IconButton
                          aria-label="View details"
                          icon={<Eye size={16} />}
                          size="xs"
                          variant="ghost"
                          colorScheme="blue"
                          onClick={() => handleJobSelect(job)}
                        />
                        {(job.status === 'running' || job.status === 'pending') && (
                          <IconButton
                            aria-label="Cancel job"
                            icon={<Square size={16} />}
                            size="xs"
                            variant="ghost"
                            colorScheme="red"
                            onClick={() => handleCancelJobClick(job)}
                          />
                        )}
                      </HStack>
                    </Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
            {filteredJobs.length === 0 && (
              <Flex py={12} justify="center" color={subtextColor} fontSize="sm">
                No jobs match your filters.
              </Flex>
            )}
          </TableContainer>
        </Box>

        {/* Job details sidebar */}
        {selectedJob && (
          <Box
            w={{ base: '100%', md: '400px' }}
            minW={{ md: '360px' }}
            borderLeftWidth="1px"
            borderColor={borderColor}
            bg={cardBg}
            display="flex"
            flexDirection="column"
            overflow="hidden"
            flexShrink={0}
          >
            <Flex px={4} py={3} borderBottomWidth="1px" borderColor={borderColor} justify="space-between" align="center">
              <Heading size="sm" color={textColor}>Job details</Heading>
              <IconButton
                aria-label="Close"
                icon={<X size={18} />}
                size="sm"
                variant="ghost"
                onClick={() => setSelectedJob(null)}
              />
            </Flex>
            <Box flex={1} overflowY="auto" p={4}>
              <VStack align="stretch" spacing={4}>
                <Box>
                  <Text fontSize="xs" fontWeight="semibold" color={subtextColor} mb={1}>Job ID</Text>
                  <Text fontSize="sm" fontFamily="mono" color={textColor} breakAll>{selectedJob.job_id}</Text>
                </Box>
                <Box>
                  <Text fontSize="xs" fontWeight="semibold" color={subtextColor} mb={1}>Canvas</Text>
                  <Text fontSize="sm" color={textColor}>{selectedJob.canvas.name}</Text>
                </Box>
                <Box>
                  <Text fontSize="xs" fontWeight="semibold" color={subtextColor} mb={1}>Status</Text>
                  <Badge
                    colorScheme={statusColorMap[selectedJob.status] || 'gray'}
                    variant="subtle"
                    textTransform="capitalize"
                  >
                    {selectedJob.status}
                  </Badge>
                </Box>
                <Box>
                  <Text fontSize="xs" fontWeight="semibold" color={subtextColor} mb={1}>Progress</Text>
                  <Progress
                    value={selectedJob.progress}
                    size="sm"
                    colorScheme="blue"
                    borderRadius="full"
                    mb={1}
                  />
                  <Text fontSize="xs" color={subtextColor}>{Math.round(selectedJob.progress)}%</Text>
                </Box>
                {selectedJob.current_step && (
                  <Box>
                    <Text fontSize="xs" fontWeight="semibold" color={subtextColor} mb={1}>Current step</Text>
                    <Text fontSize="sm" color={textColor}>{selectedJob.current_step}</Text>
                  </Box>
                )}
                {selectedJob.error_message && (
                  <Box>
                    <Text fontSize="xs" fontWeight="semibold" color="red.500" mb={1}>Error</Text>
                    <Text fontSize="sm" color="red.600" noOfLines={4}>{selectedJob.error_message}</Text>
                  </Box>
                )}
                {selectedJob.stats && Object.keys(selectedJob.stats).length > 0 && (
                  <Box>
                    <Text fontSize="xs" fontWeight="semibold" color={subtextColor} mb={1}>Statistics</Text>
                    <Box
                      as="pre"
                      fontSize="xs"
                      p={3}
                      bg={useColorModeValue('gray.50', 'gray.700')}
                      borderRadius="md"
                      overflow="auto"
                      color={textColor}
                    >
                      {JSON.stringify(selectedJob.stats, null, 2)}
                    </Box>
                  </Box>
                )}

                {(selectedJob.status === 'running' || selectedJob.status === 'pending') && (
                  <>
                    <Divider />
                    <Box>
                      <Flex justify="space-between" align="center" mb={2}>
                        <Text fontSize="xs" fontWeight="semibold" color={subtextColor}>Logs</Text>
                        <Button
                          size="xs"
                          variant="link"
                          colorScheme="blue"
                          onClick={() => loadJobLogs(selectedJob.id)}
                          isLoading={loadingLogs}
                        >
                          Refresh
                        </Button>
                      </Flex>
                      <Box
                        bg={useColorModeValue('gray.900', 'gray.800')}
                        color="green.400"
                        p={3}
                        borderRadius="md"
                        fontSize="xs"
                        fontFamily="mono"
                        maxH="240px"
                        overflowY="auto"
                      >
                        {jobLogs.length === 0 ? (
                          <Text color="gray.500">No logs yet</Text>
                        ) : (
                          jobLogs.map((log: any, idx: number) => (
                            <Box key={idx} mb={1}>
                              <Text as="span" color="gray.500">
                                [{log.timestamp ? new Date(log.timestamp).toLocaleTimeString() : '—'}]
                              </Text>{' '}
                              <Text
                                as="span"
                                color={
                                  log.level === 'ERROR'
                                    ? 'red.400'
                                    : log.level === 'WARNING'
                                    ? 'yellow.400'
                                    : 'green.400'
                                }
                              >
                                {log.message}
                              </Text>
                            </Box>
                          ))
                        )}
                      </Box>
                    </Box>
                  </>
                )}
              </VStack>
            </Box>
          </Box>
        )}
      </Flex>

      {/* Cancel job confirmation */}
      <AlertDialog
        isOpen={isCancelOpen}
        leastDestructiveRef={cancelRef}
        onClose={() => {
          onCancelClose()
          setJobToCancel(null)
        }}
      >
        <AlertDialogOverlay>
          <AlertDialogContent>
            <AlertDialogHeader fontSize="lg" fontWeight="semibold">
              Cancel job?
            </AlertDialogHeader>
            <AlertDialogBody>
              {jobToCancel && (
                <>
                  Job <Text as="span" fontFamily="mono">{jobToCancel.job_id.substring(0, 8)}…</Text> will be
                  cancelled. This cannot be undone.
                </>
              )}
            </AlertDialogBody>
            <AlertDialogFooter>
              <Button ref={cancelRef} onClick={onCancelClose}>
                Keep running
              </Button>
              <Button colorScheme="red" onClick={handleCancelJobConfirm} ml={3}>
                Cancel job
              </Button>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialogOverlay>
      </AlertDialog>
    </Box>
  )
}
