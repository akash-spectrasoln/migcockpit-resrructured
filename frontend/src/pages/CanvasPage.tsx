/**
 * Canvas Page - Chakra UI Version
 * Main page for the data migration canvas
 */
import React, { useEffect, useRef } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import {
  Box,
  Flex,
  Heading,
  HStack,
  Button,
  Spinner,
} from '@chakra-ui/react'
import { useColorModeValue } from '../hooks/useColorModeValue'
import { LogOut, BarChart3, ArrowLeft } from 'lucide-react'
import { DataFlowCanvas } from '../components/canvas/DataFlowCanvas'
import { useAuthStore } from '../store/authStore'
import { useCanvasStore } from '../store/canvasStore'
import { useCanvases } from '../hooks/useCanvas'
import { Node, Edge } from 'reactflow'

export const CanvasPage: React.FC = () => {
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const { isAuthenticated, logout, checkAuth } = useAuthStore()
  const { setNodes, setEdges, setCanvas, canvasId } = useCanvasStore()
  const isDirty = useCanvasStore((s) => s.isDirty)
  const loadedCanvasIdRef = useRef<number | null>(null)
  const { data: canvasesData, isLoading } = useCanvases()


  // Get sourceId from URL params
  const sourceIdParam = searchParams.get('sourceId')
  const sourceId = sourceIdParam ? parseInt(sourceIdParam, 10) : undefined

  // All hooks must be called unconditionally at the top level
  const headerBg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const bgColor = useColorModeValue('gray.50', 'gray.900')
  const textColor = useColorModeValue('gray.800', 'white')

  useEffect(() => {
    const init = async () => {
      // Only check auth if not already authenticated
      // This prevents redirect loops after successful login
      if (!isAuthenticated) {
        await checkAuth()
        // After checkAuth, wait a moment and check again
        setTimeout(() => {
          const currentAuth = useAuthStore.getState().isAuthenticated
          if (!currentAuth) {
            navigate('/login', { replace: true })
          }
        }, 100)
      }
    }
    init()
  }, []) // Only run once on mount

  // Get canvasId and projectId from URL params
  const canvasIdParam = searchParams.get('canvasId')
  const canvasIdFromUrl = canvasIdParam ? parseInt(canvasIdParam, 10) : null
  const projectIdParam = searchParams.get('projectId')
  const projectIdFromUrl = projectIdParam ? parseInt(projectIdParam, 10) : undefined

  useEffect(() => {
    // Only load canvas if canvasId is explicitly provided in URL
    // Don't auto-load latest canvas - this causes issues when creating new canvases
    if (canvasIdFromUrl) {
      // Normalize canvases list from React Query (canvasApi.list() returns unwrapped array)
      const canvasList: any[] = Array.isArray(canvasesData)
        ? (canvasesData as any[])
        : (canvasesData as any)?.results || (canvasesData as any)?.data || []

      // Find the canvas in the list
      const targetCanvas = canvasList.find((c: any) => c.id === canvasIdFromUrl)

      if (targetCanvas) {
        setCanvas(targetCanvas.id, targetCanvas.name)

        // Prevent background refetch from overwriting unsaved edits.
        // If we've already hydrated this canvas and the user has local changes,
        // keep the in-memory graph instead of reloading from the saved config.
        if (loadedCanvasIdRef.current === targetCanvas.id && isDirty) {
          return
        }

        // Load canvas configuration
        const config = targetCanvas.configuration || {}
        const nodes: Node[] = (config.nodes || []).map((n: any) => ({
          id: n.id,
          type: n.type || n.data?.type || 'source',
          position: n.position || { x: 0, y: 0 },
          data: {
            ...n.data,
            config: n.data?.config || n.config || {},
            type: n.data?.type || n.type,
            label: n.data?.label || n.label,
            business_name: n.data?.business_name || n.business_name,
            technical_name: n.data?.technical_name || n.technical_name,
          },
        }))
        const edges: Edge[] = (config.edges || []).map((e: any) => ({
          id: e.id || `${e.source}-${e.target}`,
          source: e.source,
          target: e.target,
          sourceHandle: e.sourceHandle,
          targetHandle: e.targetHandle,
        }))

        // Deduplicate edges by ID to prevent React warnings
        const edgeMap = new Map<string, Edge>()
        edges.forEach(edge => {
          if (!edgeMap.has(edge.id)) {
            edgeMap.set(edge.id, edge)
          } else {
            console.warn(`[CANVAS LOAD] Duplicate edge detected and removed: ${edge.id}`)
          }
        })
        const dedupedEdges = Array.from(edgeMap.values())

        setNodes(nodes)
        setEdges(dedupedEdges)
        loadedCanvasIdRef.current = targetCanvas.id
        // Schema drift detection runs inside DataFlowCanvas once nodes are in the store
      }
    } else {
      // No canvasId in URL - this is a new canvas, clear the store
      setNodes([])
      setEdges([])
      setCanvas(null, '')
      loadedCanvasIdRef.current = null
    }
  }, [canvasIdFromUrl, canvasesData, setCanvas, setNodes, setEdges, isDirty])


  const handleLogout = () => {
    logout()
    navigate('/login')
  }

  if (isLoading) {
    return (
      <Box w="100%" h="100vh" display="flex" alignItems="center" justifyContent="center">
        <Spinner size="xl" color="brand.500" thickness="4px" />
      </Box>
    )
  }

  return (
    <Box w="100%" h="100vh" display="flex" flexDirection="column" bg={bgColor}>
      {/* Header */}
      <Box
        bg={headerBg}
        borderBottomWidth="1px"
        borderColor={borderColor}
        px={6}
        py={4}
      >
        <Flex alignItems="center" justifyContent="space-between">
          <Heading size="lg" color={textColor}>
            Data Migration Canvas
          </Heading>
          <HStack spacing={4}>
            <Button
              leftIcon={<ArrowLeft />}
              variant="ghost"
              onClick={() => navigate('/dashboard')}
            >
              Dashboard
            </Button>
            <Button
              leftIcon={<BarChart3 />}
              variant="ghost"
              onClick={() => navigate('/jobs')}
            >
              Jobs
            </Button>
            <Button
              leftIcon={<LogOut />}
              variant="ghost"
              onClick={handleLogout}
            >
              Logout
            </Button>
          </HStack>
        </Flex>
      </Box>

      {/* Canvas Area */}
      <Box flex={1} overflow="hidden">
        <DataFlowCanvas
          canvasId={canvasIdFromUrl || canvasId || undefined}
          sourceId={sourceId}
          projectId={projectIdFromUrl}
          initialNodes={canvasIdFromUrl ? undefined : []}
          initialEdges={canvasIdFromUrl ? undefined : []}
        />
      </Box>
    </Box>
  )
}

