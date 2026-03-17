/**
 * Canvas Query Options
 * React Query hooks for Canvas operations
 */
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { canvasApi } from '../../services/api'
import { QueryKeys } from '../../constants/common'

// Types
interface Canvas {
  id: number
  name: string
  configuration?: any
  created_on?: string
  updated_on?: string
  project_id?: number
}

interface CreateCanvasPayload {
  name: string
  configuration?: any
  project_id?: number
}

interface UpdateCanvasPayload {
  id: number
  data: any
}

// Query Hooks
export const useFetchCanvas = (canvasId?: number) => {
  return useQuery({
    queryKey: QueryKeys.canvas(canvasId!),
    queryFn: async () => {
      const response = await canvasApi.get(canvasId!)
      return response.data as Canvas
    },
    enabled: !!canvasId,
  })
}

export const useFetchCanvases = (projectId?: number) => {
  return useQuery({
    queryKey: QueryKeys.canvases(projectId),
    queryFn: async () => {
      const response = await canvasApi.list(projectId)
      return response.data as Canvas[]
    },
  })
}

// Mutation Hooks
export const useCreateCanvas = () => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async (payload: CreateCanvasPayload) => {
      const response = await canvasApi.create(payload)
      return response.data as Canvas
    },
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['canvases'] })
      if (variables.project_id) {
        queryClient.invalidateQueries({ queryKey: QueryKeys.canvases(variables.project_id) })
      }
    },
  })
}

export const useUpdateCanvas = () => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async ({ id, data }: UpdateCanvasPayload) => {
      const response = await canvasApi.saveConfiguration(id, data)
      return response.data
    },
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: QueryKeys.canvas(variables.id) })
      queryClient.invalidateQueries({ queryKey: ['canvases'] })
    },
  })
}

export const useDeleteCanvas = () => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async (id: number) => {
      const response = await canvasApi.delete(id)
      return response.data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['canvases'] })
    },
  })
}

// Re-export legacy hooks for backward compatibility
export { useCanvas, useCanvases } from '../../hooks/useCanvas'

