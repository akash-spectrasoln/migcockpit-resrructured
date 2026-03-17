/**
 * React Query hooks for Canvas operations
 */
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { canvasApi } from '../services/api'

export const useCanvas = (canvasId?: number) => {
  return useQuery({
    queryKey: ['canvas', canvasId],
    queryFn: () => canvasApi.get(canvasId!),
    enabled: !!canvasId,
  })
}

export const useCanvases = () => {
  return useQuery({
    queryKey: ['canvases'],
    queryFn: () => canvasApi.list(),
  })
}

export const useCreateCanvas = () => {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: (data: { name: string; configuration: any }) => canvasApi.create(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['canvases'] })
    },
  })
}

export const useUpdateCanvas = () => {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: ({ id, data }: { id: number; data: any }) => 
      canvasApi.saveConfiguration(id, data),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['canvas', variables.id] })
      queryClient.invalidateQueries({ queryKey: ['canvases'] })
    },
  })
}

export const useDeleteCanvas = () => {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: (id: number) => canvasApi.delete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['canvases'] })
    },
  })
}

