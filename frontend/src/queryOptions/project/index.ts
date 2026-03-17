/**
 * Project Query Options
 * React Query hooks for Project operations
 */
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { projectApi } from '../../services/api'
import { QueryKeys } from '../../constants/common'

// Types
interface Project {
  id: number
  project_name: string
  description?: string
  is_active?: boolean
  created_on?: string
  updated_on?: string
  canvas_count?: number
}

interface CreateProjectPayload {
  project_name: string
  description?: string
}

interface UpdateProjectPayload {
  id: number
  data: Partial<CreateProjectPayload>
}

// Query Hooks
export const useFetchProject = (projectId?: number) => {
  return useQuery({
    queryKey: QueryKeys.project(projectId!),
    queryFn: async () => {
      const response = await projectApi.get(projectId!)
      return response.data as Project
    },
    enabled: !!projectId,
  })
}

export const useFetchProjects = () => {
  return useQuery({
    queryKey: QueryKeys.projects,
    queryFn: async () => {
      const response = await projectApi.list()
      return response.data as Project[]
    },
  })
}

export const useFetchProjectCanvases = (projectId: number) => {
  return useQuery({
    queryKey: [...QueryKeys.project(projectId), 'canvases'],
    queryFn: async () => {
      const response = await projectApi.getCanvases(projectId)
      return response.data
    },
    enabled: !!projectId,
  })
}

export const useFetchProjectStats = (projectId: number) => {
  return useQuery({
    queryKey: [...QueryKeys.project(projectId), 'stats'],
    queryFn: async () => {
      const response = await projectApi.getStats(projectId)
      return response.data
    },
    enabled: !!projectId,
  })
}

// Mutation Hooks
export const useCreateProject = () => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async (payload: CreateProjectPayload) => {
      const response = await projectApi.create(payload)
      return response.data as Project
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: QueryKeys.projects })
    },
  })
}

export const useUpdateProject = () => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async ({ id, data }: UpdateProjectPayload) => {
      const response = await projectApi.update(id, data)
      return response.data as Project
    },
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: QueryKeys.project(variables.id) })
      queryClient.invalidateQueries({ queryKey: QueryKeys.projects })
    },
  })
}

export const useDeleteProject = () => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async (id: number) => {
      const response = await projectApi.delete(id)
      return response.data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: QueryKeys.projects })
    },
  })
}

