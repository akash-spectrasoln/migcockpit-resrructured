/**
 * Migration Query Options
 * React Query hooks for Migration Job operations
 */
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { migrationApi, pipelineApi } from '../../services/api'
import { QueryKeys } from '../../constants/common'

// Types
interface MigrationJob {
  id: number
  status: string
  created_on?: string
  updated_on?: string
  canvas_id?: number
  [key: string]: any
}

interface ExecuteMigrationPayload {
  canvas_id: number
  [key: string]: any
}

interface ExecutePipelinePayload {
  nodes: any[]
  edges: any[]
  targetNodeId: string
  options?: {
    page?: number
    pageSize?: number
    canvasId?: number
    useCache?: boolean
    forceRefresh?: boolean
  }
}

// Query Hooks
export const useFetchMigrationJob = (jobId?: number) => {
  return useQuery({
    queryKey: QueryKeys.migrationJob(jobId!),
    queryFn: async () => {
      const response = await migrationApi.getJob(jobId!)
      return response.data as MigrationJob
    },
    enabled: !!jobId,
  })
}

export const useFetchMigrationJobs = () => {
  return useQuery({
    queryKey: QueryKeys.migrationJobs,
    queryFn: async () => {
      const response = await migrationApi.listJobs()
      return response.data as MigrationJob[]
    },
  })
}

export const useFetchMigrationJobStatus = (jobId: number) => {
  return useQuery({
    queryKey: [...QueryKeys.migrationJob(jobId), 'status'],
    queryFn: async () => {
      const response = await migrationApi.getStatus(jobId)
      return response.data
    },
    enabled: !!jobId,
    refetchInterval: 5000, // Poll every 5 seconds for status updates
  })
}

export const useFetchMigrationJobLogs = (jobId: number) => {
  return useQuery({
    queryKey: [...QueryKeys.migrationJob(jobId), 'logs'],
    queryFn: async () => {
      const response = await migrationApi.getLogs(jobId)
      return response.data
    },
    enabled: !!jobId,
  })
}

// Mutation Hooks
export const useCreateMigrationJob = () => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async (payload: any) => {
      const response = await migrationApi.create(payload)
      return response.data as MigrationJob
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: QueryKeys.migrationJobs })
    },
  })
}

export const useExecuteMigration = () => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async (payload: ExecuteMigrationPayload) => {
      const response = await migrationApi.execute(payload.canvas_id, payload)
      return response.data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: QueryKeys.migrationJobs })
    },
  })
}

export const useCancelMigrationJob = () => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async (jobId: number) => {
      const response = await migrationApi.cancel(jobId)
      return response.data
    },
    onSuccess: (_, jobId) => {
      queryClient.invalidateQueries({ queryKey: QueryKeys.migrationJob(jobId) })
      queryClient.invalidateQueries({ queryKey: QueryKeys.migrationJobs })
    },
  })
}

// Pipeline Execution
export const useExecutePipeline = () => {
  return useMutation({
    mutationFn: async ({ nodes, edges, targetNodeId, options }: ExecutePipelinePayload) => {
      const response = await pipelineApi.execute(nodes, edges, targetNodeId, options)
      return response.data
    },
  })
}

