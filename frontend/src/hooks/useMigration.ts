/**
 * React Query hooks for Migration operations
 */
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { migrationApi } from '../services/api'

export const useMigrationJob = (jobId?: number) => {
  return useQuery({
    queryKey: ['migration', jobId],
    queryFn: () => migrationApi.getJob(jobId!),
    enabled: !!jobId,
    refetchInterval: (query) => {
      const data = query.state.data as any
      if (data?.status === 'running' || data?.status === 'pending') {
        return 2000 // Refetch every 2 seconds for active jobs
      }
      return false
    },
  })
}

export const useMigrationJobs = () => {
  return useQuery({
    queryKey: ['migrations'],
    queryFn: () => migrationApi.listJobs(),
  })
}

export const useExecuteMigration = () => {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: ({ canvasId, pipeline }: { canvasId: number; pipeline: any }) =>
      migrationApi.execute(canvasId, pipeline),
    onSuccess: (response) => {
      const jobId = (response as any)?.job_id ?? (response as any)?.data?.job_id
      queryClient.invalidateQueries({ queryKey: ['migrations'] })
      queryClient.setQueryData(['migration', jobId], response)
    },
  })
}

export const useCancelMigration = () => {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: (jobId: number) => migrationApi.cancel(jobId),
    onSuccess: (_, jobId) => {
      queryClient.invalidateQueries({ queryKey: ['migration', jobId] })
      queryClient.invalidateQueries({ queryKey: ['migrations'] })
    },
  })
}

