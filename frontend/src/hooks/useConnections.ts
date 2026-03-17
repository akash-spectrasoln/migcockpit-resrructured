/**
 * React Query hooks for Connection operations
 */
import { useQuery } from '@tanstack/react-query'
import { connectionApi } from '../services/api'

export const useSourceConnections = () => {
  return useQuery({
    queryKey: ['connections', 'sources'],
    queryFn: () => connectionApi.sources(),
  })
}

export const useDestinationConnections = () => {
  return useQuery({
    queryKey: ['connections', 'destinations'],
    queryFn: () => connectionApi.destinations(),
  })
}

export const useMetadata = (sourceId?: number, tableName?: string) => {
  return useQuery({
    queryKey: ['metadata', sourceId, tableName],
    queryFn: () => {
      if (!sourceId || !tableName) throw new Error('Source ID and table name required')
      return connectionApi.getTableSchema(sourceId, tableName)
    },
    enabled: !!sourceId && !!tableName,
  })
}

