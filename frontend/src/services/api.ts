/**
 * API Service
 * Re-exports from lib/axios/api-client for backward compatibility
 * New code should import from '@/lib/axios' or '../lib/axios'
 */
export {
  api,
  projectApi,
  canvasApi,
  migrationApi,
  connectionApi,
  metadataApi,
  sourceApi,
  sourceTableApi,
  pipelineApi,
  nodeCacheApi,
} from '../lib/axios/api-client'
