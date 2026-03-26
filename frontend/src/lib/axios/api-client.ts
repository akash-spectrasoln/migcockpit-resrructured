/**
 * Axios API Client
 * Central axios instance + all domain-specific API objects.
 *
 * Import in components:
 *   import { canvasApi, migrationApi } from '../lib/axios/api-client'
 *   import { canvasApi, migrationApi } from '../services/api'   // legacy re-export
 */

import axios, { AxiosInstance, AxiosResponse } from 'axios'
import { API_BASE_URL, StorageKeys } from '../../constants/common'
import { ServerRoutes } from '../../constants/server-routes'

// ── Base axios instance ────────────────────────────────────────────────────────

export const api: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  headers: { 'Content-Type': 'application/json' },
  withCredentials: true,
})

// Request interceptor — attach JWT access token
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem(StorageKeys.ACCESS_TOKEN)
    if (token) {
      config.headers.Authorization = `Bearer ${token}`
    }
    return config
  },
  (error) => Promise.reject(error)
)

// Response interceptor — handle 401 token refresh
api.interceptors.response.use(
  (response) => response,
  async (error) => {
    const original = error.config
    if (error.response?.status === 401 && !original._retry) {
      original._retry = true
      try {
        const refreshToken = localStorage.getItem(StorageKeys.REFRESH_TOKEN)
        if (!refreshToken) throw new Error('No refresh token')
        const { data } = await axios.post(
          `${API_BASE_URL}${ServerRoutes.auth.refresh}`,
          { refresh: refreshToken }
        )
        localStorage.setItem(StorageKeys.ACCESS_TOKEN, data.access)
        original.headers.Authorization = `Bearer ${data.access}`
        return api(original)
      } catch {
        localStorage.removeItem(StorageKeys.ACCESS_TOKEN)
        localStorage.removeItem(StorageKeys.REFRESH_TOKEN)
        window.location.href = '/login'
      }
    }
    return Promise.reject(error)
  }
)

// ── Helper ─────────────────────────────────────────────────────────────────────

const unwrap = <T>(res: AxiosResponse<T>): T => res.data

// ── projectApi ─────────────────────────────────────────────────────────────────

export const projectApi = {
  list: () =>
    api.get(ServerRoutes.projects.list).then(unwrap),

  get: (id: number) =>
    api.get(ServerRoutes.projects.get(id)).then(unwrap),

  create: (data: Record<string, unknown>) =>
    api.post(ServerRoutes.projects.create, data).then(unwrap),

  update: (id: number, data: Record<string, unknown>) =>
    api.patch(ServerRoutes.projects.update(id), data).then(unwrap),

  delete: (id: number) =>
    api.delete(ServerRoutes.projects.delete(id)).then(unwrap),

  canvases: (projectId: number) =>
    api.get(ServerRoutes.projects.canvases(projectId)).then(unwrap),

  stats: (projectId: number) =>
    api.get(ServerRoutes.projects.stats(projectId)).then(unwrap),
}

// ── canvasApi ──────────────────────────────────────────────────────────────────

export const canvasApi = {
  list: (params?: Record<string, unknown>) =>
    api.get(ServerRoutes.canvas.list, { params }).then(unwrap),

  get: (id: number) =>
    api.get(ServerRoutes.canvas.get(id)).then(unwrap),

  create: (data: Record<string, unknown>) =>
    api.post(ServerRoutes.canvas.create, data).then(unwrap),

  update: (id: number, data: Record<string, unknown>) =>
    api.patch(ServerRoutes.canvas.update(id), data).then(unwrap),

  delete: (id: number) =>
    api.delete(ServerRoutes.canvas.delete(id)).then(unwrap),

  saveConfiguration: (canvasId: number, data: Record<string, unknown>) =>
    api.post(ServerRoutes.canvas.saveConfiguration(canvasId), data).then(unwrap),
}

// ── migrationApi ───────────────────────────────────────────────────────────────

export const migrationApi = {
  list: (params?: Record<string, unknown>) =>
    api.get(ServerRoutes.migration.list, { params }).then(unwrap),

  get: (id: number) =>
    api.get(ServerRoutes.migration.get(id)).then(unwrap),

  create: (data: Record<string, unknown>) =>
    api.post(ServerRoutes.migration.create, data).then(unwrap),

  execute: (canvasId: number, data?: Record<string, unknown>) =>
    api.post(ServerRoutes.migration.execute, { canvas_id: canvasId, ...data }).then(unwrap),

  createForExecute: (data: Record<string, unknown>) =>
    api.post(ServerRoutes.migration.createForExecute, data).then(unwrap),

  startExecution: (id: number, data?: Record<string, unknown>) =>
    api.post(ServerRoutes.migration.startExecution(id), data).then(unwrap),

  cancel: (id: number) =>
    api.post(ServerRoutes.migration.cancel(id)).then(unwrap),

  status: (id: number) =>
    api.get(ServerRoutes.migration.status(id)).then(unwrap),

  logs: (id: number) =>
    api.get(ServerRoutes.migration.logs(id)).then(unwrap),
}

// ── connectionApi ──────────────────────────────────────────────────────────────

export const connectionApi = {
  sources: (params?: Record<string, unknown>) =>
    api.get(ServerRoutes.connection.sources, { params }).then(unwrap),

  // Backwards-compatible alias for older code
  getSources: (params?: Record<string, unknown>) =>
    api.get(ServerRoutes.connection.sources, { params }).then(unwrap),

  destinations: (params?: Record<string, unknown>) =>
    api.get(ServerRoutes.connection.destinations, { params }).then(unwrap),

  // Backwards-compatible alias
  getDestinations: (params?: Record<string, unknown>) =>
    api.get(ServerRoutes.connection.destinations, { params }).then(unwrap),

  createDestination: (data: Record<string, unknown>) =>
    api.post(ServerRoutes.connection.createDestination, data).then(unwrap),

  destinationTables: (destinationId: number) =>
    api.get(ServerRoutes.connection.destinationTables(destinationId)).then(unwrap),

  testConnection: (type: 'source' | 'destination', id: number) =>
    api.get(ServerRoutes.connection.testConnection(type, id)).then(unwrap),

  // Helper used by older hooks/components to fetch a table schema
  getTableSchema: (sourceId: number, tableName: string) =>
    api.get(ServerRoutes.source.liveSchema(sourceId, tableName)).then(unwrap),

  // Helper for deleting a source connection
  deleteSource: (sourceId: number) =>
    api.post(ServerRoutes.source.deleteSource(sourceId)).then(unwrap),

  // Fetch live schema for a given source/table (used by useSchemaDrift)
  getLiveSchema: (sourceId: number, tableName: string, schema?: string) =>
    api.get(ServerRoutes.source.liveSchema(sourceId, tableName), {
      params: schema ? { schema } : undefined,
    }),
}

// ── metadataApi ────────────────────────────────────────────────────────────────

export const metadataApi = {
  tables: (params?: Record<string, unknown>) =>
    api.get(ServerRoutes.metadata.tables, { params }).then(unwrap),

  columns: (params?: Record<string, unknown>) =>
    api.get(ServerRoutes.metadata.columns, { params }).then(unwrap),

  validationRules: () =>
    api.get(ServerRoutes.metadata.validationRules).then(unwrap),

  validatePipeline: (data: Record<string, unknown>) =>
    api.post(ServerRoutes.metadata.validatePipeline, data).then(unwrap),
}

// ── validationApi ──────────────────────────────────────────────────────────────

export const validationApi = {
  validateExpression: (data: Record<string, unknown>) =>
    api.post(ServerRoutes.validation.expression, data).then(unwrap),
}

// ── sourceApi ──────────────────────────────────────────────────────────────────

export const sourceApi = {
  create: (data: Record<string, unknown>) =>
    api.post(ServerRoutes.source.create, data).then(unwrap),

  columns: (sourceId: number, params?: Record<string, unknown>) =>
    api.get(ServerRoutes.source.columns(sourceId), { params }).then(unwrap),

  tableData: (sourceId: number, params?: Record<string, unknown>) =>
    api.get(ServerRoutes.source.tableData(sourceId), { params }).then(unwrap),

  liveSchema: (sourceId: number, tableName: string) =>
    api.get(ServerRoutes.source.liveSchema(sourceId, tableName)).then(unwrap),
}

// ── sourceTableApi ─────────────────────────────────────────────────────────────

export const sourceTableApi = {
  selected: (sourceId: number, params?: Record<string, unknown>) =>
    api.get(ServerRoutes.source.selectedTables(sourceId), { params }).then(unwrap),

  // Backwards-compatible alias used by some components (expects paginated data)
  getTableData: (
    sourceId: number,
    tableName: string,
    schema: string,
    page: number,
    pageSize: number,
  ) =>
    api.get(ServerRoutes.source.tableData(sourceId), {
      params: { table_name: tableName, schema, page, page_size: pageSize },
    }).then(unwrap),

  liveSchema: (sourceId: number, tableName: string) =>
    api.get(ServerRoutes.source.liveSchema(sourceId, tableName)).then(unwrap),

  tableData: (sourceId: number, params?: Record<string, unknown>) =>
    api.get(ServerRoutes.source.tableData(sourceId), { params }).then(unwrap),

  /**
   * Backwards-compatible helper used by multiple canvas panels to fetch
   * column definitions for a given source/table.
   *
   * NOTE: This intentionally returns the full AxiosResponse so existing
   * callers that use `response.data.columns` keep working.
   */
  getColumns: (
    sourceId: number,
    tableName: string,
    schema?: string,
    refreshSchema?: boolean,
  ) =>
    api.get(ServerRoutes.source.columns(sourceId), {
      params: {
        table_name: tableName,
        schema,
        refresh: refreshSchema,
      },
    }),
  repositoryTables: (params?: Record<string, unknown>) =>
    api.get(ServerRoutes.source.repositoryTables, { params }).then(unwrap),
  repositoryColumns: (params?: Record<string, unknown>) =>
    api.get(ServerRoutes.source.repositoryColumns, { params }).then(unwrap),
  repositoryTableData: (params?: Record<string, unknown>) =>
    api.get(ServerRoutes.source.repositoryTableData, { params }).then(unwrap),
  repositoryFilter: (data: Record<string, unknown>) =>
    api.post(ServerRoutes.source.repositoryFilter, data).then(unwrap),
}

// ── pipelineApi ────────────────────────────────────────────────────────────────

export const pipelineApi = {
  execute: (data: Record<string, unknown>) =>
    api.post(ServerRoutes.pipeline.execute, data).then(unwrap),

  insertNode: (data: Record<string, unknown>) =>
    api.post(ServerRoutes.pipeline.insertNode, data).then(unwrap),

  addNodeAfter: (data: Record<string, unknown>) =>
    api.post(ServerRoutes.pipeline.addNodeAfter, data).then(unwrap),

  deleteNode: (data: Record<string, unknown>) =>
    api.post(ServerRoutes.pipeline.deleteNode, data).then(unwrap),
}

// ── nodeCacheApi ───────────────────────────────────────────────────────────────

export const nodeCacheApi = {
  get: (canvasId: number, nodeId: string) =>
    api.get(ServerRoutes.nodeCache.get(canvasId, nodeId)).then(unwrap),

  save: (canvasId: number, nodeId: string, data: Record<string, unknown>) =>
    api.post(ServerRoutes.nodeCache.save(canvasId, nodeId), data).then(unwrap),

  invalidate: (canvasId: number, nodeId?: string) =>
    api.delete(ServerRoutes.nodeCache.invalidate(canvasId, nodeId)).then(unwrap),

  stats: (canvasId?: number) =>
    api.get(ServerRoutes.nodeCache.stats(canvasId)).then(unwrap),

  cleanup: () =>
    api.post(ServerRoutes.nodeCache.cleanup).then(unwrap),
}
