/**
 * Server Routes
 * Backend API endpoint constants
 */

const API_BASE = '/api'

export const ServerRoutes = {
  // Auth endpoints
  auth: {
    login: `${API_BASE}/api-login/`,
    logout: `${API_BASE}/api-logout/`,
    refresh: `${API_BASE}/api-refresh/`,
    profile: `${API_BASE}/api-profile/`,
  },

  // Project endpoints
  projects: {
    list: `${API_BASE}/projects/`,
    get: (id: number) => `${API_BASE}/projects/${id}/`,
    create: `${API_BASE}/projects/`,
    update: (id: number) => `${API_BASE}/projects/${id}/`,
    delete: (id: number) => `${API_BASE}/projects/${id}/`,
    canvases: (projectId: number) => `${API_BASE}/projects/${projectId}/canvases/`,
    stats: (projectId: number) => `${API_BASE}/projects/${projectId}/stats/`,
  },

  // Canvas endpoints
  canvas: {
    list: `${API_BASE}/canvas/`,
    get: (id: number) => `${API_BASE}/canvas/${id}/`,
    create: `${API_BASE}/canvas/`,
    update: (id: number) => `${API_BASE}/canvas/${id}/`,
    delete: (id: number) => `${API_BASE}/canvas/${id}/`,
    saveConfiguration: (canvasId: number) => `${API_BASE}/canvas/${canvasId}/save-configuration/`,
  },

  // Migration job endpoints
  migration: {
    list: `${API_BASE}/migration-jobs/`,
    get: (id: number) => `${API_BASE}/migration-jobs/${id}/`,
    create: `${API_BASE}/migration-jobs/`,
    cancel: (id: number) => `${API_BASE}/migration-jobs/${id}/cancel/`,
    status: (id: number) => `${API_BASE}/migration-jobs/${id}/status/`,
    logs: (id: number) => `${API_BASE}/migration-jobs/${id}/logs/`,
    execute: `${API_BASE}/migration-jobs/execute/`,
    createForExecute: `${API_BASE}/migration-jobs/create_for_execute/`,
    startExecution: (id: number) => `${API_BASE}/migration-jobs/${id}/start_execution/`,
  },

  // Connection endpoints
  connection: {
    sources: `${API_BASE}/api-customer/sources/`,
    destinations: `${API_BASE}/api-customer/destinations/`,
    createDestination: `${API_BASE}/destinations-connection/`,
    destinationTables: (destinationId: number) => `${API_BASE}/api-customer/destinations/${destinationId}/tables/`,
    testConnection: (type: 'source' | 'destination', id: number) => `${API_BASE}/test-connection/${type}/${id}/`,
  },

  // Metadata endpoints
  metadata: {
    tables: `${API_BASE}/metadata/tables/`,
    columns: `${API_BASE}/metadata/columns/`,
    validationRules: `${API_BASE}/metadata/validation_rules/`,
    validatePipeline: `${API_BASE}/metadata/validate_pipeline/`,
  },

  // Source endpoints
  source: {
    create: `${API_BASE}/sources-connection/`,
    selectedTables: (sourceId: number) => `${API_BASE}/api-customer/sources/${sourceId}/selected-tables/`,
    tableData: (sourceId: number) => `${API_BASE}/api-customer/sources/${sourceId}/table-data/`,
    columns: (sourceId: number) => `${API_BASE}/api-customer/sources/${sourceId}/columns/`,
    repositoryTables: `${API_BASE}/api-customer/repository/tables/`,
    repositoryColumns: `${API_BASE}/api-customer/repository/columns/`,
    repositoryTableData: `${API_BASE}/api-customer/repository/table-data/`,
    repositoryFilter: `${API_BASE}/api-customer/repository/filter/`,
    liveSchema: (sourceId: number, tableName: string) =>
      `${API_BASE}/api-customer/sources/${sourceId}/table/${encodeURIComponent(tableName)}/schema/`,
    deleteSource: (sourceId: number) =>
      `${API_BASE}/api-customer/sources/${sourceId}/delete/`,
  },

  // Pipeline endpoints
  pipeline: {
    execute: `${API_BASE}/pipeline/execute/`,
    insertNode: `${API_BASE}/pipeline/insert-node/`,
    addNodeAfter: `${API_BASE}/pipeline/add-node-after/`,
    deleteNode: `${API_BASE}/pipeline/delete-node/`,
  },

  // Node cache endpoints
  nodeCache: {
    get: (canvasId: number, nodeId: string) => `${API_BASE}/node-cache/${canvasId}/${nodeId}/`,
    save: (canvasId: number, nodeId: string) => `${API_BASE}/node-cache/${canvasId}/${nodeId}/`,
    invalidate: (canvasId: number, nodeId?: string) => 
      nodeId ? `${API_BASE}/node-cache/${canvasId}/${nodeId}/` : `${API_BASE}/node-cache/${canvasId}/`,
    stats: (canvasId?: number) => 
      canvasId ? `${API_BASE}/node-cache/stats/${canvasId}/` : `${API_BASE}/node-cache/stats/`,
    cleanup: `${API_BASE}/node-cache/cleanup/`,
  },

  // Expression validation
  validation: {
    expression: `${API_BASE}/validate-expression/`,
  },
} as const

export type ServerRoutesType = typeof ServerRoutes

