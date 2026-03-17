/**
 * Common Constants
 * Shared application constants
 */

// API Configuration
export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'

// Local Storage Keys
export const StorageKeys = {
  IS_AUTHENTICATED: 'is_authenticated',
  ACCESS_TOKEN: 'access_token',
  REFRESH_TOKEN: 'refresh_token',
} as const

// Query Cache Keys
export const QueryKeys = {
  projects: ['projects'] as const,
  project: (id: number) => ['project', id] as const,
  canvases: (projectId?: number) => projectId ? ['canvases', projectId] as const : ['canvases'] as const,
  canvas: (id: number) => ['canvas', id] as const,
  migrationJobs: ['migration-jobs'] as const,
  migrationJob: (id: number) => ['migration-job', id] as const,
  sources: (projectId?: number) => projectId ? ['sources', projectId] as const : ['sources'] as const,
  destinations: (projectId?: number) => projectId ? ['destinations', projectId] as const : ['destinations'] as const,
  tables: (sourceId: number) => ['tables', sourceId] as const,
  columns: (sourceId: number, tableName: string) => ['columns', sourceId, tableName] as const,
  nodeCache: (canvasId: number, nodeId: string) => ['node-cache', canvasId, nodeId] as const,
} as const

// View Configuration
export const ViewConfig = {
  pageGap: 4,
  sidebarWidth: 280,
  headerHeight: 64,
  defaultPageSize: 50,
  maxPageSize: 100,
} as const

// Pagination Defaults
export const PaginationDefaults = {
  page: 1,
  pageSize: 50,
} as const

// Data Types
export const DataTypes = [
  'STRING',
  'INTEGER', 
  'DECIMAL',
  'DATE',
  'DATETIME',
  'BOOLEAN',
] as const

export type DataType = typeof DataTypes[number]

