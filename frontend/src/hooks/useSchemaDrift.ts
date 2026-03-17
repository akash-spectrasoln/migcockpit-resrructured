/**
 * useSchemaDrift — Schema drift detection hook
 *
 * When called with a list of canvas nodes it:
 *   1. Filters all "source" nodes that have a stored schema in output_metadata.
 *   2. Fetches the live schema for each table from the backend (with 60s TTL cache).
 *   3. Compares live vs stored columns (added / removed / type-changed).
 *   4. Returns per-node drift results so the canvas can update node metadata
 *      and display warning badges.
 *
 * Nothing is ever persisted to the platform DB — all work is in-memory.
 */
import { useCallback, useRef } from 'react'
import { Node } from 'reactflow'
import { connectionApi, sourceApi } from '../services/api'

// ── Types ────────────────────────────────────────────────────────────────────

export interface ColumnDef {
  name: string
  type: string
}

export interface DriftResult {
  nodeId: string
  tableName: string
  connectionId: number
  schema?: string
  addedColumns: ColumnDef[]
  removedColumns: ColumnDef[]
  typeChanges: Array<{ name: string; oldType: string; newType: string }>
  hasDrift: boolean
  liveColumns: ColumnDef[]
}

// ── In-memory cache (module-level, survives re-renders, reset on page load) ──

interface CacheEntry {
  columns: ColumnDef[]
  fetchedAt: number // Date.now()
}

const CACHE_TTL_MS = 60_000 // 60 seconds

const schemaCache: Record<string, CacheEntry> = {}

function cacheKey(connectionId: number, tableName: string, schema?: string) {
  return `${connectionId}/${tableName}/${schema || ''}`
}

function getCached(connectionId: number, tableName: string, schema?: string): ColumnDef[] | null {
  const key = cacheKey(connectionId, tableName, schema)
  const entry = schemaCache[key]
  if (!entry) return null
  if (Date.now() - entry.fetchedAt > CACHE_TTL_MS) {
    delete schemaCache[key]
    return null
  }
  return entry.columns
}

function setCache(connectionId: number, tableName: string, schema: string | undefined, columns: ColumnDef[]) {
  schemaCache[cacheKey(connectionId, tableName, schema)] = {
    columns,
    fetchedAt: Date.now(),
  }
}

/** Clear the entire schema cache (used when user explicitly refreshes schema). */
export function clearSchemaCache() {
  Object.keys(schemaCache).forEach((k) => delete schemaCache[k])
}

/** Clear cache for a specific table. */
export function clearTableSchemaCache(connectionId: number, tableName: string, schema?: string) {
  delete schemaCache[cacheKey(connectionId, tableName, schema)]
}

// ── Comparison logic ─────────────────────────────────────────────────────────

export function compareSchemas(
  storedColumns: ColumnDef[],
  liveColumns: ColumnDef[]
): Pick<DriftResult, 'addedColumns' | 'removedColumns' | 'typeChanges' | 'hasDrift'> {
  const storedMap = new Map(storedColumns.map((c) => [c.name.toLowerCase(), c]))
  const liveMap = new Map(liveColumns.map((c) => [c.name.toLowerCase(), c]))

  const addedColumns: ColumnDef[] = []
  const removedColumns: ColumnDef[] = []
  const typeChanges: Array<{ name: string; oldType: string; newType: string }> = []

  // Columns in live but not in stored → added
  liveMap.forEach((liveCol, nameLower) => {
    if (!storedMap.has(nameLower)) {
      addedColumns.push(liveCol)
    } else {
      // Column exists in both — check type
      const stored = storedMap.get(nameLower)!
      if (stored.type.toLowerCase() !== liveCol.type.toLowerCase()) {
        typeChanges.push({ name: liveCol.name, oldType: stored.type, newType: liveCol.type })
      }
    }
  })

  // Columns in stored but not in live → removed
  storedMap.forEach((storedCol, nameLower) => {
    if (!liveMap.has(nameLower)) {
      removedColumns.push(storedCol)
    }
  })

  const hasDrift = addedColumns.length > 0 || removedColumns.length > 0 || typeChanges.length > 0
  return { addedColumns, removedColumns, typeChanges, hasDrift }
}

// ── Hook ─────────────────────────────────────────────────────────────────────

export function useSchemaDrift() {
  // Track in-flight requests to avoid duplicate fetches
  const inflightRef = useRef<Set<string>>(new Set())

  /**
   * Detect drift for all source nodes in the canvas.
   * Returns an array of DriftResult (only nodes where drift was detectable).
   * Does NOT mutate nodes — caller is responsible for applying changes.
   *
   * @param nodes     Current canvas nodes
   * @param forceRefresh  If true, bypass the 60s cache
   */
  const detectDrift = useCallback(
    async (nodes: Node[], forceRefresh = false): Promise<DriftResult[]> => {
      const sourceNodes = nodes.filter(
        (n) =>
          (n.data?.type === 'source' || n.type === 'source') &&
          n.data?.config?.sourceId &&
          n.data?.config?.tableName
      )

      if (!sourceNodes.length) return []

      const results: DriftResult[] = []

      // Fetch live schemas (deduplicated by connectionId + tableName)
      await Promise.allSettled(
        sourceNodes.map(async (node) => {
          const connectionId: number = node.data.config.sourceId
          const tableName: string = node.data.config.tableName
          const schema: string | undefined = node.data.config.schema || undefined

          const key = cacheKey(connectionId, tableName, schema)
          if (inflightRef.current.has(key)) return  // another await is already fetching this

          // Check cache (unless force-refreshing)
          let liveColumns = forceRefresh ? null : getCached(connectionId, tableName, schema)

          if (!liveColumns) {
            inflightRef.current.add(key)
            try {
              const res = await connectionApi.getLiveSchema(connectionId, tableName, schema)
              liveColumns = (res.data?.columns ?? []) as ColumnDef[]
              setCache(connectionId, tableName, schema, liveColumns)
            } catch (err) {
              console.warn(`[useSchemaDrift] Failed to fetch live schema for ${tableName}:`, err)
              inflightRef.current.delete(key)
              return  // skip this node on error
            } finally {
              inflightRef.current.delete(key)
            }
          }

          // Get stored schema from node metadata
          const storedColumnsRaw: any[] =
            node.data?.output_metadata?.columns ?? []

          // Normalise stored columns to {name, type}
          const storedColumns: ColumnDef[] = storedColumnsRaw.map((c: any) => ({
            name: String(c.name || c.column_name || c.technical_name || ''),
            type: String(c.type || c.datatype || c.data_type || 'unknown').toLowerCase(),
          })).filter((c) => c.name)

          // If we have no stored schema yet (new node), record live columns as the baseline —
          // no drift to report since there's nothing to compare against.
          if (!storedColumns.length) {
            results.push({
              nodeId: node.id,
              tableName,
              connectionId,
              schema,
              addedColumns: [],
              removedColumns: [],
              typeChanges: [],
              hasDrift: false,
              liveColumns,
            })
            return
          }

          const diff = compareSchemas(storedColumns, liveColumns)
          results.push({
            nodeId: node.id,
            tableName,
            connectionId,
            schema,
            liveColumns,
            ...diff,
          })
        })
      )

      return results
    },
    []
  )

  /**
   * Fetch live columns for a single table and return as ColumnDef[].
   * Respects the 60s cache.
   */
  const fetchLiveSchema = useCallback(
    async (connectionId: number, tableName: string, schema?: string, forceRefresh = false): Promise<ColumnDef[]> => {
      if (!forceRefresh) {
        const cached = getCached(connectionId, tableName, schema)
        if (cached) return cached
      }
      try {
        const res = await connectionApi.getLiveSchema(connectionId, tableName, schema)
        const columns = ((res as any)?.columns ?? (res as any)?.data?.columns ?? []) as ColumnDef[]
        if (columns && columns.length) {
          setCache(connectionId, tableName, schema, columns)
          return columns
        }
      } catch (err) {
        console.warn(`[useSchemaDrift] getLiveSchema failed for ${tableName}, falling back to /columns:`, err)
      }

      // Fallback: use Django /columns API which is already working in logs
      const fallback = await sourceApi.columns(connectionId, {
        table_name: tableName,
        schema,
        page: 1,
        page_size: 500,
      } as any)
      const rawCols = ((fallback as any)?.columns ?? (fallback as any)?.data?.columns ?? []) as any[]
      const mapped: ColumnDef[] = rawCols
        .map((c) => ({
          name: String(c.name || c.column_name || c.column || ''),
          type: String(c.type || c.datatype || c.data_type || 'unknown'),
        }))
        .filter((c) => c.name)

      setCache(connectionId, tableName, schema, mapped)
      return mapped
    },
    []
  )

  return { detectDrift, fetchLiveSchema, clearSchemaCache, clearTableSchemaCache }
}
