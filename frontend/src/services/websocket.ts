/**
 * WebSocket Service for Real-Time Job Updates
 * Uses native WebSocket for real-time communication with FastAPI backend
 */

const WS_BASE_URL = (import.meta as any).env?.VITE_WS_BASE_URL || 'ws://localhost:8004'

export interface JobUpdateMessage {
  type: 'status' | 'node_progress' | 'segment_execution' | 'complete' | 'error' | 'cancelled'
  status?: string
  progress?: number
  current_step?: string
  current_level?: number
  total_levels?: number
  level_status?: string
  node_id?: string
  node_progress?: Array<{ node_id: string; status: string; progress?: number }>
  message?: string
  error?: string
  stats?: Record<string, any>

  // Segment execution (linear SQL container) event payload
  segment_id?: string
  segment_node_ids?: string[]
  started_at?: number
  completed_at?: number
  duration_seconds?: number
  rowcount?: number
  sql?: string
}

interface WebSocketCallbacks {
  onStatus?: (data: JobUpdateMessage) => void
  onNodeProgress?: (data: JobUpdateMessage) => void
  onSegmentExecution?: (data: JobUpdateMessage) => void
  onComplete?: (data: JobUpdateMessage) => void
  onError?: (data: JobUpdateMessage) => void
  onCancelled?: (data: JobUpdateMessage) => void
  onJoined?: () => void
}

class WebSocketService {
  private ws: WebSocket | null = null
  private reconnectAttempts = 0
  private maxReconnectAttempts = 5
  private reconnectDelay = 1000
  private currentJobId: string | null = null
  private callbacks: WebSocketCallbacks = {}
  private reconnectTimeout: number | null = null

  connect(jobId: string): WebSocket {
    // If already connected to the same job, return existing socket
    if (this.ws?.readyState === WebSocket.OPEN && this.currentJobId === jobId) {
      return this.ws
    }

    // Disconnect existing connection if different job
    if (this.ws) {
      this.disconnect()
    }

    this.currentJobId = jobId

    // Convert http:// to ws:// if needed
    const wsUrl = WS_BASE_URL.replace('http://', 'ws://').replace('https://', 'wss://')
    const url = `${wsUrl}/ws/${jobId}`

    console.log('[WebSocket] Connecting to:', url)
    this.ws = new WebSocket(url)

    this.ws.onopen = () => {
      console.log('[WebSocket] Connected to job:', jobId)
      this.reconnectAttempts = 0

      // Clear any pending reconnect
      if (this.reconnectTimeout) {
        clearTimeout(this.reconnectTimeout)
        this.reconnectTimeout = null
      }

      // Trigger onJoined callback
      this.callbacks.onJoined?.()
    }

    this.ws.onmessage = (event) => {
      try {
        const data: JobUpdateMessage = JSON.parse(event.data)
        console.log('[WebSocket] Received:', data.type, data)

        // Route message to appropriate callback based on type
        switch (data.type) {
          case 'status':
            this.callbacks.onStatus?.(data)
            break
          case 'node_progress':
            this.callbacks.onNodeProgress?.(data)
            break
          case 'segment_execution':
            this.callbacks.onSegmentExecution?.(data)
            break
          case 'complete':
            this.callbacks.onComplete?.(data)
            break
          case 'error':
            this.callbacks.onError?.(data)
            break
          case 'cancelled':
            this.callbacks.onCancelled?.(data)
            break
          default:
            console.warn('[WebSocket] Unknown message type:', data.type)
        }
      } catch (error) {
        console.error('[WebSocket] Failed to parse message:', error)
      }
    }

    this.ws.onerror = (error) => {
      console.error('[WebSocket] Error:', error)
    }

    this.ws.onclose = (event) => {
      console.log('[WebSocket] Closed:', event.code, event.reason)

      // Attempt reconnection if not manually closed
      if (event.code !== 1000 && this.reconnectAttempts < this.maxReconnectAttempts) {
        this.reconnectAttempts++
        const delay = this.reconnectDelay * this.reconnectAttempts
        console.log(`[WebSocket] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`)

        this.reconnectTimeout = window.setTimeout(() => {
          if (this.currentJobId) {
            this.connect(this.currentJobId)
          }
        }, delay)
      } else if (this.reconnectAttempts >= this.maxReconnectAttempts) {
        console.error('[WebSocket] Max reconnection attempts reached')
      }
    }

    return this.ws
  }

  subscribeToJobUpdates(jobId: string, callbacks: WebSocketCallbacks) {
    // Store callbacks
    this.callbacks = callbacks

    // Connect if not already connected
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN || this.currentJobId !== jobId) {
      this.connect(jobId)
    } else {
      // Already connected, trigger onJoined immediately
      callbacks.onJoined?.()
    }
  }

  unsubscribeFromJobUpdates(jobId: string) {
    // Clear callbacks
    this.callbacks = {}

    // Disconnect if this is the current job
    if (this.currentJobId === jobId) {
      this.disconnect()
    }
  }

  disconnect() {
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout)
      this.reconnectTimeout = null
    }

    if (this.ws) {
      // Close with normal closure code
      this.ws.close(1000, 'Client disconnect')
      this.ws = null
      this.currentJobId = null
      this.reconnectAttempts = 0
      this.callbacks = {}
    }
  }

  isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN
  }

  send(data: any) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data))
    } else {
      console.warn('[WebSocket] Cannot send, not connected')
    }
  }
}

export const wsService = new WebSocketService()
