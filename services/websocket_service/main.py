"""
WebSocket server for real-time migration updates
Uses Socket.IO for better compatibility with frontend
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

try:
    from socketio import ASGIApp, AsyncServer
    from socketio.asyncio_namespace import AsyncNamespace
except ImportError:
    # Fallback if python-socketio not installed
    AsyncServer = None
    ASGIApp = None
    AsyncNamespace = None

import json
import logging
import time

import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="WebSocket Server",
    description="WebSocket server for real-time migration updates",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all incoming requests"""
    start_time = time.time()

    # Log request details
    logger.info(f"[WEBSOCKET] {request.method} {request.url.path}")
    logger.info(f"[WEBSOCKET] Query params: {dict(request.query_params)}")

    # Log request body for POST/PUT requests
    if request.method in ["POST", "PUT", "PATCH"]:
        try:
            body = await request.body()
            if body:
                try:
                    body_json = json.loads(body.decode())
                    logger.info(f"[WEBSOCKET] Body: {json.dumps(body_json, indent=2)}")
                except Exception:
                    logger.info(f"[WEBSOCKET] Body: {body.decode()[:200]}")  # First 200 chars if not JSON
        except Exception as e:
            logger.warning(f"[WEBSOCKET] Could not read body: {e}")

    # Process request
    response = await call_next(request)

    # Log response
    process_time = time.time() - start_time
    logger.info(f"[WEBSOCKET] {request.method} {request.url.path} - Status: {response.status_code} - Time: {process_time:.3f}s")

    return response

# Socket.IO server (if available)
if AsyncServer and ASGIApp:
    # True = allow all origins (avoids 403 when frontend runs on any port)
    sio = AsyncServer(
        cors_allowed_origins=True,
        async_mode='asgi',
        logger=False,
        engineio_logger=False,
    )
    logger.info("[WEBSOCKET] Socket.IO server started (cors_allowed_origins=True)")

    socketio_app = ASGIApp(sio, app)

    class JobNamespace(AsyncNamespace):
        """Socket.IO namespace for job updates"""

        async def on_connect(self, sid, environ, auth):
            """Handle client connection"""
            logger.info(f"[WEBSOCKET] Client connected: {sid}")
            logger.info(f"[WEBSOCKET] Connection details: {environ.get('REMOTE_ADDR', 'unknown')}")
            await self.emit('connected', {'sid': sid})

        async def on_disconnect(self, sid):
            """Handle client disconnection"""
            logger.info(f"[WEBSOCKET] Client disconnected: {sid}")

        async def on_join_job(self, sid, data):
            """Join a job room to receive updates"""
            job_id = data.get('job_id')
            logger.info(f"[WEBSOCKET] Client {sid} requesting to join job room: {job_id}")
            if job_id:
                self.enter_room(sid, f"job:{job_id}")
                logger.info(f"[WEBSOCKET] Client {sid} successfully joined job room: {job_id}")
                await self.emit('joined', {'job_id': job_id}, room=sid)
            else:
                logger.warning(f"[WEBSOCKET] Client {sid} attempted to join without job_id")

        async def on_leave_job(self, sid, data):
            """Leave a job room"""
            job_id = data.get('job_id')
            logger.info(f"[WEBSOCKET] Client {sid} leaving job room: {job_id}")
            if job_id:
                self.leave_room(sid, f"job:{job_id}")
                logger.info(f"[WEBSOCKET] Client {sid} successfully left job room: {job_id}")

    # Register namespace
    sio.register_namespace(JobNamespace('/'))

    async def broadcast_to_job(job_id: str, event: str, data: dict):
        """Broadcast event to all clients in a job room"""
        room = f"job:{job_id}"
        logger.info(f"[WEBSOCKET] Broadcasting event '{event}' to room '{room}' for job {job_id}")
        logger.info(f"[WEBSOCKET] Broadcast data: {json.dumps(data, indent=2)}")
        await sio.emit(event, data, room=room)
        logger.info(f"[WEBSOCKET] Successfully broadcasted {event} to {room}")

    @app.post("/broadcast/{job_id}")
    async def broadcast_update(job_id: str, message: dict):
        """Broadcast update to all connections for a job"""
        logger.info(f"[WEBSOCKET] POST /broadcast/{job_id} - Received broadcast request")
        logger.info(f"[WEBSOCKET] Message type: {message.get('type', 'update')}")
        event_type = message.get('type', 'update')
        await broadcast_to_job(job_id, event_type, message)
        logger.info(f"[WEBSOCKET] Broadcast completed for job {job_id}")
        return {"status": "broadcasted", "job_id": job_id, "event": event_type}

    # Export the Socket.IO app as the main ASGI application
    app = socketio_app
else:
    # Fallback: Simple WebSocket without Socket.IO (python-socketio not installed)
    # Frontend uses socket.io-client which connects to /socket.io/ -> will get 403.
    # Install: pip install -r services/websocket_service/requirements.txt
    logger.warning(
        "[WEBSOCKET] Socket.IO not available (install python-socketio). "
        "Using fallback; /socket.io/ will return 403. Connect to /ws/{job_id} for simple WebSocket."
    )
    from fastapi import WebSocket, WebSocketDisconnect

    active_connections: dict[str, list[WebSocket]] = {}

    @app.websocket("/ws/{job_id}")
    async def websocket_endpoint(websocket: WebSocket, job_id: str):
        """WebSocket endpoint for migration job updates"""
        logger.info(f"[WEBSOCKET] WebSocket connection request for job {job_id}")
        await websocket.accept()
        logger.info(f"[WEBSOCKET] WebSocket connection accepted for job {job_id}")
        if job_id not in active_connections:
            active_connections[job_id] = []
        active_connections[job_id].append(websocket)
        logger.info(f"[WEBSOCKET] Active connections for job {job_id}: {len(active_connections[job_id])}")

        try:
            while True:
                message = await websocket.receive_text()
                logger.info(f"[WEBSOCKET] Received message from job {job_id}: {message[:100]}")
        except WebSocketDisconnect:
            logger.info(f"[WEBSOCKET] WebSocket disconnected for job {job_id}")
            active_connections[job_id].remove(websocket)
            if not active_connections[job_id]:
                del active_connections[job_id]
                logger.info(f"[WEBSOCKET] Removed job {job_id} from active connections (no more clients)")

    @app.post("/broadcast/{job_id}")
    async def broadcast_update(job_id: str, message: dict):
        """Broadcast update to all connections for a job"""
        logger.info(f"[WEBSOCKET] POST /broadcast/{job_id} - Received broadcast request (fallback mode)")
        logger.info(f"[WEBSOCKET] Message type: {message.get('type', 'update')}")
        if job_id in active_connections:
            message_str = json.dumps(message)
            logger.info(f"[WEBSOCKET] Broadcasting to {len(active_connections[job_id])} connections for job {job_id}")
            disconnected = []
            for connection in active_connections[job_id]:
                try:
                    await connection.send_text(message_str)
                    logger.debug(f"[WEBSOCKET] Sent message to connection for job {job_id}")
                except Exception as e:
                    logger.warning(f"[WEBSOCKET] Failed to send to connection for job {job_id}: {e}")
                    disconnected.append(connection)

            for conn in disconnected:
                active_connections[job_id].remove(conn)
                if not active_connections[job_id]:
                    del active_connections[job_id]
                    logger.info(f"[WEBSOCKET] Removed job {job_id} from active connections (all clients disconnected)")
            logger.info(f"[WEBSOCKET] Broadcast completed for job {job_id}")
        else:
            logger.warning(f"[WEBSOCKET] No active connections found for job {job_id}")

        return {"status": "broadcasted", "job_id": job_id}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    logger.info("[WEBSOCKET] GET /health")
    return {
        "status": "healthy",
        "service": "websocket_service"
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8004, reload=True)
