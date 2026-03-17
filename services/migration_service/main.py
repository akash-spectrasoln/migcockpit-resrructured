"""
FastAPI Migration Orchestrator Service
Routers → Models → Services. All route handlers live in routers/; business logic in orchestrator, planner, lifecycle, loaders, utils.
"""

from contextlib import asynccontextmanager
import os
import sys
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

# Ensure service directory is on path (so routers, orchestrator, utils resolve to this service)
_service_dir = os.path.dirname(os.path.abspath(__file__))
if _service_dir not in sys.path:
    sys.path.insert(0, _service_dir)

import logging

from routers import execution_state_router, migration_router
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    from orchestrator.execution_state import get_execution_store
    from orchestrator.progress_emitter import get_ws_emitter

    execution_store = get_execution_store()
    await execution_store.start()
    logger.info("Execution state store started")
    yield
    await execution_store.stop()
    ws_emitter = get_ws_emitter()
    await ws_emitter.close()
    logger.info("Execution state store stopped")

app = FastAPI(
    title="Migration Orchestrator Service",
    description="Service for orchestrating complete data migration pipelines",
    version="1.0.0",
    docs_url="/docs",
    lifespan=lifespan,
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
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    logger.info(f"{request.method} {request.url.path} → {response.status_code} ({duration:.3f}s)")
    return response

# Routers: routes only; they delegate to services (orchestrator, planner, lifecycle, loaders, utils)
app.include_router(migration_router)
app.include_router(execution_state_router)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8003, reload=True)
