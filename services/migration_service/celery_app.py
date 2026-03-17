"""
Celery app for the migration service. Uses the same Redis broker as Django (optional).
Run worker from migration_service dir: celery -A celery_app worker -l info
"""

import os
import sys

# Ensure service dir is on path so worker can import routers, orchestrator, state, etc.
_service_dir = os.path.dirname(os.path.abspath(__file__))
if _service_dir not in sys.path:
    sys.path.insert(0, _service_dir)

from celery import Celery

REDIS_URL = os.getenv("REDIS_URL") or os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")

app = Celery(
    "migration_service",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["tasks"],
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=60 * 60,  # 1 hour for long migrations
    task_soft_time_limit=55 * 60,
    worker_prefetch_multiplier=1,  # one task at a time per worker for heavy pipelines
)

if sys.platform == "win32":
    app.conf.worker_pool = "solo"
