"""
Celery configuration for background task processing.

On Windows, the default prefork pool causes "ValueError: not enough values to unpack (expected 3, got 0)".
We set worker_pool='solo' on Windows so the worker works even when started without --pool=solo.
"""

import os
import sys

from celery import Celery

# Set default Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'datamigrationapi.settings')

app = Celery('datamigrationapi')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django apps.
app.autodiscover_tasks()

# Celery configuration
app.conf.update(
    broker_url='redis://localhost:6379/0',
    result_backend='redis://localhost:6379/0',
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    task_soft_time_limit=25 * 60,  # 25 minutes
    worker_prefetch_multiplier=4,
    worker_max_tasks_per_child=1000,
    broker_connection_retry_on_startup=True,  # Retry broker connection on startup (Celery 6+)
)

# On Windows, prefork pool causes "ValueError: not enough values to unpack (expected 3, got 0)".
# Default to solo pool so worker runs correctly even without --pool=solo on the command line.
if sys.platform == 'win32':
    app.conf.worker_pool = 'solo'

@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
