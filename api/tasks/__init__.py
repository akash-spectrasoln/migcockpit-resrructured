"""Tasks package - import tasks so Celery discovers them when autodiscover loads api.tasks."""
from api.tasks.migration_tasks import execute_migration_task, update_migration_status  # noqa: F401
