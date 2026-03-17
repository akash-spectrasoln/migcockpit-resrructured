"""
Create migration_job and migration_job_log tables in the current DB/schema if missing.

Use this when the server reports "relation migration_job does not exist" (e.g. server
uses a different database or schema than the one where migrate was run).

Run with the same environment as the Django server (same .env / DATABASE_* / DATABASE_SCHEMA):

  python manage.py ensure_migration_job_tables
"""

from django.core.management.base import BaseCommand
from django.db import connection


def table_exists(cursor, table_name: str) -> bool:
    """Return True if table exists in the current schema (search_path)."""
    cursor.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = current_schema() AND table_name = %s
        """,
        [table_name],
    )
    return cursor.fetchone() is not None

# Raw SQL to create tables without FK to user (user table may not have "id" in some schemas).
# Matches migration 0036; created_by_id is nullable bigint so app can set created_by=None.
POSTGRES_CREATE_MIGRATION_JOB = """
CREATE TABLE IF NOT EXISTS migration_job (
    id bigserial PRIMARY KEY,
    job_id varchar(100) NOT NULL UNIQUE,
    canvas_id bigint NOT NULL REFERENCES canvas(id) ON DELETE CASCADE,
    customer_id bigint NOT NULL REFERENCES customer(id) ON DELETE CASCADE,
    status varchar(20) NOT NULL DEFAULT 'pending',
    progress double precision NOT NULL DEFAULT 0.0,
    current_step varchar(255) NULL,
    config jsonb NOT NULL DEFAULT '{}',
    stats jsonb NULL,
    error_message text NULL,
    created_by_id bigint NULL,
    created_on timestamp with time zone NOT NULL DEFAULT now(),
    started_on timestamp with time zone NULL,
    completed_on timestamp with time zone NULL
);
"""

POSTGRES_CREATE_MIGRATION_JOB_LOG = """
CREATE TABLE IF NOT EXISTS migration_job_log (
    id bigserial PRIMARY KEY,
    job_id bigint NOT NULL REFERENCES migration_job(id) ON DELETE CASCADE,
    level varchar(10) NOT NULL DEFAULT 'INFO',
    message text NOT NULL,
    "timestamp" timestamp with time zone NOT NULL DEFAULT now(),
    metadata jsonb NULL
);
"""

class Command(BaseCommand):
    help = (
        "Create migration_job and migration_job_log tables in the current DB/schema "
        "if they do not exist. Use the same env as runserver."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Only report whether tables exist; do not create.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        if connection.vendor != "postgresql":
            self.stdout.write(
                self.style.ERROR("This command only supports PostgreSQL.")
            )
            return
        with connection.cursor() as cursor:
            job_exists = table_exists(cursor, "migration_job")
            log_exists = table_exists(cursor, "migration_job_log")
        if job_exists and log_exists:
            self.stdout.write(
                self.style.SUCCESS(
                    "Tables migration_job and migration_job_log already exist."
                )
            )
            return
        if dry_run:
            self.stdout.write("migration_job exists: %s" % job_exists)
            self.stdout.write("migration_job_log exists: %s" % log_exists)
            self.stdout.write(
                self.style.WARNING(
                    "Run without --dry-run to create missing tables."
                )
            )
            return
        with connection.cursor() as cursor:
            if not job_exists:
                cursor.execute(POSTGRES_CREATE_MIGRATION_JOB)
                self.stdout.write(
                    self.style.SUCCESS("Created table migration_job.")
                )
            if not log_exists:
                cursor.execute(POSTGRES_CREATE_MIGRATION_JOB_LOG)
                self.stdout.write(
                    self.style.SUCCESS("Created table migration_job_log.")
                )
        self.stdout.write(
            self.style.SUCCESS(
                "Done. Restart the Django server if it is running."
            )
        )
