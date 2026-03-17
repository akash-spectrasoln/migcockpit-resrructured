# Moved from: api/utils/ensure_migration_job_tables.py
"""
Create migration_job and migration_job_log tables in the current DB/schema if missing.

Used by the ensure_migration_job_tables management command and by the execute view
when the server hits "relation migration_job does not exist".
"""

from django.db import connection

# Raw SQL to create tables. Matches migration 0036; created_by_id nullable.
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
    status_extra jsonb NULL,
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

def ensure_migration_job_tables():
    """
    Create migration_job and migration_job_log in the current DB/schema if missing.
    Only supports PostgreSQL. Returns True if at least one table was created.
    """
    if connection.vendor != "postgresql":
        return False
    created = False
    with connection.cursor() as cursor:
        cursor.execute(POSTGRES_CREATE_MIGRATION_JOB)
        created = True
        cursor.execute(POSTGRES_CREATE_MIGRATION_JOB_LOG)
    return created
