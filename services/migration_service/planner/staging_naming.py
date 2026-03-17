"""
Shared staging schema and table naming.

All staging tables live in a single schema (staging_jobs) in the customer DB.
Table names include job_id and node_id to avoid clashes: staging_jobs.job_<job_id>_node_<node_id>.
"""

STAGING_SCHEMA = "staging_jobs"

def get_staging_table_name(job_id: str, node_id: str) -> str:
    """Return full staging table name: staging_jobs.job_<safe_job_id>_node_<node_id>."""
    safe_job = (job_id or "").replace("-", "_")
    return f"{STAGING_SCHEMA}.job_{safe_job}_node_{node_id}"
