"""
Tenant Provisioning Service
Extracted from: api/models/base.py Customer.create_customer_database()

Handles the creation of per-customer PostgreSQL databases and schemas.
Moved out of the Django model to keep the model focused on persistence only.
"""
import logging

from django.conf import settings
import psycopg2

logger = logging.getLogger(__name__)

class TenantProvisioningService:
    """
    Provisions a new customer's isolated PostgreSQL database and schemas.

    Usage:
        service = TenantProvisioningService()
        service.provision(customer_instance)
    """

    def provision(self, customer) -> None:
        """
        Create the customer database and all required schemas/tables.
        Equivalent to calling customer.create_customer_database() in the old model.
        """
        self._create_database(customer)

    def _create_database(self, customer) -> None:
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database='postgres'
            )
            conn.autocommit = True
            cursor = conn.cursor()

            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (customer.cust_db,))
            if cursor.fetchone():
                logger.info(f"Database {customer.cust_db} already exists")
            else:
                cursor.execute(f'CREATE DATABASE "{customer.cust_db}";')
                logger.info(f"Created database: {customer.cust_db}")
                try:
                    self._create_schemas(customer)
                except Exception as schema_error:
                    cursor.execute(f'DROP DATABASE IF EXISTS "{customer.cust_db}";')
                    raise schema_error
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def _create_schemas(self, customer) -> None:
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=customer.cust_db
            )
            conn.autocommit = True
            cursor = conn.cursor()

            main_schema = "GENERAL"
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{main_schema}";')

            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS "{main_schema}".source (
                    id SERIAL PRIMARY KEY,
                    source_name VARCHAR(255),
                    source_config TEXT,
                    project_id INTEGER,
                    created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE
                );
            ''')

            cursor.execute(f'''
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = '{main_schema}' AND table_name = 'source'
            ''')
            existing_columns = [row[0] for row in cursor.fetchall()]
            if 'src_name' in existing_columns and 'source_name' not in existing_columns:
                cursor.execute(f'ALTER TABLE "{main_schema}".source RENAME COLUMN src_name TO source_name;')
            if 'src_config' in existing_columns and 'source_config' not in existing_columns:
                cursor.execute(f'ALTER TABLE "{main_schema}".source RENAME COLUMN src_config TO source_config;')

            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS "{main_schema}".destination (
                    id SERIAL PRIMARY KEY,
                    dest_name VARCHAR(255),
                    dest_config TEXT,
                    project_id INTEGER,
                    created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE
                );
            ''')

            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS "{main_schema}".source_table_selection (
                    tbl_id SERIAL PRIMARY KEY,
                    source_id INTEGER NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    schema VARCHAR(100),
                    table_fields JSONB,
                    selected BOOLEAN DEFAULT TRUE,
                    added_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_synced TIMESTAMP,
                    UNIQUE(source_id, table_name, schema)
                );
            ''')

            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS "{main_schema}".tbl_col_seq (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(100),
                    table_name VARCHAR(100),
                    sequence VARCHAR(400),
                    seq_name VARCHAR(100),
                    scope VARCHAR(10) CHECK (scope IN ('G', 'L'))
                );
            ''')

            cache_schema = "CANVAS_CACHE"
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{cache_schema}";')
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS "{cache_schema}".node_cache_metadata (
                    id SERIAL PRIMARY KEY,
                    canvas_id INTEGER NOT NULL,
                    node_id VARCHAR(100) NOT NULL,
                    node_name VARCHAR(255),
                    node_type VARCHAR(50) NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    config_hash VARCHAR(64),
                    row_count INTEGER DEFAULT 0,
                    column_count INTEGER DEFAULT 0,
                    columns JSONB,
                    source_node_ids JSONB,
                    created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_valid BOOLEAN DEFAULT TRUE,
                    UNIQUE(canvas_id, node_id)
                );
            ''')
            cursor.execute(f'''
                CREATE INDEX IF NOT EXISTS idx_node_cache_lookup
                ON "{cache_schema}".node_cache_metadata (canvas_id, node_id);
            ''')

            logger.info(f"Created schemas in {customer.cust_db}: {main_schema}, {cache_schema}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
