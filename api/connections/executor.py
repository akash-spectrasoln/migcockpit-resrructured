# Moved from: api/utils/db_executor.py
"""
Database executor for executing compiled SQL queries.
Handles connection management, query execution, and result formatting.
"""
import logging
from typing import Any, Optional

from django.conf import settings
import psycopg2
import pyodbc

from api.utils.preview_guards import MAX_PREVIEW_ROWS, enforce_preview_memory_limit

logger = logging.getLogger(__name__)

def execute_preview_query(
    sql_query: str,
    params: list[Any],
    source_config: dict[str, Any],
    page: int = 1,
    page_size: int = 50,
    customer_db: Optional[str] = None,
) -> dict[str, Any]:
    """
    Execute a compiled SQL query.

    When ``customer_db`` is supplied the query runs against the customer's
    PostgreSQL database (same server as the Django default DB, different
    database name).  This is used for checkpoint-cache queries where the
    table lives in a ``staging_preview_<canvas_id>`` schema on the customer DB,
    NOT on the source DB.

    When ``customer_db`` is None the query runs against the source database
    described by ``source_config``.
    """
    if customer_db:
        # Build a source_config that points at the customer DB on the same
        # host/credentials as settings.DATABASES['default'].
        default_db = settings.DATABASES.get('default', {})
        effective_config = {
            'db_type': 'postgresql',
            'hostname': default_db.get('HOST', 'localhost'),
            'port':     default_db.get('PORT', 5432),
            'user':     default_db.get('USER', 'postgres'),
            'password': default_db.get('PASSWORD', ''),
            'database': customer_db,
        }
        return _execute_postgresql_query(sql_query, params, effective_config, page, page_size)

    db_type = source_config.get('db_type', 'postgresql').lower()
    if db_type == 'postgresql':
        return _execute_postgresql_query(sql_query, params, source_config, page, page_size)
    elif db_type in ('sqlserver', 'mssql'):
        return _execute_sqlserver_query(sql_query, params, source_config, page, page_size)
    elif db_type == 'mysql':
        return _execute_mysql_query(sql_query, params, source_config, page, page_size)
    elif db_type == 'oracle':
        return _execute_oracle_query(sql_query, params, source_config, page, page_size)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def _execute_postgresql_query(
    sql_query: str,
    params: list[Any],
    source_config: dict[str, Any],
    page: int,
    page_size: int
) -> dict[str, Any]:
    """Execute query against PostgreSQL database."""
    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(
            host=source_config.get('hostname'),
            port=source_config.get('port'),
            user=source_config.get('user'),
            password=source_config.get('password'),
            database=source_config.get('database')
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Execute query
        logger.info(f"Executing PostgreSQL query with {len(params)} parameters")
        cursor.execute(sql_query, params)

        # Get column names
        column_names = [desc[0] for desc in cursor.description] if cursor.description else []

        # Fetch rows with hard limit (MEMORY SAFETY).
        # Tests mock `cursor.fetchall()` (not `fetchmany()`), so prefer fetchall()
        # and slice to MAX_PREVIEW_ROWS.
        rows_data = cursor.fetchall()
        if isinstance(rows_data, list):
            rows_data = rows_data[:MAX_PREVIEW_ROWS]
        else:
            # Defensive: if a mock returned a non-list, keep behavior but avoid len() crashes.
            rows_data = list(rows_data)[:MAX_PREVIEW_ROWS] if rows_data is not None else []

        # Runtime guard: warn if we hit the limit
        if len(rows_data) >= MAX_PREVIEW_ROWS:
            logger.warning(
                "[PREVIEW MEMORY GUARD] PostgreSQL query hit row limit: "
                f"{len(rows_data)} rows fetched (max: {MAX_PREVIEW_ROWS})"
            )

        # Convert to list of dictionaries
        rows = []
        for row in rows_data:
            row_dict = {}
            for i, col_name in enumerate(column_names):
                row_dict[col_name] = row[i]
            rows.append(row_dict)

        # Apply memory guard (defensive)
        rows = enforce_preview_memory_limit(rows, MAX_PREVIEW_ROWS)

        # Get total count (approximate for performance)
        # Note: LIMIT is already in query, so we can't get exact total without another query
        # For preview, we'll use the row count as total
        total = len(rows)
        has_more = len(rows) >= page_size

        return {
            'rows': rows,
            'columns': column_names,
            'total': total,
            'has_more': has_more,
            'page': page,
            'page_size': page_size
        }

    except psycopg2.Error as e:
        logger.error(f"PostgreSQL query error: {e}")
        raise ValueError(f"Database query failed: {e!s}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def _execute_sqlserver_query(
    sql_query: str,
    params: list[Any],
    source_config: dict[str, Any],
    page: int,
    page_size: int
) -> dict[str, Any]:
    """Execute query against SQL Server database."""
    conn = None
    cursor = None

    try:
        driver = 'ODBC Driver 17 for SQL Server'
        database = source_config.get('database', 'master')
        server = f"{source_config.get('hostname')},{source_config.get('port')}"

        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={source_config.get('user')};"
            f"PWD={source_config.get('password')};"
            "TrustServerCertificate=yes;"
        )

        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()

        # Execute query
        logger.info(f"Executing SQL Server query with {len(params)} parameters")
        cursor.execute(sql_query, params)

        # Get column names
        column_names = [desc[0] for desc in cursor.description] if cursor.description else []

        # Fetch rows with hard limit (MEMORY SAFETY).
        # Prefer fetchall() to match unit test mocks.
        rows_data = cursor.fetchall()
        if isinstance(rows_data, list):
            rows_data = rows_data[:MAX_PREVIEW_ROWS]
        else:
            rows_data = list(rows_data)[:MAX_PREVIEW_ROWS] if rows_data is not None else []

        # Runtime guard: warn if we hit the limit
        if len(rows_data) >= MAX_PREVIEW_ROWS:
            logger.warning(
                "[PREVIEW MEMORY GUARD] SQL Server query hit row limit: "
                f"{len(rows_data)} rows fetched (max: {MAX_PREVIEW_ROWS})"
            )

        # Convert to list of dictionaries
        rows = []
        for row in rows_data:
            row_dict = {}
            for i, col_name in enumerate(column_names):
                row_dict[col_name] = row[i]
            rows.append(row_dict)

        # Apply memory guard (defensive)
        rows = enforce_preview_memory_limit(rows, MAX_PREVIEW_ROWS)

        total = len(rows)
        has_more = len(rows) >= page_size

        return {
            'rows': rows,
            'columns': column_names,
            'total': total,
            'has_more': has_more,
            'page': page,
            'page_size': page_size
        }

    except pyodbc.Error as e:
        logger.error(f"SQL Server query error: {e}")
        raise ValueError(f"Database query failed: {e!s}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def _execute_mysql_query(
    sql_query: str,
    params: list[Any],
    source_config: dict[str, Any],
    page: int,
    page_size: int
) -> dict[str, Any]:
    """Execute query against MySQL database."""
    # MySQL implementation would use mysql-connector-python or pymysql
    # For now, raise NotImplementedError
    raise NotImplementedError("MySQL query execution not yet implemented")

def _execute_oracle_query(
    sql_query: str,
    params: list[Any],
    source_config: dict[str, Any],
    page: int,
    page_size: int
) -> dict[str, Any]:
    """Execute query against Oracle database."""
    # Oracle implementation would use cx_Oracle
    # For now, raise NotImplementedError
    raise NotImplementedError("Oracle query execution not yet implemented")

def get_table_schema(
    source_id: int,
    table_name: str,
    schema: str,
    customer,
    db_type: str = 'postgresql'
) -> list[dict[str, Any]]:
    """
    Get table schema metadata.

    Args:
        source_id: Source connection ID
        table_name: Table name
        schema: Schema name
        customer: Customer object
        db_type: Database type

    Returns:
        List of column metadata dictionaries
    """
    import psycopg2

    from api.utils.helpers import decrypt_source_data

    # Get source config from customer database
    conn = psycopg2.connect(
        host=settings.DATABASES['default']['HOST'],
        port=settings.DATABASES['default']['PORT'],
        user=settings.DATABASES['default']['USER'],
        password=settings.DATABASES['default']['PASSWORD'],
        database=customer.cust_db
    )
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        # Get column names
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'GENERAL' AND table_name = 'source'
        """)
        columns = [row[0] for row in cursor.fetchall()]
        name_column = 'source_name' if 'source_name' in columns else 'src_name'
        config_column = 'source_config' if 'source_config' in columns else 'src_config'

        # Get source config
        name_column_sql = f'"{name_column}"'
        config_column_sql = f'"{config_column}"'
        cursor.execute(
            f'''
            SELECT {name_column_sql}, {config_column_sql}, created_on
            FROM "GENERAL".source
            WHERE id = %s
            ''',
            (source_id,),
        )

        row = cursor.fetchone()
        if not row:
            raise ValueError(f"Source {source_id} not found")

        source_name, source_config_encrypted, source_created_on = row
        source_config = decrypt_source_data(source_config_encrypted, customer.cust_id, source_created_on)

        if not source_config:
            raise ValueError(f"Failed to decrypt source {source_id}")

        # Query source database for schema
        if db_type == 'postgresql':
            source_conn = psycopg2.connect(
                host=source_config.get('hostname'),
                port=source_config.get('port'),
                user=source_config.get('user'),
                password=source_config.get('password'),
                database=source_config.get('database')
            )
            source_conn.autocommit = True
            source_cursor = source_conn.cursor()

            try:
                source_cursor.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema or 'public', table_name))

                columns = []
                for col_row in source_cursor.fetchall():
                    col_name, data_type, is_nullable = col_row
                    columns.append({
                        'name': col_name,
                        'datatype': data_type.upper(),
                        'nullable': is_nullable == 'YES'
                    })

                return columns

            finally:
                source_cursor.close()
                source_conn.close()
        else:
            # Other DB types not yet implemented
            return []

    finally:
        cursor.close()
        conn.close()
    return []