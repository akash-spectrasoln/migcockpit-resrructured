"""
SQL Server Database Connector
"""

import datetime
import decimal
import logging
from typing import Any, Optional
import uuid

import pyodbc

logger = logging.getLogger(__name__)

class SQLServerConnector:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.connection_string = self._build_connection_string()

    def _build_connection_string(self) -> str:
        """Build SQL Server connection string"""
        hostname = self.config.get('hostname')
        port = self.config.get('port', 1433)
        database = self.config.get('database')
        user = self.config.get('user')
        password = self.config.get('password')

        driver = '{ODBC Driver 17 for SQL Server}'  # Adjust based on installed driver

        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={hostname},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            "TrustServerCertificate=yes;"
        )

        return conn_str

    def get_connection(self):
        """Get database connection"""
        try:
            return pyodbc.connect(self.connection_string)
        except Exception as e:
            logger.error(f"Failed to get SQL Server connection: {e}")
            raise

    def _normalize_value(self, val: Any) -> Any:
        """
        Normalize database values to JSON-serializable types.
        Handles: bytes (VARBINARY, IMAGE), Decimal, datetime, UUID.
        """
        if val is None:
            return None
        if isinstance(val, (bytes, memoryview)):
            b_val = val.tobytes() if isinstance(val, memoryview) else val
            try:
                # Try to decode as UTF-8 first
                return b_val.decode('utf-8')
            except (UnicodeDecodeError, AttributeError):
                # Fallback to base64 for binary data
                import base64
                return base64.b64encode(b_val).decode('utf-8')
        if isinstance(val, decimal.Decimal):
            return float(val)
        if isinstance(val, (datetime.datetime, datetime.date, datetime.time)):
            return val.isoformat()
        if isinstance(val, uuid.UUID):
            return str(val)
        return val

    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"SQL Server connection test failed: {e}")
            return False

    def get_table_schema(self, table_name: str, schema: Optional[str] = None) -> dict[str, Any]:
        """Get table schema information"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            schema_name = schema or 'dbo'

            query = """
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """

            cursor.execute(query, (schema_name, table_name))

            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "COLUMN_NAME": row[0],
                    "DATA_TYPE": row[1],
                    "IS_NULLABLE": row[2],
                    "COLUMN_DEFAULT": row[3],
                    "CHARACTER_MAXIMUM_LENGTH": row[4]
                })

            cursor.close()
            conn.close()

            return {
                "table_name": table_name,
                "schema": schema_name,
                "columns": columns
            }
        except Exception as e:
            logger.error(f"Failed to get SQL Server table schema: {e}")
            raise

    def get_row_count(self, table_name: str, schema: Optional[str] = None, filters: Optional[dict[str, Any]] = None) -> int:
        """Get total row count"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            schema_name = schema or 'dbo'
            query = f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]"

            # Add filters if provided
            if filters:
                where_clauses = []
                params = []
                for key, value in filters.items():
                    where_clauses.append(f"[{key}] = ?")
                    params.append(value)
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)

            cursor.execute(query, tuple(params) if filters else None)
            count = cursor.fetchone()[0]

            cursor.close()
            conn.close()

            return count
        except Exception as e:
            logger.error(f"Failed to get SQL Server row count: {e}")
            raise

    def list_tables(self, schema: Optional[str] = None, search: Optional[str] = None, limit: int = 100, cursor: Optional[str] = None) -> list[dict[str, Any]]:
        """List tables in the database with pagination"""
        try:
            conn = self.get_connection()
            cursor_obj = conn.cursor()

            schema_name = schema or 'dbo'

            query = '''
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ?
                AND TABLE_TYPE = 'BASE TABLE'
            '''
            params = [schema_name]

            if search:
                query += " AND TABLE_NAME LIKE ?"
                params.append(f'%{search}%')

            if cursor:
                query += " AND TABLE_NAME > ?"
                params.append(cursor)

            query += " ORDER BY TABLE_NAME OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY"
            params.append(limit + 1)

            cursor_obj.execute(query, params)
            rows = cursor_obj.fetchall()

            tables = []
            for row in rows[:limit]:
                tables.append({
                    'schema': row[0],
                    'table_name': row[1]
                })

            cursor_obj.close()
            conn.close()

            return tables
        except Exception as e:
            logger.error(f"Failed to list SQL Server tables: {e}")
            raise

    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a query and return results"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(query, params)

            # Get column names
            columns = [column[0] for column in cursor.description]

            # Fetch all results
            rows = cursor.fetchall()

            # Convert to list of dicts with normalization
            results = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = self._normalize_value(row[i])
                results.append(row_dict)

            cursor.close()
            conn.close()

            return results
        except Exception as e:
            logger.error(f"Failed to execute SQL Server query: {e}")
            raise

    def get_table_data(self, table_name: str, schema: Optional[str] = None, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        """Get table data with pagination"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            schema_name = schema or 'dbo'
            table_ref = f"[{schema_name}].[{table_name}]"

            # Get total count
            count_query = f"SELECT COUNT(*) FROM {table_ref}"
            cursor.execute(count_query)
            total = cursor.fetchone()[0]

            # Get columns
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
                ORDER BY ordinal_position
            """, (schema_name, table_name))
            columns = [row[0] for row in cursor.fetchall()]

            # Get paginated data
            offset = (page - 1) * page_size
            data_query = f"SELECT * FROM {table_ref} ORDER BY (SELECT NULL) OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
            cursor.execute(data_query, (offset, page_size))

            rows = []
            for row in cursor.fetchall():
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = self._normalize_value(row[i])
                rows.append(row_dict)

            cursor.close()
            conn.close()

            return {
                "rows": rows,
                "columns": columns,
                "total": total,
                "has_more": offset + page_size < total,
                "page": page,
                "page_size": page_size
            }
        except Exception as e:
            logger.error(f"Failed to get SQL Server table data: {e}")
            raise

    def execute_filter(self, table_name: str, schema: Optional[str] = None, filters: Optional[dict[str, Any]] = None, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        """Execute filter conditions on a table"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            schema_name = schema or 'dbo'
            table_ref = f"[{schema_name}].[{table_name}]"

            # Build WHERE clause
            where_clause = ""
            where_params = []

            if filters:
                where_parts = []
                _build_sqlserver_where(filters, where_parts, where_params)
                if where_parts:
                    where_clause = " WHERE " + " AND ".join(where_parts)

            # Get total count
            count_query = f"SELECT COUNT(*) FROM {table_ref}{where_clause}"
            cursor.execute(count_query, tuple(where_params))
            total = cursor.fetchone()[0]

            # Get columns
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
                ORDER BY ordinal_position
            """, (schema_name, table_name))
            columns = [row[0] for row in cursor.fetchall()]

            # Get paginated filtered data
            offset = (page - 1) * page_size
            data_query = f"SELECT * FROM {table_ref}{where_clause} ORDER BY (SELECT NULL) OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
            cursor.execute(data_query, (*where_params, offset, page_size))

            rows = []
            for row in cursor.fetchall():
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = self._normalize_value(row[i])
                rows.append(row_dict)

            cursor.close()
            conn.close()

            return {
                "rows": rows,
                "columns": columns,
                "total": total,
                "has_more": offset + page_size < total,
                "page": page,
                "page_size": page_size
            }
        except Exception as e:
            logger.error(f"Failed to execute SQL Server filter: {e}")
            raise

def _build_sqlserver_where(filter_spec: dict[str, Any], where_parts: list[str], where_params: list[Any]):
    """Build SQL Server WHERE clause from filter specification"""
    if not filter_spec:
        return

    expr_type = filter_spec.get('type', 'condition')

    if expr_type == 'condition':
        column = filter_spec.get('column')
        operator = filter_spec.get('operator')
        value = filter_spec.get('value')

        if not column or not operator:
            return

        col_ref = f"[{column}]"

        if operator == 'IS NULL':
            where_parts.append(f"{col_ref} IS NULL")
        elif operator == 'IS NOT NULL':
            where_parts.append(f"{col_ref} IS NOT NULL")
        elif operator == '=':
            where_parts.append(f"{col_ref} = ?")
            where_params.append(value)
        elif operator == '!=':
            where_parts.append(f"{col_ref} != ?")
            where_params.append(value)
        elif operator in ['>', '<', '>=', '<=']:
            where_parts.append(f"{col_ref} {operator} ?")
            where_params.append(value)
        elif operator in ['LIKE']:
            where_parts.append(f"{col_ref} LIKE ?")
            where_params.append(f"%{value}%")
        elif operator == 'IN':
            if isinstance(value, (list, tuple)):
                placeholders = ','.join(['?'] * len(value))
                where_parts.append(f"{col_ref} IN ({placeholders})")
                where_params.extend(value)
        elif operator == 'NOT IN':
            if isinstance(value, (list, tuple)):
                placeholders = ','.join(['?'] * len(value))
                where_parts.append(f"{col_ref} NOT IN ({placeholders})")
                where_params.extend(value)
        elif operator == 'BETWEEN':
            if isinstance(value, (list, tuple)) and len(value) == 2:
                where_parts.append(f"{col_ref} BETWEEN ? AND ?")
                where_params.extend(value)

    elif expr_type == 'logical':
        operator = filter_spec.get('operator', 'AND')
        expressions = filter_spec.get('expressions', [])

        if len(expressions) > 1:
            sub_parts = []
            sub_params = []
            for expr in expressions:
                _build_sqlserver_where(expr, sub_parts, sub_params)

            if sub_parts:
                if operator == 'OR':
                    combined = f"({' OR '.join(sub_parts)})"
                else:
                    combined = f"({' AND '.join(sub_parts)})"
                where_parts.append(combined)
                where_params.extend(sub_params)
