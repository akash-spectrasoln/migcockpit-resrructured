"""
MySQL Database Connector
"""

import datetime
import decimal
import logging
from typing import Any, Optional
import uuid

from mysql.connector import pooling

logger = logging.getLogger(__name__)

class MySQLConnector:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.pool = None
        self._create_pool()

    def _create_pool(self):
        """Create connection pool for MySQL"""
        pool_config = {
            'pool_name': 'mysql_pool',
            'pool_size': 5,
            'pool_reset_session': True,
            'host': self.config.get('hostname'),
            'port': self.config.get('port', 3306),
            'user': self.config.get('user'),
            'password': self.config.get('password'),
            'database': self.config.get('database'),
            'charset': 'utf8mb4',
        }

        try:
            self.pool = pooling.MySQLConnectionPool(**pool_config)
            logger.info("MySQL connection pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create MySQL connection pool: {e}")
            raise

    def get_connection(self):
        """Get connection from pool"""
        try:
            return self.pool.get_connection()
        except Exception as e:
            logger.error(f"Failed to get MySQL connection: {e}")
            raise

    def _normalize_value(self, val: Any) -> Any:
        """
        Normalize database values to JSON-serializable types.
        Handles: bytes, Decimal, datetime, UUID.
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
            logger.error(f"MySQL connection test failed: {e}")
            return False

    def get_table_schema(self, table_name: str, schema: Optional[str] = None) -> dict[str, Any]:
        """Get table schema information"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True)

            db_name = schema or self.config.get('database')
            query = """
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """

            cursor.execute(query, (db_name, table_name))
            columns = cursor.fetchall()

            cursor.close()
            conn.close()

            return {
                "table_name": table_name,
                "schema": db_name,
                "columns": columns
            }
        except Exception as e:
            logger.error(f"Failed to get MySQL table schema: {e}")
            raise

    def get_row_count(self, table_name: str, schema: Optional[str] = None, filters: Optional[dict[str, Any]] = None) -> int:
        """Get total row count"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            db_name = schema or self.config.get('database')
            query = f"SELECT COUNT(*) FROM `{db_name}`.`{table_name}`"

            # Add filters if provided
            if filters:
                where_clauses = []
                params = []
                for key, value in filters.items():
                    where_clauses.append(f"`{key}` = %s")
                    params.append(value)
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)

            cursor.execute(query, tuple(params) if filters else None)
            count = cursor.fetchone()[0]

            cursor.close()
            conn.close()

            return count
        except Exception as e:
            logger.error(f"Failed to get MySQL row count: {e}")
            raise

    def list_tables(self, schema: Optional[str] = None, search: Optional[str] = None, limit: int = 100, cursor: Optional[str] = None) -> list[dict[str, Any]]:
        """List tables in the database with pagination"""
        try:
            conn = self.get_connection()
            cursor_obj = conn.cursor()

            database = schema or self.config.get('database')

            query = '''
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_type = 'BASE TABLE'
            '''
            params = [database]

            if search:
                query += " AND table_name LIKE %s"
                params.append(f'%{search}%')

            if cursor:
                query += " AND table_name > %s"
                params.append(cursor)

            query += " ORDER BY table_name LIMIT %s"
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
            logger.error(f"Failed to list MySQL tables: {e}")
            raise

    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a query and return results"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True)

            cursor.execute(query, params)
            results = cursor.fetchall()

            # Normalize results
            normalized_results = []
            for row in results:
                normalized_results.append({k: self._normalize_value(v) for k, v in row.items()})

            cursor.close()
            conn.close()

            return normalized_results
        except Exception as e:
            logger.error(f"Failed to execute MySQL query: {e}")
            raise

    def get_table_data(self, table_name: str, schema: Optional[str] = None, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        """Get table data with pagination"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True)

            db_name = schema or self.config.get('database')
            table_ref = f"`{db_name}`.`{table_name}`"

            # Get total count
            count_query = f"SELECT COUNT(*) as total FROM {table_ref}"
            cursor.execute(count_query)
            total = cursor.fetchone()['total']

            # Get columns
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (db_name, table_name))
            columns = [row['column_name'] for row in cursor.fetchall()]

            # Get paginated data
            offset = (page - 1) * page_size
            data_query = f"SELECT * FROM {table_ref} LIMIT %s OFFSET %s"
            cursor.execute(data_query, (page_size, offset))

            rows = cursor.fetchall()

            # Normalize rows
            normalized_rows = []
            for row in rows:
                normalized_rows.append({k: self._normalize_value(v) for k, v in row.items()})

            cursor.close()
            conn.close()

            return {
                "rows": normalized_rows,
                "columns": columns,
                "total": total,
                "has_more": offset + page_size < total,
                "page": page,
                "page_size": page_size
            }
        except Exception as e:
            logger.error(f"Failed to get MySQL table data: {e}")
            raise

    def execute_filter(self, table_name: str, schema: Optional[str] = None, filters: Optional[dict[str, Any]] = None, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        """Execute filter conditions on a table"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True)

            db_name = schema or self.config.get('database')
            table_ref = f"`{db_name}`.`{table_name}`"

            # Build WHERE clause
            where_clause = ""
            where_params = []

            if filters:
                where_parts = []
                _build_mysql_where(filters, where_parts, where_params)
                if where_parts:
                    where_clause = " WHERE " + " AND ".join(where_parts)

            # Get total count
            count_query = f"SELECT COUNT(*) as total FROM {table_ref}{where_clause}"
            cursor.execute(count_query, tuple(where_params))
            total = cursor.fetchone()['total']

            # Get columns WITH datatypes for type preservation
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (db_name, table_name))
            column_metadata = cursor.fetchall()

            # Build column list with type information
            columns = []
            for col_meta in column_metadata:
                columns.append({
                    'name': col_meta['column_name'],
                    'datatype': col_meta['data_type'].upper(),
                    'nullable': col_meta['is_nullable'] == 'YES'
                })

            # Get paginated filtered data
            offset = (page - 1) * page_size
            data_query = f"SELECT * FROM {table_ref}{where_clause} LIMIT %s OFFSET %s"
            cursor.execute(data_query, (*where_params, page_size, offset))

            rows = cursor.fetchall()

            # Normalize rows
            normalized_rows = []
            for row in rows:
                normalized_rows.append({k: self._normalize_value(v) for k, v in row.items()})

            cursor.close()
            conn.close()

            return {
                "rows": normalized_rows,
                "columns": columns,
                "total": total,
                "has_more": offset + page_size < total,
                "page": page,
                "page_size": page_size
            }
        except Exception as e:
            logger.error(f"Failed to execute MySQL filter: {e}")
            raise

def _build_mysql_where(filter_spec: dict[str, Any], where_parts: list[str], where_params: list[Any]):
    """Build MySQL WHERE clause from filter specification"""
    if not filter_spec:
        return

    expr_type = filter_spec.get('type', 'condition')

    if expr_type == 'condition':
        column = filter_spec.get('column')
        operator = filter_spec.get('operator')
        value = filter_spec.get('value')

        if not column or not operator:
            return

        col_ref = f"`{column}`"

        if operator == 'IS NULL':
            where_parts.append(f"{col_ref} IS NULL")
        elif operator == 'IS NOT NULL':
            where_parts.append(f"{col_ref} IS NOT NULL")
        elif operator == '=':
            where_parts.append(f"{col_ref} = %s")
            where_params.append(value)
        elif operator == '!=':
            where_parts.append(f"{col_ref} != %s")
            where_params.append(value)
        elif operator in ['>', '<', '>=', '<=']:
            where_parts.append(f"{col_ref} {operator} %s")
            where_params.append(value)
        elif operator in ['LIKE']:
            where_parts.append(f"{col_ref} LIKE %s")
            where_params.append(f"%{value}%")
        elif operator == 'IN':
            if isinstance(value, (list, tuple)):
                placeholders = ','.join(['%s'] * len(value))
                where_parts.append(f"{col_ref} IN ({placeholders})")
                where_params.extend(value)
        elif operator == 'NOT IN':
            if isinstance(value, (list, tuple)):
                placeholders = ','.join(['%s'] * len(value))
                where_parts.append(f"{col_ref} NOT IN ({placeholders})")
                where_params.extend(value)
        elif operator == 'BETWEEN':
            if isinstance(value, (list, tuple)) and len(value) == 2:
                where_parts.append(f"{col_ref} BETWEEN %s AND %s")
                where_params.extend(value)

    elif expr_type == 'logical':
        operator = filter_spec.get('operator', 'AND')
        expressions = filter_spec.get('expressions', [])

        if len(expressions) > 1:
            sub_parts = []
            sub_params = []
            for expr in expressions:
                _build_mysql_where(expr, sub_parts, sub_params)

            if sub_parts:
                if operator == 'OR':
                    combined = f"({' OR '.join(sub_parts)})"
                else:
                    combined = f"({' AND '.join(sub_parts)})"
                where_parts.append(combined)
                where_params.extend(sub_params)
    elif expr_type == 'expression':
        expression = str(filter_spec.get('expression') or '').strip()
        if expression:
            where_parts.append(f"({expression})")
