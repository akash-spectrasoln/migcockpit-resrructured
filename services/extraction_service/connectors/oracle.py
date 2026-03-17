"""
Oracle Database Connector
"""

import datetime
import decimal
import logging
from typing import Any, Optional
import uuid

import cx_Oracle

logger = logging.getLogger(__name__)

class OracleConnector:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.pool = None
        self._create_pool()

    def _create_pool(self):
        """Create connection pool for Oracle"""
        try:
            dsn = cx_Oracle.makedsn(
                self.config.get('hostname'),
                self.config.get('port', 1521),
                service_name=self.config.get('service_name') or self.config.get('database')
            )

            self.pool = cx_Oracle.SessionPool(
                user=self.config.get('user'),
                password=self.config.get('password'),
                dsn=dsn,
                min=2,
                max=5,
                increment=1
            )
            logger.info("Oracle connection pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create Oracle connection pool: {e}")
            raise

    def get_connection(self):
        """Get connection from pool"""
        try:
            return self.pool.acquire()
        except Exception as e:
            logger.error(f"Failed to get Oracle connection: {e}")
            raise

    def release_connection(self, conn):
        """Release connection back to pool"""
        try:
            self.pool.release(conn)
        except Exception as e:
            logger.error(f"Failed to release Oracle connection: {e}")

    def _normalize_value(self, val: Any) -> Any:
        """
        Normalize database values to JSON-serializable types.
        Handles: LOBs (Oracle), bytes, Decimal, datetime, UUID.
        """
        if val is None:
            return None

        # Handle Oracle LOBs (CLOB, BLOB)
        import cx_Oracle
        if isinstance(val, cx_Oracle.LOB):
            try:
                val = val.read()
            except Exception as e:
                logger.warning(f"Failed to read Oracle LOB: {e}")
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
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.close()
            self.release_connection(conn)
            return True
        except Exception as e:
            logger.error(f"Oracle connection test failed: {e}")
            return False

    def get_table_schema(self, table_name: str, schema: Optional[str] = None) -> dict[str, Any]:
        """Get table schema information"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            schema_name = schema or self.config.get('user', '').upper()

            query = """
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    NULLABLE,
                    DATA_DEFAULT,
                    DATA_LENGTH
                FROM ALL_TAB_COLUMNS
                WHERE OWNER = :schema AND TABLE_NAME = :table_name
                ORDER BY COLUMN_ID
            """

            cursor.execute(query, {"schema": schema_name, "table_name": table_name.upper()})

            columns = []
            for row in cursor:
                columns.append({
                    "COLUMN_NAME": row[0],
                    "DATA_TYPE": row[1],
                    "IS_NULLABLE": row[2],
                    "COLUMN_DEFAULT": row[3],
                    "CHARACTER_MAXIMUM_LENGTH": row[4]
                })

            cursor.close()
            self.release_connection(conn)

            return {
                "table_name": table_name,
                "schema": schema_name,
                "columns": columns
            }
        except Exception as e:
            logger.error(f"Failed to get Oracle table schema: {e}")
            raise

    def get_row_count(self, table_name: str, schema: Optional[str] = None, filters: Optional[dict[str, Any]] = None) -> int:
        """Get total row count"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            schema_name = schema or self.config.get('user', '').upper()
            query = f'SELECT COUNT(*) FROM "{schema_name}"."{table_name.upper()}"'

            # Add filters if provided
            if filters:
                where_clauses = []
                params = {}
                for key, value in filters.items():
                    where_clauses.append(f'"{key.upper()}" = :{key.lower()}')
                    params[key.lower()] = value
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)

            cursor.execute(query, params if filters else None)
            count = cursor.fetchone()[0]

            cursor.close()
            self.release_connection(conn)

            return count
        except Exception as e:
            logger.error(f"Failed to get Oracle row count: {e}")
            raise

    def list_tables(self, schema: Optional[str] = None, search: Optional[str] = None, limit: int = 100, cursor: Optional[str] = None) -> list[dict[str, Any]]:
        """List tables in the database with pagination"""
        try:
            conn = self.get_connection()
            cursor_obj = conn.cursor()

            schema_name = schema or self.config.get('user', '').upper()

            query = '''
                SELECT owner, table_name
                FROM all_tables
                WHERE owner = :schema
            '''
            params = {'schema': schema_name}

            if search:
                query += " AND table_name LIKE :search"
                params['search'] = f'%{search.upper()}%'

            if cursor:
                query += " AND table_name > :cursor"
                params['cursor'] = cursor.upper()

            query += " ORDER BY table_name FETCH FIRST :limit ROWS ONLY"
            params['limit'] = limit + 1

            cursor_obj.execute(query, params)
            rows = cursor_obj.fetchall()

            tables = []
            for row in rows[:limit]:
                tables.append({
                    'schema': row[0],
                    'table_name': row[1]
                })

            cursor_obj.close()
            self.release_connection(conn)

            return tables
        except Exception as e:
            logger.error(f"Failed to list Oracle tables: {e}")
            raise

    def execute_query(self, query: str, params: Optional[dict] = None):
        """Execute a query and return results"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(query, params or {})
            results = cursor.fetchall()

            # Get column names
            columns = [desc[0] for desc in cursor.description]

            # Convert to list of dicts with normalization
            result_list = []
            for row in results:
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = self._normalize_value(row[i])
                result_list.append(row_dict)

            cursor.close()
            self.release_connection(conn)

            return result_list
        except Exception as e:
            logger.error(f"Failed to execute Oracle query: {e}")
            raise

    def get_table_data(self, table_name: str, schema: Optional[str] = None, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        """Get table data with pagination"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            schema_name = schema or self.config.get('user', '').upper()
            table_ref = f'"{schema_name}"."{table_name.upper()}"'

            # Get total count
            count_query = f'SELECT COUNT(*) FROM {table_ref}'
            cursor.execute(count_query)
            total = cursor.fetchone()[0]

            # Get columns
            cursor.execute("""
                SELECT column_name
                FROM all_tab_columns
                WHERE owner = :schema AND table_name = :table_name
                ORDER BY column_id
            """, {"schema": schema_name, "table_name": table_name.upper()})
            columns = [row[0] for row in cursor.fetchall()]

            # Get paginated data
            offset = (page - 1) * page_size
            data_query = f'SELECT * FROM {table_ref} OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY'
            cursor.execute(data_query, {"offset": offset, "page_size": page_size})

            rows = []
            for row in cursor.fetchall():
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = self._normalize_value(row[i])
                rows.append(row_dict)

            cursor.close()
            self.release_connection(conn)

            return {
                "rows": rows,
                "columns": columns,
                "total": total,
                "has_more": offset + page_size < total,
                "page": page,
                "page_size": page_size
            }
        except Exception as e:
            logger.error(f"Failed to get Oracle table data: {e}")
            raise

    def execute_filter(self, table_name: str, schema: Optional[str] = None, filters: Optional[dict[str, Any]] = None, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        """Execute filter conditions on a table"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            schema_name = schema or self.config.get('user', '').upper()
            table_ref = f'"{schema_name}"."{table_name.upper()}"'

            # Build WHERE clause
            where_clause = ""
            where_params = {}

            if filters:
                where_parts = []
                _build_oracle_where(filters, where_parts, where_params)
                if where_parts:
                    where_clause = " WHERE " + " AND ".join(where_parts)

            # Get total count
            count_query = f'SELECT COUNT(*) FROM {table_ref}{where_clause}'
            cursor.execute(count_query, where_params)
            total = cursor.fetchone()[0]

            # Get columns
            cursor.execute("""
                SELECT column_name
                FROM all_tab_columns
                WHERE owner = :schema AND table_name = :table_name
                ORDER BY column_id
            """, {"schema": schema_name, "table_name": table_name.upper()})
            columns = [row[0] for row in cursor.fetchall()]

            # Get paginated filtered data
            offset = (page - 1) * page_size
            data_query = f'SELECT * FROM {table_ref}{where_clause} OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY'
            cursor.execute(data_query, {**where_params, "offset": offset, "page_size": page_size})

            rows = []
            for row in cursor.fetchall():
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = self._normalize_value(row[i])
                rows.append(row_dict)

            cursor.close()
            self.release_connection(conn)

            return {
                "rows": rows,
                "columns": columns,
                "total": total,
                "has_more": offset + page_size < total,
                "page": page,
                "page_size": page_size
            }
        except Exception as e:
            logger.error(f"Failed to execute Oracle filter: {e}")
            raise

def _build_oracle_where(filter_spec: dict[str, Any], where_parts: list[str], where_params: dict[str, Any], param_counter: Optional[list[int]] = None):
    """Build Oracle WHERE clause from filter specification"""
    if param_counter is None:
        param_counter = [0]
    if not filter_spec:
        return

    expr_type = filter_spec.get('type', 'condition')

    if expr_type == 'condition':
        column = filter_spec.get('column')
        operator = filter_spec.get('operator')
        value = filter_spec.get('value')

        if not column or not operator:
            return

        param_counter[0] += 1
        param_name = f"param{param_counter[0]}"
        col_ref = f'"{column.upper()}"'

        if operator == 'IS NULL':
            where_parts.append(f"{col_ref} IS NULL")
        elif operator == 'IS NOT NULL':
            where_parts.append(f"{col_ref} IS NOT NULL")
        elif operator == '=':
            where_parts.append(f"{col_ref} = :{param_name}")
            where_params[param_name] = value
        elif operator == '!=':
            where_parts.append(f"{col_ref} != :{param_name}")
            where_params[param_name] = value
        elif operator in ['>', '<', '>=', '<=']:
            where_parts.append(f"{col_ref} {operator} :{param_name}")
            where_params[param_name] = value
        elif operator in ['LIKE']:
            where_parts.append(f"{col_ref} LIKE :{param_name}")
            where_params[param_name] = f"%{value}%"
        elif operator == 'IN':
            if isinstance(value, (list, tuple)):
                param_names = []
                for i, v in enumerate(value):
                    pname = f"{param_name}_{i}"
                    param_names.append(f":{pname}")
                    where_params[pname] = v
                where_parts.append(f"{col_ref} IN ({','.join(param_names)})")
        elif operator == 'NOT IN':
            if isinstance(value, (list, tuple)):
                param_names = []
                for i, v in enumerate(value):
                    pname = f"{param_name}_{i}"
                    param_names.append(f":{pname}")
                    where_params[pname] = v
                where_parts.append(f"{col_ref} NOT IN ({','.join(param_names)})")
        elif operator == 'BETWEEN':
            if isinstance(value, (list, tuple)) and len(value) == 2:
                where_parts.append(f"{col_ref} BETWEEN :{param_name}_min AND :{param_name}_max")
                where_params[f"{param_name}_min"] = value[0]
                where_params[f"{param_name}_max"] = value[1]

    elif expr_type == 'logical':
        operator = filter_spec.get('operator', 'AND')
        expressions = filter_spec.get('expressions', [])

        if len(expressions) > 1:
            sub_parts = []
            sub_params = {}
            for expr in expressions:
                _build_oracle_where(expr, sub_parts, sub_params, param_counter)

            if sub_parts:
                if operator == 'OR':
                    combined = f"({' OR '.join(sub_parts)})"
                else:
                    combined = f"({' AND '.join(sub_parts)})"
                where_parts.append(combined)
                where_params.update(sub_params)
