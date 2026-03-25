"""
PostgreSQL Database Connector
"""

import datetime
import decimal
import logging
from typing import Any, Optional
import uuid

import psycopg2
from psycopg2 import pool as pg_pool

logger = logging.getLogger(__name__)

class PostgreSQLConnector:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.pool = None
        self._create_pool()

    def _create_pool(self):
        """Create connection pool for PostgreSQL"""
        try:
            # Build connection kwargs
            conn_kwargs = {
                'host': self.config.get('hostname'),
                'port': self.config.get('port', 5432),
                'database': self.config.get('database'),
                'user': self.config.get('user'),
                'password': self.config.get('password'),
                'sslmode': self.config.get('sslmode', 'require'),
            }
            logger.info(f"Creating PostgreSQL pool: host={conn_kwargs['host']}, port={conn_kwargs['port']}, db={conn_kwargs['database']}, sslmode={conn_kwargs['sslmode']}")
            self.pool = pg_pool.SimpleConnectionPool(
                2, 5,
                **conn_kwargs
            )
            logger.info("PostgreSQL connection pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create PostgreSQL connection pool: {e}")
            raise

    def get_connection(self):
        """Get connection from pool"""
        try:
            return self.pool.getconn()
        except Exception as e:
            logger.error(f"Failed to get PostgreSQL connection: {e}")
            raise

    def release_connection(self, conn: Any) -> None:
        """Release connection back to pool"""
        pool = self.pool
        if pool is None:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            return
        try:
            pool.putconn(conn)
        except Exception as e:
            logger.error(f"Failed to release PostgreSQL connection: {e}")

    def _normalize_value(self, val: Any) -> Any:
        """
        Normalize database values to JSON-serializable types.
        Handles: memoryview (from BYTEA), bytes, Decimal, datetime, UUID.
        """
        if val is None:
            return None
        if isinstance(val, (bytes, memoryview)):
            b_val = val.tobytes() if isinstance(val, memoryview) else val
            try:
                # Try to decode as UTF-8 first (in case it's actually text stored in BYTEA)
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
            self.release_connection(conn)
            return True
        except Exception as e:
            logger.error(f"PostgreSQL connection test failed: {e}")
            return False

    def list_tables(self, schema: Optional[str] = None, search: Optional[str] = None, limit: int = 100, cursor: Optional[str] = None) -> list[dict[str, Any]]:
        """List tables in the database with pagination"""
        try:
            conn = self.get_connection()
            cursor_obj = conn.cursor()

            schema_name = schema or self.config.get('schema', 'public')

            query = '''
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_type = 'BASE TABLE'
            '''
            params = [schema_name]

            if search:
                query += " AND table_name ILIKE %s"
                params.append(f'%{search}%')

            if cursor:
                query += " AND table_name > %s"
                params.append(cursor)

            query += " ORDER BY table_name LIMIT %s"
            params.append(limit + 1)  # Fetch one extra to check if there's more

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
            logger.error(f"Failed to list PostgreSQL tables: {e}")
            raise

    def get_table_schema(self, table_name: str, schema: Optional[str] = None) -> dict[str, Any]:
        """Get table schema information"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            schema_name = schema or self.config.get('schema', 'public')
            # Guard: ensure schema_name is a string (defense against Pydantic v2 deprecated .schema dict)
            if not isinstance(schema_name, str):
                logger.warning(f"Non-string schema value received: {type(schema_name).__name__}, defaulting to 'public'")
                schema_name = 'public'

            # First, try to find the table in the specified schema
            query = """
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """

            cursor.execute(query, (schema_name, table_name))
            rows = cursor.fetchall()

            # If no columns found in specified schema, try to find table in any schema
            if not rows:
                logger.warning(f"Table {table_name} not found in schema {schema_name}, searching all schemas...")
                search_query = """
                    SELECT DISTINCT table_schema
                    FROM information_schema.columns
                    WHERE table_name = %s
                    LIMIT 1
                """
                cursor.execute(search_query, (table_name,))
                search_row = cursor.fetchone()
                if search_row:
                    # Found in different schema
                    found_schema = search_row[0]
                    logger.info(f"Found table {table_name} in schema {found_schema} instead of {schema_name}")
                    # Re-query with correct schema
                    cursor.execute(query, (found_schema, table_name))
                    rows = cursor.fetchall()
                    schema_name = found_schema
                else:
                    logger.warning(f"Table {table_name} not found in any schema")

            columns = []
            for row in rows:
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
            if conn:
                try:
                    self.release_connection(conn)
                except Exception:
                    pass
            logger.error(f"Failed to get PostgreSQL table schema for {table_name} (schema: {schema}): {e}")
            raise

    def get_table_data(self, table_name: str, schema: Optional[str] = None, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        """Get table data with pagination"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            schema_name = schema or self.config.get('schema', 'public')

            # Get total count
            count_query = f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"'
            cursor.execute(count_query)
            total = cursor.fetchone()[0]

            # Get columns
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema_name, table_name))
            columns = [row[0] for row in cursor.fetchall()]

            # Get paginated data
            offset = (page - 1) * page_size
            data_query = f'SELECT * FROM "{schema_name}"."{table_name}" LIMIT %s OFFSET %s'
            cursor.execute(data_query, (page_size, offset))

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
            if conn:
                try:
                    self.release_connection(conn)
                except Exception:
                    pass
            logger.error(f"Failed to get PostgreSQL table data for {table_name} (schema: {schema}): {e}")
            raise

    def get_row_count(self, table_name: str, schema: Optional[str] = None, filters: Optional[dict[str, Any]] = None) -> int:
        """Get row count for table, optionally with filter_spec (pushdown: source columns only)."""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            schema_name = schema or self.config.get('schema', 'public')
            table_ref = f'"{schema_name}"."{table_name}"'
            where_clause = ""
            where_params = []
            if filters and filters.get("filter_spec"):
                where_parts: list[str] = []
                spec = filters["filter_spec"]
                if isinstance(spec, dict) and "conditions" in spec:
                    spec = spec["conditions"]
                _build_postgresql_where(spec, where_parts, where_params, cursor)
                if where_parts:
                    where_clause = " WHERE " + " AND ".join(where_parts)
            count_query = f'SELECT COUNT(*) FROM {table_ref}{where_clause}'
            cursor.execute(count_query, where_params)
            total = cursor.fetchone()[0]
            cursor.close()
            self.release_connection(conn)
            return total
        except Exception as e:
            if conn:
                try:
                    self.release_connection(conn)
                except Exception:
                    pass
            logger.error(f"Failed to get row count for {table_name} (schema: {schema}): {e}")
            raise

    def build_extraction_query_with_filter_spec(
        self,
        table_name: str,
        schema: Optional[str],
        filter_spec: dict[str, Any],
        offset: int,
        limit: int
    ) -> tuple:
        """Build (query_string, params) for extraction with filter (pushdown: source columns only)."""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            schema_name = schema or self.config.get('schema', 'public')
            table_ref = f'"{schema_name}"."{table_name}"'
            where_parts: list[str] = []
            where_params = []
            spec = filter_spec.get("conditions", filter_spec) if isinstance(filter_spec, dict) else filter_spec
            _build_postgresql_where(spec, where_parts, where_params, cursor)
            where_clause = " AND ".join(where_parts) if where_parts else ""
            query = f'SELECT * FROM {table_ref}'
            if where_clause:
                query += " WHERE " + where_clause
            query += " LIMIT %s OFFSET %s"
            params = [*where_params, limit, offset]
            cursor.close()
            self.release_connection(conn)
            return query, params
        except Exception as e:
            if conn:
                try:
                    self.release_connection(conn)
                except Exception:
                    pass
            logger.error(f"Failed to build extraction query for {table_name}: {e}")
            raise

    def execute_query(self, query: str, params: Optional[tuple] = None) -> list[dict[str, Any]]:
        """Execute query with params and return rows as list of dicts (column name -> value)."""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = []
            for row in cursor.fetchall():
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = self._normalize_value(row[i])
                rows.append(row_dict)
            cursor.close()
            self.release_connection(conn)
            return rows
        except Exception as e:
            if conn:
                try:
                    self.release_connection(conn)
                except Exception:
                    pass
            logger.error(f"Failed to execute query: {e}")
            raise

    def execute_filter(self, table_name: str, schema: Optional[str] = None, filters: Optional[dict[str, Any]] = None, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        """
        Execute filter conditions on a table and return filtered results.

        Args:
            table_name: Name of the table
            schema: Schema name (optional)
            filters: Filter specification dictionary
            page: Page number (default 1)
            page_size: Number of rows per page (default 50)

        Returns:
            Dictionary with rows, columns, total, has_more, page, page_size
        """
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            schema_name = schema or self.config.get('schema', 'public')
            table_ref = f'"{schema_name}"."{table_name}"'

            # Fetch table columns FIRST to validate filter columns exist
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema_name, table_name))
            column_metadata = cursor.fetchall()
            available_columns = {row[0] for row in column_metadata}

            # Validate filter columns exist before running query
            if filters:
                filter_columns = _extract_filter_columns(filters)
                missing = [c for c in filter_columns if c and c not in available_columns]
                if missing:
                    available_list = ", ".join(sorted(available_columns)) if available_columns else "(none)"
                    raise ValueError(
                        f"Column(s) {', '.join(repr(m) for m in missing)} do not exist in table "
                        f'"{schema_name}"."{table_name}". Available columns: {available_list}'
                    )

            logger.info(f"[PostgreSQLConnector.execute_filter] raw filters spec: {filters}")
            # Build WHERE clause from filter specification
            where_clause = ""
            where_params = []

            if filters:
                where_parts: list[str] = []
                _build_postgresql_where(filters, where_parts, where_params, cursor)
                if where_parts:
                    where_clause = " WHERE " + " AND ".join(where_parts)
            logger.info(f"[PostgreSQLConnector.execute_filter] table={table_name}, schema={schema_name}, where={where_clause}, params={where_params}")

            # Get total count
            count_query = f'SELECT COUNT(*) FROM {table_ref}{where_clause}'
            cursor.execute(count_query, where_params)
            total = cursor.fetchone()[0]

            # Build column list with type information (column_metadata already fetched above)
            columns = []
            column_type_map = {}  # Map column name to datatype for type-aware filtering
            for col_name, data_type, is_nullable in column_metadata:
                columns.append({
                    'name': col_name,
                    'datatype': data_type.upper(),
                    'nullable': is_nullable == 'YES'
                })
                column_type_map[col_name] = data_type.upper()

            # Get paginated filtered data
            offset = (page - 1) * page_size
            data_query = f'SELECT * FROM {table_ref}{where_clause} LIMIT %s OFFSET %s'
            cursor.execute(data_query, [*where_params, page_size, offset])

            rows = []
            for row in cursor.fetchall():
                row_dict = {}
                for i, col_meta in enumerate(columns):
                    col_name = col_meta['name']  # Extract name from metadata dict
                    row_dict[col_name] = self._normalize_value(row[i])
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
            if conn:
                try:
                    self.release_connection(conn)
                except Exception:
                    pass
            logger.error(f"Failed to execute filter on {table_name} (schema: {schema}): {e}")
            raise

    def execute_aggregate(
        self,
        table_name: str,
        select_clause: str,
        group_by_columns: Optional[list[str]] = None,
        schema: Optional[str] = None,
        filters: Optional[dict[str, Any]] = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        """
        Execute an aggregate query (SELECT ... GROUP BY ...) and return results.

        Args:
            table_name: Name of the source table.
            select_clause: Pre-built SELECT expression list, e.g.
                           '"region", SUM("amount") AS "total_amount"'
            group_by_columns: List of column names for GROUP BY.
            schema: Schema name (optional, defaults to config schema or 'public').
            filters: Optional filter specification (same format as execute_filter).
            page: Page number (1-based).
            page_size: Number of rows per page.

        Returns:
            Dictionary with rows, columns, total, has_more, page, page_size.
        """
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            schema_name = schema or self.config.get('schema', 'public')
            table_ref = f'"{schema_name}"."{table_name}"'

            # Build optional WHERE clause
            where_clause = ""
            where_params: list = []

            if filters:
                where_parts: list[str] = []
                _build_postgresql_where(filters, where_parts, where_params, cursor)
                if where_parts:
                    where_clause = " WHERE " + " AND ".join(where_parts)

            # Build GROUP BY clause
            group_by_clause = ""
            if group_by_columns:
                quoted_gb = [f'"{col}"' for col in group_by_columns]
                group_by_clause = " GROUP BY " + ", ".join(quoted_gb)

            # Assemble the aggregate query
            base_query = f'SELECT {select_clause} FROM {table_ref}{where_clause}{group_by_clause}'

            logger.info(f"[PostgreSQLConnector.execute_aggregate] SQL: {base_query}")
            logger.info(f"[PostgreSQLConnector.execute_aggregate] params: {where_params}")

            # Total aggregated row count (wrap the aggregate as a subquery)
            count_query = f'SELECT COUNT(*) FROM ({base_query}) AS _agg_count'
            cursor.execute(count_query, where_params)
            total = cursor.fetchone()[0]

            # Paginated data
            offset = (page - 1) * page_size
            data_query = f'{base_query} LIMIT %s OFFSET %s'
            cursor.execute(data_query, [*where_params, page_size, offset])

            col_names = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = []
            for row in cursor.fetchall():
                row_dict = {}
                for i, col in enumerate(col_names):
                    row_dict[col] = self._normalize_value(row[i])
                rows.append(row_dict)

            cursor.close()
            self.release_connection(conn)

            return {
                "rows": rows,
                "columns": col_names,
                "total": total,
                "has_more": (offset + page_size) < total,
                "page": page,
                "page_size": page_size,
            }
        except Exception as e:
            if conn:
                try:
                    self.release_connection(conn)
                except Exception:
                    pass
            logger.error(f"Failed to execute aggregate on {table_name} (schema: {schema}): {e}")
            raise

    def execute_join(self, left_table: str, right_table: str, left_schema: Optional[str] = None, right_schema: Optional[str] = None, join_type: str = 'INNER', conditions: Optional[list[dict[str, Any]]] = None, filters: Optional[dict[str, Any]] = None, page: int = 1, page_size: int = 50, output_columns: Optional[list[dict[str, Any]]] = None) -> dict[str, Any]:
        """
        Execute join operation between two tables and return results.

        Args:
            left_table: Name of the left table
            right_table: Name of the right table
            left_schema: Schema name for left table (optional)
            right_schema: Schema name for right table (optional)
            join_type: Type of join (INNER, LEFT, RIGHT, FULL OUTER, CROSS)
            conditions: List of join conditions, each with 'left_column' and 'right_column'
            filters: Filter specification dictionary to apply to the joined results (optional)
            page: Page number (default 1)
            page_size: Number of rows per page (default 50)

        Returns:
            Dictionary with rows, columns, total, has_more, page, page_size
        """
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            left_schema_name = left_schema or self.config.get('schema', 'public')
            right_schema_name = right_schema or self.config.get('schema', 'public')

            # CRITICAL: Handle self-joins (same table on both sides) by using table aliases
            # Also use aliases for all joins to simplify SQL and avoid quoting issues
            is_self_join = left_table == right_table and left_schema_name == right_schema_name

            # Use aliases for all joins (required for self-joins, helpful for all joins)
            left_table_alias = 't1'
            right_table_alias = 't2'

            # Build table references with aliases
            left_table_ref = f'"{left_schema_name}"."{left_table}" AS {left_table_alias}'
            right_table_ref = f'"{right_schema_name}"."{right_table}" AS {right_table_alias}'

            # For conditions and column references, always use aliases
            left_table_ref_for_cols = left_table_alias
            right_table_ref_for_cols = right_table_alias

            if is_self_join:
                logger.info(f"[PostgreSQLConnector.execute_join] Self-join detected: {left_table} = {right_table}, using aliases: {left_table_alias}, {right_table_alias}")
            else:
                logger.info(f"[PostgreSQLConnector.execute_join] Using aliases for join: {left_table_alias} (left), {right_table_alias} (right)")

            # Get columns with data types from both tables (needed for column validation and output_metadata)
            left_columns: list[str] = []
            left_column_types: dict[str, Any] = {}  # Map column_name -> data_type
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (left_schema_name, left_table))
            for row in cursor.fetchall():
                col_name = row[0]
                data_type = row[1]
                is_nullable = row[2] == 'YES'
                left_columns.append(col_name)
                left_column_types[col_name] = {
                    'data_type': data_type,
                    'nullable': is_nullable
                }

            right_columns: list[str] = []
            right_column_types: dict[str, Any] = {}  # Map column_name -> data_type
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (right_schema_name, right_table))
            for row in cursor.fetchall():
                col_name = row[0]
                data_type = row[1]
                is_nullable = row[2] == 'YES'
                right_columns.append(col_name)
                right_column_types[col_name] = {
                    'data_type': data_type,
                    'nullable': is_nullable
                }

            # Build JOIN clause
            join_clause = ""
            if join_type.upper() == 'CROSS':
                join_clause = f"CROSS JOIN {right_table_ref}"
            else:
                # Build ON conditions
                on_conditions: list[str] = []
                if conditions and len(conditions) > 0:
                    for condition in conditions:
                        # Support both snake_case and camelCase field names
                        left_col = condition.get('left_column') or condition.get('leftColumn', '')
                        right_col = condition.get('right_column') or condition.get('rightColumn', '')
                        operator = condition.get('operator', '=')

                        if not left_col or not right_col:
                            continue

                        # Handle column names with __L__ and __R__ prefixes (UI convention)
                        # Extract column name and determine which table it belongs to
                        def extract_column_name(col_name: str, default_source: str) -> tuple[str, str]:
                            """Extract column name and determine source table.
                            Returns: (column_name, source) where source is 'left' or 'right'
                            """
                            col_name = col_name.strip('"')

                            # Check for __L__ or __R__ prefix
                            if col_name.startswith('__L__.'):
                                return (col_name.replace('__L__.', '', 1), 'left')
                            elif col_name.startswith('__R__.'):
                                return (col_name.replace('__R__.', '', 1), 'right')

                            # Check for other prefixes (t1, t2, table names, etc.)
                            if '.' in col_name:
                                parts = col_name.split('.', 1)
                                prefix = parts[0].strip('"')
                                col_part = parts[1].strip('"')

                                # Map known prefixes
                                if prefix in ['t1', left_table, left_table_alias]:
                                    return (col_part, 'left')
                                elif prefix in ['t2', right_table, right_table_alias]:
                                    return (col_part, 'right')
                                else:
                                    # Unknown prefix, use default source
                                    return (col_part, default_source)

                            # No prefix, use as-is with default source
                            return (col_name, default_source)

                        # Extract column names and validate they exist in their respective tables
                        left_col_name, left_source = extract_column_name(left_col, 'left')
                        right_col_name, right_source = extract_column_name(right_col, 'right')

                        if left_source == 'left' and left_col_name not in left_columns:
                            msg = f"Column '{left_col_name}' does not exist in left table '{left_table}' (schema: '{left_schema_name}'). Available columns: "
                            subset: list[str] = []
                            for i in range(min(10, len(left_columns))):
                                subset.append(str(left_columns[i]))
                            msg += ", ".join(subset)
                            if len(left_columns) > 10:
                                msg += "..."
                            raise ValueError(msg)

                        if right_source == 'right' and right_col_name not in right_columns:
                            msg = f"Column '{right_col_name}' does not exist in right table '{right_table}' (schema: '{right_schema_name}'). Available columns: "
                            subset: list[str] = []
                            for i in range(min(10, len(right_columns))):
                                subset.append(str(right_columns[i]))
                            msg += ", ".join(subset)
                            if len(right_columns) > 10:
                                msg += "..."
                            raise ValueError(msg)

                        # Build column references using table aliases
                        if left_source == 'left':
                            left_col_ref = f'{left_table_ref_for_cols}."{left_col_name}"'
                        else:
                            # Shouldn't happen, but handle gracefully
                            left_col_ref = f'{left_table_ref_for_cols}."{left_col_name}"'

                        if right_source == 'right':
                            right_col_ref = f'{right_table_ref_for_cols}."{right_col_name}"'
                        else:
                            # Shouldn't happen, but handle gracefully
                            right_col_ref = f'{right_table_ref_for_cols}."{right_col_name}"'

                        on_conditions.append(f'{left_col_ref} {operator} {right_col_ref}')

                if len(on_conditions) == 0 and join_type.upper() != 'CROSS':
                    raise ValueError("Join conditions are required for non-CROSS joins")

                join_keyword = {
                    'INNER': 'INNER JOIN',
                    'LEFT': 'LEFT JOIN',
                    'RIGHT': 'RIGHT JOIN',
                    'FULL OUTER': 'FULL OUTER JOIN',
                    'FULL': 'FULL OUTER JOIN'
                }.get(join_type.upper(), 'INNER JOIN')

                join_clause = f"{join_keyword} {right_table_ref} ON {' AND '.join(on_conditions)}"

            # Build WHERE clause from filter specification (if provided)
            where_clause = ""
            where_params = []

            if filters:
                where_parts: list[str] = []
                # Create a context for column name resolution in joins
                def resolve_column_name(col_name: str) -> str:
                    """Resolve column name to table-qualified format for joins (always uses aliases)"""
                    # If already prefixed with table name or alias (e.g., "left_table.column", "t1.column", or "right_table.column")
                    if '.' in col_name:
                        parts = col_name.split('.', 1)
                        table_part = parts[0]
                        col_part = parts[1]
                        # Map table name/alias to table reference (always use aliases: t1, t2)
                        if table_part == left_table or table_part == left_table_alias or table_part == 'left':
                            return f'{left_table_ref_for_cols}."{col_part}"'
                        elif table_part == right_table or table_part == right_table_alias or table_part == 'right':
                            return f'{right_table_ref_for_cols}."{col_part}"'
                        # Unknown table prefix, try to use as-is
                        return f'"{col_name}"'
                    else:
                        # Column name without prefix - try to find in both tables
                        # First check if it exists in left table columns
                        if col_name in left_columns:
                            return f'{left_table_ref_for_cols}."{col_name}"'
                        elif col_name in right_columns:
                            return f'{right_table_ref_for_cols}."{col_name}"'
                        else:
                            # Column not found in either table, use left table as default
                            return f'{left_table_ref_for_cols}."{col_name}"'

                # Build WHERE clause with column resolution
                _build_postgresql_where_join(filters, where_parts, where_params, cursor, resolve_column_name)
                if where_parts:
                    where_clause = " WHERE " + " AND ".join(where_parts)
            logger.info(f"[PostgreSQLConnector.execute_join] left_table={left_table}, right_table={right_table}, left_schema={left_schema_name}, right_schema={right_schema_name}, join_type={join_type}")
            logger.info(f"[PostgreSQLConnector.execute_join] where={where_clause}, params={where_params}")

            # Build SELECT query
            # If output_columns is provided, use only included columns with aliases
            # Otherwise, use SELECT * (default behavior)
            if output_columns and len(output_columns) > 0:
                # Filter to only included columns
                included_cols = [col for col in output_columns if col.get('included', True)]

                if len(included_cols) > 0:
                    select_parts = []
                    output_column_names = []

                    for col_config in included_cols:
                        source = col_config.get('source', 'left')
                        column_name = col_config.get('column', '')
                        # Use outputName if provided (user-renamed output field), otherwise use column name
                        output_name = col_config.get('outputName') or col_config.get('output_name') or column_name

                        # Determine table reference based on source (always use aliases)
                        if source == 'left':
                            table_ref = left_table_ref_for_cols  # Always use alias (t1)
                            col_ref = f'{table_ref}."{column_name}"'
                        else:  # right
                            table_ref = right_table_ref_for_cols  # Always use alias (t2)
                            col_ref = f'{table_ref}."{column_name}"'

                        # Use output_name as the SQL alias (AS clause)
                        select_parts.append(f'{col_ref} AS "{output_name}"')
                        output_column_names.append(output_name)

                    select_clause = ', '.join(select_parts)
                    select_query = f'SELECT {select_clause} FROM {left_table_ref} {join_clause}{where_clause}'
                    columns = output_column_names
                    logger.info(f"[PostgreSQLConnector.execute_join] Using output columns: {len(included_cols)} columns")
                else:
                    # No included columns, fall back to SELECT * (always use aliases)
                    select_query = f'SELECT {left_table_ref_for_cols}.*, {right_table_ref_for_cols}.* FROM {left_table_ref} {join_clause}{where_clause}'
                    # Build column names using __L__ and __R__ prefixes (UI convention) instead of t1/t2
                    columns = [f"__L__.{col}" for col in left_columns] + [f"__R__.{col}" for col in right_columns]
                    logger.info("[PostgreSQLConnector.execute_join] No included columns, using SELECT * with aliases")
            else:
                # Default behavior: SELECT * from both tables (always use aliases)
                select_query = f'SELECT {left_table_ref_for_cols}.*, {right_table_ref_for_cols}.* FROM {left_table_ref} {join_clause}{where_clause}'
                # Build column names using __L__ and __R__ prefixes (UI convention) instead of t1/t2
                columns = [f"__L__.{col}" for col in left_columns] + [f"__R__.{col}" for col in right_columns]
                logger.info("[PostgreSQLConnector.execute_join] No output columns specified, using SELECT * with aliases")

            # Get total count
            count_query = f'SELECT COUNT(*) FROM {left_table_ref} {join_clause}{where_clause}'
            cursor.execute(count_query, where_params)
            total = cursor.fetchone()[0]

            # Get paginated data
            offset = (page - 1) * page_size
            data_query = f'{select_query} LIMIT %s OFFSET %s'
            logger.info(f"[PostgreSQLConnector.execute_join] Executing query: {data_query}")
            logger.info(f"[PostgreSQLConnector.execute_join] Query params: {[*where_params, page_size, offset]}")
            cursor.execute(data_query, [*where_params, page_size, offset])

            rows = []
            fetched_rows = cursor.fetchall()
            logger.info(f"[PostgreSQLConnector.execute_join] Fetched {len(fetched_rows)} rows")
            logger.info(f"[PostgreSQLConnector.execute_join] Column count: {len(columns)}, Expected row length: {len(left_columns) + len(right_columns)}")

            for row_idx, row in enumerate(fetched_rows):
                if row_idx == 0:
                    logger.info(f"[PostgreSQLConnector.execute_join] First row sample: {row[:5] if len(row) > 5 else row}")
                row_dict = {}
                # Map columns to values based on output column configuration
                if output_columns is not None:
                    _out_cols: list[dict[str, Any]] = output_columns if output_columns is not None else []
                    included_cols = [col for col in _out_cols if col.get('included', True)]
                    for idx, col_config in enumerate(included_cols):
                        # Use outputName if provided (user-renamed output field), otherwise use column name
                        output_name = col_config.get('outputName') or col_config.get('output_name') or col_config.get('column', '')
                        row_dict[output_name] = self._normalize_value(row[idx])
                else:
                    # Default: map with __L__ and __R__ prefixes to match column names
                    col_idx = 0
                    for col in left_columns:
                        row_dict[f"__L__.{col}"] = self._normalize_value(row[col_idx])
                        col_idx += 1
                    for col in right_columns:
                        row_dict[f"__R__.{col}"] = self._normalize_value(row[col_idx])
                        col_idx += 1
                rows.append(row_dict)

            cursor.close()
            self.release_connection(conn)

            has_more = (offset + page_size) < total

            # Build output_metadata with column types for downstream nodes (Projection, Filter, etc.)
            output_metadata_columns = []

            # Map PostgreSQL data types to standard types
            def normalize_type(pg_type: str) -> str:
                """Normalize PostgreSQL data types to standard types"""
                pg_type_lower = pg_type.upper()
                if pg_type_lower in ['INTEGER', 'INT', 'SMALLINT', 'BIGINT', 'SERIAL', 'BIGSERIAL']:
                    return 'INTEGER'
                elif pg_type_lower in ['NUMERIC', 'DECIMAL', 'REAL', 'DOUBLE PRECISION', 'FLOAT', 'FLOAT4', 'FLOAT8']:
                    return 'NUMERIC'
                elif pg_type_lower in ['BOOLEAN', 'BOOL']:
                    return 'BOOLEAN'
                elif pg_type_lower in ['TIMESTAMP', 'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE']:
                    return 'TIMESTAMP'
                elif pg_type_lower in ['DATE']:
                    return 'DATE'
                elif pg_type_lower in ['TIME', 'TIME WITHOUT TIME ZONE', 'TIME WITH TIME ZONE']:
                    return 'TIME'
                else:
                    return 'TEXT'  # Default for VARCHAR, CHAR, TEXT, etc.

            # Build metadata for selected output columns
            if output_columns is not None:
                _out_cols_meta: list[dict[str, Any]] = output_columns if output_columns is not None else []
                included_cols = [col for col in _out_cols_meta if col.get('included', True)]
                for col_config in included_cols:
                    source = col_config.get('source', 'left')
                    column_name = col_config.get('column', '')
                    output_name = col_config.get('outputName') or col_config.get('output_name') or column_name

                    # Get type from appropriate table
                    col_name_str = str(column_name)
                    left_info = left_column_types.get(col_name_str) if isinstance(left_column_types, dict) else None
                    right_info = right_column_types.get(col_name_str) if isinstance(right_column_types, dict) else None
                    if source == 'left' and left_info is not None:
                        col_type_info = left_info
                        data_type = normalize_type(str(col_type_info.get('data_type')))
                        nullable = bool(col_type_info.get('nullable'))
                    elif source == 'right' and right_info is not None:
                        col_type_info = right_info
                        data_type = normalize_type(str(col_type_info.get('data_type')))
                        nullable = bool(col_type_info.get('nullable'))
                    else:
                        # Fallback if column not found
                        data_type = 'TEXT'
                        nullable = True

                    output_metadata_columns.append({
                        'name': output_name,
                        'datatype': data_type,
                        'data_type': data_type,  # Alias for compatibility
                        'nullable': nullable,
                        'source': source,
                        'original_column': column_name
                    })
            else:
                # Default: include all columns from both tables
                for col in left_columns:
                    col_type_info = left_column_types.get(col, {'data_type': 'TEXT', 'nullable': True})
                    data_type = normalize_type(col_type_info['data_type'])
                    nullable = col_type_info['nullable']
                    output_metadata_columns.append({
                        'name': f"__L__.{col}",
                        'datatype': data_type,
                        'data_type': data_type,
                        'nullable': nullable,
                        'source': '__L__',
                        'original_column': col
                    })

                for col in right_columns:
                    col_type_info = right_column_types.get(col, {'data_type': 'TEXT', 'nullable': True})
                    data_type = normalize_type(col_type_info['data_type'])
                    nullable = col_type_info['nullable']
                    output_metadata_columns.append({
                        'name': f"__R__.{col}",
                        'datatype': data_type,
                        'data_type': data_type,
                        'nullable': nullable,
                        'source': '__R__',
                        'original_column': col
                    })

            return {
                "rows": rows,
                "columns": columns,
                "total": total,
                "has_more": has_more,
                "page": page,
                "page_size": page_size,
                "output_metadata": {
                    "columns": output_metadata_columns
                }
            }

        except Exception as e:
            if conn:
                try:
                    self.release_connection(conn)
                except Exception:
                    pass
            logger.error(f"Failed to execute PostgreSQL join: {e}")
            raise

def _build_postgresql_where_join(filter_spec: dict[str, Any], where_parts: list[str], where_params: list[Any], cursor, resolve_column_name):
    """Recursively build PostgreSQL WHERE clause from filter specification for joins."""
    if not filter_spec:
        return

    expr_type = filter_spec.get('type', 'condition')

    if expr_type == 'condition':
        column = filter_spec.get('column')
        operator = filter_spec.get('operator')
        value = filter_spec.get('value')

        if not column or not operator:
            return

        # Use column resolver to get table-qualified column reference
        col_ref = resolve_column_name(column)

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

        elif operator in ['LIKE', 'ILIKE']:
            where_parts.append(f"{col_ref} {operator} %s")
            where_params.append(f"%{value}%")

        elif operator == 'IN':
            if isinstance(value, (list, tuple)):
                placeholders = ','.join(['%s'] * len(value))
                where_parts.append(f"{col_ref} IN ({placeholders})")
                where_params.extend(value)
            else:
                where_parts.append(f"{col_ref} = %s")
                where_params.append(value)

        elif operator == 'NOT IN':
            if isinstance(value, (list, tuple)):
                placeholders = ','.join(['%s'] * len(value))
                where_parts.append(f"{col_ref} NOT IN ({placeholders})")
                where_params.extend(value)

        elif operator == 'BETWEEN':
            if isinstance(value, (list, tuple)) and len(value) == 2:
                where_parts.append(f"{col_ref} BETWEEN %s AND %s")
                where_params.extend(value)
            elif isinstance(value, dict):
                min_val = value.get('min') or value.get('from')
                max_val = value.get('max') or value.get('to')
                if min_val is not None and max_val is not None:
                    where_parts.append(f"{col_ref} BETWEEN %s AND %s")
                    where_params.extend([min_val, max_val])

    elif expr_type == 'logical':
        operator = filter_spec.get('operator', 'AND')
        expressions = filter_spec.get('expressions', [])

        if len(expressions) > 1:
            sub_parts = []
            sub_params = []
            for expr in expressions:
                _build_postgresql_where_join(expr, sub_parts, sub_params, cursor, resolve_column_name)

            if sub_parts:
                if operator == 'OR':
                    combined = f"({' OR '.join(sub_parts)})"
                else:  # AND
                    combined = f"({' AND '.join(sub_parts)})"
                where_parts.append(combined)
                where_params.extend(sub_params)
    elif expr_type == 'expression':
        # Raw SQL expression mode used by filter expression editor preview.
        # Expression text is validated upstream before being sent here.
        expression = str(filter_spec.get('expression') or '').strip()
        if expression:
            where_parts.append(f"({expression})")
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

        elif operator in ['LIKE', 'ILIKE']:
            where_parts.append(f"{col_ref} {operator} %s")
            where_params.append(f"%{value}%")

        elif operator == 'IN':
            if isinstance(value, (list, tuple)):
                placeholders = ','.join(['%s'] * len(value))
                where_parts.append(f"{col_ref} IN ({placeholders})")
                where_params.extend(value)
            else:
                where_parts.append(f"{col_ref} = %s")
                where_params.append(value)

        elif operator == 'NOT IN':
            if isinstance(value, (list, tuple)):
                placeholders = ','.join(['%s'] * len(value))
                where_parts.append(f"{col_ref} NOT IN ({placeholders})")
                where_params.extend(value)

        elif operator == 'BETWEEN':
            if isinstance(value, (list, tuple)) and len(value) == 2:
                where_parts.append(f"{col_ref} BETWEEN %s AND %s")
                where_params.extend(value)
            elif isinstance(value, dict):
                min_val = value.get('min') or value.get('from')
                max_val = value.get('max') or value.get('to')
                if min_val is not None and max_val is not None:
                    where_parts.append(f"{col_ref} BETWEEN %s AND %s")
                    where_params.extend([min_val, max_val])

    elif expr_type == 'logical':
        operator = filter_spec.get('operator', 'AND')
        expressions = filter_spec.get('expressions', [])

        if len(expressions) > 1:
            sub_parts = []
            sub_params = []
            for expr in expressions:
                _build_postgresql_where(expr, sub_parts, sub_params, cursor)

            if sub_parts:
                if operator == 'OR':
                    combined = f"({' OR '.join(sub_parts)})"
                else:  # AND
                    combined = f"({' AND '.join(sub_parts)})"
                where_parts.append(combined)
                where_params.extend(sub_params)

def _extract_filter_columns(filter_spec) -> list[str]:
    """Extract all column names used in a filter specification."""
    columns = []
    if not filter_spec:
        return columns

    if isinstance(filter_spec, list):
        for condition in filter_spec:
            col = condition.get('column')
            if col:
                columns.append(col)
        return columns

    if filter_spec.get('type') == 'condition':
        col = filter_spec.get('column')
        if col:
            columns.append(col)
        return columns

    if filter_spec.get('type') == 'logical':
        for expr in filter_spec.get('expressions', []):
            columns.extend(_extract_filter_columns(expr))
        return columns

    return columns

def _build_postgresql_where(filter_spec, where_parts: list[str], where_params: list[Any], cursor):
    """Recursively build PostgreSQL WHERE clause from filter specification.

    Handles two formats:
    1. List format (legacy): [{'column': 'id', 'operator': '=', 'value': '2', 'logicalOperator': 'AND'}, ...]
    2. Dict format (internal): {'type': 'logical', 'operator': 'AND', 'expressions': [...]}
    """
    if not filter_spec:
        return

    # Handle list format (legacy) - convert to conditions
    if isinstance(filter_spec, list):
        for i, condition in enumerate(filter_spec):
            column = condition.get('column')
            operator = condition.get('operator')
            value = condition.get('value')
            condition.get('logicalOperator', 'AND') if i > 0 else None

            if not column or not operator:
                continue

            col_ref = f'"{column}"'

            # Build condition based on operator
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
            elif operator in ['LIKE', 'ILIKE']:
                where_parts.append(f"{col_ref} {operator} %s")
                where_params.append(f"%{value}%")
            elif operator == 'IN':
                if isinstance(value, (list, tuple)):
                    placeholders = ','.join(['%s'] * len(value))
                    where_parts.append(f"{col_ref} IN ({placeholders})")
                    where_params.extend(value)
                else:
                    where_parts.append(f"{col_ref} = %s")
                    where_params.append(value)
            elif operator == 'NOT IN':
                if isinstance(value, (list, tuple)):
                    placeholders = ','.join(['%s'] * len(value))
                    where_parts.append(f"{col_ref} NOT IN ({placeholders})")
                    where_params.extend(value)
            elif operator == 'BETWEEN':
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    where_parts.append(f"{col_ref} BETWEEN %s AND %s")
                    where_params.extend(value)
                elif isinstance(value, dict):
                    min_val = value.get('min') or value.get('from')
                    max_val = value.get('max') or value.get('to')
                    if min_val is not None and max_val is not None:
                        where_parts.append(f"{col_ref} BETWEEN %s AND %s")
                        where_params.extend([min_val, max_val])
        return

    # Handle dict format (internal)
    expr_type = filter_spec.get('type', 'condition')

    if expr_type == 'condition':
        column = filter_spec.get('column')
        operator = filter_spec.get('operator')
        value = filter_spec.get('value')

        if not column or not operator:
            return

        col_ref = f'"{column}"'

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

        elif operator in ['LIKE', 'ILIKE']:
            where_parts.append(f"{col_ref} {operator} %s")
            where_params.append(f"%{value}%")

        elif operator == 'IN':
            if isinstance(value, (list, tuple)):
                placeholders = ','.join(['%s'] * len(value))
                where_parts.append(f"{col_ref} IN ({placeholders})")
                where_params.extend(value)
            else:
                where_parts.append(f"{col_ref} = %s")
                where_params.append(value)

        elif operator == 'NOT IN':
            if isinstance(value, (list, tuple)):
                placeholders = ','.join(['%s'] * len(value))
                where_parts.append(f"{col_ref} NOT IN ({placeholders})")
                where_params.extend(value)

        elif operator == 'BETWEEN':
            if isinstance(value, (list, tuple)) and len(value) == 2:
                where_parts.append(f"{col_ref} BETWEEN %s AND %s")
                where_params.extend(value)
            elif isinstance(value, dict):
                min_val = value.get('min') or value.get('from')
                max_val = value.get('max') or value.get('to')
                if min_val is not None and max_val is not None:
                    where_parts.append(f"{col_ref} BETWEEN %s AND %s")
                    where_params.extend([min_val, max_val])

    elif expr_type == 'logical':
        operator = filter_spec.get('operator', 'AND')
        expressions = filter_spec.get('expressions', [])

        if len(expressions) > 1:
            sub_parts = []
            sub_params = []
            for expr in expressions:
                _build_postgresql_where(expr, sub_parts, sub_params, cursor)

            if sub_parts:
                if operator == 'OR':
                    combined = f"({' OR '.join(sub_parts)})"
                else:  # AND
                    combined = f"({' AND '.join(sub_parts)})"
                where_parts.append(combined)
                where_params.extend(sub_params)
