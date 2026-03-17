"""
Temporary Table Manager for Complex Transformations
Handles aggregations, joins, and window functions efficiently using temp tables.
"""

import logging
from typing import Any, Optional
import uuid

import psycopg2
from psycopg2.extras import execute_batch

logger = logging.getLogger(__name__)

class TempTableManager:
    """
    Manages temporary tables in destination database for memory-efficient complex operations.

    Use cases:
    - Aggregations (GROUP BY, SUM, COUNT, AVG)
    - Large joins (> 100K rows)
    - Window functions (ROW_NUMBER, RANK, etc.)
    """

    def __init__(self, connection_config: dict[str, Any]):
        """
        Initialize temp table manager.

        Args:
            connection_config: Database connection config (host, port, database, user, password)
        """
        self.connection_config = connection_config
        self.temp_tables: list[str] = []
        self.connection = None

    def _connect(self):
        """Establish database connection."""
        if self.connection is None or self.connection.closed:
            self.connection = psycopg2.connect(
                host=self.connection_config.get("host") or self.connection_config.get("hostname"),
                port=int(self.connection_config.get("port", 5432)),
                dbname=self.connection_config.get("database"),
                user=self.connection_config.get("user") or self.connection_config.get("username"),
                password=self.connection_config.get("password", "")
            )
        return self.connection

    def _infer_columns_from_data(self, sample_rows: list[dict[str, Any]]) -> list[tuple]:
        """
        Infer column names and types from sample data.

        Returns:
            List of (column_name, pg_type) tuples
        """
        if not sample_rows:
            return []

        # Get all unique keys
        all_keys = set()
        for row in sample_rows[:100]:  # Sample first 100 rows
            all_keys.update(row.keys())

        # Infer types
        columns = []
        for key in all_keys:
            # Sample values for this key
            values = [row.get(key) for row in sample_rows[:100] if key in row]

            # Infer type
            pg_type = "TEXT"  # Default
            for val in values:
                if val is not None:
                    if isinstance(val, bool):
                        pg_type = "BOOLEAN"
                        break
                    elif isinstance(val, int):
                        pg_type = "BIGINT"
                    elif isinstance(val, float):
                        pg_type = "DOUBLE PRECISION"
                    elif isinstance(val, str):
                        pg_type = "TEXT"

            columns.append((key, pg_type))

        return columns

    async def create_temp_table_from_data(
        self,
        data: list[dict[str, Any]],
        table_name: Optional[str] = None,
        schema: Optional[list[dict]] = None
    ) -> str:
        """
        Create temporary table and load data into it.

        Args:
            data: List of row dictionaries
            table_name: Optional table name (auto-generated if not provided)
            schema: Optional schema definition [{"name": "col1", "datatype": "BIGINT"}, ...]

        Returns:
            Temporary table name
        """
        if not data:
            raise ValueError("Cannot create temp table from empty data")

        # Generate table name if not provided
        if table_name is None:
            table_name = f"temp_{uuid.uuid4().hex[:12]}"

        conn = self._connect()
        cursor = conn.cursor()

        try:
            # Determine columns
            if schema:
                columns = [(col["name"], col.get("datatype", "TEXT")) for col in schema]
            else:
                columns = self._infer_columns_from_data(data)

            # Create temp table
            col_defs = ", ".join([f'"{name}" {dtype}' for name, dtype in columns])
            create_sql = f'CREATE TEMP TABLE "{table_name}" ({col_defs})'

            logger.info(f"Creating temp table: {table_name} with {len(columns)} columns")
            cursor.execute(create_sql)

            # Insert data in batches
            column_names = [col[0] for col in columns]
            insert_sql = '''
                INSERT INTO "{table_name}" ({", ".join([f'"{c}"' for c in column_names])})
                VALUES ({", ".join(["%s"] * len(column_names))})
            '''

            # Convert dicts to tuples
            rows = [[row.get(col) for col in column_names] for row in data]

            # Batch insert
            execute_batch(cursor, insert_sql, rows, page_size=1000)
            conn.commit()

            logger.info(f"Loaded {len(rows)} rows into temp table {table_name}")

            # Track temp table for cleanup
            self.temp_tables.append(table_name)

            return table_name

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to create temp table: {e}")
            raise

    def execute_aggregation(
        self,
        temp_table: str,
        group_by: list[str],
        aggregates: list[dict[str, str]]
    ) -> list[dict[str, Any]]:
        """
        Execute aggregation on temp table.

        Args:
            temp_table: Name of temp table
            group_by: List of columns to group by
            aggregates: List of aggregation specs:
                [
                    {"function": "SUM", "column": "amount", "alias": "total_amount"},
                    {"function": "COUNT", "column": "*", "alias": "count"},
                    {"function": "AVG", "column": "price", "alias": "avg_price"}
                ]

        Returns:
            List of aggregated rows as dictionaries
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            # Build SELECT clause
            select_parts = []

            # Add GROUP BY columns
            for col in group_by:
                select_parts.append(f'"{col}"')

            # Add aggregates
            for agg in aggregates:
                func = agg["function"].upper()
                col = agg["column"]
                alias = agg.get("alias", f"{func}_{col}")

                if col == "*":
                    select_parts.append(f'{func}(*) AS "{alias}"')
                else:
                    select_parts.append(f'{func}("{col}") AS "{alias}"')

            select_clause = ", ".join(select_parts)

            # Build GROUP BY clause
            if group_by:
                group_clause = "GROUP BY " + ", ".join([f'"{col}"' for col in group_by])
            else:
                group_clause = ""

            # Execute aggregation
            sql = f'SELECT {select_clause} FROM "{temp_table}" {group_clause}'
            logger.info(f"Executing aggregation SQL: {sql}")

            cursor.execute(sql)

            # Fetch results
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            # Convert to list of dicts
            result = [dict(zip(columns, row)) for row in rows]

            logger.info(f"Aggregation returned {len(result)} rows")

            return result

        except Exception as e:
            logger.error(f"Aggregation failed: {e}")
            raise

    def execute_join(
        self,
        left_table: str,
        right_table: str,
        join_type: str,
        conditions: list[dict[str, str]]
    ) -> list[dict[str, Any]]:
        """
        Execute join on two temp tables.

        Args:
            left_table: Left temp table name
            right_table: Right temp table name
            join_type: "INNER", "LEFT", "RIGHT", "FULL"
            conditions: Join conditions:
                [{"left_column": "customer_id", "right_column": "customer_id", "operator": "="}]

        Returns:
            List of joined rows
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            # Build ON clause
            on_parts = []
            for cond in conditions:
                left_col = cond["left_column"]
                right_col = cond["right_column"]
                operator = cond.get("operator", "=")
                on_parts.append(f'l."{left_col}" {operator} r."{right_col}"')

            " AND ".join(on_parts)

            # Build SQL
            sql = '''
                SELECT l.*, r.*
                FROM "{left_table}" l
                {join_type} JOIN "{right_table}" r
                ON {on_clause}
            '''

            logger.info(f"Executing join SQL: {sql}")
            cursor.execute(sql)

            # Fetch results
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            result = [dict(zip(columns, row)) for row in rows]

            logger.info(f"Join returned {len(result)} rows")

            return result

        except Exception as e:
            logger.error(f"Join failed: {e}")
            raise

    def execute_window_function(
        self,
        temp_table: str,
        window_spec: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Execute window function on temp table.

        Args:
            temp_table: Temp table name
            window_spec: Window function specification:
                {
                    "function": "ROW_NUMBER",  # or RANK, DENSE_RANK, LAG, LEAD, etc.
                    "partition_by": ["department"],
                    "order_by": [{"column": "salary", "direction": "DESC"}],
                    "alias": "row_num"
                }

        Returns:
            List of rows with window function result
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            # Build window function
            window_spec["function"].upper()
            window_spec.get("alias", "window_result")

            # PARTITION BY
            partition_by = window_spec.get("partition_by", [])
            if partition_by:
                partition_clause = "PARTITION BY " + ", ".join([f'"{col}"' for col in partition_by])
            else:
                partition_clause = ""

            # ORDER BY
            order_by = window_spec.get("order_by", [])
            if order_by:
                order_parts = []
                for order in order_by:
                    col = order["column"]
                    direction = order.get("direction", "ASC")
                    order_parts.append(f'"{col}" {direction}')
                order_clause = "ORDER BY " + ", ".join(order_parts)
            else:
                order_clause = ""

            # Build SQL
            f"{partition_clause} {order_clause}".strip()
            sql = '''
                SELECT
                    *,
                    {func}() OVER ({window_clause}) AS "{alias}"
                FROM "{temp_table}"
            '''

            logger.info(f"Executing window function SQL: {sql}")
            cursor.execute(sql)

            # Fetch results
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            result = [dict(zip(columns, row)) for row in rows]

            logger.info(f"Window function returned {len(result)} rows")

            return result

        except Exception as e:
            logger.error(f"Window function failed: {e}")
            raise

    def cleanup(self):
        """Drop all temporary tables created by this manager."""
        if not self.connection or self.connection.closed:
            logger.warning("Cannot cleanup: connection is closed")
            return

        cursor = self.connection.cursor()

        for table in self.temp_tables:
            try:
                cursor.execute(f'DROP TABLE IF EXISTS "{table}"')
                logger.info(f"Dropped temp table: {table}")
            except Exception as e:
                logger.warning(f"Failed to drop temp table {table}: {e}")

        self.connection.commit()
        self.temp_tables.clear()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup temp tables."""
        self.cleanup()
        if self.connection and not self.connection.closed:
            self.connection.close()
