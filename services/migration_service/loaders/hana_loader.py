"""
SAP HANA Data Loader
"""

from datetime import date, datetime
import logging
from typing import Any, Optional

import hdbcli.dbapi as dbapi

from utils import get_business_columns_from_metadata, remap_rows_to_business_names

logger = logging.getLogger(__name__)

# HANA type mapping for CREATE TABLE (inferred from Python values)
_HANA_TYPE_MAP = {
    type(None): "NVARCHAR(5000)",
    str: "NVARCHAR(5000)",
    int: "BIGINT",
    float: "DOUBLE",
    bool: "BOOLEAN",
    datetime: "TIMESTAMP",
    date: "TIMESTAMP",
}

def _all_columns_from_data(data: list[dict[str, Any]], sample_size: int = 2000) -> list[str]:
    """
    Return ordered list of all unique column names that appear in any row.
    Uses first row's key order as base, then appends any extra keys from other rows.
    Ensures the destination table has every column present in the pipeline output.
    """
    if not data:
        return []
    sample = data[: min(sample_size, len(data))]
    first_keys = list(data[0].keys())
    seen = set(first_keys)
    ordered = list(first_keys)
    for row in sample[1:]:
        for k in row.keys():
            if k not in seen:
                seen.add(k)
                ordered.append(k)
    return ordered

def _infer_hana_columns(data: list[dict[str, Any]]) -> list[tuple[str, str]]:
    """
    Infer (column_name, hana_type) from all rows' columns (union of keys) and a sample for types.
    For mixed types in a column, prefer NVARCHAR(5000). None/null -> NVARCHAR(5000) nullable.
    """
    if not data:
        return []
    columns = _all_columns_from_data(data)
    result = []
    for col in columns:
        types_seen = set()
        for row in data[: min(100, len(data))]:
            val = row.get(col)
            if val is None:
                types_seen.add(type(None))
            else:
                types_seen.add(type(val))
        non_null = types_seen - {type(None)}
        if not non_null:
            hana_type = "NVARCHAR(5000)"
        elif str in types_seen or (len(non_null) > 1 and type(None) in types_seen):
            hana_type = "NVARCHAR(5000)"
        elif non_null >= {int, float}:
            hana_type = "DOUBLE"
        else:
            py_type = next(iter(non_null))
            hana_type = _HANA_TYPE_MAP.get(py_type, "NVARCHAR(5000)")
        result.append((col, hana_type))
    return result

class HanaLoader:
    def __init__(self):
        self.connection = None

    def _table_exists(self, cursor, schema: str, table_name: str) -> bool:
        """Check if table exists in HANA (SYS.M_TABLES or TABLES)."""
        try:
            if schema:
                cursor.execute(
                    "SELECT 1 FROM SYS.M_TABLES WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?",
                    (schema, table_name),
                )
            else:
                cursor.execute("SELECT 1 FROM SYS.M_TABLES WHERE TABLE_NAME = ?", (table_name,))
            return cursor.fetchone() is not None
        except Exception as e:
            logger.debug("Table existence check failed (trying TABLES view): %s", e)
            try:
                if schema:
                    cursor.execute(
                        "SELECT 1 FROM SYS.TABLES WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?",
                        (schema, table_name),
                    )
                else:
                    cursor.execute("SELECT 1 FROM SYS.TABLES WHERE TABLE_NAME = ?", (table_name,))
                return cursor.fetchone() is not None
            except Exception as e2:
                logger.warning("Table existence check failed: %s", e2)
                return False

    def _drop_table(self, cursor, schema: str, table_name: str) -> None:
        """Drop the table if it exists."""
        if schema:
            full_name = f'"{schema}"."{table_name}"'
        else:
            full_name = f'"{table_name}"'
        try:
            cursor.execute(f"DROP TABLE {full_name}")
            print(f"[HANA_LOADER] Dropped table: {full_name}")
            logger.info("HANA: dropped table %s", full_name)
        except Exception as e:
            # Table may not exist; HANA may raise
            err_msg = str(e).upper()
            if "DOES NOT EXIST" in err_msg or "NOT FOUND" in err_msg or "Unknown table" in err_msg:
                print(f"[HANA_LOADER] Table {full_name} did not exist (drop no-op)")
                logger.info("HANA: table %s did not exist, drop no-op", full_name)
            else:
                raise

    def _create_empty_table(self, cursor, schema: str, table_name: str) -> None:
        """Create an empty table with a single column when there is no data to infer schema."""
        if schema:
            full_name = f'"{schema}"."{table_name}"'
        else:
            full_name = f'"{table_name}"'
        create_sql = f'CREATE COLUMN TABLE {full_name} ("id" BIGINT)'
        print(f"[HANA_LOADER] Creating empty table: {full_name}")
        logger.info("HANA: creating empty table %s (no data to infer schema)", full_name)
        cursor.execute(create_sql)

    def _create_table_from_metadata(
        self, cursor, schema: str, table_name: str, column_metadata: list[dict[str, Any]]
    ) -> None:
        """Create table from destination business metadata only. Never uses technical/row keys."""
        columns = get_business_columns_from_metadata(column_metadata)
        if not columns:
            raise ValueError("column_metadata produced no business columns")
        if schema:
            full_name = f'"{schema}"."{table_name}"'
        else:
            full_name = f'"{table_name}"'
        col_defs = ", ".join(f'"{c}" NVARCHAR(5000)' for c in columns)
        create_sql = f"CREATE COLUMN TABLE {full_name} ({col_defs})"
        logger.info("HANA LOADER creating table from business metadata: %s", columns)
        print(f"[HANA_LOADER] Creating table from metadata: {full_name} with columns: {columns}")
        cursor.execute(create_sql)

    def _create_table(self, cursor, schema: str, table_name: str, data: list[dict[str, Any]]) -> None:
        """Create table in HANA from inferred column types. HANA has no IF NOT EXISTS; caller must check first."""
        columns_spec = _infer_hana_columns(data)
        col_defs = ", ".join(f'"{name}" {htype}' for name, htype in columns_spec)
        if schema:
            full_name = f'"{schema}"."{table_name}"'
        else:
            full_name = f'"{table_name}"'
        create_sql = f"CREATE COLUMN TABLE {full_name} ({col_defs})"
        print(f"[HANA_LOADER] Creating table: {full_name} with columns: {[c[0] for c in columns_spec]}")
        logger.info("HANA: creating table %s with %s columns: %s", full_name, len(columns_spec), [c[0] for c in columns_spec])
        cursor.execute(create_sql)
        print(f"[HANA_LOADER] Table created: {full_name}")
        logger.info("HANA: table %s created", full_name)

    async def load_data(
        self,
        data: list[dict[str, Any]],
        destination_config: dict[str, Any],
        table_name: str,
        schema: str = "",
        create_if_not_exists: bool = False,
        drop_and_reload: bool = False,
        column_metadata: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        """
        Load data into SAP HANA database.
        If column_metadata provided: table schema comes ONLY from metadata (business names). Never from row keys.
        If schema is set, uses qualified name "schema"."table_name".
        If create_if_not_exists is True, creates the table when it does not exist.
        If drop_and_reload is True, drops the table if it exists then creates and inserts (full reload).
        """
        try:
            if not destination_config or not (destination_config.get("host") or destination_config.get("hostname")):
                print("[HANA_LOADER] No destination config or host; skipping load.")
                logger.warning("No destination config or host; skipping load")
                return {"rows_loaded": 0, "message": "No destination config"}

            if not (table_name and table_name.strip()):
                print("[HANA_LOADER] ERROR: table_name is empty. Cannot create or insert. Set tableName in destination node config.")
                logger.error("HANA: table_name is empty")
                return {"rows_loaded": 0, "message": "table_name is required"}

            if schema:
                qualified_table = f'"{schema}"."{table_name}"'
            else:
                qualified_table = f'"{table_name}"'

            if data:
                logger.info("HANA LOADER received first row keys: %s", list(data[0].keys()))
            if column_metadata:
                data = remap_rows_to_business_names(data, column_metadata)
                if data:
                    logger.info("HANA LOADER row keys after remap: %s", list(data[0].keys()))
                columns = get_business_columns_from_metadata(column_metadata)
            else:
                columns = _all_columns_from_data(data)

            rows_to_insert = len(data) if data else 0
            print(f"[HANA_LOADER] Connecting to destination. Table={qualified_table}, rows_to_insert={rows_to_insert}, create_if_not_exists={create_if_not_exists}, drop_and_reload={drop_and_reload}")
            logger.info("HANA: connecting to destination, table=%s, rows_to_insert=%s, create_if_not_exists=%s, drop_and_reload=%s", qualified_table, rows_to_insert, create_if_not_exists, drop_and_reload)
            conn = self._connect(destination_config)
            cursor = conn.cursor()

            if drop_and_reload:
                print(f"[HANA_LOADER] Drop and reload: dropping table if exists {qualified_table}")
                logger.info("HANA: drop and reload - dropping table %s if exists", qualified_table)
                self._drop_table(cursor, schema, table_name)
                conn.commit()

            if create_if_not_exists:
                exists = self._table_exists(cursor, schema, table_name)
                print(f"[HANA_LOADER] Table exists check: {qualified_table} -> exists={exists}")
                if not exists:
                    try:
                        print(f"[HANA_LOADER] Creating table {qualified_table}")
                        if data or column_metadata:
                            if column_metadata:
                                self._create_table_from_metadata(cursor, schema, table_name, column_metadata)
                            else:
                                self._create_table(cursor, schema, table_name, data)
                        else:
                            self._create_empty_table(cursor, schema, table_name)
                        conn.commit()
                    except Exception as create_err:
                        err_msg = str(create_err).upper()
                        if "ALREADY EXISTS" in err_msg or "EXISTS" in err_msg:
                            print(f"[HANA_LOADER] Table already exists, proceeding to insert: {qualified_table}")
                            logger.info("Table %s already exists, proceeding to insert", qualified_table)
                            conn.rollback()
                        else:
                            raise

            if not data:
                cursor.close()
                conn.close()
                print(f"[HANA_LOADER] DONE: 0 rows (table created if requested): {qualified_table}")
                return {"rows_loaded": 0, "table_name": qualified_table, "message": "Table created; no data to load"}

            placeholders = ",".join(["?" for _ in columns])
            column_names = ",".join([f'"{col}"' for col in columns])
            insert_sql = f'INSERT INTO {qualified_table} ({column_names}) VALUES ({placeholders})'
            rows = [[row.get(col) for col in columns] for row in data]
            print(f"[HANA_LOADER] Inserting {len(rows)} rows into {qualified_table} (columns: {columns})")
            logger.info("HANA: inserting %s rows into %s", len(rows), qualified_table)
            cursor.executemany(insert_sql, rows)
            conn.commit()

            rows_loaded = len(rows)
            cursor.close()
            conn.close()

            print(f"[HANA_LOADER] DONE: {rows_loaded} rows loaded into {qualified_table}")
            logger.info("HANA: loaded %s rows into %s", rows_loaded, qualified_table)
            return {
                "rows_loaded": rows_loaded,
                "table_name": qualified_table,
                "message": f"Successfully loaded {rows_loaded} rows",
            }
        except Exception as e:
            logger.error("Error loading data to HANA: %s", e)
            raise

    def _connect(self, config: dict[str, Any]):
        """Connect to SAP HANA database. Uses empty string for missing user/password to avoid TypeError."""
        try:
            # dbapi.connect() requires user to be string, not None
            user = config.get("user") or config.get("username")
            if user is None:
                user = ""
            password = config.get("password")
            if password is None:
                password = ""
            conn = dbapi.connect(
                address=config.get("hostname") or config.get("host"),
                port=config.get("port", 30015),
                user=user,
                password=password,
                databaseName=config.get("database") or ""
            )
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to HANA: {e}")
            raise
