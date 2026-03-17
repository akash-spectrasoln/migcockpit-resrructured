"""
PostgreSQL Data Loader
Same contract as HanaLoader: load_data(data, destination_config, table_name, schema="", create_if_not_exists=False).
Uses COPY FROM STDIN for bulk load (fast) when data is non-empty; falls back to executemany if COPY fails.
"""

import csv
from datetime import date, datetime
import io
import logging
from typing import Any, Optional

import psycopg2

from utils import get_business_columns_from_metadata, remap_rows_to_business_names

logger = logging.getLogger(__name__)

# Normalize column name so display names (e.g. with spaces) match created columns. Preserves case and underscores.
def _normalize_column_name(name: str) -> str:
    if not name or not isinstance(name, str):
        return name
    return name.strip().replace(" ", "_")

# PostgreSQL type mapping for CREATE TABLE (inferred from Python values). Use TEXT for strings to avoid length limits.
_PG_TYPE_MAP = {
    type(None): "TEXT",
    str: "TEXT",
    int: "BIGINT",
    float: "DOUBLE PRECISION",
    bool: "BOOLEAN",
    datetime: "TIMESTAMP",
    date: "TIMESTAMP",
}

def _all_columns_from_data(data: list[dict[str, Any]], sample_size: int = 2000) -> list[str]:
    """
    Return ordered list of all unique column names that appear in any row.
    Uses first row's key order as base, then appends any extra keys from other rows.
    This ensures the destination table has every column present in the pipeline output.
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

def _pg_type_from_metadata(datatype: str) -> str:
    """Map metadata datatype to PostgreSQL type."""
    if not datatype:
        return "TEXT"
    dt = str(datatype).upper()
    if dt in ("INTEGER", "INT", "BIGINT", "SMALLINT"):
        return "BIGINT"
    if dt in ("NUMERIC", "DECIMAL", "REAL", "FLOAT", "DOUBLE"):
        return "DOUBLE PRECISION"
    if dt in ("BOOLEAN", "BOOL"):
        return "BOOLEAN"
    if dt in ("TIMESTAMP", "DATE", "TIME", "DATETIME"):
        return "TIMESTAMP"
    return "TEXT"

def _infer_pg_columns(
    data: list[dict[str, Any]],
    normalized_columns: Optional[list[str]] = None,
    norm_to_orig: Optional[dict[str, str]] = None,
) -> list[tuple[str, str]]:
    """
    Infer (column_name, pg_type). If norm_to_orig is provided, column_name is normalized and values are read via original key.
    """
    if not data:
        return []
    if normalized_columns is None:
        normalized_columns = _all_columns_from_data(data)
    if norm_to_orig is None:
        norm_to_orig = {c: c for c in normalized_columns}
    result = []
    for col in normalized_columns:
        orig = norm_to_orig.get(col, col)
        types_seen = set()
        for row in data[: min(100, len(data))]:
            val = row.get(orig)
            if val is None:
                types_seen.add(type(None))
            else:
                types_seen.add(type(val))
        non_null = types_seen - {type(None)}
        if not non_null:
            pg_type = "TEXT"
        elif str in types_seen or (len(non_null) > 1 and type(None) in types_seen):
            pg_type = "TEXT"
        elif non_null >= {int, float}:
            pg_type = "DOUBLE PRECISION"
        else:
            py_type = next(iter(non_null))
            pg_type = _PG_TYPE_MAP.get(py_type, "TEXT")
        result.append((col, pg_type))
    return result

def _value_to_copy_cell(val: Any) -> str:
    """Convert a Python value to string for PostgreSQL COPY CSV. None -> empty (NULL)."""
    if val is None:
        return ""
    if isinstance(val, bool):
        return "t" if val else ""
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    return str(val)

def _load_via_copy(
    cursor,
    qualified_table: str,
    columns: list[str],
    rows: list[list[Any]],
) -> None:
    """
    Bulk load using PostgreSQL COPY FROM STDIN (CSV). Much faster than executemany for large datasets.
    """
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    for row in rows:
        writer.writerow([_value_to_copy_cell(v) for v in row])
    buf.seek(0)
    column_list = ",".join(f'"{c}"' for c in columns)
    copy_sql = f"COPY {qualified_table} ({column_list}) FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL '')"
    cursor.copy_expert(copy_sql, buf)

class PostgresLoader:
    def __init__(self):
        self.connection = None

    def _connect(self, config: dict[str, Any]):
        """Connect to PostgreSQL. Uses empty string for missing user/password."""
        host = config.get("hostname") or config.get("host") or ""
        port = config.get("port", 5432)
        database = config.get("database") or ""
        user = config.get("user") or config.get("username")
        if user is None:
            user = ""
        password = config.get("password")
        if password is None:
            password = ""
        try:
            conn = psycopg2.connect(
                host=host,
                port=int(port) if port is not None else 5432,
                dbname=database,
                user=user,
                password=password,
            )
            return conn
        except Exception as e:
            logger.error("Failed to connect to PostgreSQL: %s", e)
            raise

    def _table_exists(self, cursor, schema: str, table_name: str) -> bool:
        """Return True if the table exists in the given schema."""
        if schema:
            cursor.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table_name),
            )
        else:
            cursor.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
                """,
                (table_name,),
            )
        return cursor.fetchone() is not None

    def _get_existing_columns(self, cursor, schema: str, table_name: str) -> list[str]:
        """Return list of column names for an existing table (empty if table missing)."""
        schema_qual = schema or "public"
        cursor.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema_qual, table_name),
        )
        return [row[0] for row in cursor.fetchall()]

    def _drop_table(self, cursor, schema: str, table_name: str) -> None:
        """Drop the table if it exists (CASCADE to handle dependencies)."""
        if schema:
            full_name = f'"{schema}"."{table_name}"'
        else:
            full_name = f'"{table_name}"'
        cursor.execute(f'DROP TABLE IF EXISTS {full_name} CASCADE')
        print(f"[POSTGRES_LOADER] Dropped table (schema mismatch): {full_name}")
        logger.info("PostgreSQL: dropped table %s (schema mismatch)", full_name)

    def _create_empty_table_if_not_exists(self, cursor, schema: str, table_name: str) -> None:
        """Create an empty table with a single placeholder column when there is no data to infer schema."""
        if schema:
            full_name = f'"{schema}"."{table_name}"'
        else:
            full_name = f'"{table_name}"'
        create_sql = f'CREATE TABLE IF NOT EXISTS {full_name} ("id" SERIAL PRIMARY KEY)'
        print(f"[POSTGRES_LOADER] Creating empty table: {full_name}")
        logger.info("PostgreSQL: creating empty table %s (no data to infer schema)", full_name)
        cursor.execute(create_sql)

    def _create_table_if_not_exists(
        self,
        cursor,
        schema: str,
        table_name: str,
        data: list[dict[str, Any]],
        normalized_columns: Optional[list[str]] = None,
        norm_to_orig: Optional[dict[str, str]] = None,
    ) -> None:
        """Create table in PostgreSQL from inferred column types. Uses CREATE TABLE IF NOT EXISTS."""
        columns_spec = _infer_pg_columns(data, normalized_columns, norm_to_orig)
        col_defs = ", ".join(f'"{name}" {ptype}' for name, ptype in columns_spec)
        if schema:
            full_name = f'"{schema}"."{table_name}"'
        else:
            full_name = f'"{table_name}"'
        create_sql = f"CREATE TABLE IF NOT EXISTS {full_name} ({col_defs})"
        print(f"[POSTGRES_LOADER] Creating table: {full_name} with columns: {[c[0] for c in columns_spec]}")
        logger.info("PostgreSQL: creating table %s with %s columns: %s", full_name, len(columns_spec), [c[0] for c in columns_spec])
        cursor.execute(create_sql)
        print(f"[POSTGRES_LOADER] Table created (or already existed): {full_name}")
        logger.info("PostgreSQL: table %s created (IF NOT EXISTS)", full_name)

    def _create_table_from_metadata(
        self,
        cursor,
        schema: str,
        table_name: str,
        column_metadata: list[dict[str, Any]],
    ) -> None:
        """Create table from destination business metadata only. Never uses technical/row keys."""
        columns = get_business_columns_from_metadata(column_metadata)
        if not columns:
            raise ValueError("column_metadata produced no business columns")
        # Match by position: columns[i] corresponds to column_metadata[i] (with duplicate resolution)
        columns_spec = []
        for i, col_name in enumerate(columns):
            m = column_metadata[i] if i < len(column_metadata) else {}
            dtype = _pg_type_from_metadata(m.get("datatype") or m.get("data_type"))
            columns_spec.append((col_name, dtype))
        col_defs = ", ".join(f'"{name}" {ptype}' for name, ptype in columns_spec)
        if schema:
            full_name = f'"{schema}"."{table_name}"'
        else:
            full_name = f'"{table_name}"'
        create_sql = f"CREATE TABLE IF NOT EXISTS {full_name} ({col_defs})"
        logger.info("LOADER creating table from business metadata: %s", columns)
        print(f"[POSTGRES_LOADER] Creating table from metadata: {full_name} with columns: {columns}")
        cursor.execute(create_sql)

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
        Load data into PostgreSQL database.
        If column_metadata provided: table schema comes ONLY from metadata (business names). Never from row keys.
        If schema is set, uses qualified name "schema"."table_name".
        If create_if_not_exists is True, runs CREATE TABLE IF NOT EXISTS then INSERT.
        If drop_and_reload is True, drops the table if it exists then creates and inserts (full reload).
        """
        try:
            if not destination_config or not (destination_config.get("host") or destination_config.get("hostname")):
                print("[POSTGRES_LOADER] No destination config or host; skipping load.")
                logger.warning("No destination config or host; skipping load")
                return {"rows_loaded": 0, "message": "No destination config"}

            if not (table_name and table_name.strip()):
                print("[POSTGRES_LOADER] ERROR: table_name is empty. Cannot create or insert. Set tableName in destination node config.")
                logger.error("PostgreSQL: table_name is empty")
                return {"rows_loaded": 0, "message": "table_name is required"}

            if schema:
                qualified_table = f'"{schema}"."{table_name}"'
            else:
                qualified_table = f'"{table_name}"'

            # GOLDEN RULE: When column_metadata provided, use it for schema. Remap rows before any inference.
            if data:
                logger.info("LOADER received first row keys: %s", list(data[0].keys()) if data else [])
            if column_metadata:
                logger.info("LOADER column_metadata (business names): %s", [c.get("business_name") or c.get("name") for c in column_metadata[:20]])

            rows_to_insert = len(data) if data else 0
            print(f"[POSTGRES_LOADER] Connecting to destination. Table={qualified_table}, rows_to_insert={rows_to_insert}, create_if_not_exists={create_if_not_exists}, drop_and_reload={drop_and_reload}")
            logger.info("PostgreSQL: connecting to destination, table=%s, rows_to_insert=%s, create_if_not_exists=%s, drop_and_reload=%s", qualified_table, rows_to_insert, create_if_not_exists, drop_and_reload)
            conn = self._connect(destination_config)
            cursor = conn.cursor()

            # When column_metadata provided: remap rows, use metadata for schema only
            if column_metadata:
                data = remap_rows_to_business_names(data, column_metadata)
                if data:
                    logger.info("LOADER row keys after remap: %s", list(data[0].keys()))
                columns = get_business_columns_from_metadata(column_metadata)
                norm_to_orig = {c: c for c in columns}
            else:
                # Fallback: infer from row keys (when no metadata)
                raw_columns = _all_columns_from_data(data) if data else []
                if data and not raw_columns:
                    seen = set()
                    for row in data:
                        if row and isinstance(row, dict):
                            for k in row.keys():
                                if k not in seen:
                                    seen.add(k)
                                    raw_columns.append(k)
                if data and not raw_columns:
                    raise ValueError(
                        "Cannot load data: rows are present but no column names could be inferred. "
                        "Ensure upstream data has dict rows with non-empty keys."
                    )
                columns = raw_columns
                norm_to_orig = {c: c for c in columns}

            normalized_columns = columns

            if drop_and_reload:
                print(f"[POSTGRES_LOADER] Drop and reload: dropping table if exists {qualified_table}")
                logger.info("PostgreSQL: drop and reload - dropping table %s if exists", qualified_table)
                self._drop_table(cursor, schema, table_name)
                conn.commit()

            if create_if_not_exists:
                print(f"[POSTGRES_LOADER] Creating table if not exists for {qualified_table}")
                if data:
                    required_cols = set(normalized_columns)
                    exists = self._table_exists(cursor, schema, table_name)
                    if exists:
                        existing_cols = set(self._get_existing_columns(cursor, schema, table_name))
                        if existing_cols != required_cols:
                            self._drop_table(cursor, schema, table_name)
                            exists = False
                    if not exists:
                        if column_metadata:
                            self._create_table_from_metadata(
                                cursor, schema, table_name, column_metadata,
                            )
                        else:
                            self._create_table_if_not_exists(
                                cursor, schema, table_name, data,
                                normalized_columns=normalized_columns,
                                norm_to_orig=norm_to_orig,
                            )
                else:
                    self._create_empty_table_if_not_exists(cursor, schema, table_name)
                conn.commit()

            if not data:
                cursor.close()
                conn.close()
                print(f"[POSTGRES_LOADER] DONE: 0 rows (table created if requested): {qualified_table}")
                return {"rows_loaded": 0, "table_name": qualified_table, "message": "Table created; no data to load"}

            columns = normalized_columns
            # Use original keys from norm_to_orig so display-name keys in row dict match
            rows = [[row.get(norm_to_orig.get(col, col)) for col in columns] for row in data]
            print(f"[POSTGRES_LOADER] Inserting {len(rows)} rows into {qualified_table} (columns: {columns})")
            logger.info("PostgreSQL: inserting %s rows into %s", len(rows), qualified_table)

            # Prefer COPY (bulk API) for speed; fall back to executemany if COPY fails
            try:
                _load_via_copy(cursor, qualified_table, columns, rows)
                print(f"[POSTGRES_LOADER] Used COPY bulk load for {len(rows)} rows")
                logger.info("PostgreSQL: COPY bulk load completed for %s rows", len(rows))
            except Exception as copy_err:
                logger.warning("PostgreSQL: COPY failed (%s), falling back to executemany", copy_err)
                print(f"[POSTGRES_LOADER] COPY failed, using executemany: {copy_err}")
                placeholders = ",".join(["%s"] * len(columns))
                column_names = ",".join([f'"{col}"' for col in columns])
                insert_sql = f'INSERT INTO {qualified_table} ({column_names}) VALUES ({placeholders})'
                batch_size = 500
                for i in range(0, len(rows), batch_size):
                    batch = rows[i : i + batch_size]
                    try:
                        cursor.executemany(insert_sql, batch)
                    except Exception as batch_err:
                        logger.error(
                            "PostgreSQL: insert failed at batch %s-%s of %s: %s",
                            i + 1, i + len(batch), len(rows), batch_err,
                        )
                        raise
            conn.commit()

            rows_loaded = len(rows)
            cursor.close()
            conn.close()

            print(f"[POSTGRES_LOADER] DONE: {rows_loaded} rows loaded into {qualified_table}")
            logger.info("PostgreSQL: loaded %s rows into %s", rows_loaded, qualified_table)
            return {
                "rows_loaded": rows_loaded,
                "table_name": qualified_table,
                "message": f"Successfully loaded {rows_loaded} rows",
            }
        except Exception as e:
            logger.error("Error loading data to PostgreSQL: %s", e)
            raise
