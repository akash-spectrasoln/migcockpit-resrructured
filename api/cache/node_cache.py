# Moved from: api/services/node_cache.py
"""
Node Cache Manager for storing and retrieving node transformation results.
Each node's output is cached in the customer database under CANVAS_CACHE schema.
"""

import hashlib
import json
import logging
from typing import Any, Optional

from django.conf import settings
import psycopg2

logger = logging.getLogger(__name__)

class NodeCacheManager:
    """
    Manages caching of node transformation results in customer database.

    Cache tables are created in CANVAS_CACHE schema with format:
    - node_{canvas_id}_{node_id} - stores the actual data
    - node_cache_metadata - stores cache metadata (timestamps, row counts, etc.)
    """

    CACHE_SCHEMA = "CANVAS_CACHE"
    METADATA_TABLE = "node_cache_metadata"

    def __init__(self, customer_db: str):
        """
        Initialize cache manager for a customer database.

        Args:
            customer_db: Customer database name (e.g., 'C00001')
        """
        self.customer_db = customer_db
        self._ensure_schema_exists()

    def _get_connection(self):
        """Get database connection to customer database."""
        return psycopg2.connect(
            host=settings.DATABASES['default']['HOST'],
            port=settings.DATABASES['default']['PORT'],
            user=settings.DATABASES['default']['USER'],
            password=settings.DATABASES['default']['PASSWORD'],
            database=self.customer_db
        )

    def _ensure_schema_exists(self):
        """Ensure CANVAS_CACHE schema and metadata table exist."""
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            conn.autocommit = True
            cursor = conn.cursor()

            # Create schema
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.CACHE_SCHEMA}";')

            # Create metadata table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS "{self.CACHE_SCHEMA}"."{self.METADATA_TABLE}" (
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

            # Create index for faster lookups
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_node_cache_lookup
                ON "{self.CACHE_SCHEMA}"."{self.METADATA_TABLE}" (canvas_id, node_id);
            ''')

            logger.info(f"Ensured CANVAS_CACHE schema exists in {self.customer_db}")

        except Exception as e:
            logger.error(f"Error creating cache schema: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def _get_cache_table_name(self, canvas_id: int, node_id: str, node_name: Optional[str] = None) -> str:
        """
        Generate cache table name for a node.

        If node_name is provided, uses format: cv{canvas_id}_{sanitized_node_name}
        Otherwise uses format: cv{canvas_id}_{sanitized_node_id}

        Examples:
        - cv1_active_customers (if node_name="Active Customers")
        - cv1_filter_abc123 (if no node_name, using node_id)
        """
        import re

        if node_name:
            # Sanitize node_name: lowercase, replace spaces/special chars with underscore
            sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', node_name.lower())
            sanitized = re.sub(r'_+', '_', sanitized)  # Remove multiple underscores
            sanitized = sanitized.strip('_')
            if len(sanitized) > 50:
                sanitized_chars: list[str] = []
                for i in range(50):
                    sanitized_chars.append(sanitized[i])
                sanitized = "".join(sanitized_chars)
        else:
            # Sanitize node_id
            sanitized = node_id.replace('-', '_').replace('.', '_')
            if len(sanitized) > 50:
                sanitized_chars = []
                for i in range(50):
                    sanitized_chars.append(sanitized[i])
                sanitized = "".join(sanitized_chars)

        return f"cv{canvas_id}_{sanitized}"

    def _compute_config_hash(self, config: dict[str, Any]) -> str:
        """Compute hash of node configuration for cache invalidation."""
        config_str = json.dumps(config, sort_keys=True)
        hash_val = hashlib.sha256(config_str.encode()).hexdigest()
        hash_chars: list[str] = []
        for i in range(min(16, len(hash_val))):
            hash_chars.append(hash_val[i])
        return "".join(hash_chars)

    def get_cache(self, canvas_id: int, node_id: str, config: Optional[dict[str, Any]] = None) -> Optional[dict[str, Any]]:
        """
        Get cached data for a node if it exists and is valid.

        Args:
            canvas_id: Canvas ID
            node_id: Node ID
            config: Node configuration (optional, for hash validation)

        Returns:
            dict with 'rows', 'columns', 'metadata' if cache exists and config matches
            None if no valid cache or config changed
        """
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            conn.autocommit = True
            cursor = conn.cursor()

            # Get cache metadata
            cursor.execute('''
                SELECT table_name, row_count, column_count, columns, is_valid, created_on, config_hash
                FROM "{self.CACHE_SCHEMA}"."{self.METADATA_TABLE}"
                WHERE canvas_id = %s AND node_id = %s AND is_valid = TRUE
            ''', (canvas_id, node_id))

            row = cursor.fetchone()
            if not row:
                return None

            table_name, row_count, column_count, columns, is_valid, created_on, cached_hash = row

            # Validate config hash if config provided
            if config:
                current_hash = self._compute_config_hash(config)
                if cached_hash != current_hash:
                    logger.info(f"Cache invalid for node {node_id}: config changed (cached={cached_hash}, current={current_hash})")
                    # Invalidate cache - config has changed
                    self.invalidate_cache(canvas_id, node_id)
                    return None
                else:
                    logger.debug(f"Cache valid for node {node_id}: config hash matches ({current_hash})")

            # Update last_accessed
            cursor.execute('''
                UPDATE "{self.CACHE_SCHEMA}"."{self.METADATA_TABLE}"
                SET last_accessed = CURRENT_TIMESTAMP
                WHERE canvas_id = %s AND node_id = %s
            ''', (canvas_id, node_id))

            # Check if cache table exists
            cursor.execute('''
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                )
            ''', (self.CACHE_SCHEMA, table_name))

            if not cursor.fetchone()[0]:
                # Table doesn't exist, invalidate metadata
                self.invalidate_cache(canvas_id, node_id)
                return None

            # Fetch data from cache table (with pagination support)
            cursor.execute('''
                SELECT * FROM "{self.CACHE_SCHEMA}"."{table_name}"
                LIMIT 1000
            ''')

            # Get column names from cursor description
            col_names = [desc[0] for desc in cursor.description]
            rows = []
            for data_row in cursor.fetchall():
                rows.append(dict(zip(col_names, data_row)))

            return {
                'rows': rows,
                'columns': columns if columns else [{'name': c, 'type': 'text'} for c in col_names],
                'metadata': {
                    'row_count': row_count,
                    'column_count': column_count,
                    'cached_on': created_on.isoformat() if created_on else None,
                    'from_cache': True
                }
            }

        except Exception as e:
            logger.error(f"Error reading cache for node {node_id}: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def save_cache(
        self,
        canvas_id: int,
        node_id: str,
        node_type: str,
        rows: list,
        columns: list[dict[str, Any]],
        config: Optional[dict[str, Any]] = None,
        source_node_ids: Optional[list[str]] = None,
        node_name: Optional[str] = None
    ) -> bool:
        """
        Save node transformation result to cache.

        Args:
            canvas_id: Canvas ID
            node_id: Node ID (unique within canvas)
            node_type: Type of node (filter, projection, etc.)
            rows: List of row dictionaries
            columns: List of column metadata dicts
            config: Node configuration for hash computation
            source_node_ids: IDs of upstream nodes this depends on
            node_name: User-defined name for the node (used in table naming)

        Returns:
            True if cache saved successfully
        """
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            conn.autocommit = True
            cursor = conn.cursor()

            table_name = self._get_cache_table_name(canvas_id, node_id, node_name)
            config_hash = self._compute_config_hash(config) if config else None

            # Drop existing cache table if any
            cursor.execute(f'DROP TABLE IF EXISTS "{self.CACHE_SCHEMA}"."{table_name}";')

            if not rows:
                # No data to cache, just update metadata
                cursor.execute('''
                    INSERT INTO "{self.CACHE_SCHEMA}"."{self.METADATA_TABLE}"
                    (canvas_id, node_id, node_name, node_type, table_name, config_hash, row_count, column_count, columns, source_node_ids, is_valid)
                    VALUES (%s, %s, %s, %s, %s, %s, 0, %s, %s, %s, TRUE)
                    ON CONFLICT (canvas_id, node_id) DO UPDATE SET
                        node_name = EXCLUDED.node_name,
                        node_type = EXCLUDED.node_type,
                        table_name = EXCLUDED.table_name,
                        config_hash = EXCLUDED.config_hash,
                        row_count = 0,
                        column_count = EXCLUDED.column_count,
                        columns = EXCLUDED.columns,
                        source_node_ids = EXCLUDED.source_node_ids,
                        created_on = CURRENT_TIMESTAMP,
                        last_accessed = CURRENT_TIMESTAMP,
                        is_valid = TRUE
                ''', (
                    canvas_id, node_id, node_name, node_type, table_name, config_hash,
                    len(columns), json.dumps(columns), json.dumps(source_node_ids)
                ))
                return True

            # Build column definitions from first row + column metadata
            col_names = list(rows[0].keys())
            col_defs = []
            for col in col_names:
                # Find column type from metadata
                col_type = 'TEXT'  # Default
                for col_meta in columns:
                    if col_meta.get('name') == col:
                        pg_type = col_meta.get('type', 'text').lower()
                        if 'int' in pg_type:
                            col_type = 'BIGINT'
                        elif 'float' in pg_type or 'double' in pg_type or 'numeric' in pg_type:
                            col_type = 'NUMERIC'
                        elif 'bool' in pg_type:
                            col_type = 'BOOLEAN'
                        elif 'date' in pg_type:
                            col_type = 'DATE'
                        elif 'timestamp' in pg_type:
                            col_type = 'TIMESTAMP'
                        else:
                            col_type = 'TEXT'
                        break
                col_defs.append(f'"{col}" {col_type}')

            # Create cache table
            create_sql = '''
                CREATE TABLE "{self.CACHE_SCHEMA}"."{table_name}" (
                    _cache_row_id SERIAL PRIMARY KEY,
                    {", ".join(col_defs)}
                );
            '''
            cursor.execute(create_sql)

            # Insert rows in batches
            if rows:
                ", ".join(["%s"] * len(col_names))
                insert_sql = '''
                    INSERT INTO "{self.CACHE_SCHEMA}"."{table_name}" ({", ".join([f'"{c}"' for c in col_names])})
                    VALUES ({placeholders})
                '''

                batch_size = 1000
                for i in range(0, len(rows), batch_size):
                    batch: list[dict[str, Any]] = []
                    for j in range(i, min(i + batch_size, len(rows))):
                        batch.append(rows[j])
                    values = [tuple(row.get(col) for col in col_names) for row in batch]
                    cursor.executemany(insert_sql, values)

            # Update metadata
            cursor.execute('''
                INSERT INTO "{self.CACHE_SCHEMA}"."{self.METADATA_TABLE}"
                (canvas_id, node_id, node_name, node_type, table_name, config_hash, row_count, column_count, columns, source_node_ids, is_valid)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                ON CONFLICT (canvas_id, node_id) DO UPDATE SET
                    node_name = EXCLUDED.node_name,
                    node_type = EXCLUDED.node_type,
                    table_name = EXCLUDED.table_name,
                    config_hash = EXCLUDED.config_hash,
                    row_count = EXCLUDED.row_count,
                    column_count = EXCLUDED.column_count,
                    columns = EXCLUDED.columns,
                    source_node_ids = EXCLUDED.source_node_ids,
                    created_on = CURRENT_TIMESTAMP,
                    last_accessed = CURRENT_TIMESTAMP,
                    is_valid = TRUE
            ''', (
                canvas_id, node_id, node_name, node_type, table_name, config_hash,
                len(rows), len(columns), json.dumps(columns), json.dumps(source_node_ids)
            ))

            logger.info(f"Cached {len(rows)} rows for node {node_id} in canvas {canvas_id}")
            return True

        except Exception as e:
            logger.error(f"Error saving cache for node {node_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        return False

    def invalidate_cache(self, canvas_id: int, node_id: Optional[str] = None):
        """
        Invalidate cache for a node or all nodes in a canvas.

        Args:
            canvas_id: Canvas ID
            node_id: Specific node ID, or None to invalidate all nodes in canvas
        """
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            conn.autocommit = True
            cursor = conn.cursor()

            if node_id:
                # Invalidate specific node
                cursor.execute('''
                    UPDATE "{self.CACHE_SCHEMA}"."{self.METADATA_TABLE}"
                    SET is_valid = FALSE
                    WHERE canvas_id = %s AND node_id = %s
                ''', (canvas_id, node_id))

                # Also invalidate downstream nodes that depend on this one
                cursor.execute('''
                    UPDATE "{self.CACHE_SCHEMA}"."{self.METADATA_TABLE}"
                    SET is_valid = FALSE
                    WHERE canvas_id = %s AND source_node_ids::jsonb ? %s
                ''', (canvas_id, node_id))
            else:
                # Invalidate all nodes in canvas
                cursor.execute('''
                    UPDATE "{self.CACHE_SCHEMA}"."{self.METADATA_TABLE}"
                    SET is_valid = FALSE
                    WHERE canvas_id = %s
                ''', (canvas_id,))

            logger.info(f"Invalidated cache for canvas {canvas_id}" + (f", node {node_id}" if node_id else ""))

        except Exception as e:
            logger.error(f"Error invalidating cache: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def cleanup_old_caches(self, days_old: int = 7):
        """Remove cache tables not accessed in the specified number of days."""
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            conn.autocommit = True
            cursor = conn.cursor()

            # Get old cache tables
            cursor.execute('''
                SELECT table_name FROM "{self.CACHE_SCHEMA}"."{self.METADATA_TABLE}"
                WHERE last_accessed < NOW() - INTERVAL '%s days'
            ''', (days_old,))

            old_tables = [row[0] for row in cursor.fetchall()]

            for table_name in old_tables:
                cursor.execute(f'DROP TABLE IF EXISTS "{self.CACHE_SCHEMA}"."{table_name}";')

            # Delete metadata
            cursor.execute('''
                DELETE FROM "{self.CACHE_SCHEMA}"."{self.METADATA_TABLE}"
                WHERE last_accessed < NOW() - INTERVAL '%s days'
            ''', (days_old,))

            logger.info(f"Cleaned up {len(old_tables)} old cache tables")

        except Exception as e:
            logger.error(f"Error cleaning up caches: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def get_cache_stats(self, canvas_id: Optional[int] = None) -> dict[str, Any]:
        """Get cache statistics for debugging/monitoring."""
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            conn.autocommit = True
            cursor = conn.cursor()

            if canvas_id:
                cursor.execute('''
                    SELECT node_id, node_type, row_count, column_count, is_valid, created_on, last_accessed
                    FROM "{self.CACHE_SCHEMA}"."{self.METADATA_TABLE}"
                    WHERE canvas_id = %s
                    ORDER BY created_on DESC
                ''', (canvas_id,))
            else:
                cursor.execute('''
                    SELECT canvas_id, node_id, node_type, row_count, is_valid, created_on
                    FROM "{self.CACHE_SCHEMA}"."{self.METADATA_TABLE}"
                    ORDER BY created_on DESC
                    LIMIT 100
                ''')

            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]

            return {
                'caches': [dict(zip(col_names, row)) for row in rows],
                'total_count': len(rows)
            }

        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {'caches': [], 'total_count': 0, 'error': str(e)}
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        return {'caches': [], 'total_count': 0, 'error': 'Unknown error'}

def get_node_cache_manager(customer) -> NodeCacheManager:
    """
    Factory function to get NodeCacheManager for a customer.

    Args:
        customer: Customer object or customer database name string

    Returns:
        NodeCacheManager instance
    """
    if hasattr(customer, 'cust_db'):
        db_name = customer.cust_db
    else:
        db_name = str(customer)

    return NodeCacheManager(db_name)
