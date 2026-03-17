# Moved from: api/services/checkpoint_cache.py
"""
Checkpoint-Based Preview Cache Manager.
Enforces SQL-only preview with physical table materialization at complexity checkpoints.
"""
from datetime import datetime, timedelta
import hashlib
import json
import logging
from typing import Any, Optional

from django.conf import settings
import psycopg2

logger = logging.getLogger(__name__)

# Constants based on user requirements
MAX_CACHE_ROWS = 100
CACHE_TTL_MINUTES = 20
CHECKPOINT_NODE_TYPES = {'join', 'aggregate', 'compute', 'window', 'sort', 'source'}

class CheckpointCacheManager:
    """
    Manages physical SQL table checkpoints for preview mode.
    Schema: staging_preview_<canvas_id>
    Tables: node_<node_id>_cache
    """

    def __init__(self, customer_db: str, canvas_id: str):
        self.customer_db = customer_db
        self.canvas_id = str(canvas_id)
        self.schema_name = f"staging_preview_{self.canvas_id}"
        self._ensure_schema_exists()

    def _get_connection(self):
        return psycopg2.connect(
            host=settings.DATABASES['default']['HOST'],
            port=settings.DATABASES['default']['PORT'],
            user=settings.DATABASES['default']['USER'],
            password=settings.DATABASES['default']['PASSWORD'],
            database=self.customer_db
        )

    def _ensure_schema_exists(self):
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema_name}"')

            # Metadata table to track hashes and TTLs
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS "{self.schema_name}"."_checkpoint_metadata" (
                    node_id VARCHAR(255) PRIMARY KEY,
                    node_version_hash VARCHAR(64) NOT NULL,
                    upstream_version_hash VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP NOT NULL,
                    column_metadata JSONB
                )
            ''')
        except Exception as e:
            logger.error(f"Error creating checkpoint schema: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def _compute_node_hash(self, node_id: str, node_config: dict[str, Any]) -> str:
        config_str = json.dumps(node_config, sort_keys=True)
        return hashlib.sha256(f"{node_id}:{config_str}".encode()).hexdigest()

    def is_checkpoint_node(self, node_type: str) -> bool:
        return node_type.lower() in CHECKPOINT_NODE_TYPES

    def get_valid_checkpoint(self, node_id: str, node_version_hash: str, upstream_version_hash: str) -> Optional[dict[str, Any]]:
        """Check if a valid, non-expired cache table exists for this node."""
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(f'''
                SELECT node_version_hash, upstream_version_hash, column_metadata
                FROM "{self.schema_name}"."_checkpoint_metadata"
                WHERE node_id = %s AND expires_at > CURRENT_TIMESTAMP
            ''', (node_id,))

            row = cursor.fetchone()
            if row:
                stored_node_hash, stored_upstream_hash, col_meta = row
                if stored_node_hash == node_version_hash and stored_upstream_hash == (upstream_version_hash or ""):
                    # Verify table exists
                    table_name = f"node_{node_id.replace('-', '_')}_cache"
                    cursor.execute('''
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_schema = %s AND table_name = %s
                        )
                    ''', (self.schema_name, table_name))
                    if cursor.fetchone()[0]:
                        return {
                            'table_ref': f'"{self.schema_name}"."{table_name}"',
                            'columns': col_meta if isinstance(col_meta, list) else json.loads(col_meta)
                        }
            return None
        except Exception as e:
            logger.error(f"Error checking checkpoint: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def save_checkpoint(
        self,
        node_id: str,
        node_type: str,
        node_config: dict,
        upstream_version_hash: str,
        columns: list[dict],
        sql_query: Optional[str] = None,
        sql_params: Optional[list] = None,
        rows: Optional[list[dict]] = None,
    ):
        """
        Materialize checkpoint. Can be done via SQL (ctas) or from memory (for compute nodes).
        Enforces MAX_CACHE_ROWS limit.
        """
        node_version_hash = self._compute_node_hash(node_id, node_config)
        table_name = f"node_{node_id.replace('-', '_')}_cache"
        full_table_name = f'"{self.schema_name}"."{table_name}"'
        expires_at = datetime.utcnow() + timedelta(minutes=CACHE_TTL_MINUTES)

        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            conn.autocommit = True
            cursor = conn.cursor()

            cursor.execute(f'DROP TABLE IF EXISTS {full_table_name}')

            logger.info(f"[CHECKPOINT SAVE] Node: {node_id}, Type: {node_type}")
            logger.info(f"[CHECKPOINT SAVE] Has sql_query: {sql_query is not None}, Has rows: {rows is not None}")

            # Always prefer rows over SQL: the compiled SQL references source-DB tables (e.g. tool_connection),
            # but the checkpoint runs in the customer DB. Using SQL would fail with "relation does not exist".
            if rows is not None:
                # Materialize from memory (works for source, join, aggregate, etc.)
                col_names = list(rows[0].keys()) if rows else [c.get('name', f'col_{i}') for i, c in enumerate(columns or [])]
                if not col_names:
                    logger.warning("[CHECKPOINT SAVE] No columns available, skipping checkpoint")
                    return False

                logger.info(f"[CHECKPOINT SAVE] Using ROWS method, row count: {len(rows)}")

                rows_limited = rows[:MAX_CACHE_ROWS] if rows else []
                col_defs = [f'"{c}" TEXT' for c in col_names]
                cursor.execute(f'CREATE TABLE {full_table_name} ({", ".join(col_defs)})')

                if rows_limited:
                    placeholders = ", ".join(["%s"] * len(col_names))
                    cols_str = ", ".join([f'"{c}"' for c in col_names])
                    insert_sql = f'INSERT INTO {full_table_name} ({cols_str}) VALUES ({placeholders})'
                    cursor.executemany(insert_sql, [tuple(r.get(c) for c in col_names) for r in rows_limited])
                logger.info(f"[CHECKPOINT SAVE] Successfully created cache table ({len(rows_limited)} rows)")
            elif sql_query:
                # Materialize via SQL (for join, aggregate, etc.)
                # Use subquery to enforce limit
                logger.info("[CHECKPOINT SAVE] Using SQL method")
                ctas_sql = f'CREATE TABLE {full_table_name} AS SELECT * FROM ({sql_query}) _sub LIMIT {MAX_CACHE_ROWS}'
                cursor.execute(ctas_sql, sql_params or [])
                logger.info("[CHECKPOINT SAVE] Successfully created cache table via SQL")

            # Update metadata
            cursor.execute(f'''
                INSERT INTO "{self.schema_name}"."_checkpoint_metadata"
                (node_id, node_version_hash, upstream_version_hash, expires_at, column_metadata)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (node_id) DO UPDATE SET
                    node_version_hash = EXCLUDED.node_version_hash,
                    upstream_version_hash = EXCLUDED.upstream_version_hash,
                    expires_at = EXCLUDED.expires_at,
                    column_metadata = EXCLUDED.column_metadata
            ''', (node_id, node_version_hash, upstream_version_hash or "", expires_at, json.dumps(columns)))

            return True
        except Exception as e:
            logger.error(f"Error saving checkpoint for {node_id}: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def invalidate_downstream(self, node_id: str, nodes: list[dict], edges: list[dict]):
        """Delete this checkpoint and all downstream ones."""
        from api.utils.graph_utils import find_downstream_nodes
        downstream_ids = find_downstream_nodes(node_id, nodes, edges)
        targets = [node_id, *downstream_ids]

        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            conn.autocommit = True
            cursor = conn.cursor()
            for tid in targets:
                table_name = f"node_{tid.replace('-', '_')}_cache"
                cursor.execute(f'DROP TABLE IF EXISTS "{self.schema_name}"."{table_name}"')
                cursor.execute(f'DELETE FROM "{self.schema_name}"."_checkpoint_metadata" WHERE node_id = %s', (tid,))
        except Exception as e:
            logger.error(f"Error invalidating downstream: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def cleanup_schema(self):
        """Drop the entire preview schema."""
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(f'DROP SCHEMA IF EXISTS "{self.schema_name}" CASCADE')
        except Exception as e:
            logger.error(f"Error dropping schema: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def find_nearest_checkpoint(self, target_node_id: str, nodes: list[dict], edges: list[dict]) -> tuple[Optional[str], Optional[dict]]:
        """Traverse backwards to find the closest valid checkpoint."""
        from collections import deque
        node_map = {n['id']: n for n in nodes}
        reverse_adj = {}
        for e in edges:
            reverse_adj.setdefault(e['target'], []).append(e['source'])

        queue = deque([target_node_id])
        visited = set()

        while queue:
            curr_id = queue.popleft()
            if curr_id in visited:
                continue
            visited.add(curr_id)

            node = node_map.get(curr_id)
            if node:
                node_config = node.get('data', {}).get('config', {})
                node_hash = self._compute_node_hash(curr_id, node_config)
                # For upstream hash, we need to compute it based on current graph state
                upstream_ids = reverse_adj.get(curr_id, [])
                u_hashes = {}
                for uid in upstream_ids:
                    unode = node_map.get(uid)
                    if unode:
                        u_hashes[uid] = self._compute_node_hash(uid, unode.get('data', {}).get('config', {}))

                upstream_hash = ""
                if upstream_ids:
                    u_str = ":".join(sorted([f"{nid}:{u_hashes.get(nid, '')}" for nid in upstream_ids]))
                    upstream_hash = hashlib.sha256(u_str.encode()).hexdigest()

                checkpoint = self.get_valid_checkpoint(curr_id, node_hash, upstream_hash)
                if checkpoint:
                    return curr_id, checkpoint

            for upstream in reverse_adj.get(curr_id, []):
                queue.append(upstream)

        return None, None