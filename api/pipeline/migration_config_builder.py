# Moved from: api/utils/migration_config_builder.py
"""
Build migration service config with source_configs (and optionally destination_configs)
from canvas nodes and customer DB. Used when calling the migration service execute endpoint.
"""

import copy
import logging

from django.conf import settings
import psycopg2

from api.utils.helpers import decrypt_source_data

logger = logging.getLogger(__name__)

def _fetch_source_row(cursor, config_column, source_id):
    """Fetch one row from GENERAL.source by id. Returns (source_config_encrypted, created_on) or None."""
    cursor.execute(
        """
        SELECT {config_column}, created_on
        FROM "GENERAL".source
        WHERE id = %s
        """,
        (source_id,),
    )
    return cursor.fetchone()

def build_migration_config(canvas, customer, base_config=None):
    """
    Build config dict for the migration service with source_configs populated.

    For each source node in the canvas, fetches the source connection from GENERAL.source
    (customer DB, or default DB as fallback), decrypts it, and maps to extraction-service format:
    { "host", "port", "database", "username", "password", "schema" }.

    :param canvas: Canvas instance (with get_nodes(), get_edges())
    :param customer: Customer instance (cust_id, cust_db)
    :param base_config: Optional existing config dict to merge into
    :return: Config dict with "source_configs" = { node_id: { "connection_config": {...} } }
    """
    config = copy.deepcopy(base_config) if base_config else {}
    if "source_configs" not in config:
        config["source_configs"] = {}
    if "destination_configs" not in config:
        config["destination_configs"] = {}

    nodes = canvas.get_nodes()
    if not nodes:
        logger.info("build_migration_config: no nodes on canvas")
        return config

    db_settings = settings.DATABASES["default"]
    databases_to_try = []
    if getattr(customer, "cust_db", None):
        databases_to_try.append(customer.cust_db)
    default_name = db_settings.get("NAME")
    if default_name and default_name not in databases_to_try:
        databases_to_try.append(default_name)

    if not databases_to_try:
        logger.warning("build_migration_config: no database to use (customer.cust_db and default NAME missing)")
        return config

    conn = None
    cursor = None
    config_column = None

    for database in databases_to_try:
        try:
            conn = psycopg2.connect(
                host=db_settings.get("HOST"),
                port=db_settings.get("PORT", 5432),
                user=db_settings.get("USER"),
                password=db_settings.get("PASSWORD"),
                database=database,
            )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'GENERAL' AND table_name = 'source'
            """)
            columns = [row[0] for row in cursor.fetchall()]
            config_column = "source_config" if "source_config" in columns else "src_config"
            break
        except Exception as e:
            logger.debug("build_migration_config: could not use DB %s: %s", database, e)
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            conn = None
            cursor = None

    if not conn or not cursor or config_column is None:
        logger.warning("build_migration_config: could not connect to any DB for GENERAL.source")
        return config

    try:
        for node in nodes:
            node_type = (node.get("type") or (node.get("data") or {}).get("type") or "").strip().lower()
            if node_type != "source":
                continue

            node_id = node.get("id")
            if not node_id:
                continue

            config_data = (node.get("data") or {}).get("config") or {}
            source_id = config_data.get("sourceId")
            if source_id is None:
                source_id = config_data.get("source_id")
            if source_id is not None and not isinstance(source_id, int):
                try:
                    source_id = int(source_id)
                except (TypeError, ValueError):
                    pass
            if not source_id:
                logger.warning("Source node %s has no sourceId; skipping source_configs", node_id)
                continue

            row = _fetch_source_row(cursor, config_column, source_id)
            if not row:
                logger.warning("Source connection id=%s not found for node %s", source_id, node_id)
                continue

            source_config_encrypted, created_on = row
            decrypted = decrypt_source_data(source_config_encrypted, customer.cust_id, created_on)
            if not decrypted:
                logger.warning("Failed to decrypt source id=%s for node %s", source_id, node_id)
                continue

            # Map to extraction service connection_config format
            connection_config = {
                "host": (decrypted.get("hostname") or decrypted.get("host") or ""),
                "port": decrypted.get("port"),
                "database": (decrypted.get("database") or ""),
                "username": (decrypted.get("user") or decrypted.get("username") or ""),
                "password": (decrypted.get("password") or ""),
            }
            if decrypted.get("schema") is not None:
                connection_config["schema"] = decrypted.get("schema")

            config["source_configs"][node_id] = {"connection_config": connection_config}
            logger.info("Added source_config for node %s (source id=%s)", node_id, source_id)

        # Destination nodes: fetch from GENERAL.destination, decrypt, add to destination_configs
        try:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'GENERAL' AND table_name = 'destination'
            """)
            dest_columns = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.debug("GENERAL.destination not available: %s", e)
            dest_columns = []
        dest_config_column = None
        if dest_columns:
            for c in ["destination_config", "config_data", "config", "dest_config", "dst_config"]:
                if c in dest_columns:
                    dest_config_column = c
                    break

        # Customer DB connection (used for customer_database destinations and execution plan storage)
        customer_db_connection = None
        if getattr(customer, "cust_db", None):
            from api.utils.db_connection import get_customer_db_config
            customer_db_connection = get_customer_db_config(customer.cust_db)
        elif default_name:
            from api.utils.db_connection import get_customer_db_config
            customer_db_connection = get_customer_db_config(default_name)

        if dest_config_column or customer_db_connection:
            for node in nodes:
                node_type = (node.get("type") or (node.get("data") or {}).get("type") or "").strip().lower()
                if node_type not in ("destination", "destination-postgresql", "destination-postgres", "destination-hana"):
                    continue
                node_id = node.get("id")
                if not node_id:
                    continue
                config_data = (node.get("data") or {}).get("config") or {}
                dest_type = (config_data.get("destinationType") or "").strip().lower()

                # Customer Database: write to same DB as customer (e.g. C00008)
                if dest_type == "customer_database" and customer_db_connection:
                    conn_cfg = dict(customer_db_connection)
                    schema = config_data.get("schema") or "public"
                    if schema:
                        conn_cfg["schema"] = schema
                    config["destination_configs"][node_id] = {
                        "connection_config": conn_cfg,
                        "db_type": "postgresql",
                    }
                    logger.info("Added destination_config for node %s (customer_database, schema=%s)", node_id, schema)
                    continue

                # Remote destination: fetch from GENERAL.destination
                dest_id = config_data.get("destinationId") or config_data.get("destination_id")
                if dest_id is not None and not isinstance(dest_id, int):
                    try:
                        dest_id = int(dest_id)
                    except (TypeError, ValueError):
                        pass
                if not dest_id:
                    logger.warning("Destination node %s has no destinationId; skipping destination_configs", node_id)
                    continue
                if not dest_config_column:
                    continue
                cursor.execute(
                    """
                    SELECT {dest_config_column}, created_on
                    FROM "GENERAL".destination
                    WHERE id = %s
                    """,
                    (dest_id,),
                )
                row = cursor.fetchone()
                if not row:
                    logger.warning("Destination id=%s not found for node %s", dest_id, node_id)
                    continue
                dest_config_encrypted, created_on = row
                decrypted = decrypt_source_data(dest_config_encrypted, customer.cust_id, created_on)
                if not decrypted:
                    logger.warning("Failed to decrypt destination id=%s for node %s", dest_id, node_id)
                    continue
                # Loader format: host/hostname, port, user/username, password, database
                connection_config = {
                    "host": (decrypted.get("hostname") or decrypted.get("host") or ""),
                    "port": decrypted.get("port") or decrypted.get("instance_number"),
                    "username": (decrypted.get("user") or decrypted.get("username") or ""),
                    "password": (decrypted.get("password") or ""),
                    "database": (decrypted.get("database") or decrypted.get("tenant_db_name") or ""),
                }
                if decrypted.get("destination_schema_name") is not None:
                    connection_config["schema"] = decrypted.get("destination_schema_name")
                db_type = (decrypted.get("db_type") or decrypted.get("database_type") or "hana").lower().strip()
                config["destination_configs"][node_id] = {
                    "connection_config": connection_config,
                    "db_type": db_type,
                }
                logger.info("Added destination_config for node %s (destination id=%s)", node_id, dest_id)

        # Customer DB connection so migration service can load/save execution plan (CANVAS_CACHE.execution_plans)
        if customer_db_connection:
            config["connection_config"] = customer_db_connection
        # Log summary so operators can verify extraction will be triggered (orchestrator needs source_configs)
        num_sources = len(config.get("source_configs") or {})
        num_dests = len(config.get("destination_configs") or {})
        logger.info(
            "build_migration_config: done. source_configs=%s, destination_configs=%s (extraction triggered only when source_configs present)",
            num_sources,
            num_dests,
        )
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    return config
