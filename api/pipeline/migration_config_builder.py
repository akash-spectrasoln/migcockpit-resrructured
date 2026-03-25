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
    # config_column is discovered from information_schema; quote it as an identifier.
    config_column_sql = f'"{config_column}"'
    cursor.execute(
        f"""
        SELECT {config_column_sql}, created_on
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

    detected_source_nodes = [
        n for n in nodes
        if (n.get("type") or (n.get("data") or {}).get("type") or "").strip().lower().startswith("source")
    ]
    logger.info(
        "build_migration_config: detected source nodes=%s ids(sample)=%s",
        len(detected_source_nodes),
        [n.get("id") for n in detected_source_nodes[:8] if n.get("id")],
    )

    # GENERAL.source / GENERAL.destination live on the DEFAULT Django DB
    # (shared metadata store), not on the customer tenant DB.
    #
    # Your logs show source configs were not found because the code tried the
    # customer DB first. Force DEFAULT first to make source_configs reliable.
    db_settings = settings.DATABASES["default"]
    default_name = db_settings.get("NAME")
    if not default_name:
        logger.warning("build_migration_config: default DB NAME missing in settings.DATABASES['default']")
        return config
    databases_to_try = [default_name]
    logger.info(
        "build_migration_config: forcing DEFAULT DB only for GENERAL store: %s (not checking customer.cust_db)",
        default_name,
    )

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
            logger.info("build_migration_config: connected to GENERAL store DB=%s", database)
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
        sources_seen_with_ids = 0
        sources_added = 0
        sources_missing_source_id = 0
        sources_not_found_in_general = 0
        sources_decrypt_failed = 0

        for node in nodes:
            node_type = (node.get("type") or (node.get("data") or {}).get("type") or "").strip().lower()
            # Support variant node types like 'source-postgresql', 'source-mysql', 'sourcepostgresql', etc.
            if not node_type or not node_type.startswith("source"):
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
                sources_missing_source_id += 1
                logger.warning(
                    "build_migration_config: Source node %s missing sourceId (config_data_keys=%s config_data=%s)",
                    node_id,
                    list(config_data.keys())[:10],
                    {k: ("***" if "pass" in str(k).lower() else v) for k, v in list(config_data.items())[:10]},
                )
                logger.warning("Source node %s has no sourceId; skipping source_configs", node_id)
                continue
            sources_seen_with_ids += 1

            row = _fetch_source_row(cursor, config_column, source_id)
            if not row:
                sources_not_found_in_general += 1
                logger.warning("Source connection id=%s not found for node %s", source_id, node_id)
                continue

            source_config_encrypted, created_on = row
            decrypted = decrypt_source_data(source_config_encrypted, customer.cust_id, created_on)
            if not decrypted:
                sources_decrypt_failed += 1
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
            sources_added += 1
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
                    f"""
                    SELECT "{dest_config_column}", created_on
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
        if num_sources == 0:
            logger.warning(
                "build_migration_config: no source_configs were created. detected_source_nodes=%s sources_seen_with_ids=%s missing_source_id=%s not_found_in_general=%s decrypt_failed=%s",
                len(detected_source_nodes),
                sources_seen_with_ids,
                sources_missing_source_id,
                sources_not_found_in_general,
                sources_decrypt_failed,
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
