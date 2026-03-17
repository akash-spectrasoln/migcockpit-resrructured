# Moved from: api/utils/metadata_sync.py
"""
Sync node_cache_metadata when canvas or nodes are saved.

Call this after any persistence that changes the canvas configuration
(save-configuration, node insert, node delete, node add) so metadata
(including calculated columns) stays up to date without requiring Validate.
"""
import logging

logger = logging.getLogger(__name__)

def delete_node_metadata_from_cache(canvas_id, node_id, customer):
    """
    Delete metadata for a node from CANVAS_CACHE.node_cache_metadata.

    Call when a node is deleted so stale metadata is removed.

    :param canvas_id: Canvas ID
    :param node_id: Node ID to delete metadata for
    :param customer: Customer instance with cust_db
    """
    if not customer or not getattr(customer, 'cust_db', None):
        logger.debug("[METADATA_SYNC] Skipping delete: no customer or cust_db")
        return
    try:
        from api.utils.db_connection import get_customer_db_config, get_customer_db_connection
        connection_config = get_customer_db_config(customer.cust_db)
        conn = get_customer_db_connection(connection_config)
        try:
            cursor = conn.cursor()
            cursor.execute(
                'DELETE FROM "CANVAS_CACHE".node_cache_metadata WHERE canvas_id = %s AND node_id = %s',
                (int(canvas_id), str(node_id)),
            )
            deleted = cursor.rowcount
            conn.commit()
            cursor.close()
            if deleted:
                logger.info(f"[METADATA_SYNC] Deleted metadata for node {node_id[:8]}... (canvas_id={canvas_id})")
        finally:
            if not conn.closed:
                conn.close()
    except ImportError as e:
        logger.debug(f"[METADATA_SYNC] Skipped delete (db_connection not available): {e}")
    except Exception as e:
        logger.warning(f"[METADATA_SYNC] Failed to delete node metadata: {e}", exc_info=True)

def update_node_metadata_for_canvas(canvas):
    """
    Regenerate and save metadata for all nodes in the canvas to node_cache_metadata.

    Uses the canvas's current configuration (nodes, edges). Safe to call after
    canvas.save(); does not modify the canvas. On failure logs and returns
    without raising so the calling save flow is not broken.

    :param canvas: Django Canvas instance with configuration and customer set.
    :return: dict with keys: success (bool), count (int), message (str) for caller to surface if needed
    """
    configuration = getattr(canvas, 'configuration', None) or {}
    nodes_list = configuration.get('nodes', [])
    edges_list = configuration.get('edges', []) or []

    if not nodes_list:
        logger.info("[METADATA_SYNC] Skipping: no nodes in configuration")
        return {"success": False, "count": 0, "message": "No nodes in configuration"}

    # Allow metadata sync even with no edges (e.g. source-only canvas) — source nodes can still get metadata
    if not edges_list:
        logger.info(f"[METADATA_SYNC] No edges yet (canvas_id={canvas.id}); will generate metadata for source nodes only")

    try:
        from api.utils.db_connection import get_customer_db_config
        customer = getattr(canvas, 'customer', None)
        if not customer or not getattr(customer, 'cust_db', None):
            logger.warning(
                "[METADATA_SYNC] Skipping: canvas has no customer or customer has no cust_db. "
                "Metadata will not be persisted to node_cache_metadata."
            )
            return {"success": False, "count": 0, "message": "Canvas customer or cust_db missing"}

        connection_config = get_customer_db_config(customer.cust_db)
        from services.migration_service.planner.metadata_generator import generate_all_node_metadata
        nodes_dict = {n.get('id') or n.get('data', {}).get('id'): n for n in nodes_list if n.get('id') or n.get('data', {}).get('id')}
        if not nodes_dict:
            logger.warning("[METADATA_SYNC] Skipping: no valid node ids in configuration")
            return {"success": False, "count": 0, "message": "No valid node ids"}

        count = generate_all_node_metadata(
            nodes=nodes_dict,
            edges=edges_list,
            canvas_id=canvas.id,
            connection_config=connection_config,
            config=configuration,
        )
        logger.info(f"[METADATA_SYNC] Updated node_cache_metadata for {count}/{len(nodes_dict)} nodes (canvas_id={canvas.id})")
        return {"success": True, "count": count, "message": f"Updated metadata for {count} nodes"}
    except ImportError as e:
        logger.warning(f"[METADATA_SYNC] Skipped (migration_service or db_connection not available): {e}")
        return {"success": False, "count": 0, "message": str(e)}
    except Exception as e:
        logger.warning(f"[METADATA_SYNC] Failed (save succeeded): {e}", exc_info=True)
        return {"success": False, "count": 0, "message": str(e)}
