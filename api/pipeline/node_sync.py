# Moved from: api/utils/canvas_node_sync.py
"""
Sync canvas configuration (nodes, edges) to CanvasNode and CanvasEdge DB tables.

Node details (business_name, config, etc.) are persisted to pipeline_nodes and canvas_edge
tables on save, instead of relying only on the JSON configuration or cache.
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Map frontend node type to CanvasNode.node_type choices
NODE_TYPE_MAP = {
    'source': 'SOURCE',
    'filter': 'FILTER',
    'projection': 'PROJECTION',
    'join': 'JOIN',
    'calculated': 'CALCULATED_COLUMN',
    'destination': 'DESTINATION',
    'group': 'GROUP',
    'sort': 'SORT',
    'union': 'UNION',
    'aggregate': 'AGGREGATION',
    'aggregation': 'AGGREGATION',
    'compute': 'TRANSFORM',
    'transform': 'TRANSFORM',
}

def _normalize_node_type(raw: str) -> str:
    """Normalize frontend node type to CanvasNode choices."""
    if not raw:
        return 'UNKNOWN'
    s = str(raw).lower().strip()
    if s.startswith('destination-'):
        return 'DESTINATION'
    return NODE_TYPE_MAP.get(s, str(raw).upper()[:50])

def sync_configuration_to_db(canvas) -> dict[str, int]:
    """
    Sync canvas configuration (nodes, edges) to CanvasNode and CanvasEdge tables.

    Persists node details (business_name, config_json, output_metadata, etc.) to DB
    so they are stored in pipeline_nodes and canvas_edge, not just in JSON or cache.

    :param canvas: Django Canvas instance with configuration set (call after canvas.save())
    :return: dict with nodes_upserted, nodes_deleted, edges_upserted, edges_deleted
    """
    from api.models.canvas import CanvasEdge, CanvasNode

    configuration = getattr(canvas, 'configuration', None) or {}
    nodes_list = configuration.get('nodes', [])
    edges_list = configuration.get('edges', []) or []

    result = {'nodes_upserted': 0, 'nodes_deleted': 0, 'edges_upserted': 0, 'edges_deleted': 0}

    if not nodes_list:
        # Remove all nodes/edges for this canvas
        deleted_nodes = CanvasNode.objects.filter(canvas=canvas).delete()
        result['nodes_deleted'] = deleted_nodes[0] if isinstance(deleted_nodes, tuple) else deleted_nodes
        deleted_edges = CanvasEdge.objects.filter(canvas=canvas).delete()
        result['edges_deleted'] = deleted_edges[0] if isinstance(deleted_edges, tuple) else deleted_edges
        logger.info(f"[CANVAS_SYNC] Cleared {result['nodes_deleted']} nodes, {result['edges_deleted']} edges for canvas {canvas.id}")
        return result

    # Build node_id -> node_data map
    node_map: dict[str, dict[str, Any]] = {}
    for node_data in nodes_list:
        nid = node_data.get('id') or (node_data.get('data', {}) or {}).get('node_id')
        if not nid:
            continue
        nid = str(nid)
        node_map[nid] = node_data

    # Upsert CanvasNode for each node
    existing_node_ids = set()
    for nid, node_data in node_map.items():
        data = node_data.get('data', {}) or {}
        node_type_raw = node_data.get('type') or data.get('type', 'unknown')
        node_type = _normalize_node_type(node_type_raw)
        valid_types = [c[0] for c in CanvasNode._meta.get_field('node_type').choices]
        if node_type not in valid_types:
            node_type = 'TRANSFORM'

        business_name = (
            data.get('business_name') or data.get('node_name') or data.get('label')
            or f"New {node_type_raw or 'Node'}"
        )
        technical_name = data.get('technical_name') or f"{node_type_raw}_{nid[:8]}"
        config = data.get('config', {})
        input_nodes = data.get('input_nodes', [])
        output_metadata = data.get('output_metadata') or {}
        pos = node_data.get('position', {}) or {}
        pos_x = float(pos.get('x', 0))
        pos_y = float(pos.get('y', 0))

        obj, created = CanvasNode.objects.update_or_create(
            canvas=canvas,
            node_id=nid,
            defaults={
                'business_name': str(business_name)[:255],
                'technical_name': str(technical_name)[:255],
                'node_name': str(business_name)[:255],
                'node_type': node_type,
                'config_json': config,
                'input_nodes': input_nodes,
                'output_metadata': output_metadata if output_metadata else {},
                'position_x': pos_x,
                'position_y': pos_y,
            },
        )
        result['nodes_upserted'] += 1
        existing_node_ids.add(nid)

    # Delete CanvasNode entries no longer in configuration
    to_delete = CanvasNode.objects.filter(canvas=canvas).exclude(node_id__in=existing_node_ids)
    try:
        deleted_count = to_delete.count()
        to_delete.delete()
    except Exception as delete_err:
        # In some legacy schemas, related edge foreign keys may have mismatched types.
        # Log and continue so canvas.save_configuration() still succeeds.
        logger.warning("[CANVAS_SYNC] Failed to delete stale CanvasNode records for canvas %s: %s", canvas.id, delete_err, exc_info=True)
        deleted_count = 0
    result['nodes_deleted'] = deleted_count

    # Build node_id -> CanvasNode lookup for edges
    node_id_to_canvas_node: dict[str, CanvasNode] = {
        cn.node_id: cn for cn in CanvasNode.objects.filter(canvas=canvas)
    }

    # Upsert CanvasEdge for each edge
    edge_ids_in_config = set()
    for edge_data in edges_list:
        edge_id = edge_data.get('id') or f"{edge_data.get('source')}-{edge_data.get('target')}"
        source_id = str(edge_data.get('source', ''))
        target_id = str(edge_data.get('target', ''))

        source_node = node_id_to_canvas_node.get(source_id)
        target_node = node_id_to_canvas_node.get(target_id)
        if not source_node or not target_node:
            logger.warning(f"[CANVAS_SYNC] Skipping edge {edge_id}: source or target node not found")
            continue

        CanvasEdge.objects.update_or_create(
            canvas=canvas,
            edge_id=edge_id,
            defaults={
                'source_node': source_node,
                'target_node': target_node,
            },
        )
        result['edges_upserted'] += 1
        edge_ids_in_config.add(edge_id)

    # Delete CanvasEdge entries no longer in configuration
    to_delete_edges = CanvasEdge.objects.filter(canvas=canvas).exclude(edge_id__in=edge_ids_in_config)
    try:
        deleted_edges_count = to_delete_edges.count()
        to_delete_edges.delete()
    except Exception as edge_delete_err:
        logger.warning("[CANVAS_SYNC] Failed to delete stale CanvasEdge records for canvas %s: %s", canvas.id, edge_delete_err, exc_info=True)
        deleted_edges_count = 0
    result['edges_deleted'] = deleted_edges_count

    logger.info(
        f"[CANVAS_SYNC] Synced canvas {canvas.id}: "
        f"nodes upserted={result['nodes_upserted']}, deleted={result['nodes_deleted']}; "
        f"edges upserted={result['edges_upserted']}, deleted={result['edges_deleted']}"
    )
    return result
