"""
Node API Views
Merged from: node_addition.py, node_management.py, node_cache_views.py
"""

# ------------------------------------------------------------
# From: api/views/node_addition.py
# ------------------------------------------------------------
"""
Node Addition API endpoints for explicit node insertion methods.
Supports two methods:
1. Edge-based insertion (insert between nodes)
2. Output handle-based addition (add after node)
"""
import logging
import uuid

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from api.authentications import JWTCookieAuthentication
from api.utils.graph_utils import validate_dag
from api.utils.helpers import ensure_user_has_customer

logger = logging.getLogger(__name__)

class AddNodeAfterView(APIView):
    """
    API endpoint for adding a node after another node (output handle-based).

    POST /api/pipeline/add-node-after/
    Body:
    {
        "canvas_id": 1,
        "new_node": {
            "id": "uuid",
            "type": "filter",
            "config": {...},
            "position": {"x": 100, "y": 200}
        },
        "source_node_id": "node-a-id"
    }
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """
        Add a node after a source node (output handle-based).

        Creates edge: A → X
        If A already has downstream nodes, creates parallel branch:
        A → B
        A → X

        Cache behavior:
        - Preserve cache for A and upstream
        - Only new branch (X and downstream) recomputes
        """
        try:
            user = request.user
            customer = ensure_user_has_customer(user)

            canvas_id = request.data.get('canvas_id')
            new_node_data = request.data.get('new_node', {})
            source_node_id = request.data.get('source_node_id')  # Node A (parent)

            if not canvas_id or not new_node_data or not source_node_id:
                return Response(
                    {"error": "canvas_id, new_node, and source_node_id are required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Get canvas
            from api.models.canvas import Canvas
            try:
                canvas = Canvas.objects.get(id=canvas_id, customer=customer)
            except Canvas.DoesNotExist:
                return Response(
                    {"error": f"Canvas {canvas_id} not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Get current pipeline configuration
            config = canvas.configuration or {}
            nodes = config.get('nodes', [])
            edges = config.get('edges', [])

            # Validate that source node exists
            source_node = next((n for n in nodes if n.get('id') == source_node_id), None)
            if not source_node:
                return Response(
                    {"error": f"Source node {source_node_id} not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            # ENFORCEMENT: Cannot add Source node after another node
            new_node_type = new_node_data.get('type')
            if new_node_type == 'source':
                return Response(
                    {"error": "Source node can only be added via table drop, not via output handle insertion."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # ENFORCEMENT: Check if Source node already exists (only one Source allowed)
            existing_source_nodes = [n for n in nodes if n.get('data', {}).get('type') == 'source']
            if len(existing_source_nodes) > 0:
                return Response(
                    {"error": "Only one Source node is allowed per pipeline. Source node is always the root."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # ENFORCEMENT: Cannot add Source node via output handle (Source is only via table drop)
            if new_node_type == 'source':
                return Response(
                    {"error": "Source node can only be added via table drop, not via output handle insertion."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Generate new node ID if not provided
            new_node_id = new_node_data.get('id') or str(uuid.uuid4())

            # Validate DAG after addition
            new_nodes = [*nodes, {'id': new_node_id, 'data': {'type': new_node_data.get('type', 'filter'), 'config': new_node_data.get('config', {}), 'label': new_node_data.get('label', f'New {new_node_data.get("type", "node")}')}, 'position': new_node_data.get('position', {'x': 0, 'y': 0})}]

            # Add edge: A → X (parallel branches allowed)
            new_edges = edges.copy()

            # Check if edge already exists (shouldn't happen, but validate)
            existing_edge = next(
                (e for e in edges if e.get('source') == source_node_id and e.get('target') == new_node_id),
                None
            )

            if not existing_edge:
                new_edges.append({
                    'id': f"{source_node_id}-{new_node_id}",
                    'source': source_node_id,
                    'target': new_node_id,
                    'sourceHandle': 'output',
                    'targetHandle': 'input'
                })

            # Validate DAG
            is_valid, dag_error = validate_dag(new_nodes, new_edges)
            if not is_valid:
                return Response(
                    {"error": f"Invalid DAG after addition: {dag_error}"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Update canvas configuration
            config['nodes'] = new_nodes
            config['edges'] = new_edges
            canvas.configuration = config
            canvas.save()

            # Persist node details to DB (pipeline_nodes, canvas_edge)
            try:
                from api.utils.canvas_node_sync import sync_configuration_to_db
                sync_configuration_to_db(canvas)
            except Exception as sync_err:
                logger.warning(f"[NODE ADD] Failed to sync nodes to DB: {sync_err}")

            from api.utils.metadata_sync import update_node_metadata_for_canvas
            update_node_metadata_for_canvas(canvas)

            # Cache invalidation: Only invalidate new branch (X and downstream)
            from api.services.checkpoint_cache import CheckpointCacheManager
            checkpoint_mgr = CheckpointCacheManager(customer.cust_db, canvas_id)
            checkpoint_mgr.invalidate_downstream(new_node_id, new_nodes, new_edges)

            logger.info(f"Added node {new_node_id} after {source_node_id}")
            logger.info("Preserved upstream caches (A and upstream)")
            logger.info(f"Invalidated downstream caches in new branch for node {new_node_id}")

            return Response({
                "success": True,
                "node_id": new_node_id,
                "preserved_caches": [source_node_id],  # A's cache is preserved
                "message": "Node added successfully. Downstream caches invalidated in new branch."
            }, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error adding node after: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return Response(
                {"error": f"Failed to add node: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

# From: api/views/node_management.py
# ------------------------------------------------------------
"""
Node Insertion and Deletion Management for DAG-based Pipelines.
Implements cache-aware node insertion and deletion with edge rewiring.

Core Principles:
- Pipelines are DAGs (nodes + edges)
- Nodes are replaceable units
- Insert/delete rewires edges, not rebuilds pipelines
- Recompute only downstream of change
- Preserve upstream caches
"""
import logging

from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView

from api.authentications import JWTCookieAuthentication
from api.utils.cache_aware_execution import (
    get_execution_path_from_cache,
    validate_metadata_compatibility,
)

logger = logging.getLogger(__name__)

class NodeInsertionView(APIView):
    """
    API endpoint for inserting a node between two existing nodes.

    POST /api/pipeline/insert-node/
    Body:
    {
        "canvas_id": 1,
        "new_node": {
            "id": "uuid",
            "type": "filter",
            "config": {...},
            "position": {"x": 100, "y": 200}
        },
        "source_node_id": "node-b-id",
        "target_node_id": "node-c-id"
    }
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """Test endpoint to verify route is accessible"""
        return Response({"message": "NodeInsertionView is accessible", "method": "GET"}, status=status.HTTP_200_OK)

    def post(self, request):
        """
        Insert a node between two existing nodes.

        Rewires edges: B → C becomes B → X → C
        Invalidates downstream caches: C and all downstream nodes
        Preserves upstream caches: A, B remain valid
        """
        try:
            logger.info("[NODE INSERT] Received insert node request")
            logger.debug(f"[NODE INSERT] Request data: {request.data}")

            user = request.user
            customer = ensure_user_has_customer(user)

            canvas_id = request.data.get('canvas_id')
            new_node_data = request.data.get('new_node', {})
            source_node_id = request.data.get('source_node_id')  # Node B (parent)
            target_node_id = request.data.get('target_node_id')  # Node C (child)

            # Accept current nodes/edges from frontend if provided (frontend state is source of truth)
            frontend_nodes = request.data.get('nodes', None)
            frontend_edges = request.data.get('edges', None)

            logger.info(f"[NODE INSERT] Parsed request - canvas_id: {canvas_id}, source: {source_node_id}, target: {target_node_id}, new_node: {new_node_data.get('type', 'unknown')}")
            logger.info(f"[NODE INSERT] Frontend provided nodes: {frontend_nodes is not None}, edges: {frontend_edges is not None}")

            if not canvas_id or not new_node_data or not source_node_id or not target_node_id:
                logger.error(f"[NODE INSERT] Missing required fields - canvas_id: {canvas_id}, new_node: {bool(new_node_data)}, source: {source_node_id}, target: {target_node_id}")
                return Response(
                    {"error": "canvas_id, new_node, source_node_id, and target_node_id are required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Get canvas
            from api.models.canvas import Canvas
            try:
                canvas = Canvas.objects.get(id=canvas_id, customer=customer)
            except Canvas.DoesNotExist:
                return Response(
                    {"error": f"Canvas {canvas_id} not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Get current pipeline configuration
            config = canvas.configuration or {}

            # Use frontend-provided nodes/edges if available (frontend state is source of truth)
            # Otherwise fall back to saved configuration
            if frontend_nodes is not None and frontend_edges is not None:
                logger.info("[NODE INSERT] Using nodes/edges from frontend request")
                nodes = frontend_nodes if isinstance(frontend_nodes, list) else []
                edges = frontend_edges if isinstance(frontend_edges, list) else []
            else:
                # Debug: Log configuration structure
                logger.info("[NODE INSERT] Loading from canvas.configuration")
                logger.info(f"[NODE INSERT] Canvas configuration type: {type(config)}")
                logger.info(f"[NODE INSERT] Canvas configuration keys: {list(config.keys()) if isinstance(config, dict) else 'Not a dict'}")

                # Handle different configuration formats
                if isinstance(config, dict):
                    nodes = config.get('nodes', [])
                    edges = config.get('edges', [])
                else:
                    # If configuration is not a dict, try to parse it
                    logger.warning("[NODE INSERT] Configuration is not a dict, attempting to parse")
                    nodes = []
                    edges = []

                logger.info(f"[NODE INSERT] Canvas has {len(nodes)} nodes and {len(edges)} edges from saved configuration")

                # If configuration is empty, try to load from CanvasNode/CanvasEdge models
                if len(nodes) == 0 or len(edges) == 0:
                    logger.warning("[NODE INSERT] Configuration appears empty, attempting to load from CanvasNode/CanvasEdge models")
                    try:
                        from api.models.canvas import CanvasEdge, CanvasNode
                        canvas_nodes = CanvasNode.objects.filter(canvas=canvas)
                        canvas_edges = CanvasEdge.objects.filter(canvas=canvas)

                        if canvas_nodes.exists():
                            logger.info(f"[NODE INSERT] Found {canvas_nodes.count()} nodes in CanvasNode table")
                            # Convert CanvasNode to ReactFlow format
                            nodes = []
                            for cn in canvas_nodes:
                                node = {
                                    'id': cn.node_id,
                                    'data': {
                                        'type': cn.node_type.lower() if cn.node_type else 'unknown',
                                        'label': cn.business_name,
                                        'config': cn.config_json or {},
                                        'input_nodes': cn.input_nodes or [],
                                        'output_metadata': cn.output_metadata or {},
                                    },
                                    'position': {
                                        'x': cn.position_x,
                                        'y': cn.position_y,
                                    }
                                }
                                nodes.append(node)

                            # Convert CanvasEdge to ReactFlow format
                            edges = []
                            for ce in canvas_edges:
                                edge = {
                                    'id': ce.edge_id,
                                    'source': ce.source_node.node_id,
                                    'target': ce.target_node.node_id,
                                }
                                edges.append(edge)

                            logger.info(f"[NODE INSERT] Loaded {len(nodes)} nodes and {len(edges)} edges from CanvasNode/CanvasEdge tables")
                    except Exception as model_load_error:
                        logger.error(f"[NODE INSERT] Failed to load from CanvasNode/CanvasEdge: {model_load_error}", exc_info=True)

            # Validate that source and target nodes exist
            source_node = next((n for n in nodes if n.get('id') == source_node_id), None)
            target_node = next((n for n in nodes if n.get('id') == target_node_id), None)

            if not source_node:
                logger.error(f"[NODE INSERT] Source node {source_node_id} not found. Available nodes: {[n.get('id') for n in nodes]}")
                logger.error(f"[NODE INSERT] Canvas configuration structure: {type(config)}, keys: {list(config.keys()) if isinstance(config, dict) else 'N/A'}")
                return Response(
                    {
                        "error": f"Source node {source_node_id} not found in canvas configuration",
                        "details": f"Canvas has {len(nodes)} nodes. Available node IDs: {[n.get('id') for n in nodes]}",
                        "suggestion": "Please ensure the canvas has been saved with nodes before inserting new nodes."
                    },
                    status=status.HTTP_404_NOT_FOUND
                )

            if not target_node:
                logger.error(f"[NODE INSERT] Target node {target_node_id} not found. Available nodes: {[n.get('id') for n in nodes]}")
                return Response(
                    {
                        "error": f"Target node {target_node_id} not found in canvas configuration",
                        "details": f"Canvas has {len(nodes)} nodes. Available node IDs: {[n.get('id') for n in nodes]}",
                        "suggestion": "Please ensure the canvas has been saved with nodes before inserting new nodes."
                    },
                    status=status.HTTP_404_NOT_FOUND
                )

            logger.info(f"[NODE INSERT] Source node found: {source_node.get('data', {}).get('type', 'unknown')}, Target node found: {target_node.get('data', {}).get('type', 'unknown')}")

            # ENFORCEMENT: Source node constraints
            # 1. Cannot insert nodes *into* a Source node (i.e., no upstream transform before a Source)
            target_node_type = target_node.get('data', {}).get('type')
            if target_node_type == 'source':
                return Response(
                    {"error": "Cannot insert nodes before a Source node. Source nodes are always roots and cannot have upstream transforms."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # 2. Cannot insert Source node between other nodes
            new_node_type = new_node_data.get('type')
            if new_node_type == 'source':
                return Response(
                    {"error": "Source node can only be added via table drop, not via edge insertion."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # 3. Cannot insert Destination node between other nodes (destinations are terminal)
            if new_node_type == 'destination':
                return Response(
                    {"error": "Destination node cannot be inserted between nodes. Destinations are terminal and must be added at the end of pipeline branches."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # 4. Cannot insert node if target is already a destination (destinations cannot have downstream nodes)
            if target_node_type == 'destination':
                return Response(
                    {"error": "Cannot insert node before a destination. Destinations are terminal and cannot have upstream transform nodes."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Validate that edge B → C exists
            existing_edge = next(
                (e for e in edges if e.get('source') == source_node_id and e.get('target') == target_node_id),
                None
            )

            if not existing_edge:
                logger.error(f"[NODE INSERT] Edge from {source_node_id} to {target_node_id} does not exist. Available edges: {[(e.get('source'), e.get('target')) for e in edges]}")
                return Response(
                    {"error": f"Edge from {source_node_id} to {target_node_id} does not exist"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            logger.info(f"[NODE INSERT] Edge validated: {source_node_id} → {target_node_id}")

            # Generate new node ID if not provided
            new_node_id = new_node_data.get('id') or str(uuid.uuid4())

            # MANDATORY ORDER: Remove ALL edges between A and B first, then add node, then add new edges
            logger.info(f"[NODE INSERT] Step 1: Removing all edges {source_node_id} → {target_node_id}")
            # Step 1: Remove every edge (A → B) so no duplicate or stale edge remains
            # Normalize to str for robust matching (frontend may send different types)
            src_str = str(source_node_id) if source_node_id is not None else ""
            tgt_str = str(target_node_id) if target_node_id is not None else ""
            new_edges = []
            edges_removed_count = 0
            for edge in edges:
                e_src = str(edge.get('source') or "") if edge.get('source') is not None else ""
                e_tgt = str(edge.get('target') or "") if edge.get('target') is not None else ""
                if e_src == src_str and e_tgt == tgt_str:
                    edges_removed_count += 1
                    logger.info(f"[NODE INSERT] Removed edge: {source_node_id} → {target_node_id} (id={edge.get('id')})")
                    continue
                new_edges.append(edge)
            if edges_removed_count == 0:
                logger.error(f"[NODE INSERT] Failed to find any edge to remove: {source_node_id} → {target_node_id}")
                return Response(
                    {"error": "Failed to find edge to remove"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            if edges_removed_count > 1:
                logger.info(f"[NODE INSERT] Removed {edges_removed_count} duplicate edges between {source_node_id} and {target_node_id}")

            logger.info(f"[NODE INSERT] Step 2: Creating new node {new_node_id}")
            # Step 2: Create the new node (X) — include top-level type for React Flow / save_config
            new_node_type = new_node_data.get('type', 'filter')
            new_node = {
                'id': new_node_id,
                'type': new_node_type,
                'data': {
                    'type': new_node_type,
                    'config': new_node_data.get('config', {}),
                    'label': new_node_data.get('label', f"New {new_node_type}"),
                    'input_nodes': [source_node_id],  # X has A as input
                },
                'position': new_node_data.get('position', {'x': 0, 'y': 0})
            }
            new_nodes = [*nodes, new_node]
            logger.info(f"[NODE INSERT] Created new node: {new_node_id} (type: {new_node_data.get('type', 'filter')})")

            logger.info("[NODE INSERT] Step 3: Creating new edges")
            # Step 3: Create new edges (A → X and X → B)
            # Preserve edge metadata (handle, etc.)
            edge_handle = existing_edge.get('sourceHandle') or 'output'
            target_handle = existing_edge.get('targetHandle') or 'input'

            # Ensure target handle is valid for special node types (like Join)
            target_node_data = target_node.get('data', {})
            target_node_type = target_node_data.get('type')
            if target_node_type == 'join' and target_handle not in ['left', 'right']:
                # Find other edges to this join node to determine which handle is free
                other_edges = [e for e in edges if e.get('target') == target_node_id and e.get('id') != existing_edge.get('id')]
                left_taken = any(e.get('targetHandle') == 'left' for e in other_edges)
                target_handle = 'right' if left_taken else 'left'
                logger.info(f"[NODE INSERT] Fixed invalid targetHandle '{target_handle}' for join node {target_node_id}")

            new_edges.append({
                'id': f"{source_node_id}-{new_node_id}",
                'source': source_node_id,
                'target': new_node_id,
                'sourceHandle': edge_handle,
                'targetHandle': 'input'
            })
            logger.info(f"[NODE INSERT] Added edge: {source_node_id} → {new_node_id}")

            new_edges.append({
                'id': f"{new_node_id}-{target_node_id}",
                'source': new_node_id,
                'target': target_node_id,
                'sourceHandle': 'output',
                'targetHandle': target_handle
            })
            logger.info(f"[NODE INSERT] Added edge: {new_node_id} → {target_node_id}")

            # Deduplicate edges by (source, target) so no duplicate or backward edges slip through
            node_ids_after = {n.get('id') for n in new_nodes}
            seen_pairs = set()
            deduped_edges = []
            for edge in new_edges:
                src, tgt = edge.get('source'), edge.get('target')
                if src not in node_ids_after or tgt not in node_ids_after:
                    logger.warning(f"[NODE INSERT] Skipping orphaned edge: {src} → {tgt}")
                    continue
                key = (src, tgt)
                if key in seen_pairs:
                    logger.warning(f"[NODE INSERT] Skipping duplicate edge: {src} → {tgt}")
                    continue
                seen_pairs.add(key)
                deduped_edges.append(edge)
            new_edges = deduped_edges

            # Step 4: Update node configs (input_nodes arrays)
            # Remove A from B.input_nodes (B now gets input from X, not A)
            # Add X to B.input_nodes
            for i, node in enumerate(new_nodes):
                if node.get('id') == target_node_id:
                    # Update B's input_nodes: remove A, add X
                    node_data = node.get('data', {})
                    current_input_nodes = node_data.get('input_nodes', [])
                    updated_input_nodes = [nid for nid in current_input_nodes if nid != source_node_id]
                    if new_node_id not in updated_input_nodes:
                        updated_input_nodes.append(new_node_id)

                    # Update node with new input_nodes
                    updated_node_data = dict(node_data)
                    updated_node_data['input_nodes'] = updated_input_nodes

                    updated_node = dict(node)
                    updated_node['data'] = updated_node_data
                    new_nodes[i] = updated_node

                    logger.info(f"Updated node {target_node_id} input_nodes: removed {source_node_id}, added {new_node_id}")
                    break

            # Validate DAG
            is_valid, dag_error = validate_dag(new_nodes, new_edges)
            if not is_valid:
                return Response(
                    {"error": f"Invalid DAG after insertion: {dag_error}"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Update canvas configuration
            config['nodes'] = new_nodes
            config['edges'] = new_edges
            canvas.configuration = config
            canvas.save()

            # Persist node details to DB (pipeline_nodes, canvas_edge)
            try:
                from api.utils.canvas_node_sync import sync_configuration_to_db
                sync_configuration_to_db(canvas)
            except Exception as sync_err:
                logger.warning(f"[NODE INSERT] Failed to sync nodes to DB: {sync_err}")

            from api.utils.metadata_sync import update_node_metadata_for_canvas
            update_node_metadata_for_canvas(canvas)

            # VALIDATION: Verify graph structure after insertion
            # 1. Original edge A → B should NOT exist
            original_edge_exists = any(
                e.get('source') == source_node_id and e.get('target') == target_node_id
                for e in new_edges
            )
            if original_edge_exists:
                logger.error(f"VALIDATION FAILED: Original edge {source_node_id} → {target_node_id} still exists!")
                return Response(
                    {"error": "Validation failed: Original edge was not removed"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            # 2. Two new edges should exist: A → X and X → B
            edge_a_to_x = any(
                e.get('source') == source_node_id and e.get('target') == new_node_id
                for e in new_edges
            )
            edge_x_to_b = any(
                e.get('source') == new_node_id and e.get('target') == target_node_id
                for e in new_edges
            )

            if not edge_a_to_x or not edge_x_to_b:
                logger.error("VALIDATION FAILED: Missing edges after insertion!")
                logger.error(f"  A → X exists: {edge_a_to_x}")
                logger.error(f"  X → B exists: {edge_x_to_b}")
                return Response(
                    {"error": "Validation failed: Required edges were not created"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            logger.info("VALIDATION PASSED: Graph structure is correct after insertion")

            # Cache invalidation: Invalidate downstream nodes (B and all downstream)
            # Preserve upstream caches (A and all upstream remain valid)
            from api.services.checkpoint_cache import CheckpointCacheManager
            checkpoint_mgr = CheckpointCacheManager(customer.cust_db, canvas_id)
            checkpoint_mgr.invalidate_downstream(target_node_id, new_nodes, new_edges)

            logger.info(f"Inserted node {new_node_id} between {source_node_id} and {target_node_id}")
            logger.info("Preserved upstream caches (A and upstream)")
            logger.info(f"Invalidated downstream caches starting from node {target_node_id}")

            return Response({
                "success": True,
                "new_node_id": new_node_id,  # Match frontend expectation
                "node_id": new_node_id,  # Keep for backward compatibility
                "preserved_caches": [source_node_id],  # A's cache is preserved
                "nodes": new_nodes,  # Return updated nodes
                "edges": new_edges,  # Return updated edges
                "message": "Node inserted successfully. Downstream caches invalidated."
            }, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error inserting node: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return Response(
                {"error": f"Failed to insert node: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def _find_downstream_nodes(self, node_id: str, nodes: list[dict], edges: list[dict]) -> set[str]:
        """Find all downstream nodes from a given node."""
        downstream = set()
        visited = set()

        def dfs(current_id: str):
            if current_id in visited:
                return
            visited.add(current_id)

            # Find all nodes that this node connects to
            for edge in edges:
                if edge.get('source') == current_id:
                    target_id = edge.get('target')
                    if target_id:
                        downstream.add(target_id)
                        dfs(target_id)

        dfs(node_id)
        return downstream

class NodeDeletionView(APIView):
    """
    API endpoint for deleting a node with auto-bridging.

    POST /api/pipeline/delete-node/
    Body (saved canvas):
    { "canvas_id": 1, "node_id": "node-x-id" }

    Body (unsaved canvas): nodes and edges required
    { "node_id": "node-x-id", "nodes": [...], "edges": [...] }
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """
        Delete a node and auto-bridge its parents to children.

        Before: A → B → X → C → D
        After:  A → B → C → D

        Validates metadata compatibility before bridging.
        Invalidates downstream caches.
        """
        try:
            user = request.user
            customer = ensure_user_has_customer(user)

            canvas_id = request.data.get('canvas_id')
            node_id = request.data.get('node_id')
            request_nodes = request.data.get('nodes')
            request_edges = request.data.get('edges')

            if not node_id:
                return Response(
                    {"error": "node_id is required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Unsaved canvas: canvas_id not required when nodes/edges provided
            has_canvas_id = canvas_id is not None and str(canvas_id).strip() != ""
            if not has_canvas_id:
                if request_nodes is None or request_edges is None:
                    return Response(
                        {"error": "For unsaved canvas, nodes and edges are required in request"},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                nodes = request_nodes
                edges = request_edges
                config = {'nodes': nodes, 'edges': edges}
                canvas = None
                logger.info(f"[NODE DELETE] Unsaved canvas: using nodes/edges from request. Nodes: {len(nodes)}, Edges: {len(edges)}")
            else:
                from api.models.canvas import Canvas
                try:
                    canvas = Canvas.objects.get(id=canvas_id, customer=customer)
                except Canvas.DoesNotExist:
                    return Response(
                        {"error": f"Canvas {canvas_id} not found"},
                        status=status.HTTP_404_NOT_FOUND
                    )
                config = canvas.configuration or {}
                if request_nodes is not None and request_edges is not None:
                    nodes = request_nodes
                    edges = request_edges
                    logger.info(f"[NODE DELETE] Using nodes/edges from request. Nodes: {len(nodes)}, Edges: {len(edges)}")
                else:
                    nodes = config.get('nodes', [])
                    edges = config.get('edges', [])
                    logger.info(f"[NODE DELETE] Using nodes/edges from canvas.configuration. Nodes: {len(nodes)}, Edges: {len(edges)}")

            # Validate that node exists
            node_to_delete = next((n for n in nodes if n.get('id') == node_id), None)
            if not node_to_delete:
                return Response(
                    {"error": f"Node {node_id} not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Find parents and children
            parent_ids = []
            child_ids = []

            for edge in edges:
                if edge.get('target') == node_id:
                    parent_ids.append(edge.get('source'))
                elif edge.get('source') == node_id:
                    child_ids.append(edge.get('target'))

            # Validate metadata compatibility for each parent-child pair
            metadata_warnings = []
            incompatible_pairs = []

            for parent_id in parent_ids:
                for child_id in child_ids:
                    parent_node = next((n for n in nodes if n.get('id') == parent_id), None)
                    child_node = next((n for n in nodes if n.get('id') == child_id), None)

                    if parent_node and child_node:
                        is_compatible, warning = validate_metadata_compatibility(
                            parent_node, child_node
                        )

                        if not is_compatible:
                            incompatible_pairs.append({
                                'parent': parent_id,
                                'child': child_id,
                                'warning': warning
                            })
                            metadata_warnings.append(warning)

            # If incompatible pairs exist, return warnings but don't crash
            if incompatible_pairs:
                logger.warning(f"Metadata incompatibility detected for {len(incompatible_pairs)} pairs")
                # Continue with deletion but mark children as invalid

            # Remove node from nodes list
            new_nodes = [n for n in nodes if n.get('id') != node_id]

            # Preserve targetHandle from removed edge (deleted_node → child) so Join etc. get correct handle (left/right)
            deleted_to_child_handles = {}  # child_id -> { targetHandle, sourceHandle } from edge node_id → child_id
            for edge in edges:
                if edge.get('source') == node_id:
                    child_id = edge.get('target')
                    if child_id and child_id not in deleted_to_child_handles:
                        deleted_to_child_handles[child_id] = {
                            'targetHandle': edge.get('targetHandle') or 'input',
                            'sourceHandle': edge.get('sourceHandle') or 'output',
                        }

            # Rewire edges: Remove all edges connected to X, add direct parent → child edges
            new_edges = []
            edges_to_add = []

            for edge in edges:
                source = edge.get('source')
                target = edge.get('target')

                # Remove edges connected to deleted node
                if source == node_id or target == node_id:
                    continue

                new_edges.append(edge)

            # Add direct parent → child edges (auto-bridge); preserve targetHandle for Join (left/right)
            for parent_id in parent_ids:
                for child_id in child_ids:
                    # Check if edge already exists (avoid duplicate/unwanted edges)
                    existing = next(
                        (e for e in new_edges if e.get('source') == parent_id and e.get('target') == child_id),
                        None
                    )
                    if existing:
                        continue
                    # Use preserved handle so reconnection goes to same input (e.g. Join left/right)
                    handle_info = deleted_to_child_handles.get(child_id, {})
                    target_handle = handle_info.get('targetHandle', 'input')
                    edges_to_add.append({
                        'id': f"{parent_id}-{child_id}",
                        'source': parent_id,
                        'target': child_id,
                        'sourceHandle': 'output',
                        'targetHandle': target_handle,
                    })

            new_edges.extend(edges_to_add)

            # Step: Update input_nodes for children to point to parents now that X is gone
            for child_id in child_ids:
                for i, node in enumerate(new_nodes):
                    if node.get('id') == child_id:
                        node_data = node.get('data', {})
                        current_input_nodes = node_data.get('input_nodes', [])
                        # Remove deleted node X, add all its parents
                        updated_input_nodes = [nid for nid in current_input_nodes if nid != node_id]
                        for parent_id in parent_ids:
                            if parent_id not in updated_input_nodes:
                                updated_input_nodes.append(parent_id)

                        # Apply update
                        new_nodes[i]['data']['input_nodes'] = updated_input_nodes
                        logger.info(f"[NODE DELETE] Updated child {child_id} input_nodes: removed {node_id}, added {parent_ids}")
                        break

            # Clean up orphaned edges: Remove any edges that reference non-existent nodes
            node_ids = {n.get('id') for n in new_nodes}
            cleaned_edges = []
            seen_edges = set()  # Track (source, target) pairs to avoid duplicates

            for edge in new_edges:
                source = edge.get('source')
                target = edge.get('target')

                # Skip edges where source or target doesn't exist
                if source not in node_ids or target not in node_ids:
                    logger.warning(f"[NODE DELETE] Removing orphaned edge: {source} → {target} (one or both nodes don't exist)")
                    continue

                # Skip duplicate edges (same source and target)
                edge_key = (source, target)
                if edge_key in seen_edges:
                    logger.warning(f"[NODE DELETE] Removing duplicate edge: {source} → {target}")
                    continue

                seen_edges.add(edge_key)
                cleaned_edges.append(edge)

            new_edges = cleaned_edges

            # Validate DAG after deletion
            is_valid, dag_error = validate_dag(new_nodes, new_edges)
            if not is_valid:
                return Response(
                    {"error": f"Invalid DAG after deletion: {dag_error}"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Update configuration
            config['nodes'] = new_nodes
            config['edges'] = new_edges

            # Save to DB and invalidate cache only when canvas exists (saved canvas)
            if canvas is not None:
                canvas.configuration = config
                canvas.save()
                # Persist node details to DB (pipeline_nodes, canvas_edge)
                try:
                    from api.utils.canvas_node_sync import sync_configuration_to_db
                    sync_configuration_to_db(canvas)
                except Exception as sync_err:
                    logger.warning(f"[NODE DELETE] Failed to sync nodes to DB: {sync_err}")
                from api.utils.metadata_sync import delete_node_metadata_from_cache, update_node_metadata_for_canvas
                delete_node_metadata_from_cache(canvas_id, node_id, customer)
                update_node_metadata_for_canvas(canvas)
                from api.services.checkpoint_cache import CheckpointCacheManager
                checkpoint_mgr = CheckpointCacheManager(customer.cust_db, canvas_id)
                checkpoint_mgr.invalidate_downstream(node_id, new_nodes, new_edges)

            logger.info(f"Deleted node {node_id}")
            logger.info(f"Auto-bridged {len(parent_ids)} parents to {len(child_ids)} children")
            logger.info(f"Invalidated downstream caches for node {node_id}")
            logger.info(f"Final node count: {len(new_nodes)}, Final edge count: {len(new_edges)}")

            response_data = {
                "success": True,
                "deleted_node_id": node_id,
                "nodes": new_nodes,  # Return cleaned nodes
                "edges": new_edges,  # Return cleaned edges (with orphaned edges removed)
                "bridged_edges": [f"{p} → {c}" for p in parent_ids for c in child_ids],
                "message": "Node deleted successfully. Downstream caches invalidated."
            }

            if metadata_warnings:
                response_data['warnings'] = metadata_warnings
                response_data['incompatible_pairs'] = incompatible_pairs

            return Response(response_data, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error deleting node: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return Response(
                {"error": f"Failed to delete node: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class PipelineRecomputeView(APIView):
    """
    API endpoint for recomputing pipeline from nearest upstream cache.

    POST /api/pipeline/recompute/
    Body:
    {
        "canvas_id": 1,
        "target_node_id": "node-c-id"
    }
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """
        Recompute pipeline from nearest upstream cache.

        Finds the nearest cached node upstream from target.
        Resumes execution from that cache.
        Never re-fetches from DB unless required.
        """
        try:
            user = request.user
            customer = ensure_user_has_customer(user)

            canvas_id = request.data.get('canvas_id')
            target_node_id = request.data.get('target_node_id')

            if not canvas_id or not target_node_id:
                return Response(
                    {"error": "canvas_id and target_node_id are required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Get canvas
            from api.models.canvas import Canvas
            try:
                canvas = Canvas.objects.get(id=canvas_id, customer=customer)
            except Canvas.DoesNotExist:
                return Response(
                    {"error": f"Canvas {canvas_id} not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Get pipeline configuration
            config = canvas.configuration or {}
            nodes = config.get('nodes', [])
            edges = config.get('edges', [])

            # Find nearest upstream cache
            from api.services.checkpoint_cache import CheckpointCacheManager
            checkpoint_mgr = CheckpointCacheManager(customer.cust_db, canvas_id)

            # Find nearest cached node upstream
            nearest_cache_node, checkpoint = checkpoint_mgr.find_nearest_checkpoint(
                target_node_id, nodes, edges
            )

            if nearest_cache_node:
                recompute_path = get_execution_path_from_cache(
                    nearest_cache_node, target_node_id, nodes, edges
                )
                return Response({
                    "success": True,
                    "nearest_cache_node": nearest_cache_node,
                    "message": f"Recompute should start from cached node {nearest_cache_node}",
                    "recompute_path": recompute_path,
                    "from_cache": True
                }, status=status.HTTP_200_OK)
            else:
                # No cache found - recompute from sources
                source_nodes = [n['id'] for n in nodes if n.get('data', {}).get('type') == 'source']
                recompute_path = []
                if source_nodes:
                    # Get path from first source to target
                    recompute_path = get_execution_path_from_cache(
                        source_nodes[0], target_node_id, nodes, edges
                    ) if source_nodes else []

                return Response({
                    "success": True,
                    "nearest_cache_node": None,
                    "message": "No upstream cache found. Recompute from source nodes.",
                    "recompute_path": recompute_path
                }, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error finding recompute path: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return Response(
                {"error": f"Failed to find recompute path: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

# From: api/views/node_cache_views.py
# ------------------------------------------------------------
"""
Node Cache API Views

Views for managing and accessing node transformation caches.
"""

import logging

from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView

from api.authentications import JWTCookieAuthentication
from api.services.node_cache import get_node_cache_manager

logger = logging.getLogger(__name__)

# Import ensure_user_has_customer - using late import to avoid circular dependency
def get_ensure_user_has_customer():
    from api.utils.helpers import ensure_user_has_customer
    return ensure_user_has_customer

class NodeCacheView(APIView):
    """
    API view for node cache operations.

    GET /api/node-cache/<canvas_id>/<node_id>/
        Get cached data for a specific node

    POST /api/node-cache/<canvas_id>/<node_id>/
        Save node data to cache

    DELETE /api/node-cache/<canvas_id>/<node_id>/
        Invalidate cache for a node
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, canvas_id, node_id):
        """Get cached data for a node."""
        try:
            ensure_user_has_customer = get_ensure_user_has_customer()
            customer = ensure_user_has_customer(request.user)

            cache_manager = get_node_cache_manager(customer)
            cached_data = cache_manager.get_cache(int(canvas_id), node_id)

            if cached_data:
                return Response({
                    'success': True,
                    'from_cache': True,
                    **cached_data
                }, status=status.HTTP_200_OK)
            else:
                return Response({
                    'success': False,
                    'from_cache': False,
                    'message': 'No valid cache found for this node'
                }, status=status.HTTP_404_NOT_FOUND)

        except Exception as e:
            logger.error(f"Error getting node cache: {e}")
            return Response({
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def post(self, request, canvas_id, node_id):
        """Save node data to cache."""
        try:
            ensure_user_has_customer = get_ensure_user_has_customer()
            customer = ensure_user_has_customer(request.user)

            node_type = request.data.get('node_type', 'unknown')
            rows = request.data.get('rows', [])
            columns = request.data.get('columns', [])
            config = request.data.get('config', {})
            source_node_ids = request.data.get('source_node_ids', [])

            cache_manager = get_node_cache_manager(customer)
            success = cache_manager.save_cache(
                canvas_id=int(canvas_id),
                node_id=node_id,
                node_type=node_type,
                rows=rows,
                columns=columns,
                config=config,
                source_node_ids=source_node_ids
            )

            if success:
                return Response({
                    'success': True,
                    'message': f'Cached {len(rows)} rows for node {node_id}'
                }, status=status.HTTP_201_CREATED)
            else:
                return Response({
                    'success': False,
                    'error': 'Failed to save cache'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        except Exception as e:
            logger.error(f"Error saving node cache: {e}")
            return Response({
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, canvas_id, node_id=None):
        """Invalidate cache for a node or entire canvas."""
        try:
            ensure_user_has_customer = get_ensure_user_has_customer()
            customer = ensure_user_has_customer(request.user)

            cache_manager = get_node_cache_manager(customer)
            cache_manager.invalidate_cache(int(canvas_id), node_id)

            return Response({
                'success': True,
                'message': f'Cache invalidated for canvas {canvas_id}' + (f', node {node_id}' if node_id else '')
            }, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error invalidating node cache: {e}")
            return Response({
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class NodeCacheStatsView(APIView):
    """
    API view for cache statistics.

    GET /api/node-cache/stats/
        Get all cache statistics

    GET /api/node-cache/stats/<canvas_id>/
        Get cache statistics for a specific canvas
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, canvas_id=None):
        """Get cache statistics."""
        try:
            ensure_user_has_customer = get_ensure_user_has_customer()
            customer = ensure_user_has_customer(request.user)

            cache_manager = get_node_cache_manager(customer)
            stats = cache_manager.get_cache_stats(int(canvas_id) if canvas_id else None)

            # Convert datetime objects to strings for JSON serialization
            for cache in stats.get('caches', []):
                for key in ['created_on', 'last_accessed']:
                    if cache.get(key):
                        cache[key] = cache[key].isoformat() if hasattr(cache[key], 'isoformat') else str(cache[key])

            return Response(stats, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return Response({
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class NodeCacheCleanupView(APIView):
    """
    API view for cache cleanup operations.

    POST /api/node-cache/cleanup/
        Clean up old caches (default: older than 7 days)
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """Clean up old caches."""
        try:
            ensure_user_has_customer = get_ensure_user_has_customer()
            customer = ensure_user_has_customer(request.user)

            days_old = int(request.data.get('days_old', 7))

            cache_manager = get_node_cache_manager(customer)
            cache_manager.cleanup_old_caches(days_old)

            return Response({
                'success': True,
                'message': f'Cleaned up caches older than {days_old} days'
            }, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error cleaning up caches: {e}")
            return Response({
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
