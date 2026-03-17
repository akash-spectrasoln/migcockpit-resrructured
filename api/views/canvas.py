"""
Canvas API Views
"""

import logging

from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from api.authentications import JWTCookieAuthentication
from api.models.canvas import Canvas
from api.serializers import CanvasCreateSerializer, CanvasSerializer
from api.utils.graph_utils import strip_orphaned_edges, validate_dag


# Import ensure_user_has_customer - using late import to avoid circular dependency
def get_ensure_user_has_customer():
    from api.utils.helpers import ensure_user_has_customer
    return ensure_user_has_customer

logger = logging.getLogger(__name__)

class CanvasViewSet(viewsets.ModelViewSet):
    """
    ViewSet for Canvas CRUD operations
    """
    queryset = Canvas.objects.all()
    serializer_class = CanvasSerializer
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get_serializer_class(self):
        """Use different serializer for create"""
        if self.action == 'create':
            return CanvasCreateSerializer
        return CanvasSerializer

    def get_queryset(self):
        """Filter canvases by customer and optionally by project"""
        try:
            user = self.request.user
            project_id = self.request.query_params.get('project_id')

            # Check if project_id column exists in canvas table
            has_project_id_column = False
            try:
                from django.db import connection
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'canvas' AND column_name = 'project_id'
                    """)
                    has_project_id_column = cursor.fetchone() is not None
            except Exception as check_error:
                logger.warning(f"Could not check for project_id column: {check_error}")

            if user.is_superuser:
                queryset = Canvas.objects.all()
            else:
                # Ensure user has a customer
                ensure_user_has_customer = get_ensure_user_has_customer()
                customer = ensure_user_has_customer(user)
                if customer:
                    queryset = Canvas.objects.filter(customer=customer)
                else:
                    return Canvas.objects.none()

            # Filter by project if provided and column exists
            if project_id and has_project_id_column:
                try:
                    queryset = queryset.filter(project_id=project_id)
                except Exception as filter_error:
                    logger.warning(f"Could not filter by project_id: {filter_error}")

            return queryset
        except Exception as e:
            logger.error(f"Error in get_queryset: {e}", exc_info=True)
            import traceback
            logger.error(traceback.format_exc())
            # Return empty queryset to prevent 500 error
            return Canvas.objects.none()

    def perform_create(self, serializer):
        """Set customer from user. We intentionally do NOT set created_by,
        because the legacy user table does not have an integer primary key
        compatible with Django's ForeignKey expectations.
        """
        user = self.request.user  # kept for future extension/logging
        ensure_user_has_customer = get_ensure_user_has_customer()
        customer = ensure_user_has_customer(user)

        # Get project_id from request data if provided (check both 'project' and 'project_id')
        project_id = self.request.data.get('project_id') or self.request.data.get('project')

        if customer:
            if project_id:
                try:
                    project_id = int(project_id)
                    serializer.save(customer=customer, project_id=project_id)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid project_id: {project_id}, creating canvas without project")
                    serializer.save(customer=customer)
            else:
                serializer.save(customer=customer)
        else:
            serializer.save()

    def create(self, request, *args, **kwargs):
        """Override create to return full CanvasSerializer response with id"""
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)

        # Get the created canvas instance
        canvas = serializer.instance

        # Return full CanvasSerializer response (includes id and all fields)
        response_serializer = CanvasSerializer(canvas)
        headers = self.get_success_headers(response_serializer.data)
        return Response(response_serializer.data, status=status.HTTP_201_CREATED, headers=headers)

    # Use hyphenated URL path to match documented endpoint:
    # POST /api/canvas/{id}/save-configuration/
    @action(detail=True, methods=['post'], url_path='save-configuration')
    def save_configuration(self, request, pk=None):
        """Save canvas configuration"""
        canvas = self.get_object()
        # Prefetch customer so metadata sync has it (avoids lazy-load issues)
        if not hasattr(canvas, '_prefetched_objects_cache') or 'customer' not in (canvas._prefetched_objects_cache or {}):
            from django.db.models import prefetch_related_objects
            prefetch_related_objects([canvas], 'customer')
        configuration = request.data.get('configuration', {})

        # Validate DAG (no cycles) before saving; strip edges that reference deleted nodes
        nodes = configuration.get('nodes', [])
        edges = configuration.get('edges', [])
        if nodes:
            node_list = [{'id': n.get('id', str(i)), **n} if isinstance(n, dict) else {'id': str(i)} for i, n in enumerate(nodes)]
            edge_list = list(edges) if edges else []
            cleaned_edges = strip_orphaned_edges(node_list, edge_list)
            if len(cleaned_edges) < len(edge_list):
                logger.warning(
                    "Save configuration: removed %d orphaned edge(s) referencing missing nodes; saving cleaned edges",
                    len(edge_list) - len(cleaned_edges),
                )
                configuration = dict(configuration)
                configuration['edges'] = cleaned_edges
                edge_list = cleaned_edges
            if edge_list:
                is_valid_dag, dag_error = validate_dag(node_list, edge_list)
                if not is_valid_dag and dag_error:
                    return Response(
                        {"error": f"Pipeline has a cycle or invalid structure: {dag_error}"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

        # ============================================================
        # DEBUG: Track calculated columns being saved
        # ============================================================
        logger.info("=" * 80)
        logger.info(f"[SAVE DEBUG] Saving canvas configuration for canvas_id={canvas.id}")

        nodes = configuration.get('nodes', [])
        for node_data in nodes:
            node_id = node_data.get('id')
            node_type = node_data.get('type')
            data = node_data.get('data', {})
            config = data.get('config', {})

            # Check for calculated columns in various locations
            calc_cols_in_config = config.get('calculatedColumns', [])
            calc_cols_in_data = data.get('calculatedColumns', [])

            # Check output_metadata for calculated columns
            output_metadata = data.get('output_metadata', {})
            output_cols = output_metadata.get('columns', []) if isinstance(output_metadata, dict) else []
            calc_cols_in_metadata = [
                col for col in output_cols
                if col.get('source') == 'calculated' or col.get('isCalculated')
            ]

            if calc_cols_in_config or calc_cols_in_data or calc_cols_in_metadata:
                logger.info(f"[SAVE DEBUG] Node: {node_id} (type={node_type})")
                logger.info(f"[SAVE DEBUG]   - config.calculatedColumns: {len(calc_cols_in_config)} columns")
                if calc_cols_in_config:
                    for cc in calc_cols_in_config:
                        logger.info(f"[SAVE DEBUG]       * {cc.get('name')}: {cc.get('expression')}")

                logger.info(f"[SAVE DEBUG]   - data.calculatedColumns: {len(calc_cols_in_data)} columns")
                if calc_cols_in_data:
                    for cc in calc_cols_in_data:
                        logger.info(f"[SAVE DEBUG]       * {cc.get('name')}: {cc.get('expression')}")

                logger.info(f"[SAVE DEBUG]   - output_metadata calculated columns: {len(calc_cols_in_metadata)} columns")
                if calc_cols_in_metadata:
                    for col in calc_cols_in_metadata:
                        expr = col.get('expression') or col.get('formula', '')
                        logger.info(f"[SAVE DEBUG]       * {col.get('name')}: {expr}")

        logger.info("=" * 80)

        canvas.configuration = configuration
        # Do NOT set modified_by - legacy user table doesn't have integer PK
        # The modified_on field will auto-update via auto_now=True
        canvas.save()

        # Persist node details to DB (pipeline_nodes, canvas_edge) - not just JSON or cache
        try:
            from api.utils.canvas_node_sync import sync_configuration_to_db
            sync_result = sync_configuration_to_db(canvas)
            logger.info(f"[SAVE] Synced node details to DB: {sync_result}")
        except Exception as sync_err:
            logger.warning(f"[SAVE] Failed to sync nodes to DB (configuration saved): {sync_err}", exc_info=True)

        # Only update node_cache_metadata when explicitly requested (explicit Save Pipeline).
        # Auto-saves (add destination, rename node, etc.) skip metadata sync to avoid
        # writing metadata before the user has finished configuring.
        skip_metadata_sync = request.data.get('skip_metadata_sync', False)
        metadata_result = None
        if not skip_metadata_sync:
            from api.utils.metadata_sync import update_node_metadata_for_canvas
            metadata_result = update_node_metadata_for_canvas(canvas)

        serializer = self.get_serializer(canvas)
        response_data = serializer.data
        if isinstance(response_data, dict) and metadata_result:
            response_data["metadata_sync"] = metadata_result
        return Response(response_data)

    def perform_destroy(self, instance):
        """
        Override delete to handle missing migration_job table gracefully.
        The migration_job table may not exist if migrations haven't been run,
        but we still want to allow canvas deletion.
        """
        try:
            # Try normal deletion first
            instance.delete()
        except Exception as e:
            error_str = str(e)
            # If the error is about missing migration_job table, delete using raw SQL
            if 'migration_job' in error_str and 'does not exist' in error_str:
                logger.warning(f"migration_job table does not exist, deleting canvas {instance.id} directly")
                from django.db import connection
                with connection.cursor() as cursor:
                    # Delete canvas directly via SQL, bypassing Django's cascade checks
                    cursor.execute("DELETE FROM canvas WHERE id = %s", [instance.id])
            else:
                # Re-raise if it's a different error
                raise

    @action(detail=False, methods=['get'])
    def my_canvases(self, request):
        """Get canvases for current user's customer"""
        user = request.user
        if hasattr(user, 'cust_id') and user.cust_id:
            canvases = Canvas.objects.filter(customer=user.cust_id, is_active=True)
        else:
            canvases = Canvas.objects.none()

        serializer = self.get_serializer(canvases, many=True)
        return Response(serializer.data)
