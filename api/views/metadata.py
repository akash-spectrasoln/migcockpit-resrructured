"""
Metadata API Views
Provides metadata for canvas configuration (tables, columns, validation rules, etc.)
"""

import logging

import httpx
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from api.authentications import JWTCookieAuthentication
from api.utils.graph_utils import strip_orphaned_edges, validate_dag

logger = logging.getLogger(__name__)

EXTRACTION_SERVICE_URL = "http://localhost:8001"

class MetadataViewSet(viewsets.ViewSet):
    """
    ViewSet for metadata operations
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    @action(detail=False, methods=['get'])
    def tables(self, request):
        """Get list of tables for a source connection"""
        source_id = request.query_params.get('source_id')
        if not source_id:
            return Response(
                {"error": "source_id parameter is required"},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            # Get source connection details from Django
            from api.models import SourceDB
            source = SourceDB.objects.get(id=source_id)

            # Call extraction service to get tables
            async def get_tables():
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.post(
                        f"{EXTRACTION_SERVICE_URL}/metadata/tables",
                        json={
                            "connection_type": source.db_type.lower(),
                            "connection_config": {
                                "host": source.host,
                                "port": source.port,
                                "database": source.database_name,
                                "username": source.username,
                                "password": source.password,  # In production, decrypt this
                            }
                        }
                    )
                    return response.json()

            import asyncio
            result = asyncio.run(get_tables())

            return Response(result)

        except Exception as e:
            logger.error(f"Error fetching tables: {e}")
            return Response(
                {"error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @action(detail=False, methods=['get'])
    def columns(self, request):
        """Get list of columns for a table"""
        source_id = request.query_params.get('source_id')
        table_name = request.query_params.get('table_name')
        schema = request.query_params.get('schema', '')

        if not source_id or not table_name:
            return Response(
                {"error": "source_id and table_name parameters are required"},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            from api.models import SourceDB
            source = SourceDB.objects.get(id=source_id)

            async def get_columns():
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.post(
                        f"{EXTRACTION_SERVICE_URL}/metadata/columns",
                        json={
                            "connection_type": source.db_type.lower(),
                            "connection_config": {
                                "host": source.host,
                                "port": source.port,
                                "database": source.database_name,
                                "username": source.username,
                                "password": source.password,
                            },
                            "table_name": table_name,
                            "schema": schema,
                        }
                    )
                    return response.json()

            import asyncio
            result = asyncio.run(get_columns())

            return Response(result)

        except Exception as e:
            logger.error(f"Error fetching columns: {e}")
            return Response(
                {"error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @action(detail=False, methods=['get'])
    def validation_rules(self, request):
        """Get available validation rules"""
        # Return predefined validation rules schema
        rules = [
            {
                "id": "required",
                "name": "Required Field",
                "description": "Field must not be empty",
                "schema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "id": "min_length",
                "name": "Minimum Length",
                "description": "String must be at least N characters",
                "schema": {
                    "type": "object",
                    "properties": {
                        "min": {"type": "number", "minimum": 0}
                    },
                    "required": ["min"]
                }
            },
            {
                "id": "max_length",
                "name": "Maximum Length",
                "description": "String must be at most N characters",
                "schema": {
                    "type": "object",
                    "properties": {
                        "max": {"type": "number", "minimum": 1}
                    },
                    "required": ["max"]
                }
            },
            {
                "id": "regex",
                "name": "Regex Pattern",
                "description": "Value must match regex pattern",
                "schema": {
                    "type": "object",
                    "properties": {
                        "pattern": {"type": "string"}
                    },
                    "required": ["pattern"]
                }
            },
            {
                "id": "range",
                "name": "Numeric Range",
                "description": "Number must be within range",
                "schema": {
                    "type": "object",
                    "properties": {
                        "min": {"type": "number"},
                        "max": {"type": "number"}
                    },
                    "required": ["min", "max"]
                }
            },
            {
                "id": "email",
                "name": "Email Format",
                "description": "Value must be a valid email",
                "schema": {
                    "type": "object",
                    "properties": {}
                }
            },
        ]

        return Response(rules)

    @action(detail=False, methods=['post'])
    def validate_pipeline(self, request):
        """
        Validate pipeline and create frozen execution plan.

        This implements the validation-gated lifecycle:
        1. Validate DAG structure
        2. Build deterministic execution plan
        3. Compute plan hash
        4. Return validation result with plan metadata
        """
        from datetime import datetime

        nodes = request.data.get('nodes', [])
        edges = request.data.get('edges', [])
        canvas_id = (
            request.data.get('canvas_id')
            or request.data.get('canvasId')
            or request.data.get('config', {}).get('canvas_id')
            or request.data.get('config', {}).get('canvasId')
        )

        logger.info(f"[VALIDATE] Starting validation for canvas {canvas_id}")
        logger.info(f"[VALIDATE] Received {len(nodes)} nodes, {len(edges)} edges")

        if not canvas_id:
            logger.warning(f"[VALIDATE] ⚠️ No canvas_id in request! Keys: {list(request.data.keys())}")

        errors = []
        warnings = []

        # Normalize to list of dicts
        nodes = list(nodes) if nodes else []
        edges = list(edges) if edges else []
        node_list = [{'id': n.get('id', str(i)), **n} if isinstance(n, dict) else {'id': str(i)} for i, n in enumerate(nodes)]
        edge_list = list(edges)

        logger.debug(f"[VALIDATE] Normalized nodes: {[n.get('id') for n in node_list]}")
        logger.debug(f"[VALIDATE] Normalized edges: {[(e.get('source'), e.get('target')) for e in edge_list]}")

        # STEP 1: Basic DAG validation (ignore edges that reference deleted nodes)
        logger.info("[VALIDATE] Step 1: Validating DAG structure")
        if node_list and edge_list:
            cleaned_edges = strip_orphaned_edges(node_list, edge_list)
            is_valid_dag, dag_error = validate_dag(node_list, cleaned_edges)
            if not is_valid_dag and dag_error:
                errors.append(f"Pipeline has a cycle or invalid structure: {dag_error}")
                logger.error(f"[VALIDATE] DAG validation failed: {dag_error}")

        # Check for at least one source
        source_nodes = [
            n for n in node_list
            if str(n.get('type', '')).lower().startswith('source')
            or str(n.get('data', {}).get('type', '')).lower().startswith('source')
        ]
        if len(source_nodes) == 0:
            errors.append("At least one source node is required")
            logger.error("[VALIDATE] No source nodes found")
        else:
            logger.info(f"[VALIDATE] Found {len(source_nodes)} source nodes")

        # Check for at least one destination (includes destination-postgresql, destination-hana, etc.)
        def _is_destination_node(n):
            t = n.get('type') or n.get('data', {}).get('type') or ''
            return t == 'destination' or (isinstance(t, str) and t.startswith('destination-'))
        dest_nodes = [n for n in node_list if _is_destination_node(n)]
        if len(dest_nodes) == 0:
            errors.append("At least one destination node is required")
            logger.error("[VALIDATE] No destination nodes found")
        else:
            logger.info(f"[VALIDATE] Found {len(dest_nodes)} destination nodes")

        # Validate node configurations
        {n.get('id') for n in node_list}
        for node in node_list:
            node_data = node.get('data', {})
            config = node_data.get('config', {})
            node_type = node.get('type') or node_data.get('type')

            if node_type == 'source':
                if not config.get('sourceId'):
                    errors.append(f"Source node '{node_data.get('label', node.get('id'))}' is missing source connection")
                if not config.get('tableName'):
                    errors.append(f"Source node '{node_data.get('label', node.get('id'))}' is missing table name")

            elif node_type == 'destination' or (isinstance(node_type, str) and node_type.startswith('destination')):
                is_customer_db = config.get('destinationType') == 'customer_database'
                if not is_customer_db and not config.get('destinationId'):
                    errors.append(f"Destination node '{node_data.get('label', node.get('id'))}' is missing destination connection")
                if not config.get('tableName'):
                    errors.append(f"Destination node '{node_data.get('label', node.get('id'))}' is missing table name")

        # Check connectivity
        if len(edge_list) == 0 and len(node_list) > 1:
            warnings.append("No connections between nodes")

        # If basic validation failed, return early
        if errors:
            logger.error(f"[VALIDATE] Basic validation failed with {len(errors)} errors")
            return Response({
                "valid": False,
                "errors": errors,
                "warnings": warnings,
                "execution_plan_created": False
            })

        # STEP 2: Create execution plan using SQL pushdown planner
        logger.info("[VALIDATE] Step 2: Creating execution plan via Migration Service")

        try:
            # Call Migration Service to validate and create execution plan
            logger.debug("[VALIDATE] Calling Migration Service API")

            async def validate_with_migration_service():
                from api.models.canvas import Canvas
                from api.utils.db_connection import get_customer_db_config_from_request_async
                from api.utils.migration_config_builder import build_migration_config

                # Use same config as Execute (source_configs, destination_configs, connection_config)
                # so plan hash at Validate matches hash at Execute and saved plan is reused.
                connection_config = await get_customer_db_config_from_request_async(request)
                config = {}
                try:
                    def _get_canvas_and_config():
                        canvas = Canvas.objects.get(id=canvas_id)
                        customer = getattr(canvas, "customer", None)
                        return build_migration_config(canvas, customer, {}) if customer else {}
                    config = await asyncio.to_thread(_get_canvas_and_config)
                except Exception as e:
                    logger.warning(f"[VALIDATE] Could not build migration config: {e}")
                if not config.get("connection_config") and connection_config:
                    config["connection_config"] = connection_config

                # Execution plan creation can take 1-2+ min for large pipelines (metadata, SQL compilation)
                async with httpx.AsyncClient(timeout=180.0) as client:
                    response = await client.post(
                        "http://localhost:8003/validate",
                        json={
                            "job_id": f"validate_{canvas_id}_{int(datetime.utcnow().timestamp())}",
                            "canvas_id": canvas_id,
                            "nodes": node_list,
                            "edges": edge_list,
                            "connection_config": config.get("connection_config") or connection_config,
                            "persist": True,
                            "config": config,
                        }
                    )
                    return response.json()

            import asyncio
            result = asyncio.run(validate_with_migration_service())

            # Check if validation succeeded
            if result.get("success"):
                logger.info("[VALIDATE] ✓✓✓ Validation SUCCESSFUL ✓✓✓")
                logger.info("[VALIDATE] Execution plan created and ready for execution")

                # Extract metadata from result
                metadata = result.get("metadata", {})
                logger.info("[VALIDATE] ✓ Execution plan created:")
                logger.info(f"[VALIDATE]   - Staging schema: {metadata.get('staging_schema')}")
                logger.info(f"[VALIDATE]   - Execution levels: {metadata.get('levels')}")
                logger.info(f"[VALIDATE]   - Total queries: {metadata.get('total_queries')}")
                logger.info(f"[VALIDATE]   - Plan persisted to DB: {metadata.get('plan_persisted')}")

                return Response({
                    "valid": True,
                    "errors": [],
                    "warnings": warnings,
                    "execution_plan_created": True,
                    "execution_plan_metadata": metadata
                })
            else:
                # Validation failed
                validation_errors = result.get("errors", ["Validation failed"])
                logger.error(f"[VALIDATE] ✗ Validation failed: {validation_errors}")
                errors.extend(validation_errors)
                return Response({
                    "valid": False,
                    "errors": errors,
                    "warnings": warnings,
                    "execution_plan_created": False
                })

        except httpx.ConnectError as e:
            logger.error(f"[VALIDATE] ✗ Migration Service not available: {e}")
            errors.append("Migration Service is not available. Please ensure it's running on port 8003.")
            return Response({
                "valid": False,
                "errors": errors,
                "warnings": warnings,
                "execution_plan_created": False
            })
        except httpx.ReadTimeout as e:
            logger.error(f"[VALIDATE] ✗ Migration Service read timeout: {e}")
            errors.append(
                "Validation is taking too long (timeout). The pipeline may be large or the Migration Service is busy. "
                "Try again or simplify the pipeline."
            )
            return Response({
                "valid": False,
                "errors": errors,
                "warnings": warnings,
                "execution_plan_created": False
            })
        except Exception as e:
            logger.error(f"[VALIDATE] ✗✗✗ Execution plan creation FAILED: {e}", exc_info=True)
            errors.append(f"Failed to create execution plan: {e!s}")
            return Response({
                "valid": False,
                "errors": errors,
                "warnings": warnings,
                "execution_plan_created": False
            })
