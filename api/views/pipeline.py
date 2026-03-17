"""
Pipeline-related API views.
Handles pipeline execution, filters, and joins.
"""
import asyncio
import json
import logging

# Lazy import pandas to avoid DLL load errors during Django startup
# import pandas as pd  # Moved to function level where needed
import re
from typing import Optional

from django.conf import settings
import httpx
import psycopg2
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from api.authentications import JWTCookieAuthentication
from api.cache.strategy import (
    compute_depth_since_last_cache,
    compute_fan_out,
    is_filter_pushdown_candidate,
)
from api.utils.calculated_column_evaluator import evaluate_calculated_expression
from api.utils.filters import build_sql_where_clause, parse_filter_from_canvas
from api.utils.graph_utils import strip_orphaned_edges, validate_dag
from api.utils.helpers import decrypt_source_data, ensure_user_has_customer
from api.views.query_parser import apply_auto_group_by_rule, build_group_by_sql, validate_aggregate_configuration

logger = logging.getLogger(__name__)

class FilterExecutionView(APIView):
    """
    API view to execute filter conditions and return filtered results.
    Uses the reusable filter system from api.filters.
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request, source_id):
        """
        Execute filter conditions on a source table.
        Path params:
        - source_id: ID of the source connection (from URL path)
        Body:
        - table_name: Table name (required)
        - schema: Schema name (optional)
        - conditions: List of filter conditions (required)
        - page: Page number (default 1)
        - page_size: Number of rows per page (default 50)
        """
        try:
            source_id = int(source_id)
            table_name = request.data.get('table_name')
            schema = request.data.get('schema', '')
            conditions = request.data.get('conditions', [])
            page = int(request.data.get('page', 1))
            page_size = int(request.data.get('page_size', 50))

            # Log request for debugging
            logger.info(f"FilterExecutionView request - source_id: {source_id}, table_name: {table_name}, schema: {schema}")
            logger.info(f"FilterExecutionView conditions: {conditions}")
            logger.info(f"FilterExecutionView conditions type: {type(conditions)}, is_list: {isinstance(conditions, list)}")

            if not table_name:
                logger.error(f"FilterExecutionView: table_name is missing. Request data: {request.data}")
                return Response(
                    {"error": "table_name is required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if not conditions or not isinstance(conditions, list):
                logger.error(f"FilterExecutionView: conditions invalid. Type: {type(conditions)}, Value: {conditions}")
                return Response(
                    {"error": f"conditions must be a non-empty array. Received: {type(conditions).__name__}"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if len(conditions) == 0:
                logger.error("FilterExecutionView: conditions array is empty")
                return Response(
                    {"error": "conditions must be a non-empty array"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Get user and customer
            user = request.user
            customer = ensure_user_has_customer(user)

            # Connect to customer's database to get source connection
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=customer.cust_db
            )
            conn.autocommit = True
            db_cursor = conn.cursor()

            # Get source connection details
            db_cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'GENERAL' AND table_name = 'source'
            """)
            [row[0] for row in db_cursor.fetchall()]

            db_cursor.execute(f'''
                SELECT id, {name_column}, {config_column}, created_on
                FROM "GENERAL".source
                WHERE id = %s
            ''', (source_id,))

            source_row = db_cursor.fetchone()
            if not source_row:
                db_cursor.close()
                conn.close()
                return Response(
                    {"error": "Source connection not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            source_id_db, source_name, source_config, created_on = source_row

            # Decrypt source configuration
            decrypted_config = decrypt_source_data(source_config, customer.cust_id, created_on)
            if not decrypted_config:
                db_cursor.close()
                conn.close()
                return Response(
                    {"error": "Failed to decrypt source configuration"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            # DEBUG: log exact connection details the service is using
            logger.info(
                f"[FilterExecution] Source '{source_name}' connection => "
                f"hostname={decrypted_config.get('hostname')!r} "
                f"port={decrypted_config.get('port')!r} "
                f"database={decrypted_config.get('database')!r} "
                f"user={decrypted_config.get('user')!r} "
                f"schema={decrypted_config.get('schema')!r} "
                f"db_type={decrypted_config.get('db_type')!r}"
            )

            db_type = decrypted_config.get('db_type', '').lower()

            # Attempt to fetch cached table metadata to preserve column types
            cached_metadata = []
            try:
                db_cursor.execute('''
                    SELECT table_fields
                    FROM "GENERAL".source_table_selection
                    WHERE source_id = %s
                    AND table_name = %s
                    AND (schema = %s OR schema IS NULL OR schema = '')
                ''', (source_id, table_name, schema))

                stored_row = db_cursor.fetchone()
                if stored_row and stored_row[0]:
                    cached_metadata = stored_row[0] if isinstance(stored_row[0], list) else json.loads(stored_row[0])
                    logger.info(f"FilterExecutionView: Found {len(cached_metadata)} cached columns for enrichment")
            except Exception as meta_err:
                logger.warning(f"FilterExecutionView: Failed to fetch cached metadata: {meta_err}")

            db_cursor.close()
            conn.close()

            # Use reusable filter system to parse and validate filters
            # Parse filter from canvas format to internal format
            canvas_filter = {'conditions': conditions}
            filter_spec = parse_filter_from_canvas(canvas_filter)
            logger.info(f"FilterExecutionView parsed filter_spec: {filter_spec}")

            # Build SQL WHERE clause for FastAPI (if needed for raw SQL)
            where_clause, where_params = build_sql_where_clause(filter_spec)
            logger.info(f"FilterExecutionView SQL where_clause: {where_clause}, params: {where_params}")

            # Also build filters list for FastAPI compatibility (legacy format)
            filters = []
            for condition in conditions:
                value = condition.get('value', '')
                # Handle BETWEEN operator value format
                if condition.get('operator') == 'BETWEEN':
                    if isinstance(value, str):
                        try:
                            value = json.loads(value)
                        except Exception:
                            parts = [v.strip() for v in value.split(',')]
                            if len(parts) == 2:
                                value = parts

                filters.append({
                    'column': condition.get('column', ''),
                    'operator': condition.get('operator', '='),
                    'value': value,
                    'logicalOperator': condition.get('logicalOperator', 'AND'),
                })

            # Call FastAPI service to fetch filtered table data
            try:
                EXTRACTION_SERVICE_URL = getattr(settings, 'FASTAPI_EXTRACTION_SERVICE_URL', 'http://localhost:8001')

                async def fetch_filtered_data():
                    # Increased timeout to 120 seconds for large table queries
                    async with httpx.AsyncClient(timeout=120.0) as client:
                        response = await client.post(
                            f"{EXTRACTION_SERVICE_URL}/metadata/filter",
                            json={
                                "db_type": db_type,
                                "connection_config": {
                                    "hostname": decrypted_config.get('hostname'),
                                    "port": decrypted_config.get('port'),
                                    "database": decrypted_config.get('database'),
                                    "user": decrypted_config.get('user'),
                                    "password": decrypted_config.get('password'),
                                    "schema": decrypted_config.get('schema'),
                                    "service_name": decrypted_config.get('service_name'),
                                },
                                "table_name": table_name,
                                "schema": schema or decrypted_config.get('schema'),
                                "filters": filters,  # Send filters array in legacy format
                                "page": page,
                                "page_size": page_size,
                            }
                        )
                        response.raise_for_status()
                        return response.json()

                result = asyncio.run(fetch_filtered_data())

                # SCHEMA PRESERVATION: Filter must not alter column types.
                # Fetch source schema and apply it to filtered results.
                def get_source_schema():
                    """Get complete source table schema with data types."""
                    # First try cached metadata
                    if cached_metadata and isinstance(cached_metadata, list) and len(cached_metadata) > 0:
                        logger.info(f"FilterExecutionView: Using cached schema ({len(cached_metadata)} columns)")
                        return cached_metadata

                    # If no cache, fetch from FastAPI
                    try:
                        logger.info(f"FilterExecutionView: Fetching schema from FastAPI for {table_name}")

                        async def fetch_schema():
                            async with httpx.AsyncClient(timeout=30.0) as client:
                                response = await client.post(
                                    f"{EXTRACTION_SERVICE_URL}/metadata/columns",
                                    json={
                                        "db_type": db_type,
                                        "connection_config": {
                                            "hostname": decrypted_config.get('hostname'),
                                            "port": decrypted_config.get('port'),
                                            "database": decrypted_config.get('database'),
                                            "user": decrypted_config.get('user'),
                                            "password": decrypted_config.get('password'),
                                            "schema": decrypted_config.get('schema'),
                                            "service_name": decrypted_config.get('service_name'),
                                        },
                                        "table_name": table_name,
                                        "schema": schema or decrypted_config.get('schema')
                                    }
                                )
                                response.raise_for_status()
                                return response.json()

                        schema_result = asyncio.run(fetch_schema())
                        source_schema = schema_result.get('columns', [])
                        logger.info(f"FilterExecutionView: Fetched schema from FastAPI ({len(source_schema)} columns)")
                        return source_schema
                    except Exception as e:
                        logger.error(f"FilterExecutionView: Failed to fetch schema: {e}")
                        return []

                # Get source schema
                source_schema = get_source_schema()

                # Apply source schema to filtered results
                result_columns = result.get('columns', [])

                if source_schema:
                    # Build schema map
                    schema_map = {}
                    for col_meta in source_schema:
                        if isinstance(col_meta, dict) and 'name' in col_meta:
                            schema_map[col_meta['name']] = col_meta

                    # Map result columns to source schema
                    enriched_columns = []
                    for col in result_columns:
                        col_name = col if isinstance(col, str) else col.get('name')

                        if col_name in schema_map:
                            # Use exact schema from source
                            enriched_columns.append(schema_map[col_name])
                        else:
                            # Column not in schema (shouldn't happen)
                            logger.warning(f"FilterExecutionView: Column '{col_name}' not in source schema")
                            if isinstance(col, dict):
                                enriched_columns.append(col)
                            else:
                                enriched_columns.append({'name': col_name, 'datatype': 'TEXT', 'nullable': True})

                    result_columns = enriched_columns
                    logger.info(f"FilterExecutionView: Applied source schema to {len(result_columns)} columns")
                else:
                    # No schema available, normalize what we have
                    logger.warning("FilterExecutionView: No source schema available")
                    result_columns = [
                        {'name': c, 'datatype': 'TEXT', 'nullable': True} if isinstance(c, str) else c
                        for c in result_columns
                    ]

                return Response({
                    "rows": result.get('rows', []),
                    "columns": result_columns,
                    "has_more": result.get('has_more', False),
                    "total": result.get('total', 0),
                    "filtered_count": result.get('total', 0),
                    "page": page,
                    "page_size": page_size
                }, status=status.HTTP_200_OK)

            except httpx.ConnectError as e:
                logger.error(f"FastAPI service connection error: {e}")
                return Response(
                    {
                        "error": "FastAPI extraction service is not available",
                        "details": f"Could not connect to {EXTRACTION_SERVICE_URL}",
                    },
                    status=status.HTTP_503_SERVICE_UNAVAILABLE
                )
            except httpx.ReadTimeout as e:
                logger.error(f"FastAPI service read timeout: {e}")
                return Response(
                    {
                        "error": "FastAPI extraction service request timed out",
                        "details": f"The request to {EXTRACTION_SERVICE_URL} exceeded the timeout limit (120 seconds). The table may be too large or the service may be slow.",
                    },
                    status=status.HTTP_504_GATEWAY_TIMEOUT
                )
            except httpx.TimeoutException as e:
                logger.error(f"FastAPI service timeout exception: {e}")
                return Response(
                    {
                        "error": "FastAPI extraction service request timed out",
                        "details": f"The request to {EXTRACTION_SERVICE_URL} exceeded the timeout limit. The table may be too large or the service may be slow.",
                    },
                    status=status.HTTP_504_GATEWAY_TIMEOUT
                )
            except Exception as e:
                logger.error(f"Error executing filter: {e}", exc_info=True)
                import traceback
                logger.error(f"FilterExecutionView traceback: {traceback.format_exc()}")
                return Response(
                    {"error": f"Failed to execute filter: {e!s}"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

        except Exception as e:
            logger.error(f"Error in FilterExecutionView: {e}")
            return Response(
                {"error": f"Internal server error: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class JoinExecutionView(APIView):
    """
    API view to execute join operations between two tables.
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request, source_id):
        """
        Execute join operation on source tables.
        Path params:
        - source_id: ID of the source connection (from URL path)
        Body:
        - left_table: Left table name (required)
        - right_table: Right table name (required)
        - left_schema: Left table schema (optional)
        - right_schema: Right table schema (optional)
        - join_type: Join type (INNER, LEFT, RIGHT, FULL OUTER, CROSS) (required)
        - conditions: List of join conditions (required for non-CROSS joins)
        - page: Page number (default 1)
        - page_size: Number of rows per page (default 50)
        """
        try:
            source_id = int(source_id)
            left_table = request.data.get('left_table')
            right_table = request.data.get('right_table')
            left_schema = request.data.get('left_schema', '')
            right_schema = request.data.get('right_schema', '')
            join_type = request.data.get('join_type', 'INNER')
            conditions = request.data.get('conditions', [])
            page = int(request.data.get('page', 1))
            page_size = int(request.data.get('page_size', 50))

            if not left_table or not right_table:
                return Response(
                    {"error": "left_table and right_table are required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if join_type not in ['INNER', 'LEFT', 'RIGHT', 'FULL OUTER', 'CROSS']:
                return Response(
                    {"error": "Invalid join_type. Must be INNER, LEFT, RIGHT, FULL OUTER, or CROSS"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if join_type != 'CROSS' and (not conditions or not isinstance(conditions, list) or len(conditions) == 0):
                return Response(
                    {"error": "conditions are required for non-CROSS joins"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Get user and customer
            user = request.user
            customer = ensure_user_has_customer(user)

            # Connect to customer's database to get source connection
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=customer.cust_db
            )
            conn.autocommit = True
            db_cursor = conn.cursor()

            # Get source connection details
            db_cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'GENERAL' AND table_name = 'source'
            """)
            [row[0] for row in db_cursor.fetchall()]

            db_cursor.execute(f'''
                SELECT {config_column}, created_on
                FROM "GENERAL".source
                WHERE id = %s
            ''', (source_id,))

            source_row = db_cursor.fetchone()
            if not source_row:
                db_cursor.close()
                conn.close()
                return Response(
                    {"error": "Source connection not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            source_config, created_on = source_row

            # Decrypt source configuration
            decrypted_config = decrypt_source_data(source_config, customer.cust_id, created_on)
            if not decrypted_config:
                db_cursor.close()
                conn.close()
                return Response(
                    {"error": "Failed to decrypt source configuration"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            db_type = decrypted_config.get('db_type', '').lower()
            db_cursor.close()
            conn.close()

            # Call FastAPI service to execute join
            try:
                EXTRACTION_SERVICE_URL = getattr(settings, 'FASTAPI_EXTRACTION_SERVICE_URL', 'http://localhost:8001')

                async def execute_join():
                    async with httpx.AsyncClient(timeout=60.0) as client:
                        response = await client.post(
                            f"{EXTRACTION_SERVICE_URL}/metadata/join",
                            json={
                                "db_type": db_type,
                                "connection_config": {
                                    "hostname": decrypted_config.get('hostname'),
                                    "port": decrypted_config.get('port'),
                                    "database": decrypted_config.get('database'),
                                    "user": decrypted_config.get('user'),
                                    "password": decrypted_config.get('password'),
                                    "schema": decrypted_config.get('schema'),
                                    "service_name": decrypted_config.get('service_name'),
                                },
                                "left_table": left_table,
                                "right_table": right_table,
                                "left_schema": left_schema or decrypted_config.get('schema'),
                                "right_schema": right_schema or decrypted_config.get('schema'),
                                "join_type": join_type,
                                "conditions": conditions,
                                "page": page,
                                "page_size": page_size,
                            }
                        )
                        response.raise_for_status()
                        return response.json()

                result = asyncio.run(execute_join())

                return Response({
                    "rows": result.get('rows', []),
                    "columns": result.get('columns', []),
                    "has_more": result.get('has_more', False),
                    "total": result.get('total', 0),
                    "page": page,
                    "page_size": page_size
                }, status=status.HTTP_200_OK)

            except httpx.ConnectError as e:
                logger.error(f"FastAPI service connection error: {e}")
                return Response(
                    {
                        "error": "FastAPI extraction service is not available",
                        "details": f"Could not connect to {EXTRACTION_SERVICE_URL}",
                    },
                    status=status.HTTP_503_SERVICE_UNAVAILABLE
                )
            except Exception as e:
                logger.error(f"Error executing join: {e}")
                return Response(
                    {"error": f"Failed to execute join: {e!s}"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

        except Exception as e:
            logger.error(f"Error in JoinExecutionView: {e}")
            return Response(
                {"error": f"Internal server error: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class PipelineQueryExecutionView(APIView):
    """
    API view to execute a complete pipeline query and return results.
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def _parse_nested_expression(self, expression: str):
        """
        Parse nested expression into execution stack.
        Example: SUBSTRING(UPPER(table), 2, 5) -> [
            { fn: "UPPER", args: ["table"], depth: 1 },
            { fn: "SUBSTRING", args: [<result_of_UPPER>, 2, 5], depth: 0 }
        ]
        """

        execution_stack = []

        def extract_nested_parens(expr: str, start_pos: int) -> tuple:
            """Extract content between parentheses, handling nesting"""
            if start_pos >= len(expr) or expr[start_pos] != '(':
                return None, start_pos
            start_pos += 1
            content = ''
            depth = 1
            pos = start_pos
            while pos < len(expr) and depth > 0:
                if expr[pos] == '(':
                    depth += 1
                elif expr[pos] == ')':
                    depth -= 1
                    if depth == 0:
                        return content, pos + 1
                content += expr[pos]
                pos += 1
            return content, pos

        def parse_function_call(expr: str, depth: int = 0):
            """Recursively parse function calls"""
            expr = expr.strip()

            # Match function name followed by parentheses
            func_match = re.match(r'^([A-Z_][A-Z0-9_]*)\s*\(', expr, re.IGNORECASE)
            if not func_match:
                # Not a function call, return as-is
                return {'type': 'literal', 'value': expr, 'depth': depth}

            func_name = func_match.group(1).upper()
            start_pos = func_match.end()

            # Extract arguments
            args_content, end_pos = extract_nested_parens(expr, start_pos - 1)
            if args_content is None:
                return {'type': 'literal', 'value': expr, 'depth': depth}

            # Parse arguments
            args = []
            current_arg = ''
            paren_depth = 0
            for char in args_content:
                if char == '(':
                    paren_depth += 1
                    current_arg += char
                elif char == ')':
                    paren_depth -= 1
                    current_arg += char
                elif char == ',' and paren_depth == 0:
                    arg_str = current_arg.strip()
                    if arg_str:
                        # Check if argument is a nested function
                        nested_func = re.match(r'^([A-Z_][A-Z0-9_]*)\s*\(', arg_str, re.IGNORECASE)
                        if nested_func:
                            args.append(parse_function_call(arg_str, depth + 1))
                        else:
                            args.append({'type': 'literal', 'value': arg_str, 'depth': depth + 1})
                    current_arg = ''
                else:
                    current_arg += char

            # Add last argument
            if current_arg.strip():
                arg_str = current_arg.strip()
                nested_func = re.match(r'^([A-Z_][A-Z0-9_]*)\s*\(', arg_str, re.IGNORECASE)
                if nested_func:
                    args.append(parse_function_call(arg_str, depth + 1))
                else:
                    args.append({'type': 'literal', 'value': arg_str, 'depth': depth + 1})

            return {
                'type': 'function',
                'fn': func_name,
                'args': args,
                'depth': depth
            }

        parsed = parse_function_call(expression)

        # Build execution stack (deepest first)
        def build_stack(node, stack):
            if node['type'] == 'function':
                # Process arguments first (they may be nested functions)
                for arg in node['args']:
                    if arg['type'] == 'function':
                        build_stack(arg, stack)
                # Then add this function to stack
                stack.append({
                    'fn': node['fn'],
                    'args': node['args'],
                    'depth': node['depth']
                })

        build_stack(parsed, execution_stack)

        return execution_stack

    def _build_nested_sql(self, execution_stack, original_expression):
        """
        Build SQL from execution stack, preserving nested structure.
        For nested functions like SUBSTRING(UPPER(table), 2, 5), this ensures
        the nested structure is maintained before COALESCE wrapping.
        """
        # The execution stack helps us understand nesting, but for SQL generation,
        # we keep the original expression structure and let _apply_coalesce_to_nested
        # handle the COALESCE wrapping recursively
        logger.debug(f"[Nested SQL] Execution stack: {execution_stack}")
        return original_expression

    def _apply_coalesce_to_nested(self, expression: str) -> str:
        """
        Recursively apply COALESCE wrapping to nested function arguments.
        Handles cases like SUBSTRING(UPPER(column), 2, 5) by wrapping
        the innermost arguments first, then outer ones.
        """

        def extract_nested_parens(expr: str, start_pos: int) -> tuple:
            """Extract content between parentheses, handling nesting"""
            if start_pos >= len(expr) or expr[start_pos] != '(':
                return None, start_pos
            start_pos += 1
            content = ''
            depth = 1
            pos = start_pos
            while pos < len(expr) and depth > 0:
                if expr[pos] == '(':
                    depth += 1
                elif expr[pos] == ')':
                    depth -= 1
                    if depth == 0:
                        return content, pos + 1
                content += expr[pos]
                pos += 1
            return content, pos

        def wrap_string_arg(arg: str) -> str:
            """Wrap a string argument with COALESCE if it's not already wrapped"""
            arg = arg.strip()
            # Already wrapped
            if arg.upper().startswith('COALESCE('):
                return arg
            # String literals - wrap with COALESCE
            if arg.startswith("'") or arg.startswith('"'):
                return f"COALESCE({arg}, '')"
            # Column names or expressions - wrap with COALESCE
            return f"COALESCE({arg}, '')"

        # Process nested functions from innermost to outermost
        # Find all function calls (UPPER, LOWER, SUBSTRING, CONCAT)
        normalized = expression

        # Handle UPPER/LOWER with nested functions
        for func_name in ['UPPER', 'LOWER']:
            pattern = rf'{func_name}\s*\('
            matches = list(re.finditer(pattern, normalized, re.IGNORECASE))
            for match in reversed(matches):  # Process from end to preserve positions
                start_pos = match.end()
                arg_expr, end_pos = extract_nested_parens(normalized, start_pos - 1)
                if arg_expr:
                    # Check if arg_expr contains another function call
                    if re.search(r'\b(UPPER|LOWER|SUBSTRING|CONCAT)\s*\(', arg_expr, re.IGNORECASE):
                        # Recursively process nested function
                        processed_arg = self._apply_coalesce_to_nested(arg_expr)
                        wrapped_arg = wrap_string_arg(processed_arg)
                    else:
                        wrapped_arg = wrap_string_arg(arg_expr)
                    normalized = normalized[:start_pos] + wrapped_arg + normalized[end_pos-1:]

        # Handle SUBSTRING with nested functions
        substring_pattern = r'SUBSTRING\s*\('
        substring_matches = list(re.finditer(substring_pattern, normalized, re.IGNORECASE))
        for match in reversed(substring_matches):
            start_pos = match.end()
            args_expr, end_pos = extract_nested_parens(normalized, start_pos - 1)
            if args_expr:
                # Split arguments (handling nested parentheses)
                parts = []
                current_part = ''
                paren_depth = 0
                for char in args_expr:
                    if char == '(':
                        paren_depth += 1
                        current_part += char
                    elif char == ')':
                        paren_depth -= 1
                        current_part += char
                    elif char == ',' and paren_depth == 0:
                        parts.append(current_part.strip())
                        current_part = ''
                    else:
                        current_part += char
                if current_part.strip():
                    parts.append(current_part.strip())

                # Process first argument (may be nested function)
                if parts:
                    first_arg = parts[0]
                    if re.search(r'\b(UPPER|LOWER|SUBSTRING|CONCAT)\s*\(', first_arg, re.IGNORECASE):
                        processed_arg = self._apply_coalesce_to_nested(first_arg)
                        parts[0] = wrap_string_arg(processed_arg)
                    else:
                        parts[0] = wrap_string_arg(first_arg)

                    # Rebuild SUBSTRING with wrapped first argument
                    wrapped_args = ', '.join(parts)
                    normalized = normalized[:start_pos] + wrapped_args + normalized[end_pos-1:]

        # Handle CONCAT with nested functions
        concat_pattern = r'CONCAT\s*\('
        concat_matches = list(re.finditer(concat_pattern, normalized, re.IGNORECASE))
        for match in reversed(concat_matches):
            start_pos = match.end()
            args_expr, end_pos = extract_nested_parens(normalized, start_pos - 1)
            if args_expr:
                # Split arguments (handling nested parentheses)
                parts = []
                current_part = ''
                paren_depth = 0
                for char in args_expr:
                    if char == '(':
                        paren_depth += 1
                        current_part += char
                    elif char == ')':
                        paren_depth -= 1
                        current_part += char
                    elif char == ',' and paren_depth == 0:
                        parts.append(current_part.strip())
                        current_part = ''
                    else:
                        current_part += char
                if current_part.strip():
                    parts.append(current_part.strip())

                # Process each argument (may be nested function)
                wrapped_parts = []
                for part in parts:
                    if re.search(r'\b(UPPER|LOWER|SUBSTRING|CONCAT)\s*\(', part, re.IGNORECASE):
                        processed_part = self._apply_coalesce_to_nested(part)
                        wrapped_parts.append(wrap_string_arg(processed_part))
                    else:
                        wrapped_parts.append(wrap_string_arg(part))

                # Rebuild CONCAT with wrapped arguments
                wrapped_args = ', '.join(wrapped_parts)
                normalized = normalized[:start_pos] + wrapped_args + normalized[end_pos-1:]

        return normalized

    def _map_column_names_in_expression(self, expression: str, available_columns: list) -> str:
        """
        Map column names in expression to actual database column names.
        Handles cases where expression uses underscores but actual column has spaces.
        Example: execution_order -> "execution order"
        """

        # Create a mapping: normalized column names -> actual column name
        column_map = {}
        for col in available_columns:
            col_name = col if isinstance(col, str) else (col.get('name') or str(col))
            # Normalized key: lowercase, spaces -> underscores
            normalized_key = col_name.lower().replace(' ', '_')
            column_map[normalized_key] = col_name
            # Also map exact match (case-insensitive)
            column_map[col_name.lower()] = col_name

        # Replace column references in expression
        # Handle bracketed: [column name] -> "column name"
        # Handle quoted: "column name" -> keep as-is
        # Handle regular: column_name -> "column name" (if mapping exists)

        def replace_column(match):
            col_ref = match.group(0)
            # If already quoted/bracketed, return as-is
            if col_ref.startswith('[') and col_ref.endswith(']'):
                inner = col_ref[1:-1]
                # Check if inner name needs mapping
                normalized = inner.lower().replace(' ', '_')
                if normalized in column_map:
                    actual_col = column_map[normalized]
                    return f'"{actual_col}"'
                return col_ref

            if (col_ref.startswith('"') and col_ref.endswith('"')) or (col_ref.startswith("'") and col_ref.endswith("'")):
                return col_ref  # Keep quoted format

            # Try to find mapping
            normalized = col_ref.lower().replace(' ', '_')
            if normalized in column_map:
                actual_col = column_map[normalized]
                # Quote if it has spaces or special characters
                if ' ' in actual_col or not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', actual_col):
                    return f'"{actual_col}"'
                return actual_col

            # No mapping found, return as-is
            return col_ref

        # Replace column names (word boundaries, but handle brackets/quotes)
        # Process from end to preserve positions
        result = expression
        function_names = ['UPPER', 'LOWER', 'CONCAT', 'SUBSTRING', 'TRIM', 'COALESCE', 'CAST', 'IF', 'CASE', 'DATEADD', 'DATEDIFF']

        # Pattern to match: [bracketed], "quoted", 'quoted', or word identifiers
        pattern = r'\[[^\]]+\]|"[^"]+"|\'[^\']+\'|\b[a-zA-Z_][a-zA-Z0-9_]*\b'

        matches = list(re.finditer(pattern, result))
        for match in reversed(matches):  # Process from end to preserve positions
            col_ref = match.group(0)
            # Skip if it's a function name
            if col_ref.upper() in function_names:
                continue
            # Replace with mapped column name
            replacement = replace_column(match)
            if replacement != col_ref:
                result = result[:match.start()] + replacement + result[match.end():]

        return result

    def _normalize_calculated_expression(self, expression: str, available_columns: list) -> str:
        """
        Normalize calculated column expressions for SQL generation.
        Wraps string function arguments with COALESCE to handle NULL values.

        Examples:
        CONCAT(a, b, c) -> CONCAT(COALESCE(a, ''), COALESCE(b, ''), COALESCE(c, ''))
        UPPER(column) -> UPPER(COALESCE(column, ''))
        LOWER(column) -> LOWER(COALESCE(column, ''))
        SUBSTRING(column, 1, 5) -> SUBSTRING(COALESCE(column, ''), 1, 5)
        """

        logger.info(f"[Calculated Column] Original expression: {expression}")

        def extract_nested_parens(expr: str, start_pos: int) -> tuple:
            """Extract content between parentheses, handling nesting"""
            if start_pos >= len(expr) or expr[start_pos] != '(':
                return None, start_pos
            start_pos += 1
            content = ''
            depth = 1
            pos = start_pos
            while pos < len(expr) and depth > 0:
                if expr[pos] == '(':
                    depth += 1
                elif expr[pos] == ')':
                    depth -= 1
                    if depth == 0:
                        return content, pos + 1
                content += expr[pos]
                pos += 1
            return content, pos

        def wrap_string_arg(arg: str) -> str:
            """Wrap a string function argument with COALESCE if needed"""
            arg = arg.strip()
            if not arg:
                return "''"
            # Skip if already wrapped
            if arg.upper().startswith('COALESCE'):
                return arg
            # String literals - wrap with COALESCE
            if arg.startswith("'") or arg.startswith('"'):
                return f"COALESCE({arg}, '')"
            # Column names or expressions - wrap with COALESCE
            return f"COALESCE({arg}, '')"

        # Parse nested expression to handle nested functions recursively
        execution_stack = self._parse_nested_expression(expression)

        if not execution_stack:
            # Simple expression, use old method
            normalized = expression
        else:
            # Build SQL recursively from execution stack
            normalized = self._build_nested_sql(execution_stack, expression)

        # Apply COALESCE wrapping to string function arguments (handles nested functions)
        normalized = self._apply_coalesce_to_nested(normalized)

        # Legacy handling for backward compatibility (if no nested functions detected)
        if not execution_stack:
            # Handle UPPER(column) -> UPPER(COALESCE(column, ''))
            upper_pattern = r'UPPER\s*\('
            upper_matches = list(re.finditer(upper_pattern, normalized, re.IGNORECASE))
            for match in reversed(upper_matches):  # Process from end to preserve positions
                start_pos = match.end()
                arg_expr, end_pos = extract_nested_parens(normalized, start_pos - 1)
                if arg_expr:
                    wrapped_arg = wrap_string_arg(arg_expr)
                    normalized = normalized[:start_pos] + wrapped_arg + normalized[end_pos-1:]
                    logger.debug(f"[Calculated Column] UPPER transformation: {arg_expr} -> {wrapped_arg}")

        # Handle LOWER(column) -> LOWER(COALESCE(column, ''))
        lower_pattern = r'LOWER\s*\('
        lower_matches = list(re.finditer(lower_pattern, normalized, re.IGNORECASE))
        for match in reversed(lower_matches):
            start_pos = match.end()
            arg_expr, end_pos = extract_nested_parens(normalized, start_pos - 1)
            if arg_expr:
                wrapped_arg = wrap_string_arg(arg_expr)
                normalized = normalized[:start_pos] + wrapped_arg + normalized[end_pos-1:]
                logger.debug(f"[Calculated Column] LOWER transformation: {arg_expr} -> {wrapped_arg}")

        # Handle SUBSTRING(column, start, length) -> SUBSTRING(COALESCE(column, ''), start, length)
        substring_pattern = r'SUBSTRING\s*\('
        substring_matches = list(re.finditer(substring_pattern, normalized, re.IGNORECASE))
        for match in reversed(substring_matches):
            start_pos = match.end()
            args_expr, end_pos = extract_nested_parens(normalized, start_pos - 1)
            if args_expr:
                # Split by comma, but only the first argument needs COALESCE
                parts = []
                current_part = ''
                paren_depth = 0
                for char in args_expr:
                    if char == '(':
                        paren_depth += 1
                        current_part += char
                    elif char == ')':
                        paren_depth -= 1
                        current_part += char
                    elif char == ',' and paren_depth == 0:
                        parts.append(current_part.strip())
                        current_part = ''
                    else:
                        current_part += char
                if current_part:
                    parts.append(current_part.strip())

                if parts:
                    # Wrap first argument (the column) with COALESCE
                    parts[0] = wrap_string_arg(parts[0])
                    normalized = normalized[:start_pos] + ', '.join(parts) + normalized[end_pos-1:]
                    logger.debug(f"[Calculated Column] SUBSTRING transformation: first arg -> {parts[0]}")

        # Handle CONCAT(a, b, c) -> CONCAT(COALESCE(a, ''), COALESCE(b, ''), COALESCE(c, ''))
        concat_pattern = r'CONCAT\s*\('
        concat_matches = list(re.finditer(concat_pattern, normalized, re.IGNORECASE))
        for match in reversed(concat_matches):
            start_pos = match.end()
            args_str, end_pos = extract_nested_parens(normalized, start_pos - 1)
            if args_str:
                # Split arguments by comma, respecting nested parentheses
                args = []
                current_arg = ''
                paren_depth = 0

                for char in args_str:
                    if char == '(':
                        paren_depth += 1
                        current_arg += char
                    elif char == ')':
                        paren_depth -= 1
                        current_arg += char
                    elif char == ',' and paren_depth == 0:
                        arg = current_arg.strip()
                        if arg:
                            args.append(wrap_string_arg(arg))
                        current_arg = ''
                    else:
                        current_arg += char

                # Add the last argument
                if current_arg.strip():
                    args.append(wrap_string_arg(current_arg.strip()))

                # Rebuild CONCAT with wrapped arguments
                wrapped_args = ', '.join(args)
                normalized = normalized[:start_pos] + wrapped_args + normalized[end_pos-1:]
                logger.debug(f"[Calculated Column] CONCAT transformation: {len(args)} arguments wrapped")

        logger.info(f"[Calculated Column] Transformed SQL: {normalized}")
        return normalized

    def _evaluate_calculated_column(self, expression: str, row: dict, available_columns: list, debug_steps=None):
        """
        Evaluate a calculated column expression using row data with nested function support.
        Returns (result, debug_steps) where debug_steps contains intermediate evaluation results.
        """

        if debug_steps is None:
            debug_steps = []

        try:
            # Parse nested expression into execution stack
            execution_stack = self._parse_nested_expression(expression)

            if not execution_stack:
                # Simple expression (no functions), evaluate directly
                eval_expr = expression
                # CRITICAL FIX: Handle quoted column names in normalized expression
                # Expression might have: "pricing type" or 'pricing type' or [pricing type]
                # IMPORTANT: Row keys might be different from available_columns format
                # We need to find the actual row key that matches each column

                # Build a mapping: column name -> actual row key (case-insensitive)
                row_keys_map = {}
                row_keys_lower = {k.lower(): k for k in row.keys()}
                for col in available_columns:
                    # Try exact match first
                    if col in row:
                        row_keys_map[col] = col
                    # Try case-insensitive match
                    elif col.lower() in row_keys_lower:
                        row_keys_map[col] = row_keys_lower[col.lower()]
                    else:
                        # Column not found in row - will use None
                        row_keys_map[col] = None

                for col in available_columns:
                    # Get the actual row key (might be different case/format)
                    actual_row_key = row_keys_map.get(col)
                    if actual_row_key:
                        col_value = row.get(actual_row_key)
                    else:
                        col_value = None

                    # Log if column not found (for debugging)
                    if actual_row_key is None and col.lower() in expression.lower():
                        logger.warning(f"[Eval] Column '{col}' not found in row keys: {list(row.keys())[:10]}...")

                    if col_value is None:
                        col_value = ''  # Default to empty string for NULL

                    # Format value for SQL replacement
                    if isinstance(col_value, str):
                        col_value_escaped = col_value.replace("'", "''")
                        sql_value = f"'{col_value_escaped}'"
                    elif isinstance(col_value, (int, float)):
                        sql_value = str(col_value)
                    elif isinstance(col_value, bool):
                        sql_value = "TRUE" if col_value else "FALSE"
                    else:
                        sql_value = f"'{col_value!s}'"

                    # Replace column references in multiple formats:
                    # 1. Quoted: "pricing type" or 'pricing type'
                    # 2. Bracketed: [pricing type]
                    # 3. Unquoted: pricing_type (if matches)
                    escaped_col = re.escape(col)

                    # Replace quoted column names: "column name" or 'column name'
                    eval_expr = re.sub(
                        r'["\']{escaped_col}["\']',
                        sql_value,
                        eval_expr,
                        flags=re.IGNORECASE
                    )

                    # Replace bracketed column names: [column name]
                    eval_expr = re.sub(
                        rf'\[{escaped_col}\]',
                        sql_value,
                        eval_expr,
                        flags=re.IGNORECASE
                    )

                    # Replace unquoted column names (word boundaries)
                    eval_expr = re.sub(
                        rf'\b{escaped_col}\b',
                        sql_value,
                        eval_expr,
                        flags=re.IGNORECASE
                    )

                result = eval_expr.strip().strip("'\"")
                return result, debug_steps

            # Evaluate execution stack (deepest functions first)
            intermediate_results = {}

            for step in execution_stack:
                fn_name = step['fn']
                args = step['args']

                # Resolve arguments (may be intermediate results from previous steps)
                resolved_args = []
                for arg in args:
                    if arg['type'] == 'function':
                        # This is a nested function result - get from intermediate_results
                        # Use a key based on function signature
                        arg_key = f"{arg['fn']}({','.join([str(a.get('value', '')) for a in arg.get('args', [])])})"
                        if arg_key in intermediate_results:
                            resolved_args.append(intermediate_results[arg_key])
                        else:
                            resolved_args.append('')
                    else:
                        # Literal value - resolve column references
                        # CRITICAL FIX: Handle quoted column names in normalized expression
                        # Use the same row key mapping logic as above
                        arg_value = arg['value']

                        # DEBUGGING: Log before replacement
                        if step['depth'] == 0:  # Only log top-level for readability
                            logger.debug(f"[Eval] Resolving literal arg: {arg_value}")

                        # Build row key mapping (reuse same logic as above)
                        row_keys_map = {}
                        row_keys_lower = {k.lower(): k for k in row.keys()}
                        for col in available_columns:
                            if col in row:
                                row_keys_map[col] = col
                            elif col.lower() in row_keys_lower:
                                row_keys_map[col] = row_keys_lower[col.lower()]
                            else:
                                row_keys_map[col] = None

                        for col in available_columns:
                            # Get the actual row key (might be different case/format)
                            actual_row_key = row_keys_map.get(col)
                            if actual_row_key:
                                col_value = row.get(actual_row_key)
                            else:
                                col_value = None

                            if col_value is None:
                                col_value = ''  # Default to empty string for NULL

                            # Format value for replacement
                            if isinstance(col_value, str):
                                col_value_str = col_value
                            else:
                                col_value_str = str(col_value)

                            escaped_col = re.escape(col)

                            # Replace quoted column names: "column name" or 'column name'
                            arg_value = re.sub(
                                r'["\']{escaped_col}["\']',
                                col_value_str,
                                arg_value,
                                flags=re.IGNORECASE
                            )

                            # Replace bracketed column names: [column name]
                            arg_value = re.sub(
                                rf'\[{escaped_col}\]',
                                col_value_str,
                                arg_value,
                                flags=re.IGNORECASE
                            )

                            # Replace unquoted column names (word boundaries)
                            arg_value = re.sub(
                                rf'\b{escaped_col}\b',
                                col_value_str,
                                arg_value,
                                flags=re.IGNORECASE
                            )

                        # DEBUGGING: Log after replacement
                        if step['depth'] == 0 and arg_value != arg['value']:
                            logger.debug(f"[Eval] After column replacement: {arg['value']} -> {arg_value}")

                        resolved_args.append(arg_value)

                # Evaluate function
                input_value = resolved_args[0] if resolved_args else ''
                output_value = None

                if fn_name == 'UPPER':
                    output_value = str(input_value).upper()
                    debug_steps.append({
                        'stage': 'UPPER',
                        'input': input_value,
                        'output': output_value,
                        'depth': step['depth']
                    })
                elif fn_name == 'LOWER':
                    output_value = str(input_value).lower()
                    debug_steps.append({
                        'stage': 'LOWER',
                        'input': input_value,
                        'output': output_value,
                        'depth': step['depth']
                    })
                elif fn_name == 'SUBSTRING':
                    if len(resolved_args) >= 2:
                        start = int(resolved_args[1]) if resolved_args[1].isdigit() else 1
                        length = int(resolved_args[2]) if len(resolved_args) >= 3 and resolved_args[2].isdigit() else None
                        s = str(input_value)
                        # SQL SUBSTRING is 1-indexed
                        if length:
                            output_value = s[start-1:start-1+length] if start > 0 and start <= len(s) else ''
                        else:
                            output_value = s[start-1:] if start > 0 and start <= len(s) else ''
                    else:
                        output_value = str(input_value)
                    debug_steps.append({
                        'stage': 'SUBSTRING',
                        'input': input_value,
                        'output': output_value,
                        'args': resolved_args[1:] if len(resolved_args) > 1 else [],
                        'depth': step['depth']
                    })
                elif fn_name == 'CONCAT':
                    output_value = ''.join([str(a) for a in resolved_args])
                    debug_steps.append({
                        'stage': 'CONCAT',
                        'input': resolved_args,
                        'output': output_value,
                        'depth': step['depth']
                    })
                else:
                    # Unknown function, return first argument
                    output_value = str(input_value)

                # Store intermediate result
                step_key = f"{fn_name}({','.join([str(a) for a in resolved_args[:3]])})"
                intermediate_results[step_key] = output_value

            # Final result is the last step's output
            final_result = list(intermediate_results.values())[-1] if intermediate_results else None

            return final_result, debug_steps

        except Exception as e:
            logger.error(f"[Calculated Column] Error evaluating expression '{expression}': {e!s}")
            debug_steps.append({
                'stage': 'ERROR',
                'input': expression,
                'output': None,
                'error': str(e)
            })
            return None, debug_steps

    def post(self, request):
        """
        Execute a pipeline query from nodes and edges.
        Body:
        - nodes: List of pipeline nodes (required)
        - edges: List of pipeline edges (required)
        - targetNodeId: ID of the node to execute up to (required)
        - page: Page number (default 1)
        - page_size: Number of rows per page (default 50)
        - previewMode: Boolean, if True uses single-query compilation (default True for preview)
        """
        try:
            # Get customer for the authenticated user
            user = request.user
            customer = ensure_user_has_customer(user)

            nodes = request.data.get('nodes', [])
            edges = request.data.get('edges', [])
            target_node_id = request.data.get('targetNodeId')
            page = int(request.data.get('page', 1))
            page_size = int(request.data.get('page_size', 50))
            raw_preview_mode = request.data.get('previewMode', True)
            # Normalize preview mode:
            # - "input"  => explicit input preview
            # - False    => preview disabled (full execution path)
            # - anything else/truthy => default output preview
            if raw_preview_mode == "input":
                preview_mode = "input"
            elif raw_preview_mode is False:
                preview_mode = "disabled"
            else:
                preview_mode = "output"

            # Log incoming request
            logger.info("=" * 60)
            logger.info("PipelineQueryExecutionView: NEW REQUEST")
            logger.info(f"  User: {user.email}, Customer DB: {customer.cust_db}")
            logger.info(f"  Target Node ID: {target_node_id}")
            logger.info(f"  Nodes count: {len(nodes)}, Edges count: {len(edges)}")

            if not nodes or not isinstance(nodes, list):
                return Response(
                    {"error": "nodes must be a non-empty array"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if not isinstance(edges, list):
                return Response(
                    {"error": "edges must be an array"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if not target_node_id:
                return Response(
                    {"error": "targetNodeId is required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Find target node (selected canvas node = preview target; do NOT substitute with source unless user is previewing a source node).
            # Be defensive: skip any malformed items that are not dict-like.
            target_node = next(
                (n for n in nodes if isinstance(n, dict) and n.get('id') == target_node_id),
                None,
            )
            if not target_node:
                return Response(
                    {"error": f"Target node {target_node_id} not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Determine target node type
            target_node_data = target_node.get('data', {})
            target_node_type = target_node_data.get('type')

            # Validate edges based on node type
            # Source nodes don't need input edges, but all other nodes do
            if target_node_type != 'source':
                if not edges or len(edges) == 0:
                    return Response(
                        {
                            "error": f"Node type '{target_node_type}' requires at least one input edge",
                            "details": f"The {target_node_type} node must be connected to an input node via an edge"
                        },
                        status=status.HTTP_400_BAD_REQUEST
                    )

            # Validate DAG (no cycles) before compiling/executing; ignore edges to deleted nodes
            if nodes and edges:
                cleaned_edges = strip_orphaned_edges(nodes, edges)
                is_valid_dag, dag_error = validate_dag(nodes, cleaned_edges)
                if not is_valid_dag and dag_error:
                    return Response(
                        {"error": f"Pipeline has a cycle or invalid structure: {dag_error}"},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                edges = cleaned_edges  # use cleaned edges for rest of request

            # ============================================================
            # PREVIEW MODE  (previewMode = "output" | "input")
            # ============================================================
            # Single path for all node types.  Flow:
            #   1. Check checkpoint cache → return rows directly if hit
            #   2. Resolve source credentials (needed by SQLCompiler + executor)
            #   3. Compile the pipeline DAG to a CTE-based SQL query
            #   4. Execute the SQL against the source DB
            #   5. For compute nodes: run the Python code on the SQL result
            #   6. Save a new checkpoint for expensive node types
            # ============================================================
            if preview_mode in ("output", "input"):
                from api.services.checkpoint_cache import CheckpointCacheManager
                from api.pipeline.preview_compiler import SQLCompiler
                from api.utils.db_executor import execute_preview_query

                canvas_id = request.data.get('canvasId')
                has_canvas_id = bool(canvas_id and str(canvas_id).strip())
                canvas_id = str(canvas_id) if has_canvas_id else "preview_unsaved"
                use_cache = request.data.get('useCache', True) if has_canvas_id else False
                force_refresh = request.data.get('forceRefresh', False)

                checkpoint_mgr = CheckpointCacheManager(customer.cust_db, canvas_id)

                # ── 1. Checkpoint cache lookup ───────────────────────────────
                if use_cache and not force_refresh:
                    ancestor_id, checkpoint = checkpoint_mgr.find_nearest_checkpoint(
                        target_node_id, nodes, edges
                    )
                    if ancestor_id == target_node_id and checkpoint:
                        logger.info("[PREVIEW] Checkpoint HIT for %s", target_node_id)
                        sql = f'SELECT * FROM {checkpoint["table_ref"]} LIMIT %s'
                        result = execute_preview_query(
                            sql, [page_size], {}, page, page_size,
                            customer_db=customer.cust_db,
                        )
                        rows = result if isinstance(result, list) else result.get("rows", [])
                        col_names = [
                            (c.get("name") or c.get("column") or str(c))
                            for c in (checkpoint.get("columns") or [])
                            if (c.get("name") or c.get("column") or isinstance(c, str))
                        ]
                        return Response({
                            "rows": rows,
                            "columns": col_names,
                            "has_more": False,
                            "total": len(rows),
                            "page": page,
                            "page_size": page_size,
                            "from_cache": True,
                            "preview_mode": preview_mode,
                        })
                else:
                    ancestor_id, checkpoint = None, None

                # ── 2. Resolve source credentials ────────────────────────────
                # GENERAL.source lives on the DEFAULT Django DB, not on the customer DB.
                # Use get_default_db_connection() — same as SQLCompiler._get_source_config.
                source_nodes = [n for n in nodes if isinstance(n, dict) and
                                n.get("data", {}).get("type") == "source"]
                if not source_nodes:
                    return Response({"error": "Pipeline must have at least one source node"},
                                    status=status.HTTP_400_BAD_REQUEST)

                first_source = next(
                    (n for n in source_nodes if isinstance(n.get("data"), dict)),
                    source_nodes[0],
                )
                source_id = first_source.get("data", {}).get("config", {}).get("sourceId")

                db_type = "postgresql"
                source_config: dict = {}
                if source_id:
                    from api.utils.db_connection import get_default_db_connection
                    _conn = get_default_db_connection()
                    _conn.autocommit = True
                    try:
                        with _conn.cursor() as _cur:
                            _cur.execute("""
                                SELECT column_name FROM information_schema.columns
                                WHERE table_schema = 'GENERAL' AND table_name = 'source'
                            """)
                            _avail = [r[0] for r in _cur.fetchall()]
                            _cfg_col = "source_config" if "source_config" in _avail else "src_config"
                            _cur.execute(
                                f'SELECT "{_cfg_col}", created_on FROM "GENERAL".source WHERE id = %s',
                                (source_id,),
                            )
                            _row = _cur.fetchone()
                            if _row:
                                from api.utils.helpers import decrypt_source_data
                                _dec = decrypt_source_data(_row[0], customer.cust_id, _row[1])
                                if _dec:
                                    source_config = _dec
                                    db_type = source_config.get("db_type", "postgresql")
                    finally:
                        _conn.close()

                # ── 3. Compile the pipeline DAG to SQL ───────────────────────
                try:
                    compiler = SQLCompiler(
                        nodes, edges, target_node_id, customer, db_type,
                        start_node_id=ancestor_id,
                        start_table_ref=checkpoint["table_ref"] if checkpoint else None,
                        initial_columns=checkpoint["columns"] if checkpoint else None,
                    )
                    sql_query, sql_params, output_metadata = compiler.compile()
                except Exception as compile_err:
                    logger.error("[PREVIEW] Compile error: %s", compile_err, exc_info=True)
                    return Response({"error": f"Pipeline compile failed: {compile_err!s}"},
                                    status=status.HTTP_400_BAD_REQUEST)

                # ── 4. Execute SQL against source DB ─────────────────────────
                if sql_params:
                    sql_params[-1] = page_size   # enforce page_size as LIMIT
                else:
                    sql_params = [page_size]
                    if "LIMIT" not in sql_query.upper():
                        sql_query += "\nLIMIT %s"

                try:
                    exec_result = execute_preview_query(
                        sql_query, sql_params, source_config, page, page_size
                    )
                except Exception as exec_err:
                    logger.error("[PREVIEW] Execute error: %s", exec_err, exc_info=True)
                    return Response({"error": f"Query execution failed: {exec_err!s}"},
                                    status=status.HTTP_500_INTERNAL_SERVER_ERROR)

                rows = exec_result if isinstance(exec_result, list) else exec_result.get("rows", [])
                out_cols = output_metadata.get("columns", [])

                # ── 4b. input-preview: return data before compute runs ────────
                if preview_mode == "input":
                    return Response({
                        "rows": rows[((page - 1) * page_size):(page * page_size)],
                        "columns": [c.get("name") for c in out_cols],
                        "has_more": len(rows) > page * page_size,
                        "total": len(rows),
                        "page": page,
                        "page_size": page_size,
                        "from_cache": False,
                        "preview_mode": "input",
                    })

                # ── 5. Compute node: run Python code on the SQL result ────────
                if target_node_type == "compute":
                    try:
                        import numpy as np
                        import pandas as pd
                        code = target_node_data.get("config", {}).get("code", "")
                        if not code.strip():
                            return Response({"error": "Compute node has no code configured"},
                                            status=status.HTTP_400_BAD_REQUEST)
                        input_df = pd.DataFrame(rows)
                        local_vars = {
                            "d": input_df, "_input_d": input_df,
                            "pd": pd, "np": np, "_output_d": None,
                        }
                        exec(code, {}, local_vars)  # noqa: S102
                        output_df = local_vars.get("_output_d") or local_vars.get("output_df")
                        if not isinstance(output_df, pd.DataFrame):
                            return Response(
                                {"error": "Compute node must assign a DataFrame to _output_d"},
                                status=status.HTTP_400_BAD_REQUEST,
                            )
                        MAX_ROWS = 100
                        if len(output_df) > MAX_ROWS:
                            output_df = output_df.head(MAX_ROWS)
                        output_df = output_df.replace({np.nan: None, np.inf: None, -np.inf: None})
                        rows = output_df.to_dict("records")
                        out_cols = [
                            {"name": col, "datatype": str(output_df[col].dtype)}
                            for col in output_df.columns
                        ]
                        if has_canvas_id:
                            checkpoint_mgr.save_checkpoint(
                                node_id=target_node_id, node_type="compute",
                                node_config=target_node_data.get("config", {}),
                                upstream_version_hash=None, columns=out_cols, rows=rows,
                            )
                    except Exception as compute_err:
                        logger.error("[PREVIEW] Compute error: %s", compute_err, exc_info=True)
                        return Response({"error": f"Compute execution failed: {compute_err!s}"},
                                        status=status.HTTP_400_BAD_REQUEST)

                # ── 6. Save checkpoint for expensive nodes ───────────────────
                elif has_canvas_id and checkpoint_mgr.is_checkpoint_node(target_node_type):
                    ok = checkpoint_mgr.save_checkpoint(
                        target_node_id, target_node_type,
                        target_node_data.get("config", {}),
                        None, out_cols,
                        rows=rows, sql_query=sql_query, sql_params=sql_params,
                    )
                    logger.info("[PREVIEW] Checkpoint %s for %s",
                                "saved" if ok else "FAILED", target_node_id)

                return Response({
                    "rows": rows[((page - 1) * page_size):(page * page_size)],
                    "columns": [c.get("name") for c in out_cols],
                    "has_more": len(rows) > page * page_size,
                    "total": len(rows),
                    "page": page,
                    "page_size": page_size,
                    "from_cache": False,
                    "preview_mode": "output",
                })

            # ============================================================
            # PRODUCTION MODE: Existing execution logic
            # ============================================================

            # Get canvas_id for caching (optional - if not provided, skip caching)
            canvas_id = request.data.get('canvasId')
            use_cache = request.data.get('useCache', True)  # Default to using cache
            force_refresh = request.data.get('forceRefresh', False)  # Force re-execution

            # ADAPTIVE CACHE DISABLED: Bypassing in favor of checkpoint caching
            adaptive_cache = None
            cached_node_ids = set()

            def save_to_adaptive_cache(
                node_id: str,
                node_type: str,
                node_config: dict,
                rows: list,
                columns: list,
                upstream_node_ids: Optional[list] = None,
                input_rows: int = 0,
                column_lineage: Optional[dict] = None
            ) -> bool:
                """
                Helper function to save node data to adaptive cache with proper decision logic.

                Returns:
                    True if cached, False otherwise
                """
                if not adaptive_cache or not canvas_id:
                    return False

                try:
                    # Compute caching metrics
                    depth = compute_depth_since_last_cache(node_id, nodes, edges, cached_node_ids)
                    fan_out = compute_fan_out(node_id, edges)
                    is_pushdown = is_filter_pushdown_candidate(node_id, nodes, edges, column_lineage or {}) if node_type == 'filter' else False

                    # Estimate row reduction (for filters)
                    output_rows = len(rows)
                    if node_type == 'filter' and input_rows > 0:
                        (input_rows - output_rows) / input_rows if input_rows > 0 else 0.0
                    else:
                        pass

                    # Decide caching
                    logger.debug(f"[CACHE DECISION] Evaluating cache for node {node_id} (type: {node_type}, depth: {depth}, fan_out: {fan_out}, input_rows: {input_rows}, output_rows: {output_rows}, pushdown: {is_pushdown})")

                    should_cache, cache_layer = adaptive_cache.should_cache(
                        node_id,
                        node_type,
                        node_config,
                        input_rows=input_rows,
                        output_rows=output_rows,
                        depth_since_last_cache=depth,
                        fan_out=fan_out,
                        is_pushdown_candidate=is_pushdown
                    )

                    if should_cache:
                        logger.info(f"[CACHE DECISION] ✅ Will cache node {node_id} in {cache_layer.value} layer")
                    else:
                        logger.debug(f"[CACHE DECISION] ❌ Will NOT cache node {node_id} (low priority or cost)")

                    if should_cache:
                        # Prepare column metadata
                        column_metadata = []
                        for col in columns:
                            if isinstance(col, dict):
                                column_metadata.append(col)
                            else:
                                column_metadata.append({'name': str(col), 'datatype': 'TEXT'})

                        # Get upstream node IDs and hashes
                        upstream_ids = upstream_node_ids or []
                        upstream_hashes = {}
                        for upstream_id in upstream_ids:
                            upstream_node = next((n for n in nodes if n.get('id') == upstream_id), None)
                            if upstream_node:
                                upstream_config = upstream_node.get('data', {}).get('config', {})
                                upstream_hashes[upstream_id] = adaptive_cache._compute_node_hash(upstream_id, upstream_config)

                        # Save to cache (V2: depth for cache_cost_score)
                        success = adaptive_cache.save_cache(
                            pipeline_id=str(canvas_id),
                            node_id=node_id,
                            node_type=node_type,
                            node_config=node_config,
                            rows=rows,
                            columns=column_metadata,
                            upstream_node_ids=upstream_ids,
                            upstream_hashes=upstream_hashes,
                            cache_layer=cache_layer,
                            input_rows=input_rows,
                            output_rows=output_rows,
                            depth_since_last_cache=depth,
                        )

                        if success:
                            logger.info(f"Cached {len(rows)} rows for {node_type} node {node_id} in {cache_layer.value} cache")
                        return success

                    return False

                except Exception as e:
                    logger.warning(f"Failed to save to adaptive cache for node {node_id}: {e}")
                    return False

            # Check cache if enabled
            # CRITICAL: For projection nodes with calculated columns, we need to reprocess them
            # even when using cache, because calculated columns are evaluated in Python
            cached_base_rows = None
            cached_base_columns = None

            if canvas_id and use_cache and not force_refresh and adaptive_cache:
                try:
                    # Compute node version hash
                    target_node_config = target_node_data.get('config', {})
                    node_version_hash = adaptive_cache._compute_node_hash(target_node_id, target_node_config)

                    # Get upstream node IDs and hashes
                    upstream_node_ids = []
                    upstream_hashes = {}
                    for edge in edges:
                        if edge.get('target') == target_node_id:
                            upstream_id = edge.get('source')
                            upstream_node_ids.append(upstream_id)
                            upstream_node = next((n for n in nodes if n.get('id') == upstream_id), None)
                            if upstream_node:
                                upstream_config = upstream_node.get('data', {}).get('config', {})
                                upstream_hashes[upstream_id] = adaptive_cache._compute_node_hash(upstream_id, upstream_config)

                    upstream_version_hash = adaptive_cache._compute_upstream_hash(upstream_node_ids, upstream_hashes)

                    cached_data = adaptive_cache.get_cache(
                        str(canvas_id),
                        target_node_id,
                        node_version_hash,
                        upstream_version_hash
                    )

                    if cached_data:
                        # Get current calculated columns from request to check if we need to reprocess
                        target_node = next((n for n in nodes if n.get('id') == target_node_id), None)
                        current_calculated_columns = []
                        if target_node and target_node.get('data', {}).get('type') == 'projection':
                            current_config = target_node.get('data', {}).get('config', {})
                            current_calculated_columns = current_config.get('calculatedColumns', [])

                        # If calculated columns exist, we need to reprocess them on top of cached base data
                        # Otherwise, use cache as-is
                        if current_calculated_columns and len(current_calculated_columns) > 0:
                            logger.info("Cache hit but calculated columns detected - will reprocess calculated columns on cached base data")
                            # Store cached base rows (without calculated columns) for reprocessing
                            cached_base_rows = cached_data.get('rows', [])
                            cached_base_columns = cached_data.get('columns', [])
                            # Remove calculated columns from cached columns to get base columns only
                            calc_col_names = [cc.get('name', '') for cc in current_calculated_columns if cc.get('name')]
                            cached_base_columns = [col for col in cached_base_columns if col not in calc_col_names]
                            # Remove calculated columns from cached rows
                            for row in cached_base_rows:
                                for calc_name in calc_col_names:
                                    row.pop(calc_name, None)
                        else:
                            # No calculated columns, safe to use cache as-is
                            logger.info(f"Cache hit for node {target_node_id} in canvas {canvas_id}")
                            return Response({
                                "rows": cached_data.get('rows', [])[:page_size],
                                "columns": cached_data.get('columns', []),
                                "has_more": len(cached_data.get('rows', [])) > page_size,
                                "total": cached_data['metadata'].get('row_count', len(cached_data.get('rows', []))),
                                "page": page,
                                "page_size": page_size,
                                "from_cache": True,
                                "cached_on": cached_data['metadata'].get('cached_on')
                            }, status=status.HTTP_200_OK)
                except Exception as cache_e:
                    logger.warning(f"Cache check failed, proceeding with execution: {cache_e}")

            # ------------------------------------------------------------------
            # JOIN execution (including Filter-after-Join)
            # ------------------------------------------------------------------
            # Normalise to a join node + optional filters, whether the target is:
            # - the join node itself, or
            # - a filter node whose parent is a join node.
            join_node = None
            join_config = {}
            join_type = 'INNER'
            conditions = []
            filters = None

            if target_node_type == 'join':
                # Direct join node execution
                join_node = target_node
                join_config = target_node_data.get('config', {})
                join_type = join_config.get('joinType', 'INNER')
                raw_conditions = join_config.get('conditions', [])

                # Validate and format conditions for FastAPI
                conditions = []
                for cond in raw_conditions:
                    if isinstance(cond, dict):
                        # Ensure required fields exist
                        left_col = cond.get('leftColumn') or cond.get('left_column')
                        right_col = cond.get('rightColumn') or cond.get('right_column')
                        operator = cond.get('operator', '=')

                        if left_col and right_col:
                            conditions.append({
                                'leftColumn': left_col,
                                'rightColumn': right_col,
                                'operator': operator
                            })
                        else:
                            logger.warning(f"[Join Execution] Skipping invalid condition: {cond}")
                    else:
                        logger.warning(f"[Join Execution] Invalid condition format (not a dict): {cond}")

                filters = join_config.get('filters')  # Optional filters defined on join node

                # CRITICAL: Validate join conditions for non-CROSS joins
                if join_type != 'CROSS' and len(conditions) == 0:
                    logger.error(f"[Join Execution] Join node {target_node_id} has joinType={join_type} but no valid conditions")
                    return Response(
                        {"error": f"Join conditions are required for {join_type} JOIN. Please configure join conditions in the Join node."},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                logger.info(f"[Join Execution] Validated {len(conditions)} conditions from {len(raw_conditions)} raw conditions")
                logger.info(f"[Join Execution] Join config: joinType={join_type}, conditions={conditions}")

            elif target_node_type == 'filter':
                # Filter node - check if its parent is a join node
                input_edge = next((e for e in edges if e.get('target') == target_node_id), None)
                if input_edge:
                    parent_node = next((n for n in nodes if n.get('id') == input_edge.get('source')), None)
                else:
                    parent_node = None

                if parent_node and parent_node.get('data', {}).get('type') == 'join':
                    # Treat as "join + filters from this filter node"
                    join_node = parent_node
                    join_config = parent_node.get('data', {}).get('config', {})
                    join_type = join_config.get('joinType', 'INNER')
                    raw_conditions = join_config.get('conditions', [])

                    # CRITICAL: Validate join conditions for non-CROSS joins
                    if join_type != 'CROSS' and (not raw_conditions or len(raw_conditions) == 0):
                        logger.error(f"[Join Execution] Join node {parent_node.get('id')} (parent of filter {target_node_id}) has joinType={join_type} but no conditions")
                        return Response(
                            {"error": f"Join conditions are required for {join_type} JOIN. Please configure join conditions in the Join node."},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                    # Validate and format conditions for FastAPI
                    conditions = []
                    for cond in raw_conditions:
                        if isinstance(cond, dict):
                            # Ensure required fields exist
                            left_col = cond.get('leftColumn') or cond.get('left_column')
                            right_col = cond.get('rightColumn') or cond.get('right_column')
                            operator = cond.get('operator', '=')

                            if left_col and right_col:
                                conditions.append({
                                    'leftColumn': left_col,
                                    'rightColumn': right_col,
                                    'operator': operator
                                })
                            else:
                                logger.warning(f"[Join Execution] Skipping invalid condition: {cond}")
                        else:
                            logger.warning(f"[Join Execution] Invalid condition format (not a dict): {cond}")

                    # CRITICAL: Validate join conditions for non-CROSS joins after processing
                    if join_type != 'CROSS' and len(conditions) == 0:
                        logger.error(f"[Join Execution] Join node {parent_node.get('id')} (parent of filter {target_node_id}) has joinType={join_type} but no valid conditions after processing")
                        return Response(
                            {"error": f"Join conditions are required for {join_type} JOIN. Please configure valid join conditions in the Join node."},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                    logger.info(f"[Join Execution] Validated {len(conditions)} conditions from {len(raw_conditions)} raw conditions")

                    # Build filters spec from filter node conditions using reusable filter system
                    from api.pipeline.filter_builder import parse_filter_from_canvas
                    filter_config = target_node_data.get('config', {})
                    filter_conditions = filter_config.get('conditions', [])
                    canvas_filter = {'conditions': filter_conditions}
                    filters = parse_filter_from_canvas(canvas_filter)

            if join_node is not None:
                # Handle join (or join + filter) execution
                logger.info("PipelineQueryExecutionView: executing join pipeline")
                logger.info(f"  target_node_id={target_node_id}, target_type={target_node_type}")
                logger.info(f"  join_node_id={join_node.get('id')}, join_type={join_type}")
                logger.info(f"  join_conditions={conditions}")
                logger.info(f"  join_filters_spec={filters}")
                # Find left and right input nodes for the join node
                join_node_id = join_node.get('id')
                input_edges = [e for e in edges if e.get('target') == join_node_id]

                logger.info(f"[Join Execution] Resolving input edges for join node {join_node_id}")
                logger.info(f"[Join Execution] Total input edges: {len(input_edges)}")
                for idx, edge in enumerate(input_edges):
                    logger.info(f"[Join Execution]   Edge {idx}: source={edge.get('source')}, targetHandle={edge.get('targetHandle')}, sourceHandle={edge.get('sourceHandle')}")

                # CRITICAL: Find edges with explicit targetHandle
                # Must use targetHandle to correctly identify left vs right inputs
                left_edge = next((e for e in input_edges if e.get('targetHandle') == 'left'), None)
                right_edge = next((e for e in input_edges if e.get('targetHandle') == 'right'), None)

                logger.info(f"[Join Execution] After explicit handle lookup: left_edge={left_edge.get('source') if left_edge else 'None'}, right_edge={right_edge.get('source') if right_edge else 'None'}")

                # If no explicit handles, use order (first = left, second = right)
                # This is a fallback - frontend should always send targetHandle
                # CRITICAL: Ensure we never assign the same edge to both left and right
                if not left_edge and not right_edge:
                    # No explicit handles - use order-based assignment
                    if len(input_edges) >= 2:
                        logger.warning("[Join Execution] No explicit handles found, using order-based assignment (first=left, second=right)")
                        left_edge = input_edges[0]
                        right_edge = input_edges[1]
                        # CRITICAL: Verify that left and right edges point to different source nodes
                        if left_edge.get('source') == right_edge.get('source'):
                            logger.error(f"[Join Execution] Fallback assignment resulted in same source node: {left_edge.get('source')}")
                            return Response(
                                {"error": f"Invalid join configuration: Both input edges point to the same source node '{left_edge.get('source')}'. Join requires two different input nodes."},
                                status=status.HTTP_400_BAD_REQUEST
                            )
                    elif len(input_edges) == 1:
                        logger.error("[Join Execution] Only one edge found without explicit handles - cannot determine left vs right")
                        return Response(
                            {"error": "Join node requires two input connections with explicit left/right handles. Only one connection found."},
                            status=status.HTTP_400_BAD_REQUEST
                        )
                elif not left_edge:
                    # Right handle found but not left - find a different edge for left
                    logger.warning("[Join Execution] Right handle found but not left, searching for left edge")
                    left_edge = next((e for e in input_edges if e.get('source') != right_edge.get('source')), None)
                    if not left_edge:
                        logger.error("[Join Execution] Could not find a different edge for left handle")
                        return Response(
                            {"error": f"Join node requires two different input nodes. Right input is node '{right_edge.get('source')}', but could not find a different node for left input."},
                            status=status.HTTP_400_BAD_REQUEST
                        )
                elif not right_edge:
                    # Left handle found but not right - find a different edge for right
                    logger.warning("[Join Execution] Left handle found but not right, searching for right edge")
                    right_edge = next((e for e in input_edges if e.get('source') != left_edge.get('source')), None)
                    if not right_edge:
                        logger.error("[Join Execution] Could not find a different edge for right handle")
                        return Response(
                            {"error": f"Join node requires two different input nodes. Left input is node '{left_edge.get('source')}', but could not find a different node for right input."},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                if not left_edge or not right_edge:
                    logger.error(f"[Join Execution] Missing input edges: left={left_edge is not None}, right={right_edge is not None}")
                    return Response(
                        {"error": "Join node must have both left and right input connections"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Find left and right nodes
                left_node_id = left_edge.get('source')
                right_node_id = right_edge.get('source')
                left_node = next((n for n in nodes if n.get('id') == left_node_id), None)
                right_node = next((n for n in nodes if n.get('id') == right_node_id), None)

                logger.info(f"[Join Execution] Resolved nodes: left_node_id={left_node_id}, right_node_id={right_node_id}")
                logger.info(f"[Join Execution] Node lookup: left_node={left_node.get('id') if left_node else 'NOT FOUND'}, right_node={right_node.get('id') if right_node else 'NOT FOUND'}")

                if not left_node or not right_node:
                    logger.error(f"[Join Execution] Could not find nodes: left_node={left_node_id}, right_node={right_node_id}")
                    return Response(
                        {"error": "Could not find left or right input nodes for join"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # CRITICAL: Prevent self-joins (same node connected to both inputs)
                # Validation MUST use node_id comparison ONLY, NOT table names or lineage
                # Rule: Reject ONLY if left_node_id == right_node_id
                left_node_id_final = left_node.get('id')
                right_node_id_final = right_node.get('id')

                logger.info(f"[Join Execution] Self-join check: left_node_id={left_node_id_final}, right_node_id={right_node_id_final}")

                if left_node_id_final == right_node_id_final:
                    logger.error(f"[Join Execution] Invalid self-join detected: same node '{left_node_id_final}' connected to both left and right inputs of join node '{join_node_id}'")
                    logger.error(f"[Join Execution] Left edge: source={left_edge.get('source')}, targetHandle={left_edge.get('targetHandle')}")
                    logger.error(f"[Join Execution] Right edge: source={right_edge.get('source')}, targetHandle={right_edge.get('targetHandle')}")
                    return Response(
                        {"error": f"Invalid join configuration: Cannot connect the same node '{left_node.get('data', {}).get('label', left_node_id_final)}' (ID: {left_node_id_final}) to both left and right inputs of a join. Please connect two different nodes."},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                logger.info(f"[Join Execution] Self-join check PASSED: Different nodes ({left_node_id_final} vs {right_node_id_final})")

                # Helper function to get table info from a node (traverse back to source)
                # CRITICAL: Must traverse graph lineage, NOT use cached table names
                def get_table_info_from_node(node, visited=None):
                    """
                    Traverse back through the pipeline to find source table info.
                    Uses graph traversal (node IDs + edges) to ensure correct lineage.

                    Args:
                        node: The node to resolve
                        visited: Set of visited node IDs to prevent infinite loops

                    Returns:
                        Dict with source_id, table_name, schema, and source_node_id
                    """
                    if visited is None:
                        visited = set()

                    node_id = node.get('id')
                    if node_id in visited:
                        logger.warning(f"[Join Execution] Circular reference detected in node {node_id}")
                        return None
                    visited.add(node_id)

                    node_data = node.get('data', {})
                    node_type = node_data.get('type')
                    node_config = node_data.get('config', {})

                    # If it's a source node, return its table info directly
                    if node_type == 'source':
                        table_info = {
                            'source_id': node_config.get('sourceId'),
                            'table_name': node_config.get('tableName'),
                            'schema': node_config.get('schema', ''),
                            'source_node_id': node_id,  # Track the actual source node ID
                        }
                        logger.debug(f"[Join Execution] Resolved source node {node_id}: {table_info['table_name']}.{table_info['schema']}")
                        return table_info

                    # CRITICAL: For transform nodes (projection, filter, join, etc.), ALWAYS traverse back
                    # Do NOT use cached table_name from config - it may be stale or incorrect
                    # Find the input edge and recurse to the actual source
                    input_edge = next((e for e in edges if e.get('target') == node_id), None)
                    if input_edge:
                        input_node_id = input_edge.get('source')
                        input_node = next((n for n in nodes if n.get('id') == input_node_id), None)
                        if input_node:
                            logger.debug(f"[Join Execution] Traversing from {node_id} ({node_type}) to input node {input_node_id}")
                            result = get_table_info_from_node(input_node, visited)
                            if result:
                                # Preserve the source node ID from the actual source
                                result['source_node_id'] = result.get('source_node_id', input_node_id)
                            return result

                    # Fallback: If traversal fails, try config (but log warning)
                    if node_config.get('sourceId') and node_config.get('tableName'):
                        logger.warning(f"[Join Execution] Could not traverse graph for node {node_id}, using cached config (may be incorrect)")
                        return {
                            'source_id': node_config.get('sourceId'),
                            'table_name': node_config.get('tableName'),
                            'schema': node_config.get('schema', ''),
                            'source_node_id': node_id,  # Fallback: use current node ID
                        }

                    logger.error(f"[Join Execution] Could not resolve table info for node {node_id} (type: {node_type})")
                    return None

                # CRITICAL: Resolve table info using graph traversal (not cached table names)
                left_table_info = get_table_info_from_node(left_node)
                right_table_info = get_table_info_from_node(right_node)

                if not left_table_info or not right_table_info:
                    return Response(
                        {"error": "Could not determine table information for join inputs"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Log resolution for debugging
                logger.info("[Join Execution] Resolved LEFT input:")
                logger.info(f"  Node ID: {left_node.get('id')}")
                logger.info(f"  Table: {left_table_info.get('table_name')}.{left_table_info.get('schema')}")
                logger.info(f"  Source Node ID: {left_table_info.get('source_node_id')}")
                logger.info("[Join Execution] Resolved RIGHT input:")
                logger.info(f"  Node ID: {right_node.get('id')}")
                logger.info(f"  Table: {right_table_info.get('table_name')}.{right_table_info.get('schema')}")
                logger.info(f"  Source Node ID: {right_table_info.get('source_node_id')}")

                # CRITICAL: Validate that distinct inputs don't resolve to same table incorrectly
                # This is a safeguard against graph traversal bugs
                if (left_node.get('id') != right_node.get('id') and
                    left_table_info.get('table_name') == right_table_info.get('table_name') and
                    left_table_info.get('schema') == right_table_info.get('schema') and
                    left_table_info.get('source_node_id') == right_table_info.get('source_node_id')):
                    logger.error("[Join Execution] VALIDATION FAILED: Distinct input nodes resolved to same source table")
                    logger.error(f"  Left node: {left_node.get('id')} -> Source: {left_table_info.get('source_node_id')}")
                    logger.error(f"  Right node: {right_node.get('id')} -> Source: {right_table_info.get('source_node_id')}")
                    logger.error(f"  Both resolved to: {left_table_info.get('table_name')}.{left_table_info.get('schema')}")
                    return Response(
                        {"error": f"Invalid join resolution: Distinct input nodes resolved to the same source table '{left_table_info.get('table_name')}'. This indicates a graph traversal issue. Please check your pipeline connections."},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Use the same source_id for both tables (assuming they're from the same source)
                source_id = left_table_info['source_id']
                if source_id != right_table_info['source_id']:
                    # For now, use left source_id - in future, we might need to handle cross-source joins
                    logger.warning(f"Join between different sources: {source_id} and {right_table_info['source_id']}")

                # Connect to customer's database to get source connection
                from django.conf import settings
                import psycopg2

                conn = psycopg2.connect(
                    host=settings.DATABASES['default']['HOST'],
                    port=settings.DATABASES['default']['PORT'],
                    user=settings.DATABASES['default']['USER'],
                    password=settings.DATABASES['default']['PASSWORD'],
                    database=customer.cust_db
                )
                conn.autocommit = True
                db_cursor = conn.cursor()

                # Get source connection details
                db_cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'GENERAL' AND table_name = 'source'
                """)
                columns = [row[0] for row in db_cursor.fetchall()]
                config_column = 'source_config' if 'source_config' in columns else 'src_config'

                db_cursor.execute(f'''
                    SELECT {config_column}, created_on
                    FROM "GENERAL".source
                    WHERE id = %s
                ''', (source_id,))

                source_row = db_cursor.fetchone()
                if not source_row:
                    db_cursor.close()
                    conn.close()
                    return Response(
                        {"error": "Source connection not found"},
                        status=status.HTTP_404_NOT_FOUND
                    )

                source_config, created_on = source_row

                # Decrypt source configuration
                decrypted_config = decrypt_source_data(source_config, customer.cust_id, created_on)
                if not decrypted_config:
                    db_cursor.close()
                    conn.close()
                    return Response(
                        {"error": "Failed to decrypt source configuration"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )

                db_type = decrypted_config.get('db_type', '').lower()
                db_cursor.close()
                conn.close()

                # Call FastAPI service to execute join
                try:
                    import asyncio

                    import httpx

                    EXTRACTION_SERVICE_URL = getattr(settings, 'FASTAPI_EXTRACTION_SERVICE_URL', 'http://localhost:8001')

                    # CRITICAL: Handle self-joins STRICTLY by node identity, NOT table name
                    # Self-join detection must be based on:
                    # 1. Same input node ID (same node connected to both left and right)
                    # 2. Same source node ID (traversed back to same source)
                    # Table name equality alone is NOT sufficient for self-join detection
                    left_table_name = left_table_info['table_name']
                    right_table_name = right_table_info['table_name']
                    left_schema = left_table_info.get('schema', '')
                    right_schema = right_table_info.get('schema', '')
                    left_node_id = left_node.get('id')
                    right_node_id = right_node.get('id')
                    left_source_node_id = left_table_info.get('source_node_id')
                    right_source_node_id = right_table_info.get('source_node_id')

                    # CRITICAL: Self-join ONLY if the same node is connected to both inputs
                    # This is the ONLY valid self-join scenario
                    is_self_join = (left_node_id == right_node_id)

                    # Additional validation: If same source node but different input nodes, log warning
                    if (left_source_node_id == right_source_node_id and
                        left_node_id != right_node_id and
                        left_table_name == right_table_name and
                        left_schema == right_schema):
                        logger.info(f"[Join Execution] Different input nodes ({left_node_id} vs {right_node_id}) from same source ({left_source_node_id}) and table ({left_table_name}.{left_schema})")
                        logger.info("[Join Execution] This is NOT a self-join - using normal join logic")

                    if is_self_join:
                        logger.info(f"[Join Execution] TRUE self-join detected: same input node ({left_node_id}) connected to both left and right")
                        logger.info(f"[Join Execution] Self-join table: {left_table_name}.{left_schema}")
                    else:
                        logger.info(f"[Join Execution] Normal join: LEFT node {left_node_id} ({left_table_name}.{left_schema}) JOIN RIGHT node {right_node_id} ({right_table_name}.{right_schema})")

                    # Generate table aliases for self-joins
                    left_table_alias = 't1' if is_self_join else None
                    right_table_alias = 't2' if is_self_join else None

                    # Modify conditions to include __L__ and __R__ prefixes for column identification
                    # This allows the backend to validate columns exist in the correct tables
                    conditions_with_aliases = []
                    for cond in conditions:
                        left_col = cond.get('leftColumn', '')
                        right_col = cond.get('rightColumn', '')
                        operator = cond.get('operator', '=')

                        # Add __L__ and __R__ prefixes to column names
                        # This ensures the backend knows which table each column belongs to
                        left_col_prefixed = f"__L__.{left_col}" if left_col and not left_col.startswith('__L__.') else left_col
                        right_col_prefixed = f"__R__.{right_col}" if right_col and not right_col.startswith('__R__.') else right_col

                        conditions_with_aliases.append({
                            'leftColumn': left_col_prefixed,
                            'rightColumn': right_col_prefixed,
                            'operator': operator
                        })

                    if is_self_join:
                        logger.info(f"[Join Execution] Self-join detected: {left_table_name} = {right_table_name}")
                        logger.info(f"[Join Execution] Using aliases: {left_table_alias} (left), {right_table_alias} (right)")
                    else:
                        logger.info("[Join Execution] Using __L__ and __R__ prefixes for column identification")

                    # Extract outputColumns from join config for field selection and renaming
                    output_columns = join_config.get('outputColumns', [])
                    if output_columns:
                        logger.info(f"[Join Execution] Found {len(output_columns)} output columns in join config")
                        # Log included/excluded counts for debugging
                        included_count = sum(1 for col in output_columns if col.get('included', True))
                        excluded_count = len(output_columns) - included_count
                        logger.info(f"[Join Execution] Output columns: {included_count} included, {excluded_count} excluded")

                    # Log join request details for debugging
                    join_payload = {
                                    "db_type": db_type,
                                    "connection_config": {
                                        "hostname": decrypted_config.get('hostname'),
                                        "port": decrypted_config.get('port'),
                                        "database": decrypted_config.get('database'),
                                        "user": decrypted_config.get('user'),
                            "password": "***",  # Don't log password
                                        "schema": decrypted_config.get('schema'),
                                        "service_name": decrypted_config.get('service_name'),
                                    },
                                    "left_table": left_table_name,
                                    "right_table": right_table_name,
                                    "left_schema": left_table_info['schema'] or decrypted_config.get('schema'),
                                    "right_schema": right_table_info['schema'] or decrypted_config.get('schema'),
                                    "join_type": join_type,
                                    "conditions": conditions_with_aliases,
                                    "filters": filters,  # Pass filters if they exist
                                    "output_columns": output_columns if output_columns else None,  # Pass outputColumns for field selection/renaming
                                    "page": page,
                                    "page_size": page_size,
                                }

                    # Add table aliases to payload if self-join
                    if is_self_join:
                        join_payload["left_table_alias"] = left_table_alias
                        join_payload["right_table_alias"] = right_table_alias
                        logger.info(f"[Join Execution] Added table aliases to payload: left={left_table_alias}, right={right_table_alias}")

                    logger.info("[Join Execution] Calling FastAPI /metadata/join endpoint")
                    logger.info(f"[Join Execution] Join type: {join_type}")
                    logger.info(f"[Join Execution] Left table: {left_table_info['table_name']} (schema: {left_table_info.get('schema', '')})")
                    logger.info(f"[Join Execution] Right table: {right_table_info['table_name']} (schema: {right_table_info.get('schema', '')})")
                    logger.info(f"[Join Execution] Conditions count: {len(conditions_with_aliases)}")
                    for idx, cond in enumerate(conditions_with_aliases):
                        logger.info(f"[Join Execution]   Condition {idx + 1}: {cond.get('leftColumn')} {cond.get('operator', '=')} {cond.get('rightColumn')}")
                    if filters:
                        logger.info(f"[Join Execution] Filters: {filters}")

                    async def execute_join():
                        async with httpx.AsyncClient(timeout=60.0) as client:
                            # Restore password in actual payload
                            join_payload['connection_config']['password'] = decrypted_config.get('password')

                            response = await client.post(
                                f"{EXTRACTION_SERVICE_URL}/metadata/join",
                                json=join_payload
                            )

                            # Log response details for debugging
                            logger.info(f"[Join Execution] FastAPI response status: {response.status_code}")

                            if response.status_code != 200:
                                error_text = response.text
                                logger.error(f"[Join Execution] FastAPI error response: {error_text}")
                                try:
                                    error_json = response.json()
                                    logger.error(f"[Join Execution] FastAPI error JSON: {error_json}")
                                except Exception:
                                    pass

                            response.raise_for_status()
                            return response.json()

                    result = asyncio.run(execute_join())

                    rows = result.get('rows', [])
                    columns = result.get('columns', [])
                    output_metadata = result.get('output_metadata', {})

                    # Save to adaptive cache if canvas_id is provided
                    if canvas_id:
                        try:
                            # ADAPTIVE CACHE DISABLED
                            adaptive_cache = None
                            cached_node_ids = set()
                            depth = 0
                            fan_out = 1

                            node_config = join_config if target_node_type == 'join' else target_node_data.get('config', {})

                            # Estimate input rows (approximate)
                            input_rows = len(rows)  # In real scenario, get from upstream cache
                            output_rows = len(rows)

                            # Decide caching
                            should_cache, cache_layer = adaptive_cache.should_cache(
                                target_node_id,
                                target_node_type,
                                node_config,
                                input_rows=input_rows,
                                output_rows=output_rows,
                                depth_since_last_cache=depth,
                                fan_out=fan_out,
                                is_pushdown_candidate=False
                            )

                            if should_cache:
                                # Prepare column metadata
                                column_metadata = []
                                for col in columns:
                                    if isinstance(col, dict):
                                        column_metadata.append(col)
                                    else:
                                        column_metadata.append({'name': str(col), 'datatype': 'TEXT'})

                                # Get upstream node IDs and hashes
                                upstream_node_ids = [left_node.get('id'), right_node.get('id')]
                                upstream_hashes = {}
                                for upstream_id in upstream_node_ids:
                                    upstream_node = next((n for n in nodes if n.get('id') == upstream_id), None)
                                    if upstream_node:
                                        upstream_config = upstream_node.get('data', {}).get('config', {})
                                        upstream_hashes[upstream_id] = adaptive_cache._compute_node_hash(upstream_id, upstream_config)

                                adaptive_cache.save_cache(
                                    pipeline_id=str(canvas_id),
                                    node_id=target_node_id,
                                    node_type=target_node_type,
                                    node_config=node_config,
                                    rows=rows,
                                    columns=column_metadata,
                                    upstream_node_ids=upstream_node_ids,
                                    upstream_hashes=upstream_hashes,
                                    cache_layer=cache_layer,
                                    input_rows=input_rows,
                                    output_rows=output_rows,
                                    depth_since_last_cache=depth,
                                )
                                logger.info(f"Cached {len(rows)} rows for {target_node_type} node {target_node_id} in {cache_layer.value} cache")

                            # CRITICAL: Store output_metadata in node cache for downstream nodes (Projection, Filter, etc.)
                            if output_metadata and output_metadata.get('columns'):
                                logger.info(f"[Join Execution] Storing output_metadata with {len(output_metadata['columns'])} columns for node {target_node_id}")
                                # Update the cached node's output_metadata using adaptive cache
                                try:
                                    if adaptive_cache:
                                        node_config = join_config if target_node_type == 'join' else target_node_data.get('config', {})
                                        node_config_with_metadata = {**node_config, 'output_metadata': output_metadata}

                                        # Estimate input rows
                                        left_cached = adaptive_cache.get_cache(str(canvas_id), left_node.get('id'), "", "")
                                        right_cached = adaptive_cache.get_cache(str(canvas_id), right_node.get('id'), "", "")
                                        input_rows = max(
                                            left_cached.get('metadata', {}).get('row_count', 0) if left_cached else 0,
                                            right_cached.get('metadata', {}).get('row_count', 0) if right_cached else 0
                                        )

                                        save_to_adaptive_cache(
                                            node_id=target_node_id,
                                            node_type=target_node_type,
                                            node_config=node_config_with_metadata,
                                            rows=rows,
                                            columns=columns,
                                            upstream_node_ids=[left_node.get('id'), right_node.get('id')],
                                            input_rows=input_rows
                                        )
                                except Exception as metadata_e:
                                    logger.warning(f"Failed to store output_metadata in cache: {metadata_e}")
                        except Exception as cache_e:
                            logger.warning(f"Failed to cache join results: {cache_e}")

                    return Response({
                        "rows": rows,
                        "columns": columns,
                        "has_more": result.get('has_more', False),
                        "total": result.get('total', 0),
                        "page": page,
                        "page_size": page_size,
                        "from_cache": False,
                        "output_metadata": output_metadata,  # Include output_metadata in response
                    }, status=status.HTTP_200_OK)

                except httpx.ConnectError as e:
                    logger.error(f"FastAPI service connection error: {e}")
                    return Response(
                        {
                            "error": "FastAPI extraction service is not available",
                            "details": f"Could not connect to {EXTRACTION_SERVICE_URL}",
                        },
                        status=status.HTTP_503_SERVICE_UNAVAILABLE
                    )
                except httpx.HTTPStatusError as e:
                    # FastAPI returned an error status code
                    error_details = "Unknown error"
                    error_response = None
                    try:
                        error_response = e.response.json()
                        error_details = error_response.get('detail', error_response.get('error', str(error_response)))
                        logger.error(f"[Join Execution] FastAPI HTTP error {e.response.status_code}: {error_details}")
                        logger.error(f"[Join Execution] Full error response: {error_response}")
                    except Exception:
                        error_details = e.response.text
                        logger.error(f"[Join Execution] FastAPI HTTP error {e.response.status_code}: {error_details}")

                    # Extract column validation errors for better user experience
                    if error_response and 'detail' in error_response:
                        detail = error_response['detail']
                        # Check if it's a column validation error
                        if 'does not exist' in detail and 'Available columns' in detail:
                            # This is a column validation error - return it as-is for clear user feedback
                            return Response(
                                {
                                    "error": detail,
                                    "error_type": "column_validation",
                                },
                                status=status.HTTP_400_BAD_REQUEST  # Use 400 for validation errors
                            )

                    return Response(
                        {
                            "error": f"FastAPI service error: {error_details}",
                            "status_code": e.response.status_code,
                            "details": "Join execution failed at FastAPI service"
                        },
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
                except Exception as e:
                    logger.error(f"Error executing join: {e}")
                    import traceback
                    logger.error(f"[Join Execution] Traceback: {traceback.format_exc()}")
                    return Response(
                        {"error": f"Failed to execute join: {e!s}"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )

            # ------------------------------------------------------------------
            # FILTER execution (filter node after source node)
            # ------------------------------------------------------------------
            if target_node_type == 'filter' and join_node is None:
                # Filter node that is NOT after a join - must be after a source node
                logger.info("PipelineQueryExecutionView: executing filter pipeline")
                logger.info(f"  target_node_id={target_node_id}, target_type={target_node_type}")

                # Get filter configuration
                filter_config = target_node_data.get('config', {})
                filter_conditions = filter_config.get('conditions', [])

                if not filter_conditions:
                    return Response(
                        {"error": "Filter node has no conditions defined"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Find input node (should be source or another filter)
                input_edge = next((e for e in edges if e.get('target') == target_node_id), None)
                if not input_edge:
                    return Response(
                        {"error": "Filter node must have an input connection"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                input_node = next((n for n in nodes if n.get('id') == input_edge.get('source')), None)
                if not input_node:
                    return Response(
                        {"error": "Could not find input node for filter"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Trace back to source node and collect all filter conditions
                current_node = input_node
                all_filter_conditions = []

                # First, collect conditions from upstream filter nodes
                while current_node and current_node.get('data', {}).get('type') == 'filter':
                    current_conditions = current_node.get('data', {}).get('config', {}).get('conditions', [])
                    all_filter_conditions.extend(current_conditions)

                    # Find parent of this filter node
                    parent_edge = next((e for e in edges if e.get('target') == current_node.get('id')), None)
                    if not parent_edge:
                        break
                    current_node = next((n for n in nodes if n.get('id') == parent_edge.get('source')), None)

                # Add conditions from the target filter node
                all_filter_conditions.extend(filter_conditions)

                # Get source table info
                if current_node and current_node.get('data', {}).get('type') == 'source':
                    source_config = current_node.get('data', {}).get('config', {})
                    source_id = source_config.get('sourceId')
                    table_name = source_config.get('tableName')
                    schema = source_config.get('schema', '')
                else:
                    # Try to get from filter node's config (propagated from source)
                    source_id = filter_config.get('sourceId')
                    table_name = filter_config.get('tableName')
                    schema = filter_config.get('schema', '')

                if not source_id or not table_name:
                    return Response(
                        {"error": "Could not determine source table information for filter"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Connect to customer's database to get source connection
                from django.conf import settings
                import psycopg2

                conn = psycopg2.connect(
                    host=settings.DATABASES['default']['HOST'],
                    port=settings.DATABASES['default']['PORT'],
                    user=settings.DATABASES['default']['USER'],
                    password=settings.DATABASES['default']['PASSWORD'],
                    database=customer.cust_db
                )
                conn.autocommit = True
                db_cursor = conn.cursor()

                # Get source connection details
                db_cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'GENERAL' AND table_name = 'source'
                """)
                columns = [row[0] for row in db_cursor.fetchall()]
                config_column = 'source_config' if 'source_config' in columns else 'src_config'

                db_cursor.execute(f'''
                    SELECT {config_column}, created_on
                    FROM "GENERAL".source
                    WHERE id = %s
                ''', (source_id,))

                source_row = db_cursor.fetchone()
                if not source_row:
                    db_cursor.close()
                    conn.close()
                    return Response(
                        {"error": f"Source connection {source_id} not found"},
                        status=status.HTTP_404_NOT_FOUND
                    )

                source_config_encrypted = source_row[0]
                source_created_on = source_row[1]
                source_name = f"source_{source_id}"

                # Decrypt source configuration
                decrypted_config = decrypt_source_data(source_config_encrypted, customer.cust_id, source_created_on)
                if not decrypted_config:
                    db_cursor.close()
                    conn.close()
                    return Response(
                        {"error": "Failed to decrypt source configuration"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )

                db_type = decrypted_config.get('db_type', '').lower()
                db_cursor.close()
                conn.close()

                # Build filters list for FastAPI
                filters = []
                for condition in all_filter_conditions:
                    value = condition.get('value', '')
                    # Handle BETWEEN operator value format
                    if condition.get('operator') == 'BETWEEN':
                        if isinstance(value, str):
                            try:
                                import json
                                value = json.loads(value)
                            except Exception:
                                parts = [v.strip() for v in value.split(',')]
                                if len(parts) == 2:
                                    value = parts

                    # Clean column name - remove table prefix if present (for source filters)
                    column = condition.get('column', '').strip()
                    if column and '.' in column:
                        # For filters after source (not join), remove table prefix
                        # Keep table prefix only if this is after a join
                        parts = column.split('.')
                        column = parts[-1].strip()  # Use last part (column name)

                    filters.append({
                        'column': column,
                        'operator': condition.get('operator', '='),
                        'value': value,
                        'logicalOperator': condition.get('logicalOperator', 'AND'),
                    })

                logger.info(f"Filter execution: {len(filters)} filter conditions")
                logger.info(f"Filter conditions: {filters}")

                # Call FastAPI service to execute filter
                try:
                    import asyncio

                    import httpx

                    EXTRACTION_SERVICE_URL = getattr(settings, 'FASTAPI_EXTRACTION_SERVICE_URL', 'http://localhost:8001')

                    async def execute_filter():
                        async with httpx.AsyncClient(timeout=60.0) as client:
                            response = await client.post(
                                f"{EXTRACTION_SERVICE_URL}/metadata/filter",
                                json={
                                    "db_type": db_type,
                                    "connection_config": {
                                        "hostname": decrypted_config.get('hostname'),
                                        "port": decrypted_config.get('port'),
                                        "database": decrypted_config.get('database'),
                                        "user": decrypted_config.get('user'),
                                        "password": decrypted_config.get('password') or '',
                                        "schema": decrypted_config.get('schema'),
                                        "service_name": decrypted_config.get('service_name'),
                                    },
                                    "table_name": table_name,
                                    "schema": schema or decrypted_config.get('schema'),
                                    "filters": filters,
                                    "page": page,
                                    "page_size": page_size,
                                }
                            )
                            response.raise_for_status()
                            return response.json()

                    result = asyncio.run(execute_filter())

                    rows = result.get('rows', [])
                    columns = result.get('columns', [])

                    # SCHEMA PROPAGATION: Filter must be schema-transparent.
                    # Get schema from immediate input node (not traced-back source).
                    # Filter output schema = input schema (exact copy).
                    try:
                        input_node_data = input_node.get('data', {})
                        input_schema = []

                        # Priority 1: output_metadata from input node (most reliable)
                        if input_node_data.get('output_metadata'):
                            input_schema = input_node_data['output_metadata'].get('columns', [])
                            logger.info(f"Filter: Using input node output_metadata ({len(input_schema)} columns)")

                        # Priority 2: config.columns from input node
                        if not input_schema:
                            input_config = input_node_data.get('config', {})
                            if 'columns' in input_config:
                                input_schema = input_config.get('columns', [])
                                logger.info(f"Filter: Using input node config.columns ({len(input_schema)} columns)")

                        # If we have input schema, use it directly (schema-transparent)
                        if input_schema and isinstance(input_schema, list):
                            # Build a map for quick lookup
                            schema_map = {}
                            for col_meta in input_schema:
                                if isinstance(col_meta, dict) and 'name' in col_meta:
                                    schema_map[col_meta['name']] = col_meta
                                elif isinstance(col_meta, str):
                                    schema_map[col_meta] = {'name': col_meta, 'datatype': 'TEXT', 'nullable': True}

                            # Map returned columns to input schema
                            enriched_columns = []
                            for col in columns:
                                col_name = col if isinstance(col, str) else col.get('name')

                                # Get schema from input (exact copy)
                                if col_name in schema_map:
                                    enriched_columns.append(schema_map[col_name])
                                else:
                                    # Column not in input schema (shouldn't happen)
                                    logger.warning(f"Filter: Column '{col_name}' not found in input schema")
                                    if isinstance(col, dict):
                                        enriched_columns.append(col)
                                    else:
                                        enriched_columns.append({'name': col_name, 'datatype': 'TEXT', 'nullable': True})

                            columns = enriched_columns
                            logger.info(f"Filter: Schema propagated from input node ({len(columns)} columns)")
                        else:
                            logger.warning("Filter: No input schema found, columns may lose type information")
                    except Exception as enrich_err:
                        logger.error(f"Filter: Failed to propagate schema: {enrich_err}")
                        import traceback
                        logger.error(traceback.format_exc())

                    # Save to adaptive cache if canvas_id is provided
                    if canvas_id and adaptive_cache:
                        # Get upstream node for input row count estimation
                        upstream_node_id = current_node.get('id') if current_node else None
                        input_rows = 0
                        if upstream_node_id:
                            # Try to get row count from upstream cache
                            upstream_cached = adaptive_cache.get_cache(str(canvas_id), upstream_node_id, "", "")
                            if upstream_cached:
                                input_rows = upstream_cached.get('metadata', {}).get('row_count', len(rows))

                        # Get column lineage from SQL compiler if available (for pushdown analysis)
                        column_lineage = {}
                        try:
                            from api.pipeline.preview_compiler import SQLCompiler
                            # Try to get lineage from a compiler instance if we have one
                            # For now, use empty dict - will be populated if compiler was used
                        except Exception:
                            pass

                        save_to_adaptive_cache(
                            node_id=target_node_id,
                            node_type='filter',
                            node_config=filter_config,
                            rows=rows,
                            columns=columns,
                            upstream_node_ids=[upstream_node_id] if upstream_node_id else [],
                            input_rows=input_rows,
                            column_lineage=column_lineage
                        )

                    return Response({
                        "rows": rows,
                        "columns": columns,
                        "has_more": result.get('has_more', False),
                        "total": result.get('total', 0),
                        "page": page,
                        "page_size": page_size,
                        "from_cache": False,
                        "output_metadata": {"columns": columns},  # Schema for frontend propagation
                    }, status=status.HTTP_200_OK)

                except httpx.ConnectError as e:
                    logger.error(f"FastAPI service connection error: {e}")
                    return Response(
                        {
                            "error": "FastAPI extraction service is not available",
                            "details": f"Could not connect to {EXTRACTION_SERVICE_URL}",
                        },
                        status=status.HTTP_503_SERVICE_UNAVAILABLE
                    )
                except Exception as e:
                    logger.error(f"Error executing filter: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    return Response(
                        {"error": f"Failed to execute filter: {e!s}"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )

            # ------------------------------------------------------------------
            # PROJECTION execution
            # ------------------------------------------------------------------
            if target_node_type == 'projection':
                logger.info("🔧 CODE VERSION: 2026-01-20-12:05 - Schema Validation Fix Applied")
                logger.info("PipelineQueryExecutionView: executing projection pipeline")
                logger.info(f"  target_node_id={target_node_id}")

                # Get projection configuration
                projection_config = target_node_data.get('config', {})

                # Check for projection metadata in node.data.projection (new format from frontend)
                # CRITICAL: Handle None case - projection_metadata might be None, not {}
                projection_metadata = target_node_data.get('projection') or {}
                projection_columns_metadata = projection_metadata.get('columns', []) if projection_metadata else []

                selected_mode = (projection_metadata.get('mode') if projection_metadata else None) or projection_config.get('selectedMode', 'INCLUDE')
                # Support both new format (includedColumns/excludedColumns) and legacy format
                included_columns = projection_config.get('includedColumns', projection_config.get('selectedColumns', []))
                excluded_columns = projection_config.get('excludedColumns', [])
                column_mappings = projection_config.get('columnMappings', [])
                # Legacy support
                exclude_mode = projection_config.get('excludeMode', False) or (selected_mode == 'EXCLUDE')

                # ============================================================
                # FETCH DEBUG: Track where calculated columns are coming from
                # ============================================================
                logger.info("=" * 80)
                logger.info(f"[FETCH DEBUG] Starting preview for projection node: {target_node_id}")

                # CRITICAL FIX: Get calculated columns from ALL possible locations
                # 1. Request data (frontend may send calculated columns in multiple places)
                calculated_columns_from_config = projection_config.get('calculatedColumns', [])
                calculated_columns_from_data = target_node_data.get('calculatedColumns', [])
                calculated_columns_from_projection = projection_metadata.get('calculatedColumns', []) if projection_metadata else []

                logger.info(f"[FETCH DEBUG] Source 1: projection_config.calculatedColumns = {len(calculated_columns_from_config)} columns")
                if calculated_columns_from_config:
                    for cc in calculated_columns_from_config:
                        if isinstance(cc, dict):
                            logger.info(f"[FETCH DEBUG]   * {cc.get('name')}: {cc.get('expression')}")

                logger.info(f"[FETCH DEBUG] Source 2: target_node_data.calculatedColumns = {len(calculated_columns_from_data)} columns")
                if calculated_columns_from_data:
                    for cc in calculated_columns_from_data:
                        if isinstance(cc, dict):
                            logger.info(f"[FETCH DEBUG]   * {cc.get('name')}: {cc.get('expression')}")

                logger.info(f"[FETCH DEBUG] Source 3: projection_metadata.calculatedColumns = {len(calculated_columns_from_projection)} columns")
                if calculated_columns_from_projection:
                    for cc in calculated_columns_from_projection:
                        if isinstance(cc, dict):
                            logger.info(f"[FETCH DEBUG]   * {cc.get('name')}: {cc.get('expression')}")

                # 2. DATABASE FALLBACK: Fetch from CanvasNode.config_json if not in request
                # This ensures calculated columns are loaded even if frontend doesn't send them
                # CRITICAL: Always try to fetch from database as fallback to ensure calculated columns are available
                calculated_columns_from_db = []
                try:
                    from api.models.canvas import CanvasNode
                    # Try to fetch node from database using node_id
                    # CRITICAL: Handle case where pipeline_nodes table doesn't exist (database not migrated)
                    try:
                        canvas_node = CanvasNode.objects.filter(node_id=target_node_id).first()
                    except Exception as db_error:
                        # Database table might not exist - this is OK for preview/execution
                        logger.warning(f"[FETCH DEBUG] ✗ Database query failed (table may not exist): {db_error!s}")
                        canvas_node = None
                    if canvas_node:
                        logger.info(f"[FETCH DEBUG] ✓ Found CanvasNode in database: {canvas_node.business_name}")
                        if canvas_node.config_json:
                            # Check multiple locations in config_json
                            db_calculated = (
                                canvas_node.config_json.get('calculatedColumns', []) or
                                canvas_node.config_json.get('calculated_columns', []) or
                                []
                            )
                            if db_calculated:
                                calculated_columns_from_db = db_calculated if isinstance(db_calculated, list) else [db_calculated]
                                logger.info(f"[FETCH DEBUG] Source 4 (DB): CanvasNode.config_json.calculatedColumns = {len(calculated_columns_from_db)} columns")
                                for cc in calculated_columns_from_db:
                                    if isinstance(cc, dict):
                                        logger.info(f"[FETCH DEBUG]   * {cc.get('name')}: {cc.get('expression')}")
                            else:
                                logger.info("[FETCH DEBUG] ✗ CanvasNode.config_json exists but no calculatedColumns found")
                                logger.info(f"[FETCH DEBUG]   config_json keys: {list(canvas_node.config_json.keys())}")
                        else:
                            logger.info("[FETCH DEBUG] ✗ CanvasNode found but config_json is empty or None")
                    else:
                        logger.info(f"[FETCH DEBUG] ✗ CanvasNode not found in database for node_id: {target_node_id}")
                except Exception as db_fetch_error:
                    logger.warning(f"[FETCH DEBUG] ✗ Failed to fetch from database: {db_fetch_error}")
                    import traceback
                    logger.debug(traceback.format_exc())

                logger.info("=" * 80)

                # Merge and deduplicate by name (prefer request > database order)
                # Priority: config > data > projection > database
                all_calculated_sources = []
                seen_names = set()
                for calc_col in calculated_columns_from_config + calculated_columns_from_data + calculated_columns_from_projection + calculated_columns_from_db:
                    if isinstance(calc_col, dict):
                        name = calc_col.get('name', '').strip()
                        if name and name not in seen_names:
                            seen_names.add(name)
                            all_calculated_sources.append(calc_col)

                calculated_columns = all_calculated_sources

                # Log where calculated columns were found (for debugging)
                logger.info("[FETCH DEBUG] Merging calculated columns from all sources:")
                logger.info(f"[FETCH DEBUG]   - From config: {len(calculated_columns_from_config)}")
                logger.info(f"[FETCH DEBUG]   - From data: {len(calculated_columns_from_data)}")
                logger.info(f"[FETCH DEBUG]   - From projection: {len(calculated_columns_from_projection)}")
                logger.info(f"[FETCH DEBUG]   - From database: {len(calculated_columns_from_db)}")
                logger.info(f"[FETCH DEBUG]   - TOTAL (after deduplication): {len(calculated_columns)}")

                if calculated_columns:
                    logger.info("[FETCH DEBUG] Final calculated columns list:")
                    for cc in calculated_columns:
                        logger.info(f"[FETCH DEBUG]   * {cc.get('name')}: {cc.get('expression')}")
                else:
                    logger.warning("[FETCH DEBUG] ⚠️  NO CALCULATED COLUMNS DETECTED FROM ANY SOURCE!")

                logger.info("=" * 80)

                # Build set of calculated column names for filtering (CRITICAL: remove from selected_columns)
                calculated_col_names_set = {cc.get('name', '').strip() for cc in calculated_columns if cc.get('name')}
                # Get aggregate columns (for GROUP BY support)
                aggregate_columns = projection_config.get('aggregateColumns', [])
                # Get group-by columns (explicit or auto-generated)
                group_by_columns = projection_config.get('groupByColumns', [])
                # Get output_columns if available (preserves UI order)
                output_columns = projection_config.get('output_columns', [])

                # Log projection config for debugging
                logger.info(f"Projection config keys: {list(projection_config.keys())}")
                logger.info(f"Projection metadata present: {bool(projection_metadata)}")
                logger.info(f"Projection columns metadata count: {len(projection_columns_metadata)}")
                logger.info(f"Projection mode: selected_mode={selected_mode}, exclude_mode={exclude_mode}")
                logger.info(f"Projection columns: output_columns={len(output_columns) if output_columns else 0}, "
                           f"includedColumns={len(included_columns) if included_columns else 0}, "
                           f"excludedColumns={len(excluded_columns) if excluded_columns else 0}")
                if output_columns:
                    logger.info(f"output_columns content: {output_columns[:10]}")
                if included_columns:
                    logger.info(f"includedColumns content: {included_columns[:10]}")
                if projection_columns_metadata:
                    logger.info(f"projection.columns metadata: {[col.get('name') for col in projection_columns_metadata[:10]]}")

                # CRITICAL: Log calculated columns for debugging - Enhanced logging
                logger.info("[Calculated Column Debug] Checking all locations for calculated columns:")
                logger.info(f"[Calculated Column Debug]  1. projection_config.calculatedColumns: {calculated_columns_from_config}")
                logger.info(f"[Calculated Column Debug]  2. target_node_data.calculatedColumns: {calculated_columns_from_data}")
                logger.info(f"[Calculated Column Debug]  3. projection_metadata.calculatedColumns: {calculated_columns_from_projection}")
                logger.info(f"[Calculated Column Debug] Merged calculated columns (deduplicated): {len(calculated_columns)} columns")
                for idx, calc_col in enumerate(calculated_columns):
                    logger.info(f"[Calculated Column Debug]   [{idx+1}] {calc_col.get('name', 'UNNAMED')}: {calc_col.get('expression', 'NO EXPRESSION')}")
                logger.info(f"[Calculated Column Debug] Calculated column names set: {calculated_col_names_set}")
                logger.info(f"[Calculated Column Debug] Full projection_config structure: {list(projection_config.keys())}")
                logger.info(f"[Calculated Column Debug] target_node_data keys: {list(target_node_data.keys())}")
                logger.info(f"[Calculated Column Debug] projection_metadata keys: {list(projection_metadata.keys()) if projection_metadata else 'None'}")

                # Check inside config for other potential locations
                for key in projection_config.keys():
                    if 'calc' in key.lower() or 'computed' in key.lower() or 'expression' in key.lower():
                        logger.info(f"[Calculated Column Debug] Found potential calculated field '{key}': {projection_config.get(key)}")

                # Log the last column in output_metadata_cols to see structure of 'upper'
                logger.info("[Calculated Column Debug] Checking for 'upper' column in projection_metadata.columns:")
                if projection_metadata and projection_columns_metadata:
                    for col in projection_columns_metadata:
                        if col.get('name') == 'upper' or 'upper' in str(col.get('name', '')).lower():
                            logger.info(f"[Calculated Column Debug] Found 'upper' in projection.columns: {col}")

                # CRITICAL FIX: Also check output_metadata for calculated columns (might be saved there)
                output_metadata_from_node = target_node_data.get('output_metadata', {})
                output_metadata_cols = output_metadata_from_node.get('columns', [])
                calculated_from_metadata = [col for col in output_metadata_cols if col.get('source') == 'calculated']

                if calculated_from_metadata:
                    logger.info(f"[Calculated Column Debug] Found {len(calculated_from_metadata)} calculated columns in output_metadata")
                    # Merge with calculated_columns from config
                    for calc_meta in calculated_from_metadata:
                        # Check if already in calculated_columns
                        if not any(cc.get('name') == calc_meta.get('name') for cc in calculated_columns if isinstance(cc, dict)):
                            calculated_columns.append({
                                'name': calc_meta.get('name'),
                                'expression': calc_meta.get('expression', ''),
                                'dataType': calc_meta.get('datatype', 'STRING')
                            })
                            logger.info(f"[Calculated Column Debug] Added calculated column from output_metadata: {calc_meta.get('name')}")

                if calculated_columns:
                    for idx, cc in enumerate(calculated_columns[:5]):  # First 5
                        logger.info(f"[Calculated Column Debug] [{idx}] {cc}")

                # Find input node (parent)
                input_edge = next((e for e in edges if e.get('target') == target_node_id), None)
                if not input_edge:
                    return Response(
                        {"error": "Projection node must have an input connection"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                input_node = next((n for n in nodes if n.get('id') == input_edge.get('source')), None)
                if not input_node:
                    logger.error(f"Could not find input node for projection. Input edge source: {input_edge.get('source')}")
                    logger.error(f"Available node IDs: {[n.get('id') for n in nodes]}")
                    return Response(
                        {"error": "Could not find input node for projection"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Log input node info
                input_node_data = input_node.get('data', {})
                input_node_type = input_node_data.get('type')
                input_node_config = input_node_data.get('config', {})
                logger.info(f"Input node: type={input_node_type}, id={input_node.get('id')}")
                logger.info(f"TRACKER: input_node initialized at start. Object ID: {id(input_node)}, Value is None: {input_node is None}")
                logger.info(f"Input node config keys: {list(input_node_config.keys())}")
                if 'columns' in input_node_config:
                    input_cols = input_node_config.get('columns', [])
                    logger.info(f"Input node has {len(input_cols) if isinstance(input_cols, list) else 0} columns in config")

                # Determine final selected columns based on mode
                if selected_mode == 'EXCLUDE' or exclude_mode:
                    # EXCLUDE mode: Get all columns from projection metadata or input node and exclude selected ones
                    all_columns = []
                    all_column_names = []

                    # First, try to get columns from projection metadata (preferred)
                    if projection_columns_metadata and len(projection_columns_metadata) > 0:
                        # Extract all column names from projection metadata
                        all_column_names = [col.get('name') or str(col) for col in projection_columns_metadata if col.get('name')]
                        logger.info(f"Projection EXCLUDE mode: Using {len(all_column_names)} columns from projection metadata")
                    else:
                        # Fallback: Get columns from input node
                        all_columns = input_node_config.get('columns', [])
                        if isinstance(all_columns, list) and len(all_columns) > 0:
                            # Extract column names if they're objects
                            all_column_names = [col if isinstance(col, str) else (col.get('name') or col.get('column_name') or str(col)) for col in all_columns]
                            logger.info(f"Projection EXCLUDE mode: Using {len(all_column_names)} columns from input node")

                    if len(all_column_names) > 0:
                        # Exclude columns
                        columns_to_exclude = excluded_columns if excluded_columns else (included_columns if not included_columns or selected_mode == 'EXCLUDE' else [])
                        selected_columns = [col for col in all_column_names if col not in columns_to_exclude]
                        logger.info(f"Projection EXCLUDE mode: {len(all_column_names)} total columns, excluding {len(columns_to_exclude)}, resulting in {len(selected_columns)} columns")
                    else:
                        return Response(
                            {"error": "EXCLUDE mode requires projection column metadata or input node to have column metadata"},
                            status=status.HTTP_400_BAD_REQUEST
                        )
                else:
                    # INCLUDE mode: Use included columns in EXACT order from config
                    # CRITICAL: Prefer output_columns (preserves UI drag-and-drop order), then includedColumns, then selectedColumns
                    # Do NOT reorder or sort - preserve the exact sequence from the UI
                    # CRITICAL FIX: Remove calculated columns from selected_columns - they'll be added separately
                    calculated_col_names_set = {cc.get('name', '').strip() for cc in calculated_columns if cc.get('name')}

                    if output_columns and len(output_columns) > 0:
                        # Filter out calculated columns from output_columns - they're not base columns
                        selected_columns = [col for col in output_columns if col not in calculated_col_names_set]
                        logger.info(f"Projection: Using output_columns order ({len(output_columns)} total, {len(selected_columns)} base columns after removing {len(calculated_col_names_set)} calculated)")
                        logger.info(f"Projection: Calculated columns removed from selected_columns: {list(calculated_col_names_set)}")
                    elif included_columns and len(included_columns) > 0:
                        # Filter out calculated columns from included_columns
                        selected_columns = [col for col in included_columns if col not in calculated_col_names_set]
                        logger.info(f"Projection: Using includedColumns order ({len(included_columns)} total, {len(selected_columns)} base columns after removing {len(calculated_col_names_set)} calculated)")
                    else:
                        # ✅ FIX: Default to ALL columns if none selected (SQL SELECT * behavior)
                        logger.info("Projection: No columns explicitly selected, defaulting to ALL columns")
                        all_column_names = []

                        # First, try to get columns from projection metadata (preferred)
                        if projection_columns_metadata and len(projection_columns_metadata) > 0:
                            all_column_names = [col.get('name') or str(col) for col in projection_columns_metadata if col.get('name')]
                            logger.info(f"Projection default ALL mode: Using {len(all_column_names)} columns from projection metadata")
                        else:
                            # Fallback: Get columns from input node
                            all_columns = input_node_config.get('columns', [])
                            if isinstance(all_columns, list) and len(all_columns) > 0:
                                all_column_names = [col if isinstance(col, str) else (col.get('name') or col.get('column_name') or str(col)) for col in all_columns]
                                logger.info(f"Projection default ALL mode: Using {len(all_column_names)} columns from input node")

                        # CRITICAL FIX: Also filter out calculated columns when defaulting to ALL
                        selected_columns = [col for col in all_column_names if col not in calculated_col_names_set]
                        removed_calc = [col for col in all_column_names if col in calculated_col_names_set]
                        if removed_calc:
                            logger.info(f"Projection: Removed {len(removed_calc)} calculated columns from default ALL columns: {removed_calc}")
                        logger.info(f"Projection: Defaulting to ALL {len(all_column_names)} columns ({len(selected_columns)} base after removing calculated)")

                if not selected_columns:
                    logger.error(f"Projection node has no columns selected. Config: {projection_config}")
                    logger.error(f"  output_columns: {output_columns}")
                    logger.error(f"  includedColumns: {included_columns}")
                    logger.error(f"  excludedColumns: {excluded_columns}")
                    logger.error(f"  exclude_mode: {exclude_mode}, selected_mode: {selected_mode}")
                    return Response(
                        {"error": "Projection node has no columns selected. Please select at least one column in the Projection configuration."},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # BACKEND DEBUGGING (per specification)
                # OnCalculatedColumnSave: Print saved calculated columns
                logger.info("=== SAVED CALCULATED COLUMNS ===")
                logger.info(f"Projection config full structure: {projection_config}")
                logger.info(f"Raw calculated_columns from config: {calculated_columns}")
                logger.info(f"calculated_columns type: {type(calculated_columns)}")
                logger.info(f"calculated_columns length: {len(calculated_columns) if calculated_columns else 0}")
                if calculated_columns:
                    logger.info(f"First calculated column: {calculated_columns[0] if len(calculated_columns) > 0 else 'N/A'}")

                # Get all available columns from input node for column name mapping
                # Calculated columns can reference any column from the input, not just selected ones
                all_available_columns = []

                # For join nodes, use join output columns
                if input_node_type == 'join':
                    join_output_cols = input_node_config.get('columns', [])
                    if isinstance(join_output_cols, list) and len(join_output_cols) > 0:
                        all_available_columns = [col if isinstance(col, str) else (col.get('name') or col.get('column_name') or str(col)) for col in join_output_cols]
                    logger.info(f"Projection after join: Using {len(all_available_columns)} columns from join output for calculated column evaluation")
                elif projection_columns_metadata and len(projection_columns_metadata) > 0:
                    all_available_columns = [col.get('name') or str(col) for col in projection_columns_metadata if col.get('name')]
                else:
                    input_cols = input_node_config.get('columns', [])
                    if isinstance(input_cols, list) and len(input_cols) > 0:
                        all_available_columns = [col if isinstance(col, str) else (col.get('name') or col.get('column_name') or str(col)) for col in input_cols]

                # CRITICAL FIX: Filter out calculated columns from all_available_columns before conflict check
                # projection_columns_metadata may include previously saved calculated columns, which causes false conflicts
                calculated_col_names_set_for_filter = {cc.get('name', '').strip() for cc in calculated_columns if cc.get('name')}
                all_available_columns_filtered = [
                    col for col in all_available_columns
                    if col not in calculated_col_names_set_for_filter
                ]
                # Also check projection_columns_metadata for isCalculated flag
                if projection_columns_metadata:
                    for col_meta in projection_columns_metadata:
                        if col_meta.get('isCalculated') or col_meta.get('is_calculated'):
                            col_name = col_meta.get('name')
                            if col_name and col_name in all_available_columns_filtered:
                                all_available_columns_filtered.remove(col_name)
                                logger.debug(f"[Calculated Column] Filtered out calculated column '{col_name}' from all_available_columns (has isCalculated flag)")

                # Use filtered list for conflict check (only actual source columns)
                all_available_columns = all_available_columns_filtered
                logger.info(f"[Calculated Column] Filtered all_available_columns: {len(all_available_columns)} source columns (removed calculated columns)")
                logger.info(f"[Calculated Column DEBUG] Filtered source columns: {all_available_columns[:15]}...")
                logger.info(f"[Calculated Column DEBUG] Calculated columns removed from all_available_columns: {sorted(calculated_col_names_set_for_filter)}")

                # Validate calculated columns
                valid_calculated_columns = []
                if calculated_columns:
                    logger.info(f"TRACKER: Processing calculated columns. Count: {len(calculated_columns)}")
                    # CRITICAL FIX: Check for conflicts against SOURCE columns only (all_available_columns)
                    # Calculated columns are intentionally added to output_columns/includedColumns by the frontend,
                    # so selected_columns already includes them. We should only check against original source columns.
                    logger.info(f"[Calculated Column] Checking conflicts against source columns: {all_available_columns[:10]}...")
                    logger.info(f"[Calculated Column] selected_columns includes calculated columns: {selected_columns[:10]}...")

                    for calc_col in calculated_columns:
                        if isinstance(calc_col, dict):
                            name = calc_col.get('name', '').strip()
                            expression = calc_col.get('expression', '').strip()
                            if name and expression:
                                # Check for name conflicts with SOURCE columns only
                                # Calculated columns are intentionally added to output_columns, so don't check against selected_columns
                                if name in all_available_columns:
                                    logger.warning(f"Calculated column '{name}' conflicts with source column '{name}', skipping")
                                    continue
                                valid_calculated_columns.append({
                                    'name': name,
                                    'expression': expression,
                                    'dataType': calc_col.get('dataType', 'STRING')
                                })

                logger.info(f"Valid calculated columns: {len(valid_calculated_columns)}")
                for cc in valid_calculated_columns:
                    logger.info(f"  - {cc['name']}: {cc['expression']}")

                logger.info("=== PROJECTION COLUMNS ===")
                logger.info(f"Selected columns: {selected_columns[:10]}...")  # First 10
                logger.info(f"Projection: {len(selected_columns)} projected columns, {len(valid_calculated_columns)} calculated columns")
                logger.info(f"All available columns (for calculated column evaluation): {all_available_columns[:10]}...")

                # Get source table info from input node (already defined above, reuse)
                input_config = input_node_config

                # Support projection on source nodes and filter nodes
                source_id = None
                table_name = None
                schema = None
                filters = []

                if input_node_type == 'source':
                    source_id = input_config.get('sourceId')
                    table_name = input_config.get('tableName')
                    schema = input_config.get('schema', '')
                elif input_node_type == 'filter':
                    # If input is a filter node, we need to get the source info from its input
                    # and also apply the filter conditions

                    # 1. Get filter conditions
                    filter_conditions = input_config.get('conditions', [])

                    # 2. Trace back to source node
                    current_node = input_node
                    while current_node and current_node.get('data', {}).get('type') == 'filter':
                        # Add conditions from this filter node
                        current_conditions = current_node.get('data', {}).get('config', {}).get('conditions', [])
                        filters.extend(current_conditions)

                        # Find parent of this filter node
                        parent_edge = next((e for e in edges if e.get('target') == current_node.get('id')), None)
                        if not parent_edge:
                            break
                        current_node = next((n for n in nodes if n.get('id') == parent_edge.get('source')), None)

                    if current_node and current_node.get('data', {}).get('type') == 'source':
                        source_config = current_node.get('data', {}).get('config', {})
                        source_id = source_config.get('sourceId')
                        table_name = source_config.get('tableName')
                        schema = source_config.get('schema', '')
                    else:
                        return Response(
                            {"error": "Could not trace filter back to a valid source node"},
                            status=status.HTTP_400_BAD_REQUEST
                        )
                elif input_node_type == 'projection':
                    # ✅ FIX: Add support for projection-to-projection chaining
                    # Trace back through projection(s) to find source/filter
                    current_node = input_node
                    trace_path = [current_node.get('id')]

                    while current_node:
                        parent_edge = next((e for e in edges if e.get('target') == current_node.get('id')), None)
                        if not parent_edge:
                            break

                        current_node = next((n for n in nodes if n.get('id') == parent_edge.get('source')), None)
                        if not current_node:
                            break

                        current_type = current_node.get('data', {}).get('type')
                        trace_path.append(f"{current_node.get('id')}:{current_type}")

                        if current_type == 'source':
                            source_config = current_node.get('data', {}).get('config', {})
                            source_id = source_config.get('sourceId')
                            table_name = source_config.get('tableName')
                            schema = source_config.get('schema', '')
                            break
                        elif current_type == 'filter':
                            filter_conditions = current_node.get('data', {}).get('config', {}).get('conditions', [])
                            if filter_conditions:
                                filters.extend(filter_conditions)
                            # Continue tracing back
                        elif current_type == 'projection':
                            # Another projection, keep going back
                            continue
                        elif current_type == 'join':
                            # Join node - get columns from join output
                            join_output_columns = current_node.get('data', {}).get('config', {}).get('columns', [])
                            if join_output_columns:
                                # Use join node's output columns for projection
                                # We'll execute the join first, then apply projection on results
                                logger.info(f"Projection after join: Found {len(join_output_columns)} columns from join node")
                                # Set a flag to indicate we need to execute join first
                                source_id = None  # Will trigger join execution path
                                table_name = None
                                break

                    if not source_id or not table_name:
                        return Response(
                            {"error": "Could not trace projection back to a valid source node"},
                            status=status.HTTP_400_BAD_REQUEST
                        )
                elif input_node_type == 'join':
                    # ✅ FIX: Add support for projection after join
                    # Strategy: Execute join first, then apply projection on results
                    logger.info("Projection after join: Executing join first, then applying projection")

                    # Check cache first
                    join_rows = None
                    join_columns = None
                    if canvas_id and use_cache and not force_refresh:
                        try:
                            # Use adaptive cache
                            if adaptive_cache:
                                # Get node version hash for cache validation
                                input_node_config = input_node.get('data', {}).get('config', {})
                                node_version_hash = adaptive_cache._compute_node_hash(input_node.get('id'), input_node_config)

                                # Get upstream hashes
                                upstream_node_ids = []
                                upstream_hashes = {}
                                join_input_edges = [e for e in edges if e.get('target') == input_node.get('id')]
                                for edge in join_input_edges:
                                    upstream_id = edge.get('source')
                                    upstream_node_ids.append(upstream_id)
                                    upstream_node = next((n for n in nodes if n.get('id') == upstream_id), None)
                                    if upstream_node:
                                        upstream_config = upstream_node.get('data', {}).get('config', {})
                                        upstream_hashes[upstream_id] = adaptive_cache._compute_node_hash(upstream_id, upstream_config)

                                upstream_version_hash = adaptive_cache._compute_upstream_hash(upstream_node_ids, upstream_hashes)

                                cached_join_data = adaptive_cache.get_cache(
                                    str(canvas_id),
                                    input_node.get('id'),
                                    node_version_hash,
                                    upstream_version_hash
                                )
                                if cached_join_data:
                                    join_rows = cached_join_data.get('rows', [])
                                    join_columns = cached_join_data.get('columns', [])
                                    logger.info(f"Using cached join results: {len(join_rows)} rows, {len(join_columns)} columns")
                        except Exception as cache_e:
                            logger.warning(f"Cache check failed for join node: {cache_e}")

                    # If not cached, execute join by recursively calling pipeline execution
                    if join_rows is None:
                        logger.info(f"Join results not cached, executing join node {input_node.get('id')}")
                        # Recursively execute the join node
                        try:
                            # Create a temporary request with join node as target
                            join_execution_data = {
                                'nodes': nodes,
                                'edges': edges,
                                'targetNodeId': input_node.get('id'),
                                'page': page,
                                'page_size': page_size,
                                'canvasId': canvas_id,
                                'useCache': use_cache,
                                'forceRefresh': force_refresh
                            }

                            # Execute join by calling the same view method recursively
                            # We'll create a new request object with join execution data
                            type('Request', (), {
                                'data': join_execution_data,
                                'user': user
                            })()

                            # Call the join execution logic directly
                            # Instead of recursive call, we'll inline the join execution
                            # Get join config
                            join_config = input_node_config
                            join_type = join_config.get('joinType', 'INNER')
                            conditions = join_config.get('conditions', [])

                            # Find left and right input nodes (reuse logic from join execution)
                            join_input_edges = [e for e in edges if e.get('target') == input_node.get('id')]
                            left_edge = next((e for e in join_input_edges if e.get('targetHandle') == 'left'), None)
                            right_edge = next((e for e in join_input_edges if e.get('targetHandle') == 'right'), None)

                            if not left_edge or not right_edge:
                                if len(join_input_edges) >= 2:
                                    left_edge = join_input_edges[0]
                                    right_edge = join_input_edges[1]
                                else:
                                    return Response(
                                        {"error": "Join node must have both left and right input connections"},
                                        status=status.HTTP_400_BAD_REQUEST
                                    )

                            left_node = next((n for n in nodes if n.get('id') == left_edge.get('source')), None)
                            right_node = next((n for n in nodes if n.get('id') == right_edge.get('source')), None)

                            if not left_node or not right_node:
                                return Response(
                                    {"error": "Could not find left or right input nodes for join"},
                                    status=status.HTTP_400_BAD_REQUEST
                                )

                            # Use the get_table_info_from_node helper from join execution (defined above)
                            # We need to define it here or reuse it
                            def get_table_info_from_node_for_projection(node, visited=None):
                                """Helper to get table info, reusing logic from join execution"""
                                if visited is None:
                                    visited = set()
                                node_id = node.get('id')
                                if node_id in visited:
                                    return None
                                visited.add(node_id)

                                node_data = node.get('data', {})
                                node_type = node_data.get('type')
                                node_config = node_data.get('config', {})

                                if node_type == 'source':
                                    return {
                                        'source_id': node_config.get('sourceId'),
                                        'table_name': node_config.get('tableName'),
                                        'schema': node_config.get('schema', ''),
                                    }

                                input_edge = next((e for e in edges if e.get('target') == node_id), None)
                                if input_edge:
                                    input_node_id = input_edge.get('source')
                                    input_node_for_traverse = next((n for n in nodes if n.get('id') == input_node_id), None)
                                    if input_node_for_traverse:
                                        return get_table_info_from_node_for_projection(input_node_for_traverse, visited)

                                if node_config.get('sourceId') and node_config.get('tableName'):
                                    return {
                                        'source_id': node_config.get('sourceId'),
                                        'table_name': node_config.get('tableName'),
                                        'schema': node_config.get('schema', ''),
                                    }
                                return None

                            left_table_info = get_table_info_from_node_for_projection(left_node)
                            right_table_info = get_table_info_from_node_for_projection(right_node)

                            if not left_table_info or not right_table_info:
                                return Response(
                                    {"error": "Could not determine table information for join inputs"},
                                    status=status.HTTP_400_BAD_REQUEST
                                )

                            # Get source connection
                            source_id_for_join = left_table_info.get('source_id')
                            if not source_id_for_join:
                                return Response(
                                    {"error": "Could not determine source connection for join"},
                                    status=status.HTTP_400_BAD_REQUEST
                                )

                            # Get source config (reuse code from above)
                            from django.conf import settings
                            import psycopg2
                            conn = psycopg2.connect(
                                host=settings.DATABASES['default']['HOST'],
                                port=settings.DATABASES['default']['PORT'],
                                user=settings.DATABASES['default']['USER'],
                                password=settings.DATABASES['default']['PASSWORD'],
                                database=customer.cust_db
                            )
                            conn.autocommit = True
                            db_cursor = conn.cursor()

                            db_cursor.execute("""
                                SELECT column_name
                                FROM information_schema.columns
                                WHERE table_schema = 'GENERAL' AND table_name = 'source'
                            """)
                            columns = [row[0] for row in db_cursor.fetchall()]
                            config_column = 'source_config' if 'source_config' in columns else 'src_config'

                            db_cursor.execute(f'''
                                SELECT {config_column}, created_on
                                FROM "GENERAL".source
                                WHERE id = %s
                            ''', (source_id_for_join,))

                            source_row = db_cursor.fetchone()
                            if not source_row:
                                db_cursor.close()
                                conn.close()
                                return Response(
                                    {"error": f"Source connection {source_id_for_join} not found"},
                                    status=status.HTTP_404_NOT_FOUND
                                )

                            source_config_encrypted = source_row[0]
                            source_created_on = source_row[1]
                            source_name = f"source_{source_id_for_join}"
                            source_config = decrypt_source_data(source_config_encrypted, customer.cust_id, source_created_on)
                            db_cursor.close()
                            conn.close()

                            if not source_config:
                                return Response(
                                    {"error": "Failed to decrypt source configuration"},
                                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                                )

                            db_type = source_config.get('db_type', 'postgresql')

                            # Prepare join conditions with aliases
                            conditions_with_aliases = []
                            for cond in conditions:
                                left_col = cond.get('leftColumn', '')
                                right_col = cond.get('rightColumn', '')
                                operator = cond.get('operator', '=')
                                left_col_prefixed = f"__L__.{left_col}" if left_col and not left_col.startswith('__L__.') else left_col
                                right_col_prefixed = f"__R__.{right_col}" if right_col and not right_col.startswith('__R__.') else right_col
                                conditions_with_aliases.append({
                                    'leftColumn': left_col_prefixed,
                                    'rightColumn': right_col_prefixed,
                                    'operator': operator
                                })

                            # Execute join via FastAPI
                            async def execute_join_for_projection():
                                async with httpx.AsyncClient(timeout=60.0) as client:
                                    join_payload = {
                                        "db_type": db_type,
                                        "connection_config": {
                                            "hostname": source_config.get('hostname'),
                                            "port": source_config.get('port'),
                                            "database": source_config.get('database'),
                                            "user": source_config.get('user'),
                                            "password": source_config.get('password'),
                                            "schema": source_config.get('schema'),
                                            "service_name": source_config.get('service_name'),
                                        },
                                        "left_table": left_table_info['table_name'],
                                        "right_table": right_table_info['table_name'],
                                        "left_schema": left_table_info.get('schema') or source_config.get('schema', 'public'),
                                        "right_schema": right_table_info.get('schema') or source_config.get('schema', 'public'),
                                        "join_type": join_type,
                                        "conditions": conditions_with_aliases,
                                        "output_columns": join_config.get('outputColumns'),
                                        "page": page,
                                        "page_size": page_size,
                                    }

                                    EXTRACTION_SERVICE_URL = getattr(settings, 'FASTAPI_EXTRACTION_SERVICE_URL', 'http://localhost:8001')
                                    response = await client.post(
                                        f"{EXTRACTION_SERVICE_URL}/metadata/join",
                                        json=join_payload
                                    )
                                    response.raise_for_status()
                                    return response.json()

                            join_result = asyncio.run(execute_join_for_projection())
                            join_rows = join_result.get('rows', [])
                            join_columns = join_result.get('columns', [])

                            logger.info(f"Join executed for projection: {len(join_rows)} rows, {len(join_columns)} columns")

                            # Cache join results using adaptive cache
                            if canvas_id and adaptive_cache:
                                # Estimate input rows from left/right nodes
                                left_cached = adaptive_cache.get_cache(str(canvas_id), left_node.get('id'), "", "")
                                right_cached = adaptive_cache.get_cache(str(canvas_id), right_node.get('id'), "", "")
                                input_rows = max(
                                    left_cached.get('metadata', {}).get('row_count', 0) if left_cached else 0,
                                    right_cached.get('metadata', {}).get('row_count', 0) if right_cached else 0
                                )

                                save_to_adaptive_cache(
                                    node_id=input_node.get('id'),
                                    node_type='join',
                                    node_config=join_config,
                                    rows=join_rows,
                                    columns=join_columns,
                                    upstream_node_ids=[left_node.get('id'), right_node.get('id')],
                                    input_rows=input_rows
                                )

                        except Exception as e:
                            logger.error(f"Error executing join for projection: {e}")
                            import traceback
                            logger.error(traceback.format_exc())
                            return Response(
                                {"error": f"Failed to execute join for projection: {e!s}"},
                                status=status.HTTP_500_INTERNAL_SERVER_ERROR
                            )

                    # Ensure join_rows and join_columns are defined
                    if join_rows is None or join_columns is None:
                        return Response(
                            {"error": "Failed to get join results for projection"},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR
                        )

                    # Now apply projection on join results
                    logger.info(f"Applying projection on {len(join_rows)} join rows with {len(join_columns)} columns")
                    logger.info(f"Selected columns for projection: {selected_columns[:10]}...")

                    # Filter rows to only include selected columns
                    projected_rows = []
                    for row in join_rows:
                        projected_row = {}
                        for col in selected_columns:
                            if col in row:
                                projected_row[col] = row[col]
                        projected_rows.append(projected_row)

                    # Filter columns to only selected ones
                    projected_columns = [col for col in selected_columns if col in join_columns]

                    # Add calculated columns if any
                    if valid_calculated_columns:
                        logger.info(f"Evaluating {len(valid_calculated_columns)} calculated columns on join results")
                        for calc_col in valid_calculated_columns:
                            calc_name = calc_col['name']
                            calc_expression = calc_col['expression']

                            # Add column to output
                            projected_columns.append(calc_name)

                            # Evaluate expression for each row
                            for row in projected_rows:
                                try:
                                    row[calc_name] = evaluate_calculated_expression(
                                        calc_expression,
                                        row,
                                        all_available_columns
                                    )
                                except Exception as e:
                                    logger.warning(f"Error evaluating calculated column {calc_name}: {e}")
                                    row[calc_name] = None

                    # Build output metadata
                    output_metadata_columns = []
                    for col in projected_columns:
                        calc_col_match = next((cc for cc in valid_calculated_columns if cc['name'] == col), None)
                        if calc_col_match:
                            # Calculated column: business name and technical_name are the same; db_name is not applicable
                            output_metadata_columns.append({
                                'name': col,
                                'business_name': col,
                                'technical_name': col,
                                'datatype': calc_col_match.get('dataType', 'STRING'),
                                'source': 'calculated',
                                'expression': calc_col_match['expression'],
                                'nullable': True,
                            })
                        else:
                            # Base column: at this stage we only know the projected name; treat it as both business and technical
                            output_metadata_columns.append({
                                'name': col,
                                'business_name': col,
                                'technical_name': col,
                                'datatype': 'TEXT',
                                'source': 'base',
                                'nullable': True,
                            })

                    # Cache projection results using adaptive cache
                    if canvas_id and adaptive_cache:
                        # Get input row count from upstream cache
                        input_rows = 0
                        if input_node:
                            upstream_cached = adaptive_cache.get_cache(str(canvas_id), input_node.get('id'), "", "")
                            if upstream_cached:
                                input_rows = upstream_cached.get('metadata', {}).get('row_count', len(projected_rows))

                        save_to_adaptive_cache(
                            node_id=target_node_id,
                            node_type='projection',
                            node_config=projection_config,
                            rows=projected_rows,
                            columns=projected_columns,
                            upstream_node_ids=[input_node.get('id')] if input_node else [],
                            input_rows=input_rows
                        )

                    return Response({
                        "rows": projected_rows,
                        "columns": projected_columns,
                        "has_more": False,  # TODO: Implement pagination for projection after join
                        "total": len(projected_rows),
                        "page": page,
                        "page_size": page_size,
                        "from_cache": False,
                        "output_metadata": {"columns": output_metadata_columns}
                    }, status=status.HTTP_200_OK)
                else:
                    return Response(
                        {"error": f"Projection input type '{input_node_type}' is not supported. Only 'source', 'filter', 'projection', and 'join' are supported."},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                if not source_id or not table_name:
                    return Response(
                        {"error": "Source node must have sourceId and tableName configured"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Connect to customer's database to get source connection
                from django.conf import settings
                import psycopg2

                conn = psycopg2.connect(
                    host=settings.DATABASES['default']['HOST'],
                    port=settings.DATABASES['default']['PORT'],
                    user=settings.DATABASES['default']['USER'],
                    password=settings.DATABASES['default']['PASSWORD'],
                    database=customer.cust_db
                )
                conn.autocommit = True
                db_cursor = conn.cursor()

                # Get source connection details
                db_cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'GENERAL' AND table_name = 'source'
                """)
                columns = [row[0] for row in db_cursor.fetchall()]
                config_column = 'source_config' if 'source_config' in columns else 'src_config'

                # NOTE:
                # The 'GENERAL.source' table uses 'id' as the primary key column.
                # Frontend passes this value as 'sourceId'. There is no 'source_id'
                # column, so we must query by 'id' and we only need the config column.
                # IMPORTANT: We must use the same timestamp that was used when encrypting
                # the source_config. That value is stored in the source table's created_on
                # column, not the customer's created_on. Using the wrong timestamp will
                # cause authentication tag mismatch errors during decryption.
                db_cursor.execute(f'''
                    SELECT {name_column}, {config_column}, created_on
                    FROM "GENERAL".source
                    WHERE id = %s
                ''', (source_id,))

                source_row = db_cursor.fetchone()
                if not source_row:
                    return Response(
                        {"error": f"Source connection {source_id} not found"},
                        status=status.HTTP_404_NOT_FOUND
                    )

                source_name = source_row[0]
                source_config_encrypted = source_row[1]
                source_created_on = source_row[2]

                # Decrypt source config using the SAME created_on timestamp that was used
                # when the source was inserted (see SourceConnectionCreateView)
                source_config = decrypt_source_data(source_config_encrypted, customer.cust_id, source_created_on)

                if not source_config:
                    db_cursor.close()
                    conn.close()
                    return Response(
                        {
                            "error": "Failed to decrypt source configuration",
                            "details": "The source connection credentials could not be decrypted. Please re-create the source connection."
                        },
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )

                # Log decrypted config keys (but not values for security)
                logger.info(f"Projection: Decrypted source_config keys: {list(source_config.keys())}")
                logger.info(f"Projection: Password field present: {'password' in source_config}")
                logger.info(f"Projection: Password value length: {len(str(source_config.get('password', ''))) if source_config.get('password') else 0}")

                # Check if password is missing or empty
                password_value = source_config.get('password')
                # Check for None, empty string, or whitespace-only string
                if password_value is None or (isinstance(password_value, str) and password_value.strip() == ''):
                    logger.warning(f"Projection: Source config missing password for source_id={source_id}, source_name={source_name}")
                    logger.warning(f"Projection: Source config keys: {list(source_config.keys())}")
                    logger.warning(f"Projection: Source config password type: {type(password_value)}, value: {password_value!r}")
                    db_cursor.close()
                    conn.close()
                    return Response(
                        {
                            "error": "Source connection is missing password",
                            "details": f"The source connection '{source_name}' (ID: {source_id}) does not have a password configured. Please edit the source connection and add a password, or re-create it with a password.",
                            "source_id": source_id,
                            "source_name": source_name
                        },
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Ensure password is a non-empty string before sending to FastAPI
                password_str = str(password_value).strip() if password_value is not None else ''

                # Double-check that password_str is not empty after conversion
                if not password_str:
                    logger.error(f"Projection: Password became empty after conversion. Original type: {type(password_value)}, value: {password_value!r}")
                    db_cursor.close()
                    conn.close()
                    return Response(
                        {
                            "error": "Source connection password is invalid",
                            "details": f"The source connection '{source_name}' (ID: {source_id}) has an invalid password. Please edit the source connection and update the password.",
                            "source_id": source_id,
                            "source_name": source_name
                        },
                        status=status.HTTP_400_BAD_REQUEST
                    )

                db_type = source_config.get('db_type', 'postgresql')

                # Build connection_data from source_config for use in schema validation and FastAPI calls
                connection_data = {
                    'db_type': db_type,
                    'hostname': source_config.get('hostname'),
                    'port': source_config.get('port'),
                    'user': source_config.get('user'),
                    'password': password_str,
                    'database': source_config.get('database'),
                    'schema': schema or source_config.get('schema', 'public'),
                    'service_name': source_config.get('service_name', '')
                }

                # Build column list for SELECT (with mappings)
                # Create mapping dict: source_col -> target_col (for renames/aliases)
                mapping_dict = {}
                for mapping in column_mappings:
                    mapping_dict[mapping.get('source')] = mapping.get('target')
                # Also apply renames from columnOrder (frontend double-click rename)
                column_order = projection_config.get('columnOrder') or []
                for col_entry in column_order:
                    if isinstance(col_entry, dict):
                        source_name = col_entry.get('name')
                        output_name = col_entry.get('outputName') or col_entry.get('output_name')
                        if source_name and output_name and str(output_name).strip() and str(output_name).strip() != source_name:
                            mapping_dict[source_name] = str(output_name).strip()

                # Handle aggregates with auto group-by if aggregates are present
                has_aggregates = aggregate_columns and len(aggregate_columns) > 0

                if has_aggregates:
                    # Validate aggregate configuration
                    is_valid, agg_errors = validate_aggregate_configuration(
                        projection_config,
                        all_available_columns
                    )
                    if not is_valid:
                        logger.error(f"Aggregate configuration errors: {agg_errors}")
                        return Response(
                            {"error": "Invalid aggregate configuration", "details": agg_errors},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                    # Apply auto group-by rule
                    if not group_by_columns:
                        # Build column metadata for auto group-by
                        column_metadata = []
                        for col in selected_columns:
                            column_metadata.append({'name': col})
                        group_by_columns = apply_auto_group_by_rule(projection_config, column_metadata)
                        logger.info(f"Auto group-by applied: {group_by_columns}")

                    # Build SELECT clause with aggregates
                    select_fields, group_by_fields = build_group_by_sql(
                        selected_columns,
                        aggregate_columns,
                        db_type.upper()
                    )
                    select_parts = select_fields
                else:
                    # ============================================================
                    # EXPRESSION-DRIVEN PROJECTION MODEL
                    # ============================================================
                    # Build SELECT clause from output_metadata.columns, treating:
                    # - Base columns: SELECT "column_name"
                    # - Calculated columns: SELECT <expression> AS "column_name"
                    #
                    # This ensures calculated columns are executed as SQL expressions,
                    # not treated as physical columns.
                    # ============================================================

                    # Step 1: Build output metadata from node configuration
                    # This is the SINGLE SOURCE OF TRUTH for what columns exist and how they're computed
                    output_metadata_columns = []

                    # CRITICAL FIX: Always rebuild output_metadata_columns from selected_columns to avoid stale metadata
                    # This ensures we use the current selected columns, not stale saved metadata
                    # Only use saved metadata if selected_columns is empty (shouldn't happen, but safety check)
                    if selected_columns and len(selected_columns) > 0:
                        logger.info(f"[Projection] Rebuilding output_metadata from selected_columns ({len(selected_columns)} columns)")

                        # Build a set of calculated column names for quick lookup
                        calculated_col_names = {cc['name'] for cc in valid_calculated_columns} if valid_calculated_columns else set()

                        # CRITICAL FIX: Build output metadata with calculated columns ALWAYS at the end
                        # Step 1: Process ALL selected_columns, but SKIP calculated columns (they'll be added at end)
                        # Initialize output_metadata_columns as empty list
                        output_metadata_columns = []

                        for col in selected_columns:
                            # Skip calculated columns - they'll be added at the end
                            if col in calculated_col_names:
                                continue

                            # This is a base column.
                            # Use mapping_dict / columnOrder renames for the *business* (display) name,
                            # but keep the original column name as technical_name for lineage / SQL.
                            display_name = mapping_dict.get(col, col)

                            # Try to get datatype/db_name from input node metadata
                            input_col_meta = None
                            if projection_columns_metadata:
                                input_col_meta = next((c for c in projection_columns_metadata if c.get('name') == col), None)

                            col_entry = {
                                'name': display_name,
                                'business_name': display_name,
                                'technical_name': col,
                                'datatype': input_col_meta.get('datatype', 'TEXT') if input_col_meta else 'TEXT',
                                'source': 'base',
                                'nullable': input_col_meta.get('nullable', True) if input_col_meta else True,
                            }
                            if input_col_meta is not None and input_col_meta.get('db_name') is not None:
                                col_entry['db_name'] = input_col_meta.get('db_name')

                            output_metadata_columns.append(col_entry)
                            logger.debug(f"[Projection] Added base column to metadata: {col} (display: {display_name})")

                        # Step 2: Append ALL calculated columns at the end (regardless of whether they're in selected_columns)
                        # This ensures calculated columns always appear at the end of the projection fields
                        if valid_calculated_columns and len(valid_calculated_columns) > 0:
                            logger.info(f"[Projection] Adding {len(valid_calculated_columns)} calculated columns to metadata")
                            for calc_col in valid_calculated_columns:
                                output_metadata_columns.append({
                                    'name': calc_col['name'],
                                    'business_name': calc_col['name'],
                                    'technical_name': calc_col['name'],
                                    'datatype': calc_col.get('dataType', 'STRING'),
                                    'source': 'calculated',
                                    'expression': calc_col['expression'],
                                    'nullable': True
                                })
                                logger.info(f"[Projection] Added calculated column to metadata (at end): {calc_col['name']} = {calc_col['expression']}")

                        logger.info(f"[Projection] Built output_metadata from selected_columns: {len(output_metadata_columns)} columns (base: {len([c for c in output_metadata_columns if c.get('source') == 'base'])}, calculated: {len([c for c in output_metadata_columns if c.get('source') == 'calculated'])})")

                    # Fallback: Get projection output metadata if selected_columns is empty (shouldn't happen)
                    elif projection_metadata and 'columns' in projection_metadata:
                        logger.warning("[Projection] ⚠️ FALLBACK: selected_columns is empty, using saved metadata. This may contain stale columns!")
                        output_metadata_columns = projection_metadata['columns']
                        logger.info(f"[Projection] Using output_metadata from node config: {len(output_metadata_columns)} columns")
                        logger.info(f"[Projection] selected_columns count: {len(selected_columns) if selected_columns else 0}")
                        logger.info(f"[Projection] selected_columns: {selected_columns[:10] if selected_columns else 'None'}")
                        logger.info(f"[Projection] output_metadata_columns (from saved): {[c.get('name') for c in output_metadata_columns[:10]]}")
                        logger.info(f"[Projection] Base columns in saved metadata: {[c.get('name') for c in output_metadata_columns if c.get('source') != 'calculated'][:10]}")
                        logger.info(f"[Projection] Calculated columns in saved metadata: {[c.get('name') for c in output_metadata_columns if c.get('source') == 'calculated']}")

                        # DEBUG: Log the actual structure of columns to see if source/expression are present
                        logger.info("[Projection DEBUG] First 3 columns structure:")
                        for idx, col in enumerate(output_metadata_columns[:5]):
                            logger.info(f"  [{idx}] {col}")

                        # DEBUG: Log output_metadata_from_node structure
                        logger.info(f"[Projection DEBUG] output_metadata_from_node keys: {list(output_metadata_from_node.keys()) if output_metadata_from_node else 'None'}")
                        logger.info(f"[Projection DEBUG] output_metadata_cols count: {len(output_metadata_cols) if output_metadata_cols else 0}")
                        if output_metadata_cols:
                            logger.info("[Projection DEBUG] First 3 output_metadata_cols structure:")
                            for idx, col in enumerate(output_metadata_cols[:5]):
                                logger.info(f"  [{idx}] {col}")

                        # CRITICAL FIX: Check for calculated columns already in projection_metadata['columns']
                        # These may have source='calculated' and expression already set (saved from previous sessions)
                        existing_calculated_in_metadata = [col for col in output_metadata_columns if col.get('source') == 'calculated' and col.get('expression')]
                        if existing_calculated_in_metadata:
                            logger.info(f"[Projection] Found {len(existing_calculated_in_metadata)} calculated columns already in projection.columns:")
                            for calc_col in existing_calculated_in_metadata:
                                logger.info(f"  - {calc_col.get('name')}: {calc_col.get('expression')}")
                                # Ensure these are in valid_calculated_columns for later processing
                                if not any(vc.get('name') == calc_col.get('name') for vc in valid_calculated_columns):
                                    valid_calculated_columns.append({
                                        'name': calc_col.get('name'),
                                        'expression': calc_col.get('expression'),
                                        'dataType': calc_col.get('datatype', 'STRING')
                                    })
                                    logger.info(f"[Projection] Added calculated column from projection.columns to valid_calculated_columns: {calc_col.get('name')}")

                        # CRITICAL FIX: Also merge calculated columns from output_metadata_from_node (target_node_data.output_metadata)
                        # This is a different path that may have calculated columns stored
                        if output_metadata_from_node and output_metadata_cols:
                            for col_meta in output_metadata_cols:
                                if col_meta.get('source') == 'calculated' and col_meta.get('expression'):
                                    calc_name = col_meta.get('name')
                                    # Check if already in output_metadata_columns
                                    existing_in_output = next((c for c in output_metadata_columns if c.get('name') == calc_name), None)
                                    if existing_in_output:
                                        # Update existing column with calculated info
                                        if existing_in_output.get('source') != 'calculated':
                                            existing_in_output['source'] = 'calculated'
                                            existing_in_output['expression'] = col_meta.get('expression')
                                            logger.info(f"[Projection] Updated column '{calc_name}' to calculated from output_metadata")
                                    else:
                                        # Add new calculated column
                                        output_metadata_columns.append({
                                            'name': calc_name,
                                            'datatype': col_meta.get('datatype', 'STRING'),
                                            'source': 'calculated',
                                            'expression': col_meta.get('expression'),
                                            'nullable': True
                                        })
                                        logger.info(f"[Projection] Added calculated column from output_metadata: {calc_name}")

                                    # Also ensure it's in valid_calculated_columns
                                    if not any(vc.get('name') == calc_name for vc in valid_calculated_columns):
                                        valid_calculated_columns.append({
                                            'name': calc_name,
                                            'expression': col_meta.get('expression'),
                                            'dataType': col_meta.get('datatype', 'STRING')
                                        })

                        # CRITICAL FIX: Rebuild output_metadata_columns with calculated columns ALWAYS at end
                        # Separate base columns from calculated columns, then rebuild with calculated at end
                        if valid_calculated_columns and len(valid_calculated_columns) > 0:
                            logger.info("[Projection] Rebuilding output_metadata with calculated columns at end")
                            calculated_col_dict = {cc['name']: cc for cc in valid_calculated_columns}
                            calculated_col_names_set = set(calculated_col_dict.keys())

                            # CRITICAL FIX: First, ensure ALL base columns from selected_columns are included
                            # Build base columns from selected_columns (source of truth for what should be projected)
                            base_columns_from_selected = []
                            logger.info(f"[Projection Rebuild] Building base columns from selected_columns: {len(selected_columns) if selected_columns else 0} columns")
                            logger.info(f"[Projection Rebuild] selected_columns: {selected_columns[:10] if selected_columns else 'None'}")

                            if not selected_columns or len(selected_columns) == 0:
                                logger.warning("[Projection Rebuild] ⚠️ selected_columns is empty! This will cause only calculated columns to show.")
                                logger.warning("[Projection Rebuild] Checking projection_columns_metadata for base columns...")
                                # Fallback: Get base columns from projection_columns_metadata
                                if projection_columns_metadata:
                                    for col_meta in projection_columns_metadata:
                                        col_name = col_meta.get('name')
                                        if col_name and col_name not in calculated_col_names_set:
                                            base_columns_from_selected.append({
                                                'name': col_name,
                                                'datatype': col_meta.get('datatype', 'TEXT'),
                                                'source': 'base',
                                                'nullable': col_meta.get('nullable', True)
                                            })
                                    logger.info(f"[Projection Rebuild] Fallback: Added {len(base_columns_from_selected)} base columns from projection_columns_metadata")

                            for col in (selected_columns or []):
                                # Skip calculated columns - they'll be added at the end
                                if col in calculated_col_names_set:
                                    continue

                                # Check if this column already exists in output_metadata_columns
                                existing_col = next((c for c in output_metadata_columns if c.get('name') == col), None)
                                if existing_col:
                                    # Use existing metadata but ensure it's marked as base
                                    if existing_col.get('source') != 'calculated':
                                        base_columns_from_selected.append(existing_col)
                                else:
                                    # Add new base column metadata
                                    input_col_meta = None
                                    if projection_columns_metadata:
                                        input_col_meta = next((c for c in projection_columns_metadata if c.get('name') == col), None)

                                    base_columns_from_selected.append({
                                        'name': col,
                                        'datatype': input_col_meta.get('datatype', 'TEXT') if input_col_meta else 'TEXT',
                                        'source': 'base',
                                        'nullable': input_col_meta.get('nullable', True) if input_col_meta else True
                                    })

                            logger.info(f"[Projection Rebuild] Built {len(base_columns_from_selected)} base columns: {[c.get('name') for c in base_columns_from_selected]}")

                            # Separate calculated columns from existing output_metadata_columns
                            existing_calculated_columns = []
                            for col_meta in output_metadata_columns:
                                col_name = col_meta.get('name')
                                if col_name in calculated_col_dict:
                                    # Update calculated column metadata
                                    calc_col = calculated_col_dict[col_name]
                                    existing_calculated_columns.append({
                                        'name': col_name,
                                        'datatype': calc_col.get('dataType', 'STRING'),
                                        'source': 'calculated',
                                        'expression': calc_col['expression'],
                                        'nullable': True
                                    })
                                    logger.info(f"[Projection] Updated calculated column '{col_name}': {calc_col['expression']}")
                                elif col_meta.get('source') == 'calculated':
                                    # Existing calculated column not in valid_calculated_columns (might be stale)
                                    # Keep it but will be moved to end
                                    existing_calculated_columns.append(col_meta)
                                    # CRITICAL FIX: Also add to valid_calculated_columns so it gets evaluated
                                    if not any(vc.get('name') == col_name for vc in valid_calculated_columns):
                                        valid_calculated_columns.append({
                                            'name': col_name,
                                            'expression': col_meta.get('expression', ''),
                                            'dataType': col_meta.get('datatype', 'STRING')
                                        })
                                        logger.info(f"[Projection] Added existing calculated column to valid_calculated_columns for evaluation: {col_name}")

                            # Rebuild: base columns first (from selected_columns), then calculated columns at end
                            output_metadata_columns = base_columns_from_selected.copy()

                            # CRITICAL FIX: Add all calculated columns at the end
                            # First, add calculated columns from valid_calculated_columns
                            for calc_col in valid_calculated_columns:
                                calc_name = calc_col['name']
                                # Check if already added
                                if not any(c.get('name') == calc_name for c in output_metadata_columns):
                                    output_metadata_columns.append({
                                        'name': calc_col['name'],
                                        'datatype': calc_col.get('dataType', 'STRING'),
                                        'source': 'calculated',
                                        'expression': calc_col['expression'],
                                        'nullable': True
                                    })
                                    logger.info(f"[Projection] Added calculated column to metadata (at end): {calc_col['name']} = {calc_col['expression']}")

                            # CRITICAL FIX: Also add existing calculated columns that were found in saved metadata
                            # but might not be in valid_calculated_columns (e.g., if config wasn't sent)
                            for existing_calc in existing_calculated_columns:
                                calc_name = existing_calc.get('name')
                                # Check if already added (might have been added from valid_calculated_columns above)
                                if not any(c.get('name') == calc_name for c in output_metadata_columns):
                                    output_metadata_columns.append(existing_calc)
                                    logger.info(f"[Projection] Added existing calculated column from saved metadata: {calc_name} = {existing_calc.get('expression')}")

                            logger.info(f"[Projection] Rebuilt metadata: {len(base_columns_from_selected)} base + {len([c for c in output_metadata_columns if c.get('source') == 'calculated'])} calculated columns")
                        else:
                            # Build output metadata from selected columns + calculated columns
                            # CRITICAL FIX: Also check output_metadata_from_node for calculated columns
                            if output_metadata_from_node and output_metadata_cols:
                                for col_meta in output_metadata_cols:
                                    if col_meta.get('source') == 'calculated' and col_meta.get('expression'):
                                        calc_name = col_meta.get('name')
                                        if not any(vc.get('name') == calc_name for vc in valid_calculated_columns):
                                            valid_calculated_columns.append({
                                                'name': calc_name,
                                                'expression': col_meta.get('expression'),
                                                'dataType': col_meta.get('datatype', 'STRING')
                                            })
                                            logger.info(f"[Projection] Added calculated column from output_metadata in else branch: {calc_name}")

                            # Build a set of calculated column names for quick lookup
                            calculated_col_names = {cc['name'] for cc in valid_calculated_columns} if valid_calculated_columns else set()

                            # CRITICAL FIX: Build output metadata with calculated columns ALWAYS at the end
                            # Step 1: Process ALL selected_columns, but SKIP calculated columns (they'll be added at end)
                            for col in selected_columns:
                                # Skip calculated columns - they'll be added at the end
                                if col in calculated_col_names:
                                    continue

                                # Check if already in output_metadata_columns
                                if not any(c.get('name') == col for c in output_metadata_columns):
                                    # This is a base column - add metadata without expression
                                    # Try to get datatype/db_name/technical_name from input node metadata
                                    input_col_meta = None
                                    if projection_columns_metadata:
                                        input_col_meta = next((c for c in projection_columns_metadata if c.get('name') == col), None)

                                    technical_name = (input_col_meta.get('technical_name') if input_col_meta else None) or col

                                    col_entry = {
                                        'name': col,
                                        'business_name': col,
                                        'technical_name': technical_name,
                                        'datatype': input_col_meta.get('datatype', 'TEXT') if input_col_meta else 'TEXT',
                                        'source': 'base',
                                        'nullable': input_col_meta.get('nullable', True) if input_col_meta else True,
                                    }
                                    if input_col_meta is not None and input_col_meta.get('db_name') is not None:
                                        col_entry['db_name'] = input_col_meta.get('db_name')

                                    output_metadata_columns.append(col_entry)

                            # Step 2: Add calculated columns at the end
                            if valid_calculated_columns:
                                for calc_col in valid_calculated_columns:
                                    calc_name = calc_col['name']
                                    if not any(c.get('name') == calc_name for c in output_metadata_columns):
                                        output_metadata_columns.append({
                                            'name': calc_col['name'],
                                            'business_name': calc_col['name'],
                                            'technical_name': calc_col['name'],
                                            'datatype': calc_col.get('dataType', 'STRING'),
                                            'source': 'calculated',
                                            'expression': calc_col['expression'],
                                            'nullable': True
                                        })
                                        logger.info(f"[Projection] Added calculated column to metadata (at end): {calc_col['name']} = {calc_col['expression']}")

                        # Step 2: Append ALL calculated columns at the end (regardless of whether they're in selected_columns)
                        # This ensures calculated columns always appear at the end of the projection fields
                        if valid_calculated_columns:
                            for calc_col in valid_calculated_columns:
                                output_metadata_columns.append({
                                    'name': calc_col['name'],
                                    'technical_name': calc_col['name'],
                                    'datatype': calc_col.get('dataType', 'STRING'),
                                    'source': 'calculated',
                                    'expression': calc_col['expression'],
                                    'nullable': True
                                })
                                logger.info(f"[Projection] Added calculated column to metadata (at end): {calc_col['name']} = {calc_col['expression']}")

                        logger.info(f"[Projection] Built output_metadata from config: {len(output_metadata_columns)} columns (base: {len([c for c in output_metadata_columns if c.get('source') == 'base'])}, calculated: {len([c for c in output_metadata_columns if c.get('source') == 'calculated'])})")

                    # FALLBACK DETECTION: Always check output_metadata_columns for calculated columns AFTER building it
                    # This handles cases where calculated columns are in output_metadata but weren't detected earlier
                    # CRITICAL: Check output_metadata_columns even if valid_calculated_columns is not empty,
                    # because output_metadata might have calculated columns that weren't in the initial detection
                    # ALSO: Check columns that don't have 'source' field but might be calculated (from old format)
                    if output_metadata_columns:
                        logger.info(f"[Calculated Column Fallback] Checking output_metadata_columns for calculated columns (current valid_calculated_columns: {len(valid_calculated_columns)})")
                        existing_calc_names = {cc.get('name') for cc in valid_calculated_columns if cc.get('name')}
                        found_new = False
                        for col_meta in output_metadata_columns:
                            calc_name = col_meta.get('name')

                            # Check if it's explicitly marked as calculated
                            is_calculated = col_meta.get('source') == 'calculated' and col_meta.get('expression')

                            # ALSO CHECK: If column has 'isCalculated' flag (from old format)
                            if not is_calculated:
                                is_calculated = col_meta.get('isCalculated', False) or col_meta.get('is_calculated', False)

                            if is_calculated:
                                # Skip if already in valid_calculated_columns
                                if calc_name in existing_calc_names:
                                    continue

                                # This is a calculated column that wasn't detected earlier
                                calc_expression = col_meta.get('expression', '')
                                if not calc_expression:
                                    # Try to get expression from other fields
                                    calc_expression = col_meta.get('formula') or col_meta.get('expression_text') or ''

                                if not calc_expression:
                                    logger.warning(f"[Calculated Column Fallback] Calculated column '{calc_name}' has no expression, skipping")
                                    continue

                                calc_datatype = col_meta.get('datatype', col_meta.get('dataType', 'STRING'))

                                # Check if it conflicts with source columns
                                if calc_name not in all_available_columns:
                                    valid_calculated_columns.append({
                                        'name': calc_name,
                                        'expression': calc_expression,
                                        'dataType': calc_datatype
                                    })
                                    existing_calc_names.add(calc_name)
                                    found_new = True
                                    logger.info(f"[Calculated Column Fallback] Found calculated column in output_metadata: {calc_name} = {calc_expression}")
                                else:
                                    logger.warning(f"[Calculated Column Fallback] Calculated column '{calc_name}' conflicts with source column, skipping")

                        if found_new:
                            logger.info(f"[Calculated Column Fallback] Added calculated columns from output_metadata. Total now: {len(valid_calculated_columns)}")
                            # Update calculated_col_names_set
                            calculated_col_names_set = {cc.get('name', '').strip() for cc in valid_calculated_columns if cc.get('name')}

                    # ALSO CHECK: CanvasNode.output_metadata for calculated columns
                    try:
                        from api.models.canvas import CanvasNode
                        # CRITICAL: Handle case where pipeline_nodes table doesn't exist
                        try:
                            canvas_node = CanvasNode.objects.filter(node_id=target_node_id).first()
                        except Exception as db_error:
                            # Database table might not exist - this is OK for preview/execution
                            logger.warning(f"[Calculated Column Fallback] Database query failed (table may not exist): {db_error!s}")
                            canvas_node = None
                        if canvas_node and canvas_node.output_metadata:
                            output_meta_cols = canvas_node.output_metadata.get('columns', [])
                            if output_meta_cols:
                                logger.info("[Calculated Column Fallback] Checking CanvasNode.output_metadata.columns for calculated columns")
                                existing_calc_names = {cc.get('name') for cc in valid_calculated_columns if cc.get('name')}
                                found_new = False
                                for col_meta in output_meta_cols:
                                    if col_meta.get('source') == 'calculated' and col_meta.get('expression'):
                                        calc_name = col_meta.get('name')
                                        if calc_name not in existing_calc_names and calc_name not in all_available_columns:
                                            valid_calculated_columns.append({
                                                'name': calc_name,
                                                'expression': col_meta.get('expression'),
                                                'dataType': col_meta.get('datatype', 'STRING')
                                            })
                                            existing_calc_names.add(calc_name)
                                            found_new = True
                                            logger.info(f"[Calculated Column Fallback] Found calculated column in CanvasNode.output_metadata: {calc_name}")
                                if found_new:
                                    logger.info(f"[Calculated Column Fallback] Added calculated columns from CanvasNode.output_metadata. Total now: {len(valid_calculated_columns)}")
                                    calculated_col_names_set = {cc.get('name', '').strip() for cc in valid_calculated_columns if cc.get('name')}
                    except Exception as output_meta_error:
                        logger.debug(f"[Calculated Column Fallback] Error checking CanvasNode.output_metadata: {output_meta_error}")

                    # ============================================================
                    # CRITICAL FIX: Validate base columns against actual table schema
                    # This prevents SQL errors from non-existent columns
                    # ============================================================
                    logger.info("[Schema Validation] Fetching actual table schema to validate columns")

                    # Fetch actual table schema from source
                    try:
                        # Get source connection details (already decrypted above)
                        schema_endpoint = "/table-schema"
                        schema_payload = {
                            "db_type": connection_data.get('db_type', 'postgresql'),
                            "table_name": table_name,
                            "schema": schema or connection_data.get('schema', 'public'),
                            "connection_config": connection_data
                        }

                        logger.info(f"[Schema Validation] Fetching schema for table: {table_name}")

                        # Properly handle async client with context manager
                        import asyncio

                        import httpx  # Import httpx before async function to fix scoping error
                        async def fetch_schema():
                            async with httpx.AsyncClient(timeout=30.0) as async_client:
                                return await async_client.post(
                                    f"{EXTRACTION_SERVICE_URL}{schema_endpoint}",
                                    json=schema_payload
                                )

                        schema_response = asyncio.run(fetch_schema())

                        if schema_response.status_code == 200:
                            schema_result = schema_response.json()
                            actual_table_columns = schema_result.get('columns', [])
                            actual_column_names = {col['name'].lower() for col in actual_table_columns}
                            logger.info(f"[Schema Validation] Table has {len(actual_column_names)} columns: {sorted(actual_column_names)[:10]}...")

                            # Validate output_metadata_columns against actual schema
                            validated_output_metadata = []
                            columns_without_expression = []

                            for col_meta in output_metadata_columns:
                                col_name = col_meta.get('name')
                                col_source = col_meta.get('source', 'base')

                                if col_source == 'calculated':
                                    # Calculated columns don't need to exist in table
                                    if col_meta.get('expression'):
                                        validated_output_metadata.append(col_meta)
                                    else:
                                        # Calculated column without expression - cannot be evaluated
                                        logger.error(f"[Schema Validation] ❌ Calculated column '{col_name}' has NO EXPRESSION - cannot be evaluated!")
                                        logger.error("[Schema Validation]    This is a FRONTEND BUG - calculated columns must be saved with their expressions")
                                        columns_without_expression.append(col_name)
                                        # Don't add to validated list - will be excluded from query
                                else:
                                    # Base column - must exist in table
                                    if col_name.lower() in actual_column_names:
                                        validated_output_metadata.append(col_meta)
                                        logger.debug(f"[Schema Validation] ✓ Base column '{col_name}' exists in table")
                                    else:
                                        # ENHANCED: Check if this is a calculated column that wasn't detected earlier
                                        if col_name in calculated_col_names_set:
                                            logger.info(f"[Schema Validation] Column '{col_name}' does NOT exist in table but IS a calculated column")
                                            logger.info("[Schema Validation]    Marking as calculated column - will be evaluated in Python")
                                            # Convert to calculated column metadata
                                            calc_col_info = next((cc for cc in calculated_columns if cc.get('name') == col_name), None)
                                            if calc_col_info:
                                                validated_output_metadata.append({
                                                    'name': col_name,
                                                    'datatype': calc_col_info.get('dataType', col_meta.get('datatype', 'STRING')),
                                                    'source': 'calculated',
                                                    'expression': calc_col_info.get('expression', ''),
                                                    'nullable': True
                                                })
                                                logger.info(f"[Schema Validation] ✓ Converted '{col_name}' to calculated column with expression: {calc_col_info.get('expression', '')}")
                                            else:
                                                logger.warning(f"[Schema Validation] Column '{col_name}' is in calculated_col_names_set but not found in calculated_columns list")
                                                columns_without_expression.append(col_name)
                                        else:
                                            logger.warning(f"[Schema Validation] ❌ Base column '{col_name}' does NOT exist in table '{table_name}'")
                                            logger.warning("[Schema Validation]    This column will be EXCLUDED from the query")
                                            logger.warning("[Schema Validation]    Possible causes:")
                                            logger.warning("[Schema Validation]      1. Column was deleted from table")
                                            logger.warning("[Schema Validation]      2. This is a calculated column missing its expression (FRONTEND BUG)")
                                            columns_without_expression.append(col_name)

                            # Log summary
                            excluded_count = len(output_metadata_columns) - len(validated_output_metadata)
                            if excluded_count > 0:
                                logger.warning(f"[Schema Validation] Excluded {excluded_count} columns that don't exist in table or lack expressions")
                                logger.warning(f"[Schema Validation] Excluded columns: {columns_without_expression}")

                            # Replace output_metadata_columns with validated list
                            output_metadata_columns = validated_output_metadata
                            logger.info(f"[Schema Validation] Validated: {len(output_metadata_columns)} columns will be included in query")

                        else:
                            logger.warning(f"[Schema Validation] Failed to fetch table schema: {schema_response.status_code}")
                            logger.warning("[Schema Validation] Proceeding without validation - may encounter SQL errors")

                    except Exception as schema_error:
                        logger.error(f"[Schema Validation] Error fetching table schema: {schema_error}")
                        logger.warning("[Schema Validation] Proceeding without validation - may encounter SQL errors")
                        import traceback
                        logger.error(traceback.format_exc())

                    # Step 2: Build SELECT clause from output_metadata
                    # CRITICAL FIX: Calculated columns should NOT be in SELECT clause
                    # They will be evaluated in Python AFTER fetching base data
                    # Only include BASE columns in SELECT clause

                    # CRITICAL FIX: Build comprehensive set of calculated column names from multiple sources
                    # This ensures calculated columns are filtered even if valid_calculated_columns is empty
                    # (e.g., due to false conflict rejection)
                    all_calculated_names = set()
                    # From valid_calculated_columns
                    if valid_calculated_columns:
                        all_calculated_names.update({cc.get('name') for cc in valid_calculated_columns if cc.get('name')})
                    # From output_metadata_columns (check source='calculated' and isCalculated flags)
                    for col_meta in output_metadata_columns:
                        col_name = col_meta.get('name')
                        if (col_meta.get('source') == 'calculated' or
                            col_meta.get('isCalculated') or
                            col_meta.get('is_calculated')):
                            all_calculated_names.add(col_name)
                    # From original calculated_columns (request data) - fallback
                    if calculated_columns:
                        all_calculated_names.update({cc.get('name', '').strip() for cc in calculated_columns if cc.get('name')})

                    logger.info(f"[SQL SELECT Filter] Comprehensive calculated column names set: {sorted(all_calculated_names)}")

                    select_parts = []
                    calculated_column_logs = []

                    # Separate base columns from calculated columns
                    base_columns_for_select = []
                    calculated_columns_for_eval = []

                    for col_meta in output_metadata_columns:
                        col_name = col_meta.get('name')
                        col_source = col_meta.get('source', 'base')

                        # Check if this is a calculated column using comprehensive set
                        is_calculated = (
                            col_source == 'calculated' or
                            col_meta.get('isCalculated') or
                            col_meta.get('is_calculated') or
                            col_name in all_calculated_names
                        )

                        if is_calculated:
                            # Calculated columns are NOT included in SELECT clause
                            # They will be evaluated in Python after fetching base data
                            calculated_columns_for_eval.append(col_meta)
                            logger.info(f"[Calculated Column] '{col_name}' will be evaluated in Python, NOT in SQL SELECT")
                        else:
                            # Base column - include in SELECT clause
                            base_columns_for_select.append(col_meta)

                    # Build SELECT clause with ONLY base columns
                    for col_meta in base_columns_for_select:
                        col_name = col_meta.get('name')
                        target_name = mapping_dict.get(col_name, col_name)
                        if target_name != col_name:
                            # Column is renamed
                            select_parts.append(f'"{col_name}" AS "{target_name}"')
                        else:
                            select_parts.append(f'"{col_name}"')

                    # Log calculated columns that will be evaluated (not in SELECT)
                    for col_meta in calculated_columns_for_eval:
                        col_name = col_meta.get('name')
                        calc_expression = col_meta.get('expression', '')
                        calc_data_type = col_meta.get('datatype', 'STRING')

                        if not calc_expression:
                            logger.error(f"[Projection] Calculated column '{col_name}' has no expression in metadata")
                            continue

                        logger.info(f"[Calculated Column] Will evaluate in Python: {col_name}")
                        logger.info(f"[Calculated Column] Expression: {calc_expression}")
                        logger.info(f"[Calculated Column] Data Type: {calc_data_type}")

                        calculated_column_logs.append({
                            'name': col_name,
                                'original_expression': calc_expression,
                            'transformed_sql': 'EVALUATED_IN_PYTHON',  # Not in SQL
                                'data_type': calc_data_type
                            })

                    select_clause = ', '.join(select_parts)

                    # Log generated SQL
                    logger.info("=== GENERATED SELECT SQL (Expression-Driven) ===")
                    logger.info(f"SELECT {select_clause}")
                    logger.info(f"FROM {table_name}")
                    logger.info("=" * 60)
                    logger.info(f"[SQL] Final SELECT clause: {select_clause}")
                    logger.info(f"[Calculated Column] Summary: {len(calculated_column_logs)} calculated columns")
                    for log_entry in calculated_column_logs:
                        logger.info(f"[Calculated Column] {log_entry['name']}: {log_entry['original_expression']} -> {log_entry['transformed_sql']}")
                    logger.info(f"[Projection] Output metadata columns: {[c.get('name') for c in output_metadata_columns]}")

                # Call FastAPI extraction service to get projected data
                import httpx

                EXTRACTION_SERVICE_URL = getattr(settings, 'EXTRACTION_SERVICE_URL', 'http://localhost:8001')

                try:
                    # Use filter endpoint if filters are present, otherwise table-data
                    endpoint = "/metadata/filter" if filters else "/table-data"

                    # Final validation before building connection_data
                    if not password_str or password_str.strip() == '':
                        logger.error(f"Projection: password_str is empty before building connection_data. password_value type: {type(password_value)}, value: {password_value!r}")
                        db_cursor.close()
                        conn.close()
                        return Response(
                            {
                                "error": "Database connection password is invalid",
                                "details": f"The password for source connection '{source_name}' (ID: {source_id}) is empty or invalid. Please edit the source connection and update the password.",
                                "source_id": source_id,
                                "source_name": source_name
                            },
                            status=status.HTTP_400_BAD_REQUEST
                        )

                    connection_data = {
                        "hostname": source_config.get('hostname'),
                        "port": source_config.get('port'),
                        "user": source_config.get('user'),
                        "password": password_str,  # Use validated password string
                        "database": source_config.get('database', ''),
                        "schema": source_config.get('schema', ''),
                        "service_name": source_config.get('service_name', ''),
                    }

                    # Log connection data (without password value for security)
                    logger.info(f"Projection: Connection data keys: {list(connection_data.keys())}")
                    logger.info(f"Projection: Password present in connection_data: {bool(connection_data.get('password'))}")
                    logger.info(f"Projection: Password length: {len(str(connection_data.get('password', '')))}")
                    logger.info(f"Projection: Password is empty string: {connection_data.get('password') == ''}")
                    logger.info(f"Projection: Password is None: {connection_data.get('password') is None}")

                    # Final validation: ensure password is in connection_data and is not empty
                    if not connection_data.get('password') or connection_data.get('password') == '':
                        logger.error("Projection: Password is missing or empty in connection_data before sending to FastAPI")
                        logger.error(f"Projection: connection_data keys: {list(connection_data.keys())}")
                        logger.error(f"Projection: password_str value: {password_str!r}")
                        db_cursor.close()
                        conn.close()
                        return Response(
                            {
                                "error": "Database connection password is missing",
                                "details": f"The password for source connection '{source_name}' (ID: {source_id}) is missing or empty. Please edit the source connection and update the password.",
                                "source_id": source_id,
                                "source_name": source_name
                            },
                            status=status.HTTP_400_BAD_REQUEST
                        )

                    # ============================================================
                    # PAYLOAD CONSTRUCTION (Expression-Driven Model)
                    # ============================================================
                    # Send SELECT clause to FastAPI, not column name lists.
                    # FastAPI will execute the SELECT clause directly.
                    # ============================================================

                    payload = {
                        "db_type": db_type,
                        "connection_config": connection_data,
                        "table_name": table_name,
                        "schema": schema,
                        "page": page,
                        "page_size": page_size,
                        "select_clause": select_clause,  # ALWAYS send SELECT clause (expression-driven)
                    }

                    # Add aggregate and group-by information if aggregates are present
                    if has_aggregates:
                        payload["aggregate_columns"] = aggregate_columns
                        payload["group_by_columns"] = group_by_fields if has_aggregates else []
                        logger.info(f"[Aggregates] Added aggregate_columns: {len(aggregate_columns)}")
                        logger.info(f"[Aggregates] Added group_by_columns: {group_by_fields}")

                    logger.info(f"[FastAPI] Sending SELECT clause with {len(output_metadata_columns)} columns ({len(calculated_column_logs)} calculated)")
                    logger.info(f"[FastAPI] SELECT clause: {select_clause[:200]}...")

                    # Add filters if present
                    if filters:
                        # Convert filters to legacy format for FastAPI
                        legacy_filters = []
                        for f in filters:
                            legacy_filters.append({
                                'column': f.get('column'),
                                'operator': f.get('operator'),
                                'value': f.get('value'),
                                'logicalOperator': f.get('logicalOperator', 'AND')
                            })
                        payload["filters"] = legacy_filters
                    # Note: Both /table-data and /metadata/filter endpoints use connection_config
                    # No need to rename it

                    # Log payload structure (without sensitive data)
                    payload_log = {k: v for k, v in payload.items() if k != 'connection' and k != 'connection_config'}
                    if 'connection' in payload:
                        payload_log['connection'] = {k: '***' if k == 'password' else v for k, v in payload['connection'].items()}
                    if 'connection_config' in payload:
                        payload_log['connection_config'] = {k: '***' if k == 'password' else v for k, v in payload['connection_config'].items()}
                    logger.info(f"Projection: Sending payload to {endpoint}: {payload_log}")

                    # CRITICAL FIX: If we have cached base rows (from cache hit with calculated columns),
                    # use them and reprocess calculated columns on top
                    # Note: Cached rows should have all base columns needed for calculated column evaluation
                    # If cached rows don't have all needed columns, we'll fetch fresh data
                    if cached_base_rows is not None:
                        logger.info("[Cache] Using cached base rows and reprocessing calculated columns")
                        logger.info(f"[Cache] Cached rows have columns: {list(cached_base_rows[0].keys()) if cached_base_rows else '[]'}")
                        logger.info(f"[Cache] All available columns needed: {all_available_columns[:10]}...")

                        # Check if cached rows have all columns needed for calculated column evaluation
                        cached_row_keys = set(cached_base_rows[0].keys()) if cached_base_rows else set()
                        needed_columns_set = set(all_available_columns)

                        # Check if we have all needed columns (case-insensitive)
                        cached_keys_lower = {k.lower() for k in cached_row_keys}
                        needed_lower = {c.lower() for c in needed_columns_set}
                        missing_needed = needed_lower - cached_keys_lower

                        if missing_needed:
                            logger.warning(f"[Cache] Missing columns in cache for calculated column evaluation: {list(missing_needed)[:10]}...")
                            logger.info("[Cache] Fetching fresh data from FastAPI to get all columns")
                            cached_base_rows = None  # Reset to force FastAPI call
                        else:
                            # Use cached rows - they have all needed columns
                            logger.info("[Cache] Cached rows have all needed columns, using cached data")
                            rows = cached_base_rows  # Use cached rows for calculated column reprocessing
                    else:
                        # Properly handle async client with context manager
                        import asyncio
                        async def fetch_data():
                            async with httpx.AsyncClient(timeout=30.0) as async_client:
                                return await async_client.post(
                                    f"{EXTRACTION_SERVICE_URL}{endpoint}",
                                    json=payload
                                )

                        response = asyncio.run(fetch_data())

                        if response.status_code != 200:
                            logger.error(f"FastAPI service error: {response.text}")
                            return Response(
                                {"error": f"Failed to fetch projected data: {response.text}"},
                                status=status.HTTP_500_INTERNAL_SERVER_ERROR
                            )

                        result = response.json()
                        rows = result.get('rows', [])

                        # If we were using cached_base_rows, rows is already set above
                        # Otherwise, rows comes from FastAPI response

                    # Log SQL execution results for calculated columns
                    logger.info(f"[SQL Execution] Processing {len(rows)} rows")
                    if rows:
                        row_keys = list(rows[0].keys())
                        logger.info(f"[SQL Execution] Row columns ({len(row_keys)}): {row_keys}")
                        logger.info(f"[SQL Execution] First row sample: {dict(list(rows[0].items())[:5])}")

                        # DEBUG: Show complete first row to prove what data exists
                        logger.info("[SQL DEBUG] ===== COMPLETE FIRST ROW DATA =====")
                        logger.info(f"[SQL DEBUG] Full first row: {rows[0]}")

                        # DEBUG: Specifically check for the columns that are supposedly missing
                        missing_cols_to_check = ['cmp_id', 'connection_id', 'user_id', 'terry']
                        logger.info("[SQL DEBUG] Checking for supposedly missing columns:")
                        for col in missing_cols_to_check:
                            if col in rows[0]:
                                logger.info(f"[SQL DEBUG]   ✓ '{col}' EXISTS in SQL result, value: {rows[0][col]!r}")
                            else:
                                # Check case-insensitive
                                found = False
                                for key in rows[0].keys():
                                    if key.lower() == col.lower():
                                        logger.info(f"[SQL DEBUG]   ✓ '{col}' EXISTS (as '{key}'), value: {rows[0][key]!r}")
                                        found = True
                                        break
                                if not found:
                                    logger.warning(f"[SQL DEBUG]   ✗ '{col}' DOES NOT EXIST in SQL result")

                        logger.info("[SQL DEBUG] ===== END COMPLETE ROW DATA =====")

                        # CRITICAL: Check if all_available_columns match row keys
                        logger.info(f"[SQL Execution] All available columns ({len(all_available_columns)}): {all_available_columns[:10]}...")
                        missing_in_row = [col for col in all_available_columns if col not in row_keys and col.lower() not in [k.lower() for k in row_keys]]
                        if missing_in_row:
                            logger.warning(f"[SQL Execution] Columns NOT in row keys: {missing_in_row[:10]}...")
                        # Check case-insensitive matches
                        row_keys_lower = {k.lower(): k for k in row_keys}
                        available_lower = {col.lower(): col for col in all_available_columns}
                        case_mismatches = []
                        for col_lower, col in available_lower.items():
                            if col_lower in row_keys_lower:
                                actual_key = row_keys_lower[col_lower]
                                if actual_key != col:
                                    case_mismatches.append(f"{col} -> {actual_key}")
                        if case_mismatches:
                            logger.info(f"[SQL Execution] Case mismatches: {case_mismatches[:10]}...")
                    else:
                        logger.warning("[SQL Execution] No rows returned from FastAPI")

                    # CRITICAL FIX: Check if calculated columns are already in the SQL result
                    # FastAPI might execute them, or we need to evaluate them in Python
                    # Process calculated columns in Python if they're missing from SQL result
                    # CRITICAL: Always process calculated columns, even when using cached base data

                    # CRITICAL: Also check output_metadata_columns for calculated columns detected late
                    # (via fallback detection or schema validation enhancement)
                    # ALSO: Check columns that don't exist in SQL result - they might be calculated columns
                    calculated_from_metadata = []
                    if output_metadata_columns and rows:
                        logger.info("[Calculated Column] Checking output_metadata_columns for late-detected calculated columns")
                        first_row_keys_set = set(rows[0].keys()) if rows else set()

                        for col_meta in output_metadata_columns:
                            calc_name = col_meta.get('name')

                            # Check if explicitly marked as calculated
                            is_explicitly_calculated = col_meta.get('source') == 'calculated' and col_meta.get('expression')

                            # Check if column doesn't exist in SQL result (might be calculated)
                            col_not_in_sql = calc_name not in first_row_keys_set

                            # Check if it's not a source column
                            not_source_column = calc_name not in all_available_columns

                            # CRITICAL: If explicitly marked as calculated, ALWAYS include it
                            # This ensures calculated columns from output_metadata are always evaluated
                            if is_explicitly_calculated:
                                calc_expression = col_meta.get('expression') or col_meta.get('formula', '')
                                calc_datatype = col_meta.get('datatype', col_meta.get('dataType', 'STRING'))

                                # Check if already in valid_calculated_columns
                                if not any(cc.get('name') == calc_name for cc in (valid_calculated_columns or [])):
                                    # Verify it doesn't conflict with source columns
                                    if calc_name not in all_available_columns:
                                        calculated_from_metadata.append({
                                            'name': calc_name,
                                            'expression': calc_expression,
                                            'dataType': calc_datatype
                                        })
                                        logger.info(f"[Calculated Column] Found calculated column in output_metadata (explicit): {calc_name} = {calc_expression}")
                                    else:
                                        logger.warning(f"[Calculated Column] Skipping '{calc_name}' from output_metadata - conflicts with source column")
                            # Also check if column doesn't exist in SQL and not a source column (might be calculated)
                            elif col_not_in_sql and not_source_column:
                                calc_expression = col_meta.get('expression') or col_meta.get('formula', '')

                                # Skip if no expression
                                if not calc_expression:
                                    continue

                                calc_datatype = col_meta.get('datatype', col_meta.get('dataType', 'STRING'))

                                # Check if already in valid_calculated_columns
                                if not any(cc.get('name') == calc_name for cc in (valid_calculated_columns or [])):
                                    # Verify it doesn't conflict with source columns
                                    if calc_name not in all_available_columns:
                                        calculated_from_metadata.append({
                                            'name': calc_name,
                                            'expression': calc_expression,
                                            'dataType': calc_datatype
                                        })
                                        logger.info(f"[Calculated Column] Found calculated column in output_metadata (inferred): {calc_name} = {calc_expression}")
                                    else:
                                        logger.warning(f"[Calculated Column] Skipping '{calc_name}' from output_metadata - conflicts with source column")

                    # Combine valid_calculated_columns with calculated columns from metadata
                    all_calculated_columns = (valid_calculated_columns or []) + calculated_from_metadata

                    # Log final calculated columns list for debugging
                    if all_calculated_columns:
                        logger.info(f"[Calculated Column] Final calculated columns list ({len(all_calculated_columns)} total):")
                        for idx, calc_col in enumerate(all_calculated_columns):
                            logger.info(f"  [{idx+1}] {calc_col.get('name')}: {calc_col.get('expression')}")
                    else:
                        logger.info("[Calculated Column] No calculated columns found in any location")

                    if all_calculated_columns and rows:
                        # Check if calculated columns are already in the result (from SQL execution)
                        first_row = rows[0] if rows else {}
                        calc_cols_in_result = [cc['name'] for cc in all_calculated_columns if cc['name'] in first_row]
                        calc_cols_missing = [cc['name'] for cc in all_calculated_columns if cc['name'] not in first_row]

                        if calc_cols_in_result:
                            logger.info(f"[Calculated Column] Found {len(calc_cols_in_result)} calculated columns in SQL result: {calc_cols_in_result}")
                        if calc_cols_missing:
                            logger.info(f"[Calculated Column] Need to evaluate {len(calc_cols_missing)} calculated columns in Python: {calc_cols_missing}")

                        # Always reprocess calculated columns in Python to ensure correctness
                        # (Even if SQL returned them, we want to use the mapped/normalized expression)
                        logger.info(f"[Calculated Column] Processing {len(all_calculated_columns)} calculated columns in Python")
                        {log['name']: log for log in calculated_column_logs}

                        for row_idx, row in enumerate(rows):
                            for calc_col in all_calculated_columns:
                                calc_name = calc_col['name']
                                calc_expression = calc_col['expression']

                                try:
                                    # CRITICAL: Use actual row keys + available_columns for evaluation
                                    # Row keys might differ from available_columns (case, format)
                                    actual_row_keys = list(row.keys())
                                    combined_columns = list(set(all_available_columns + actual_row_keys))

                                    # Evaluate expression with actual row values
                                    evaluated_value = evaluate_calculated_expression(
                                        calc_expression,
                                        row,
                                        combined_columns
                                    )

                                    row[calc_name] = evaluated_value

                                    # Log first 3 rows for debugging
                                    if row_idx < 3:
                                        logger.info(f"[Calc Column] Row {row_idx + 1}: {calc_name} = {evaluated_value!r}")
                                        if evaluated_value is None or evaluated_value == '':
                                            logger.warning(f"[Calc Column] Expression '{calc_expression}' returned NULL/empty")
                                            # Log input column values that are referenced in expression
                                            expr_lower = calc_expression.lower()
                                            for col in combined_columns:
                                                if col.lower() in expr_lower:
                                                    # Try to find value in row (case-insensitive)
                                                    row_value = None
                                                    for row_key, row_val in row.items():
                                                        if row_key.lower() == col.lower():
                                                            row_value = row_val
                                                            break
                                                    logger.warning(f"[Calc Column] Input column '{col}' (row key match) = {row_value!r}")

                                            # Also log all row keys for reference
                                            logger.info(f"[Calc Column] Available row keys: {actual_row_keys[:10]}...")
                                            logger.info(f"[Calc Column] Available columns list: {all_available_columns[:10]}...")

                                except Exception as e:
                                    logger.error(f"[Calculated Column] Error evaluating {calc_name}: {e!s}")
                                    import traceback
                                    logger.error(traceback.format_exc())
                                    row[calc_name] = None

                        # Log first 3 rows for calculated columns after processing
                        # Verify calculated columns are in row keys
                        if rows:
                            first_row_keys_after = list(rows[0].keys())
                            calc_col_names_in_rows = [cc['name'] for cc in all_calculated_columns if cc['name'] in first_row_keys_after]
                            logger.info(f"[Calculated Column] After evaluation - Row keys: {first_row_keys_after}")
                            logger.info(f"[Calculated Column] Calculated columns in row keys: {calc_col_names_in_rows}")

                            if len(calc_col_names_in_rows) != len(all_calculated_columns):
                                missing = [cc['name'] for cc in all_calculated_columns if cc['name'] not in first_row_keys_after]
                                logger.warning(f"[Calculated Column] Missing calculated columns in row keys: {missing}")

                        for row_idx, row in enumerate(rows[:3]):
                            logger.info(f"[Row Eval] Row {row_idx + 1} (after processing):")
                            for calc_col in all_calculated_columns:
                                calc_name = calc_col['name']
                                calc_value = row.get(calc_name)
                                if calc_value is None:
                                    logger.warning(f"[NULL CHECK] {calc_name} = NULL")
                                    # Check input columns used in expression
                                    for col in selected_columns:
                                        col_value = row.get(col)
                                        if col_value is None:
                                            logger.warning(f"[NULL CHECK] Input column '{col}' = NULL")
                                else:
                                    logger.info(f"[Row Eval] {calc_name} = {calc_value!r} (type: {type(calc_value).__name__})")

                    # ============================================================
                    # FILTER OUTPUT METADATA TO ONLY INCLUDE COLUMNS IN SQL RESULT
                    # ============================================================
                    # CRITICAL FIX: Only include columns that actually exist in SQL result
                    # or are calculated columns that will be evaluated
                    # NOTE: This happens AFTER calculated columns are evaluated, so calculated columns
                    # will be in the row keys
                    if rows:
                        # Get row keys AFTER calculated columns have been evaluated
                        first_row_keys = set(rows[0].keys()) if rows else set()
                        # Get all calculated column names (from valid_calculated_columns + output_metadata)
                        calculated_col_names = set()
                        if valid_calculated_columns:
                            calculated_col_names.update({cc['name'] for cc in valid_calculated_columns})
                        # Also add calculated columns from output_metadata
                        for col_meta in output_metadata_columns:
                            if col_meta.get('source') == 'calculated' and col_meta.get('expression'):
                                calculated_col_names.add(col_meta.get('name'))

                        logger.info(f"[Row Processing] Filtering output_metadata. Row keys: {sorted(first_row_keys)}")
                        logger.info(f"[Row Processing] Calculated column names: {sorted(calculated_col_names)}")
                        logger.info(f"[Row Processing] Output metadata columns before filtering: {[c.get('name') for c in output_metadata_columns]}")

                        # Filter output_metadata_columns to only include:
                        # 1. Base columns that exist in SQL result
                        # 2. Calculated columns (they're evaluated and added to rows)
                        # CRITICAL: Also detect calculated columns that don't have 'source' field set
                        filtered_output_metadata = []
                        for col_meta in output_metadata_columns:
                            col_name = col_meta.get('name')
                            col_source = col_meta.get('source', 'base')

                            # Check if this column is in calculated_col_names_set (from all_calculated_columns)
                            is_in_calculated_set = col_name in calculated_col_names

                            # Check if column exists in SQL result
                            col_exists_in_sql = False
                            for row_key in first_row_keys:
                                if row_key.lower() == col_name.lower():
                                    col_exists_in_sql = True
                                    break

                            # Determine if this is a calculated column
                            # It's calculated if:
                            # 1. Explicitly marked as calculated
                            # 2. In calculated_col_names set
                            # 3. Not in SQL result AND not in source columns (might be calculated)
                            is_calculated = (
                                col_source == 'calculated' or
                                is_in_calculated_set or
                                (not col_exists_in_sql and col_name not in all_available_columns and col_name in calculated_col_names_set)
                            )

                            # Always include calculated columns (they're evaluated and added to rows)
                            if is_calculated:
                                # Include if it's in valid_calculated_columns OR if it has an expression in metadata
                                # (might have been loaded from saved output_metadata)
                                if col_name in calculated_col_names or col_meta.get('expression') or is_in_calculated_set:
                                    # Ensure it has source='calculated' for consistency
                                    if col_source != 'calculated':
                                        col_meta['source'] = 'calculated'
                                    filtered_output_metadata.append(col_meta)
                                    logger.info(f"[Row Processing] Including calculated column '{col_name}' (will be evaluated)")
                                else:
                                    logger.warning(f"[Row Processing] Calculated column '{col_name}' has no expression, excluding")
                            # Only include base columns that exist in SQL result
                            else:
                                if col_exists_in_sql:
                                    filtered_output_metadata.append(col_meta)
                                else:
                                    logger.warning(f"[Row Processing] Column '{col_name}' in output_metadata but not in SQL result, excluding from response")

                        # Update output_metadata_columns to filtered list
                        if len(filtered_output_metadata) != len(output_metadata_columns):
                            logger.info(f"[Row Processing] Filtered output_metadata: {len(output_metadata_columns)} -> {len(filtered_output_metadata)} columns")
                            output_metadata_columns = filtered_output_metadata

                    # ============================================================
                    # ROW PROCESSING (Metadata-Driven)
                    # ============================================================
                    # Process rows using output_metadata_columns for ordering
                    # This ensures row key order matches the schema
                    # CRITICAL: Calculated columns are at the end of output_metadata_columns,
                    # so they will appear at the end of projected_rows
                    # ============================================================

                    projected_rows = []
                    for row in rows:
                        projected_row = {}

                        # Iterate through output_metadata_columns to preserve order
                        # This ensures calculated columns (at end) appear at end of rows
                        for col_meta in output_metadata_columns:
                            col_name = col_meta.get('name')
                            col_source = col_meta.get('source', 'base')

                            # Determine target name (apply mapping for base columns / renames)
                            if col_source == 'base':
                                target_name = mapping_dict.get(col_name, col_name)
                            else:
                                target_name = col_name

                            # Get value from row (result set keys are aliases, so use target_name for base columns)
                            col_value = None
                            lookup_key = target_name if col_source == 'base' else col_name
                            if lookup_key in row:
                                col_value = row[lookup_key]
                            elif col_name in row:
                                col_value = row[col_name]
                            else:
                                # Try case-insensitive match
                                for row_key, row_val in row.items():
                                    if row_key.lower() == lookup_key.lower() or row_key.lower() == col_name.lower():
                                        col_value = row_val
                                        break

                                # Try without table prefix if still not found
                                if col_value is None and '.' in col_name:
                                    col_name_only = col_name.split('.')[-1]
                                    if col_name_only in row:
                                        col_value = row[col_name_only]

                            # Add to projected row (even if None, to preserve schema)
                            projected_row[target_name] = col_value

                            # Log NULL calculated columns for debugging
                            if col_source == 'calculated' and col_value is None:
                                logger.debug(f"[Row Processing] Calculated column '{col_name}' is NULL in row")

                        projected_rows.append(projected_row)

                    # Column metadata map building is now obsolete
                    # Schema is built directly from output_metadata_columns (see below)

                    # ============================================================
                    # METADATA-DRIVEN SCHEMA CONSTRUCTION
                    # ============================================================
                    # Build output schema from output_metadata_columns (single source of truth)
                    # This ensures schema matches exactly what was executed in SQL
                    # ============================================================

                    projected_columns = []

                    # Use output_metadata_columns as the single source of truth for schema
                    for col_meta in output_metadata_columns:
                        col_name = col_meta.get('name')
                        col_datatype = col_meta.get('datatype', 'TEXT')
                        col_nullable = col_meta.get('nullable', True)
                        col_source = col_meta.get('source', 'base')

                        # Apply column mapping if this is a base column
                        if col_source == 'base':
                            target_name = mapping_dict.get(col_name, col_name)
                        else:
                            # Calculated columns don't get renamed
                            target_name = col_name

                        projected_columns.append({
                            'name': target_name,
                            'datatype': col_datatype,
                            'nullable': col_nullable,
                            'source': col_source
                        })

                        logger.debug(f"[Schema] {target_name}: {col_datatype} (source: {col_source})")

                    logger.info(f"[Schema] Built output schema with {len(projected_columns)} columns from metadata")
                    logger.info(f"[Schema] Column names: {[c['name'] for c in projected_columns]}")
                    logger.info(f"[Schema] Calculated columns: {[c['name'] for c in projected_columns if c.get('source') == 'calculated']}")

                    logger.info(f"Projection: Final column order for response ({len(projected_columns)})")
                    logger.info(f"Projection output columns sample: {projected_columns[:5]}...")

                    # Save to cache if canvas_id is provided
                    # CRITICAL: Cache save happens AFTER calculated columns are evaluated and added to rows
                    # projected_rows and projected_columns already include calculated columns at the end
                    if canvas_id:
                        try:
                            # Verify calculated columns are in cached data before saving
                            if projected_rows and projected_columns:
                                first_row_keys = list(projected_rows[0].keys()) if projected_rows else []
                                calc_cols_in_cache = [c['name'] for c in projected_columns if c.get('source') == 'calculated']
                                logger.info(f"[Cache Save] Saving {len(projected_rows)} rows with {len(projected_columns)} columns")
                                logger.info(f"[Cache Save] Calculated columns in cache: {calc_cols_in_cache}")
                                logger.info(f"[Cache Save] First row keys (should include calculated): {first_row_keys}")

                                # Verify calculated columns are at the end
                                if calc_cols_in_cache:
                                    base_cols = [c['name'] for c in projected_columns if c.get('source') != 'calculated']
                                    expected_order = base_cols + calc_cols_in_cache
                                    if first_row_keys == expected_order:
                                        logger.info("[Cache Save] ✓ Column order correct: base columns first, calculated at end")
                                    else:
                                        logger.warning(f"[Cache Save] ⚠ Column order mismatch. Expected: {expected_order}, Got: {first_row_keys}")

                            # Use adaptive cache
                            if adaptive_cache:
                                # Get input row count from upstream cache
                                input_rows = 0
                                if input_node:
                                    upstream_cached = adaptive_cache.get_cache(str(canvas_id), input_node.get('id'), "", "")
                                    if upstream_cached:
                                        input_rows = upstream_cached.get('metadata', {}).get('row_count', len(projected_rows))

                                save_to_adaptive_cache(
                                    node_id=target_node_id,
                                    node_type='projection',
                                    node_config=projection_config,
                                    rows=projected_rows,
                                    columns=projected_columns,
                                    upstream_node_ids=[input_node.get('id')] if input_node else [],
                                    input_rows=input_rows
                                )
                        except Exception as cache_e:
                            logger.warning(f"Failed to cache projection results: {cache_e}")

                    # ============================================================
                    # RESPONSE DEBUG: Log what's being sent to frontend
                    # ============================================================
                    logger.info("=" * 80)
                    logger.info("[RESPONSE DEBUG] Returning preview data to frontend")
                    logger.info(f"[RESPONSE DEBUG] Total rows: {len(projected_rows)}")
                    logger.info(f"[RESPONSE DEBUG] Total columns: {len(projected_columns)}")

                    # Log column details
                    calc_cols_in_response = [c for c in projected_columns if c.get('source') == 'calculated']
                    base_cols_in_response = [c for c in projected_columns if c.get('source') != 'calculated']

                    logger.info(f"[RESPONSE DEBUG] Base columns ({len(base_cols_in_response)}): {[c['name'] for c in base_cols_in_response]}")
                    logger.info(f"[RESPONSE DEBUG] Calculated columns ({len(calc_cols_in_response)}): ")
                    for cc in calc_cols_in_response:
                        # Try to get expression from column metadata
                        expr = cc.get('expression') or cc.get('formula', 'NO EXPRESSION')
                        logger.info(f"[RESPONSE DEBUG]   * {cc['name']}: {expr}")

                    # Log first row data for calculated columns
                    if projected_rows and calc_cols_in_response:
                        first_row = projected_rows[0]
                        logger.info("[RESPONSE DEBUG] First row calculated column values:")
                        for cc in calc_cols_in_response:
                            col_name = cc['name']
                            col_value = first_row.get(col_name)
                            logger.info(f"[RESPONSE DEBUG]   * {col_name} = {col_value!r}")

                    logger.info("=" * 80)

                    return Response({
                        "rows": projected_rows,
                        "columns": projected_columns,
                        "has_more": result.get('has_more', False),
                        "total": result.get('total', len(projected_rows)),
                        "page": page,
                        "page_size": page_size,
                        "from_cache": False,
                        "output_metadata": {"columns": projected_columns},  # Schema for frontend propagation
                    }, status=status.HTTP_200_OK)

                except httpx.ConnectError:
                    return Response(
                        {
                            "error": "FastAPI extraction service is not available",
                            "details": f"Could not connect to {EXTRACTION_SERVICE_URL}",
                        },
                        status=status.HTTP_503_SERVICE_UNAVAILABLE
                    )
                except Exception as e:
                    logger.error(f"Error executing projection: {e}")
                    return Response(
                        {"error": f"Failed to execute projection: {e!s}"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )

            # ------------------------------------------------------------------
            # AGGREGATE execution
            # ------------------------------------------------------------------
            if target_node_type == 'aggregate':
                logger.info("PipelineQueryExecutionView: executing aggregate pipeline")
                logger.info(f"  target_node_id={target_node_id}")

                # Get aggregate configuration
                agg_config = target_node_data.get('config', {})
                logger.info(f"[Aggregate Debug] Raw config: {agg_config}")

                aggregate_columns = agg_config.get('aggregateColumns', [])
                logger.info(f"[Aggregate Debug] Found {len(aggregate_columns)} aggregate columns")

                if not aggregate_columns:
                    logger.warning("[Aggregate Debug] No aggregate columns found in config")
                    return Response({
                        "rows": [],
                        "columns": [],
                        "has_more": False,
                        "total": 0,
                        "page": 1,
                        "page_size": 0
                    }, status=status.HTTP_200_OK)

                # Find input node (parent)
                input_edge = next((e for e in edges if e.get('target') == target_node_id), None)
                if not input_edge:
                    logger.warning("[Aggregate Debug] No input edge found")
                    return Response(
                        {"error": "Aggregate node must have an input connection"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                input_node = next((n for n in nodes if n.get('id') == input_edge.get('source')), None)
                if not input_node:
                    logger.warning(f"[Aggregate Debug] Input node not found for edge source: {input_edge.get('source')}")
                    return Response(
                        {"error": "Could not find input node for aggregate"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                input_node_type = input_node.get('data', {}).get('type')
                logger.info(f"[Aggregate Debug] Input node type: {input_node_type}, ID: {input_node.get('id')}")
                input_config = input_node.get('data', {}).get('config', {})

                # Resolve source info (Source ID, Table Name, Schema)
                source_id = None
                table_name = None
                schema = None
                filters = []

                if input_node_type == 'source':
                    source_id = input_config.get('sourceId')
                    table_name = input_config.get('tableName')
                    schema = input_config.get('schema', '')
                    logger.info(f"[Aggregate Debug] Resolved source from SOURCE node: ID={source_id}, Table={table_name}")
                elif input_node_type == 'filter':
                    filter_conditions = input_config.get('conditions', [])
                    if filter_conditions:
                        filters.extend(filter_conditions)

                    # Trace back to source
                    current_node = input_node
                    trace_path = []
                    while current_node and current_node.get('data', {}).get('type') == 'filter':
                        trace_path.append(current_node.get('id'))
                        parent_edge = next((e for e in edges if e.get('target') == current_node.get('id')), None)
                        if not parent_edge:
                            break
                        current_node = next((n for n in nodes if n.get('id') == parent_edge.get('source')), None)

                        # Add conditions from intermediate filter nodes
                        if current_node and current_node.get('data', {}).get('type') == 'filter':
                             current_conditions = current_node.get('data', {}).get('config', {}).get('conditions', [])
                             filters.extend(current_conditions)

                    if current_node and current_node.get('data', {}).get('type') == 'source':
                        source_config = current_node.get('data', {}).get('config', {})
                        source_id = source_config.get('sourceId')
                        table_name = source_config.get('tableName')
                        schema = source_config.get('schema', '')
                        logger.info(f"[Aggregate Debug] Traced back to source through filters: {trace_path} -> Source ID={source_id}")
                    else:
                        # Fallback
                         source_id = input_config.get('sourceId')
                         table_name = input_config.get('tableName')
                         schema = input_config.get('schema', '')
                         logger.warning(f"[Aggregate Debug] Could not trace to source node, using fallback config from filter: Source ID={source_id}")
                elif input_node_type == 'projection':
                    # ✅ FIX: Add support for projection inputs
                    # Trace back through projection to find source/filter
                    logger.info("[Aggregate Debug] Input is PROJECTION node, tracing back to source")
                    current_node = input_node
                    trace_path = [current_node.get('id')]

                    # Traverse backwards through the pipeline
                    while current_node:
                        parent_edge = next((e for e in edges if e.get('target') == current_node.get('id')), None)
                        if not parent_edge:
                            logger.warning(f"[Aggregate Debug] No parent edge found for node {current_node.get('id')}")
                            break

                        current_node = next((n for n in nodes if n.get('id') == parent_edge.get('source')), None)
                        if not current_node:
                            logger.warning("[Aggregate Debug] Parent node not found")
                            break

                        current_type = current_node.get('data', {}).get('type')
                        trace_path.append(f"{current_node.get('id')}:{current_type}")
                        logger.info(f"[Aggregate Debug] Traced to node type: {current_type}")

                        if current_type == 'source':
                            # Found source node
                            source_config = current_node.get('data', {}).get('config', {})
                            source_id = source_config.get('sourceId')
                            table_name = source_config.get('tableName')
                            schema = source_config.get('schema', '')
                            logger.info(f"[Aggregate Debug] Traced back to SOURCE through projection: {' -> '.join(trace_path)}, Source ID={source_id}")
                            break
                        elif current_type == 'filter':
                            # Found filter node - collect conditions
                            filter_conditions = current_node.get('data', {}).get('config', {}).get('conditions', [])
                            if filter_conditions:
                                filters.extend(filter_conditions)
                                logger.info(f"[Aggregate Debug] Collected {len(filter_conditions)} filter conditions from filter node")
                            # Continue tracing back
                        elif current_type == 'projection':
                            # Another projection, keep going back
                            logger.info("[Aggregate Debug] Found another projection, continuing trace")
                            continue
                        else:
                            # Other node types - keep tracing back
                            logger.info(f"[Aggregate Debug] Found {current_type} node, continuing trace")
                            continue

                    if not source_id or not table_name:
                        logger.warning(f"[Aggregate Debug] Could not trace to source through projection path: {' -> '.join(trace_path)}")

                if not source_id or not table_name:
                    logger.error(f"[Aggregate Debug] Failed to resolve source info. SourceID: {source_id}, Table: {table_name}")
                    return Response(
                        {"error": f"Aggregate input type '{input_node_type}' is not supported. Only 'source' and 'filter' inputs are fully supported."},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Connect to DB to get credentials
                from django.conf import settings
                import psycopg2

                conn = psycopg2.connect(
                    host=settings.DATABASES['default']['HOST'],
                    port=settings.DATABASES['default']['PORT'],
                    user=settings.DATABASES['default']['USER'],
                    password=settings.DATABASES['default']['PASSWORD'],
                    database=customer.cust_db
                )
                conn.autocommit = True
                db_cursor = conn.cursor()

                # Get source config
                db_cursor.execute('SELECT source_name, source_config, created_on FROM "GENERAL".source WHERE id = %s', (source_id,))
                source_row = db_cursor.fetchone()

                if not source_row:
                    db_cursor.close()
                    conn.close()
                    logger.error(f"[Aggregate Debug] Source ID {source_id} not found in database")
                    return Response({"error": "Source connection not found"}, status=status.HTTP_404_NOT_FOUND)

                source_name, source_config_encrypted, source_created_on = source_row
                source_config = decrypt_source_data(source_config_encrypted, customer.cust_id, source_created_on)

                if not source_config:
                    db_cursor.close()
                    conn.close()
                    logger.error("[Aggregate Debug] Failed to decrypt source config")
                    return Response({"error": "Failed to decrypt source configuration"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

                password_str = source_config.get('password')
                if not password_str:
                     db_cursor.close()
                     conn.close()
                     logger.error("[Aggregate Debug] Password missing in source config")
                     return Response({"error": "Source connection password is missing"}, status=status.HTTP_400_BAD_REQUEST)

                db_type = source_config.get('db_type', 'postgresql')
                db_cursor.close()
                conn.close()

                # Build group-by columns
                all_group_by_cols = set()
                for agg in aggregate_columns:
                    if 'groupBy' in agg and isinstance(agg['groupBy'], list):
                        for gb in agg['groupBy']:
                            all_group_by_cols.add(gb)

                group_by_columns = list(all_group_by_cols)
                logger.info(f"[Aggregate Debug] Resolved Group By columns: {group_by_columns}")

                # Build SELECT clause
                select_parts = []

                # Group By columns first
                for gb_col in group_by_columns:
                    select_parts.append(f'"{gb_col}"')

                # Aggregate columns
                for agg in aggregate_columns:
                    func = agg.get('function', '').upper()
                    col = agg.get('column', '').strip()
                    alias = agg.get('alias', '').strip()

                    # Handle COUNT function - allow empty column or '*' for counting all rows
                    if func == 'COUNT':
                        if not col or col == '*' or col == '':
                            expr = 'COUNT(*)'
                        else:
                            # COUNT with column name counts non-null values in that column
                            expr = f'COUNT("{col}")'
                    elif func == 'COUNT_DISTINCT':
                        if not col or col == '*' or col == '':
                            # COUNT_DISTINCT with no column doesn't make sense, default to COUNT(*)
                            expr = 'COUNT(*)'
                        else:
                            expr = f'COUNT(DISTINCT "{col}")'
                    else:
                        # For SUM, AVG, MIN, MAX - column is required
                        if not col:
                            logger.warning(f"[Aggregate Debug] {func} requires a column, skipping aggregate: {agg}")
                            continue
                        expr = f'{func}("{col}")'

                    if alias:
                        expr += f' AS "{alias}"'

                    select_parts.append(expr)

                select_clause = ', '.join(select_parts)
                logger.info(f"[Aggregate Debug] Constructed SELECT clause: {select_clause}")

                # Call FastAPI
                import httpx
                EXTRACTION_SERVICE_URL = getattr(settings, 'EXTRACTION_SERVICE_URL', 'http://localhost:8001')

                try:
                    endpoint = "/metadata/filter" if filters else "/table-data"
                    logger.info(f"[Aggregate Debug] Using endpoint: {endpoint}")

                    connection_data = {
                        "hostname": source_config.get('hostname'),
                        "port": source_config.get('port'),
                        "user": source_config.get('user'),
                        "password": password_str,
                        "database": source_config.get('database', ''),
                        "schema": source_config.get('schema', ''),
                        "service_name": source_config.get('service_name', ''),
                    }

                    columns_for_fastapi = []
                    if 'columns' in input_config:
                        # Use input node columns if available to pass to FastAPI
                        columns_for_fastapi = [c['name'] if isinstance(c, dict) else c for c in input_config['columns']]

                    legacy_filters = []
                    if filters:
                         for f in filters:
                            legacy_filters.append({
                                'column': f.get('column'),
                                'operator': f.get('operator'),
                                'value': f.get('value'),
                                'logicalOperator': f.get('logicalOperator', 'AND')
                            })
                         logger.info(f"[Aggregate Debug] Applying {len(legacy_filters)} filters")

                    payload = {
                        "db_type": db_type,
                        "connection_config": connection_data,
                        "table_name": table_name,
                        "schema": schema,
                        "page": page,
                        "page_size": page_size,
                        "columns": columns_for_fastapi,
                        "aggregate_columns": aggregate_columns,
                        "group_by_columns": group_by_columns,
                        "select_clause": select_clause,
                        "filters": legacy_filters
                    }

                    # Log payload (exclude password)
                    debug_payload = payload.copy()
                    if 'connection_config' in debug_payload:
                        debug_payload['connection_config'] = debug_payload['connection_config'].copy()
                        debug_payload['connection_config']['password'] = '******'
                    logger.info(f"[Aggregate Debug] FastAPI Payload: {debug_payload}")

                    async def fetch_data():
                        async with httpx.AsyncClient(timeout=60.0) as client:
                            resp = await client.post(f"{EXTRACTION_SERVICE_URL}{endpoint}", json=payload)
                            return resp

                    import asyncio
                    response = asyncio.run(fetch_data())

                    if response.status_code != 200:
                        logger.error(f"[Aggregate Debug] FastAPI Error: Status {response.status_code}, Response: {response.text}")
                        return Response({"error": f"Failed to fetch aggregate data: {response.text}"}, status=500)

                    result = response.json()
                    rows = result.get('rows', [])
                    logger.info(f"[Aggregate Debug] Received {len(rows)} rows from FastAPI")
                    if rows:
                        logger.info(f"[Aggregate Debug] First row sample: {rows[0]}")

                    # Build output columns metadata
                    output_columns = []
                    for gb_col in group_by_columns:
                        output_columns.append({'name': gb_col, 'datatype': 'TEXT'})
                    for agg in aggregate_columns:
                        output_columns.append({
                            'name': agg.get('alias') or f"{agg.get('function')}_{agg.get('column')}",
                            'datatype': 'INTEGER' if agg.get('function') == 'COUNT' else 'NUMERIC'
                        })

                    logger.info(f"[Aggregate Debug] Output columns metadata: {output_columns}")

                    # Cache using adaptive cache
                    if canvas_id and adaptive_cache:
                        # Get input row count from upstream cache
                        input_rows = 0
                        if input_node:
                            upstream_cached = adaptive_cache.get_cache(str(canvas_id), input_node.get('id'), "", "")
                            if upstream_cached:
                                input_rows = upstream_cached.get('metadata', {}).get('row_count', len(rows))

                        save_to_adaptive_cache(
                            node_id=target_node_id,
                            node_type='aggregate',
                            node_config=agg_config,
                            rows=rows,
                            columns=output_columns,
                            upstream_node_ids=[input_node.get('id')] if input_node else [],
                            input_rows=input_rows
                        )

                    return Response({
                        "rows": rows,
                        "columns": output_columns,
                        "total": result.get('total', len(rows)),
                        "page": page,
                        "page_size": page_size,
                        "from_cache": False
                    }, status=status.HTTP_200_OK)

                except Exception as e:
                     logger.error(f"[Aggregate Debug] Aggregate execution exception: {e}")
                     import traceback
                     logger.error(traceback.format_exc())
                     return Response({"error": str(e)}, status=500)

            # ------------------------------------------------------------------
            # COMPUTE execution
            # ------------------------------------------------------------------
            if target_node_type == 'compute':
                logger.info("PipelineQueryExecutionView: executing compute pipeline")
                logger.info(f"  target_node_id={target_node_id}")

                # Get compute configuration
                compute_config = target_node_data.get('config', {})
                code = compute_config.get('code', '')

                if not code or not code.strip():
                    return Response(
                        {"error": "Compute node has no code configured"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Find input node (parent)
                input_edge = next((e for e in edges if e.get('target') == target_node_id), None)
                if not input_edge:
                    return Response(
                        {"error": "Compute node must have an input connection"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                input_node = next((n for n in nodes if n.get('id') == input_edge.get('source')), None)
                if not input_node:
                    return Response(
                        {"error": "Could not find input node for compute"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                input_node_type = input_node.get('data', {}).get('type')
                input_config = input_node.get('data', {}).get('config', {})

                # Resolve source info (Source ID, Table Name, Schema)
                source_id = None
                table_name = None
                schema = None
                filters = []

                # Reuse the source resolution logic (similar to aggregate)
                if input_node_type == 'source':
                    source_id = input_config.get('sourceId')
                    table_name = input_config.get('tableName')
                    schema = input_config.get('schema', '')
                elif input_node_type == 'filter':
                    filter_conditions = input_config.get('conditions', [])
                    if filter_conditions:
                        filters.extend(filter_conditions)

                    # Trace back to source
                    current_node = input_node
                    while current_node and current_node.get('data', {}).get('type') == 'filter':
                        parent_edge = next((e for e in edges if e.get('target') == current_node.get('id')), None)
                        if not parent_edge:
                            break
                        current_node = next((n for n in nodes if n.get('id') == parent_edge.get('source')), None)

                        if current_node and current_node.get('data', {}).get('type') == 'filter':
                             current_conditions = current_node.get('data', {}).get('config', {}).get('conditions', [])
                             filters.extend(current_conditions)

                    if current_node and current_node.get('data', {}).get('type') == 'source':
                        source_config = current_node.get('data', {}).get('config', {})
                        source_id = source_config.get('sourceId')
                        table_name = source_config.get('tableName')
                        schema = source_config.get('schema', '')
                    else:
                         source_id = input_config.get('sourceId')
                         table_name = input_config.get('tableName')
                         schema = input_config.get('schema', '')
                elif input_node_type == 'projection':
                    # ✅ FIX: Add support for projection inputs to compute node
                    current_node = input_node

                    while current_node:
                        parent_edge = next((e for e in edges if e.get('target') == current_node.get('id')), None)
                        if not parent_edge:
                            break

                        current_node = next((n for n in nodes if n.get('id') == parent_edge.get('source')), None)
                        if not current_node:
                            break

                        current_type = current_node.get('data', {}).get('type')

                        if current_type == 'source':
                            source_config = current_node.get('data', {}).get('config', {})
                            source_id = source_config.get('sourceId')
                            table_name = source_config.get('tableName')
                            schema = source_config.get('schema', '')
                            break
                        elif current_type == 'filter':
                            filter_conditions = current_node.get('data', {}).get('config', {}).get('conditions', [])
                            if filter_conditions:
                                filters.extend(filter_conditions)
                            # Continue tracing back
                        elif current_type == 'projection':
                            # Another projection, keep going back
                            continue

                if not source_id or not table_name:
                    return Response(
                        {"error": f"Compute input type '{input_node_type}' is not supported or source not found. Supported: 'source', 'filter', 'projection'."},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Connect to DB to get credentials
                from django.conf import settings
                import psycopg2

                conn = psycopg2.connect(
                    host=settings.DATABASES['default']['HOST'],
                    port=settings.DATABASES['default']['PORT'],
                    user=settings.DATABASES['default']['USER'],
                    password=settings.DATABASES['default']['PASSWORD'],
                    database=customer.cust_db
                )
                conn.autocommit = True
                db_cursor = conn.cursor()

                db_cursor.execute('SELECT source_name, source_config, created_on FROM "GENERAL".source WHERE id = %s', (source_id,))
                source_row = db_cursor.fetchone()

                if not source_row:
                    db_cursor.close()
                    conn.close()
                    return Response({"error": "Source connection not found"}, status=status.HTTP_404_NOT_FOUND)

                source_name, source_config_encrypted, source_created_on = source_row
                source_config = decrypt_source_data(source_config_encrypted, customer.cust_id, source_created_on)

                if not source_config:
                    db_cursor.close()
                    conn.close()
                    return Response({"error": "Failed to decrypt source configuration"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

                password_str = source_config.get('password')
                db_type = source_config.get('db_type', 'postgresql')
                db_cursor.close()
                conn.close()

                # Fetch upstream data
                import httpx
                EXTRACTION_SERVICE_URL = getattr(settings, 'EXTRACTION_SERVICE_URL', 'http://localhost:8001')

                try:
                    endpoint = "/metadata/filter" if filters else "/table-data"

                    connection_data = {
                        "hostname": source_config.get('hostname'),
                        "port": source_config.get('port'),
                        "user": source_config.get('user'),
                        "password": password_str,
                        "database": source_config.get('database', ''),
                        "schema": source_config.get('schema', ''),
                        "service_name": source_config.get('service_name', ''),
                    }

                    columns_for_fastapi = []
                    if 'columns' in input_config:
                        columns_for_fastapi = [c['name'] if isinstance(c, dict) else c for c in input_config['columns']]

                    legacy_filters = []
                    if filters:
                         for f in filters:
                            legacy_filters.append({
                                'column': f.get('column'),
                                'operator': f.get('operator'),
                                'value': f.get('value'),
                                'logicalOperator': f.get('logicalOperator', 'AND')
                            })

                    payload = {
                        "db_type": db_type,
                        "connection_config": connection_data,
                        "table_name": table_name,
                        "schema": schema,
                        "page": page,
                        "page_size": page_size, # Limit rows for preview
                        "columns": columns_for_fastapi,
                        "filters": legacy_filters
                    }

                    async def fetch_data():
                        async with httpx.AsyncClient(timeout=60.0) as client:
                            resp = await client.post(f"{EXTRACTION_SERVICE_URL}{endpoint}", json=payload)
                            return resp

                    import asyncio
                    response = asyncio.run(fetch_data())

                    if response.status_code != 200:
                        return Response({"error": f"Failed to fetch upstream data: {response.text}"}, status=500)

                    result = response.json()
                    input_rows = result.get('rows', [])

                    # Get requirements from config
                    requirements = compute_config.get('requirements', '').strip()

                    # Execute Python Code with dependency management
                    import os
                    import subprocess
                    import sys
                    import tempfile

                    import pandas as pd

                    # Create isolated execution environment
                    execution_success = False
                    output_df = None

                    try:
                        # Install dependencies if requirements.txt is provided
                        if requirements:
                            logger.info("[Compute] Installing dependencies from requirements.txt")

                            # Validate requirements.txt - block dangerous packages
                            blocked_packages = ['os', 'sys', 'subprocess', 'socket', 'urllib', 'requests', 'http']
                            requirements_lines = [line.strip() for line in requirements.split('\n') if line.strip() and not line.strip().startswith('#')]

                            for req_line in requirements_lines:
                                # Extract package name (before ==, >=, etc.)
                                pkg_name = req_line.split('==')[0].split('>=')[0].split('<=')[0].split('>')[0].split('<')[0].strip()
                                if pkg_name.lower() in blocked_packages:
                                    return Response(
                                        {"error": f"Package '{pkg_name}' is not allowed for security reasons"},
                                        status=status.HTTP_400_BAD_REQUEST
                                    )

                            # Create temporary requirements file
                            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as req_file:
                                req_file.write(requirements)
                                req_file_path = req_file.name

                            try:
                                # Install packages with timeout and resource limits
                                install_result = subprocess.run(
                                    [sys.executable, '-m', 'pip', 'install', '-r', req_file_path, '--quiet', '--no-warn-script-location'],
                                    capture_output=True,
                                    text=True,
                                    timeout=120,  # 2 minute timeout for installation
                                    env={**os.environ, 'PYTHONUSERBASE': tempfile.gettempdir()}
                                )

                                if install_result.returncode != 0:
                                    logger.error(f"[Compute] Dependency installation failed: {install_result.stderr}")
                                    return Response(
                                        {
                                            "error": "Failed to install dependencies",
                                            "details": install_result.stderr[:500]  # Limit error message length
                                        },
                                        status=status.HTTP_400_BAD_REQUEST
                                    )

                                logger.info("[Compute] Dependencies installed successfully")
                            finally:
                                # Clean up temp file
                                try:
                                    os.unlink(req_file_path)
                                except Exception:
                                    pass

                        # Prepare input DataFrame (read-only)
                        _input_df_data = pd.DataFrame(input_rows)

                        # Import numpy if available
                        try:
                            import numpy as np
                        except ImportError:
                            np = None

                        # Sandboxed execution environment
                        # _input_df is provided as read-only (we'll validate it's not reassigned)
                        local_vars = {
                            '_input_d': _input_df_data,  # Primary input (as per contract)
                            'input_d': _input_df_data,   # Alias for convenience
                            'd': _input_df_data,         # Common alias
                            'pd': pd,
                            '_output_d': None,
                            'output_df': None             # Alias for convenience
                        }

                        # Add numpy if available
                        if np is not None:
                            local_vars['np'] = np
                            local_vars['numpy'] = np

                        # Restricted globals - only safe builtins
                        restricted_globals = {
                            '__builtins__': {
                                'len': len, 'range': range, 'enumerate': enumerate, 'zip': zip,
                                'map': map, 'filter': filter, 'sum': sum, 'min': min, 'max': max,
                                'abs': abs, 'round': round, 'int': int, 'float': float, 'str': str,
                                'bool': bool, 'list': list, 'dict': dict, 'set': set, 'tuple': tuple,
                                'sorted': sorted, 'reversed': reversed, 'any': any, 'all': all,
                                'print': print, 'type': type, 'isinstance': isinstance,
                            },
                            'pd': pd,
                        }

                        # Add numpy if available (commonly used with pandas)
                        try:
                            import numpy as np
                            restricted_globals['np'] = np
                            local_vars['np'] = np
                        except ImportError:
                            pass

                        # Execute code with timeout
                        import signal

                        def timeout_handler(signum, frame):
                            raise TimeoutError("Code execution exceeded 30 second timeout")

                        # Set execution timeout (30 seconds)
                        if hasattr(signal, 'SIGALRM'):  # Unix-like systems
                            signal.signal(signal.SIGALRM, timeout_handler)
                            signal.alarm(30)

                        try:
                            exec(code, restricted_globals, local_vars)
                        finally:
                            if hasattr(signal, 'SIGALRM'):
                                signal.alarm(0)  # Cancel alarm

                        # Validate _input_df was not reassigned
                        if local_vars.get('_input_df') is not _input_df_data:
                            return Response(
                                {"error": "_input_df is read-only and must not be reassigned. Use _input_df.copy() instead."},
                                status=status.HTTP_400_BAD_REQUEST
                            )

                        # Validate output
                        # Check for output in multiple possible variable names
                        # Use explicit None check to avoid DataFrame truth value ambiguity error
                        _output_df = local_vars.get('_output_d')
                        if _output_df is None:
                            _output_df = local_vars.get('output_df')

                        if _output_df is None:
                            return Response(
                                {"error": "Code must assign a DataFrame to '_output_d' or 'output_df'"},
                                status=status.HTTP_400_BAD_REQUEST
                            )

                        if not isinstance(_output_df, pd.DataFrame):
                            return Response(
                                {"error": f"_output_df must be a DataFrame, got {type(_output_df).__name__}"},
                                status=status.HTTP_400_BAD_REQUEST
                            )

                        execution_success = True

                    except TimeoutError as te:
                        logger.error(f"[Compute] Execution timeout: {te}")
                        return Response(
                            {"error": "Code execution timeout (30 seconds)", "type": "TimeoutError"},
                            status=status.HTTP_400_BAD_REQUEST
                        )
                    except subprocess.TimeoutExpired:
                        logger.error("[Compute] Dependency installation timeout")
                        return Response(
                            {"error": "Dependency installation timeout (120 seconds)"},
                            status=status.HTTP_400_BAD_REQUEST
                        )
                    except Exception as exec_error:
                        logger.error(f"[Compute] Execution error: {exec_error}")
                        import traceback
                        logger.error(traceback.format_exc())
                        return Response(
                            {
                                "error": f"Execution Error: {exec_error!s}",
                                "type": type(exec_error).__name__
                            },
                            status=status.HTTP_400_BAD_REQUEST
                        )

                    if not execution_success or _output_df is None:
                        return Response(
                            {"error": "Code execution failed"},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR
                        )

                    # Convert NaN/Infinities to None for JSON serialization
                    import numpy as np
                    _output_df = _output_df.replace({np.nan: None, np.inf: None, -np.inf: None})

                    output_data = _output_df.to_dict('records')
                    output_columns = [
                        {
                            'name': col,
                            'datatype': str(_output_df[col].dtype),
                            'nullable': True
                        }
                        for col in _output_df.columns
                    ]

                    # Cache results using adaptive cache
                    if canvas_id and adaptive_cache:
                        # Get input row count from upstream cache
                        input_rows = 0
                        if input_node:
                            upstream_cached = adaptive_cache.get_cache(str(canvas_id), input_node.get('id'), "", "")
                            if upstream_cached:
                                input_rows = upstream_cached.get('metadata', {}).get('row_count', len(output_data))

                        save_to_adaptive_cache(
                            node_id=target_node_id,
                            node_type='compute',
                            node_config=compute_config,
                            rows=output_data,
                            columns=output_columns,
                            upstream_node_ids=[input_node.get('id')] if input_node else [],
                            input_rows=input_rows
                        )

                    return Response({
                        "rows": output_data,
                        "columns": output_columns,
                        "total": len(output_data),
                        "page": 1,
                        "page_size": len(output_data),
                        "from_cache": False
                    }, status=status.HTTP_200_OK)

                except Exception as e:
                     logger.error(f"Compute pipeline error: {e}")
                     import traceback
                     logger.error(traceback.format_exc())
                     return Response({"error": str(e)}, status=500)

            # For other node types, return placeholder for now
            return Response({
                "rows": [],
                "columns": [],
                "has_more": False,
                "total": 0,
                "page": page,
                "page_size": page_size,
                "message": f"Pipeline query execution for node type '{target_node_type}' will be fully implemented with SQL query builder"
            }, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error in PipelineQueryExecutionView: {e}")
            return Response(
                {"error": f"Internal server error: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )