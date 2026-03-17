"""
Source-related API views.
Handles source connection management, table discovery, and column metadata.
"""
import asyncio
import datetime
import json
import logging
import time

from django.conf import settings
import httpx
import psycopg2
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from api.authentications import JWTCookieAuthentication
from api.connections.encryption import encrypt_field
from api.models import Country, Customer, SourceDB, SourceForm
from api.serializers import (
    CountrySerializer,
    SourceConnectionSerializer,
    SourceDbSerializer,
    SourceFormSerializer,
    SqlConnectionSerializer,
)
from api.services.sqlserver_connector import extract_data
from api.utils.helpers import (
    create_connection_config,
    decrypt_source_data,
    ensure_user_has_customer,
    generate_encryption_key,
    test_database_connection,
    test_postgresql_connection,
)

logger = logging.getLogger(__name__)

# ── Server-side table list cache ──────────────────────────────────────────────
# Keyed by (source_id, cursor or '', search, limit).
# Entries expire after TABLE_CACHE_TTL_SECONDS.
# force_refresh=true bypasses this cache.
TABLE_CACHE_TTL_SECONDS = 300  # 5 minutes

_TABLE_CACHE: dict = {}

def _table_cache_key(source_id: int, cursor, search: str, limit: int) -> str:
    return f"{source_id}|{cursor or ''}|{search}|{limit}"

def _get_table_cache(source_id: int, cursor, search: str, limit: int):
    key = _table_cache_key(source_id, cursor, search, limit)
    entry = _TABLE_CACHE.get(key)
    if entry and (time.time() - entry['ts']) < TABLE_CACHE_TTL_SECONDS:
        return entry['data']
    return None

def _set_table_cache(source_id: int, cursor, search: str, limit: int, data: dict):
    key = _table_cache_key(source_id, cursor, search, limit)
    _TABLE_CACHE[key] = {'data': data, 'ts': time.time()}

def _clear_table_cache(source_id: int):
    """Evict all cached pages for a given source (called on force_refresh)."""
    prefix = f"{source_id}|"
    for k in list(_TABLE_CACHE.keys()):
        if k.startswith(prefix):
            del _TABLE_CACHE[k]

def _sanitize_for_json(obj):
    """Convert non-JSON-serializable values so json.dumps succeeds. Avoids 'can't adapt type dict' when storing."""
    if obj is None:
        return None
    if isinstance(obj, bytes):
        return obj.decode('utf-8', errors='replace')
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v) for v in obj]
    return obj

class SqlConnectionView(APIView):
    def post(self, request):
        serializer = SqlConnectionSerializer(data=request.data)
        if serializer.is_valid():
            data = serializer.validated_data
            success, message = extract_data(
                data['sql_hostname'],
                data['sql_database'],
                data['sql_username'],
                data['sql_password'],
                1433
            )
            if success:
                return Response({"message": "success"}, status=status.HTTP_201_CREATED)
            return Response({"message": "failed", "error": message}, status=status.HTTP_400_BAD_REQUEST)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class SourcesListView(APIView):
    def get(self, request):
        sources = SourceDB.objects.all()
        return Response(SourceDbSerializer(sources, many=True).data, status=status.HTTP_200_OK)

class SourceFieldsView(APIView):
    def get(self, request, source_id: int):
        try:
            source = SourceDB.objects.get(id=source_id)
            fields = SourceForm.objects.filter(src_db=source)
        except SourceDB.DoesNotExist:
            return Response({"detail": "Source not found"}, status=status.HTTP_404_NOT_FOUND)
        return Response(SourceFormSerializer(fields, many=True).data, status=status.HTTP_200_OK)

class CountryListView(APIView):
    """API view for listing all countries."""

    def get(self, request):
        """Get list of all countries."""
        try:
            countries = Country.objects.all()
            serializer = CountrySerializer(countries, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            return Response(
                {"error": f"Error retrieving countries: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class SourceConnectionCreateView(APIView):
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        serializer = SourceConnectionSerializer(data=request.data)
        if serializer.is_valid():
            try:
                #Get the customer object, create if doesn't exist
                user = request.user
                customer = ensure_user_has_customer(user)

                # Test database connection before saving
                hostname = serializer.validated_data.get("hostname")
                port = serializer.validated_data.get("port")
                db_user = serializer.validated_data.get("user")  # Renamed to avoid conflict with request.user
                db_password = serializer.validated_data.get("password")
                schema = serializer.validated_data.get("schema")
                database = serializer.validated_data.get("database")

                # Test connection if any connection details are provided.
                # Only save the source if the connection test succeeds.
                if any([hostname, port, db_user, db_password]):
                    # Prefer explicit database name; fall back to schema (legacy behaviour)
                    db_for_test = database or schema
                    connection_success, error_message = test_database_connection(
                        hostname, port, db_user, db_password, db_for_test
                    )

                    if not connection_success:
                        return Response(
                            {
                                "error": "Database connection test failed",
                                "details": error_message,
                                "connection_params": {
                                    "hostname": hostname,
                                    "port": port,
                                    "user": db_user,
                                    "schema": schema,
                                },
                            },
                            status=status.HTTP_400_BAD_REQUEST,
                        )

                # Create JSON structure with connection details (excluding id and name)
                # Only include non-null values in the JSON config
                connection_config = create_connection_config(serializer.validated_data)

                # Add db_type to config if provided
                db_type = serializer.validated_data.get('db_type')
                if db_type:
                    connection_config['db_type'] = db_type

                # Add database name if provided
                database = serializer.validated_data.get('database')
                if database:
                    connection_config['database'] = database

                # Add service_name for Oracle if provided
                service_name = serializer.validated_data.get('service_name')
                if service_name:
                    connection_config['service_name'] = service_name

                # Store source configs centrally in the main app DB (GENERAL schema)
                target_database = settings.DATABASES['default']['NAME']
                customer_id_for_encryption = customer.cust_id

                # Validate customer_id_for_encryption is a string (cust_id format like "C00001")
                if not isinstance(customer_id_for_encryption, str):
                    raise ValueError(f"customer_id_for_encryption must be a string, got {type(customer_id_for_encryption)}: {customer_id_for_encryption}")

                conn = psycopg2.connect(
                    host=settings.DATABASES['default']['HOST'],
                    port=settings.DATABASES['default']['PORT'],
                    user=settings.DATABASES['default']['USER'],
                    password=settings.DATABASES['default']['PASSWORD'],
                    database=target_database
                )
                conn.autocommit = True
                cursor = conn.cursor()

                # Ensure GENERAL schema exists
                cursor.execute('CREATE SCHEMA IF NOT EXISTS "GENERAL";')

                # Check if source table exists and what columns it has
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'GENERAL' AND table_name = 'source'
                """)
                existing_columns = [row[0] for row in cursor.fetchall()]

                # Decide which column names to use in INSERT
                name_col = "source_name" if "source_name" in existing_columns or not existing_columns else "src_name"
                config_col = "source_config" if "source_config" in existing_columns or not existing_columns else "src_config"

                # Create or alter source table to use source_name (standardize column names)
                if not existing_columns:
                    # Table doesn't exist, create it with correct column names including project_id
                    create_source_table_sql = '''
                    CREATE TABLE "GENERAL".source (
                        id SERIAL PRIMARY KEY,
                        source_name VARCHAR(255) NOT NULL,
                        source_config TEXT,
                        project_id INTEGER,
                        created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE
                    );
                    '''
                    cursor.execute(create_source_table_sql)
                else:
                    # Table exists, check and rename columns if needed (migrate old schema)
                    try:
                        if 'src_name' in existing_columns and 'source_name' not in existing_columns:
                            cursor.execute('ALTER TABLE "GENERAL".source RENAME COLUMN src_name TO source_name;')
                    except Exception as e:
                        print(f"Note: Could not rename src_name column (may already be renamed): {e}")

                    try:
                        if 'src_config' in existing_columns and 'source_config' not in existing_columns:
                            cursor.execute('ALTER TABLE "GENERAL".source RENAME COLUMN src_config TO source_config;')
                    except Exception as e:
                        print(f"Note: Could not rename src_config column (may already be renamed): {e}")

                    # Add project_id column if it doesn't exist
                    try:
                        if 'project_id' not in existing_columns:
                            cursor.execute('ALTER TABLE "GENERAL".source ADD COLUMN project_id INTEGER;')
                            print("Added project_id column to GENERAL.source table")
                    except Exception as e:
                        print(f"Note: Could not add project_id column (may already exist): {e}")

                # Get the current timestamp from the database to ensure consistency
                cursor.execute("SELECT NOW()")
                db_timestamp = cursor.fetchone()[0]

                # Generate encryption key using the database timestamp
                # customer_id_for_encryption should be a string like "C00001"
                encryption_key = generate_encryption_key(customer_id_for_encryption, db_timestamp)

                # Validate encryption_key is an integer
                if not isinstance(encryption_key, int):
                    raise ValueError(f"encryption_key must be an integer, got {type(encryption_key)}: {encryption_key}")

                # Encrypt the entire JSON configuration using the database timestamp
                encrypted_config = encrypt_field(connection_config, encryption_key)

                # Verify table has correct columns before inserting (after potential migration)
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'GENERAL' AND table_name = 'source'
                """)
                final_columns = [row[0] for row in cursor.fetchall()]

                # Use the correct column names (should be source_name after migration)

                # Get project_id from request data (optional)
                project_id = request.data.get('project_id') or serializer.validated_data.get('project_id')
                if project_id:
                    try:
                        project_id = int(project_id)
                    except (ValueError, TypeError):
                        project_id = None

                # Build insert SQL based on whether project_id column exists
                has_project_id = 'project_id' in final_columns
                if has_project_id:
                    insert_sql = f'''
                        INSERT INTO "GENERAL".source ({name_col}, {config_col}, project_id, created_on, modified_on, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id
                    '''
                    insert_params = (
                        serializer.validated_data.get("source_name"),
                        json.dumps(encrypted_config),
                        project_id,
                        db_timestamp,
                        db_timestamp,
                        True  # is_active defaults to True
                    )
                else:
                    insert_sql = f'''
                        INSERT INTO "GENERAL".source ({name_col}, {config_col}, created_on, modified_on, is_active)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id
                    '''
                    insert_params = (
                        serializer.validated_data.get("source_name"),
                        json.dumps(encrypted_config),
                        db_timestamp,
                        db_timestamp,
                        True  # is_active defaults to True
                    )

                cursor.execute(insert_sql, insert_params)
                # Get the inserted source ID directly from RETURNING clause
                source_id_row = cursor.fetchone()
                source_id = source_id_row[0] if source_id_row else None

                cursor.close()
                conn.close()

                return Response({
                    "message": "Source connection added successfully",
                    "id": source_id,
                    "source_name": serializer.validated_data.get("source_name")
                }, status=status.HTTP_201_CREATED)

            except Customer.DoesNotExist:
                return Response(
                    {"error": "Customer not found"},
                    status=status.HTTP_404_NOT_FOUND
                )
            except Exception as e:
                import traceback
                error_traceback = traceback.format_exc()
                print(f"Error in SourceConnectionCreateView: {e!s}")
                print(f"Traceback: {error_traceback}")
                return Response(
                    {"error": f"Failed to store source details in customer database: {e!s}", "traceback": error_traceback},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class CustomerSourcesView(APIView):

    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request,):
        try:
            # Get the customer object, create if doesn't exist
            user = request.user
            customer = ensure_user_has_customer(user)

            # Connect to the main app database and fetch sources from GENERAL.source
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=settings.DATABASES['default']['NAME'],
            )
            conn.autocommit = True
            cursor = conn.cursor()

            # Check table structure and use appropriate column names
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'GENERAL' AND table_name = 'source'
            """)
            columns = [row[0] for row in cursor.fetchall()]

            # Decide which column names to use
            name_column = "source_name" if "source_name" in columns else "src_name"
            config_column = "source_config" if "source_config" in columns else "src_config"
            has_project_id = 'project_id' in columns
            project_id_col = "project_id"

            # Always return all sources (including those with NULL project_id).
            if has_project_id:
                query = f'''
                    SELECT id, {name_column}, {config_column}, {project_id_col}, created_on, modified_on, is_active
                    FROM "GENERAL".source
                    ORDER BY created_on DESC
                '''
            else:
                query = f'''
                    SELECT id, {name_column}, {config_column}, created_on, modified_on, is_active
                    FROM "GENERAL".source
                    ORDER BY created_on DESC
                '''
            cursor.execute(query)

            sources = []
            for row in cursor.fetchall():
                if has_project_id:
                    source_id, source_name, source_config, project_id_val, created_on, modified_on, is_active = row
                else:
                    source_id, source_name, source_config, created_on, modified_on, is_active = row
                    project_id_val = None

                # Decrypt the source configuration
                decrypted_config = {}
                if source_config:
                    try:
                        decrypted_config = decrypt_source_data(source_config, customer.cust_id, created_on)

                    except Exception as e:
                        print(f"Error decrypting source config: {e}")
                        decrypted_config = {}

                source_data = {
                    'source_id': source_id,
                    'source_name': source_name,
                    'project_id': project_id_val,
                    'db_type': decrypted_config.get('db_type') if decrypted_config else None,
                    'hostname': decrypted_config.get('hostname') if decrypted_config else None,
                    'port': decrypted_config.get('port') if decrypted_config else None,
                    'user': decrypted_config.get('user') if decrypted_config else None,
                    'password': decrypted_config.get('password') if decrypted_config else None,
                    'schema': decrypted_config.get('schema') if decrypted_config else None,
                    'database': decrypted_config.get('database') if decrypted_config else None,
                    'service_name': decrypted_config.get('service_name') if decrypted_config else None,
                    'created_on': created_on.isoformat() if created_on else None,
                    'modified_on': modified_on.isoformat() if modified_on else None,
                    'is_active': is_active
                }
                sources.append(source_data)

            cursor.close()
            conn.close()

            return Response({
                'customer_id': customer.cust_id,
                'customer_name': customer.name,
                'sources': sources
            }, status=status.HTTP_200_OK)

        except Customer.DoesNotExist:
            return Response(
                {"error": "Customer not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            return Response(
                {"error": f"Failed to fetch sources: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class SourceAttributesView(APIView):
    """API view to get source attributes for dynamic form generation"""
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, source_type=None):
        """
        Get source attributes for a given source type.
        If source_type is provided, return attributes for that type.
        Otherwise, return all source types with their attributes.
        """
        try:
            from api.models import SourceAttribute, SourceModel

            if source_type:
                # Get specific source type
                try:
                    source = SourceModel.objects.get(name__iexact=source_type)
                    attributes = SourceAttribute.objects.filter(src=source, is_visible=True).order_by('src_attr_id')

                    return Response({
                        "source_type": source.name,
                        "source_id": source.src_id,
                        "attributes": [
                            {
                                "attribute_name": attr.attribute_name,
                                "input_type": attr.input_type,
                                "label": attr.label,
                                "widget": attr.widget,
                                "required": attr.required,
                                "choices": attr.choices,
                                "depend_on": attr.depend_on,
                                "dependency_value": attr.dependency_value,
                            }
                            for attr in attributes
                        ]
                    }, status=status.HTTP_200_OK)
                except SourceModel.DoesNotExist:
                    return Response(
                        {"error": f"Source type '{source_type}' not found"},
                        status=status.HTTP_404_NOT_FOUND
                    )
            else:
                # Get all source types
                sources = SourceModel.objects.all()
                result = []
                for source in sources:
                    attributes = SourceAttribute.objects.filter(src=source, is_visible=True).order_by('src_attr_id')
                    result.append({
                        "source_type": source.name,
                        "source_id": source.src_id,
                        "attributes": [
                            {
                                "attribute_name": attr.attribute_name,
                                "input_type": attr.input_type,
                                "label": attr.label,
                                "widget": attr.widget,
                                "required": attr.required,
                                "choices": attr.choices,
                                "depend_on": attr.depend_on,
                                "dependency_value": attr.dependency_value,
                            }
                            for attr in attributes
                        ]
                    })

                return Response({
                    "sources": result
                }, status=status.HTTP_200_OK)

        except Exception as e:
            return Response(
                {"error": f"Failed to fetch source attributes: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class SourceConnectionCreateWithValidationView(APIView):
    """
    Create source connection with FastAPI validation.
    Flow: React → Django → FastAPI (validate) → Django (save to SourceConfig) → React
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """
        Create source connection:
        1. Validate connection via FastAPI
        2. Save to SourceConfig model if validation succeeds
        """
        try:
            import requests

            from api.models import SourceConfig, SourceModel

            # Get request data
            source_name = request.data.get('source_name')
            db_type = request.data.get('db_type', '').lower()
            connection_data = request.data.copy()

            if not source_name or not db_type:
                return Response(
                    {"error": "source_name and db_type are required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Get user and customer, create if doesn't exist
            user = request.user
            customer = ensure_user_has_customer(user)

            # Get SourceModel
            try:
                source_model = SourceModel.objects.get(name__iexact=db_type)
            except SourceModel.DoesNotExist:
                return Response(
                    {"error": f"Source type '{db_type}' not found. Please create it in SourceModel first."},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Prepare connection config for FastAPI
            connection_config = {
                "db_type": db_type,
                "hostname": connection_data.get('hostname'),
                "port": connection_data.get('port'),
                "user": connection_data.get('user'),
                "password": connection_data.get('password'),
            }

            # Add optional fields
            if connection_data.get('database'):
                connection_config['database'] = connection_data.get('database')
            if connection_data.get('schema'):
                connection_config['schema'] = connection_data.get('schema')
            if connection_data.get('service_name'):
                connection_config['service_name'] = connection_data.get('service_name')

            # For PostgreSQL, use existing Django test function
            # For others, call FastAPI
            if db_type == 'postgresql':
                # Use existing PostgreSQL test
                hostname = connection_config.get('hostname')
                port = connection_config.get('port')
                db_user = connection_config.get('user')
                db_password = connection_config.get('password')
                schema = connection_config.get('schema')

                connection_success, error_message = test_postgresql_connection(
                    hostname, port, db_user, db_password, schema
                )

                if not connection_success:
                    return Response(
                        {
                            "error": "Database connection test failed",
                            "details": error_message
                        },
                        status=status.HTTP_400_BAD_REQUEST
                    )
            else:
                # Call FastAPI for validation
                fastapi_url = getattr(settings, 'FASTAPI_EXTRACTION_SERVICE_URL', 'http://localhost:8001')
                test_url = f"{fastapi_url}/test-connection"

                try:
                    response = requests.post(test_url, json=connection_config, timeout=10)
                    response.raise_for_status()
                    result = response.json()

                    if not result.get('success', False):
                        return Response(
                            {
                                "error": "Database connection test failed",
                                "details": result.get('message', 'Unknown error'),
                                "fastapi_error": result.get('error')
                            },
                            status=status.HTTP_400_BAD_REQUEST
                        )
                except requests.exceptions.RequestException as e:
                    return Response(
                        {
                            "error": "Failed to validate connection with FastAPI service",
                            "details": str(e)
                        },
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )

            # Connection validated, now save to SourceConfig
            # Check if source config with same name already exists
            existing_config = SourceConfig.objects.filter(cmp=customer, name=source_name).first()

            if existing_config:
                # Update existing config
                existing_config.data = connection_config
                existing_config.src = source_model
                existing_config.is_active = True
                existing_config.response_message = "Connection validated and updated successfully"
                existing_config.save()

                return Response({
                    "message": "Source connection updated successfully",
                    "id": existing_config.src_config_id,
                    "source_name": source_name,
                    "source_type": db_type
                }, status=status.HTTP_200_OK)
            else:
                # Create new config
                source_config = SourceConfig(
                    name=source_name,
                    src=source_model,
                    cmp=customer,
                    is_active=True,
                    response_message="Connection validated and saved successfully"
                )
                source_config.data = connection_config  # This will trigger encryption via setter
                source_config.save()

                return Response({
                    "message": "Source connection added successfully",
                    "id": source_config.src_config_id,
                    "source_name": source_name,
                    "source_type": db_type
                }, status=status.HTTP_201_CREATED)

        except Exception as e:
            import traceback
            traceback.print_exc()
            return Response(
                {"error": f"Failed to create source connection: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class SourceEditView(APIView):
    """API view for editing existing source connections."""

    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, source_id):
        """Get source data for editing."""
        try:
            # Get the customer object
            user = request.user
            customer = ensure_user_has_customer(user)

            # Connect to the customer's database
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=customer.cust_db
            )
            conn.autocommit = True
            cursor = conn.cursor()

            # Fetch the specific source
            cursor.execute('''
                SELECT source_name, source_config, created_on, modified_on, is_active
                FROM "GENERAL".source
                WHERE id = %s
            ''', (source_id,))

            source_row = cursor.fetchone()
            if not source_row:
                return Response(
                    {"error": "Source not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            source_name_db, source_config, created_on, modified_on, is_active = source_row

            # Decrypt the source configuration
            decrypted_config = {}
            if source_config:
                try:
                    decrypted_config = decrypt_source_data(source_config, customer.cust_id, created_on)
                except Exception as e:
                    print(f"Error decrypting source config: {e}")
                    decrypted_config = {}

            source_data = {
                'source_name': source_name_db,
                'hostname': decrypted_config.get('hostname') if decrypted_config else '',
                'port': decrypted_config.get('port') if decrypted_config else '',
                'user': decrypted_config.get('user') if decrypted_config else '',
                'password': decrypted_config.get('password') if decrypted_config else '',
                'schema': decrypted_config.get('schema') if decrypted_config else '',
                'db_type': decrypted_config.get('db_type') if decrypted_config else '',
                'created_on': created_on.isoformat() if created_on else None,
                'modified_on': modified_on.isoformat() if modified_on else None,
                'is_active': is_active
            }

            cursor.close()
            conn.close()

            return Response({
                'customer_id': customer.cust_id,
                'customer_name': customer.name,
                'source': source_data
            }, status=status.HTTP_200_OK)

        except Customer.DoesNotExist:
            return Response(
                {"error": "Customer not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            return Response(
                {"error": f"Failed to fetch source data: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def put(self, request, source_id):
        """Update an existing source connection."""
        try:
            # Get the customer object
            user = request.user
            customer = ensure_user_has_customer(user)

            # Connect to the customer's database
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=customer.cust_db
            )
            conn.autocommit = True
            cursor = conn.cursor()

            # Check if source exists
            cursor.execute('''
                SELECT source_name, source_config, created_on, is_active
                FROM "GENERAL".source
                WHERE id = %s
            ''', (source_id,))

            existing_source = cursor.fetchone()
            if not existing_source:
                return Response(
                    {"error": "Source not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Get the original created_on timestamp to maintain encryption consistency
            original_source_name, original_source_config, original_created_on, original_is_active = existing_source

            # Create new connection configuration
            connection_config = create_connection_config(request.data)

            # Get current timestamp for modification
            cursor.execute("SELECT NOW()")
            current_timestamp = cursor.fetchone()[0]

            # Use the original created_on timestamp for encryption key generation
            # This ensures we can still decrypt the data
            encryption_key = generate_encryption_key(customer.cust_id, original_created_on)

            # Encrypt the new configuration using the same key
            encrypted_config = encrypt_field(connection_config, encryption_key)

            # Update the source record
            update_sql = '''
                UPDATE "GENERAL".source
                SET source_name = %s,
                    source_config = %s,
                    modified_on = %s,
                    is_active = %s
                WHERE id = %s
            '''

            cursor.execute(
                update_sql,
                (
                    request.data.get('source_name'),
                    json.dumps(encrypted_config),
                    current_timestamp,
                    request.data.get('is_active', True),
                    source_id  # Original source name for WHERE clause
                )
            )

            # Verify the update by fetching the updated record
            cursor.execute('''
                SELECT source_name, source_config, created_on, modified_on, is_active
                FROM "GENERAL".source
                WHERE id = %s
            ''', (source_id,))

            updated_source = cursor.fetchone()
            if updated_source:
                updated_source_name, updated_source_config, created_on, modified_on, is_active = updated_source

                # Configuration successfully updated

            cursor.close()
            conn.close()

            return Response({
                "message": "Source connection updated successfully",
                "modified_on": current_timestamp.isoformat()
            }, status=status.HTTP_200_OK)

        except Customer.DoesNotExist:
            return Response(
                {"error": "Customer not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            return Response(
                {"error": f"Failed to update source connection: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class SourceDeleteView(APIView):
    """API view for deleting existing source connections."""

    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def delete(self, request, source_id):
        """Delete an existing source connection."""
        try:
            # Get the customer object
            user = request.user
            customer = ensure_user_has_customer(user)

            # Connect to the customer's database
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=customer.cust_db
            )
            conn.autocommit = True
            cursor = conn.cursor()

            # Check if source exists
            cursor.execute('''
                SELECT source_name, created_on
                FROM "GENERAL".source
                WHERE id = %s
            ''', (source_id,))

            existing_source = cursor.fetchone()
            if not existing_source:
                return Response(
                    {"error": "Source not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Delete the source record
            delete_sql = '''
                DELETE FROM "GENERAL".source
                WHERE id = %s
            '''

            cursor.execute(delete_sql, (source_id,))

            # Verify the deletion
            cursor.execute('''
                SELECT COUNT(*) FROM "GENERAL".source
                WHERE id = %s
            ''', (source_id,))

            remaining_count = cursor.fetchone()[0]
            if remaining_count > 0:
                return Response(
                    {"error": "Failed to delete source"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            cursor.close()
            conn.close()

            return Response({
                "message": "Source connection deleted successfully",
                "source_id": source_id
            }, status=status.HTTP_200_OK)

        except Customer.DoesNotExist:
            return Response(
                {"error": "Customer not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            return Response(
                {"error": f"Failed to delete source connection: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class SourceTablesView(APIView):
    """
    API view to fetch tables from a source connection with cursor-based pagination.
    Supports search and returns 100 tables per page.
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, source_id):
        """
        Get tables from a source connection with pagination.
        Path params:
        - source_id: ID of the source connection (from URL path)
        Query params:
        - cursor: Cursor for pagination (table name from last result)
        - search: Optional search term to filter tables
        - limit: Number of tables to return (default 100)
        - force_refresh: If true, force refresh from FastAPI service (default false)
        """
        try:
            # source_id comes from URL path parameter
            source_id = int(source_id)
            cursor = request.query_params.get('cursor')  # Last table name from previous page
            search = request.query_params.get('search', '').strip()
            limit = int(request.query_params.get('limit', 100))
            force_refresh = request.query_params.get('force_refresh', 'false').lower() == 'true'

            # Get user and customer (customer used for decryption key)
            user = request.user
            customer = ensure_user_has_customer(user)

            # Connect to the main app database to get source connection from GENERAL.source
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=settings.DATABASES['default']['NAME'],
            )
            conn.autocommit = True
            db_cursor = conn.cursor()

            # Get source connection details
            db_cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'GENERAL' AND table_name = 'source'
            """)
            src_columns = [row[0] for row in db_cursor.fetchall()]

            # Decide which column names to use
            name_column = "source_name" if "source_name" in src_columns else "src_name"
            config_column = "source_config" if "source_config" in src_columns else "src_config"

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

            db_type = decrypted_config.get('db_type', '').lower()
            db_cursor.close()
            conn.close()

            # ── Cache check ───────────────────────────────────────────────────
            # force_refresh evicts the cache so the Refresh button always hits FastAPI.
            if force_refresh:
                _clear_table_cache(source_id)
            else:
                cached = _get_table_cache(source_id, cursor, search, limit)
                if cached is not None:
                    logger.debug(f"[SourceTablesView] Cache HIT for source_id={source_id}")
                    return Response(cached, status=status.HTTP_200_OK)

            # ── Fetch from FastAPI ────────────────────────────────────────────
            try:
                fastapi_request = {
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
                    "schema": decrypted_config.get('schema'),
                    "search": search if search else "",
                    "limit": limit,
                    "cursor": cursor if cursor else None
                }

                EXTRACTION_SERVICE_URL = getattr(settings, 'FASTAPI_EXTRACTION_SERVICE_URL', 'http://localhost:8001')

                async def fetch_tables_from_fastapi():
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        response = await client.post(
                            f"{EXTRACTION_SERVICE_URL}/metadata/tables",
                            json=fastapi_request
                        )
                        response.raise_for_status()
                        return response.json()

                result = asyncio.run(fetch_tables_from_fastapi())

                tables = result.get('tables', [])
                next_cursor = result.get('next_cursor')
                has_more = result.get('has_more', False)

                # ── fields_count removed: bulk column fetch was causing 2x latency ──
                # Column counts are a cosmetic feature. Don't make a second serial
                # HTTP round-trip to the source DB just to get column counts for display.

                payload = {
                    'tables': tables,
                    'next_cursor': next_cursor,
                    'has_more': has_more,
                    'count': len(tables)
                }
                # Store in cache (skipped when force_refresh already cleared it)
                _set_table_cache(source_id, cursor, search, limit, payload)

                return Response(payload, status=status.HTTP_200_OK)

            except httpx.ConnectError as e:
                logger.error(f"FastAPI service connection error: {e}")
                logger.error(f"FastAPI service URL: {EXTRACTION_SERVICE_URL}")
                return Response(
                    {
                        "error": "FastAPI extraction service is not available",
                        "details": f"Could not connect to {EXTRACTION_SERVICE_URL}. Please ensure the FastAPI service is running.",
                        "service_url": EXTRACTION_SERVICE_URL,
                        "hint": "Start the FastAPI service using: python services/extraction_service/main.py"
                    },
                    status=status.HTTP_503_SERVICE_UNAVAILABLE
                )
            except httpx.HTTPError as e:
                logger.error(f"FastAPI service HTTP error: {e}")
                return Response(
                    {
                        "error": f"FastAPI service error: {e!s}",
                        "service_url": EXTRACTION_SERVICE_URL
                    },
                    status=status.HTTP_503_SERVICE_UNAVAILABLE
                )
            except httpx.TimeoutException as e:
                logger.error(f"FastAPI service timeout: {e}")
                return Response(
                    {
                        "error": "FastAPI service request timed out",
                        "details": f"Request to {EXTRACTION_SERVICE_URL} took too long",
                        "service_url": EXTRACTION_SERVICE_URL
                    },
                    status=status.HTTP_503_SERVICE_UNAVAILABLE
                )
            except Exception as e:
                import traceback
                logger.error(f"Error fetching tables from FastAPI: {e}")
                logger.error(traceback.format_exc())
                return Response(
                    {"error": f"Failed to fetch tables: {e!s}"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

        except Exception as e:
            import traceback
            logger.error(f"Error in SourceTablesView: {e}")
            logger.error(traceback.format_exc())
            return Response(
                {"error": f"Internal server error: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class SourceTableDataView(APIView):
    """
    API view to fetch table data from a source connection.
    Returns paginated table rows.
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, source_id):
        """
        Get table data from a source connection.
        Path params:
        - source_id: ID of the source connection (from URL path)
        Query params:
        - table_name: Table name (required)
        - schema: Schema name (optional)
        - page: Page number (default 1)
        - page_size: Number of rows per page (default 50)
        """
        try:
            source_id = int(source_id)
            table_name = request.query_params.get('table_name')
            schema = request.query_params.get('schema', '')
            page = int(request.query_params.get('page', 1))
            page_size = int(request.query_params.get('page_size', 50))

            if not table_name:
                return Response(
                    {"error": "table_name is required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Get user and customer
            user = request.user
            customer = ensure_user_has_customer(user)

            # Connect to main app database where GENERAL.source lives
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=settings.DATABASES['default']['NAME'],
            )
            conn.autocommit = True
            db_cursor = conn.cursor()

            # Get source connection details
            db_cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'GENERAL' AND table_name = 'source'
            """)
            src_columns = [row[0] for row in db_cursor.fetchall()]

            # Decide which column names to use based on actual schema
            name_column = "source_name" if "source_name" in src_columns else "src_name"
            config_column = "source_config" if "source_config" in src_columns else "src_config"

            name_column_sql = f'"{name_column}"'
            config_column_sql = f'"{config_column}"'

            db_cursor.execute(
                f'''
                SELECT id, {name_column_sql}, {config_column_sql}, created_on
                FROM "GENERAL".source
                WHERE id = %s
                ''',
                (source_id,),
            )

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

            db_type = decrypted_config.get('db_type', '').lower()
            db_cursor.close()
            conn.close()

            # Call FastAPI service to fetch table data (columns from response — no DB storage)
            try:
                EXTRACTION_SERVICE_URL = getattr(settings, 'FASTAPI_EXTRACTION_SERVICE_URL', 'http://localhost:8001')

                async def fetch_table_data():
                    # Increased timeout to 120 seconds for large table queries
                    async with httpx.AsyncClient(timeout=120.0) as client:
                        response = await client.post(
                            f"{EXTRACTION_SERVICE_URL}/table-data",
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
                                "page": page,
                                "page_size": page_size
                            }
                        )
                        response.raise_for_status()
                        return response.json()

                result = asyncio.run(fetch_table_data())
                columns = result.get('columns', [])

                return Response({
                    "rows": result.get('rows', []),
                    "columns": columns,
                    "has_more": result.get('has_more', False),
                    "total": result.get('total', 0),
                    "page": page,
                    "page_size": page_size
                }, status=status.HTTP_200_OK)

            except httpx.ConnectError as e:
                logger.error(f"[SourceTableDataView] FastAPI service connection error: {e}")
                return Response(
                    {
                        "error": "FastAPI extraction service is not available",
                        "details": f"Could not connect to {EXTRACTION_SERVICE_URL}",
                    },
                    status=status.HTTP_503_SERVICE_UNAVAILABLE
                )
            except httpx.ReadTimeout as e:
                logger.error(f"[SourceTableDataView] FastAPI service read timeout: {e}")
                return Response(
                    {
                        "error": "FastAPI extraction service request timed out",
                        "details": f"The request to {EXTRACTION_SERVICE_URL} exceeded the timeout limit (120 seconds). The table may be too large or the service may be slow.",
                    },
                    status=status.HTTP_504_GATEWAY_TIMEOUT
                )
            except httpx.TimeoutException as e:
                logger.error(f"[SourceTableDataView] FastAPI service timeout exception: {e}")
                return Response(
                    {
                        "error": "FastAPI extraction service request timed out",
                        "details": f"The request to {EXTRACTION_SERVICE_URL} exceeded the timeout limit. The table may be too large or the service may be slow.",
                    },
                    status=status.HTTP_504_GATEWAY_TIMEOUT
                )
            except httpx.HTTPStatusError as e:
                logger.error(f"[SourceTableDataView] FastAPI service HTTP error: {e.response.status_code} - {e.response.text}")
                return Response(
                    {
                        "error": "FastAPI extraction service returned an error",
                        "details": f"Status {e.response.status_code}: {e.response.text[:500]}",
                    },
                    status=status.HTTP_502_BAD_GATEWAY
                )
            except Exception as e:
                logger.error(f"[SourceTableDataView] Error fetching table data: {e}", exc_info=True)
                return Response(
                    {"error": f"Failed to fetch table data: {e!s}"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

        except Exception as e:
            logger.error(f"[SourceTableDataView] Error in SourceTableDataView: {e}", exc_info=True)
            return Response(
                {"error": f"Internal server error: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class SourceColumnsView(APIView):
    """
    API view to fetch column definitions for a table from a source connection.
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, source_id):
        """
        Get column definitions for a table.
        Path params:
        - source_id: ID of the source connection (from URL path)
        Query params:
        - table_name: Table name (required)
        - schema: Schema name (optional)
        - page: Page number (default 1)
        - page_size: Number of columns per page (default 100)
        - search: Search term for column name or type (optional)
        - type_filter: Filter by column type category (string, number, date, boolean, other) (optional)
        """
        try:
            source_id = int(source_id)
            table_name = request.query_params.get('table_name')
            schema = request.query_params.get('schema', '')
            page = int(request.query_params.get('page', 1))
            page_size = int(request.query_params.get('page_size', 100))
            search = request.query_params.get('search', '').strip()
            type_filter = request.query_params.get('type_filter', '').strip().lower()
            _force_refresh = request.query_params.get('force_refresh', 'false').lower() == 'true'

            if not table_name:
                return Response(
                    {"error": "table_name is required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Validate pagination
            if page < 1:
                page = 1
            if page_size < 1 or page_size > 500:
                page_size = 100

            # Get user and customer
            user = request.user
            customer = ensure_user_has_customer(user)

            # Get source connection from DB (configuration only — columns always fetched from source)
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=settings.DATABASES['default']['NAME'],
            )
            conn.autocommit = True
            db_cursor = conn.cursor()
            db_cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'GENERAL' AND table_name = 'source'
            """)
            src_columns = [row[0] for row in db_cursor.fetchall()]

            # Decide which column names to use
            name_column = "source_name" if "source_name" in src_columns else "src_name"
            config_column = "source_config" if "source_config" in src_columns else "src_config"

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

            db_type = decrypted_config.get('db_type', '').lower()
            db_cursor.close()
            conn.close()

            # Call FastAPI service to fetch columns
            try:
                EXTRACTION_SERVICE_URL = getattr(settings, 'FASTAPI_EXTRACTION_SERVICE_URL', 'http://localhost:8001')

                async def fetch_columns():
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

                result = asyncio.run(fetch_columns())
                all_columns = result.get('columns', [])
                # Apply filters
                filtered_columns = all_columns

                # Apply search filter
                if search:
                    filtered_columns = [
                        c for c in filtered_columns
                        if search.lower() in c.get('name', '').lower() or
                           search.lower() in c.get('data_type', '').lower()
                    ]

                # Apply type filter
                if type_filter:
                    def get_type_category(data_type):
                        dt = str(data_type).lower()
                        if any(x in dt for x in ['int', 'number', 'decimal', 'float', 'double']):
                            return 'number'
                        if any(x in dt for x in ['char', 'text', 'varchar', 'string']):
                            return 'string'
                        if any(x in dt for x in ['date', 'time', 'timestamp']):
                            return 'date'
                        if any(x in dt for x in ['bool', 'bit']):
                            return 'boolean'
                        return 'other'

                    filtered_columns = [
                        c for c in filtered_columns
                        if get_type_category(c.get('data_type', '')) == type_filter
                    ]

                # Apply pagination
                total = len(filtered_columns)
                start = (page - 1) * page_size
                end = start + page_size
                paginated_columns = filtered_columns[start:end]

                return Response({
                    "columns": paginated_columns,
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "has_more": end < total
                }, status=status.HTTP_200_OK)

            except Exception as e:
                logger.error(f"Error fetching columns: {e}")
                return Response(
                    {"error": f"Failed to fetch columns: {e!s}"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

        except Exception as e:
            logger.error(f"Error in SourceColumnsView: {e}")
            return Response(
                {"error": f"Internal server error: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class SourceTableSelectionView(APIView):
    """
    API view to save and retrieve selected tables from source connections.
    Tables are stored in customer database's GENERAL.source_table_selection table.
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request, source_id):
        """
        Save selected tables for a source connection.
        Path params:
        - source_id: ID of the source connection (from URL path)
        Body: {
            "tables": [
                {"table_name": str, "schema": str (optional)}
            ]
        }
        """
        try:
            # source_id comes from URL path parameter
            source_id = int(source_id)
            tables = request.data.get('tables', [])

            if not tables or not isinstance(tables, list):
                return Response(
                    {"error": "tables must be a non-empty array"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Get user and customer
            user = request.user
            customer = ensure_user_has_customer(user)

            # Connect to customer's database
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=customer.cust_db
            )
            conn.autocommit = True
            cursor = conn.cursor()

            # Ensure GENERAL schema and table exist with table_fields JSONB column
            cursor.execute('CREATE SCHEMA IF NOT EXISTS "GENERAL";')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS "GENERAL".source_table_selection (
                    tbl_id SERIAL PRIMARY KEY,
                    source_id INTEGER NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    schema VARCHAR(100),
                    table_fields JSONB,
                    selected BOOLEAN DEFAULT TRUE,
                    added_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_synced TIMESTAMP,
                    UNIQUE(source_id, table_name, schema)
                );
            ''')

            # Check if table_fields column exists, if not add it
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'GENERAL'
                AND table_name = 'source_table_selection'
                AND column_name = 'table_fields'
            """)
            if not cursor.fetchone():
                cursor.execute('''
                    ALTER TABLE "GENERAL".source_table_selection
                    ADD COLUMN table_fields JSONB
                ''')

            # Insert or update selected tables
            saved_tables = []
            for table in tables:
                table_name = table.get('table_name')
                schema = table.get('schema')
                # Coerce to str for psycopg2 (avoid "can't adapt type 'dict'")
                table_name = str(table_name) if table_name is not None else None
                schema = str(schema) if schema is not None else None

                if not table_name:
                    continue

                # Use INSERT ... ON CONFLICT to handle duplicates
                cursor.execute('''
                    INSERT INTO "GENERAL".source_table_selection
                    (source_id, table_name, schema, selected, added_on)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (source_id, table_name, schema)
                    DO UPDATE SET selected = TRUE, added_on = CURRENT_TIMESTAMP
                    RETURNING tbl_id, source_id, table_name, schema, selected, added_on
                ''', (source_id, table_name, schema, True))

                row = cursor.fetchone()
                if row:
                    saved_tables.append({
                        'tbl_id': row[0],
                        'source_id': row[1],
                        'table_name': row[2],
                        'schema': row[3],
                        'selected': row[4],
                        'added_on': row[5].isoformat() if row[5] else None,
                    })

            cursor.close()
            conn.close()

            return Response({
                'message': f'Successfully saved {len(saved_tables)} table(s)',
                'tables': saved_tables
            }, status=status.HTTP_201_CREATED)

        except Exception as e:
            import traceback
            logger.error(f"Error saving selected tables: {e}")
            logger.error(traceback.format_exc())
            return Response(
                {"error": f"Failed to save selected tables: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def get(self, request, source_id):
        """
        Get selected tables for a source connection.
        Path params:
        - source_id: ID of the source connection (from URL path)
        """
        try:
            # source_id comes from URL path parameter
            source_id = int(source_id)

            # Get user and customer
            user = request.user
            customer = ensure_user_has_customer(user)

            # Connect to customer's database
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=customer.cust_db
            )
            conn.autocommit = True
            cursor = conn.cursor()

            # Fetch selected tables with fields
            cursor.execute('''
                SELECT tbl_id, source_id, table_name, schema, table_fields, selected, added_on, last_synced
                FROM "GENERAL".source_table_selection
                WHERE source_id = %s AND selected = TRUE
                ORDER BY added_on DESC
            ''', (source_id,))

            tables = []
            for row in cursor.fetchall():
                table_fields = row[4]  # JSONB column
                fields_list = None
                if table_fields:
                    try:
                        if isinstance(table_fields, str):
                            fields_list = json.loads(table_fields)
                        else:
                            fields_list = table_fields  # Already a dict/list
                    except Exception:
                        fields_list = None

                tables.append({
                    'tbl_id': row[0],
                    'source_id': row[1],
                    'table_name': row[2],
                    'schema': row[3],
                    'table_fields': fields_list,
                    'fields_count': len(fields_list) if fields_list else 0,
                    'selected': row[5],
                    'added_on': row[6].isoformat() if row[6] else None,
                    'last_synced': row[7].isoformat() if row[7] else None,
                    'is_synced': row[7] is not None,  # Add sync status flag
                })

            cursor.close()
            conn.close()

            return Response({
                'source_id': source_id,
                'tables': tables,
                'count': len(tables)
            }, status=status.HTTP_200_OK)

        except Exception as e:
            import traceback
            logger.error(f"Error fetching selected tables: {e}")
            logger.error(traceback.format_exc())
            return Response(
                {"error": f"Failed to fetch selected tables: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def delete(self, request, source_id):
        """
        Unselect tables (set selected = FALSE) for a source connection.
        Path params:
        - source_id: ID of the source connection (from URL path)
        Body: {
            "table_names": [str] (optional - if not provided, unselects all)
        }
        """
        try:
            # source_id comes from URL path parameter
            source_id = int(source_id)
            table_names = request.data.get('table_names', [])

            # Get user and customer
            user = request.user
            customer = ensure_user_has_customer(user)

            # Connect to customer's database
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=customer.cust_db
            )
            conn.autocommit = True
            cursor = conn.cursor()

            if table_names:
                # Unselect specific tables
                ','.join(['%s'] * len(table_names))
                cursor.execute('''
                    UPDATE "GENERAL".source_table_selection
                    SET selected = FALSE
                    WHERE source_id = %s AND table_name IN ({placeholders})
                ''', [source_id, *table_names])
            else:
                # Unselect all tables for this source
                cursor.execute('''
                    UPDATE "GENERAL".source_table_selection
                    SET selected = FALSE
                    WHERE source_id = %s
                ''', (source_id,))

            updated_count = cursor.rowcount
            cursor.close()
            conn.close()

            return Response({
                'message': f'Successfully unselected {updated_count} table(s)'
            }, status=status.HTTP_200_OK)

        except Exception as e:
            import traceback
            logger.error(f"Error unselecting tables: {e}")
            logger.error(traceback.format_exc())
            return Response(
                {"error": f"Failed to unselect tables: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class SourceLiveSchemaView(APIView):
    """
    Fetch the live column schema for a specific table from a source connection.
    Used for schema drift detection when a canvas opens.
    Returns: { "columns": [{"name": str, "type": str}] }
    Nothing is persisted to the platform database.
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, source_id, table_name):
        """
        GET /api/api-customer/sources/{source_id}/table/{table_name}/schema
        Query params:
          - schema: optional schema name (defaults to connection default schema)
        """
        try:
            source_id = int(source_id)
            schema_param = request.query_params.get('schema', '').strip()

            user = request.user
            customer = ensure_user_has_customer(user)

            # Connect to main app database where GENERAL.source lives
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=settings.DATABASES['default']['NAME'],
            )
            conn.autocommit = True
            db_cursor = conn.cursor()

            db_cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'GENERAL' AND table_name = 'source'
            """)
            col_names = [r[0] for r in db_cursor.fetchall()]
            name_col = 'source_name' if 'source_name' in col_names else 'src_name'
            config_col = 'source_config' if 'source_config' in col_names else 'src_config'

            db_cursor.execute(
                f'SELECT id, {name_col}, {config_col}, created_on FROM "GENERAL".source WHERE id = %s',
                (source_id,)
            )
            row = db_cursor.fetchone()
            db_cursor.close()
            conn.close()

            if not row:
                return Response({"error": "Source connection not found"}, status=status.HTTP_404_NOT_FOUND)

            _, _, source_config, created_on = row
            decrypted = decrypt_source_data(source_config, customer.cust_id, created_on)
            if not decrypted:
                return Response({"error": "Failed to decrypt source configuration"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            db_type = decrypted.get('db_type', '').lower()
            effective_schema = schema_param or decrypted.get('schema') or ''

            EXTRACTION_SERVICE_URL = getattr(settings, 'FASTAPI_EXTRACTION_SERVICE_URL', 'http://localhost:8001')

            async def _fetch_columns():
                async with httpx.AsyncClient(timeout=30.0) as client:
                    resp = await client.post(
                        f"{EXTRACTION_SERVICE_URL}/metadata/columns",
                        json={
                            "db_type": db_type,
                            "connection_config": {
                                "hostname": decrypted.get('hostname'),
                                "port": decrypted.get('port'),
                                "database": decrypted.get('database'),
                                "user": decrypted.get('user'),
                                "password": decrypted.get('password'),
                                "schema": effective_schema,
                                "service_name": decrypted.get('service_name'),
                            },
                            "table_name": table_name,
                            "schema": effective_schema,
                        }
                    )
                    resp.raise_for_status()
                    return resp.json()

            result = asyncio.run(_fetch_columns())
            raw_columns = result.get('columns', [])

            columns = []
            for col in raw_columns:
                col_name = col.get('name') or col.get('column_name') or col.get('column') or ''
                col_type = col.get('type') or col.get('data_type') or col.get('datatype') or 'unknown'
                if col_name:
                    columns.append({"name": col_name, "type": str(col_type).lower()})

            return Response(
                {"columns": columns, "table": table_name, "schema": effective_schema},
                status=status.HTTP_200_OK
            )

        except httpx.ConnectError:
            return Response({"error": "FastAPI extraction service is not available"}, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        except httpx.HTTPError as exc:
            logger.error(f"[SourceLiveSchemaView] FastAPI HTTP error: {exc}")
            return Response({"error": f"FastAPI service error: {exc!s}"}, status=status.HTTP_502_BAD_GATEWAY)
        except Exception as exc:
            import traceback
            logger.error(f"[SourceLiveSchemaView] {exc}\n{traceback.format_exc()}")
            return Response({"error": f"Failed to fetch live schema: {exc!s}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
