"""
Destination-related API views.
Handles destination connection management.
"""
import json
import logging

from django.conf import settings
import psycopg2
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from api.authentications import JWTCookieAuthentication
from api.connections.encryption import encrypt_field
from api.models import Customer
from api.serializers import DestinationConnectionSerializer
from api.utils.helpers import (
    decrypt_source_data,
    ensure_user_has_customer,
    generate_encryption_key,
)

logger = logging.getLogger(__name__)

def _get_destination_table_columns(cursor):
    """Return (name_column, config_column) for GENERAL.destination. Uses actual DB columns."""
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'GENERAL' AND table_name = 'destination'
    """)
    columns = [row[0] for row in cursor.fetchall()]
    name_col = next(
        (c for c in ['destination_name', 'name', 'dest_name', 'dst_name'] if c in columns),
        None
    )
    config_col = next(
        (c for c in ['destination_config', 'config_data', 'config', 'dest_config', 'dst_config'] if c in columns),
        None
    )
    return (name_col, config_col)

class DestinationConnectionCreateView(APIView):
    """API view for creating destination connections."""
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """Create a new destination connection."""
        logger.info(f"[DESTINATION CREATE] Request data: {request.data}")
        serializer = DestinationConnectionSerializer(data=request.data)
        if serializer.is_valid():
            try:
                # Get the customer object (ensure user has a customer, create if missing)
                user = request.user
                customer = ensure_user_has_customer(user)

                # Get database type
                db_type = serializer.validated_data.get("db_type", "hana").lower()

                # Create destination configuration based on database type
                if db_type == 'hana':
                    # HANA configuration
                    destination_config = {
                        'db_type': 'hana',
                        'hostname': serializer.validated_data.get("hostname"),
                        'instance_number': serializer.validated_data.get("instance_number"),
                        'mode': serializer.validated_data.get("mode"),
                        'destination_schema_name': serializer.validated_data.get("destination_schema_name"),
                        's4_schema_name': serializer.validated_data.get("s4_schema_name"),
                    }

                    # Add database type and name based on mode
                    if serializer.validated_data.get("mode") == 'multiple_containers':
                        destination_config['database_type'] = serializer.validated_data.get("database_type")

                        # Add the appropriate database name based on database_type
                        if serializer.validated_data.get("database_type") == 'tenant_database':
                            destination_config['tenant_db_name'] = serializer.validated_data.get("tenant_db_name")
                        elif serializer.validated_data.get("database_type") == 'system_database':
                            destination_config['system_db_name'] = serializer.validated_data.get("system_db_name")
                else:
                    # PostgreSQL/MySQL/SQL Server/Oracle configuration
                    destination_config = {
                        'db_type': db_type,
                        'hostname': serializer.validated_data.get("hostname"),
                        'port': serializer.validated_data.get("port"),
                        'database': serializer.validated_data.get("database"),
                        'user': serializer.validated_data.get("user"),
                        'password': serializer.validated_data.get("password"),
                    }
                    # destination_schema_name is optional for non-HANA destinations
                    if serializer.validated_data.get("destination_schema_name"):
                        destination_config['destination_schema_name'] = serializer.validated_data.get("destination_schema_name")

                # Connect to the customer's database and insert into GENERAL.destination
                conn = psycopg2.connect(
                    host=settings.DATABASES['default']['HOST'],
                    port=settings.DATABASES['default']['PORT'],
                    user=settings.DATABASES['default']['USER'],
                    password=settings.DATABASES['default']['PASSWORD'],
                    database=customer.cust_db
                )
                conn.autocommit = True
                cursor = conn.cursor()

                # Ensure GENERAL schema and destination table exist (create if missing)
                cursor.execute('CREATE SCHEMA IF NOT EXISTS "GENERAL";')
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'GENERAL' AND table_name = 'destination'
                """)
                existing_columns = [row[0] for row in cursor.fetchall()]
                if not existing_columns:
                    # Create destination table (same structure as Customer.create_customer_schemas)
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS "GENERAL".destination (
                            id SERIAL PRIMARY KEY,
                            dest_name VARCHAR(255),
                            dest_config TEXT,
                            project_id INTEGER,
                            created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            is_active BOOLEAN DEFAULT TRUE
                        );
                    """)
                    logger.info("Created GENERAL.destination table in customer database")
                    existing_columns = ['id', 'dest_name', 'dest_config', 'project_id', 'created_on', 'modified_on', 'is_active']

                # Re-fetch columns in case we just created the table
                if not existing_columns:
                    cursor.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'GENERAL' AND table_name = 'destination'
                    """)
                    existing_columns = [row[0] for row in cursor.fetchall()]
                if not existing_columns:
                    cursor.close()
                    conn.close()
                    return Response(
                        {"error": "Destination table could not be created in customer database"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )

                # Determine the actual column names (handle different schema versions)
                if 'name' in existing_columns:
                    pass
                elif 'dest_name' in existing_columns:
                    pass
                elif 'destination_name' not in existing_columns:
                    # If none of the expected name columns exist, use the first available
                    logger.warning(f"Unexpected column structure. Available columns: {existing_columns}")
                    return Response(
                        {"error": f"Destination table has unexpected structure. Available columns: {existing_columns}"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )

                if 'config_data' in existing_columns:
                    pass
                elif 'dst_config' in existing_columns:
                    pass
                elif 'dest_config' in existing_columns:
                    pass
                elif 'destination_config' not in existing_columns:
                    logger.warning(f"Config column not found. Available columns: {existing_columns}")
                    return Response(
                        {"error": f"Destination table missing config column. Available columns: {existing_columns}"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )

                # Add project_id column if it doesn't exist
                if 'project_id' not in existing_columns:
                    try:
                        cursor.execute('ALTER TABLE "GENERAL".destination ADD COLUMN project_id INTEGER;')
                        logger.info("Added project_id column to GENERAL.destination table")
                        existing_columns.append('project_id')
                    except Exception as e:
                        logger.warning(f"Note: Could not add project_id column (may already exist): {e}")

                # Get the current timestamp from the database to ensure consistency
                cursor.execute("SELECT NOW()")
                db_timestamp = cursor.fetchone()[0]

                # Generate encryption key using the database timestamp
                encryption_key = generate_encryption_key(customer.cust_id, db_timestamp)

                # Encrypt the entire JSON configuration using the database timestamp
                encrypted_config = encrypt_field(destination_config, encryption_key)

                # Get project_id from request data (optional)
                project_id = request.data.get('project_id') or serializer.validated_data.get('project_id')
                if project_id:
                    try:
                        project_id = int(project_id)
                    except (ValueError, TypeError):
                        project_id = None

                # Check if project_id column exists (after potential ALTER)
                has_project_id = 'project_id' in existing_columns

                # Build insert SQL using detected column names
                if has_project_id:
                    insert_sql = '''
                        INSERT INTO "GENERAL".destination ({name_column}, {config_column}, project_id, created_on, modified_on, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    '''
                    insert_params = (
                        serializer.validated_data.get("destination_name"),
                        json.dumps(encrypted_config),
                        project_id,
                        db_timestamp,
                        db_timestamp,
                        True  # is_active defaults to True
                    )
                else:
                    insert_sql = '''
                        INSERT INTO "GENERAL".destination ({name_column}, {config_column}, created_on, modified_on, is_active)
                        VALUES (%s, %s, %s, %s, %s)
                    '''
                    insert_params = (
                        serializer.validated_data.get("destination_name"),
                        json.dumps(encrypted_config),
                        db_timestamp,
                        db_timestamp,
                        True  # is_active defaults to True
                    )

                cursor.execute(insert_sql, insert_params)

                # Verify the insertion using the same column names as the insert
                cursor.execute(
                    '''
                    SELECT {name_col_q}, {config_col_q}, created_on, modified_on, is_active
                    FROM "GENERAL".destination
                    WHERE {name_col_q} = %s
                    ORDER BY {order_col} DESC LIMIT 1
                    ''',
                    (serializer.validated_data.get("destination_name"),)
                )
                fetched_row = cursor.fetchone()
                if fetched_row:
                    # Configuration successfully encrypted and stored (decrypt not needed for response)
                    pass

                cursor.close()
                conn.close()

                return Response({
                    "message": "Destination connection added successfully",
                    "destination_name": serializer.validated_data.get("destination_name"),
                    "mode": serializer.validated_data.get("mode")
                }, status=status.HTTP_201_CREATED)

            except Customer.DoesNotExist:
                return Response(
                    {"error": "Customer not found"},
                    status=status.HTTP_404_NOT_FOUND
                )
            except Exception as e:
                return Response(
                    {"error": f"Failed to store destination details in customer database: {e!s}"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        logger.error(f"[DESTINATION CREATE] Validation errors: {serializer.errors}")
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class CustomerDestinationsView(APIView):
    """
    API view to fetch destinations for a specific customer.
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            # Get the customer, create if doesn't exist
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

            # Check if destination table exists and what columns it has
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'GENERAL' AND table_name = 'destination'
            """)
            columns = [row[0] for row in cursor.fetchall()]

            # If table doesn't exist, return empty list
            if not columns:
                cursor.close()
                conn.close()
                return Response({
                    'customer_id': customer.cust_id,
                    'customer_name': customer.name,
                    'destinations': []
                })

            # Determine the actual column names (handle different schema versions)
            # NOTE: Some DBs use dest_name/dest_config (not dst_*). Keep fallbacks broad.
            def pick_column(candidates):
                return next((c for c in candidates if c in columns), None)

            name_column = pick_column(['destination_name', 'name', 'dest_name', 'dst_name'])
            config_column = pick_column(['destination_config', 'config_data', 'config', 'dest_config', 'dst_config'])

            if not name_column or not config_column:
                cursor.close()
                conn.close()
                return Response(
                    {'error': f'Invalid destination schema. Missing required columns. Found columns: {columns}'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            # Quote identifiers to safely handle reserved words / mixed-case columns
            name_column_sql = f'"{name_column}"'
            config_column_sql = f'"{config_column}"'
            has_project_id = 'project_id' in columns

            # Get optional project_id filter from query params
            project_id = request.query_params.get('project_id')
            if project_id:
                try:
                    project_id = int(project_id)
                except (ValueError, TypeError):
                    project_id = None

            # Build query with optional project_id filter
            if has_project_id:
                if project_id:
                    query = f'''
                        SELECT id, {name_column_sql}, {config_column_sql}, project_id, created_on, modified_on, is_active
                        FROM "GENERAL".destination
                        WHERE project_id = %s
                        ORDER BY created_on DESC
                    '''
                    cursor.execute(query, (project_id,))
                else:
                    query = f'''
                        SELECT id, {name_column_sql}, {config_column_sql}, project_id, created_on, modified_on, is_active
                        FROM "GENERAL".destination
                        ORDER BY created_on DESC
                    '''
                    cursor.execute(query)
            else:
                query = f'''
                    SELECT id, {name_column_sql}, {config_column_sql}, created_on, modified_on, is_active
                    FROM "GENERAL".destination
                    ORDER BY created_on DESC
                '''
                cursor.execute(query)

            destinations = []
            for row in cursor.fetchall():
                if has_project_id:
                    destination_id, destination_name, destination_config, project_id_val, created_on, modified_on, is_active = row
                else:
                    destination_id, destination_name, destination_config, created_on, modified_on, is_active = row
                    project_id_val = None

                # Decrypt the destination configuration
                decrypted_config = {}
                if destination_config:
                    try:
                        decrypted_config = decrypt_source_data(destination_config, customer.cust_id, created_on)
                    except Exception as e:
                        print(f"Error decrypting destination config: {e}")
                        decrypted_config = {}

                # Extract db_type from config (defaults to 'hana' if not present for backward compatibility)
                db_type = decrypted_config.get('db_type', 'hana') if decrypted_config else 'hana'

                # For HANA, port is instance_number; for others, it's port
                port_value = None
                if db_type == 'hana':
                    port_value = decrypted_config.get('instance_number') if decrypted_config else None
                else:
                    port_value = decrypted_config.get('port') if decrypted_config else None

                destination_data = {
                    'id': destination_id,
                    'destination_id': destination_id,
                    'destination_name': destination_name,
                    'name': destination_name,  # Alias for compatibility
                    'project_id': project_id_val,
                    'db_type': db_type,  # Include db_type in response
                    'hostname': decrypted_config.get('hostname') if decrypted_config else None,
                    'port': port_value,
                    'user': decrypted_config.get('user') if decrypted_config else None,  # For PostgreSQL/MySQL/etc
                    'password': None,  # Never return password
                    'database': decrypted_config.get('database') if decrypted_config else None,  # For PostgreSQL/MySQL/etc
                    'schema': decrypted_config.get('destination_schema_name') if decrypted_config else None,
                    'mode': decrypted_config.get('mode') if decrypted_config else None,
                    'database_type': decrypted_config.get('database_type') if decrypted_config else None,
                    'tenant_db_name': decrypted_config.get('tenant_db_name') if decrypted_config else None,
                    'system_db_name': decrypted_config.get('system_db_name') if decrypted_config else None,
                    's4_schema_name': decrypted_config.get('s4_schema_name') if decrypted_config else None,
                    'created_on': created_on.isoformat() if created_on else None,
                    'modified_on': modified_on.isoformat() if modified_on else None,
                    'is_active': is_active
                }
                destinations.append(destination_data)

            cursor.close()
            conn.close()

            return Response({
                'customer_id': customer.cust_id,
                'customer_name': customer.name,
                'destinations': destinations
            }, status=status.HTTP_200_OK)

        except Customer.DoesNotExist:
            return Response(
                {'error': 'Customer not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            logger.error(f"Error fetching customer destinations: {e}")
            logger.error(f"Traceback: {error_trace}")
            return Response(
                {'error': f'Failed to fetch destinations: {e!s}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class DestinationTablesView(APIView):
    """
    GET /api/api-customer/destinations/<destination_id>/tables/
    Returns list of tables for the destination connection (for Upsert/Replace table selection).
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, destination_id):
        try:
            user = request.user
            customer = ensure_user_has_customer(user)

            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=customer.cust_db
            )
            conn.autocommit = True
            cursor = conn.cursor()

            _, config_col = _get_destination_table_columns(cursor)
            if not config_col:
                cursor.close()
                conn.close()
                return Response(
                    {"tables": [], "error": "Destination table missing config column"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            cursor.execute('''
                SELECT {config_col_q}, created_on
                FROM "GENERAL".destination
                WHERE id = %s
            ''', (destination_id,))
            row = cursor.fetchone()
            cursor.close()
            conn.close()

            if not row:
                return Response({"error": "Destination not found"}, status=status.HTTP_404_NOT_FOUND)

            destination_config_enc, created_on = row
            decrypted_config = {}
            if destination_config_enc:
                try:
                    decrypted_config = decrypt_source_data(destination_config_enc, customer.cust_id, created_on)
                except Exception as e:
                    logger.warning(f"Error decrypting destination config: {e}")

            db_type = (decrypted_config.get('db_type') or decrypted_config.get('database_type') or 'postgresql').lower()
            connection_config = {
                "host": decrypted_config.get('hostname') or decrypted_config.get('host'),
                "port": decrypted_config.get('port') or decrypted_config.get('instance_number'),
                "database": decrypted_config.get('database') or decrypted_config.get('tenant_db_name'),
                "username": decrypted_config.get('user') or decrypted_config.get('username'),
                "password": decrypted_config.get('password'),
            }
            if not connection_config.get("host") and not connection_config.get("database"):
                return Response({"tables": [], "error": "Destination connection config incomplete"})

            try:
                import asyncio

                import httpx
                EXTRACTION_SERVICE_URL = getattr(settings, 'EXTRACTION_SERVICE_URL', 'http://localhost:8001')
                async def fetch_tables():
                    async with httpx.AsyncClient(timeout=15.0) as client:
                        response = await client.post(
                            f"{EXTRACTION_SERVICE_URL}/metadata/tables",
                            json={
                                "connection_type": db_type,
                                "connection_config": connection_config,
                            }
                        )
                        return response.json()
                result = asyncio.run(fetch_tables())
                tables = result.get('tables', result) if isinstance(result, dict) else (result if isinstance(result, list) else [])
                if not isinstance(tables, list):
                    tables = []
                return Response({"tables": tables})
            except Exception as e:
                logger.warning(f"Error fetching tables for destination {destination_id}: {e}")
                return Response({"tables": [], "error": str(e)})

        except Exception as e:
            logger.error(f"Destination tables error: {e}")
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class DestinationEditView(APIView):
    """API view for editing existing destination connections."""

    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, destination_id):
        """Get destination data for editing."""
        try:
            # Get the customer object]
            user = request.user
            customer = user.cust_id

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

            name_col, config_col = _get_destination_table_columns(cursor)
            if not name_col or not config_col:
                cursor.close()
                conn.close()
                return Response(
                    {"error": "Destination table has unexpected structure"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            _name_q, _config_q = f'"{name_col}"', f'"{config_col}"'

            cursor.execute('''
                SELECT {name_q}, {config_q}, created_on, modified_on, is_active
                FROM "GENERAL".destination
                WHERE id = %s
            ''', (destination_id,))

            destination_row = cursor.fetchone()
            if not destination_row:
                cursor.close()
                conn.close()
                return Response(
                    {"error": "Destination not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            destination_name_db, destination_config, created_on, modified_on, is_active = destination_row

            # Decrypt the destination configuration
            decrypted_config = {}
            if destination_config:
                try:
                    decrypted_config = decrypt_source_data(destination_config, customer.cust_id, created_on)
                except Exception as e:
                    print(f"Error decrypting destination config: {e}")
                    decrypted_config = {}

            destination_data = {
                'destination_name': destination_name_db,
                'hostname': decrypted_config.get('hostname') if decrypted_config else '',
                'instance_number': decrypted_config.get('instance_number') if decrypted_config else '',
                'mode': decrypted_config.get('mode') if decrypted_config else '',
                'database_type': decrypted_config.get('database_type') if decrypted_config else '',
                'tenant_db_name': decrypted_config.get('tenant_db_name') if decrypted_config else '',
                'system_db_name': decrypted_config.get('system_db_name') if decrypted_config else '',
                'destination_schema_name': decrypted_config.get('destination_schema_name') if decrypted_config else '',
                's4_schema_name': decrypted_config.get('s4_schema_name') if decrypted_config else '',
                'created_on': created_on.isoformat() if created_on else None,
                'modified_on': modified_on.isoformat() if modified_on else None,
                'is_active': is_active
            }

            cursor.close()
            conn.close()

            return Response({
                'customer_id': customer.cust_id,
                'customer_name': customer.name,
                'destination': destination_data
            }, status=status.HTTP_200_OK)

        except Customer.DoesNotExist:
            return Response(
                {"error": "Customer not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            return Response(
                {"error": f"Failed to fetch destination data: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def put(self, request, destination_id):
        """Update an existing destination connection."""
        try:
            # Get the customer object
            user = request.user
            customer = user.cust_id

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

            name_col, config_col = _get_destination_table_columns(cursor)
            if not name_col or not config_col:
                cursor.close()
                conn.close()
                return Response(
                    {"error": "Destination table has unexpected structure"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            _name_q, _config_q = f'"{name_col}"', f'"{config_col}"'

            cursor.execute('''
                SELECT {name_q}, {config_q}, created_on, is_active
                FROM "GENERAL".destination
                WHERE id = %s
            ''', (destination_id,))

            existing_destination = cursor.fetchone()
            if not existing_destination:
                cursor.close()
                conn.close()
                return Response(
                    {"error": "Destination not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Get the original created_on timestamp to maintain encryption consistency
            original_destination_name, original_destination_config, original_created_on, original_is_active = existing_destination

            # Create new destination configuration (dict for encrypt)
            destination_config = {
                'hostname': request.data.get('hostname'),
                'instance_number': request.data.get('instance_number'),
                'mode': request.data.get('mode'),
                'destination_schema_name': request.data.get('destination_schema_name'),
                's4_schema_name': request.data.get('s4_schema_name'),
            }

            # Add database type and name based on mode
            if request.data.get('mode') == 'multiple_containers':
                destination_config['database_type'] = request.data.get('database_type')

                # Add the appropriate database name based on database_type
                if request.data.get('database_type') == 'tenant_database':
                    destination_config['tenant_db_name'] = request.data.get('tenant_db_name')
                elif request.data.get('database_type') == 'system_database':
                    destination_config['system_db_name'] = request.data.get('system_db_name')

            # Get current timestamp for modification
            cursor.execute("SELECT NOW()")
            current_timestamp = cursor.fetchone()[0]

            # Use the original created_on timestamp for encryption key generation
            # This ensures we can still decrypt the data
            encryption_key = generate_encryption_key(customer.cust_id, original_created_on)

            # Encrypt the new configuration using the same key
            encrypted_config = encrypt_field(destination_config, encryption_key)

            # Update the destination record (use detected column names)
            update_sql = '''
                UPDATE "GENERAL".destination
                SET {name_q} = %s,
                    {config_q} = %s,
                    modified_on = %s,
                    is_active = %s
                WHERE id = %s
            '''

            cursor.execute(
                update_sql,
                (
                    request.data.get('destination_name'),
                    json.dumps(encrypted_config),
                    current_timestamp,
                    request.data.get('is_active', True),
                    destination_id
                )
            )

            cursor.close()
            conn.close()

            return Response({
                "message": "Destination connection updated successfully",
                "modified_on": current_timestamp.isoformat()
            }, status=status.HTTP_200_OK)

        except Customer.DoesNotExist:
            return Response(
                {"error": "Customer not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            return Response(
                {"error": f"Failed to update destination connection: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class DestinationDeleteView(APIView):
    """API view for deleting existing destination connections."""
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def delete(self, request, destination_id):
        """Delete an existing destination connection."""
        try:
            # Get the customer object
            customer = request.user.cust_id

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

            cursor.execute('''
                SELECT id FROM "GENERAL".destination WHERE id = %s
            ''', (destination_id,))
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                return Response(
                    {"error": "Destination not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Delete the destination record
            delete_sql = '''
                DELETE FROM "GENERAL".destination
                WHERE id = %s
            '''

            cursor.execute(delete_sql, (destination_id,))

            # Verify the deletion
            cursor.execute('''
                SELECT COUNT(*) FROM "GENERAL".destination
                WHERE id = %s
            ''', (destination_id,))

            remaining_count = cursor.fetchone()[0]
            if remaining_count > 0:
                return Response(
                    {"error": "Failed to delete destination"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            cursor.close()
            conn.close()

            return Response({
                "message": "Destination connection deleted successfully",
            }, status=status.HTTP_200_OK)

        except Customer.DoesNotExist:
            return Response(
                {"error": "Customer not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            return Response(
                {"error": f"Failed to delete destination connection: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
