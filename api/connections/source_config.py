# Moved from: api/utils/helpers.py
"""
Shared utility functions used across multiple views.
These functions are moved from api/views.py to maintain separation of concerns.
"""
import hashlib
import json
import logging

import psycopg2
import pyodbc

from api.connections.encryption import decrypt_field

logger = logging.getLogger(__name__)

def generate_encryption_key(cust_id, created_on):
    """
    Generate encryption key from cust_id + created_on date.
    Returns a hash that can be used as an integer for the encryption function.
    """
    # Convert created_on to string and combine with cust_id
    key_string = f"{cust_id}{created_on.strftime('%Y%m%d%H%M%S')}"

    # Generate a hash and convert to integer
    hash_object = hashlib.sha256(key_string.encode())
    # Take first 8 bytes and convert to integer (to fit within reasonable range)
    return int(hash_object.hexdigest()[:8], 16)

def create_connection_config(validated_data):
    """
    Create a clean JSON configuration from validated data, excluding null/empty values.
    """
    connection_config = {}

    # Define the fields to include in the config
    config_fields = ['hostname', 'user', 'password', 'schema', 'port']

    for field in config_fields:
        value = validated_data.get(field)
        # Only add non-null, non-empty values
        if value is not None and value != '':
            if field == 'port':
                connection_config[field] = value
            elif value.strip():  # For string fields, check if not empty after stripping
                connection_config[field] = value

    return connection_config

def test_database_connection(hostname, port, user, password, schema=None):
    """
    Test database connection with provided credentials.
    Supports both PostgreSQL and SQL Server connections.
    Returns (success, error_message)
    """
    try:
        # Validate required parameters
        if not hostname or not user or not password:
            return False, "Missing required connection parameters (hostname, user, password)"

        if port is None or port <= 0:
            return False, "Invalid port number"

        # Determine database type based on port
        port = int(port)
        if port == 1433:
            # SQL Server connection
            return test_sqlserver_connection(hostname, port, user, password, schema)
        elif port == 5432:
            # PostgreSQL connection
            return test_postgresql_connection(hostname, port, user, password, schema)
        else:
            # Try both PostgreSQL and SQL Server
            # First try SQL Server
            success, error = test_sqlserver_connection(hostname, port, user, password, schema)
            if success:
                return True, None

            # Then try PostgreSQL
            success, error = test_postgresql_connection(hostname, port, user, password, schema)
            if success:
                return True, None
            else:
                return False, f"Connection failed for both SQL Server and PostgreSQL. Last error: {error}"

    except ValueError as e:
        return False, f"Invalid parameter: {e!s}"
    except Exception as e:
        return False, f"Unexpected error: {e!s}"

def test_sqlserver_connection(hostname, port, user, password, schema=None):
    """
    Test SQL Server database connection.
    """
    try:
        # SQL Server connection string
        driver = 'ODBC Driver 17 for SQL Server'
        database = schema.strip() if schema and schema.strip() else 'master'

        conn_str = (
            f'Driver={driver};'
            f'Server={hostname.strip()},{port};'
            f'Database={database};'
            f'UID={user.strip()};'
            f'PWD={password};'
            'Connection Timeout=10;'
        )

        conn = pyodbc.connect(conn_str)
        conn.close()
        return True, None

    except pyodbc.Error as e:
        error_msg = str(e)
        if "login failed" in error_msg.lower():
            return False, "Authentication failed: Invalid username or password"
        elif "could not connect" in error_msg.lower():
            return False, f"Connection failed: Unable to connect to {hostname}:{port}"
        elif "timeout" in error_msg.lower():
            return False, "Connection timeout: Server did not respond within 10 seconds"
        else:
            return False, f"SQL Server connection error: {error_msg}"
    except Exception as e:
        return False, f"SQL Server unexpected error: {e!s}"

def test_postgresql_connection(hostname, port, user, password, schema=None):
    """
    Test PostgreSQL database connection.
    """
    try:
        # Build connection parameters
        conn_params = {
            'host': hostname.strip(),
            'port': port,
            'user': user.strip(),
            'password': password,
        }

        # Add database name if schema is provided, otherwise use 'postgres'
        if schema and schema.strip():
            conn_params['database'] = schema.strip()
        else:
            conn_params['database'] = 'postgres'

        # Test the connection with timeout
        conn = psycopg2.connect(
            **conn_params,
            connect_timeout=10  # 10 second timeout
        )
        conn.close()
        return True, None

    except psycopg2.OperationalError as e:
        error_msg = str(e)
        if "authentication failed" in error_msg.lower():
            return False, "Authentication failed: Invalid username or password"
        elif "could not connect to server" in error_msg.lower():
            return False, f"Connection failed: Unable to connect to {hostname}:{port}"
        elif "timeout expired" in error_msg.lower():
            return False, "Connection timeout: Server did not respond within 10 seconds"
        else:
            return False, f"PostgreSQL connection error: {error_msg}"
    except psycopg2.InterfaceError as e:
        return False, f"PostgreSQL interface error: {e!s}"
    except Exception as e:
        return False, f"PostgreSQL unexpected error: {e!s}"

def convert_user_date_format_to_strftime(user_date_format):
    """
    Convert user-friendly date format (e.g., 'MM-DD-YYYY') to Python strftime format (e.g., '%m-%d-%Y').

    Args:
        user_date_format: String format like 'MM-DD-YYYY', 'DD-MM-YYYY', etc.

    Returns:
        Python strftime format string
    """
    # Mapping of user-friendly format to strftime format
    format_mapping = {
        'YYYY': '%Y',
        'YY': '%y',
        'MM': '%m',
        'DD': '%d',
        'HH': '%H',
        'mm': '%M',
        'SS': '%S'
    }

    strftime_format = user_date_format
    for user_fmt, strftime_fmt in format_mapping.items():
        strftime_format = strftime_format.replace(user_fmt, strftime_fmt)

    return strftime_format

def format_date_columns(data, columns, date_format):
    """
    Format date and timestamp columns in the data based on column datatypes.

    Args:
        data: List of dictionaries containing row data
        columns: List of tuples containing (column_name, data_type, is_nullable)
        date_format: String format for output dates in strftime format

    Returns:
        Formatted data with dates properly formatted
    """
    if not data:
        return data

    # Identify date/timestamp columns based on datatype
    date_columns = []
    time_columns = []  # Track columns that should include time

    for col in columns:
        col_name, col_type = col[0], col[1].lower()
        # Check for date, timestamp, or datetime datatypes (including PostgreSQL variations)
        if any(dtype in col_type for dtype in ['timestamp', 'datetime']):
            # Timestamp columns may include time information
            date_columns.append(col_name)
            time_columns.append(col_name)
        elif 'date' in col_type and 'time' not in col_type:
            # Pure date columns (not timestamp)
            date_columns.append(col_name)

    # If no date columns found, return data as is
    if not date_columns:
        return data

    # Format date columns in each row
    for row in data:
        for col_name in date_columns:
            if col_name in row and row[col_name] is not None:
                value = row[col_name]
                try:
                    # Determine format to use based on whether this column should include time
                    format_to_use = date_format

                    # If value is a string (ISO format from database), parse it first
                    if isinstance(value, str):
                        # Try parsing ISO format or other common formats
                        import dateutil.parser
                        parsed_date = dateutil.parser.parse(value)

                        # For timestamp columns, check if time component exists and is not midnight
                        if col_name in time_columns:
                            # Check if the time component is not 00:00:00
                            if parsed_date.hour != 0 or parsed_date.minute != 0 or parsed_date.second != 0:
                                # Append time format to date format
                                format_to_use = f"{date_format} %H:%M:%S"

                        row[col_name] = parsed_date.strftime(format_to_use)
                    # If value has strftime method (datetime object), format directly
                    elif hasattr(value, 'strftime'):
                        # For timestamp columns, check if time component exists and is not midnight
                        if col_name in time_columns:
                            # Check if the time component is not 00:00:00
                            if value.hour != 0 or value.minute != 0 or value.second != 0:
                                # Append time format to date format
                                format_to_use = f"{date_format} %H:%M:%S"

                        row[col_name] = value.strftime(format_to_use)
                except Exception:
                    # If formatting fails, convert to ISO format string for JSON serialization
                    try:
                        if hasattr(value, 'isoformat'):
                            row[col_name] = value.isoformat()
                    except Exception:
                        # Last resort: keep original value
                        continue

    return data

def decrypt_source_data(encrypted_data, cust_id, created_on):
    """
    Decrypt source data using the same key generation logic.
    """
    if not encrypted_data:
        return None

    try:
        # Parse the JSON data
        data_list = json.loads(encrypted_data) if isinstance(encrypted_data, str) else encrypted_data

        # Generate the same encryption key
        encryption_key = generate_encryption_key(cust_id, created_on)

        # Decrypt the field
        decrypted = decrypt_field(
            data_list[0],  # encrypted_data
            encryption_key,  # cmp_id
            data_list[1],   # nonce
            data_list[2],   # tag
            data_list[3],   # salt
            data_list[4],   # original_type
            data_list[5]    # iterations
        )

        # Handle case where decrypt_field returns bytes or string
        if isinstance(decrypted, bytes):
            try:
                # Try to decode as UTF-8 and parse as JSON
                decrypted_str = decrypted.decode('utf-8')
                return json.loads(decrypted_str)
            except Exception:
                # If not JSON, return as string
                return decrypted.decode('utf-8')
        elif isinstance(decrypted, str):
            try:
                # Try to parse as JSON if it's a JSON string
                return json.loads(decrypted)
            except Exception:
                # If not JSON, return as string
                return decrypted
        else:
            # Already a dict/list
            return decrypted
    except Exception as e:
        logger.error(f"Error decrypting data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def ensure_user_has_customer(user):
    """
    Ensure user has a customer. If not, create a default customer and database.
    Returns the customer object.
    """
    from django.db import connection

    from api.models import Country, Customer, User

    if user.cust_id:
        return user.cust_id

    # Create a default customer for the user
    # Get or create a default country (you may want to adjust this)
    default_country, _ = Country.objects.get_or_create(
        country_id='US',
        defaults={'name': 'United States'}
    )

    # Create customer with minimal required fields
    customer = Customer(
        name=f"{user.first_name} {user.last_name}'s Organization",
        street1="Not Provided",
        city="Not Provided",
        region="Not Provided",
        country=default_country,
        phone="Not Provided",
        created_by=user.email,
        active=True
    )
    customer.save()  # This will auto-generate cust_id and cust_db

    # Create the customer database
    try:
        customer.create_customer_database()
    except Exception as e:
        print(f"Warning: Could not create customer database: {e}")
        # Continue anyway - database might already exist

    # Associate user with customer
    # Make sure customer is saved and has a primary key
    if not customer.pk:
        customer.save()

    # Use raw SQL to update cust_id to avoid Django ORM issues with primary key
    # The User model's primary key structure has changed over time (email -> id -> email)
    # Using raw SQL is more reliable
    try:
        with connection.cursor() as cursor:
            # Get the customer's database primary key (not the cust_id string)
            customer_pk = customer.pk

            # Update the user's cust_id using raw SQL
            # The ForeignKey field is stored as cust_id_id in the database
            cursor.execute(
                """
                UPDATE "user"
                SET cust_id_id = %s
                WHERE email = %s
                """,
                [customer_pk, user.email]
            )
        print(f"Successfully associated user {user.email} with customer {customer.cust_id} (PK: {customer_pk})")

        # Update the user object in memory so it reflects the change
        user.cust_id = customer
        user.cust_id_id = customer_pk

    except Exception as e:
        import traceback
        print(f"Error updating user cust_id with raw SQL: {e}")
        print(traceback.format_exc())
        # Fallback: try to refresh user and use ORM
        try:
            # Refresh user from database
            user_refreshed = User.objects.get(email=user.email)
            user_refreshed.cust_id = customer
            # Don't use update_fields - save all fields to avoid pk issues
            user_refreshed.save()
            user.cust_id = customer
        except Exception as e2:
            print(f"Error saving user with customer (fallback): {e2}")
            # Last resort: just set the attribute in memory
            # The association will be lost on next request, but at least the customer is created
            user.cust_id = customer

    return customer
