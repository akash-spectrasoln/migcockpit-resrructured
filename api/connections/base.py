# Moved from: api/utils/db_connection.py
"""
Centralized Database Connection Utility
Provides reusable connection configuration for customer databases.

This module eliminates duplication of database connection logic across the application.
Instead of manually constructing connection configs in every view, use these utilities.

Usage Examples:

    # Example 1: Get config for a specific customer database
    from api.utils.db_connection import get_customer_db_config

    config = get_customer_db_config('cust_db_123')
    # Returns: {'host': '...', 'port': 5432, 'database': 'cust_db_123', ...}

    # Example 2: Get config from Django request (sync)
    from api.utils.db_connection import get_customer_db_config_from_request

    def my_view(request):
        config = get_customer_db_config_from_request(request)
        if config:
            # Use config to connect to customer DB
            pass

    # Example 3: Get config from Django request (async)
    from api.utils.db_connection import get_customer_db_config_from_request_async

    async def my_async_function(request):
        config = await get_customer_db_config_from_request_async(request)
        if config:
            # Use config to connect to customer DB
            pass
"""
import logging
from typing import Any, Optional

try:
    from django.conf import settings  # type: ignore
    # Accessing configured will trigger a check if settings are loaded
    _ = settings.configured
except (ImportError, RuntimeError, Exception):
    # Fallback for non-Django environments (like standalone FastAPI services)
    settings = None

logger = logging.getLogger(__name__)


def get_default_db_config() -> dict[str, Any]:
  """
  Get database connection configuration for the Django default database.

  This centralizes how we read HOST / PORT / NAME / USER / PASSWORD so
  callers don't duplicate settings access everywhere.
  """
  import os

  host = os.getenv('DB_HOST') or os.getenv('POSTGRES_HOST')
  port = os.getenv('DB_PORT') or os.getenv('POSTGRES_PORT')
  user = os.getenv('DB_USER') or os.getenv('POSTGRES_USER')
  password = os.getenv('DB_PASSWORD') or os.getenv('POSTGRES_PASSWORD')
  database = os.getenv('DB_NAME') or os.getenv('POSTGRES_DB')

  if settings is not None and hasattr(settings, 'DATABASES'):
    try:
      default_db = settings.DATABASES.get('default', {})
      host = host or default_db.get('HOST')
      port = port or default_db.get('PORT')
      user = user or default_db.get('USER')
      password = password or default_db.get('PASSWORD')
      database = database or default_db.get('NAME')
    except Exception as e:
      logger.warning(f"Could not read default DB from settings.DATABASES: {e}")

  host = host or 'localhost'
  port = port or 5432
  user = user or 'postgres'
  password = password or ''
  database = database or 'postgres'

  return {
    'host': host,
    'port': int(port),
    'database': database,
    'user': user,
    'password': password,
  }


def get_default_db_connection():
  """
  Establish a psycopg2 connection to the Django default database.

  Prefer using this helper over inlining psycopg2.connect with settings
  directly in views or services.
  """
  import psycopg2

  cfg = get_default_db_config()
  return psycopg2.connect(
    host=cfg['host'],
    port=cfg['port'],
    dbname=cfg['database'],
    user=cfg['user'],
    password=cfg['password'],
  )

def get_customer_db_config(customer_db_name: str) -> dict[str, Any]:
    """
    Get database connection configuration for a customer database.

    Args:
        customer_db_name: Name of the customer database (e.g., 'cust_db_123')

    Returns:
        Dictionary with connection parameters (host, port, database, user, password)
    """
    import os

    # Prefer environment variables for standalone services
    host = os.getenv('DB_HOST') or os.getenv('POSTGRES_HOST')
    port = os.getenv('DB_PORT') or os.getenv('POSTGRES_PORT')
    user = os.getenv('DB_USER') or os.getenv('POSTGRES_USER')
    password = os.getenv('DB_PASSWORD') or os.getenv('POSTGRES_PASSWORD')

    # Fallback to Django settings if environment variables are not set and settings is available
    if not host and settings is not None and hasattr(settings, 'DATABASES'):
        try:
            default_db = settings.DATABASES.get('default', {})
            host = host or default_db.get('HOST')
            port = port or default_db.get('PORT')
            user = user or default_db.get('USER')
            password = password or default_db.get('PASSWORD')
        except Exception as e:
            logger.warning(f"Could not read from Django settings.DATABASES: {e}")

    # Default fallback values if nothing is found
    host = host or 'localhost'
    port = port or 5432
    user = user or 'postgres'
    password = password or ''

    return {
        "host": host,
        "port": port,
        "database": customer_db_name,
        "user": user,
        "password": password
    }

def get_customer_db_connection(config: dict[str, Any]):
    """
    Establish a connection to a customer database using the provided config.
    Config can contain 'host', 'hostname', 'port', 'database', 'user', 'username', 'password'.

    Args:
        config: Connection configuration dictionary

    Returns:
        psycopg2 connection object
    """
    import psycopg2
    host = config.get("host") or config.get("hostname")
    port = int(config.get("port", 5432))
    dbname = config.get("database")
    user = config.get("user") or config.get("username")
    password = config.get("password", "")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )

def get_customer_from_request(request) -> Optional[Any]:
    """
    Extract customer object from request.

    Tries multiple strategies:
    1. From request.user.cust_id_id
    2. From canvas_id in request data

    Args:
        request: Django request object

    Returns:
        Customer object or None
    """
    from api.models import Canvas, Customer

    customer = None

    # Strategy 1: From user's customer ID
    if hasattr(request, 'user') and hasattr(request.user, 'cust_id_id') and request.user.cust_id_id:
        try:
            customer = Customer.objects.filter(cust_id=request.user.cust_id_id).first()
            if customer:
                logger.info(f"[DB_CONFIG] ✓ Found customer from user: {customer.cust_db}")
                return customer
        except Exception as e:
            logger.warning(f"[DB_CONFIG] Failed to get customer from user: {e}")
    else:
        logger.debug("[DB_CONFIG] User has no cust_id_id")

    # Strategy 2: From canvas_id in request data
    canvas_id = None
    if hasattr(request, 'data'):
        canvas_id = request.data.get('canvas_id')
        logger.debug(f"[DB_CONFIG] Checking request.data for canvas_id: {canvas_id}")
    elif hasattr(request, 'GET'):
        canvas_id = request.GET.get('canvas_id')
        logger.debug(f"[DB_CONFIG] Checking request.GET for canvas_id: {canvas_id}")
    elif hasattr(request, 'POST'):
        canvas_id = request.POST.get('canvas_id')
        logger.debug(f"[DB_CONFIG] Checking request.POST for canvas_id: {canvas_id}")

    if canvas_id:
        try:
            canvas = Canvas.objects.get(id=canvas_id)
            customer = canvas.customer
            if customer:
                logger.info(f"[DB_CONFIG] ✓ Found customer from canvas {canvas_id}: {customer.cust_db}")
                return customer
        except Canvas.DoesNotExist:
            logger.warning(f"[DB_CONFIG] Canvas {canvas_id} does not exist")
        except Exception as e:
            logger.warning(f"[DB_CONFIG] Failed to get customer from canvas: {e}")
    else:
        logger.debug("[DB_CONFIG] No canvas_id found in request")

    logger.warning("[DB_CONFIG] ✗ Could not determine customer from request")
    return None

def get_customer_db_config_from_request(request) -> Optional[dict[str, Any]]:
    """
    Get customer database configuration from Django request.

    Args:
        request: Django request object

    Returns:
        Connection config dict or None if customer cannot be determined
    """
    customer = get_customer_from_request(request)
    if customer and hasattr(customer, 'cust_db'):
        return get_customer_db_config(customer.cust_db)
    return None

async def get_customer_db_config_from_request_async(request) -> Optional[dict[str, Any]]:
    """
    Async version: Get customer database configuration from Django request.
    Uses sync_to_async to safely call Django ORM from async context.

    Args:
        request: Django request object

    Returns:
        Connection config dict or None if customer cannot be determined
    """
    from asgiref.sync import sync_to_async

    @sync_to_async
    def _get_config():
        return get_customer_db_config_from_request(request)

    return await _get_config()
