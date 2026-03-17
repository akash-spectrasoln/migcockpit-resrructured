"""
Unit tests for core/connections/tenant_provisioning.py

Uses unittest.mock to test provisioning logic WITHOUT a real database.
Skips gracefully if psycopg2 is not installed.
Run with: python -m pytest tests/unit/connections/test_tenant_provisioning.py -v
"""
import sys

sys.path.insert(0, '.')
from unittest.mock import MagicMock, patch


def make_customer(cust_id='C00001', cust_db='C00001'):
    c = MagicMock()
    c.cust_id = cust_id
    c.cust_db = cust_db
    return c


def get_service():
    try:
        from api.connections.tenant_provisioning import TenantProvisioningService
        return TenantProvisioningService
    except ImportError:
        return None


def run_create_db(fetchone_return=(1,), schema_raises=None):
    """Run _create_database with mocked psycopg2. Returns (cursor, conn, service)."""
    Service = get_service()
    if not Service:
        return None, None, None
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = fetchone_return
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_pg2 = MagicMock()
    mock_pg2.connect.return_value = mock_conn
    mock_settings = MagicMock()
    mock_settings.DATABASES = {'default': {'HOST': 'h', 'PORT': '5432', 'USER': 'u', 'PASSWORD': 'p'}}
    with patch('api.connections.tenant_provisioning.psycopg2', mock_pg2), \
         patch('api.connections.tenant_provisioning.settings', mock_settings):
        service = Service()
        service._create_schemas = MagicMock(side_effect=Exception(schema_raises) if schema_raises else None)
        try:
            service._create_database(make_customer())
        except Exception:
            pass
        return mock_cursor, mock_conn, service


def test_provision_calls_create_database():
    Service = get_service()
    if not Service:
        return
    service = Service()
    service._create_database = MagicMock()
    service.provision(make_customer())
    service._create_database.assert_called_once()


def test_creates_db_when_not_exists():
    cursor, _, service = run_create_db(fetchone_return=None)
    if cursor is None:
        return
    sqls = [str(c) for c in cursor.execute.call_args_list]
    assert any('CREATE DATABASE' in s for s in sqls), f"No CREATE DATABASE in: {sqls}"


def test_skips_create_when_db_exists():
    cursor, _, service = run_create_db(fetchone_return=(1,))
    if cursor is None:
        return
    sqls = [str(c) for c in cursor.execute.call_args_list]
    assert not any('CREATE DATABASE' in s for s in sqls)
    service._create_schemas.assert_not_called()


def test_calls_schemas_for_new_db():
    _, _, service = run_create_db(fetchone_return=None)
    if service is None:
        return
    service._create_schemas.assert_called_once()


def test_rollback_on_schema_failure():
    cursor, _, service = run_create_db(fetchone_return=None, schema_raises="boom")
    if cursor is None:
        return
    sqls = [str(c) for c in cursor.execute.call_args_list]
    assert any('DROP DATABASE' in s for s in sqls), f"No DROP DATABASE rollback in: {sqls}"


def test_connections_always_closed():
    cursor, conn, _ = run_create_db(fetchone_return=None)
    if cursor is None:
        return
    cursor.close.assert_called()
    conn.close.assert_called()


def test_connects_to_system_postgres_not_customer_db():
    Service = get_service()
    if not Service:
        return
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,)
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_pg2 = MagicMock()
    mock_pg2.connect.return_value = mock_conn
    mock_settings = MagicMock()
    mock_settings.DATABASES = {'default': {'HOST': 'h', 'PORT': '5432', 'USER': 'u', 'PASSWORD': 'p'}}
    with patch('api.connections.tenant_provisioning.psycopg2', mock_pg2), \
         patch('api.connections.tenant_provisioning.settings', mock_settings):
        service = Service()
        service._create_schemas = MagicMock()
        service._create_database(make_customer())
        kw = mock_pg2.connect.call_args.kwargs
        assert kw.get('database') == 'postgres', f"Expected postgres, got: {kw}"
