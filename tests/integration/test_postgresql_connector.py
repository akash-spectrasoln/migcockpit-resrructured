"""
Integration tests for the connector layer.

These tests require a real running database.
They are SKIPPED automatically if the database is not available.

Run only unit tests:
    python -m pytest tests/unit/ -v

Run all tests including integration:
    python -m pytest tests/ -v

Run only integration tests:
    python -m pytest tests/integration/ -v
"""
import sys

sys.path.insert(0, '.')
import os


def is_postgres_available(host='localhost', port=5432, user='postgres', password='postgres', db='postgres'):
    try:
        import psycopg2
        conn = psycopg2.connect(host=host, port=port, user=user,
                                password=password, database=db, connect_timeout=2)
        conn.close()
        return True
    except Exception:
        return False


POSTGRES_AVAILABLE = is_postgres_available(
    host=os.environ.get('TEST_PG_HOST', 'localhost'),
    port=int(os.environ.get('TEST_PG_PORT', '5432')),
    user=os.environ.get('TEST_PG_USER', 'postgres'),
    password=os.environ.get('TEST_PG_PASSWORD', 'postgres'),
    db=os.environ.get('TEST_PG_DB', 'postgres'),
)

SKIP_DB = not POSTGRES_AVAILABLE
SKIP_REASON = "No PostgreSQL available. Set TEST_PG_HOST/PORT/USER/PASSWORD/DB env vars."


# ── PostgreSQL Connector Integration ─────────────────────────────────────────

def test_postgresql_connector_test_connection_success():
    if SKIP_DB:
        print(f"SKIP: {SKIP_REASON}")
        return

    from domain.connection.credential import Credential
    from services.extraction_service.connectors.postgresql import PostgreSQLConnector

    cred = Credential(
        host=os.environ.get('TEST_PG_HOST', 'localhost'),
        port=int(os.environ.get('TEST_PG_PORT', '5432')),
        username=os.environ.get('TEST_PG_USER', 'postgres'),
        password=os.environ.get('TEST_PG_PASSWORD', 'postgres'),
        database=os.environ.get('TEST_PG_DB', 'postgres'),
        db_type='postgresql',
    )
    connector = PostgreSQLConnector()
    result = connector.test_connection(cred)
    assert result.get('success') is True, f"Connection failed: {result.get('message')}"


def test_postgresql_connector_fetch_tables():
    if SKIP_DB:
        print(f"SKIP: {SKIP_REASON}")
        return

    from domain.connection.credential import Credential
    from services.extraction_service.connectors.postgresql import PostgreSQLConnector

    cred = Credential(
        host=os.environ.get('TEST_PG_HOST', 'localhost'),
        port=int(os.environ.get('TEST_PG_PORT', '5432')),
        username=os.environ.get('TEST_PG_USER', 'postgres'),
        password=os.environ.get('TEST_PG_PASSWORD', 'postgres'),
        database=os.environ.get('TEST_PG_DB', 'postgres'),
        db_type='postgresql',
    )
    connector = PostgreSQLConnector()
    tables = connector.fetch_tables(cred, 'public')
    assert isinstance(tables, list)


def test_postgresql_connector_execute_simple_query():
    if SKIP_DB:
        print(f"SKIP: {SKIP_REASON}")
        return

    from domain.connection.credential import Credential
    from services.extraction_service.connectors.postgresql import PostgreSQLConnector

    cred = Credential(
        host=os.environ.get('TEST_PG_HOST', 'localhost'),
        port=int(os.environ.get('TEST_PG_PORT', '5432')),
        username=os.environ.get('TEST_PG_USER', 'postgres'),
        password=os.environ.get('TEST_PG_PASSWORD', 'postgres'),
        database=os.environ.get('TEST_PG_DB', 'postgres'),
        db_type='postgresql',
    )
    connector = PostgreSQLConnector()
    result = connector.execute_query(cred, 'SELECT 1 AS test_col', [])
    assert result.get('row_count', 0) >= 1
    assert 'columns' in result
    assert 'rows' in result


def test_postgresql_connector_bad_credentials_fails_gracefully():
    if SKIP_DB:
        print(f"SKIP: {SKIP_REASON}")
        return

    from domain.connection.credential import Credential
    from services.extraction_service.connectors.postgresql import PostgreSQLConnector

    cred = Credential(
        host='localhost',
        port=int(os.environ.get('TEST_PG_PORT', '5432')),
        username='wrong_user',
        password='wrong_password',
        database='nonexistent_db',
        db_type='postgresql',
    )
    connector = PostgreSQLConnector()
    result = connector.test_connection(cred)
    assert result.get('success') is False
    assert result.get('message') is not None
