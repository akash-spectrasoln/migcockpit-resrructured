"""
Shared pytest fixtures for SQL compilation tests.
"""

from unittest.mock import MagicMock, Mock

import pytest


@pytest.fixture
def mock_customer():
    """Create a mock customer object."""
    customer = Mock()
    customer.cust_db = 'test_db'
    customer.cust_id = 'test_cust_123'
    return customer


@pytest.fixture
def mock_source_config_postgresql():
    """Create mock PostgreSQL source configuration."""
    return {
        'hostname': 'localhost',
        'port': 5432,
        'user': 'test_user',
        'password': 'test_pass',
        'database': 'test_db',
        'db_type': 'postgresql',
        'schema': 'public'
    }


@pytest.fixture
def mock_source_config_sqlserver():
    """Create mock SQL Server source configuration."""
    return {
        'hostname': 'localhost',
        'port': 1433,
        'user': 'test_user',
        'password': 'test_pass',
        'database': 'test_db',
        'db_type': 'sqlserver',
        'schema': 'dbo'
    }


@pytest.fixture
def sample_nodes_simple():
    """Sample nodes for simple pipeline."""
    return [
        {
            'id': 'source1',
            'data': {
                'type': 'source',
                'config': {
                    'sourceId': 1,
                    'tableName': 'users',
                    'schema': 'public'
                }
            }
        }
    ]


@pytest.fixture
def sample_edges_simple():
    """Sample edges for simple pipeline."""
    return []


@pytest.fixture
def sample_nodes_with_filter():
    """Sample nodes for pipeline with filter."""
    return [
        {
            'id': 'source1',
            'data': {
                'type': 'source',
                'config': {
                    'sourceId': 1,
                    'tableName': 'users',
                    'schema': 'public'
                }
            }
        },
        {
            'id': 'filter1',
            'data': {
                'type': 'filter',
                'config': {
                    'conditions': [
                        {
                            'column': 'age',
                            'operator': '>',
                            'value': 18
                        }
                    ]
                }
            }
        }
    ]


@pytest.fixture
def sample_edges_with_filter():
    """Sample edges for pipeline with filter."""
    return [
        {'source': 'source1', 'target': 'filter1'}
    ]


@pytest.fixture
def sample_nodes_with_join():
    """Sample nodes for pipeline with join."""
    return [
        {
            'id': 'source1',
            'data': {
                'type': 'source',
                'config': {
                    'sourceId': 1,
                    'tableName': 'users',
                    'schema': 'public'
                }
            }
        },
        {
            'id': 'source2',
            'data': {
                'type': 'source',
                'config': {
                    'sourceId': 2,
                    'tableName': 'orders',
                    'schema': 'public'
                }
            }
        },
        {
            'id': 'join1',
            'data': {
                'type': 'join',
                'config': {
                    'joinType': 'INNER',
                    'conditions': [
                        {
                            'leftColumn': 'id',
                            'rightColumn': 'user_id',
                            'operator': '='
                        }
                    ],
                    'outputColumns': [
                        {
                            'column': 'id',
                            'source': 'left',
                            'included': True,
                            'outputName': 'user_id'
                        },
                        {
                            'column': 'order_id',
                            'source': 'right',
                            'included': True
                        }
                    ]
                }
            }
        }
    ]


@pytest.fixture
def sample_edges_with_join():
    """Sample edges for pipeline with join."""
    return [
        {'source': 'source1', 'target': 'join1', 'targetHandle': 'left'},
        {'source': 'source2', 'target': 'join1', 'targetHandle': 'right'}
    ]


@pytest.fixture
def sample_table_metadata():
    """Sample table metadata."""
    return {
        'columns': [
            {
                'name': 'id',
                'datatype': 'INTEGER',
                'source': 'base',
                'nullable': False
            },
            {
                'name': 'name',
                'datatype': 'TEXT',
                'source': 'base',
                'nullable': True
            },
            {
                'name': 'age',
                'datatype': 'INTEGER',
                'source': 'base',
                'nullable': True
            }
        ]
    }
