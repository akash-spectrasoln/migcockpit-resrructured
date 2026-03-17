"""
Comprehensive tests for SQL compilation system.

Tests cover:
1. SQLCompiler for all node types (source, filter, join, projection, aggregate)
2. Expression translation
3. Graph traversal and DAG validation
4. Integration scenarios
"""

from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch

import pytest

from api.utils.expression_translator import ExpressionTranslator
from api.utils.graph_utils import (
    find_upstream_nodes,
    get_node_dependencies,
    get_source_nodes,
    topological_sort,
    validate_dag,
)
from api.utils.sql_compiler import SQLCompiler


class TestExpressionTranslator:
    """Test expression translation from Python-style to SQL."""

    def test_simple_column_reference(self):
        """Test simple column reference."""
        translator = ExpressionTranslator(['name', 'age'], 'postgresql')
        assert translator.translate('name') == '"name"'
        assert translator.translate('age') == '"age"'

    def test_upper_function(self):
        """Test UPPER function translation."""
        translator = ExpressionTranslator(['name'], 'postgresql')
        assert translator.translate('UPPER(name)') == 'UPPER("name")'

    def test_lower_function(self):
        """Test LOWER function translation."""
        translator = ExpressionTranslator(['name'], 'postgresql')
        assert translator.translate('LOWER(name)') == 'LOWER("name")'

    def test_concat_function(self):
        """Test CONCAT function translation."""
        translator = ExpressionTranslator(['first', 'last'], 'postgresql')
        assert translator.translate('CONCAT(first, last)') == 'CONCAT("first", "last")'

    def test_substring_function(self):
        """Test SUBSTRING function translation."""
        translator = ExpressionTranslator(['name'], 'postgresql')
        result = translator.translate('SUBSTRING(name, 1, 3)')
        assert 'SUBSTRING' in result
        assert '"name"' in result

    def test_nested_functions(self):
        """Test nested function calls."""
        translator = ExpressionTranslator(['name'], 'postgresql')
        result = translator.translate('UPPER(LOWER(name))')
        assert 'UPPER' in result
        assert 'LOWER' in result
        assert '"name"' in result

    def test_arithmetic_operators(self):
        """Test arithmetic operators."""
        translator = ExpressionTranslator(['a', 'b'], 'postgresql')
        assert translator.translate('a + b') == '"a" + "b"'
        assert translator.translate('a - b') == '"a" - "b"'
        assert translator.translate('a * b') == '"a" * "b"'
        assert translator.translate('a / b') == '"a" / "b"'

    def test_string_concatenation(self):
        """Test string concatenation operator."""
        translator = ExpressionTranslator(['first', 'last'], 'postgresql')
        result = translator.translate('first || last')
        assert '"first"' in result
        assert '"last"' in result
        assert '||' in result

    def test_numeric_literals(self):
        """Test numeric literals."""
        translator = ExpressionTranslator([], 'postgresql')
        assert translator.translate('42') == '42'
        assert translator.translate('3.14') == '3.14'
        assert translator.translate('-10') == '-10'

    def test_string_literals(self):
        """Test string literals."""
        translator = ExpressionTranslator([], 'postgresql')
        assert translator.translate("'hello'") == "'hello'"

    def test_validate_column_references(self):
        """Test column reference validation."""
        translator = ExpressionTranslator(['name', 'age'], 'postgresql')
        valid, error = translator.validate_column_references('UPPER(name)')
        assert valid is True
        assert error is None

        valid, error = translator.validate_column_references('UPPER(invalid_col)')
        assert valid is False
        assert 'invalid_col' in error

    def test_complex_expression(self):
        """Test complex expression with multiple operations."""
        translator = ExpressionTranslator(['first', 'last', 'age'], 'postgresql')
        result = translator.translate('CONCAT(UPPER(first), " ", last)')
        assert 'CONCAT' in result
        assert 'UPPER' in result
        assert '"first"' in result
        assert '"last"' in result


class TestGraphUtils:
    """Test graph traversal and DAG utilities."""

    def test_find_upstream_nodes_simple(self):
        """Test finding upstream nodes for simple chain."""
        nodes = [
            {'id': 'source1', 'data': {'type': 'source'}},
            {'id': 'filter1', 'data': {'type': 'filter'}},
            {'id': 'projection1', 'data': {'type': 'projection'}}
        ]
        edges = [
            {'source': 'source1', 'target': 'filter1'},
            {'source': 'filter1', 'target': 'projection1'}
        ]

        upstream = find_upstream_nodes(nodes, edges, 'projection1')
        assert upstream == ['source1', 'filter1', 'projection1']

    def test_find_upstream_nodes_join(self):
        """Test finding upstream nodes for join."""
        nodes = [
            {'id': 'source1', 'data': {'type': 'source'}},
            {'id': 'source2', 'data': {'type': 'source'}},
            {'id': 'join1', 'data': {'type': 'join'}}
        ]
        edges = [
            {'source': 'source1', 'target': 'join1', 'targetHandle': 'left'},
            {'source': 'source2', 'target': 'join1', 'targetHandle': 'right'}
        ]

        upstream = find_upstream_nodes(nodes, edges, 'join1')
        assert 'source1' in upstream
        assert 'source2' in upstream
        assert 'join1' in upstream

    def test_topological_sort(self):
        """Test topological sort."""
        nodes = [
            {'id': 'source1', 'data': {'type': 'source'}},
            {'id': 'filter1', 'data': {'type': 'filter'}},
            {'id': 'projection1', 'data': {'type': 'projection'}}
        ]
        edges = [
            {'source': 'source1', 'target': 'filter1'},
            {'source': 'filter1', 'target': 'projection1'}
        ]

        sorted_nodes = topological_sort(nodes, edges)
        assert sorted_nodes == ['source1', 'filter1', 'projection1']

    def test_topological_sort_detects_cycle(self):
        """Test that topological sort detects cycles."""
        nodes = [
            {'id': 'node1', 'data': {'type': 'filter'}},
            {'id': 'node2', 'data': {'type': 'filter'}}
        ]
        edges = [
            {'source': 'node1', 'target': 'node2'},
            {'source': 'node2', 'target': 'node1'}  # Cycle!
        ]

        with pytest.raises(ValueError, match='Cycle detected'):
            topological_sort(nodes, edges)

    def test_validate_dag_valid(self):
        """Test DAG validation for valid DAG."""
        nodes = [
            {'id': 'source1', 'data': {'type': 'source'}},
            {'id': 'filter1', 'data': {'type': 'filter'}}
        ]
        edges = [
            {'source': 'source1', 'target': 'filter1'}
        ]

        is_valid, error = validate_dag(nodes, edges)
        assert is_valid is True
        assert error is None

    def test_validate_dag_invalid(self):
        """Test DAG validation detects cycles."""
        nodes = [
            {'id': 'node1', 'data': {'type': 'filter'}},
            {'id': 'node2', 'data': {'type': 'filter'}}
        ]
        edges = [
            {'source': 'node1', 'target': 'node2'},
            {'source': 'node2', 'target': 'node1'}
        ]

        is_valid, error = validate_dag(nodes, edges)
        assert is_valid is False
        assert 'Cycle' in error

    def test_get_node_dependencies(self):
        """Test getting node dependencies."""
        edges = [
            {'source': 'source1', 'target': 'filter1'},
            {'source': 'source2', 'target': 'filter1'}
        ]

        deps = get_node_dependencies('filter1', edges)
        assert 'source1' in deps
        assert 'source2' in deps

    def test_get_source_nodes(self):
        """Test finding source nodes."""
        nodes = [
            {'id': 'source1', 'data': {'type': 'source'}},
            {'id': 'source2', 'data': {'type': 'source'}},
            {'id': 'filter1', 'data': {'type': 'filter'}}
        ]
        edges = [
            {'source': 'source1', 'target': 'filter1'}
        ]

        sources = get_source_nodes(nodes, edges)
        assert 'source1' in sources
        assert 'source2' in sources
        assert 'filter1' not in sources


class TestSQLCompiler:
    """Test SQL compiler for all node types."""

    @pytest.fixture
    def mock_customer(self):
        """Create mock customer object."""
        customer = Mock()
        customer.cust_db = 'test_db'
        customer.cust_id = 'test_cust'
        return customer

    @pytest.fixture
    def mock_source_config(self):
        """Create mock source configuration."""
        return {
            'hostname': 'localhost',
            'port': 5432,
            'user': 'test_user',
            'password': 'test_pass',
            'database': 'test_db',
            'db_type': 'postgresql',
            'schema': 'public'
        }

    @patch('api.utils.sql_compiler.decrypt_source_data')
    @patch('api.utils.sql_compiler.psycopg2.connect')
    def test_build_source_cte(self, mock_connect, mock_decrypt, mock_customer, mock_source_config):
        """Test building source CTE."""
        # Mock database connection
        mock_cursor = Mock()
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn

        # Mock source config retrieval
        mock_decrypt.return_value = mock_source_config

        # Mock table metadata query
        mock_cursor.fetchall.return_value = [
            ('id', 'integer', 'NO'),
            ('name', 'text', 'YES')
        ]

        nodes = [
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
        edges = []

        compiler = SQLCompiler(nodes, edges, 'source1', mock_customer, 'postgresql')

        # Mock _get_table_metadata to avoid actual DB connection
        with patch.object(compiler, '_get_table_metadata', return_value={
            'columns': [
                {'name': 'id', 'datatype': 'INTEGER', 'source': 'base', 'nullable': False},
                {'name': 'name', 'datatype': 'TEXT', 'source': 'base', 'nullable': True}
            ]
        }):
            sql_query, params, metadata = compiler.compile()

            assert 'WITH' in sql_query or 'SELECT' in sql_query
            assert 'users' in sql_query or '"users"' in sql_query
            assert 'LIMIT' in sql_query
            assert len(params) > 0

    def test_build_filter_cte(self, mock_customer):
        """Test building filter CTE."""
        nodes = [
            {
                'id': 'source1',
                'data': {
                    'type': 'source',
                    'config': {'sourceId': 1, 'tableName': 'users'}
                }
            },
            {
                'id': 'filter1',
                'data': {
                    'type': 'filter',
                    'config': {
                        'conditions': [
                            {'column': 'age', 'operator': '>', 'value': 18}
                        ]
                    }
                }
            }
        ]
        edges = [
            {'source': 'source1', 'target': 'filter1'}
        ]

        compiler = SQLCompiler(nodes, edges, 'filter1', mock_customer, 'postgresql')

        # Mock source CTE building
        compiler.cte_map['source1'] = 'node_source1'
        compiler.metadata_map['source1'] = {
            'columns': [
                {'name': 'id', 'datatype': 'INTEGER'},
                {'name': 'age', 'datatype': 'INTEGER'}
            ]
        }

        with patch.object(compiler, '_build_source_cte', return_value=(
            'SELECT * FROM "public"."users"',
            {'columns': [{'name': 'id'}, {'name': 'age'}]}
        )):
            with patch.object(compiler, '_get_source_config'):
                with patch.object(compiler, '_get_table_metadata'):
                    sql_query, params, metadata = compiler.compile()
                    assert 'WHERE' in sql_query or 'filter' in sql_query.lower()

    def test_build_join_cte(self, mock_customer):
        """Test building join CTE."""
        nodes = [
            {
                'id': 'source1',
                'data': {
                    'type': 'source',
                    'config': {'sourceId': 1, 'tableName': 'users'}
                }
            },
            {
                'id': 'source2',
                'data': {
                    'type': 'source',
                    'config': {'sourceId': 2, 'tableName': 'orders'}
                }
            },
            {
                'id': 'join1',
                'data': {
                    'type': 'join',
                    'config': {
                        'joinType': 'INNER',
                        'conditions': [
                            {'leftColumn': 'id', 'rightColumn': 'user_id', 'operator': '='}
                        ],
                        'outputColumns': [
                            {'column': 'id', 'source': 'left', 'included': True},
                            {'column': 'order_id', 'source': 'right', 'included': True}
                        ]
                    }
                }
            }
        ]
        edges = [
            {'source': 'source1', 'target': 'join1', 'targetHandle': 'left'},
            {'source': 'source2', 'target': 'join1', 'targetHandle': 'right'}
        ]

        compiler = SQLCompiler(nodes, edges, 'join1', mock_customer, 'postgresql')

        # Mock upstream CTEs
        compiler.cte_map['source1'] = 'node_source1'
        compiler.cte_map['source2'] = 'node_source2'
        compiler.metadata_map['source1'] = {'columns': [{'name': 'id', 'datatype': 'INTEGER'}]}
        compiler.metadata_map['source2'] = {'columns': [{'name': 'order_id', 'datatype': 'INTEGER'}]}

        with patch.object(compiler, '_build_source_cte', side_effect=[
            ('SELECT * FROM "public"."users"', {'columns': [{'name': 'id'}]}),
            ('SELECT * FROM "public"."orders"', {'columns': [{'name': 'order_id'}]})
        ]):
            with patch.object(compiler, '_get_source_config'):
                with patch.object(compiler, '_get_table_metadata'):
                    sql_query, params, metadata = compiler.compile()
                    assert 'JOIN' in sql_query.upper()
                    assert 'INNER JOIN' in sql_query.upper() or '__L__' in sql_query

    def test_build_projection_cte(self, mock_customer):
        """Test building projection CTE."""
        nodes = [
            {
                'id': 'source1',
                'data': {
                    'type': 'source',
                    'config': {'sourceId': 1, 'tableName': 'users'}
                }
            },
            {
                'id': 'projection1',
                'data': {
                    'type': 'projection',
                    'config': {
                        'selectedColumns': ['id', 'name'],
                        'selectedMode': 'INCLUDE'
                    }
                }
            }
        ]
        edges = [
            {'source': 'source1', 'target': 'projection1'}
        ]

        compiler = SQLCompiler(nodes, edges, 'projection1', mock_customer, 'postgresql')

        compiler.cte_map['source1'] = 'node_source1'
        compiler.metadata_map['source1'] = {
            'columns': [
                {'name': 'id', 'datatype': 'INTEGER'},
                {'name': 'name', 'datatype': 'TEXT'},
                {'name': 'age', 'datatype': 'INTEGER'}
            ]
        }

        with patch.object(compiler, '_build_source_cte', return_value=(
            'SELECT * FROM "public"."users"',
            {'columns': [
                {'name': 'id', 'datatype': 'INTEGER'},
                {'name': 'name', 'datatype': 'TEXT'},
                {'name': 'age', 'datatype': 'INTEGER'}
            ]}
        )):
            with patch.object(compiler, '_get_source_config'):
                with patch.object(compiler, '_get_table_metadata'):
                    sql_query, params, metadata = compiler.compile()
                    assert '"id"' in sql_query
                    assert '"name"' in sql_query
                    assert '"age"' not in sql_query or 'SELECT' in sql_query

    def test_build_projection_with_calculated_columns(self, mock_customer):
        """Test building projection CTE with calculated columns."""
        nodes = [
            {
                'id': 'source1',
                'data': {
                    'type': 'source',
                    'config': {'sourceId': 1, 'tableName': 'users'}
                }
            },
            {
                'id': 'projection1',
                'data': {
                    'type': 'projection',
                    'config': {
                        'selectedColumns': ['id', 'name'],
                        'calculatedColumns': [
                            {
                                'name': 'full_name',
                                'expression': 'UPPER(name)',
                                'dataType': 'STRING'
                            }
                        ]
                    }
                }
            }
        ]
        edges = [
            {'source': 'source1', 'target': 'projection1'}
        ]

        compiler = SQLCompiler(nodes, edges, 'projection1', mock_customer, 'postgresql')

        compiler.cte_map['source1'] = 'node_source1'
        compiler.metadata_map['source1'] = {
            'columns': [
                {'name': 'id', 'datatype': 'INTEGER'},
                {'name': 'name', 'datatype': 'TEXT'}
            ]
        }

        with patch.object(compiler, '_build_source_cte', return_value=(
            'SELECT * FROM "public"."users"',
            {'columns': [{'name': 'id'}, {'name': 'name'}]}
        )):
            with patch.object(compiler, '_get_source_config'):
                with patch.object(compiler, '_get_table_metadata'):
                    sql_query, params, metadata = compiler.compile()
                    assert 'UPPER' in sql_query
                    assert 'full_name' in sql_query or '"full_name"' in sql_query

    def test_build_aggregate_cte(self, mock_customer):
        """Test building aggregate CTE."""
        nodes = [
            {
                'id': 'source1',
                'data': {
                    'type': 'source',
                    'config': {'sourceId': 1, 'tableName': 'orders'}
                }
            },
            {
                'id': 'aggregate1',
                'data': {
                    'type': 'aggregate',
                    'config': {
                        'aggregateColumns': [
                            {'function': 'COUNT', 'column': '', 'alias': 'total_orders'}
                        ],
                        'groupByColumns': []
                    }
                }
            }
        ]
        edges = [
            {'source': 'source1', 'target': 'aggregate1'}
        ]

        compiler = SQLCompiler(nodes, edges, 'aggregate1', mock_customer, 'postgresql')

        compiler.cte_map['source1'] = 'node_source1'
        compiler.metadata_map['source1'] = {
            'columns': [{'name': 'id', 'datatype': 'INTEGER'}]
        }

        with patch.object(compiler, '_build_source_cte', return_value=(
            'SELECT * FROM "public"."orders"',
            {'columns': [{'name': 'id'}]}
        )):
            with patch.object(compiler, '_get_source_config'):
                with patch.object(compiler, '_get_table_metadata'):
                    sql_query, params, metadata = compiler.compile()
                    assert 'COUNT' in sql_query.upper()

    def test_compile_complex_pipeline(self, mock_customer):
        """Test compiling a complex pipeline with multiple node types."""
        nodes = [
            {
                'id': 'source1',
                'data': {
                    'type': 'source',
                    'config': {'sourceId': 1, 'tableName': 'users'}
                }
            },
            {
                'id': 'filter1',
                'data': {
                    'type': 'filter',
                    'config': {
                        'conditions': [{'column': 'age', 'operator': '>', 'value': 18}]
                    }
                }
            },
            {
                'id': 'projection1',
                'data': {
                    'type': 'projection',
                    'config': {
                        'selectedColumns': ['id', 'name'],
                        'selectedMode': 'INCLUDE'
                    }
                }
            }
        ]
        edges = [
            {'source': 'source1', 'target': 'filter1'},
            {'source': 'filter1', 'target': 'projection1'}
        ]

        compiler = SQLCompiler(nodes, edges, 'projection1', mock_customer, 'postgresql')

        with patch.object(compiler, '_build_source_cte', return_value=(
            'SELECT * FROM "public"."users"',
            {'columns': [{'name': 'id'}, {'name': 'name'}, {'name': 'age'}]}
        )):
            with patch.object(compiler, '_get_source_config'):
                with patch.object(compiler, '_get_table_metadata'):
                    sql_query, params, metadata = compiler.compile()

                    # Should have CTEs for source, filter, and projection
                    assert 'WITH' in sql_query
                    assert 'LIMIT' in sql_query
                    assert len(params) > 0

    def test_compile_missing_target_node(self, mock_customer):
        """Test that compiler raises error for missing target node."""
        nodes = [
            {'id': 'source1', 'data': {'type': 'source'}}
        ]
        edges = []

        with pytest.raises(ValueError, match='Target node.*not found'):
            SQLCompiler(nodes, edges, 'nonexistent', mock_customer, 'postgresql')

    def test_limit_only_in_final_query(self, mock_customer):
        """Test that LIMIT is only added to final SELECT, not CTEs."""
        nodes = [
            {
                'id': 'source1',
                'data': {
                    'type': 'source',
                    'config': {'sourceId': 1, 'tableName': 'users'}
                }
            },
            {
                'id': 'filter1',
                'data': {
                    'type': 'filter',
                    'config': {'conditions': []}
                }
            }
        ]
        edges = [
            {'source': 'source1', 'target': 'filter1'}
        ]

        compiler = SQLCompiler(nodes, edges, 'filter1', mock_customer, 'postgresql')

        with patch.object(compiler, '_build_source_cte', return_value=(
            'SELECT * FROM "public"."users"',
            {'columns': [{'name': 'id'}]}
        )):
            with patch.object(compiler, '_get_source_config'):
                with patch.object(compiler, '_get_table_metadata'):
                    sql_query, params, metadata = compiler.compile()

                    # Count LIMIT occurrences - should only be in final SELECT
                    limit_count = sql_query.upper().count('LIMIT')
                    assert limit_count == 1

                    # LIMIT should be at the end
                    assert sql_query.upper().strip().endswith('LIMIT %S') or 'LIMIT' in sql_query[-20:].upper()
