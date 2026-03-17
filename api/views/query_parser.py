"""
Miscellaneous API views.
Handles XML query parsing and aggregate validation.
"""
import logging
from typing import Any
import xml.etree.ElementTree as ET

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.authentication import JWTAuthentication

logger = logging.getLogger(__name__)

# =========================================================================================
# AGGREGATE AND XML QUERY PARSING UTILITIES
# =========================================================================================

class DataMigrationQueryParser:
    """Parser for DataMigrationQuery XML format"""

    def __init__(self, xml_content: str):
        """
        Initialize parser with XML content

        Args:
            xml_content: XML string content
        """
        self.xml_content = xml_content
        self.root = ET.fromstring(xml_content)
        self.namespace = {'dmq': 'http://datamigrationquery.org/1.0'} if '{' in self.root.tag else {}

    def parse(self) -> dict[str, Any]:
        """
        Parse the XML and return structured data

        Returns:
            Dictionary containing parsed query configuration
        """
        result = {
            'context_menus': self._parse_context_menus(),
            'source': self._parse_source(),
            'projections': self._parse_projections(),
            'sql_generator': self._parse_sql_generator(),
            'preview': self._parse_preview(),
        }
        return result

    def _parse_context_menus(self) -> dict[str, Any]:
        """Parse context menu definitions"""
        context_menus = {}

        context_menus_elem = self.root.find('.//ContextMenus')
        if context_menus_elem is None:
            return context_menus

        for menu_elem in context_menus_elem.findall('ContextMenu'):
            target = menu_elem.get('target', '')
            menu_items = []

            for item_elem in menu_elem.findall('MenuItem'):
                menu_item = {
                    'id': item_elem.get('id', ''),
                    'label': item_elem.get('label', ''),
                    'action': item_elem.get('action', ''),
                    'function': item_elem.get('function', ''),
                }

                # Check for submenu
                submenu_elem = item_elem.find('SubMenu')
                if submenu_elem is not None:
                    menu_item['submenu'] = []
                    for sub_item in submenu_elem.findall('MenuItem'):
                        menu_item['submenu'].append({
                            'label': sub_item.get('label', ''),
                            'action': sub_item.get('action', ''),
                            'function': sub_item.get('function', ''),
                        })

                menu_items.append(menu_item)

            context_menus[target] = menu_items

        return context_menus

    def _parse_source(self) -> dict[str, Any]:
        """Parse source table definition"""
        source_elem = self.root.find('.//Source')
        if source_elem is None:
            return {}

        table_elem = source_elem.find('Table')
        if table_elem is None:
            return {}

        return {
            'schema': table_elem.get('schema', ''),
            'name': table_elem.get('name', ''),
        }

    def _parse_projections(self) -> list[dict[str, Any]]:
        """Parse projection definitions"""
        projections = []

        for proj_elem in self.root.findall('.//Projection'):
            projection = {
                'id': proj_elem.get('id', ''),
                'rules': self._parse_rules(proj_elem),
                'columns': self._parse_projection_columns(proj_elem),
            }
            projections.append(projection)

        return projections

    def _parse_rules(self, proj_elem: ET.Element) -> list[dict[str, Any]]:
        """Parse projection rules"""
        rules = []

        rules_elem = proj_elem.find('Rules')
        if rules_elem is None:
            return rules

        for rule_elem in rules_elem.findall('Rule'):
            rule = {
                'id': rule_elem.get('id', ''),
                'condition': '',
                'action': '',
            }

            condition_elem = rule_elem.find('Condition')
            if condition_elem is not None:
                rule['condition'] = condition_elem.text or ''

            action_elem = rule_elem.find('Action')
            if action_elem is not None:
                rule['action'] = action_elem.text or ''

            rules.append(rule)

        return rules

    def _parse_projection_columns(self, proj_elem: ET.Element) -> list[dict[str, Any]]:
        """Parse projection columns"""
        columns = []

        columns_elem = proj_elem.find('Columns')
        if columns_elem is None:
            return columns

        for col_elem in columns_elem.findall('Column'):
            column = {
                'id': col_elem.get('id', ''),
                'name': col_elem.get('name', ''),
                'source': col_elem.get('source', ''),
                'group_by': False,
                'aggregate': None,
                'ui': {},
            }

            # Parse GroupBy
            group_by_elem = col_elem.find('GroupBy')
            if group_by_elem is not None:
                column['group_by'] = group_by_elem.text.lower() == 'true'

            # Parse Aggregate
            aggregate_elem = col_elem.find('Aggregate')
            if aggregate_elem is not None:
                column['aggregate'] = {
                    'function': '',
                    'source_column': '',
                }

                func_elem = aggregate_elem.find('Function')
                if func_elem is not None:
                    column['aggregate']['function'] = func_elem.text or ''

                source_col_elem = aggregate_elem.find('SourceColumn')
                if source_col_elem is not None:
                    column['aggregate']['source_column'] = source_col_elem.text or ''

            # Parse UI
            ui_elem = col_elem.find('UI')
            if ui_elem is not None:
                column['ui'] = {
                    'icon': ui_elem.get('icon', ''),
                }

            columns.append(column)

        return columns

    def _parse_sql_generator(self) -> dict[str, Any]:
        """Parse SQL generator configuration"""
        sql_gen_elem = self.root.find('.//SQLGenerator')
        if sql_gen_elem is None:
            return {}

        sql_gen = {
            'dialect': sql_gen_elem.get('dialect', 'POSTGRESQL'),
            'select': [],
            'from': {},
            'group_by': [],
        }

        # Parse Select
        select_elem = sql_gen_elem.find('Select')
        if select_elem is not None:
            for field_elem in select_elem.findall('Field'):
                field_data = {
                    'name': field_elem.get('expression') or field_elem.text or '',
                    'alias': field_elem.get('alias', ''),
                }
                sql_gen['select'].append(field_data)

        # Parse From
        from_elem = sql_gen_elem.find('From')
        if from_elem is not None:
            table_elem = from_elem.find('Table')
            if table_elem is not None:
                sql_gen['from'] = {
                    'table': table_elem.text or '',
                }

        # Parse GroupBy
        group_by_elem = sql_gen_elem.find('GroupBy')
        if group_by_elem is not None:
            for field_elem in group_by_elem.findall('Field'):
                sql_gen['group_by'].append(field_elem.text or '')

        return sql_gen

    def _parse_preview(self) -> dict[str, Any]:
        """Parse preview configuration"""
        preview_elem = self.root.find('.//Preview')
        if preview_elem is None:
            return {}

        preview = {
            'mode': '',
            'message': '',
            'limit': 100,
        }

        mode_elem = preview_elem.find('Mode')
        if mode_elem is not None:
            preview['mode'] = mode_elem.text or ''

        message_elem = preview_elem.find('Message')
        if message_elem is not None:
            preview['message'] = message_elem.text or ''

        limit_elem = preview_elem.find('Limit')
        if limit_elem is not None:
            try:
                preview['limit'] = int(limit_elem.text or '100')
            except ValueError:
                preview['limit'] = 100

        return preview

    def to_node_config(self) -> dict[str, Any]:
        """
        Convert parsed XML to node configuration format

        Returns:
            Dictionary in node configuration format
        """
        parsed = self.parse()

        # Get first projection (assuming single projection for now)
        projection = parsed['projections'][0] if parsed['projections'] else {}

        # Build node config
        config = {
            'source': parsed['source'],
            'projection': {
                'id': projection.get('id', 'projection_1'),
                'rules': projection.get('rules', []),
                'columns': projection.get('columns', []),
            },
            'sql_generator': parsed['sql_generator'],
            'preview': parsed['preview'],
            'context_menus': parsed['context_menus'],
        }

        # Extract aggregate columns and group-by columns
        aggregate_columns = []
        group_by_columns = []

        for col in projection.get('columns', []):
            if col.get('aggregate'):
                aggregate_columns.append({
                    'function': col['aggregate']['function'],
                    'column': col['aggregate']['source_column'],
                    'alias': col['name'],
                })
            elif col.get('group_by'):
                group_by_columns.append(col['name'])

        # Apply auto group-by rule if aggregates exist
        if aggregate_columns and projection.get('rules'):
            for rule in projection['rules']:
                if rule.get('condition') == 'HAS_AGGREGATE' and rule.get('action') == 'NON_AGG_COLUMNS_GROUP_BY':
                    # Auto-add non-aggregate columns to GROUP BY
                    for col in projection.get('columns', []):
                        if not col.get('aggregate') and col.get('name'):
                            if col['name'] not in group_by_columns:
                                group_by_columns.append(col['name'])

        config['aggregateColumns'] = aggregate_columns
        config['groupByColumns'] = group_by_columns

        return config

def apply_auto_group_by_rule(
    projection_config: dict[str, Any],
    columns: list[dict[str, Any]]
) -> list[str]:
    """
    Apply auto group-by rule: when aggregates are present,
    non-aggregated columns automatically become GROUP BY columns
    """
    aggregate_columns = projection_config.get('aggregateColumns', [])

    # If no aggregates, no GROUP BY needed
    if not aggregate_columns:
        return []

    # Check if auto group-by rule is enabled
    rules = projection_config.get('rules', [])
    auto_group_by_enabled = False

    for rule in rules:
        if isinstance(rule, dict):
            condition = rule.get('condition', '')
            action = rule.get('action', '')
            if condition == 'HAS_AGGREGATE' and action == 'NON_AGG_COLUMNS_GROUP_BY':
                auto_group_by_enabled = True
                break

    if not auto_group_by_enabled:
        # Check for explicit groupByColumns
        return projection_config.get('groupByColumns', [])

    # Auto-add non-aggregate columns to GROUP BY
    group_by_columns = []

    for col in columns:
        col_name = col.get('name') if isinstance(col, dict) else str(col)

        # Skip if it's an aggregate column
        is_aggregate = False
        for agg_col in aggregate_columns:
            agg_alias = agg_col.get('alias', '') if isinstance(agg_col, dict) else ''
            if col_name == agg_alias:
                is_aggregate = True
                break

        if not is_aggregate and col_name:
            group_by_columns.append(col_name)

    return group_by_columns

def build_group_by_sql(
    selected_columns: list[str],
    aggregate_columns: list[dict[str, Any]],
    dialect: str = 'POSTGRESQL'
) -> tuple[list[str], list[str]]:
    """
    Build SELECT and GROUP BY clauses for SQL query
    """
    select_fields = []
    group_by_fields = []

    # Identify aggregate column aliases
    aggregate_aliases = {agg.get('alias', '') for agg in aggregate_columns if isinstance(agg, dict)}

    # Build SELECT clause
    for col in selected_columns:
        # Check if this column is an aggregate
        is_aggregate = False
        aggregate_expr = None

        for agg_col in aggregate_columns:
            if isinstance(agg_col, dict):
                alias = agg_col.get('alias', '')
                if col == alias:
                    is_aggregate = True
                    func = agg_col.get('function', '').upper()
                    source_col = agg_col.get('column', '')

                    # Build aggregate expression
                    if func == 'COUNT_DISTINCT':
                        aggregate_expr = f'COUNT(DISTINCT "{source_col}")'
                    elif func in ['SUM', 'AVG', 'MIN', 'MAX', 'COUNT']:
                        if func == 'COUNT' and not source_col:
                            aggregate_expr = "COUNT(*)"
                        else:
                            aggregate_expr = f'{func}("{source_col}")'
                    break

        if is_aggregate and aggregate_expr:
            select_fields.append(f'{aggregate_expr} AS "{col}"')
        else:
            select_fields.append(f'"{col}"')
            # Add to GROUP BY if not aggregate
            if col not in aggregate_aliases:
                group_by_fields.append(f'"{col}"')

    return select_fields, group_by_fields

def validate_aggregate_configuration(
    projection_config: dict[str, Any],
    available_columns: list[str]
) -> tuple[bool, list[str]]:
    """
    Validate aggregate configuration
    """
    errors = []
    aggregate_columns = projection_config.get('aggregateColumns', [])

    for agg_col in aggregate_columns:
        if not isinstance(agg_col, dict):
            errors.append(f"Invalid aggregate column definition: {agg_col}")
            continue

        function = agg_col.get('function', '').upper()
        column = agg_col.get('column', '')
        alias = agg_col.get('alias', '')

        # Validate function
        valid_functions = ['SUM', 'AVG', 'MIN', 'MAX', 'COUNT', 'COUNT_DISTINCT']
        if function not in valid_functions:
            errors.append(f"Invalid aggregate function: {function}. Must be one of {valid_functions}")

        # Validate column (for COUNT, column can be empty)
        if function != 'COUNT' and not column:
            errors.append(f"Aggregate function {function} requires a source column")

        if column and column not in available_columns:
            errors.append(f"Source column '{column}' not found in available columns")

        # Validate alias
        if not alias:
            errors.append("Aggregate column must have an alias")

    return len(errors) == 0, errors

class AggregateXMLImportView(APIView):
    """
    Import DataMigrationQuery XML format and convert to node configuration
    """
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """
        Parse XML query and return node configuration
        """
        try:
            xml_content = request.data.get('xml_content', '')

            if not xml_content:
                return Response(
                    {"error": "xml_content is required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Parse XML
            parser = DataMigrationQueryParser(xml_content)
            parsed_data = parser.parse()
            node_config = parser.to_node_config()

            logger.info(f"XML Query parsed successfully: {parsed_data.get('source', {}).get('name', 'unknown')}")

            return Response({
                "success": True,
                "config": node_config,
                "context_menus": parsed_data.get('context_menus', {}),
                "source": parsed_data.get('source', {}),
                "projection": parsed_data.get('projections', [{}])[0] if parsed_data.get('projections') else {},
                "sql_generator": parsed_data.get('sql_generator', {}),
                "preview": parsed_data.get('preview', {}),
            }, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error parsing XML query: {e}", exc_info=True)
            return Response(
                {"error": f"Failed to parse XML query: {e!s}"},
                status=status.HTTP_400_BAD_REQUEST
            )

class AggregateXMLValidateView(APIView):
    """
    Validate XML query configuration
    """
    authentication_classes = [JWTAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """
        Validate XML query configuration
        """
        try:
            xml_content = request.data.get('xml_content', '')
            available_columns = request.data.get('available_columns', [])

            if not xml_content:
                return Response(
                    {"error": "xml_content is required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Parse XML
            parser = DataMigrationQueryParser(xml_content)
            parsed_data = parser.parse()
            node_config = parser.to_node_config()

            errors = []
            warnings = []

            # Validate source
            source = parsed_data.get('source', {})
            if not source.get('name'):
                errors.append("Source table name is required")

            # Validate projection
            projection = parsed_data.get('projections', [{}])[0] if parsed_data.get('projections') else {}
            if not projection.get('columns'):
                errors.append("Projection must have at least one column")

            # Validate aggregates if available columns provided
            aggregate_columns = node_config.get('aggregateColumns', [])
            if aggregate_columns and available_columns:
                is_valid, agg_errors = validate_aggregate_configuration(
                    node_config,
                    available_columns
                )
                if not is_valid:
                    errors.extend(agg_errors)

            # Check for auto group-by rule
            rules = projection.get('rules', [])
            has_auto_group_by = any(
                r.get('condition') == 'HAS_AGGREGATE' and
                r.get('action') == 'NON_AGG_COLUMNS_GROUP_BY'
                for r in rules
            )

            if aggregate_columns and not has_auto_group_by:
                warnings.append("Aggregates present but auto group-by rule not enabled. Non-aggregate columns may need explicit GROUP BY.")

            return Response({
                "valid": len(errors) == 0,
                "errors": errors,
                "warnings": warnings,
            }, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error validating XML query: {e}", exc_info=True)
            return Response(
                {"error": f"Failed to validate XML query: {e!s}"},
                status=status.HTTP_400_BAD_REQUEST
            )
