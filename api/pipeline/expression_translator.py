# Moved from: api/utils/expression_translator.py
"""
Expression translator for converting calculated column expressions to SQL.
Translates Python-style function calls to SQL expressions.
"""
import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

class ExpressionTranslator:
    """
    Translates calculated column expressions from Python-style to SQL.
    """

    # Function mappings: Python function -> SQL function
    FUNCTION_MAP = {
        'UPPER': 'UPPER',
        'LOWER': 'LOWER',
        'SUBSTRING': 'SUBSTRING',
        'SUBSTR': 'SUBSTRING',
        'CONCAT': 'CONCAT',
        'LENGTH': 'LENGTH',
        'TRIM': 'TRIM',
        'LTRIM': 'LTRIM',
        'RTRIM': 'RTRIM',
        'REPLACE': 'REPLACE',
        'ABS': 'ABS',
        'ROUND': 'ROUND',
        'FLOOR': 'FLOOR',
        'CEIL': 'CEIL',
        'CEILING': 'CEIL',
        'SQRT': 'SQRT',
        'POWER': 'POWER',
        'EXP': 'EXP',
        'LN': 'LN',
        'LOG': 'LOG',
        'MOD': 'MOD',
        'NOW': 'NOW',
        'CURRENT_DATE': 'CURRENT_DATE',
        'CURRENT_TIMESTAMP': 'CURRENT_TIMESTAMP',
        'EXTRACT': 'EXTRACT',
        'DATE_TRUNC': 'DATE_TRUNC',
        'COALESCE': 'COALESCE',
        'NULLIF': 'NULLIF',
        'CASE': 'CASE',
    }

    def __init__(self, available_columns: list[str], db_type: str = 'postgresql', column_metadata_map: Optional[dict[str, dict[str, Any]]] = None):
        """
        Initialize translator with available columns and database type.

        Args:
            available_columns: List of column names available in the current context
            db_type: Database type ('postgresql', 'mysql', 'sqlserver', 'oracle')
            column_metadata_map: Optional dict mapping column names to their metadata (including datatype)
        """
        self.available_columns = available_columns
        self.db_type = db_type.lower()
        self.column_metadata_map = column_metadata_map or {}

    def _resolve_column_name(self, col_name: str) -> Optional[str]:
        """
        Resolve a column reference to the actual column name in available_columns.
        Handles join output where short names (e.g. is_trial) may be prefixed
        as _L_is_trial, _R_is_trial, or source_id_is_trial.
        """
        if col_name in self.available_columns:
            return col_name
        # Join conflict resolution: try _L_ and _R_ prefixes
        for prefix in ('_L_', '_R_'):
            resolved = f"{prefix}{col_name}"
            if resolved in self.available_columns:
                return resolved
        # Source-prefixed columns (e.g. 702d34ed_is_trial)
        for ac in self.available_columns:
            if ac.endswith('_' + col_name) or ac == col_name:
                return ac
        return None

    def translate(self, expression: str) -> str:
        """
        Translate a calculated column expression to SQL.

        Args:
            expression: Python-style expression (e.g., "UPPER(column_name)")

        Returns:
            SQL expression (e.g., 'UPPER("column_name")')
        """
        if not expression or not expression.strip():
            raise ValueError("Expression cannot be empty")

        expression = expression.strip()

        # Handle simple column references
        if expression in self.available_columns:
            return f'"{expression}"'

        # Parse and translate function calls
        try:
            return self._translate_expression(expression)
        except Exception as e:
            logger.error(f"Error translating expression '{expression}': {e}")
            raise ValueError(f"Invalid expression: {expression}. Error: {e!s}")

    def _translate_expression(self, expr: str) -> str:
        """Recursively translate expression."""
        expr = expr.strip()

        # Handle parentheses for grouping
        if expr.startswith('(') and expr.endswith(')'):
            inner = expr[1:-1].strip()
            return f"({self._translate_expression(inner)})"

        # Handle function calls: FUNCTION_NAME(arg1, arg2, ...)
        func_match = re.match(r'^(\w+)\s*\((.+)\)$', expr, re.IGNORECASE)
        if func_match:
            func_name = func_match.group(1).upper()
            args_str = func_match.group(2)

            # CAST uses SQL syntax: CAST(<expr> AS <type>)
            # It is not a normal comma-separated function argument list.
            if func_name == 'CAST':
                cast_expr, cast_type = self._parse_cast_parts(args_str)
                translated_expr = self._translate_expression(cast_expr)
                return f'CAST({translated_expr} AS {cast_type})'

            # Translate function name
            sql_func = self.FUNCTION_MAP.get(func_name, func_name)

            # Parse and translate arguments
            args = self._parse_arguments(args_str)
            translated_args = [self._translate_argument(arg) for arg in args]

            # Validate function compatibility with argument types
            # Check first argument type - extract column name from original arg
            if len(args) > 0:
                first_arg_original = args[0].strip()
                # Extract column name (remove quotes, handle table.column format)
                col_name = first_arg_original.strip('"\'')
                if '.' in col_name:
                    col_name = col_name.split('.')[-1].strip('"\'')

                # Check if first argument is a column reference (resolve join-prefixed names)
                resolved_col = self._resolve_column_name(col_name)
                if resolved_col:
                    col_meta = self.column_metadata_map.get(resolved_col, {})
                    col_type = col_meta.get('datatype', '').upper() if col_meta else ''

                    # String functions (UPPER, LOWER, TRIM, etc.) need text types
                    string_functions = ['UPPER', 'LOWER', 'TRIM', 'LTRIM', 'RTRIM', 'SUBSTRING', 'SUBSTR',
                                       'LENGTH', 'CONCAT', 'REPLACE']
                    if func_name in string_functions:
                        # Check if column type is not text-like
                        if col_type and not any(text_type in col_type for text_type in ['TEXT', 'VARCHAR', 'CHAR', 'STRING']):
                            # For boolean, numeric, date types, cast to text first
                            if 'BOOLEAN' in col_type or 'BOOL' in col_type:
                                translated_args[0] = f'CAST({translated_args[0]} AS TEXT)'
                                logger.info(f"Auto-casting boolean column '{col_name}' to TEXT for {func_name} function")
                            elif any(num_type in col_type for num_type in ['INT', 'NUMERIC', 'DECIMAL', 'FLOAT', 'DOUBLE', 'REAL']):
                                translated_args[0] = f'CAST({translated_args[0]} AS TEXT)'
                                logger.info(f"Auto-casting numeric column '{col_name}' to TEXT for {func_name} function")
                            elif any(date_type in col_type for date_type in ['DATE', 'TIME', 'TIMESTAMP']):
                                translated_args[0] = f'CAST({translated_args[0]} AS TEXT)'
                                logger.info(f"Auto-casting date column '{col_name}' to TEXT for {func_name} function")

            # Build SQL function call
            if func_name == 'SUBSTRING' or func_name == 'SUBSTR':
                # SUBSTRING has special syntax: SUBSTRING(string FROM start [FOR length])
                if len(translated_args) >= 2:
                    if len(translated_args) == 2:
                        return f'SUBSTRING({translated_args[0]} FROM {translated_args[1]})'
                    else:
                        return f'SUBSTRING({translated_args[0]} FROM {translated_args[1]} FOR {translated_args[2]})'

            return f'{sql_func}({", ".join(translated_args)})'

        # Handle operators: +, -, *, /, || (concatenation)
        operators = ['||', '+', '-', '*', '/', '%']
        for op in operators:
            if op in expr:
                parts = self._split_by_operator(expr, op)
                if len(parts) > 1:
                    translated_parts = [self._translate_expression(p.strip()) for p in parts]
                    return f' {op} '.join(translated_parts)

        # Handle comparison operators: =, !=, <, >, <=, >=, LIKE, ILIKE, IN, NOT IN
        comparison_ops = ['!=', '<=', '>=', '<>', '=', '<', '>', 'LIKE', 'ILIKE', 'IN', 'NOT IN']
        for op in comparison_ops:
            if f' {op} ' in expr or expr.startswith(op + ' ') or expr.endswith(' ' + op):
                # This is a comparison, but for calculated columns we usually don't have comparisons
                # If we do, it's likely part of a CASE expression
                pass

        # Handle CASE expressions
        if 'CASE' in expr.upper():
            return self._translate_case_expression(expr)

        # Handle string literals
        if expr.startswith("'") and expr.endswith("'"):
            return expr  # Already a SQL string literal

        if expr.startswith('"') and expr.endswith('"'):
            # Column reference with quotes
            col_name = expr[1:-1]
            resolved = self._resolve_column_name(col_name)
            if resolved:
                return f'"{resolved}"'
            raise ValueError(f"Column '{col_name}' not found in available columns")

        # Handle numeric literals
        if expr.replace('.', '').replace('-', '').isdigit():
            return expr

        # Handle boolean literals
        if expr.upper() in ('TRUE', 'FALSE', 'NULL'):
            return expr.upper()

        # Assume it's a column name (resolve join-prefixed names)
        resolved = self._resolve_column_name(expr)
        if resolved:
            return f'"{resolved}"'

        # Try to find column name in expression (might have table prefix)
        if '.' in expr:
            parts = expr.split('.', 1)
            col_name = parts[-1].strip('"\'')
            resolved = self._resolve_column_name(col_name)
            if resolved:
                return f'"{resolved}"'

        raise ValueError(f"Could not translate expression: {expr}")

    def _parse_cast_parts(self, cast_args: str) -> tuple[str, str]:
        """
        Parse CAST arguments in the form: <expression> AS <type>.
        Supports nested parentheses in expression/type.
        """
        depth = 0
        i = 0
        upper = cast_args.upper()
        while i < len(cast_args):
            ch = cast_args[i]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif depth == 0 and upper[i:i + 4] == ' AS ':
                left = cast_args[:i].strip()
                right = cast_args[i + 4:].strip()
                if not left or not right:
                    break
                # Keep target type as SQL text, but normalize common aliases.
                type_upper = right.upper()
                if type_upper == 'STRING':
                    right = 'TEXT'
                return left, right
            i += 1
        raise ValueError("CAST must use syntax CAST(expression AS type)")

    def _parse_arguments(self, args_str: str) -> list[str]:
        """Parse function arguments, handling nested parentheses."""
        args = []
        current = ""
        depth = 0

        for char in args_str:
            if char == '(':
                depth += 1
                current += char
            elif char == ')':
                depth -= 1
                current += char
            elif char == ',' and depth == 0:
                if current.strip():
                    args.append(current.strip())
                current = ""
            else:
                current += char

        if current.strip():
            args.append(current.strip())

        return args

    def _translate_argument(self, arg: str) -> str:
        """Translate a single function argument."""
        arg = arg.strip()

        # String literal
        if (arg.startswith("'") and arg.endswith("'")) or (arg.startswith('"') and arg.endswith('"')):
            return arg

        # Numeric literal
        if arg.replace('.', '').replace('-', '').isdigit():
            return arg

        # Boolean/NULL
        if arg.upper() in ('TRUE', 'FALSE', 'NULL'):
            return arg.upper()

        # Column reference (resolve join-prefixed names like _L_is_trial)
        resolved = self._resolve_column_name(arg)
        if resolved:
            return f'"{resolved}"'

        # Nested expression
        return self._translate_expression(arg)

    def _split_by_operator(self, expr: str, op: str) -> list[str]:
        """Split expression by operator, respecting parentheses."""
        parts = []
        current = ""
        depth = 0

        i = 0
        while i < len(expr):
            if expr[i] == '(':
                depth += 1
                current += expr[i]
            elif expr[i] == ')':
                depth -= 1
                current += expr[i]
            elif expr[i:i+len(op)] == op and depth == 0:
                if current.strip():
                    parts.append(current.strip())
                current = ""
                i += len(op) - 1
            else:
                current += expr[i]
            i += 1

        if current.strip():
            parts.append(current.strip())

        return parts if parts else [expr]

    def _translate_case_expression(self, expr: str) -> str:
        """Translate CASE WHEN expression."""
        # Simplified CASE translation - full implementation would be more complex
        # For now, return as-is if it looks like valid SQL CASE
        if re.match(r'CASE\s+.*\s+END', expr, re.IGNORECASE | re.DOTALL):
            # Basic validation - full parsing would be more complex
            return expr.upper() if self.db_type == 'postgresql' else expr

        raise ValueError(f"Invalid CASE expression: {expr}")

    def validate_column_references(self, expression: str) -> tuple[bool, Optional[str]]:
        """
        Validate that all column references in expression exist in available_columns.

        Returns:
            (is_valid, error_message)
        """
        # Extract column references from expression
        # Simple regex to find column names (not perfect but good enough)
        col_refs = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', expression)

        for ref in col_refs:
            # Skip function names and literals
            if ref.upper() in self.FUNCTION_MAP or ref.upper() in ('TRUE', 'FALSE', 'NULL'):
                continue

            # Check if it's a column reference (resolve join-prefixed names)
            if not self._resolve_column_name(ref):
                # Check if it's a table-qualified column (table.column)
                if '.' in expression:
                    parts = expression.split('.')
                    if len(parts) == 2 and parts[1].strip('"\'') == ref:
                        if self._resolve_column_name(parts[1].strip('"\'')):
                            continue

                return False, f"Column '{ref}' not found in available columns"

        return True, None
