# Moved from: api/utils/calculated_column_evaluator.py
# Production-Ready Calculated Column Expression Evaluator
# This file provides a robust row-level expression evaluator for calculated columns

import logging
import re

logger = logging.getLogger(__name__)

def evaluate_calculated_expression(expression: str, row: dict, available_columns: list) -> any:
    """
    Evaluate a calculated column expression using actual row values.

    Supports SQL-style functions:
    - UPPER(col), LOWER(col), TRIM(col)
    - SUBSTRING(col, start, length)
    - CONCAT(col1, col2, ...)
    - COALESCE(col1, col2, ...)

    Args:
        expression: SQL-style expression (e.g., "UPPER(table_name)")
        row: Dictionary of column_name -> value for current row
        available_columns: List of available column names

    Returns:
        Evaluated value (string, number, or None)
    """
    try:
        # Build case-insensitive mappings for both row keys and available columns
        row_lower = {k.lower(): (k, v) for k, v in row.items()}  # Map to (original_key, value)
        {col.lower(): col for col in available_columns}

        # First, build a mapping of all possible column references to their actual row values
        # This handles case-insensitive matching and finds the actual row key
        col_value_map = {}
        for col in available_columns:
            col_lower = col.lower()
            # Try to find matching row key (case-insensitive)
            if col_lower in row_lower:
                actual_key, value = row_lower[col_lower]
                col_value_map[col] = value
                col_value_map[col_lower] = value
                col_value_map[actual_key] = value
                col_value_map[actual_key.lower()] = value
            else:
                # Column not found in row, use empty string
                col_value_map[col] = ''
                col_value_map[col_lower] = ''

        # Also add all row keys directly (in case they're not in available_columns)
        for row_key, row_value in row.items():
            row_key_lower = row_key.lower()
            if row_key not in col_value_map:
                col_value_map[row_key] = row_value
                col_value_map[row_key_lower] = row_value

        # Replace column references with actual values
        eval_expr = expression

        # Sort columns by length (longest first) to avoid partial matches
        sorted_cols = sorted(set(list(available_columns) + list(row.keys())), key=len, reverse=True)

        for col in sorted_cols:
            # Get value from our mapping
            value = col_value_map.get(col) or col_value_map.get(col.lower(), '')

            # Convert None to empty string
            if value is None:
                value = ''

            # Escape value for safe replacement
            if isinstance(value, str):
                value_str = value.replace("'", "''")
                value_repr = f"'{value_str}'"
            elif isinstance(value, (int, float)):
                value_repr = str(value)
            elif isinstance(value, bool):
                value_repr = 'TRUE' if value else 'FALSE'
            else:
                value_repr = f"'{value!s}'"

            # Escape column name for regex
            escaped_col = re.escape(col)

            # Replace quoted column references: "col" or 'col'
            eval_expr = re.sub(
                r'["\']{escaped_col}["\']',
                value_repr,
                eval_expr,
                flags=re.IGNORECASE
            )

            # Replace bracketed column references: [col]
            eval_expr = re.sub(
                rf'\[{escaped_col}\]',
                value_repr,
                eval_expr,
                flags=re.IGNORECASE
            )

            # Replace unquoted column references (word boundary, but not inside function names)
            # Use negative lookbehind/lookahead to avoid matching inside function names
            eval_expr = re.sub(
                rf'(?<![a-zA-Z_]){escaped_col}(?![a-zA-Z_])',
                value_repr,
                eval_expr,
                flags=re.IGNORECASE
            )

        # Log the expression after column replacement for debugging
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Expression after column replacement: {eval_expr}")

        # Now evaluate SQL functions in Python
        result = _evaluate_sql_functions(eval_expr)

        return result

    except Exception as e:
        logger.error(f"Error evaluating expression '{expression}': {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def _evaluate_sql_functions(expr: str) -> any:
    """
    Evaluate SQL functions using Python equivalents.
    Processes from innermost to outermost functions using regex.
    """
    expr = expr.strip()

    # If expression is already a simple quoted string or number (no functions), return it
    if '(' not in expr:
        return expr.strip().strip("'\"")

    # Process functions recursively (innermost first)
    max_iterations = 50  # Prevent infinite loops
    iteration = 0

    while iteration < max_iterations and '(' in expr:
        iteration += 1

        # Find innermost function call using regex
        # Pattern: function_name(argument_string) where argument_string has no unmatched parens
        pattern = r'(\w+)\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)'
        match = re.search(pattern, expr, re.IGNORECASE)

        if not match:
            # No more function calls found
            break

        fn_name = match.group(1).upper().strip()
        args_str = match.group(2).strip()

        # Parse arguments - split by comma, but respect quotes and nested parens
        args = _parse_function_arguments(args_str)

        # Evaluate function
        result = ''
        try:
            if fn_name == 'UPPER':
                result = args[0].upper() if args and args[0] else ''
            elif fn_name == 'LOWER':
                result = args[0].lower() if args and args[0] else ''
            elif fn_name == 'TRIM':
                result = args[0].strip() if args and args[0] else ''
            elif fn_name == 'SUBSTRING' or fn_name == 'SUBSTR':
                if len(args) >= 2 and args[0]:
                    text = str(args[0])
                    try:
                        start = int(args[1]) - 1  # SQL is 1-indexed
                        length = int(args[2]) if len(args) > 2 else len(text)
                        if start < 0:
                            start = 0
                        if start >= len(text):
                            result = ''
                        else:
                            result = text[start:start+length]
                    except (ValueError, IndexError):
                        result = text
                else:
                    result = args[0] if args and args[0] else ''
            elif fn_name == 'CONCAT':
                result = ''.join(str(arg) if arg else '' for arg in args)
            elif fn_name == 'COALESCE':
                result = next((str(arg) for arg in args if arg and str(arg).strip()), '')
            else:
                # Unknown function, return first arg or empty
                result = args[0] if args and args[0] else ''
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Unknown function '{fn_name}', returning first argument: {result}")
        except Exception as e:
            logger.error(f"Error evaluating function {fn_name} with args {args}: {e}")
            result = ''

        # Replace function call with quoted result
        result_str = f"'{result}'" if isinstance(result, str) else str(result)
        expr = expr[:match.start()] + result_str + expr[match.end():]

    if iteration >= max_iterations:
        logger.warning(f"Reached max iterations evaluating expression: {expr[:100]}...")

    # Final result: strip quotes and return
    final_result = expr.strip().strip("'\"")

    # If result is empty string or just quotes, return empty string
    if not final_result or final_result in ("''", '""'):
        return ''

    return final_result

def _parse_function_arguments(args_str: str) -> list:
    """
    Parse function arguments, handling quoted strings and nested structures.
    """
    if not args_str.strip():
        return []

    args = []
    current_arg = ''
    in_quotes = False
    quote_char = None

    i = 0
    while i < len(args_str):
        char = args_str[i]

        if char in ("'", '"'):
            if not in_quotes:
                in_quotes = True
                quote_char = char
                current_arg += char
            elif char == quote_char:
                # Check if it's escaped (double quote)
                if i + 1 < len(args_str) and args_str[i + 1] == quote_char:
                    current_arg += char + char
                    i += 1  # Skip next quote
                else:
                    in_quotes = False
                    quote_char = None
                    current_arg += char
            else:
                current_arg += char
        elif char == ',' and not in_quotes:
            args.append(current_arg.strip())
            current_arg = ''
        else:
            current_arg += char

        i += 1

    if current_arg.strip():
        args.append(current_arg.strip())

    # Clean arguments - remove outer quotes and unescape
    cleaned_args = []
    for arg in args:
        arg = arg.strip()
        # Remove outer quotes if present
        if (arg.startswith("'") and arg.endswith("'")) or (arg.startswith('"') and arg.endswith('"')):
            arg = arg[1:-1]
        # Unescape quotes
        arg = arg.replace("''", "'").replace('""', '"')
        cleaned_args.append(arg)

    return cleaned_args
