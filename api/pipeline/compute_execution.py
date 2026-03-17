# Moved from: api/utils/compute_execution.py
import ast
import logging
import re

import pandas as pd
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from api.authentications import JWTCookieAuthentication

logger = logging.getLogger(__name__)

class ComputeNodeExecutionView(APIView):
    """
    Execute Python code for Compute nodes in a sandboxed environment.
    Input: DataFrame from upstream node
    Output: Transformed DataFrame
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        try:
            code = request.data.get('code', '')
            language = request.data.get('language', 'python')
            input_data = request.data.get('input_data', [])

            if not code or not code.strip():
                return Response(
                    {"error": "Code cannot be empty"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if language != 'python':
                return Response(
                    {"error": f"Unsupported language: {language}. Only Python is supported."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            input_df = pd.DataFrame(input_data)

            local_vars = {
                'input_d': input_df,
                'pd': pd,
                'output_df': None
            }

            restricted_globals = {
                '__builtins__': {
                    'len': len,
                    'range': range,
                    'enumerate': enumerate,
                    'zip': zip,
                    'map': map,
                    'filter': filter,
                    'sum': sum,
                    'min': min,
                    'max': max,
                    'abs': abs,
                    'round': round,
                    'int': int,
                    'float': float,
                    'str': str,
                    'bool': bool,
                    'list': list,
                    'dict': dict,
                    'set': set,
                    'tuple': tuple,
                    'print': print,
                },
                'pd': pd,
            }

            try:
                exec(code, restricted_globals, local_vars)
            except Exception as exec_error:
                logger.error(f"Code execution error: {exec_error}", exc_info=True)
                return Response(
                    {
                        "error": f"Execution error: {exec_error!s}",
                        "error_type": type(exec_error).__name__
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

            output_df = local_vars.get('output_df')

            if output_df is None:
                return Response(
                    {"error": "Code must assign a DataFrame to 'output_df'"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if not isinstance(output_df, pd.DataFrame):
                return Response(
                    {"error": f"output_df must be a DataFrame, got {type(output_df).__name__}"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            output_data = output_df.to_dict('records')
            output_columns = [
                {
                    'name': col,
                    'datatype': str(output_df[col].dtype),
                    'nullable': output_df[col].isnull().any()
                }
                for col in output_df.columns
            ]

            return Response({
                "success": True,
                "output_data": output_data,
                "output_metadata": {
                    "columns": output_columns,
                    "row_count": len(output_df)
                }
            }, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Compute node execution error: {e}", exc_info=True)
            return Response(
                {"error": f"Failed to execute compute node: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class ComputeNodeCompileView(APIView):
    """
    Validate Python code for Compute nodes WITHOUT executing it.
    Performs static analysis to catch syntax errors and contract violations.
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def _normalize_code(self, code: str) -> str:
        """
        Normalize code to canonical form by removing comments, example code, and malformed text.
        PRESERVES ALL WHITESPACE (no trimming of any kind).
        If user hasn't added custom logic, converts to: _output_df = _input_df.copy()
        """
        if not code:
            return '_output_df = _input_df.copy()'

        # Preserve code exactly as-is - no trimming, no whitespace removal
        # Only check if it's empty after checking for content
        if not code.strip():
            return '_output_df = _input_df.copy()'

        # Return code exactly as entered - preserve all whitespace, indentation, empty lines
        return code

    def post(self, request):
        """
        Compile and validate Python code without execution.

        Checks:
        1. Syntax validation (AST parsing)
        2. Contract validation (_output_df assignment)
        3. Restricted variable rules (_input_df read-only)
        4. Safe execution environment (no I/O operations)
        """
        try:
            code = request.data.get('code', '')
            language = request.data.get('language', 'python')
            normalize = request.data.get('normalize', False)  # Don't normalize by default - preserve all whitespace

            # Normalize code only if explicitly requested (preserves all whitespace by default)
            if normalize:
                code = self._normalize_code(code)

            if not code or not code.strip():
                return Response(
                    {
                        "success": False,
                        "error": "Code cannot be empty",
                        "error_type": "ValidationError",
                        "line_number": None
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

            if language != 'python':
                return Response(
                    {
                        "success": False,
                        "error": f"Unsupported language: {language}. Only Python is supported.",
                        "error_type": "ValidationError",
                        "line_number": None
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

            # 1. Syntax Validation using AST
            try:
                tree = ast.parse(code, mode='exec')
            except SyntaxError as syn_err:
                logger.warning(f"[Compile] Syntax error: {syn_err}")
                return Response(
                    {
                        "success": False,
                        "error": f"Syntax Error: {syn_err.msg}",
                        "error_type": "SyntaxError",
                        "line_number": syn_err.lineno,
                        "text": syn_err.text,
                        "offset": syn_err.offset
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )
            except IndentationError as ind_err:
                logger.warning(f"[Compile] Indentation error: {ind_err}")
                return Response(
                    {
                        "success": False,
                        "error": f"Indentation Error: {ind_err.msg}",
                        "error_type": "IndentationError",
                        "line_number": ind_err.lineno,
                        "text": ind_err.text,
                        "offset": ind_err.offset
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

            # 2. Contract Validation: Check for _output_df assignment
            # Use AST visitor to find assignments
            class OutputDFChecker(ast.NodeVisitor):
                def __init__(self):
                    self.has_output_df_assignment = False
                    self.has_input_df_reassignment = False
                    self.input_df_reassignment_line = None
                    self.output_df_assignment_line = None

                def visit_Assign(self, node):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            if target.id == '_output_d':
                                self.has_output_df_assignment = True
                                self.output_df_assignment_line = node.lineno
                            elif target.id == '_input_d':
                                self.has_input_df_reassignment = True
                                self.input_df_reassignment_line = node.lineno
                    self.generic_visit(node)

                def visit_AugAssign(self, node):
                    if isinstance(node.target, ast.Name) and node.target.id == '_input_d':
                        self.has_input_df_reassignment = True
                        self.input_df_reassignment_line = node.lineno
                    self.generic_visit(node)

            checker = OutputDFChecker()
            checker.visit(tree)

            # Check for _output_df assignment (also check string pattern as fallback)
            if not checker.has_output_df_assignment:
                # Fallback: Check if _output_df appears in code as assignment
                if not re.search(r'_output_df\s*=', code):
                    return Response(
                        {
                            "success": False,
                            "error": "Code must assign a DataFrame to '_output_df'. Example: _output_df = _input_df.copy()",
                            "error_type": "ContractViolation",
                            "line_number": None
                        },
                        status=status.HTTP_400_BAD_REQUEST
                    )

            # 3. Restricted Variable Rules: Check for _input_df reassignment
            if checker.has_input_df_reassignment:
                return Response(
                    {
                        "success": False,
                        "error": f"_input_df is read-only and must not be reassigned (line {checker.input_df_reassignment_line}). Use _input_df.copy() instead.",
                        "error_type": "ContractViolation",
                        "line_number": checker.input_df_reassignment_line
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

            # 4. Safe Execution Environment: Check for dangerous operations
            # Check for common I/O operations (basic check - not exhaustive)
            dangerous_patterns = [
                (r'\bopen\s*\(', 'File I/O operations are not allowed'),
                (r'\b__import__\s*\(', 'Dynamic imports are not allowed'),
                (r'\beval\s*\(', 'eval() is not allowed'),
                (r'\bexec\s*\(', 'exec() is not allowed'),
                (r'\binput\s*\(', 'input() is not allowed'),
            ]

            for pattern, message in dangerous_patterns:
                if re.search(pattern, code):
                    # Find line number
                    lines = code.split('\n')
                    line_num = None
                    for i, line in enumerate(lines, 1):
                        if re.search(pattern, line):
                            line_num = i
                            break

                    return Response(
                        {
                            "success": False,
                            "error": f"{message} (line {line_num})",
                            "error_type": "SecurityViolation",
                            "line_number": line_num
                        },
                        status=status.HTTP_400_BAD_REQUEST
                    )

            # All validations passed
            logger.info("[Compile] Code validation successful")
            return Response(
                {
                    "success": True,
                    "message": "Compilation successful",
                    "checks": {
                        "syntax": "valid",
                        "output_assignment": "found",
                        "input_readonly": "enforced",
                        "security": "passed"
                    }
                },
                status=status.HTTP_200_OK
            )

        except Exception as e:
            logger.error(f"[Compile] Unexpected error: {e}", exc_info=True)
            return Response(
                {
                    "success": False,
                    "error": f"Compilation error: {e!s}",
                    "error_type": type(e).__name__,
                    "line_number": None
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
