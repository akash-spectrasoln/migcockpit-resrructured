"""
Example: Production-Safe Compute Node Execution with Read-Only Input

This demonstrates the recommended pattern for compute node execution
with read-only DataFrame protection.
"""

import logging

import numpy as np
import pandas as pd
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from api.authentications import JWTCookieAuthentication
from api.services.readonly_dataframe import ComputeNodeContract, ReadOnlyDataFrameError

logger = logging.getLogger(__name__)

class SafeComputeNodeExecutionView(APIView):
    """
    Production-safe compute node execution with read-only input protection.

    Key Features:
    - Input DataFrame is read-only (cannot be mutated)
    - Clear error messages guide users to correct pattern
    - Output validation ensures type safety
    - Mutation attempt tracking for debugging
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        try:
            code = request.data.get('code', '')
            language = request.data.get('language', 'python')
            input_data = request.data.get('input_data', [])
            validate_output = request.data.get('validate_output', True)
            preserve_schema = request.data.get('preserve_schema', False)

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

            # Create input DataFrame
            input_df = pd.DataFrame(input_data)

            # Create compute contract with read-only protection
            contract = ComputeNodeContract(
                input_df=input_df,
                input_name="input_df",
                validate_output=validate_output,
                preserve_schema=preserve_schema
            )

            # Get execution context with read-only input
            local_vars = contract.get_execution_context()

            # Restricted globals for security
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
                'np': np,
            }

            try:
                # Execute user code with read-only input
                exec(code, restricted_globals, local_vars)
            except ReadOnlyDataFrameError as ro_error:
                # User tried to mutate input - provide helpful error
                logger.warning(f"Mutation attempt detected: {ro_error}")
                mutation_report = contract.get_mutation_report()
                return Response(
                    {
                        "error": f"Read-only violation: {ro_error!s}",
                        "error_type": "ReadOnlyDataFrameError",
                        "hint": "Create a new DataFrame: output_df = input_df.copy()",
                        "mutation_attempts": mutation_report['mutation_attempts'],
                        "example_code": """
# ❌ WRONG (mutates input):
input_df['new_col'] = values

# ✅ CORRECT (creates new DataFrame):
output_df = input_df.copy()
output_df['new_col'] = values
"""
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )
            except Exception as exec_error:
                logger.error(f"Code execution error: {exec_error}", exc_info=True)
                return Response(
                    {
                        "error": f"Execution error: {exec_error!s}",
                        "error_type": type(exec_error).__name__
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Validate output
            try:
                output_df = contract.validate_output(local_vars.get('output_df'))
            except ValueError as validation_error:
                return Response(
                    {
                        "error": f"Output validation failed: {validation_error!s}",
                        "error_type": "ValidationError"
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Get mutation report (for debugging and monitoring)
            mutation_report = contract.get_mutation_report()
            if mutation_report['mutation_attempts'] > 0:
                logger.warning(
                    f"Compute node had {mutation_report['mutation_attempts']} "
                    "mutation attempts (all blocked safely)"
                )

            # Convert to response format
            output_data = output_df.to_dict('records')
            output_columns = [
                {
                    'name': col,
                    'datatype': str(output_df[col].dtype),
                    'nullable': output_df[col].isnull().any()
                }
                for col in output_df.columns
            ]

            response_data = {
                "success": True,
                "output_data": output_data,
                "output_metadata": {
                    "columns": output_columns,
                    "row_count": len(output_df)
                }
            }

            # Include mutation report in debug mode
            if request.data.get('debug', False):
                response_data['debug'] = {
                    'mutation_attempts': mutation_report['mutation_attempts'],
                    'mutation_details': mutation_report['details']
                }

            return Response(response_data, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Compute node execution error: {e}", exc_info=True)
            return Response(
                {"error": f"Failed to execute compute node: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

# Example usage patterns for users:

EXAMPLE_CODE_SAFE = """
# ✅ SAFE: Create new DataFrame for transformations
output_df = input_df.copy()
output_df['calculated'] = output_df['col1'] * 2 + output_df['col2']
output_df = output_df.dropna()

# ✅ SAFE: Chain operations (each returns new DataFrame)
output_df = input_df.copy().dropna().rename(columns={'old': 'new'})

# ✅ SAFE: Use helper function
output_df = create_output()  # Returns input_df.copy()
output_df['new_col'] = output_df['existing_col'].apply(lambda x: x.upper())
"""

EXAMPLE_CODE_UNSAFE = """
# ❌ UNSAFE: Direct mutation (will raise ReadOnlyDataFrameError)
input_df['new_col'] = values

# ❌ UNSAFE: In-place operations (will raise ReadOnlyDataFrameError)
input_df.dropna(inplace=True)

# ❌ UNSAFE: Assignment (will raise ReadOnlyDataFrameError)
input_df.loc[:, 'col'] = values
"""
