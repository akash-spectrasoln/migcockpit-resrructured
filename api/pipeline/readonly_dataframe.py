# Moved from: api/services/readonly_dataframe.py
"""
Production-Safe Read-Only DataFrame Wrapper for Compute Nodes

Design Principles:
1. Defensive Immutability: Prevent accidental mutations while allowing reads
2. Copy-on-Write: Only copy when mutation is attempted (performance)
3. Type Preservation: Maintain schema and data types through transformations
4. Parallel Safety: Thread-safe for concurrent execution
5. Clear Contracts: Explicit API for compute node operations

Why Pandas DataFrames Cannot Be Truly Immutable:
- Python objects are mutable by default
- Views vs copies ambiguity (df[col] may return view or copy)
- In-place operations (df.drop(inplace=True))
- Shared memory references across operations
- No built-in immutability guarantees
- Assignment operations (df['new_col'] = values) modify in-place
"""

from contextlib import contextmanager
import copy
import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

class ReadOnlyDataFrameError(Exception):
    """Raised when attempting to mutate a read-only DataFrame"""
    pass

class ReadOnlyDataFrame:
    """
    Production-safe read-only wrapper for Pandas DataFrames.

    Prevents accidental mutations while maintaining full read access.
    Uses copy-on-write semantics for performance.

    Usage:
        readonly_df = ReadOnlyDataFrame(input_df)
        # All read operations work normally
        result = readonly_df['column'].sum()
        # Mutations raise ReadOnlyDataFrameError
        readonly_df['new_col'] = values  # Raises error
    """

    def __init__(self, df: pd.DataFrame, name: str = "input_d"):
        """
        Initialize read-only wrapper.

        Args:
            df: Source DataFrame (will be copied internally)
            name: Name for error messages (default: "input_d")
        """
        # CRITICAL: Always create a deep copy to prevent external mutations
        # This ensures the original DataFrame cannot be modified even if
        # someone holds a reference to it
        self._df = df.copy(deep=True)
        self._name = name
        self._is_readonly = True
        self._mutation_attempts = []

        # Store original schema for validation
        self._original_schema = {
            'columns': list(self._df.columns),
            'dtypes': {col: str(dtype) for col, dtype in self._df.dtypes.items()},
            'shape': self._df.shape
        }

    def __getattr__(self, name: str) -> Any:
        """
        Delegate attribute access to underlying DataFrame.
        Intercepts mutation methods.
        """
        attr = getattr(self._df, name)

        # Block mutation methods
        if name in self._MUTATION_METHODS:
            def blocked_method(*args, **kwargs):
                self._record_mutation_attempt(name, args, kwargs)
                raise ReadOnlyDataFrameError(
                    f"Cannot call {name}() on read-only DataFrame '{self._name}'. "
                    f"Create a new DataFrame instead: output_df = {self._name}.copy()"
                )
            return blocked_method

        # Allow read-only methods
        if callable(attr):
            def safe_method(*args, **kwargs):
                # Check for inplace=True in kwargs
                if kwargs.get('inplace', False):
                    self._record_mutation_attempt(name, args, kwargs)
                    raise ReadOnlyDataFrameError(
                        f"Cannot use inplace=True on read-only DataFrame '{self._name}'. "
                        f"Use: output_df = {self._name}.{name}(inplace=False)"
                    )
                return attr(*args, **kwargs)
            return safe_method

        return attr

    def __getitem__(self, key: Any) -> Any:
        """
        Indexing access - returns read-only view for DataFrames.
        Returns values for Series.
        """
        result = self._df[key]

        # If result is a DataFrame, wrap it in read-only
        if isinstance(result, pd.DataFrame):
            return ReadOnlyDataFrame(result, f"{self._name}[{key}]")

        # Series and scalars are returned as-is (they're copies/views)
        return result

    def __setitem__(self, key: Any, value: Any) -> None:
        """Block assignment operations"""
        self._record_mutation_attempt('__setitem__', (key,), {})
        raise ReadOnlyDataFrameError(
            f"Cannot assign to read-only DataFrame '{self._name}'. "
            f"Create a new DataFrame: output_df = {self._name}.copy()\n"
            f"Then modify: output_df['{key}'] = value"
        )

    def __delitem__(self, key: Any) -> None:
        """Block deletion operations"""
        self._record_mutation_attempt('__delitem__', (key,), {})
        raise ReadOnlyDataFrameError(
            f"Cannot delete from read-only DataFrame '{self._name}'. "
            f"Use: output_df = {self._name}.drop(columns=['{key}'])"
        )

    def copy(self, deep: bool = True) -> pd.DataFrame:
        """
        Safe copy operation - returns mutable DataFrame.
        This is the intended way to create a new DataFrame for transformations.
        """
        return self._df.copy(deep=deep)

    def to_pandas(self) -> pd.DataFrame:
        """
        Explicit conversion to mutable Pandas DataFrame.
        Returns a copy for safety.
        """
        return self._df.copy(deep=True)

    @property
    def shape(self) -> tuple:
        """Read-only shape property"""
        return self._df.shape

    @property
    def columns(self) -> pd.Index:
        """Read-only columns property"""
        return self._df.columns.copy()

    @property
    def dtypes(self) -> pd.Series:
        """Read-only dtypes property"""
        return self._df.dtypes.copy()

    @property
    def index(self) -> pd.Index:
        """Read-only index property"""
        return self._df.index.copy()

    def get_schema(self) -> dict[str, Any]:
        """
        Get original schema for validation.
        Useful for ensuring transformations preserve expected structure.
        """
        return copy.deepcopy(self._original_schema)

    def validate_schema_preservation(self, output_df: pd.DataFrame) -> bool:
        """
        Validate that output DataFrame preserves expected schema characteristics.
        Returns True if validation passes, raises ValueError if not.
        """
        # Basic validation - can be extended
        if not isinstance(output_df, pd.DataFrame):
            raise ValueError(f"Output must be a DataFrame, got {type(output_df)}")

        # Log schema changes for debugging
        original_cols = set(self._original_schema['columns'])
        output_cols = set(output_df.columns)

        added = output_cols - original_cols
        removed = original_cols - output_cols

        if added:
            logger.info(f"Schema change: Added columns {added}")
        if removed:
            logger.info(f"Schema change: Removed columns {removed}")

        return True

    def _record_mutation_attempt(self, method: str, args: tuple, kwargs: dict) -> None:
        """Record mutation attempts for debugging"""
        self._mutation_attempts.append({
            'method': method,
            'args': str(args)[:100],  # Truncate for logging
            'kwargs': {k: str(v)[:50] for k, v in kwargs.items()}
        })
        logger.warning(
            f"Mutation attempt on read-only DataFrame '{self._name}': {method}"
        )

    def get_mutation_attempts(self) -> list[dict[str, Any]]:
        """Get list of mutation attempts (for debugging)"""
        return copy.deepcopy(self._mutation_attempts)

    # Methods that mutate DataFrames (blocked)
    _MUTATION_METHODS = {
        'drop', 'dropna', 'drop_duplicates', 'fillna', 'replace',
        'rename', 'set_index', 'reset_index', 'sort_values', 'sort_index',
        'assign', 'insert', 'pop', 'update', 'append', 'concat',
        'merge', 'join', 'groupby',  # These return new objects, but blocking for safety
    }

    def __repr__(self) -> str:
        return f"ReadOnlyDataFrame(name='{self._name}', shape={self.shape})"

    def __str__(self) -> str:
        return f"ReadOnlyDataFrame(name='{self._name}', shape={self.shape})"

class ComputeNodeContract:
    """
    Contract-based design for compute node execution.
    Enforces read-only input and validates output.
    """

    def __init__(
        self,
        input_df: pd.DataFrame,
        input_name: str = "input_d",
        validate_output: bool = True,
        preserve_schema: bool = False
    ):
        """
        Initialize compute node contract.

        Args:
            input_df: Input DataFrame (will be wrapped as read-only)
            input_name: Variable name for input in execution context
            validate_output: Whether to validate output DataFrame
            preserve_schema: Whether to enforce schema preservation
        """
        self._readonly_input = ReadOnlyDataFrame(input_df, input_name)
        self._input_name = input_name
        self._validate_output = validate_output
        self._preserve_schema = preserve_schema

    def get_execution_context(self) -> dict[str, Any]:
        """
        Get execution context with read-only input.
        This is what gets passed to exec() or eval().
        """
        return {
            self._input_name: self._readonly_input,
            'pd': pd,
            'np': np,
            'output_d': None,
            # Helper function for safe DataFrame creation
            'create_output': lambda: self._readonly_input.copy()
        }

    def validate_output(self, output_df: Any) -> pd.DataFrame:
        """
        Validate output DataFrame according to contract.
        Returns validated DataFrame or raises ValueError.
        """
        if output_df is None:
            raise ValueError(
                "Compute node must assign a DataFrame to 'output_df'. "
                f"Example: output_df = {self._input_name}.copy()"
            )

        if not isinstance(output_df, pd.DataFrame):
            raise ValueError(
                f"output_df must be a pandas DataFrame, got {type(output_df).__name__}"
            )

        if self._preserve_schema:
            self._readonly_input.validate_schema_preservation(output_df)

        # Additional validations
        if len(output_df) == 0:
            logger.warning("Output DataFrame is empty")

        # Check for common issues
        if output_df.isnull().all().any():
            logger.warning("Output DataFrame contains columns with all null values")

        return output_df

    def get_mutation_report(self) -> dict[str, Any]:
        """Get report of mutation attempts (for debugging)"""
        attempts = self._readonly_input.get_mutation_attempts()
        return {
            'mutation_attempts': len(attempts),
            'details': attempts,
            'input_name': self._input_name
        }

@contextmanager
def readonly_dataframe_context(df: pd.DataFrame, name: str = "input_d"):
    """
    Context manager for read-only DataFrame access.

    Usage:
        with readonly_dataframe_context(input_df) as readonly_df:
            # Use readonly_df safely
            result = readonly_df['col'].sum()
    """
    readonly = ReadOnlyDataFrame(df, name)
    try:
        yield readonly
    finally:
        # Cleanup if needed
        pass
