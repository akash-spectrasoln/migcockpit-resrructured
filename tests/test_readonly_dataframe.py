"""
Unit tests for ReadOnlyDataFrame and ComputeNodeContract

Tests verify that:
1. Read operations work normally
2. Mutation operations are blocked
3. Copy operations create mutable DataFrames
4. Schema validation works correctly
"""

import numpy as np
import pandas as pd
import pytest

from api.services.readonly_dataframe import ComputeNodeContract, ReadOnlyDataFrame, ReadOnlyDataFrameError


class TestReadOnlyDataFrame:
    """Test ReadOnlyDataFrame wrapper"""

    def test_read_operations(self):
        """Read operations should work normally"""
        df = pd.DataFrame({
            'a': [1, 2, 3, 4, 5],
            'b': [10, 20, 30, 40, 50],
            'c': ['x', 'y', 'z', 'w', 'v']
        })
        readonly = ReadOnlyDataFrame(df, name="test_df")

        # Column access
        assert readonly['a'].sum() == 15
        assert len(readonly['b']) == 5

        # Multiple columns
        subset = readonly[['a', 'b']]
        assert isinstance(subset, ReadOnlyDataFrame)
        assert list(subset.columns) == ['a', 'b']

        # Shape and properties
        assert readonly.shape == (5, 3)
        assert len(readonly) == 5
        assert list(readonly.columns) == ['a', 'b', 'c']

    def test_mutation_blocked_setitem(self):
        """Assignment operations should be blocked"""
        df = pd.DataFrame({'a': [1, 2, 3]})
        readonly = ReadOnlyDataFrame(df, name="test_df")

        with pytest.raises(ReadOnlyDataFrameError) as exc_info:
            readonly['new_col'] = [4, 5, 6]

        assert "read-only" in str(exc_info.value).lower()
        assert "test_df" in str(exc_info.value)

    def test_mutation_blocked_delitem(self):
        """Deletion operations should be blocked"""
        df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        readonly = ReadOnlyDataFrame(df, name="test_df")

        with pytest.raises(ReadOnlyDataFrameError) as exc_info:
            del readonly['a']

        assert "read-only" in str(exc_info.value).lower()

    def test_mutation_blocked_inplace(self):
        """In-place operations should be blocked"""
        df = pd.DataFrame({'a': [1, 2, None, 4, 5]})
        readonly = ReadOnlyDataFrame(df, name="test_df")

        with pytest.raises(ReadOnlyDataFrameError) as exc_info:
            readonly.dropna(inplace=True)

        assert "inplace" in str(exc_info.value).lower()

    def test_copy_creates_mutable(self):
        """Copy should create a mutable DataFrame"""
        df = pd.DataFrame({'a': [1, 2, 3]})
        readonly = ReadOnlyDataFrame(df, name="test_df")

        # Copy should be mutable
        copy_df = readonly.copy()
        copy_df['new_col'] = [4, 5, 6]  # Should not raise
        assert 'new_col' in copy_df.columns
        assert list(copy_df['new_col']) == [4, 5, 6]

        # Original should still be read-only
        with pytest.raises(ReadOnlyDataFrameError):
            readonly['another_col'] = [1, 2, 3]

    def test_original_dataframe_unchanged(self):
        """Original DataFrame should remain unchanged"""
        original_data = {'a': [1, 2, 3], 'b': [4, 5, 6]}
        df = pd.DataFrame(original_data)
        readonly = ReadOnlyDataFrame(df, name="test_df")

        # Try to mutate (will fail, but verify original is safe)
        try:
            readonly['new'] = [1, 2, 3]
        except ReadOnlyDataFrameError:
            pass

        # Original DataFrame should be unchanged
        assert list(df.columns) == ['a', 'b']
        assert 'new' not in df.columns

    def test_mutation_tracking(self):
        """Mutation attempts should be tracked"""
        df = pd.DataFrame({'a': [1, 2, 3]})
        readonly = ReadOnlyDataFrame(df, name="test_df")

        # Attempt mutations
        try:
            readonly['new1'] = [1, 2, 3]
        except ReadOnlyDataFrameError:
            pass

        try:
            readonly['new2'] = [4, 5, 6]
        except ReadOnlyDataFrameError:
            pass

        attempts = readonly.get_mutation_attempts()
        assert len(attempts) == 2
        assert all('__setitem__' in attempt['method'] for attempt in attempts)

    def test_schema_preservation(self):
        """Schema should be preserved and accessible"""
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': [1.1, 2.2, 3.3],
            'c': ['x', 'y', 'z']
        })
        readonly = ReadOnlyDataFrame(df, name="test_df")

        schema = readonly.get_schema()
        assert schema['columns'] == ['a', 'b', 'c']
        assert schema['shape'] == (3, 3)
        assert 'dtypes' in schema


class TestComputeNodeContract:
    """Test ComputeNodeContract"""

    def test_execution_context(self):
        """Execution context should contain read-only input"""
        df = pd.DataFrame({'a': [1, 2, 3]})
        contract = ComputeNodeContract(df, input_name="input_df")

        context = contract.get_execution_context()

        # Should have read-only input
        assert 'input_df' in context
        assert isinstance(context['input_df'], ReadOnlyDataFrame)

        # Should have pandas
        assert 'pd' in context
        assert context['pd'] is pd

        # Should have helper
        assert 'create_output' in context
        assert callable(context['create_output'])

    def test_validate_output_success(self):
        """Valid output should pass validation"""
        df = pd.DataFrame({'a': [1, 2, 3]})
        contract = ComputeNodeContract(df)

        output_df = pd.DataFrame({'b': [4, 5, 6]})
        validated = contract.validate_output(output_df)

        assert validated is output_df
        assert isinstance(validated, pd.DataFrame)

    def test_validate_output_none(self):
        """None output should fail validation"""
        df = pd.DataFrame({'a': [1, 2, 3]})
        contract = ComputeNodeContract(df)

        with pytest.raises(ValueError) as exc_info:
            contract.validate_output(None)

        assert "output_df" in str(exc_info.value).lower()

    def test_validate_output_wrong_type(self):
        """Non-DataFrame output should fail validation"""
        df = pd.DataFrame({'a': [1, 2, 3]})
        contract = ComputeNodeContract(df)

        with pytest.raises(ValueError) as exc_info:
            contract.validate_output([1, 2, 3])

        assert "dataframe" in str(exc_info.value).lower()

    def test_mutation_report(self):
        """Mutation report should track attempts"""
        df = pd.DataFrame({'a': [1, 2, 3]})
        contract = ComputeNodeContract(df)

        # Simulate mutation attempt
        readonly = contract.get_execution_context()['input_df']
        try:
            readonly['new'] = [1, 2, 3]
        except ReadOnlyDataFrameError:
            pass

        report = contract.get_mutation_report()
        assert report['mutation_attempts'] == 1
        assert 'details' in report
        assert len(report['details']) == 1


class TestIntegration:
    """Integration tests for complete compute node execution"""

    def test_safe_code_execution(self):
        """Safe code should execute successfully"""
        df = pd.DataFrame({
            'a': [1, 2, 3, 4, 5],
            'b': [10, 20, 30, 40, 50]
        })
        contract = ComputeNodeContract(df, input_name="input_df")
        context = contract.get_execution_context()

        # Safe code: creates new DataFrame
        code = """
output_df = input_df.copy()
output_df['sum'] = output_df['a'] + output_df['b']
output_df = output_df[output_df['sum'] > 30]
"""
        exec(code, {'pd': pd}, context)

        output_df = contract.validate_output(context['output_df'])
        assert isinstance(output_df, pd.DataFrame)
        assert 'sum' in output_df.columns
        assert len(output_df) == 3  # Rows where sum > 30

    def test_unsafe_code_blocked(self):
        """Unsafe code should raise ReadOnlyDataFrameError"""
        df = pd.DataFrame({'a': [1, 2, 3]})
        contract = ComputeNodeContract(df, input_name="input_df")
        context = contract.get_execution_context()

        # Unsafe code: tries to mutate input
        code = """
input_df['new'] = [1, 2, 3]
output_df = input_df
"""

        with pytest.raises(ReadOnlyDataFrameError):
            exec(code, {'pd': pd}, context)

    def test_helper_function(self):
        """Helper function should work correctly"""
        df = pd.DataFrame({'a': [1, 2, 3]})
        contract = ComputeNodeContract(df, input_name="input_df")
        context = contract.get_execution_context()

        # Use helper function
        code = """
output_df = create_output()
output_df['new'] = [4, 5, 6]
"""
        exec(code, {'pd': pd}, context)

        output_df = contract.validate_output(context['output_df'])
        assert 'new' in output_df.columns
        assert list(output_df['new']) == [4, 5, 6]


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
