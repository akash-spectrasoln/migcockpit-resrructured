# Read-Only DataFrame Implementation - Quick Reference

## Overview

Production-safe read-only DataFrame wrapper for compute nodes that prevents accidental mutations while maintaining performance.

## Quick Start

```python
from api.services.readonly_dataframe import ComputeNodeContract

# Create contract
contract = ComputeNodeContract(input_df, input_name="input_df")

# Get execution context
context = contract.get_execution_context()

# Execute user code
exec(user_code, globals(), context)

# Validate output
output_df = contract.validate_output(context['output_df'])
```

## Key Files

- `readonly_dataframe.py` - Core implementation
- `compute_node_design.md` - Complete design documentation
- `compute_node_example.py` - Example usage
- `test_readonly_dataframe.py` - Unit tests

## Usage Pattern

**✅ CORRECT:**
```python
output_df = input_df.copy()
output_df['new_col'] = values
```

**❌ WRONG:**
```python
input_df['new_col'] = values  # Raises ReadOnlyDataFrameError
```

## Benefits

- ✅ Prevents accidental input mutations
- ✅ Copy-on-write (performance optimized)
- ✅ Thread-safe for parallel execution
- ✅ Schema validation support
- ✅ Clear error messages

## See Also

- Full documentation: `compute_node_design.md`
- Example implementation: `compute_node_example.py`
