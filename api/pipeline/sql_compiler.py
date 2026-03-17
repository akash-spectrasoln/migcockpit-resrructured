"""
RENAMED: This module was split into two purpose-specific compilers.

For Django node preview (CTE-based):
    from api.pipeline.preview_compiler import SQLCompiler

For migration execution (full ETL compiler):
    from services.migration_service.planner.sql_compiler import <classes>

This file exists only for backward compatibility.
"""
from api.pipeline.preview_compiler import SQLCompiler  # noqa: F401
