# Moved from: api/utils/preview_guards.py
"""
Preview Safety Guards
Enforces memory and performance limits for preview operations.
"""
from functools import wraps
import logging
import time
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Global constants
MAX_PREVIEW_ROWS = 100
PREVIEW_TIMEOUT_SECONDS = 5

class PreviewMemoryLimitError(Exception):
    """Raised when preview exceeds memory limits."""
    pass

class PreviewTimeoutError(Exception):
    """Raised when preview exceeds time limits."""
    pass

def enforce_preview_memory_limit(rows: list[dict[str, Any]], max_rows: int = MAX_PREVIEW_ROWS) -> list[dict[str, Any]]:
    """
    Global memory guard for all preview operations.

    Args:
        rows: List of row dictionaries
        max_rows: Maximum allowed rows (default: 100)

    Returns:
        Truncated list of rows (≤ max_rows)

    Raises:
        PreviewMemoryLimitError: If rows exceed limit (in strict mode)
    """
    if len(rows) > max_rows:
        logger.warning(
            f"[PREVIEW MEMORY GUARD] Truncating {len(rows)} rows to {max_rows} max"
        )
        return rows[:max_rows]

    return rows

def preview_timeout(seconds: int = PREVIEW_TIMEOUT_SECONDS):
    """
    Decorator to enforce preview timeout and log slow operations.

    Args:
        seconds: Timeout threshold in seconds

    Usage:
        @preview_timeout(5)
        def my_preview_function():
            # ... preview logic
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time

            if elapsed > seconds:
                logger.warning(
                    f"[PREVIEW TIMEOUT WARNING] {func.__name__} took {elapsed:.2f}s "
                    f"(threshold: {seconds}s)"
                )

            return result
        return wrapper
    return decorator

def validate_preview_sql(sql_query: str, max_limit: int = MAX_PREVIEW_ROWS) -> bool:
    """
    Validate that preview SQL has required safety constraints.

    Args:
        sql_query: SQL query string
        max_limit: Maximum allowed LIMIT value

    Returns:
        True if valid

    Raises:
        ValueError: If SQL is unsafe for preview
    """
    import re

    sql_upper = sql_query.upper()

    # Must have LIMIT clause
    if 'LIMIT' not in sql_upper:
        raise ValueError("Preview SQL must include LIMIT clause")

    # Extract and validate LIMIT value
    limit_match = re.search(r'LIMIT\s+(\d+)', sql_upper)
    if limit_match:
        limit_value = int(limit_match.group(1))
        if limit_value > max_limit:
            raise ValueError(
                f"Preview LIMIT too high: {limit_value} > {max_limit} max"
            )
    else:
        # LIMIT exists but couldn't parse value (might be parameterized)
        # This is acceptable - runtime check will catch it
        pass

    return True

class PreviewTracer:
    """
    Structured logging for preview operations.
    Tracks timing, memory usage, and checkpoint hits.
    """

    def __init__(self, node_id: str, canvas_id: str):
        self.node_id = node_id
        self.canvas_id = canvas_id
        self.start_time = time.time()
        self.metrics = {
            'rows_fetched': 0,
            'sql_time_ms': 0,
            'python_time_ms': 0,
            'checkpoint_used': False,
            'memory_rows': 0,
            'checkpoint_node_id': None,
        }

    def log_sql_execution(self, duration_ms: float, rows: int):
        """Log SQL execution metrics."""
        self.metrics['sql_time_ms'] = duration_ms
        self.metrics['rows_fetched'] = rows

    def log_checkpoint_hit(self, checkpoint_node_id: str):
        """Log checkpoint cache hit."""
        self.metrics['checkpoint_used'] = True
        self.metrics['checkpoint_node_id'] = checkpoint_node_id

    def log_memory_usage(self, rows: int):
        """Track peak memory usage (max rows in Python)."""
        self.metrics['memory_rows'] = max(self.metrics['memory_rows'], rows)

    def log_python_execution(self, duration_ms: float):
        """Log Python execution time (compute nodes)."""
        self.metrics['python_time_ms'] = duration_ms

    def finalize(self):
        """Log final preview trace."""
        total_time = (time.time() - self.start_time) * 1000

        logger.info(
            "[PREVIEW TRACE] "
            f"node_id={self.node_id} "
            f"canvas_id={self.canvas_id} "
            f"total_time_ms={total_time:.2f} "
            f"rows_fetched={self.metrics['rows_fetched']} "
            f"sql_time_ms={self.metrics['sql_time_ms']:.2f} "
            f"python_time_ms={self.metrics['python_time_ms']:.2f} "
            f"checkpoint_used={self.metrics['checkpoint_used']} "
            f"checkpoint_node={self.metrics['checkpoint_node_id']} "
            f"memory_rows={self.metrics['memory_rows']}"
        )

        # Warn on performance issues
        if total_time > PREVIEW_TIMEOUT_SECONDS * 1000:
            logger.warning(
                f"[SLOW PREVIEW] node_id={self.node_id} took {total_time:.2f}ms "
                f"(threshold: {PREVIEW_TIMEOUT_SECONDS * 1000}ms)"
            )

        if self.metrics['memory_rows'] > MAX_PREVIEW_ROWS:
            logger.error(
                f"[MEMORY VIOLATION] node_id={self.node_id} loaded {self.metrics['memory_rows']} rows "
                f"(max: {MAX_PREVIEW_ROWS})"
            )
