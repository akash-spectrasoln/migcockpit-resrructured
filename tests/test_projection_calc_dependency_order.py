import re

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MIGRATION_SERVICE_DIR = os.path.join(_ROOT, "services", "migration_service")
if _MIGRATION_SERVICE_DIR not in sys.path:
    sys.path.insert(0, _MIGRATION_SERVICE_DIR)

from planner.sql_compiler import flatten_segment


def test_projection_calculated_columns_are_topologically_sorted():
    """
    Order-of-execution for calculated columns inside a single projection must not
    depend on the order user provides in `calculated_columns`.

    Example:
      c = a + b
      d = c + a
    Even if `d` is listed before `c`, `d` must be compiled with `c` inlined.
    """
    nodes = {
        "p1": {
            "id": "p1",
            "type": "projection",
            "data": {
                "config": {
                    "columns": ["c", "d"],
                    # Intentionally reversed order: d depends on c.
                    "calculated_columns": [
                        {"name": "d", "expression": "c + a"},
                        {"name": "c", "expression": "a + b"},
                    ],
                }
            },
        }
    }

    sql = flatten_segment(
        segment_node_ids=["p1"],
        nodes=nodes,
        edges=[],
        config={},
        upstream_source_or_staging='"public"."t"',
        name_to_technical={"a": "a", "b": "b"},
    )

    # Must compile d successfully and inline c's definition (so b must appear in d expr).
    assert 'AS "d"' in sql

    idx = sql.find('AS "d"')
    assert idx != -1
    last_comma = sql.rfind(",", 0, idx)
    assert last_comma != -1

    # Expression for d is between last comma and 'AS "d"'
    expr_for_d = sql[last_comma + 1 : idx]

    # If c wasn't inlined, you'd likely see the raw token `c` in d's expression.
    # When inlined correctly, d should contain c's body, i.e. `(a + b)`.
    assert "a+b" in expr_for_d.replace(" ", "")
    assert re.search(r"\bc\b", expr_for_d) is None

