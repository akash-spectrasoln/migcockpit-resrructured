"""
DEPRECATED compatibility shim.

Historically some unit tests patch `api.utils.sql_compiler.psycopg2.connect`.
In this codebase `api.utils.sql_compiler` is a module (not a package), and
`psycopg2` is not an actual submodule.

To keep those tests working, we emulate a `psycopg2` submodule by:
1) marking this module as package-like (`__path__`)
2) pre-registering `api.utils.sql_compiler.psycopg2` in `sys.modules`
"""

from __future__ import annotations

import sys
import types

try:
    import psycopg2 as _psycopg2  # type: ignore
except Exception:  # pragma: no cover
    _psycopg2 = types.SimpleNamespace(connect=lambda *args, **kwargs: None)

# Make this module appear "package-like" to importlib/patch
__path__ = []  # type: ignore

_sub_name = f"{__name__}.psycopg2"
if _sub_name not in sys.modules:
    _sub_mod = types.ModuleType(_sub_name)
    setattr(_sub_mod, "connect", getattr(_psycopg2, "connect", None))
    sys.modules[_sub_name] = _sub_mod

# Also expose it as an attribute on the parent module so
# `patch('api.utils.sql_compiler.psycopg2.connect')` can resolve correctly.
psycopg2 = sys.modules[_sub_name]  # type: ignore

# Re-export actual implementation
from api.pipeline.preview_compiler import *  # noqa: F401,F403
