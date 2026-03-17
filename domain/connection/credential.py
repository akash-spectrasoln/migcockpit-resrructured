"""
Credential value object.
Credentials are always encrypted at rest — this object holds the decrypted
in-memory form for use during a request. Never persisted directly.
Pure Python — zero framework imports.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)   # frozen = immutable value object
class Credential:
    host: str
    port: int
    username: str
    password: str           # decrypted — never logged or stored
    database: str
    db_type: str
    connect_timeout: int = 15
    schema: Optional[str] = None
