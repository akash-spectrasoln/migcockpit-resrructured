"""
Source connection domain objects.
Pure Python — zero framework imports.
"""
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class SourceType(Enum):
    POSTGRESQL = 'postgresql'
    SQLSERVER  = 'sqlserver'
    MYSQL      = 'mysql'
    ORACLE     = 'oracle'
    HANA       = 'hana'

@dataclass
class Source:
    source_id: int
    name: str
    source_type: SourceType
    schema: str = 'public'
    project_id: Optional[int] = None
    is_active: bool = True
