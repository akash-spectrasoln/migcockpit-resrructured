"""
ISourceConnector port.
Any database connector (PostgreSQL, SQL Server, MySQL, Oracle, HANA)
must implement this interface.
"""
from abc import ABC, abstractmethod
from typing import Any

from domain.connection.credential import Credential
from domain.pipeline.column import ColumnMetadata


class ISourceConnector(ABC):

    @abstractmethod
    def test_connection(self, credential: Credential) -> dict[str, Any]:
        """
        Test connectivity. Returns {'success': bool, 'message': str}.
        Never raises — always returns a result dict.
        """

    @abstractmethod
    def fetch_tables(self, credential: Credential, schema: str) -> list[str]:
        """Return all table names in the given schema."""

    @abstractmethod
    def fetch_schema(self, credential: Credential, table: str, schema: str) -> list[ColumnMetadata]:
        """Return column metadata for a specific table."""

    @abstractmethod
    def execute_query(self, credential: Credential, sql: str, params: list) -> dict[str, Any]:
        """
        Execute a SQL query and return results.
        Returns {'columns': [...], 'rows': [...], 'row_count': int}.
        """
