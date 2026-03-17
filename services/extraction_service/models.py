"""
Pydantic Models for Extraction Service
Type-safe request/response models
"""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class ConnectionType(str, Enum):
    """Database connection type"""
    MYSQL = "mysql"
    ORACLE = "oracle"
    SQLSERVER = "sqlserver"
    POSTGRESQL = "postgresql"

class JobStatus(str, Enum):
    """Extraction job status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class ConnectionConfig(BaseModel):
    """Database connection configuration"""
    host: str = Field(..., description="Database host")
    port: int = Field(..., ge=1, le=65535, description="Database port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    schema_name: Optional[str] = Field(None, description="Schema name (for Oracle, PostgreSQL)", serialization_alias="schema")
    service_name: Optional[str] = Field(None, description="Service name (for Oracle)")
    driver: Optional[str] = Field(None, description="ODBC driver (for SQL Server)")

    class Config:
        populate_by_name = True
        # Allow both 'schema' and 'schema_name' in JSON
        json_encoders = {}

class ExtractionRequest(BaseModel):
    """Request to extract data"""
    source_type: ConnectionType = Field(..., description="Source database type")
    connection_config: Optional[dict[str, Any]] = Field(None, description="Connection configuration (host, port, database, username, password). Required for extraction.")
    table_name: str = Field(..., description="Table name to extract")
    schema_name: Optional[str] = Field(None, description="Schema name", serialization_alias="schema", validation_alias="schema")
    where_clause: Optional[str] = Field(None, description="WHERE clause for filtering (raw SQL fragment)")
    filter_spec: Optional[dict[str, Any]] = Field(None, description="Structured filter (conditions list or dict). Used only when filter uses source table columns, not calculated/created columns.")
    limit: Optional[int] = Field(None, ge=1, description="Row limit")
    columns: Optional[list[str]] = Field(None, description="Specific columns to extract")
    chunk_size: int = Field(default=10000, ge=1, le=1000000, description="Chunk size for extraction")

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "source_type": "mysql",
                "connection_config": {
                    "host": "localhost",
                    "port": 3306,
                    "database": "mydb",
                    "username": "user",
                    "password": "pass"
                },
                "table_name": "users",
                "chunk_size": 10000
            }
        }

class ExtractionResponse(BaseModel):
    """Response from extraction request"""
    job_id: str = Field(..., description="Extraction job identifier")
    status: JobStatus = Field(..., description="Job status")
    message: str = Field(..., description="Status message")

    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "ext-550e8400-e29b-41d4-a716-446655440000",
                "status": "pending",
                "message": "Extraction job started"
            }
        }

class ExtractionStatus(BaseModel):
    """Extraction job status"""
    job_id: str
    status: JobStatus
    progress: float = Field(..., ge=0.0, le=100.0)
    rows_extracted: Optional[int] = None
    current_chunk: Optional[int] = None
    total_chunks: Optional[int] = None
    error: Optional[str] = None
    data_location: Optional[str] = Field(None, description="Location of extracted data")

class TableMetadata(BaseModel):
    """Table metadata"""
    name: str
    schema_name: Optional[str] = Field(None, serialization_alias="schema", validation_alias="schema")
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None

    class Config:
        populate_by_name = True

class ColumnMetadata(BaseModel):
    """Column metadata"""
    name: str
    data_type: str
    nullable: bool
    primary_key: bool = False
    default_value: Optional[str] = None
    max_length: Optional[int] = None

class TableSchemaResponse(BaseModel):
    """Response with table schema"""
    table_name: str
    schema_name: Optional[str] = Field(None, serialization_alias="schema", validation_alias="schema")
    columns: list[ColumnMetadata]
    primary_keys: list[str] = Field(default_factory=list)
    indexes: list[dict[str, Any]] = Field(default_factory=list)

    class Config:
        populate_by_name = True

class TablesListResponse(BaseModel):
    """Response with list of tables"""
    tables: list[TableMetadata]
    total: int
