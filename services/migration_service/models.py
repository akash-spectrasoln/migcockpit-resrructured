"""
Pydantic Models for Migration Service
Type-safe request/response models
"""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Migration job status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class NodeType(str, Enum):
    """Node type"""
    SOURCE = "source"
    TRANSFORM = "transform"
    DESTINATION = "destination"

class NodeConfig(BaseModel):
    """Node configuration. Uses schema_name (alias 'schema') to avoid shadowing BaseModel.schema."""
    sourceId: Optional[int] = None
    destinationId: Optional[int] = None
    connectionType: Optional[str] = None
    tableName: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema")
    whereClause: Optional[str] = None
    limit: Optional[int] = None
    transformType: Optional[str] = None
    rules: Optional[list[dict[str, Any]]] = None
    mappings: Optional[list[dict[str, Any]]] = None
    conditions: Optional[list[dict[str, Any]]] = None
    operator: Optional[str] = "AND"
    loadMode: Optional[str] = "insert"

class CanvasNode(BaseModel):
    """Canvas node representation"""
    id: str
    type: NodeType
    position: Optional[dict[str, float]] = None
    data: dict[str, Any] = Field(default_factory=dict)
    config: Optional[NodeConfig] = None

class CanvasEdge(BaseModel):
    """Canvas edge representation"""
    id: Optional[str] = None
    source: str
    target: str
    sourceHandle: Optional[str] = None
    targetHandle: Optional[str] = None

class MigrationRequest(BaseModel):
    """Request to execute migration"""
    canvas_id: int = Field(..., description="Canvas ID")
    nodes: list[dict[str, Any]] = Field(..., description="List of nodes")
    edges: list[dict[str, Any]] = Field(..., description="List of edges")
    config: Optional[dict[str, Any]] = Field(
        default_factory=dict,
        description="Migration configuration (chunk_size, etc.)"
    )
    # Optional top-level DB connection payloads (some clients send these outside `config`).
    connection_config: Optional[dict[str, Any]] = Field(None, description="Top-level execution DB connection config")
    destination_configs: Optional[dict[str, Any]] = Field(None, description="Top-level destination configs map")
    connectionConfig: Optional[dict[str, Any]] = Field(None, description="CamelCase alias for connection_config")
    destinationConfigs: Optional[dict[str, Any]] = Field(None, description="CamelCase alias for destination_configs")
    job_id: Optional[str] = Field(None, description="Optional job ID from Django; if provided, used for pipeline and WebSocket broadcasts so frontend receives updates")
    execution_plan: Optional[dict[str, Any]] = Field(None, description="Pre-built execution plan (from Validate or loaded from DB); when set, compilation is skipped")

    class Config:
        json_schema_extra = {
            "example": {
                "canvas_id": 1,
                "nodes": [
                    {
                        "id": "source-1",
                        "type": "source",
                        "data": {"label": "MySQL Source", "config": {"sourceId": 1, "tableName": "users"}}
                    }
                ],
                "edges": [],
                "config": {"chunk_size": 10000}
            }
        }

class MigrationResponse(BaseModel):
    """Response from migration execution"""
    job_id: str = Field(..., description="Unique job identifier")
    status: JobStatus = Field(..., description="Job status")
    message: str = Field(..., description="Status message")

    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "pending",
                "message": "Migration job started. Job ID: 550e8400-e29b-41d4-a716-446655440000"
            }
        }

class NodeProgress(BaseModel):
    """Progress for a specific node"""
    node_id: str
    status: JobStatus
    progress: float = Field(..., ge=0.0, le=100.0, description="Progress percentage")
    message: Optional[str] = None
    rows_processed: Optional[int] = None
    error: Optional[str] = None

class MigrationStatus(BaseModel):
    """Migration job status"""
    job_id: str = Field(..., description="Job identifier")
    status: JobStatus = Field(..., description="Current status")
    progress: float = Field(..., ge=0.0, le=100.0, description="Overall progress percentage")
    current_step: Optional[str] = Field(None, description="Current step description")
    error: Optional[str] = Field(None, description="Error message if failed")
    stats: Optional[dict[str, Any]] = Field(None, description="Statistics")
    node_progress: Optional[list[NodeProgress]] = Field(None, description="Per-node progress")
    current_level: Optional[int] = Field(None, description="Current execution level (1-based)")
    total_levels: Optional[int] = Field(None, description="Total number of execution levels")
    level_status: Optional[str] = Field(None, description="Level status: running or complete")

    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "running",
                "progress": 45.5,
                "current_step": "Transforming data",
                "node_progress": [
                    {
                        "node_id": "source-1",
                        "status": "completed",
                        "progress": 100.0,
                        "rows_processed": 10000
                    }
                ]
            }
        }

class CancelResponse(BaseModel):
    """Response from cancel operation"""
    message: str
    job_id: str
    status: JobStatus

class PipelineConfig(BaseModel):
    """Pipeline execution configuration"""
    chunk_size: int = Field(default=10000, ge=1, le=1000000, description="Chunk size for processing")
    max_workers: int = Field(default=4, ge=1, le=16, description="Maximum parallel workers")
    timeout: int = Field(default=3600, ge=1, description="Timeout in seconds")
    retry_on_failure: bool = Field(default=True, description="Retry on failure")
    max_retries: int = Field(default=3, ge=0, le=10, description="Maximum retry attempts")
