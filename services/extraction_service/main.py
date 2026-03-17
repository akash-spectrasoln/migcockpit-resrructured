"""
FastAPI Data Extraction Service
Handles data extraction from various source databases (MySQL, Oracle, SQL Server)
"""

import logging
from typing import Any, Optional

from connectors.mysql import MySQLConnector
from connectors.oracle import OracleConnector
from connectors.postgresql import PostgreSQLConnector
from connectors.sqlserver import SQLServerConnector
from fastapi import BackgroundTasks, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from models import (
    ConnectionType,
    ExtractionRequest,
    ExtractionResponse,
    ExtractionStatus,
    JobStatus,
)
import uvicorn
from workers.extraction_worker import ExtractionWorker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Data Extraction Service",
    description="Service for extracting data from source databases",
    version="1.0.0",
    docs_url="/docs"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job storage (in production, use Redis)
extraction_jobs: dict[str, dict[str, Any]] = {}

def _sanitize_schema(value) -> Optional[str]:
    """Ensure schema is a string or None. Guards against Pydantic v2 deprecated .schema returning a dict."""
    if value is None:
        return None
    if isinstance(value, str):
        return value if value.strip() else None
    # If it's a dict (e.g. Pydantic deprecated schema), discard it
    logger.warning(f"Ignoring non-string schema value: {type(value).__name__}")
    return None

def _log_request_metadata(endpoint: str, request_data: dict[str, Any], connection_config: dict[str, Any]):
    """Log detailed request metadata for debugging. Masks password."""
    def _slice_str(s: str, n: int) -> str:
        if len(s) <= n:
            return s
        chars: list[str] = []
        for i in range(min(n, len(s))):
            chars.append(s[i])
        return "".join(chars)

    top_fields = {k: (type(v).__name__, _slice_str(repr(v), 100) if k != 'connection_config' else '...') for k, v in request_data.items()}
    logger.info(f"[{endpoint}] ━━━ REQUEST METADATA ━━━")
    logger.info(f"[{endpoint}] Top-level fields: {list(request_data.keys())}")
    for k, (vtype, vrepr) in top_fields.items():
        if k == 'connection_config':
            continue
        status_icon = '✓' if vrepr not in ("None", "''", '""', '{}', '[]') else '✗ MISSING'
        logger.info(f"[{endpoint}]   {status_icon} {k} = {vrepr} ({vtype})")

    # Log connection_config fields with missing detection
    expected_conn_fields = ['hostname', 'host', 'port', 'database', 'user', 'username', 'password', 'schema', 'service_name']
    logger.info(f"[{endpoint}] connection_config fields:")
    for field in expected_conn_fields:
        val = connection_config.get(field)
        if field == 'password':
            display = '****' if val else 'None'
        else:
            display = _slice_str(repr(val), 80)
        status_icon = '✓' if val is not None and val != '' else '✗ MISSING'
        logger.info(f"[{endpoint}]   {status_icon} {field} = {display} ({type(val).__name__})")

    # Flag any extra/unexpected fields in connection_config
    extra = set(connection_config.keys()) - set(expected_conn_fields)
    if extra:
        logger.info(f"[{endpoint}]   ⚠ Extra fields in connection_config: {extra}")
    logger.info(f"[{endpoint}] ━━━━━━━━━━━━━━━━━━━━━━━━")

def get_connector(source_type: ConnectionType, connection_config: dict[str, Any]):
    """Factory function to get appropriate database connector"""
    # Convert ConnectionConfig to dict format expected by connectors
    config_dict = {
        'hostname': connection_config.get('host') or connection_config.get('hostname'),
        'port': connection_config.get('port'),
        'database': connection_config.get('database'),
        'user': connection_config.get('username') or connection_config.get('user'),
        'password': connection_config.get('password'),
        'schema': connection_config.get('schema') or connection_config.get('schema_name'),
        'service_name': connection_config.get('service_name'),
    }

    if source_type == ConnectionType.POSTGRESQL:
        return PostgreSQLConnector(config_dict)
    elif source_type == ConnectionType.MYSQL:
        return MySQLConnector(config_dict)
    elif source_type == ConnectionType.ORACLE:
        return OracleConnector(config_dict)
    elif source_type == ConnectionType.SQLSERVER:
        return SQLServerConnector(config_dict)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")

def _validate_connection_config(connection_config: Optional[dict[str, Any]]) -> dict[str, Any]:
    """Validate connection_config has required keys. Raises HTTPException 400 if invalid."""
    required = ["host", "port", "database", "username", "password"]
    if not connection_config or not isinstance(connection_config, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="connection_config is required and must be an object with: host, port, database, username, password. "
                   "Provide source connection details (e.g. from Django API by source_id)."
        )
    missing = [k for k in required if not connection_config.get(k)]
    if missing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"connection_config missing required fields: {', '.join(missing)}. "
                   "Provide host, port, database, username, password."
        )
    port = connection_config.get("port")
    if port is not None:
        try:
            p = int(port)
            if p < 1 or p > 65535:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="connection_config.port must be 1-65535")
        except (TypeError, ValueError):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="connection_config.port must be an integer")
    return connection_config

@app.post("/extract", response_model=ExtractionResponse, status_code=status.HTTP_202_ACCEPTED)
async def start_extraction(
    request: ExtractionRequest,
    background_tasks: BackgroundTasks
):
    """
    Start data extraction from source database
    Returns immediately with job_id, extraction runs in background
    """
    try:
        connection_config = _validate_connection_config(request.connection_config)
        _log_request_metadata("/extract", {
            "source_type": request.source_type,
            "table_name": request.table_name,
            "schema_name": request.schema_name,
            "chunk_size": request.chunk_size,
            "where_clause": request.where_clause,
            "filter_spec": getattr(request, 'filter_spec', None),
            "limit": request.limit,
            "columns": request.columns,
        }, connection_config or {})
        if not request.table_name or not request.table_name.strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="table_name is required and must be non-empty."
            )

        import uuid
        job_id = str(uuid.uuid4())

        # Initialize job status
        extraction_jobs[job_id] = {
            "status": "pending",
            "progress": 0.0,
            "rows_extracted": 0,
            "total_rows": None,
            "error": None
        }

        # Start extraction in background
        background_tasks.add_task(
            execute_extraction,
            job_id,
            request.source_type,
            connection_config,
            request.table_name,
            request.schema_name,
            request.chunk_size,
            request.where_clause,
            getattr(request, "filter_spec", None)
        )

        logger.info(f"Extraction job {job_id} started for {request.source_type} table {request.table_name}")

        return ExtractionResponse(
            job_id=job_id,
            status=JobStatus.PENDING,
            message=f"Extraction job started. Job ID: {job_id}"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting extraction: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start extraction: {e!s}"
        )

async def execute_extraction(
    job_id: str,
    source_type: ConnectionType,
    connection_config: Any,
    table_name: str,
    schema: Optional[str],
    chunk_size: int,
    where_clause: Optional[str],
    filter_spec: Optional[dict[str, Any]] = None
):
    """Execute extraction in background. filter_spec is used when pushdown applies (source columns only)."""
    try:
        extraction_jobs[job_id]["status"] = "running"

        connector = get_connector(source_type, connection_config)
        worker = ExtractionWorker(connector, chunk_size)

        # Prefer structured filter_spec (pushdown from orchestrator); fallback to raw where_clause
        filters = None
        if filter_spec:
            filters = {"filter_spec": filter_spec}
        elif where_clause:
            filters = {"where_clause": where_clause}
        result = await worker.extract_data(
            table_name=table_name,
            schema=schema,
            filters=filters,
            progress_callback=lambda progress, rows: update_progress(job_id, progress, rows)
        )

        extraction_jobs[job_id].update({
            "status": "completed",
            "progress": 100.0,
            "rows_extracted": result["rows_extracted"],
            "total_rows": result.get("total_rows"),
            "data": result.get("data", []),
        })

        logger.info(f"Extraction job {job_id} completed successfully, %s rows stored for retrieval", result.get("rows_extracted", 0))

    except Exception as e:
        logger.error(f"Extraction job {job_id} failed: {e}")
        extraction_jobs[job_id].update({
            "status": "failed",
            "error": str(e)
        })

def update_progress(job_id: str, progress: float, rows_extracted: int):
    """Update extraction progress"""
    if job_id in extraction_jobs:
        extraction_jobs[job_id]["progress"] = progress
        extraction_jobs[job_id]["rows_extracted"] = rows_extracted

@app.get("/extract/{job_id}/status", response_model=ExtractionStatus)
async def get_extraction_status(job_id: str):
    """Get extraction job status"""
    if job_id not in extraction_jobs:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )

    job = extraction_jobs[job_id]
    return ExtractionStatus(
        job_id=job_id,
        status=JobStatus(job["status"]),
        progress=job["progress"],
        rows_extracted=job["rows_extracted"],
        total_rows=job.get("total_rows"),
        error=job.get("error")
    )

@app.get("/extract/{job_id}/data")
async def get_extraction_data(job_id: str):
    """Get extracted data for a completed job. Used by migration service to pass rows to destination loader."""
    if job_id not in extraction_jobs:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    job = extraction_jobs[job_id]
    if job["status"] != "completed":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job {job_id} is not completed (status={job['status']}). Data is only available when extraction has completed."
        )
    data = job.get("data", [])
    return {"data": data, "rows": len(data)}

@app.post("/metadata/tables")
async def get_tables(request: dict[str, Any]):
    """
    Get list of tables for a database connection with pagination.
    Request body:
    {
        "db_type": "postgresql" | "mysql" | "oracle" | "sqlserver",
        "connection_config": {
            "hostname": str,
            "port": int,
            "database": str,
            "user": str,
            "password": str,
            "schema": str (optional),
            "service_name": str (optional, for Oracle)
        },
        "schema": str (optional),
        "search": str (optional),
        "limit": int (default 100),
        "cursor": str (optional, for pagination)
    }
    """
    try:
        # Accept both db_type and connection_type (callers use either); default postgresql if missing/empty
        db_type = (request.get("db_type") or request.get("connection_type") or "").strip().lower()
        if not db_type:
            db_type = "postgresql"
        connection_config = request.get("connection_config", {})
        schema = request.get("schema")
        search = request.get("search") or ""
        search = search.strip() if isinstance(search, str) else ""
        limit = int(request.get("limit", 100))
        cursor = request.get("cursor")
        # Sanitize schema values to prevent Pydantic v2 deprecated dict leaking through
        schema = _sanitize_schema(schema)
        _log_request_metadata("/metadata/tables", request, connection_config)

        if db_type not in ["postgresql", "mysql", "oracle", "sqlserver"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported or missing database type: {db_type!r}. Expected db_type or connection_type: postgresql, mysql, oracle, sqlserver."
            )

        # Map db_type to ConnectionType enum
        type_mapping = {
            "postgresql": ConnectionType.POSTGRESQL,
            "mysql": ConnectionType.MYSQL,
            "oracle": ConnectionType.ORACLE,
            "sqlserver": ConnectionType.SQLSERVER
        }

        connection_type = type_mapping[db_type]

        # Create connector config dict
        connector_config_dict = {
            'host': connection_config.get('hostname') or connection_config.get('host'),
            'port': connection_config.get('port'),
            'database': connection_config.get('database'),
            'username': connection_config.get('user') or connection_config.get('username'),
            'password': connection_config.get('password'),
            'schema': connection_config.get('schema') or schema,
            'service_name': connection_config.get('service_name')
        }

        connector = get_connector(connection_type, connector_config_dict)

        # Run synchronous list_tables in thread pool
        import asyncio
        loop = asyncio.get_event_loop()
        tables = await loop.run_in_executor(
            None,
            connector.list_tables,
            schema or connection_config.get('schema'),
            search if search else None,
            limit,
            cursor
        )

        # Determine if there are more tables
        has_more = len(tables) > limit
        if has_more:
            tables = tables[:limit]
            next_cursor = tables[-1]['table_name'] if tables else None
        else:
            next_cursor = None

        # Return tables directly - no need for Pydantic roundtrip
        # (Pydantic v2's .schema property is deprecated and returns a dict, not the field value)
        return {
            "tables": [{"schema": table.get('schema'), "table_name": table.get('table_name')} for table in tables],
            "next_cursor": next_cursor,
            "has_more": has_more,
            "count": len(tables),
            "total": len(tables)  # For compatibility with TablesListResponse
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching tables: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch tables: {e!s}"
        )

@app.post("/metadata/columns")
async def get_columns(request: dict[str, Any]):
    """
    Get columns/fields for a table.
    Request body:
    {
        "db_type": "postgresql" | "mysql" | "oracle" | "sqlserver",
        "connection_config": {
            "hostname": str,
            "port": int,
            "database": str,
            "user": str,
            "password": str,
            "schema": str (optional),
            "service_name": str (optional, for Oracle)
        },
        "table_name": str,
        "schema": str (optional)
    }
    """
    try:
        db_type = request.get("db_type", "").lower()
        connection_config = request.get("connection_config", {})
        table_name = request.get("table_name")
        schema = request.get("schema")

        if not table_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="table_name is required"
            )

        if db_type not in ["postgresql", "mysql", "oracle", "sqlserver"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported database type: {db_type}"
            )

        # Map db_type to ConnectionType enum
        type_mapping = {
            "postgresql": ConnectionType.POSTGRESQL,
            "mysql": ConnectionType.MYSQL,
            "oracle": ConnectionType.ORACLE,
            "sqlserver": ConnectionType.SQLSERVER
        }

        connection_type = type_mapping[db_type]

        # Create connector config dict
        connector_config_dict = {
            'host': connection_config.get('hostname') or connection_config.get('host'),
            'port': connection_config.get('port'),
            'database': connection_config.get('database'),
            'username': connection_config.get('user') or connection_config.get('username'),
            'password': connection_config.get('password'),
            'schema': connection_config.get('schema') or schema,
            'service_name': connection_config.get('service_name')
        }

        connector = get_connector(connection_type, connector_config_dict)

        # Run synchronous get_table_schema in thread pool
        import asyncio
        loop = asyncio.get_event_loop()
        schema_result = await loop.run_in_executor(
            None,
            connector.get_table_schema,
            table_name,
            schema or connection_config.get('schema')
        )

        # Convert columns to standard format
        columns = schema_result.get('columns', [])
        formatted_columns = []
        for col in columns:
            # Handle both dict and list formats from different connectors
            if isinstance(col, dict):
                formatted_columns.append({
                    'name': str(col.get('COLUMN_NAME') or col.get('name') or ''),
                    'data_type': str(col.get('DATA_TYPE') or col.get('data_type') or ''),
                    'nullable': col.get('IS_NULLABLE') == 'YES' if isinstance(col.get('IS_NULLABLE'), str) else (col.get('IS_NULLABLE') if isinstance(col.get('IS_NULLABLE'), bool) else col.get('nullable', False)),
                    'default_value': str(col.get('COLUMN_DEFAULT') or col.get('column_default') or col.get('default_value') or '') if col.get('COLUMN_DEFAULT') or col.get('column_default') or col.get('default_value') else None,
                    'max_length': int(col.get('CHARACTER_MAXIMUM_LENGTH') or col.get('character_maximum_length') or col.get('max_length') or 0) if col.get('CHARACTER_MAXIMUM_LENGTH') or col.get('character_maximum_length') or col.get('max_length') else None
                })
            elif isinstance(col, (list, tuple)) and len(col) >= 2:
                # Handle tuple/list format: (name, type, nullable, default, max_length)
                formatted_columns.append({
                    'name': str(col[0]) if len(col) > 0 else '',
                    'data_type': str(col[1]) if len(col) > 1 else '',
                    'nullable': col[2] == 'YES' if len(col) > 2 and isinstance(col[2], str) else (col[2] if len(col) > 2 else False),
                    'default_value': str(col[3]) if len(col) > 3 and col[3] else None,
                    'max_length': int(col[4]) if len(col) > 4 and col[4] else None
                })

        if not formatted_columns:
            logger.warning(f"No columns found for table {table_name} in schema {schema}")
            # Return empty columns instead of error - table might exist but have no columns
            # or might be a view/materialized view that we can't query

        return {
            "table_name": table_name,
            "schema": schema_result.get('schema'),
            "columns": formatted_columns
        }

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Error fetching table columns for {table_name} (schema: {schema}): {e}\n{error_trace}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch table columns for {table_name}: {e!s}"
        )

@app.post("/metadata/columns/bulk")
async def get_columns_bulk(request: dict[str, Any]):
    """
    Get columns/fields for multiple tables in bulk.
    Request body:
    {
        "db_type": "postgresql" | "mysql" | "oracle" | "sqlserver",
        "connection_config": {
            "hostname": str,
            "port": int,
            "database": str,
            "user": str,
            "password": str,
            "schema": str (optional),
            "service_name": str (optional, for Oracle)
        },
        "tables": [
            {"table_name": str, "schema": str (optional)},
            ...
        ],
        "schema": str (optional, default schema for all tables)
    }
    """
    try:
        db_type = request.get("db_type", "").lower()
        connection_config = request.get("connection_config", {})
        tables = request.get("tables", [])
        default_schema = request.get("schema")
        _log_request_metadata("/metadata/columns/bulk", {
            "db_type": db_type,
            "tables_count": len(tables),
            "schema": default_schema,
            "first_table": tables[0] if tables else None,
        }, connection_config)

        if not tables:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="tables list is required"
            )

        if db_type not in ["postgresql", "mysql", "oracle", "sqlserver"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported database type: {db_type}"
            )

        # Map db_type to ConnectionType enum
        type_mapping = {
            "postgresql": ConnectionType.POSTGRESQL,
            "mysql": ConnectionType.MYSQL,
            "oracle": ConnectionType.ORACLE,
            "sqlserver": ConnectionType.SQLSERVER
        }

        connection_type = type_mapping[db_type]

        # Create connector config dict
        connector_config_dict = {
            'host': connection_config.get('hostname') or connection_config.get('host'),
            'port': connection_config.get('port'),
            'database': connection_config.get('database'),
            'username': connection_config.get('user') or connection_config.get('username'),
            'password': connection_config.get('password'),
            'schema': connection_config.get('schema') or default_schema,
            'service_name': connection_config.get('service_name')
        }

        connector = get_connector(connection_type, connector_config_dict)

        # Fetch columns for all tables in parallel
        import asyncio
        loop = asyncio.get_event_loop()

        async def fetch_table_columns(table_info: dict[str, Any]):
            """Fetch columns for a single table"""
            try:
                table_name = table_info.get('table_name')
                schema = _sanitize_schema(table_info.get('schema')) or _sanitize_schema(default_schema) or _sanitize_schema(connection_config.get('schema'))

                # Run synchronous get_table_schema in thread pool
                schema_result = await loop.run_in_executor(
                    None,
                    connector.get_table_schema,
                    table_name,
                    schema
                )

                # Convert columns to standard format
                columns = schema_result.get('columns', [])
                formatted_columns = []
                for col in columns:
                    if isinstance(col, dict):
                        formatted_columns.append({
                            'name': str(col.get('COLUMN_NAME') or col.get('name') or ''),
                            'data_type': str(col.get('DATA_TYPE') or col.get('data_type') or ''),
                            'nullable': col.get('IS_NULLABLE') == 'YES' if isinstance(col.get('IS_NULLABLE'), str) else (col.get('IS_NULLABLE') if isinstance(col.get('IS_NULLABLE'), bool) else col.get('nullable', False)),
                            'default_value': str(col.get('COLUMN_DEFAULT') or col.get('column_default') or col.get('default_value') or '') if col.get('COLUMN_DEFAULT') or col.get('column_default') or col.get('default_value') else None,
                            'max_length': int(col.get('CHARACTER_MAXIMUM_LENGTH') or col.get('character_maximum_length') or col.get('max_length') or 0) if col.get('CHARACTER_MAXIMUM_LENGTH') or col.get('character_maximum_length') or col.get('max_length') else None
                        })
                    elif isinstance(col, (list, tuple)) and len(col) >= 2:
                        formatted_columns.append({
                            'name': str(col[0]) if len(col) > 0 else '',
                            'data_type': str(col[1]) if len(col) > 1 else '',
                            'nullable': col[2] == 'YES' if len(col) > 2 and isinstance(col[2], str) else (col[2] if len(col) > 2 else False),
                            'default_value': str(col[3]) if len(col) > 3 and col[3] else None,
                            'max_length': int(col[4]) if len(col) > 4 and col[4] else None
                        })

                return {
                    "table_name": table_name,
                    "schema": schema_result.get('schema') or schema,
                    "columns": formatted_columns,
                    "success": True
                }
            except Exception as e:
                logger.warning(f"Failed to fetch columns for table {table_info.get('table_name')}: {e!s}")
                return {
                    "table_name": table_info.get('table_name'),
                    "schema": table_info.get('schema') or default_schema,
                    "columns": [],
                    "success": False,
                    "error": str(e)
                }

        # Fetch all tables in parallel (with concurrency limit)
        semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent requests

        async def fetch_with_semaphore(table_info):
            async with semaphore:
                return await fetch_table_columns(table_info)

        # Execute all fetches in parallel
        gathered = await asyncio.gather(*[fetch_with_semaphore(table) for table in tables])

        from typing import cast
        results: list[dict[str, Any]] = [cast(dict[str, Any], r) for r in gathered]

        return {
            "results": results,
            "total": len(results),
            "successful": sum(1 for r in results if r.get('success', False)),
            "failed": sum(1 for r in results if not r.get('success', False))
        }

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Error in bulk column fetching: {e}\n{error_trace}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch columns in bulk: {e!s}"
        )

@app.post("/test-connection")
async def test_connection(connection_config: dict[str, Any]):
    """
    Test database connection for source databases (PostgreSQL, MySQL, Oracle, SQL Server)
    Returns success status and error message if failed
    """
    try:
        source_type = connection_config.get("db_type", "").lower()

        if source_type not in ["postgresql", "mysql", "oracle", "sqlserver"]:
            return {
                "success": False,
                "message": f"Unsupported source type: {source_type}",
                "error": "Supported types: postgresql, mysql, oracle, sqlserver"
            }

        # Map source type to ConnectionType enum
        type_mapping = {
            "postgresql": ConnectionType.POSTGRESQL,
            "mysql": ConnectionType.MYSQL,
            "oracle": ConnectionType.ORACLE,
            "sqlserver": ConnectionType.SQLSERVER
        }

        connection_type = type_mapping[source_type]

        # Get connector and test connection
        # Note: connectors are synchronous, so we run in thread pool
        import asyncio
        # Convert dict to ConnectionConfig-like dict for connector
        config_obj = connection_config
        connector = get_connector(connection_type, config_obj)

        # Run synchronous test_connection in thread pool
        loop = asyncio.get_event_loop()
        test_result = await loop.run_in_executor(None, connector.test_connection)

        if test_result:
            return {
                "success": True,
                "message": f"Successfully connected to {source_type} database"
            }
        else:
            return {
                "success": False,
                "message": f"Failed to connect to {source_type} database",
                "error": "Connection test returned False"
            }

    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return {
            "success": False,
            "message": f"Connection test failed: {e!s}",
            "error": str(e)
        }

@app.post("/table-data")
async def get_table_data(request: dict[str, Any]):
    """
    Get table data with pagination.
    Request body:
    {
        "db_type": "postgresql" | "mysql" | "oracle" | "sqlserver",
        "connection_config": {
            "hostname": str,
            "port": int,
            "database": str,
            "user": str,
            "password": str,
            "schema": str (optional),
            "service_name": str (optional, for Oracle)
        },
        "table_name": str,
        "schema": str (optional),
        "page": int (default 1),
        "page_size": int (default 50)
    }
    """
    try:
        db_type = request.get("db_type", "").lower()
        connection_config = request.get("connection_config", {})
        table_name = request.get("table_name")
        schema = _sanitize_schema(request.get("schema"))
        page = int(request.get("page", 1))
        page_size = int(request.get("page_size", 50))
        _log_request_metadata("/table-data", {
            "db_type": db_type,
            "table_name": table_name,
            "schema": schema,
            "page": page,
            "page_size": page_size,
        }, connection_config)

        if not table_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="table_name is required"
            )

        if db_type not in ["postgresql", "mysql", "oracle", "sqlserver"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported database type: {db_type}"
            )

        # Map db_type to ConnectionType enum
        type_mapping = {
            "postgresql": ConnectionType.POSTGRESQL,
            "mysql": ConnectionType.MYSQL,
            "oracle": ConnectionType.ORACLE,
            "sqlserver": ConnectionType.SQLSERVER
        }

        connection_type = type_mapping[db_type]

        # Create connector config dict
        connector_config_dict = {
            'host': connection_config.get('hostname') or connection_config.get('host'),
            'port': connection_config.get('port'),
            'database': connection_config.get('database'),
            'username': connection_config.get('user') or connection_config.get('username'),
            'password': connection_config.get('password'),
            'schema': connection_config.get('schema') or schema,
            'service_name': connection_config.get('service_name')
        }

        connector = get_connector(connection_type, connector_config_dict)

        # Run synchronous get_table_data in thread pool
        import asyncio
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            connector.get_table_data,
            table_name,
            schema or connection_config.get('schema'),
            page,
            page_size
        )

        return result

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Error fetching table data: {e}\n{error_trace}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch table data: {e!s}"
        )

@app.post("/metadata/filter")
async def execute_filter(request: dict[str, Any]):
    """
    Execute filter conditions on a table and return filtered results.
    Request body:
    {
        "db_type": "postgresql" | "mysql" | "oracle" | "sqlserver",
        "connection_config": {
            "hostname": str,
            "port": int,
            "database": str,
            "user": str,
            "password": str,
            "schema": str (optional),
            "service_name": str (optional, for Oracle)
        },
        "table_name": str,
        "schema": str (optional),
        "filters": {
            "type": "logical",
            "operator": "AND" | "OR",
            "expressions": [
                {
                    "type": "condition",
                    "column": str,
                    "operator": str,
                    "value": any
                }
            ]
        },
        "page": int (default 1),
        "page_size": int (default 50)
    }
    """
    try:
        db_type = request.get("db_type", "").lower()
        connection_config = request.get("connection_config", {})
        table_name = request.get("table_name")
        schema = request.get("schema")
        filters = request.get("filters", {})
        page = int(request.get("page", 1))
        page_size = int(request.get("page_size", 50))

        logger.info(f"[extraction_service] /metadata/filter request: db_type={db_type}, table_name={table_name}, schema={schema}, page={page}, page_size={page_size}")
        logger.info(f"[extraction_service] /metadata/filter filters spec: {filters}")

        if not table_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="table_name is required"
            )

        if db_type not in ["postgresql", "mysql", "oracle", "sqlserver"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported database type: {db_type}"
            )

        # Map db_type to ConnectionType enum
        type_mapping = {
            "postgresql": ConnectionType.POSTGRESQL,
            "mysql": ConnectionType.MYSQL,
            "oracle": ConnectionType.ORACLE,
            "sqlserver": ConnectionType.SQLSERVER
        }

        connection_type = type_mapping[db_type]

        # Create connector config dict
        connector_config_dict = {
            'host': connection_config.get('hostname') or connection_config.get('host'),
            'port': connection_config.get('port'),
            'database': connection_config.get('database'),
            'username': connection_config.get('user') or connection_config.get('username'),
            'password': connection_config.get('password'),
            'schema': connection_config.get('schema') or schema,
            'service_name': connection_config.get('service_name')
        }

        # DEBUG: Log source connection details (before fetch) - excludes password
        logger.info(
            "[extraction_service] /metadata/filter source connection => "
            f"hostname={connector_config_dict.get('host')!r} "
            f"port={connector_config_dict.get('port')!r} "
            f"database={connector_config_dict.get('database')!r} "
            f"user={connector_config_dict.get('username')!r} "
            f"schema={connector_config_dict.get('schema')!r} "
            f"db_type={db_type!r}"
        )

        connector = get_connector(connection_type, connector_config_dict)

        # Run synchronous execute_filter in thread pool
        import asyncio
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            connector.execute_filter,
            table_name,
            schema or connection_config.get('schema'),
            filters,
            page,
            page_size
        )

        return result

    except ValueError as e:
        # Column validation failed (e.g. column does not exist in table)
        logger.warning(f"Filter validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Error executing filter: {e}\n{error_trace}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute filter: {e!s}"
        )

@app.post("/aggregate")
async def execute_aggregate(request: dict[str, Any]):
    """
    Execute an aggregate query on a table.
    Request body:
    {
        "db_type": "postgresql" | "mysql" | "oracle" | "sqlserver",
        "connection_config": { ... },
        "table_name": str,
        "schema": str (optional),
        "select_clause": str,          # e.g. '"region", SUM("amount") AS "total_amount"'
        "group_by_columns": [str],     # e.g. ["region"]
        "filters": [...] (optional),   # legacy filter list
        "page": int (default 1),
        "page_size": int (default 50)
    }
    """
    try:
        db_type = request.get("db_type", "").lower()
        connection_config = request.get("connection_config", {})
        table_name = request.get("table_name")
        schema = _sanitize_schema(request.get("schema"))
        select_clause = request.get("select_clause", "")
        group_by_columns = request.get("group_by_columns", [])
        filters_raw = request.get("filters", [])
        page = int(request.get("page", 1))
        page_size = int(request.get("page_size", 50))

        logger.info(
            f"[extraction_service] /aggregate request: db_type={db_type}, "
            f"table={table_name}, schema={schema}, "
            f"select_clause={select_clause}, "
            f"group_by={group_by_columns}, "
            f"page={page}, page_size={page_size}"
        )

        if not table_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="table_name is required"
            )

        if not select_clause:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="select_clause is required for aggregate queries"
            )

        if db_type not in ["postgresql", "mysql", "oracle", "sqlserver"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported database type: {db_type}"
            )

        # Map db_type to ConnectionType enum
        type_mapping = {
            "postgresql": ConnectionType.POSTGRESQL,
            "mysql": ConnectionType.MYSQL,
            "oracle": ConnectionType.ORACLE,
            "sqlserver": ConnectionType.SQLSERVER,
        }

        connection_type = type_mapping[db_type]

        # Create connector config dict
        connector_config_dict = {
            "host": connection_config.get("hostname") or connection_config.get("host"),
            "port": connection_config.get("port"),
            "database": connection_config.get("database"),
            "username": connection_config.get("user") or connection_config.get("username"),
            "password": connection_config.get("password"),
            "schema": connection_config.get("schema") or schema,
            "service_name": connection_config.get("service_name"),
        }

        connector = get_connector(connection_type, connector_config_dict)

        # Convert legacy filter list into a filter_spec dict the connector understands
        filter_spec = None
        if filters_raw and len(filters_raw) > 0:
            # Legacy format from pipeline: list of {column, operator, value, logicalOperator}
            filter_spec = filters_raw  # connector's _build_postgresql_where handles both formats

        # Run synchronous execute_aggregate in thread pool
        import asyncio
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            connector.execute_aggregate,
            table_name,
            select_clause,
            group_by_columns,
            schema or connection_config.get("schema"),
            filter_spec,
            page,
            page_size,
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Error executing aggregate: {e}\n{error_trace}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute aggregate: {e!s}",
        )

@app.post("/metadata/join")
async def execute_join(request: dict[str, Any]):
    """
    Execute join operation between two tables and return results.
    Request body:
    {
        "db_type": "postgresql" | "mysql" | "oracle" | "sqlserver",
        "connection_config": {
            "hostname": str,
            "port": int,
            "database": str,
            "user": str,
            "password": str,
            "schema": str (optional),
            "service_name": str (optional, for Oracle)
        },
        "left_table": str,
        "right_table": str,
        "left_schema": str (optional),
        "right_schema": str (optional),
        "join_type": "INNER" | "LEFT" | "RIGHT" | "FULL OUTER" | "CROSS",
        "conditions": [
            {
                "left_column": str,
                "right_column": str,
                "operator": str (default "=")
            }
        ],
        "filters": {
            "type": "logical",
            "operator": "AND" | "OR",
            "expressions": [
                {
                    "type": "condition",
                    "column": str,
                    "operator": str,
                    "value": any
                }
            ]
        } (optional),
        "page": int (default 1),
        "page_size": int (default 50)
    }
    """
    try:
        db_type = request.get("db_type", "").lower()
        connection_config = request.get("connection_config", {})
        left_table = request.get("left_table")
        right_table = request.get("right_table")
        left_schema = request.get("left_schema")
        right_schema = request.get("right_schema")
        join_type = request.get("join_type", "INNER")
        conditions = request.get("conditions", [])
        filters = request.get("filters")
        output_columns = request.get("output_columns")  # Optional output columns configuration
        page = int(request.get("page", 1))
        page_size = int(request.get("page_size", 50))
        _log_request_metadata("/metadata/join", {
            "db_type": db_type,
            "left_table": left_table,
            "right_table": right_table,
            "left_schema": left_schema,
            "right_schema": right_schema,
            "join_type": join_type,
            "conditions": conditions,
            "filters": filters,
            "output_columns": output_columns,
            "page": page,
            "page_size": page_size,
        }, connection_config)

        if not left_table or not right_table:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="left_table and right_table are required"
            )

        if db_type not in ["postgresql", "mysql", "oracle", "sqlserver"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported database type: {db_type}"
            )

        # For now, only PostgreSQL is fully implemented
        if db_type != "postgresql":
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail=f"Join operation for {db_type} is not yet implemented. Only PostgreSQL is supported."
            )

        # Map db_type to ConnectionType enum
        type_mapping = {
            "postgresql": ConnectionType.POSTGRESQL,
            "mysql": ConnectionType.MYSQL,
            "oracle": ConnectionType.ORACLE,
            "sqlserver": ConnectionType.SQLSERVER
        }

        connection_type = type_mapping[db_type]

        # Create connector config dict
        connector_config_dict = {
            'host': connection_config.get('hostname') or connection_config.get('host'),
            'port': connection_config.get('port'),
            'database': connection_config.get('database'),
            'username': connection_config.get('user') or connection_config.get('username'),
            'password': connection_config.get('password'),
            'schema': connection_config.get('schema'),
            'service_name': connection_config.get('service_name')
        }

        connector = get_connector(connection_type, connector_config_dict)

        # Run synchronous execute_join in thread pool
        import asyncio
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            connector.execute_join,
            left_table,
            right_table,
            left_schema or connection_config.get('schema'),
            right_schema or connection_config.get('schema'),
            join_type,
            conditions,
            filters,
            page,
            page_size,
            output_columns  # Pass output columns configuration
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Error executing join: {e}\n{error_trace}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute join: {e!s}"
        )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "extraction_service"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)
