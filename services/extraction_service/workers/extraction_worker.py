"""
Extraction Worker for chunked data extraction
"""

# Lazy import pandas to avoid DLL load errors during startup
# import pandas as pd  # Moved to function level
import asyncio
import logging
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

class ExtractionWorker:
    def __init__(self, connector, chunk_size: int = 10000):
        self.connector = connector
        self.chunk_size = chunk_size

    async def extract_data(
        self,
        table_name: str,
        schema: Optional[str] = None,
        filters: Optional[dict[str, Any]] = None,
        progress_callback: Optional[Callable[[float, int], None]] = None
    ) -> dict[str, Any]:
        """
        Extract data from source table in chunks
        Returns dictionary with extracted data and metadata
        """
        try:
            # Get total row count
            total_rows = self.connector.get_row_count(table_name, schema, filters)
            logger.info(f"Total rows to extract: {total_rows}")

            # Get table schema
            schema_info = self.connector.get_table_schema(table_name, schema)

            # Extract data in chunks
            all_data = []
            rows_extracted = 0
            offset = 0

            while rows_extracted < total_rows:
                # Build query with pagination
                built = self._build_extraction_query(
                    table_name, schema, filters, offset, self.chunk_size
                )
                if isinstance(built, tuple) and len(built) == 3 and built[0] == "__PARAMS__":
                    _marker, query, params = built
                    chunk_data = self.connector.execute_query(query, tuple(params))
                else:
                    query = built
                    chunk_data = self.connector.execute_query(query)
                all_data.extend(chunk_data)

                rows_extracted += len(chunk_data)
                offset += self.chunk_size

                # Update progress
                progress = (rows_extracted / total_rows) * 100 if total_rows > 0 else 0
                if progress_callback:
                    progress_callback(progress, rows_extracted)

                logger.info(f"Extracted {rows_extracted}/{total_rows} rows ({progress:.2f}%)")

                # Small delay to avoid overwhelming the database
                await asyncio.sleep(0.1)

            return {
                "data": all_data,
                "rows_extracted": rows_extracted,
                "total_rows": total_rows,
                "schema": schema_info,
                "table_name": table_name,
                "schema_name": schema
            }

        except Exception as e:
            logger.error(f"Error during data extraction: {e}")
            raise

    def _build_extraction_query(
        self,
        table_name: str,
        schema: Optional[str],
        filters: Optional[dict[str, Any]],
        offset: int,
        limit: int
    ) -> str:
        """Build extraction query with pagination and filters.
        When filters contains 'filter_spec' (structured filter from orchestrator pushdown),
        use connector's build_extraction_query_with_filter_spec if available; otherwise
        fall back to simple key=value or no filter.
        """
        # Structured filter_spec (pushdown: only source table columns)
        if filters and filters.get("filter_spec") and hasattr(self.connector, "build_extraction_query_with_filter_spec"):
            try:
                query, params = self.connector.build_extraction_query_with_filter_spec(
                    table_name, schema, filters["filter_spec"], offset, limit
                )
                # Return (query, params) so extract_data can use execute_query with params
                return ("__PARAMS__", query, params)
            except Exception as e:
                logger.warning(f"Connector filter_spec failed, extracting without filter: {e}")
        # Legacy: simple key=value (e.g. where_clause) or no filter
        query = f"SELECT * FROM {table_name}"
        if filters and "filter_spec" not in filters:
            where_clauses = []
            for key, value in (filters or {}).items():
                if key == "where_clause" and value:
                    # Raw WHERE fragment (caller must ensure safety)
                    frag = value.strip()
                    query += " WHERE " + (frag[6:].strip() if frag.upper().startswith("WHERE") else frag)
                    return query + f" LIMIT {limit} OFFSET {offset}"
                where_clauses.append(f"{key} = '{value}'")
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
        query += f" LIMIT {limit} OFFSET {offset}"
        return query
