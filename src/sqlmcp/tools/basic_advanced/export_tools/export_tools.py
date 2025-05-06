"""
Export Tools implementation for SQL MCP Server.

This module provides the implementation of the export_data function which exports
data from SQL tables into various formats.
"""
import logging
import asyncio
from typing import Dict, List, Any, Optional, Union, Tuple
import json
import csv
import io
import time

# Configure logging
logger = logging.getLogger("DB_USER_ExportTools")

# These will be set from the registration function
mcp = None
get_db_connection = None
_get_db_connection_blocking = None
_execute_query_blocking = None
is_safe_query = None

def register_dependencies(mcp_instance=None, db_connection=None, db_connection_blocking=None, 
                        execute_query_blocking=None, safe_query_function=None):
    """Register dependencies for this module."""
    global mcp, get_db_connection, _get_db_connection_blocking, _execute_query_blocking, is_safe_query
    
    if mcp_instance:
        mcp = mcp_instance
    if db_connection:
        get_db_connection = db_connection
    if db_connection_blocking:
        _get_db_connection_blocking = db_connection_blocking
    if execute_query_blocking:
        _execute_query_blocking = execute_query_blocking
    if safe_query_function:
        is_safe_query = safe_query_function
    
    logger.info("Registered dependencies for export_tools module")

def _validate_table_name(table_name: str) -> Tuple[str, str]:
    """
    Parse a table name into schema and table components.
    
    Args:
        table_name: Table name (format: 'schema.table' or just 'table' for default 'dbo' schema)
        
    Returns:
        Tuple of (schema_name, table_name_only)
    """
    parts = table_name.split('.')
    if len(parts) == 2:
        return parts[0], parts[1]
    else:
        return 'dbo', parts[0]

def _build_where_clause(filters: Dict[str, Any]) -> Tuple[str, List[Any]]:
    """
    Build a SQL WHERE clause from a filter dictionary.
    
    Args:
        filters: Dictionary of filters (e.g. {"status": "active", "age >": 30})
        
    Returns:
        Tuple of (where_clause, parameters_list)
    """
    if not filters:
        return "", []
    
    clauses = []
    params = []
    
    for key, value in filters.items():
        # Handle NULL values
        if value is None:
            if key.endswith(' IS NOT'):
                clauses.append(f"[{key[:-8]}] IS NOT NULL")
            else:
                clauses.append(f"[{key}] IS NULL")
            continue
            
        # Handle operators in the key
        operators = {
            ' =': '=', 
            ' >': '>', 
            ' <': '<', 
            ' >=': '>=', 
            ' <=': '<=', 
            ' !=': '!=',
            ' <>': '<>',
            ' LIKE': 'LIKE',
            ' NOT LIKE': 'NOT LIKE',
            ' IN': 'IN',
            ' NOT IN': 'NOT IN'
        }
        
        found_operator = False
        for op_suffix, op in operators.items():
            if key.endswith(op_suffix):
                column_name = key[:-len(op_suffix)]
                found_operator = True
                break
                
        if not found_operator:
            column_name = key
            op = '='
            
        # Handle IN and NOT IN operators which take a list of values
        if op in ('IN', 'NOT IN') and isinstance(value, list):
            placeholders = ', '.join(['?'] * len(value))
            clauses.append(f"[{column_name}] {op} ({placeholders})")
            params.extend(value)
        # Handle normal operators
        else:
            clauses.append(f"[{column_name}] {op} ?")
            params.append(value)
            
    where_clause = " AND ".join(clauses)
    return where_clause, params

async def export_data(
    source: str,
    format: str = "csv",
    filters: Optional[Dict[str, Any]] = None,
    columns: Optional[List[str]] = None,
    max_rows: int = 10000
) -> Dict[str, Any]:
    """
    Export data from a table or query to various formats.
    
    Args:
        source: Table name or SQL query
        format: Export format ("csv", "json", "excel", etc.)
        filters: Optional filters if source is a table
        columns: Specific columns to export (None for all)
        max_rows: Maximum rows to export
        
    Returns:
        Dictionary with export data, format information, and metadata.
    """
    logger.info(f"Handling export_data: source={source}, format={format}, max_rows={max_rows}")
    
    try:
        # Validate format
        format = format.lower()
        if format not in ["csv", "json", "tsv"]:
            return {
                "error": f"Unsupported export format: {format}",
                "details": "Supported formats: csv, json, tsv"
            }
            
        # Determine if source is a table name or SQL query
        is_query = source.strip().upper().startswith("SELECT ")
        
        if is_query:
            # Validate the query is safe
            if is_safe_query and not is_safe_query(source):
                return {
                    "error": "Invalid query",
                    "details": "Only SELECT queries are allowed for security reasons"
                }
                
            # Use the query directly
            query = source
            params = None
            description = "Custom SQL query"
        else:
            # It's a table name, build a query
            schema_name, table_name = _validate_table_name(source)
            
            # Build column list
            if columns and isinstance(columns, list):
                column_list = ", ".join([f"[{col}]" for col in columns])
            else:
                column_list = "*"
                
            # Build WHERE clause
            where_clause, params = _build_where_clause(filters or {})
            where_sql = f"WHERE {where_clause}" if where_clause else ""
            
            # Build the query
            query = f"""
            SELECT TOP {max_rows} {column_list}
            FROM [{schema_name}].[{table_name}]
            {where_sql}
            """
            
            description = f"Table: {schema_name}.{table_name}"
        
        # Execute the query
        start_time = time.monotonic()
        results = await asyncio.to_thread(
            _execute_query_blocking,
            query,
            params,
            max_rows=max_rows
        )
        execution_time = time.monotonic() - start_time
        
        if not results:
            return {
                "success": True,
                "format": format,
                "row_count": 0,
                "export_data": "",
                "execution_time_seconds": round(execution_time, 3),
                "description": description,
                "message": "No data found matching your criteria"
            }
        
        # Convert the data to the requested format
        if format == "json":
            # Format as JSON
            export_data = json.dumps(results, default=str, indent=2)
        elif format == "csv" or format == "tsv":
            # Format as CSV or TSV
            delimiter = "," if format == "csv" else "\t"
            output = io.StringIO()
            if results:
                fieldnames = results[0].keys()
                writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=delimiter)
                writer.writeheader()
                writer.writerows(results)
            export_data = output.getvalue()
        
        # Return the data
        return {
            "success": True,
            "format": format,
            "row_count": len(results),
            "max_rows_limit": max_rows,
            "has_more": len(results) >= max_rows,
            "export_data": export_data,
            "execution_time_seconds": round(execution_time, 3),
            "description": description
        }
        
    except Exception as e:
        logger.error(f"Error in export_data: {e}")
        return {
            "error": "Failed to export data",
            "details": str(e),
            "source": source
        }
