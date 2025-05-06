"""
Query Builder Tools for SQL MCP Server.

This module provides easy-to-use SQL query building tools for novice users.
"""
import logging
import asyncio
from typing import Dict, List, Any, Optional, Union

# Configure logging
logger = logging.getLogger("DB_USER_QueryBuilder")

# These will be set by the registration function
mcp = None
get_db_connection = None
_get_db_connection_blocking = None
_execute_query_blocking = None
is_safe_query = None


def register(mcp_instance, db_connection_function=None, db_connection_blocking=None, 
             execute_query_blocking=None, safe_query_function=None):
    """Register this module's functions with the MCP instance."""
    global mcp, get_db_connection, _get_db_connection_blocking, _execute_query_blocking, is_safe_query
    
    mcp = mcp_instance
    get_db_connection = db_connection_function
    _get_db_connection_blocking = db_connection_blocking
    _execute_query_blocking = execute_query_blocking
    if safe_query_function:
        is_safe_query = safe_query_function
    
    # Register tools manually
    mcp.add_tool(query_table)
    
    logger.info("Registered novice enhanced query builder tools with MCP instance")


async def query_table(
    table_name: str,
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    sort_by: Optional[str] = None,
    sort_order: str = "ASC",
    limit: int = 100
) -> Dict[str, Any]:
    """
    Build and execute a simple SQL query with a friendly interface.
    
    Args:
        table_name: Table to query (format: 'schema.table' or just 'table')
        columns: Columns to select (defaults to all columns)
        filters: Dictionary of {column: value} pairs for WHERE conditions
        sort_by: Column to sort by
        sort_order: Sort direction ('ASC' or 'DESC')
        limit: Maximum number of rows to return
        
    Returns:
        Dictionary with query results, metadata, and the generated SQL query.
    """
    logger.info(f"Handling query_table: table={table_name}, limit={limit}")
    
    # Parse schema and table name
    parts = table_name.split('.')
    if len(parts) == 2:
        schema_name, table_name_only = parts
    else:
        schema_name = 'dbo'  # Default schema
        table_name_only = parts[0]
    
    # Build SELECT clause
    select_clause = "*"
    if columns and len(columns) > 0:
        select_clause = ", ".join([f"[{col}]" for col in columns])
    
    # Build WHERE clause
    where_clause = ""
    params = []
    if filters and len(filters) > 0:
        conditions = []
        for col, value in filters.items():
            if value is None:
                conditions.append(f"[{col}] IS NULL")
            else:
                conditions.append(f"[{col}] = ?")
                params.append(value)
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
    
    # Build ORDER BY clause
    order_clause = ""
    if sort_by:
        order_clause = f" ORDER BY [{sort_by}] {sort_order}"
    
    # Build the full query
    query = f"SELECT TOP {limit} {select_clause} FROM [{schema_name}].[{table_name_only}]{where_clause}{order_clause}"
    
    try:
        # Execute the query
        if _execute_query_blocking:
            results = await asyncio.to_thread(_execute_query_blocking, query, tuple(params))
            
            return {
                "success": True,
                "query": query,
                "row_count": len(results),
                "results": results,
                "columns": list(results[0].keys()) if results else []
            }
        else:
            return {
                "error": "Query execution function not available",
                "details": "The query execution function has not been properly configured"
            }
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return {
            "error": "Query execution failed",
            "details": str(e),
            "query": query
        }