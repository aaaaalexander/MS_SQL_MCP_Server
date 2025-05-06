"""
Query Builder implementation for SQL MCP Server.

This module provides the implementation of the query_table function which allows
users to build SQL queries without writing SQL code.
"""
import logging
import asyncio
from typing import Dict, List, Any, Optional, Union, Tuple
import time

# Configure logging
logger = logging.getLogger("DB_USER_QueryBuilder")

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
    
    logger.info("Registered dependencies for query_builder module")

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

async def query_table(
    table_name: str,
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    sort_by: Optional[str] = None,
    sort_direction: str = "ASC",
    limit: int = 100,
    offset: int = 0
) -> Dict[str, Any]:
    """
    Query a table with simple filtering and sorting without writing SQL.
    
    Args:
        table_name: Table to query
        columns: Specific columns to return (None for all columns)
        filters: Dictionary of filters (e.g. {"status": "active", "age >": 30})
        sort_by: Column to sort by
        sort_direction: "ASC" or "DESC"
        limit: Maximum rows to return
        offset: Number of rows to skip (for pagination)
        
    Returns:
        Dictionary with query results, generated SQL, and execution stats.
    """
    logger.info(f"Handling query_table: table={table_name}, cols={columns}, filters={filters}, sort={sort_by}, limit={limit}")
    
    try:
        # Validate inputs
        if not table_name:
            return {"error": "No table name provided"}
            
        if sort_direction not in ("ASC", "DESC"):
            sort_direction = "ASC"
            
        # Parse schema and table name
        schema_name, table_name_only = _validate_table_name(table_name)
        
        # Build column list
        if columns and isinstance(columns, list):
            column_list = ", ".join([f"[{col}]" for col in columns])
        else:
            column_list = "*"
            
        # Build WHERE clause
        where_clause, params = _build_where_clause(filters or {})
        where_sql = f"WHERE {where_clause}" if where_clause else ""
        
        # Build ORDER BY clause
        order_by_sql = ""
        if sort_by:
            order_by_sql = f"ORDER BY [{sort_by}] {sort_direction}"
            
        # Build the complete query
        sql_query = f"""
        SELECT {column_list}
        FROM [{schema_name}].[{table_name_only}]
        {where_sql}
        {order_by_sql}
        OFFSET {offset} ROWS
        FETCH NEXT {limit} ROWS ONLY
        """
        
        # Execute the query
        start_time = time.monotonic()
        results = await asyncio.to_thread(
            _execute_query_blocking,
            sql_query,
            params,
            max_rows=limit
        )
        execution_time = time.monotonic() - start_time
        
        # Get count of total matching rows (without limit)
        count_query = f"""
        SELECT COUNT(*) AS total_count
        FROM [{schema_name}].[{table_name_only}]
        {where_sql}
        """
        
        count_result = await asyncio.to_thread(
            _execute_query_blocking,
            count_query,
            params
        )
        
        total_count = count_result[0]['total_count'] if count_result else None
        
        # Return the results
        return {
            "success": True,
            "rows": results,
            "row_count": len(results),
            "total_matching_rows": total_count,
            "has_more": total_count > (offset + limit) if total_count is not None else None,
            "generated_sql": sql_query,
            "execution_time_seconds": round(execution_time, 3),
            "table_name": f"{schema_name}.{table_name_only}",
            "filters_applied": filters,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error(f"Error in query_table: {e}")
        return {
            "error": "Failed to execute query",
            "details": str(e),
            "table_name": table_name
        }
