"""
Tools for executing and analyzing SQL queries.
"""
import logging
import re
import time
from typing import Dict, List, Any, Optional

# Configure logging
logger = logging.getLogger("DB_USER_Query")

# These will be set from the main server file
mcp = None
get_db_connection = None

def register_tools(mcp_instance, db_connection_function):
    """Register this module's functions with the MCP instance."""
    global mcp, get_db_connection
    mcp = mcp_instance
    get_db_connection = db_connection_function
    
    # Register tools manually
    mcp.add_tool(execute_select)
    mcp.add_tool(get_sample_data)
    mcp.add_tool(explain_query)
    
    logger.info("Registered query tools with MCP instance")

async def execute_select(
    query: str, 
    limit: int = 100, 
    parameters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Execute a SELECT query with safety checks and return formatted results.
    
    Args:
        query: The SQL SELECT query to execute
        limit: Maximum number of rows to return (default: 100)
        parameters: Optional dictionary of query parameters
    
    Returns:
        Dictionary with query results, column metadata, and execution statistics
    """
    ctx = mcp.context.get()
    db = await get_db_connection(ctx)
    
    # Import is_safe_query and sanitize_parameters here to avoid circular imports
    from DB_USER.utils.security import is_safe_query, sanitize_parameters
    
    # Validate query is safe
    if not is_safe_query(query):
        return {
            "error": "Query validation failed. Only SELECT queries are allowed for security reasons.",
            "details": "The query contains prohibited operations or syntax."
        }
    
    # Apply row limit if not already present in query
    timeout = ctx.lifespan_context.query_timeout if hasattr(ctx.lifespan_context, 'query_timeout') else 30
    if limit > 0 and "top" not in query.lower():
        match = re.match(r'^\s*SELECT\s+', query, re.IGNORECASE)
        if match:
            query = query[:match.end()] + f"TOP {limit} " + query[match.end():]
    
    # Sanitize parameters if provided
    clean_params = sanitize_parameters(parameters) if parameters else None
    
    try:
        # Execute the query with timeout
        start_time = time.time()
        results = await db.execute_query(query, clean_params, timeout)
        execution_time = time.time() - start_time
        
        # Get column metadata if results exist
        columns = []
        if results and len(results) > 0:
            # Extract column names and types from first row
            first_row = results[0]
            for col_name, value in first_row.items():
                col_type = type(value).__name__ if value is not None else "unknown"
                columns.append({
                    "name": col_name,
                    "type": col_type
                })
        
        # Check if results were limited
        was_limited = limit > 0 and len(results) >= limit
        
        response = {
            "success": True,
            "row_count": len(results),
            "columns": columns,
            "results": results,
            "execution_time_seconds": round(execution_time, 3),
            "limited": was_limited,
            "limit": limit
        }
        
        logger.info(f"Query executed successfully: {len(results)} rows in {execution_time:.3f}s")
        return response
        
    except Exception as e:
        logger.error(f"Query execution error: {str(e)}")
        return {
            "error": "Query execution failed",
            "details": str(e)
        }

async def get_sample_data(
    table_name: str, 
    sample_size: int = 10, 
    where: Optional[str] = None
) -> Dict[str, Any]:
    """
    Retrieve representative sample data from a table.
    
    Args:
        table_name: The name of the table (can include schema as 'schema.table')
        sample_size: Number of rows to sample (default: 10)
        where: Optional WHERE clause to filter results
    
    Returns:
        Dictionary with sample data and column information
    """
    # Import is_safe_query to avoid circular imports
    from DB_USER.utils.security import is_safe_query
    
    # Parse schema and table name
    parts = table_name.split('.')
    if len(parts) == 2:
        schema_name, table_name = parts
    else:
        schema_name = 'dbo'  # Default schema
        table_name = parts[0]
    
    # Build query
    query = f"SELECT TOP {sample_size} * FROM [{schema_name}].[{table_name}]"
    
    # Add WHERE clause if provided
    if where:
        # Validate WHERE clause doesn't contain prohibited operations
        if not is_safe_query(f"SELECT * FROM table WHERE {where}"):
            return {
                "error": "WHERE clause validation failed",
                "details": "The WHERE clause contains prohibited operations or syntax."
            }
        query += f" WHERE {where}"
    
    # Add ORDER BY for more representative sampling (if no WHERE clause)
    if not where:
        query += " ORDER BY NEWID()"  # Random sampling
    
    # Execute query
    return await execute_select(query, limit=sample_size)

async def explain_query(
    query: str
) -> Dict[str, Any]:
    """
    Generate and explain a query execution plan.
    
    Args:
        query: The SQL query to explain
    
    Returns:
        Dictionary with execution plan and optimization suggestions
    """
    # Import is_safe_query to avoid circular imports
    from DB_USER.utils.security import is_safe_query
    
    ctx = mcp.context.get()
    db = await get_db_connection(ctx)
    
    # Validate query is safe
    if not is_safe_query(query):
        return {
            "error": "Query validation failed. Only SELECT queries are allowed for security reasons.",
            "details": "The query contains prohibited operations or syntax."
        }
    
    # Get execution plan
    plan_query = f"SET SHOWPLAN_XML ON; {query}; SET SHOWPLAN_XML OFF;"
    
    try:
        # Execute the query to get the plan
        plan_results = await db.execute_query(plan_query)
        
        # Process the plan results
        # In a real implementation, this would parse and analyze the execution plan
        
        # Generate suggestions based on plan
        suggestions = [
            "Consider adding an index on frequently filtered columns",
            "Check for table scans which might benefit from indexes",
            "Review join conditions for efficiency"
        ]
        
        return {
            "query": query,
            "execution_plan": plan_results,
            "optimization_suggestions": suggestions
        }
        
    except Exception as e:
        logger.error(f"Failed to generate execution plan: {str(e)}")
        return {
            "error": "Failed to generate execution plan",
            "details": str(e)
        }
