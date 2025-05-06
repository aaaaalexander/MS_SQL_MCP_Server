"""
Data Summary Tools for SQL MCP Server.

This module provides tools for summarizing and visualizing SQL data for basic users.
"""
import logging
import asyncio
from typing import Dict, List, Any, Optional, Union

# Configure logging
logger = logging.getLogger("DB_USER_DataSummary")

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
    mcp.add_tool(summarize_data)
    
    logger.info("Registered basic advanced data summary tools with MCP instance")


async def summarize_data(
    table_name: str,
    group_by_column: str,
    metric_column: str,
    aggregation: str = "COUNT",
    having_min_count: Optional[int] = None,
    limit: int = 10,
    include_chart_data: bool = True
) -> Dict[str, Any]:
    """
    Generate summary statistics and aggregations for data in a table.
    
    Args:
        table_name: Table to analyze (format: 'schema.table' or just 'table')
        group_by_column: Column to group the data by
        metric_column: Column to calculate metrics on
        aggregation: Aggregation function ('COUNT', 'SUM', 'AVG', 'MIN', 'MAX')
        having_min_count: Optional minimum count for HAVING clause
        limit: Maximum number of groups to return
        include_chart_data: Include formatted data for chart visualization
        
    Returns:
        Dictionary with summary statistics, aggregate results, and optional chart data.
    """
    logger.info(f"Handling summarize_data: table={table_name}, group_by={group_by_column}, metric={metric_column}")
    
    # Parse schema and table name
    parts = table_name.split('.')
    if len(parts) == 2:
        schema_name, table_name_only = parts
    else:
        schema_name = 'dbo'  # Default schema
        table_name_only = parts[0]
    
    # Validate aggregation function
    aggregation = aggregation.upper()
    valid_aggregations = {"COUNT", "SUM", "AVG", "MIN", "MAX"}
    if aggregation not in valid_aggregations:
        return {
            "error": "Invalid aggregation function",
            "details": f"Aggregation must be one of: {', '.join(valid_aggregations)}"
        }
    
    # Build query
    if aggregation == "COUNT" and metric_column == "*":
        agg_expression = "COUNT(*)"
    else:
        agg_expression = f"{aggregation}([{metric_column}])"
    
    query = f"""
    SELECT 
        [{group_by_column}] AS category, 
        {agg_expression} AS value
    FROM 
        [{schema_name}].[{table_name_only}]
    GROUP BY 
        [{group_by_column}]
    """
    
    # Add HAVING clause if requested
    if having_min_count and aggregation == "COUNT":
        query += f"\nHAVING COUNT(*) >= {having_min_count}"
    
    # Add ORDER BY and LIMIT
    query += f"\nORDER BY value DESC\nFETCH FIRST {limit} ROWS ONLY"
    
    try:
        # Execute the query
        if _execute_query_blocking:
            results = await asyncio.to_thread(_execute_query_blocking, query)
            
            # Format data for charts
            chart_data = None
            if include_chart_data and results:
                chart_data = {
                    "type": "bar",  # Default chart type
                    "labels": [str(row["category"]) for row in results],
                    "values": [row["value"] for row in results],
                    "title": f"{aggregation} of {metric_column} by {group_by_column}"
                }
            
            return {
                "success": True,
                "query": query,
                "group_by_column": group_by_column,
                "metric_column": metric_column,
                "aggregation": aggregation,
                "row_count": len(results),
                "results": results,
                "chart_data": chart_data
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