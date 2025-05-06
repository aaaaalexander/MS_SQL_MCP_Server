"""
Data Summary implementation for SQL MCP Server.

This module provides the implementation of the summarize_data function which generates
summary statistics and aggregations for database tables.
"""
import logging
import asyncio
from typing import Dict, List, Any, Optional, Union, Tuple
import time

# Configure logging
logger = logging.getLogger("DB_USER_DataSummary")

# These will be set from the registration function
mcp = None
get_db_connection = None
_get_db_connection_blocking = None
_execute_query_blocking = None
is_safe_query = None
analyze_table_data = None  # From analyze_fixed

def register_dependencies(mcp_instance=None, db_connection=None, db_connection_blocking=None, 
                        execute_query_blocking=None, safe_query_function=None):
    """Register dependencies for this module."""
    global mcp, get_db_connection, _get_db_connection_blocking, _execute_query_blocking, is_safe_query, analyze_table_data
    
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
    
    # Try to import analyze_table_data
    try:
        from src.sqlmcp.tools import analyze_fixed
        analyze_table_data = analyze_fixed.analyze_table_data
        logger.info("Successfully imported analyze_table_data")
    except ImportError as e:
        logger.warning(f"Could not import analyze_table_data: {e}")
    
    logger.info("Registered dependencies for data_summary module")

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

async def summarize_data(
    table_name: str,
    group_by: Optional[List[str]] = None,
    metrics: Optional[Dict[str, str]] = None,
    include_quality_checks: bool = False,
    max_groups: int = 20
) -> Dict[str, Any]:
    """
    Generate summary statistics and groupings on table data.
    
    Args:
        table_name: Table to analyze
        group_by: Optional columns to group by
        metrics: Dictionary mapping columns to aggregation functions
                 (e.g. {"sales": "sum", "customers": "count_distinct"})
        include_quality_checks: Add basic data quality metrics
        max_groups: Maximum number of groups to return
        
    Returns:
        Dictionary with summarized data, aggregate metrics, and optional
        data quality information like null percentages or outlier counts.
    """
    logger.info(f"Handling summarize_data: table={table_name}, group_by={group_by}, metrics={metrics}")
    
    try:
        # Validate inputs
        if not table_name:
            return {"error": "No table name provided"}
            
        # Parse schema and table name
        schema_name, table_name_only = _validate_table_name(table_name)
        
        # If no groups or metrics, return error
        if not group_by and not metrics:
            return {
                "error": "No group_by columns or metrics specified",
                "details": "You must specify at least one group_by column or metric"
            }
            
        # Default metrics if none provided
        if not metrics:
            metrics = {"*": "count"}
            
        # Map function names to SQL
        agg_map = {
            "sum": "SUM",
            "avg": "AVG",
            "min": "MIN",
            "max": "MAX",
            "count": "COUNT",
            "count_distinct": "COUNT(DISTINCT",  # Special case, needs closing paren
            "stdev": "STDEV",
            "stdevp": "STDEVP",
            "var": "VAR",
            "varp": "VARP"
        }
        
        # Build the metrics expressions
        metric_expressions = []
        for col, func in metrics.items():
            func = func.lower()
            if func not in agg_map:
                return {
                    "error": f"Unknown aggregation function: {func}",
                    "details": f"Supported functions: {', '.join(agg_map.keys())}"
                }
                
            col_name = col if col != "*" else "*"
            
            if func == "count_distinct":
                # Special handling for COUNT DISTINCT
                metric_expressions.append(f"{agg_map[func]} [{col_name}])) AS {col}_{func}")
            else:
                metric_expressions.append(f"{agg_map[func]}([{col_name}]) AS {col}_{func}")
        
        # Build GROUP BY clause
        group_by_cols = []
        group_by_clause = ""
        if group_by:
            group_by_cols = [f"[{col}]" for col in group_by]
            group_by_clause = f"GROUP BY {', '.join(group_by_cols)}"
            
            # For grouping, include the group by columns in the select
            select_cols = group_by_cols + metric_expressions
        else:
            # No grouping, just aggregates
            select_cols = metric_expressions
            
        # Build ORDER BY clause - order by first metric by default
        first_metric = f"{next(iter(metrics.items()))[0]}_{next(iter(metrics.items()))[1]}"
        order_by_clause = f"ORDER BY {first_metric} DESC"
        
        # Build the complete query
        sql_query = f"""
        SELECT TOP {max_groups} {', '.join(select_cols)}
        FROM [{schema_name}].[{table_name_only}]
        {group_by_clause}
        {order_by_clause}
        """
        
        # Execute the query
        start_time = time.monotonic()
        results = await asyncio.to_thread(
            _execute_query_blocking,
            sql_query,
            max_rows=max_groups
        )
        execution_time = time.monotonic() - start_time
        
        # Add quality checks if requested and no grouping
        quality_info = {}
        if include_quality_checks and not group_by:
            # Use analyze_table_data to get quality metrics
            if analyze_table_data:
                column_names = list(metrics.keys())
                if "*" in column_names:
                    column_names = None  # Will analyze all columns
                    
                quality_results = await analyze_table_data(
                    table_name=f"{schema_name}.{table_name_only}", 
                    column_names=column_names,
                    sample_size=1000  # Reasonable sample
                )
                
                if "column_analysis" in quality_results:
                    quality_info = quality_results["column_analysis"]
        
        # Return the results
        response = {
            "success": True,
            "summary_data": results,
            "row_count": len(results),
            "max_groups_limit": max_groups,
            "has_more": len(results) >= max_groups,
            "generated_sql": sql_query,
            "execution_time_seconds": round(execution_time, 3),
            "table_name": f"{schema_name}.{table_name_only}",
            "group_by_columns": group_by,
            "metrics_applied": metrics
        }
        
        if quality_info:
            response["quality_info"] = quality_info
            
        return response
        
    except Exception as e:
        logger.error(f"Error in summarize_data: {e}")
        return {
            "error": "Failed to summarize data",
            "details": str(e),
            "table_name": table_name
        }
