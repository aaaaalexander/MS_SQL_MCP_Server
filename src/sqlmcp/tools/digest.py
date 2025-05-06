"""
Adapter module to bridge between the server's expected digest tool interface
and the actual implementation in usage_digest.py
"""
import logging
import asyncio
from typing import Dict, Any, Optional

from src.sqlmcp.tools import usage_digest

# Configure logging
logger = logging.getLogger("DB_USER_DigestAdapter")

# These will be set from the main server file
mcp = None
get_db_connection = None
_get_db_connection_blocking = None
_execute_query_blocking = None

# Initialize usage_digest module
def _init_usage_digest():
    """Initialize the usage_digest module with our connection methods"""
    global mcp, _get_db_connection_blocking, _execute_query_blocking
    usage_digest.register_tools(
        mcp, 
        get_db_connection,
        _get_db_connection_blocking, 
        _execute_query_blocking
    )
    logger.info("Successfully initialized usage_digest module")

async def analyze_query_history(
    days_to_analyze: int = 30,
    exclude_system_queries: bool = True,
    min_execution_count: int = 5
) -> Dict[str, Any]:
    """
    Analyzes SQL Server query history to identify frequently used tables and fields.
    
    Args:
        days_to_analyze: Number of days of history to analyze (default: 30)
        exclude_system_queries: Whether to exclude system queries (default: True)
        min_execution_count: Minimum execution count to consider (default: 5)
        
    Returns:
        Dictionary containing analysis results
    """
    logger.info(f"analyze_query_history adapter called with: days={days_to_analyze}, exclude_system={exclude_system_queries}, min_count={min_execution_count}")
    
    # Call the update_usage_digest function with force_refresh=True
    result = await usage_digest.update_usage_digest(
        days_history=days_to_analyze,
        force_refresh=True
    )
    
    # Then get the digest with the specified parameters
    digest_result = await usage_digest.get_usage_digest(
        min_query_count=min_execution_count,
        max_results=50  # Reasonable default
    )
    
    # Combine results
    combined_result = {
        "status": result.get("status", "unknown"),
        "message": result.get("message", ""),
        "last_updated": result.get("last_updated", ""),
        "tables_analyzed": result.get("tables_analyzed", 0),
        "fields_analyzed": result.get("fields_analyzed", 0),
        "joins_analyzed": result.get("joins_analyzed", 0),
        "most_used_tables": digest_result.get("most_used_tables", {}),
        "most_used_fields": digest_result.get("most_used_fields", {}),
        "most_used_joins": digest_result.get("most_used_joins", {})
    }
    
    return combined_result

async def get_table_field_digest() -> Dict[str, Any]:
    """
    Gets the current table and field usage digest or creates a new one if none exists.
    
    Returns:
        Dictionary containing the current digest information
    """
    logger.info("get_table_field_digest adapter called")
    
    # Map to the corresponding function in usage_digest
    return await usage_digest.get_usage_digest()

async def refresh_table_field_digest(
    days_to_analyze: int = 30,
    exclude_system_queries: bool = True,
    min_execution_count: int = 5
) -> Dict[str, Any]:
    """
    Refreshes the table and field usage digest with a new analysis.
    
    Args:
        days_to_analyze: Number of days of history to analyze (default: 30)
        exclude_system_queries: Whether to exclude system queries (default: True)
        min_execution_count: Minimum execution count to consider (default: 5)
        
    Returns:
        Dictionary containing refresh status
    """
    logger.info(f"refresh_table_field_digest adapter called with: days={days_to_analyze}, exclude_system={exclude_system_queries}, min_count={min_execution_count}")
    
    # Map to the corresponding function in usage_digest
    return await usage_digest.update_usage_digest(
        days_history=days_to_analyze,
        force_refresh=True
    )

async def get_table_recommendations(
    table_name: str
) -> Dict[str, Any]:
    """
    Gets detailed recommendations for a specific table based on usage patterns and structure.
    
    Args:
        table_name: Table name to get recommendations for
        
    Returns:
        Dictionary containing table recommendations
    """
    logger.info(f"get_table_recommendations adapter called for table: {table_name}")
    
    # Get table importance
    importance = await usage_digest.get_table_importance(table_name)
    
    # Get join suggestions
    joins = await usage_digest.suggest_important_joins(table_name)
    
    # Combine results
    combined_result = {
        "table_name": table_name,
        "importance_metrics": importance.get("importance_metrics", {}),
        "most_queried_fields": importance.get("most_queried_fields", {}),
        "description": importance.get("description", ""),
        "join_suggestions": joins.get("join_suggestions", []),
        "example_queries": [join.get("example_query") for join in joins.get("join_suggestions", []) if "example_query" in join]
    }
    
    return combined_result