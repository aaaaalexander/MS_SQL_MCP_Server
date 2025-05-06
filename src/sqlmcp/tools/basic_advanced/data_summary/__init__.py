"""
Data Summary module for SQL MCP Server.

This module provides tools for generating summary statistics and aggregations
on database tables for basic users.
"""

# Export summarize_data at module level
from .data_summary import summarize_data

# Function to register tools with MCP
def register(mcp, db_connection=None, db_connection_blocking=None, execute_query_blocking=None, safe_query_function=None):
    """Register this module's functions with the MCP instance."""
    from .data_summary import register_dependencies
    
    # Register dependencies
    register_dependencies(
        mcp_instance=mcp, 
        db_connection=db_connection, 
        db_connection_blocking=db_connection_blocking, 
        execute_query_blocking=execute_query_blocking,
        safe_query_function=safe_query_function
    )
    
    # Register the tool with MCP
    mcp.add_tool(summarize_data)
