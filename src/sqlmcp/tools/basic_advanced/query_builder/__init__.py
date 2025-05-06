"""
Query Builder module for SQL MCP Server.

This module provides simplified, user-friendly tools for building SQL queries without
writing SQL, aimed at basic users.
"""

# Export query_table at module level
from .query_builder import query_table

# Function to register tools with MCP
def register(mcp, db_connection=None, db_connection_blocking=None, execute_query_blocking=None, safe_query_function=None):
    """Register this module's functions with the MCP instance."""
    from .query_builder import register_dependencies
    
    # Register dependencies
    register_dependencies(
        mcp_instance=mcp, 
        db_connection=db_connection, 
        db_connection_blocking=db_connection_blocking, 
        execute_query_blocking=execute_query_blocking,
        safe_query_function=safe_query_function
    )
    
    # Register the tool with MCP
    mcp.add_tool(query_table)
