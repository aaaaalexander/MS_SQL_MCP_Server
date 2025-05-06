"""
Export Tools module for SQL MCP Server.

This module provides tools for exporting data from SQL tables into various formats
for basic users.
"""

# Export export_data at module level
from .export_tools import export_data

# Function to register tools with MCP
def register(mcp, db_connection=None, db_connection_blocking=None, execute_query_blocking=None, safe_query_function=None):
    """Register this module's functions with the MCP instance."""
    from .export_tools import register_dependencies
    
    # Register dependencies
    register_dependencies(
        mcp_instance=mcp, 
        db_connection=db_connection, 
        db_connection_blocking=db_connection_blocking, 
        execute_query_blocking=execute_query_blocking,
        safe_query_function=safe_query_function
    )
    
    # Register the tool with MCP
    mcp.add_tool(export_data)
