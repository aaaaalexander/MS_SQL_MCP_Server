"""
Advanced Inspector Tools for SQL MCP Server.

This module advances existing analysis and exploration tools to make them
more accessible and comprehensive for basic SQL users.
"""
import logging
import asyncio
from typing import Dict, List, Any, Optional, Union, Tuple
import time
import json

# Configure logging
logger = logging.getLogger("DB_USER_BasicInspector")

# These will be imported from existing tools
analyze_table_data = None
get_table_schema = None
advanced_get_sample_data = None
advanced_search_schema_objects = None
advanced_find_related_tables = None
_get_db_connection_blocking = None
_execute_query_blocking = None
is_safe_query = None

# Set by registration function
mcp = None

# The register function that was missing
def register(mcp_instance, tool_dependencies=None, db_connection_blocking=None, execute_query_blocking=None, safe_query_func=None):
    """Register this module's functions with the MCP instance."""
    global mcp, analyze_table_data, get_table_schema, advanced_get_sample_data, advanced_search_schema_objects
    global advanced_find_related_tables, _get_db_connection_blocking, _execute_query_blocking, is_safe_query
    
    mcp = mcp_instance
    
    # Import dependencies from the provided dictionary
    if tool_dependencies is None:
        tool_dependencies = {}
    
    analyze_table_data = tool_dependencies.get('analyze_table_data')
    get_table_schema = tool_dependencies.get('get_table_schema')
    advanced_get_sample_data = tool_dependencies.get('advanced_get_sample_data')
    advanced_search_schema_objects = tool_dependencies.get('advanced_search_schema_objects')
    advanced_find_related_tables = tool_dependencies.get('advanced_find_related_tables')
    
    # Set connection and execution functions
    _get_db_connection_blocking = db_connection_blocking
    _execute_query_blocking = execute_query_blocking
    if safe_query_func:
        is_safe_query = safe_query_func
    
    # Register tools manually
    mcp.add_tool(analyze_table_data_advanced)
    mcp.add_tool(search_schema_objects_advanced)
    mcp.add_tool(find_related_tables_advanced)
    
    logger.info("Registered basic advanced inspector tools with MCP instance")


# Import the enhanced implementations from enhanced_inspector.py
from .enhanced_inspector import analyze_table_data_advanced, search_schema_objects_advanced, find_related_tables_advanced
