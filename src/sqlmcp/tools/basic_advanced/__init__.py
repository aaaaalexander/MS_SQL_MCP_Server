"""
SQL MCP Basic Advanced Tools Module.

This module provides SQL tools specifically designed for basic users,
advancing existing tools and adding new ones that follow the 80/20 rule
(20% of functionality that covers 80% of use cases).
"""

# These will be set by the registration function
mcp = None
get_db_connection = None
_get_db_connection_blocking = None
_execute_query_blocking = None
is_safe_query = None

# Try to import analyze_table_data from the parent module
try:
    from ..analyze_fixed import analyze_table_data
except ImportError:
    analyze_table_data = None

# Try to import get_table_schema from the parent module
try:
    from ... import sql_mcp_server
    get_table_schema = getattr(sql_mcp_server, 'get_table_schema', None)
except ImportError:
    get_table_schema = None

# Export the specific functions for easier access
from .enhanced_inspector import analyze_table_data_advanced, search_schema_objects_advanced, find_related_tables_advanced
from .query_builder import query_table
from .data_summary import summarize_data
from .export_tools import export_data

def register_tools(mcp_instance, db_connection_function=None, db_connection_blocking=None, 
                  execute_query_blocking=None, safe_query_function=None):
    """Register all basic advanced tools with the MCP instance."""
    global mcp, get_db_connection, _get_db_connection_blocking, _execute_query_blocking, is_safe_query
    
    # Set module-level variables
    mcp = mcp_instance
    get_db_connection = db_connection_function
    _get_db_connection_blocking = db_connection_blocking
    _execute_query_blocking = execute_query_blocking
    is_safe_query = safe_query_function
    
    # Import the register functions from each module - using try/except to handle partial implementation
    try:
        from .enhanced_inspector import register as register_inspector
        register_inspector(mcp_instance, 
                          {"analyze_table_data": analyze_table_data, 
                           "get_table_schema": get_table_schema,
                           "advanced_get_sample_data": None,
                           "advanced_search_schema_objects": None,
                           "advanced_find_related_tables": None}, 
                          db_connection_blocking, 
                          execute_query_blocking, 
                          safe_query_function)
    except (ImportError, AttributeError) as e:
        import logging
        logger = logging.getLogger("DB_USER_BasicTools")
        logger.warning(f"Could not register advanced_inspector tools: {e}")
    
    try:
        from .query_builder import register as register_query
        register_query(mcp_instance, db_connection_function, db_connection_blocking, execute_query_blocking, safe_query_function)
    except (ImportError, AttributeError) as e:
        import logging
        logger = logging.getLogger("DB_USER_BasicTools")
        logger.warning(f"Could not register query_builder tools: {e}")
    
    try:
        from .data_summary import register as register_summary
        register_summary(mcp_instance, db_connection_function, db_connection_blocking, execute_query_blocking, safe_query_function)
    except (ImportError, AttributeError) as e:
        import logging
        logger = logging.getLogger("DB_USER_BasicTools")
        logger.warning(f"Could not register data_summary tools: {e}")
    
    try:
        from .export_tools import register as register_export
        register_export(mcp_instance, db_connection_function, db_connection_blocking, execute_query_blocking, safe_query_function)
    except (ImportError, AttributeError) as e:
        import logging
        logger = logging.getLogger("DB_USER_BasicTools")
        logger.warning(f"Could not register export_tools: {e}")
    
    # Manually register the advanced tools if they weren't registered properly
    if hasattr(mcp, 'add_tool'):
        if analyze_table_data_advanced:
            mcp.add_tool(analyze_table_data_advanced)
        if search_schema_objects_advanced:
            mcp.add_tool(search_schema_objects_advanced)
        if find_related_tables_advanced:
            mcp.add_tool(find_related_tables_advanced)
        if query_table:
            mcp.add_tool(query_table)
        if summarize_data:
            mcp.add_tool(summarize_data)
        if export_data:
            mcp.add_tool(export_data)
    
    # Log successful registration of available tools
    import logging
    logger = logging.getLogger("DB_USER_BasicTools")
    logger.info("Registered available basic_advanced tools with MCP instance")

# Version information
__version__ = "0.1.0"