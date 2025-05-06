"""
Enhanced SQL tools for novice users.

This module provides simplified, user-friendly tools for SQL novices 
following the 80/20 rule - the 20% of functionality that covers 80% 
of common use cases for database interaction without SQL knowledge.

This file now acts as a wrapper around the properly structured novice_enhanced package.
"""
import logging

# Configure logging
logger = logging.getLogger("DB_USER_NoviceTools")

# Import tools from the novice_enhanced package
try:
    from src.sqlmcp.tools.novice_enhanced import (
        query_table,
        summarize_data,
        export_data,
        analyze_table_data_enhanced,
        search_schema_objects_enhanced,
        find_related_tables_enhanced,
        register_tools
    )
    logger.info("Successfully imported tools from novice_enhanced package")
except ImportError as e:
    logger.error(f"Failed to import from novice_enhanced package: {e}")
    
    # Define placeholders for missing functions
    def register_tools(mcp_instance, db_connection_function, db_connection_blocking, execute_query_blocking, safe_query_function=None):
        logger.error("Cannot register novice_enhanced tools - package not properly initialized")
        return False
        
    query_table = None
    summarize_data = None
    export_data = None
    analyze_table_data_enhanced = None
    search_schema_objects_enhanced = None
    find_related_tables_enhanced = None
