"""
Advanced SQL tools for basic users.

This module provides simplified, user-friendly tools for SQL basics 
following the 80/20 rule - the 20% of functionality that covers 80% 
of common use cases for database interaction without SQL knowledge.

This file now acts as a wrapper around the properly structured basic_advanced package.
"""
import logging

# Configure logging
logger = logging.getLogger("DB_USER_BasicTools")

# Import tools from the basic_advanced package
try:
    from src.sqlmcp.tools.basic_advanced import (
        query_table,
        summarize_data,
        export_data,
        analyze_table_data_advanced,
        search_schema_objects_advanced,
        find_related_tables_advanced,
        register_tools
    )
    logger.info("Successfully imported tools from basic_advanced package")
except ImportError as e:
    logger.error(f"Failed to import from basic_advanced package: {e}")
    
    # Define placeholders for missing functions
    def register_tools(mcp_instance, db_connection_function, db_connection_blocking, execute_query_blocking, safe_query_function=None):
        logger.error("Cannot register basic_advanced tools - package not properly initialized")
        return False
        
    query_table = None
    summarize_data = None
    export_data = None
    analyze_table_data_advanced = None
    search_schema_objects_advanced = None
    find_related_tables_advanced = None
