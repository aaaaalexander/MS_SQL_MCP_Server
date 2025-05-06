"""
Simplified adapter module for SQL MCP Server.

This module provides adapter tools for the enhanced schema tools.
"""
import logging
import asyncio
import time
from typing import Dict, List, Any, Optional, Union, Tuple

# Configure logging
logger = logging.getLogger("DB_USER_SimplifiedAdapter")

# These will be set during registration
mcp = None
_get_db_connection_blocking = None
_execute_query_blocking = None
ALLOWED_SCHEMAS = ["dbo"]
safe_query_checker = None

# Import schema_extended tools
try:
    from src.sqlmcp.tools import schema_extended
    TOOLS_AVAILABLE = True
    logger.info("Successfully imported schema_extended tools")
except ImportError as e:
    TOOLS_AVAILABLE = False
    logger.error(f"Failed to import schema_extended tools: {e}")

# Define adapter functions
async def enhanced_list_schemas() -> Dict[str, Any]:
    """Get a list of all available schemas in the database."""
    logger.info("Handling enhanced_list_schemas request")
    
    try:
        schemas = await schema_extended.list_schemas()
        
        return {
            "success": True,
            "schemas": schemas,
            "schema_count": len(schemas),
            "allowed_schemas": ALLOWED_SCHEMAS
        }
    except Exception as e:
        logger.error(f"Error in enhanced_list_schemas: {e}")
        return {
            "error": "Failed to get available schemas",
            "details": str(e)
        }

async def enhanced_search_schema_objects(search_term: str, object_types: Optional[List[str]] = None) -> Dict[str, Any]:
    """Search for database objects with enhanced error handling."""
    logger.info(f"Handling enhanced_search_schema_objects: search_term={search_term}")
    
    try:
        start_time = time.monotonic()
        search_results = await schema_extended.search_schema_objects(search_term, object_types)
        execution_time = time.monotonic() - start_time
        
        # Add execution time to response
        if isinstance(search_results, dict) and "error" not in search_results:
            search_results["execution_time_seconds"] = round(execution_time, 3)
        
        return search_results
    except Exception as e:
        logger.error(f"Error searching schema objects for '{search_term}': {e}")
        return {
            "error": "Failed to search schema objects",
            "details": str(e),
            "search_term": search_term
        }

async def enhanced_find_related_tables(table_name: str) -> Dict[str, Any]:
    """Find tables related to the specified table through foreign keys."""
    logger.info(f"Handling enhanced_find_related_tables: table_name={table_name}")
    
    try:
        start_time = time.monotonic()
        related_tables = await schema_extended.find_related_tables(table_name)
        execution_time = time.monotonic() - start_time
        
        # Add execution time to response
        if isinstance(related_tables, dict) and "error" not in related_tables:
            related_tables["execution_time_seconds"] = round(execution_time, 3)
        
        return related_tables
    except Exception as e:
        logger.error(f"Error finding related tables for {table_name}: {e}")
        return {
            "error": "Failed to find related tables",
            "details": str(e),
            "table_name": table_name
        }

def register_adapter_tools(mcp_instance, db_conn_func=None, db_conn_blocking=None, 
                          exec_query_blocking=None, schemas=None, query_checker=None):
    """Register adapter tools with the MCP instance."""
    global mcp, _get_db_connection_blocking, _execute_query_blocking, ALLOWED_SCHEMAS, safe_query_checker
    
    # Set module variables
    mcp = mcp_instance
    _get_db_connection_blocking = db_conn_blocking
    _execute_query_blocking = exec_query_blocking
    
    if schemas:
        ALLOWED_SCHEMAS = schemas
        
    if query_checker:
        safe_query_checker = query_checker
    
    # First register schema_extended tools
    schema_extended.register_tools(
        mcp_instance=mcp_instance,
        db_connection=db_conn_func,
        db_connection_blocking=db_conn_blocking,
        execute_query_blocking=exec_query_blocking,
        allowed_schemas=schemas
    )
    
    # Now register adapter tools
    mcp.add_tool(enhanced_list_schemas)
    mcp.add_tool(enhanced_search_schema_objects)
    mcp.add_tool(enhanced_find_related_tables)
    
    logger.info("Registered simplified adapter tools")
    return True