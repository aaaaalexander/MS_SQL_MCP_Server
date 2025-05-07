"""
Tools loader module for SQL MCP Server.

This module provides a central location for loading and registering all tools
before the server starts. This keeps the main server file clean and makes it 
easier to add new tools in the future.
"""
import logging
import importlib
import sys
import os
import traceback
import asyncio
from typing import Any

logger = logging.getLogger("DB_USER_ToolsLoader")

def register_all_tools(mcp: Any, db_connection=None, db_connection_blocking=None, 
                      execute_query_blocking=None, allowed_schemas=None, 
                      is_safe_query=None):
    """
    Register all available tools with the MCP instance.
    
    Args:
        mcp: The MCP instance to register tools with
        db_connection: Async database connection function
        db_connection_blocking: Blocking database connection function
        execute_query_blocking: Blocking query execution function
        allowed_schemas: List of allowed schemas
        is_safe_query: Function to validate if a query is safe
    
    Returns:
        True if all tools were registered successfully, False otherwise
    """
    success = True
    
    # Core tools are registered directly in the server file
    
    # Register additional tools
    try:
        # Import and register analysis tools
        try:
            from src.sqlmcp.tools import analyze_fixed
            logger.info("Importing analyze_fixed module")
            analyze_fixed.mcp = mcp
            analyze_fixed._get_db_connection_blocking = db_connection_blocking
            analyze_fixed._execute_query_blocking = execute_query_blocking
            
            # Register tools manually
            mcp.add_tool(analyze_fixed.analyze_table_data)
            mcp.add_tool(analyze_fixed.find_duplicate_records)
            logger.info("Successfully registered analyze tools")
        except Exception as e:
            logger.error(f"Failed to import or register analyze tools: {e}")
            logger.error(traceback.format_exc())
            success = False
        
        # Import and register metadata tools
        try:
            from src.sqlmcp.tools import metadata_fixed
            logger.info("Importing metadata_fixed module")
            metadata_fixed.mcp = mcp
            metadata_fixed._get_db_connection_blocking = db_connection_blocking
            metadata_fixed._execute_query_blocking = execute_query_blocking
            
            # Register tools manually
            mcp.add_tool(metadata_fixed.get_database_info)
            mcp.add_tool(metadata_fixed.list_stored_procedures)
            mcp.add_tool(metadata_fixed.get_procedure_definition)
            logger.info("Successfully registered metadata tools")
        except Exception as e:
            logger.error(f"Failed to import or register metadata tools: {e}")
            logger.error(traceback.format_exc())
            success = False
        
        # Import and register schema_extended tools
        try:
            from src.sqlmcp.tools import schema_extended
            logger.info("Importing schema_extended module")
            # Use the module's register_tools function
            schema_extended.register_tools(
                mcp_instance=mcp,
                db_connection=db_connection,
                db_connection_blocking=db_connection_blocking,
                execute_query_blocking=execute_query_blocking,
                allowed_schemas=allowed_schemas
            )
            logger.info("Successfully registered schema_extended tools")
        except Exception as e:
            logger.error(f"Failed to import or register schema_extended tools: {e}")
            logger.error(traceback.format_exc())
            success = False
        
        # Import and register simplified adapter tools
        try:
            from src.sqlmcp.tools import simplified_adapter
            logger.info("Importing simplified_adapter module")
            
            # Register adapter tools
            simplified_adapter.register_adapter_tools(
                mcp_instance=mcp, 
                db_conn_func=db_connection,
                db_conn_blocking=db_connection_blocking, 
                exec_query_blocking=execute_query_blocking,
                schemas=allowed_schemas,
                query_checker=is_safe_query
            )
            logger.info("Successfully registered simplified adapter tools")
        except Exception as e:
            logger.error(f"Failed to import or register simplified adapter tools: {e}")
            logger.error(traceback.format_exc())
            success = False
        
        # Import and register basic_advanced tools
        try:
            # Use the properly structured module
            from src.sqlmcp.tools.basic_advanced import register_tools as register_basic_tools
            logger.info("Importing basic_advanced module (structured)")
            
            # Register tools using the module's register_tools function
            register_basic_tools(
                mcp_instance=mcp, 
                db_connection_function=db_connection,
                db_connection_blocking=db_connection_blocking, 
                execute_query_blocking=execute_query_blocking,
                safe_query_function=is_safe_query
            )
            
            logger.info("Successfully registered basic_advanced tools")
        except Exception as e:
            logger.error(f"Failed to import or register basic_advanced tools: {e}")
            logger.error(traceback.format_exc())
            success = False
            
            # Try registering individual tools
            try:
                from src.sqlmcp.tools.basic_advanced.advanced_inspector import register as register_inspector
                register_inspector(mcp, {}, db_connection_blocking, execute_query_blocking, is_safe_query)
                logger.info("Successfully registered advanced_inspector tools individually")
            except Exception as e2:
                logger.error(f"Failed to register advanced_inspector tools: {e2}")
                
            try:
                from src.sqlmcp.tools.basic_advanced.query_builder import register as register_query
                register_query(mcp, db_connection, db_connection_blocking, execute_query_blocking, is_safe_query)
                logger.info("Successfully registered query_builder tools individually")
            except Exception as e2:
                logger.error(f"Failed to register query_builder tools: {e2}")
                
            try:
                from src.sqlmcp.tools.basic_advanced.data_summary import register as register_summary
                register_summary(mcp, db_connection, db_connection_blocking, execute_query_blocking, is_safe_query)
                logger.info("Successfully registered data_summary tools individually")
            except Exception as e2:
                logger.error(f"Failed to register data_summary tools: {e2}")
                
            try:
                from src.sqlmcp.tools.basic_advanced.export_tools import register as register_export
                register_export(mcp, db_connection, db_connection_blocking, execute_query_blocking, is_safe_query)
                logger.info("Successfully registered export_tools individually")
            except Exception as e2:
                logger.error(f"Failed to register export_tools: {e2}")
        
        # Import and register digest tools if available
        try:
            from src.sqlmcp.tools import digest
            logger.info("Importing digest module")
            digest.mcp = mcp
            digest.get_db_connection = db_connection
            digest._get_db_connection_blocking = db_connection_blocking
            digest._execute_query_blocking = execute_query_blocking
            
            # Register individual tools
            mcp.add_tool(digest.analyze_query_history)
            mcp.add_tool(digest.get_table_field_digest)
            mcp.add_tool(digest.refresh_table_field_digest)
            mcp.add_tool(digest.get_table_recommendations)
            logger.info("Successfully registered digest tools")
        except Exception as e:
            logger.error(f"Failed to import or register digest tools: {e}")
            logger.error(traceback.format_exc())
        
        logger.info("Tool registration completed")
        return success
    except Exception as e:
        logger.error(f"Error in register_all_tools: {e}")
        logger.error(traceback.format_exc())
        return False

def list_registered_tools(mcp: Any) -> list:
    """
    List all registered tools in the MCP instance.
    
    Args:
        mcp: The MCP instance
    
    Returns:
        List of tool names
    """
    try:
        if hasattr(mcp, "_tool_manager") and hasattr(mcp._tool_manager, "list_tools"):
            tool_info = mcp._tool_manager.list_tools()
            tool_names = [tool.name for tool in tool_info]
            return tool_names
        else:
            logger.error("Unable to list tools: MCP instance does not have expected attributes")
            return []
    except Exception as e:
        logger.error(f"Error listing tools: {e}")
        return []


async def initialize_table_field_digest():
    """
    Initialize the table field digest by refreshing it during server startup.
    This function is called automatically when the MCP server starts.
    """
    logger.info("Initializing table field digest...")
    try:
        # Import the digest module
        from src.sqlmcp.tools import digest
        
        # Ensure it's properly initialized
        if hasattr(digest, 'refresh_table_field_digest') and callable(digest.refresh_table_field_digest):
            # Run the refresh with default parameters
            result = await digest.refresh_table_field_digest(
                days_to_analyze=30,
                exclude_system_queries=True,
                min_execution_count=5
            )
            
            if result.get("status") == "success":
                logger.info(f"Table field digest initialized successfully. {result.get('tables_analyzed', 0)} tables analyzed.")
            else:
                logger.warning(f"Table field digest initialization returned: {result.get('message', 'unknown status')}")
        else:
            logger.warning("Table field digest initialization skipped: refresh_table_field_digest function not available")
    except Exception as e:
        logger.error(f"Error initializing table field digest: {e}")
        logger.error(traceback.format_exc())
        # Continue with server startup despite digest initialization failure
        

def run_digest_initialization():
    """
    Run the digest initialization in an async context.
    This function is called from the main server file.
    """
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(initialize_table_field_digest())
    except Exception as e:
        logger.error(f"Failed to run digest initialization: {e}")
