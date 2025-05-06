"""
Tools module initialization.
"""
import logging
import importlib
import sys
from typing import Dict, List, Any, Optional, Callable

logger = logging.getLogger("DB_USER_Tools")

# Define all exported functions
__all__ = [
    # Core schema tools
    'get_table_schema', 
    'list_tables', 
    'find_foreign_keys',
    
    # Core query tools
    'execute_select',
    'get_sample_data',
    'explain_query',
    
    # Analysis tools
    'analyze_table_data',
    'find_duplicate_records',
    
    # Metadata tools
    'get_database_info',
    'list_stored_procedures',
    'get_procedure_definition',
    
    # Extended schema tools
    'list_schemas',
    'get_table_sample',
    'search_schema_objects',
    'find_related_tables',
    'get_query_examples',
    
    # Enhanced schema adapter tools
    'enhanced_list_schemas',
    'enhanced_get_sample_data',
    'enhanced_search_schema_objects',
    'enhanced_find_related_tables',
    'enhanced_get_query_examples',
    'schema_adapter'
]

# Import helper
def import_module_safe(module_name: str) -> Optional[Any]:
    """Safely import a module."""
    try:
        return importlib.import_module(module_name)
    except ImportError as e:
        logger.error(f"Failed to import module {module_name}: {e}")
        return None

# Tool registration
def register_tools(mcp_instance: Any, db_connection_function: Callable) -> None:
    """Register all tools with the MCP instance."""
    logger.info("Registering all SQL MCP tools")
    
    # Define database helper functions
    def _get_db_connection_blocking():
        """Get database connection (blocking version)."""
        from src.sqlmcp.db.connection import get_connection
        from src.sqlmcp.config import settings
        return get_connection(
            server=settings.db_server,
            database=settings.db_name,
            authentication=settings.auth_method,
            username=settings.db_username,
            password=settings.db_password
        )

    def _execute_query_blocking(query: str, params: Optional[List] = None, max_rows: int = 1000) -> List[Dict[str, Any]]:
        """Execute query (blocking version)."""
        from src.sqlmcp.utils.query import execute_query
        return execute_query(query, params, max_rows)
    
    # Import all tool modules
    modules = {
        'analyze': 'src.sqlmcp.tools.analyze_fixed',
        'metadata': 'src.sqlmcp.tools.metadata_fixed',
        'schema': 'src.sqlmcp.tools.schema_fixed',
        'query': 'src.sqlmcp.tools.query_fixed',
        'schema_extended': 'src.sqlmcp.tools.schema_extended',
        'schema_extended_adapter': 'src.sqlmcp.tools.schema_extended_adapter_fixed',
    }
    
    loaded_modules = {}
    
    for name, module_path in modules.items():
        module = import_module_safe(module_path)
        if module:
            loaded_modules[name] = module
            logger.info(f"Successfully imported {name} tools from {module_path}")
            
            # Register tools from the module
            if hasattr(module, 'register_tools'):
                try:
                    if name in ['analyze', 'metadata', 'schema_extended_adapter']:
                        module.register_tools(mcp_instance, db_connection_function, 
                                             _get_db_connection_blocking, _execute_query_blocking)
                    else:
                        module.register_tools(mcp_instance, db_connection_function)
                    logger.info(f"Registered {name} tools with MCP instance")
                except Exception as e:
                    logger.error(f"Failed to register {name} tools: {e}")
            else:
                logger.warning(f"{name} module does not have register_tools function")
        else:
            logger.error(f"Could not load {name} module from {module_path}")
            
    logger.info(f"Tool registration completed. Loaded {len(loaded_modules)} modules.")
    return loaded_modules