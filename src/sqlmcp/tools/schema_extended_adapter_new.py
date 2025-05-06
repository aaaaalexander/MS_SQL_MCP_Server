"""
Schema Extended Adapter for SQL MCP Server.

This module serves as an adapter layer between the main MCP server and the
enhanced schema exploration tools, providing additional connection management,
parameter validation, and standardized response formatting.
"""
import logging
import asyncio
import time
from typing import Dict, List, Any, Optional, Union, Tuple

# Configure logging
logger = logging.getLogger("DB_USER_SchemaAdapter")

# These will be set from the main server file
mcp = None
_get_db_connection_blocking = None
_execute_query_blocking = None
ALLOWED_SCHEMAS = ["dbo"]
is_safe_query = None

def register_tools(mcp_instance, db_connection=None, db_connection_blocking=None, 
                  execute_query_blocking=None, allowed_schemas=None, is_safe_query_func=None):
    """Register this module's functions with the MCP instance."""
    global mcp, _get_db_connection_blocking, _execute_query_blocking, ALLOWED_SCHEMAS, is_safe_query
    
    mcp = mcp_instance
    _get_db_connection_blocking = db_connection_blocking
    _execute_query_blocking = execute_query_blocking
    
    if allowed_schemas:
        ALLOWED_SCHEMAS = allowed_schemas
    
    if is_safe_query_func:
        global is_safe_query
        is_safe_query = is_safe_query_func
    
    # Register tools manually
    mcp.add_tool(enhanced_list_schemas)
    mcp.add_tool(enhanced_get_sample_data)
    mcp.add_tool(enhanced_search_schema_objects)
    mcp.add_tool(enhanced_find_related_tables)
    mcp.add_tool(enhanced_get_query_examples)
    
    logger.info("Registered schema extended adapter tools with MCP instance")

# Import schema_extended tools and registration function
try:
    from src.sqlmcp.tools import schema_extended
    from src.sqlmcp.tools.schema_extended import (
        list_schemas,
        get_sample_data,
        search_schema_objects,
        find_related_tables,
        get_query_examples,
        register_tools
    )
    TOOLS_AVAILABLE = True
    logger.info("Successfully imported schema_extended tools")
except ImportError as e:
    TOOLS_AVAILABLE = False
    logger.error(f"Failed to import schema_extended tools: {e}")


class SchemaExtendedAdapter:
    """Adapter for enhanced schema exploration tools."""
    
    def __init__(self):
        """Initialize the schema adapter."""
        self.logger = logger
        self.tools_available = TOOLS_AVAILABLE
        
        if self.tools_available:
            logger.info("Schema extended adapter initialized successfully")
        else:
            logger.warning("Schema extended adapter initialized with limited functionality")
    
    async def validate_table_exists(self, schema_name: str, table_name: str) -> Tuple[bool, Optional[str]]:
        """
        Validate that a table exists and is accessible.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            Tuple of (exists, error_message)
        """
        if schema_name not in ALLOWED_SCHEMAS:
            return False, f"Schema '{schema_name}' is not allowed. Must be one of: {', '.join(ALLOWED_SCHEMAS)}"
        
        validate_query = "SELECT COUNT(*) AS count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
        
        try:
            validation_result = await asyncio.to_thread(
                _execute_query_blocking,
                validate_query,
                (schema_name, table_name)
            )
            
            if not validation_result or validation_result[0].get('count', 0) == 0:
                return False, f"Table '{schema_name}.{table_name}' not found or not accessible"
            
            return True, None
        except Exception as e:
            self.logger.error(f"Error validating table existence: {e}")
            return False, f"Error validating table: {str(e)}"
    
    async def parse_table_name(self, table_name: str) -> Tuple[str, str]:
        """
        Parse a table name into schema and table components.
        
        Args:
            table_name: Table name (format: 'schema.table' or just 'table' for default 'dbo' schema)
            
        Returns:
            Tuple of (schema_name, table_name_only)
        """
        parts = table_name.split('.')
        if len(parts) == 2:
            return parts[0], parts[1]
        else:
            return 'dbo', parts[0]
    
    async def validate_and_parse_table(self, table_name: str) -> Tuple[bool, Optional[str], str, str]:
        """
        Parse and validate a table name.
        
        Args:
            table_name: Table name to validate
            
        Returns:
            Tuple of (is_valid, error_message, schema_name, table_name_only)
        """
        # Parse table name
        schema_name, table_name_only = await self.parse_table_name(table_name)
        
        # Validate table exists
        exists, error_message = await self.validate_table_exists(schema_name, table_name_only)
        
        return exists, error_message, schema_name, table_name_only
    
    async def get_available_schemas(self) -> Dict[str, Any]:
        """
        Get list of available schemas with standardized response format.
        
        Returns:
            Dictionary with schemas and metadata
        """
        if not self.tools_available:
            return {
                "error": "Schema extended tools not available",
                "details": "The schema_extended module could not be loaded"
            }
        
        try:
            schemas = await list_schemas()
            
            return {
                "success": True,
                "schemas": schemas,
                "schema_count": len(schemas),
                "allowed_schemas": ALLOWED_SCHEMAS
            }
        except Exception as e:
            self.logger.error(f"Error in get_available_schemas: {e}")
            return {
                "error": "Failed to get available schemas",
                "details": str(e)
            }
    
    async def get_table_sample(self, table_name: str, limit: int = 5) -> Dict[str, Any]:
        """
        Get sample data from a table with additional validation and error handling.
        
        Args:
            table_name: Table name to get sample data from
            limit: Maximum number of rows to return
            
        Returns:
            Dictionary with sample data and error information if applicable
        """
        if not self.tools_available:
            return {
                "error": "Schema extended tools not available",
                "details": "The schema_extended module could not be loaded"
            }
        
        # Validate and parse table name
        valid, error_message, schema_name, table_name_only = await self.validate_and_parse_table(table_name)
        
        if not valid:
            return {
                "error": "Invalid table",
                "details": error_message
            }
        
        # Use the schema_extended tool
        try:
            start_time = time.monotonic()
            sample_data = await get_sample_data(f"{schema_name}.{table_name_only}", limit)
            execution_time = time.monotonic() - start_time
            
            # Add execution time to response
            if isinstance(sample_data, dict) and "error" not in sample_data:
                sample_data["execution_time_seconds"] = round(execution_time, 3)
            
            return sample_data
        except Exception as e:
            self.logger.error(f"Error getting sample data for {schema_name}.{table_name_only}: {e}")
            return {
                "error": "Failed to get sample data",
                "details": str(e),
                "table_name": f"{schema_name}.{table_name_only}"
            }
    
    async def search_objects(self, search_term: str, object_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Search for database objects with enhanced error handling.
        
        Args:
            search_term: Term to search for
            object_types: Types of objects to search for
            
        Returns:
            Dictionary with search results
        """
        if not self.tools_available:
            return {
                "error": "Schema extended tools not available",
                "details": "The schema_extended module could not be loaded"
            }
        
        if not search_term:
            return {
                "error": "Empty search term",
                "details": "Please provide a non-empty search term"
            }
        
        try:
            start_time = time.monotonic()
            search_results = await search_schema_objects(search_term, object_types)
            execution_time = time.monotonic() - start_time
            
            # Add execution time to response
            if isinstance(search_results, dict) and "error" not in search_results:
                search_results["execution_time_seconds"] = round(execution_time, 3)
            
            return search_results
        except Exception as e:
            self.logger.error(f"Error searching schema objects for '{search_term}': {e}")
            return {
                "error": "Failed to search schema objects",
                "details": str(e),
                "search_term": search_term
            }
    
    async def get_related_tables(self, table_name: str) -> Dict[str, Any]:
        """
        Find tables related to the specified table.
        
        Args:
            table_name: Table to find relationships for
            
        Returns:
            Dictionary with related tables
        """
        if not self.tools_available:
            return {
                "error": "Schema extended tools not available",
                "details": "The schema_extended module could not be loaded"
            }
        
        # Validate and parse table name
        valid, error_message, schema_name, table_name_only = await self.validate_and_parse_table(table_name)
        
        if not valid:
            return {
                "error": "Invalid table",
                "details": error_message
            }
        
        try:
            start_time = time.monotonic()
            related_tables = await find_related_tables(f"{schema_name}.{table_name_only}")
            execution_time = time.monotonic() - start_time
            
            # Add execution time to response
            if isinstance(related_tables, dict) and "error" not in related_tables:
                related_tables["execution_time_seconds"] = round(execution_time, 3)
            
            return related_tables
        except Exception as e:
            self.logger.error(f"Error finding related tables for {schema_name}.{table_name_only}: {e}")
            return {
                "error": "Failed to find related tables",
                "details": str(e),
                "table_name": f"{schema_name}.{table_name_only}"
            }
    
    async def generate_query_examples(self, table_name: str) -> Dict[str, Any]:
        """
        Generate SQL query examples for a table.
        
        Args:
            table_name: Table to generate examples for
            
        Returns:
            Dictionary with query examples
        """
        if not self.tools_available:
            return {
                "error": "Schema extended tools not available",
                "details": "The schema_extended module could not be loaded"
            }
        
        # Validate and parse table name
        valid, error_message, schema_name, table_name_only = await self.validate_and_parse_table(table_name)
        
        if not valid:
            return {
                "error": "Invalid table",
                "details": error_message
            }
        
        try:
            start_time = time.monotonic()
            query_examples = await get_query_examples(f"{schema_name}.{table_name_only}")
            execution_time = time.monotonic() - start_time
            
            # Add execution time to response
            if isinstance(query_examples, dict) and "error" not in query_examples:
                query_examples["execution_time_seconds"] = round(execution_time, 3)
                
                # Add additional metadata about example queries
                if "examples" in query_examples:
                    example_types = list(query_examples["examples"].keys())
                    query_examples["available_example_types"] = example_types
                    query_examples["example_count"] = len(example_types)
            
            return query_examples
        except Exception as e:
            self.logger.error(f"Error generating query examples for {schema_name}.{table_name_only}: {e}")
            return {
                "error": "Failed to generate query examples",
                "details": str(e),
                "table_name": f"{schema_name}.{table_name_only}"
            }
    
    async def execute_schema_query(self, query: str, params: Optional[Dict[str, Any]] = None, 
                                 limit: int = 100) -> Dict[str, Any]:
        """
        Execute a SQL query related to schema information with additional validation.
        
        Args:
            query: SQL query (must be SELECT)
            params: Query parameters
            limit: Maximum rows to return
            
        Returns:
            Dictionary with query results
        """
        # Validate query is safe
        if not is_safe_query or not is_safe_query(query):
            return {
                "error": "Invalid query",
                "details": "Only SELECT queries are allowed for security reasons"
            }
        
        # Execute query using the core functionality
        try:
            start_time = time.monotonic()
            
            # Convert parameters to tuple format if provided
            query_params = tuple(params.values()) if params else None
            
            # Execute the query
            results = await asyncio.to_thread(
                _execute_query_blocking,
                query,
                query_params,
                max_rows=limit
            )
            
            execution_time = time.monotonic() - start_time
            
            # Format the results
            response = {
                "success": True,
                "row_count": len(results),
                "results": results,
                "execution_time_seconds": round(execution_time, 3),
                "limit_applied": limit
            }
            
            return response
        except Exception as e:
            self.logger.error(f"Error executing schema query: {e}")
            return {
                "error": "Failed to execute schema query",
                "details": str(e),
                "query": query[:100] + "..." if len(query) > 100 else query
            }

# Create a singleton instance
schema_adapter = SchemaExtendedAdapter()

# Define tool functions that use the adapter
async def enhanced_list_schemas() -> Dict[str, Any]:
    """
    Get a list of all available schemas in the database.
    
    Returns:
        Dictionary with list of schemas and metadata
    """
    logger.info("Handling enhanced_list_schemas request")
    return await schema_adapter.get_available_schemas()

async def enhanced_get_sample_data(table_name: str, limit: int = 5) -> Dict[str, Any]:
    """
    Get sample data from a table with enhanced error handling.
    
    Args:
        table_name: Table name (format: 'schema.table' or just 'table' for default 'dbo' schema)
        limit: Maximum number of rows to return (default: 5)
        
    Returns:
        Dictionary with sample data and metadata
    """
    logger.info(f"Handling enhanced_get_sample_data: table_name={table_name}, limit={limit}")
    return await schema_adapter.get_table_sample(table_name, limit)

async def enhanced_search_schema_objects(
    search_term: str,
    object_types: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Search for database objects (tables, views, columns) matching a specific term.
    
    Args:
        search_term: The keyword to search for (supports partial matches)
        object_types: Types of objects to search (default: ['TABLE', 'VIEW', 'COLUMN'])
        
    Returns:
        Dictionary with search results
    """
    logger.info(f"Handling enhanced_search_schema_objects: search_term={search_term}")
    return await schema_adapter.search_objects(search_term, object_types)

async def enhanced_find_related_tables(table_name: str) -> Dict[str, Any]:
    """
    Find tables related to the specified table through foreign keys.
    
    Args:
        table_name: Table name to find relationships for
        
    Returns:
        Dictionary with related tables information
    """
    logger.info(f"Handling enhanced_find_related_tables: table_name={table_name}")
    return await schema_adapter.get_related_tables(table_name)

async def enhanced_get_query_examples(table_name: str) -> Dict[str, Any]:
    """
    Generate example SQL queries for a specific table.
    
    Args:
        table_name: The name of the table to generate examples for
        
    Returns:
        Dictionary with example queries and metadata
    """
    logger.info(f"Handling enhanced_get_query_examples: table_name={table_name}")
    return await schema_adapter.generate_query_examples(table_name)