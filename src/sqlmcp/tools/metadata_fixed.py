"""
Database metadata tools for SQL MCP Server.

This module provides tools for retrieving metadata about the database server,
available databases, and database objects.
"""
import logging
import asyncio
from typing import Dict, List, Any, Optional
import time

# Configure logging
logger = logging.getLogger("DB_USER_Metadata")

# These will be set from the main server file
mcp = None
get_db_connection = None
_get_db_connection_blocking = None
_execute_query_blocking = None

def register_tools(mcp_instance, db_connection_function, db_connection_blocking, execute_query_blocking):
    """Register this module's functions with the MCP instance."""
    global mcp, get_db_connection, _get_db_connection_blocking, _execute_query_blocking
    mcp = mcp_instance
    get_db_connection = db_connection_function
    _get_db_connection_blocking = db_connection_blocking
    _execute_query_blocking = execute_query_blocking
    
    # Register tools manually
    mcp.add_tool(get_database_info)
    mcp.add_tool(list_stored_procedures)
    mcp.add_tool(get_procedure_definition)
    
    logger.info("Registered metadata tools with MCP instance")

async def get_database_info() -> Dict[str, Any]:
    """
    Retrieve information about the current database server and database.
    
    Returns:
        Dictionary containing database server version, database properties,
        and configuration information.
    """
    logger.info("Handling get_database_info request")
    
    try:
        # Get server version and edition
        version_query = """
        SELECT 
            CONVERT(NVARCHAR(100), SERVERPROPERTY('ProductVersion')) AS product_version,
            CONVERT(NVARCHAR(100), SERVERPROPERTY('ProductLevel')) AS product_level,
            CONVERT(NVARCHAR(100), SERVERPROPERTY('Edition')) AS edition,
            SERVERPROPERTY('EngineEdition') AS engine_edition,
            SERVERPROPERTY('ServerName') AS server_name,
            DB_NAME() AS current_database
        """
        
        version_result = await asyncio.to_thread(
            _execute_query_blocking,
            version_query
        )
        
        # Get database properties
        db_props_query = """
        SELECT 
            d.name AS database_name,
            d.create_date,
            d.compatibility_level,
            d.collation_name,
            d.is_read_only,
            d.state_desc,
            d.recovery_model_desc,
            SUM(CAST(mf.size AS BIGINT) * 8192) / 1024 / 1024 AS size_mb
        FROM 
            sys.databases d
        JOIN 
            sys.master_files mf ON d.database_id = mf.database_id
        WHERE 
            d.name = DB_NAME()
        GROUP BY 
            d.name, d.create_date, d.compatibility_level, d.collation_name, 
            d.is_read_only, d.state_desc, d.recovery_model_desc
        """
        
        db_props_result = await asyncio.to_thread(
            _execute_query_blocking,
            db_props_query
        )
        
        # Get database files
        files_query = """
        SELECT 
            f.name AS file_name,
            f.type_desc AS file_type,
            f.physical_name,
            CAST(f.size AS BIGINT) * 8192 / 1024 / 1024 AS size_mb,
            f.max_size,
            f.growth,
            f.is_percent_growth
        FROM 
            sys.database_files f
        """
        
        files_result = await asyncio.to_thread(
            _execute_query_blocking,
            files_query
        )
        
        # Get database options - using simpler query to avoid ODBC SQL type -16 issues
        options_query = """
        SELECT name, 
               CAST(value AS VARCHAR(100)) AS value, 
               CAST(value_in_use AS VARCHAR(100)) AS value_in_use
        FROM sys.configurations
        WHERE name IN (
            'max server memory (MB)', 
            'min server memory (MB)', 
            'max degree of parallelism', 
            'cost threshold for parallelism',
            'default language',
            'fill factor (%)'
        )
        """
        
        options_result = await asyncio.to_thread(
            _execute_query_blocking,
            options_query
        )
        
        # Format database creation date
        if db_props_result and db_props_result[0].get('create_date'):
            create_date = db_props_result[0]['create_date']
            db_props_result[0]['create_date'] = create_date.isoformat() if hasattr(create_date, 'isoformat') else str(create_date)
        
        # Format result
        result = {
            "server_info": version_result[0] if version_result else {},
            "database_properties": db_props_result[0] if db_props_result else {},
            "database_files": files_result,
            "configuration_options": {row['name']: row['value_in_use'] for row in options_result} if options_result else {}
        }
        
        logger.info("Retrieved database information successfully")
        return result
    
    except Exception as e:
        # Handle XML type error
        if "ODBC SQL type" in str(e) and "-16" in str(e):
            logger.warning(f"XML type conversion error in get_database_info: {e}")
            # Return simplified info without the XML parts
            result = {
                "server": DB_SERVER,
                "database": DB_NAME,
                "version": "Unknown (XML type error)",
                "edition": "Unknown (XML type error)"
            }
            return result

        logger.error(f"Error in get_database_info: {e}")
        return {
            "error": "Failed to retrieve database information",
            "details": str(e)
        }

async def list_stored_procedures(
    schema_name: Optional[str] = None,
    name_pattern: Optional[str] = None
) -> Dict[str, Any]:
    """
    List stored procedures in the current database with optional filtering.
    
    Args:
        schema_name: Optional schema name to filter procedures by
        name_pattern: Optional name pattern to search for (uses SQL LIKE pattern)
        
    Returns:
        Dictionary containing list of stored procedures with metadata
    """
    logger.info(f"Handling list_stored_procedures: schema_name={schema_name}, name_pattern={name_pattern}")
    
    try:
        # Build the query
        query = """
        SELECT 
            SCHEMA_NAME(p.schema_id) AS schema_name,
            p.name AS procedure_name,
            p.create_date,
            p.modify_date,
            p.is_ms_shipped,
            OBJECTPROPERTY(p.object_id, 'ExecIsAnsiNullsOn') AS is_ansi_nulls_on,
            OBJECTPROPERTY(p.object_id, 'ExecIsQuotedIdentOn') AS is_quoted_identifier_on,
            OBJECTPROPERTY(p.object_id, 'IsSchemaBound') AS is_schema_bound,
            CAST(CASE WHEN sm.definition IS NULL THEN 0 ELSE 1 END AS bit) AS has_definition
        FROM 
            sys.procedures p
        LEFT JOIN 
            sys.sql_modules sm ON p.object_id = sm.object_id
        WHERE 
            1=1
        """
        
        query_params = []
        
        # Add schema filter if provided
        if schema_name:
            query += " AND SCHEMA_NAME(p.schema_id) = ?"
            query_params.append(schema_name)
        
        # Add name pattern filter if provided
        if name_pattern:
            query += " AND p.name LIKE ?"
            query_params.append(f"%{name_pattern}%")
        
        # Order the results
        query += " ORDER BY SCHEMA_NAME(p.schema_id), p.name"
        
        # Execute the query
        procedures = await asyncio.to_thread(
            _execute_query_blocking,
            query,
            tuple(query_params) if query_params else None
        )
        
        # Format dates
        for proc in procedures:
            if proc.get('create_date'):
                create_date = proc['create_date']
                proc['create_date'] = create_date.isoformat() if hasattr(create_date, 'isoformat') else str(create_date)
            
            if proc.get('modify_date'):
                modify_date = proc['modify_date']
                proc['modify_date'] = modify_date.isoformat() if hasattr(modify_date, 'isoformat') else str(modify_date)
        
        logger.info(f"Retrieved {len(procedures)} stored procedures")
        return {
            "procedures_count": len(procedures),
            "procedures": procedures
        }
    
    except Exception as e:
        logger.error(f"Error in list_stored_procedures: {e}")
        return {
            "error": "Failed to list stored procedures",
            "details": str(e)
        }

async def get_procedure_definition(procedure_name: str) -> Dict[str, Any]:
    """
    Retrieve the definition and parameter information for a stored procedure.
    
    Args:
        procedure_name: The name of the stored procedure (format: 'schema.procedure' or just 'procedure' for default 'dbo' schema)
        
    Returns:
        Dictionary containing procedure definition, parameters, and metadata
    """
    logger.info(f"Handling get_procedure_definition: procedure_name={procedure_name}")
    
    # Parse schema and procedure name
    parts = procedure_name.split('.')
    if len(parts) == 2:
        schema_name, procedure_name_only = parts
    else:
        schema_name = 'dbo'  # Default schema
        procedure_name_only = parts[0]
    
    try:
        # Check if procedure exists
        exists_query = """
        SELECT 
            OBJECT_ID(?) AS procedure_id
        """
        
        full_name = f"[{schema_name}].[{procedure_name_only}]"
        exists_result = await asyncio.to_thread(
            _execute_query_blocking,
            exists_query,
            (full_name,)
        )
        
        if not exists_result or not exists_result[0]['procedure_id']:
            return {
                "error": f"Stored procedure '{schema_name}.{procedure_name_only}' not found",
                "details": "The specified procedure does not exist or is not accessible"
            }
        
        # Get procedure definition
        definition_query = """
        SELECT 
            SCHEMA_NAME(p.schema_id) AS schema_name,
            p.name AS procedure_name,
            sm.definition AS procedure_definition,
            p.create_date,
            p.modify_date,
            p.is_ms_shipped,
            OBJECTPROPERTY(p.object_id, 'ExecIsAnsiNullsOn') AS is_ansi_nulls_on,
            OBJECTPROPERTY(p.object_id, 'ExecIsQuotedIdentOn') AS is_quoted_identifier_on
        FROM 
            sys.procedures p
        JOIN 
            sys.sql_modules sm ON p.object_id = sm.object_id
        WHERE 
            SCHEMA_NAME(p.schema_id) = ? AND p.name = ?
        """
        
        definition_result = await asyncio.to_thread(
            _execute_query_blocking,
            definition_query,
            (schema_name, procedure_name_only)
        )
        
        if not definition_result:
            return {
                "error": f"Definition for stored procedure '{schema_name}.{procedure_name_only}' not found",
                "details": "The procedure exists but its definition is not available"
            }
        
        # Get procedure parameters
        params_query = """
        SELECT 
            p.name AS parameter_name,
            t.name AS data_type,
            p.max_length,
            p.precision,
            p.scale,
            p.is_output,
            p.is_nullable,
            p.is_cursor_ref,
            p.has_default_value,
            p.default_value
        FROM 
            sys.parameters p
        JOIN 
            sys.procedures sp ON p.object_id = sp.object_id
        JOIN 
            sys.types t ON p.system_type_id = t.system_type_id AND p.user_type_id = t.user_type_id
        WHERE 
            SCHEMA_NAME(sp.schema_id) = ? AND sp.name = ?
        ORDER BY 
            p.parameter_id
        """
        
        params_result = await asyncio.to_thread(
            _execute_query_blocking,
            params_query,
            (schema_name, procedure_name_only)
        )
        
        # Format dates
        proc_info = definition_result[0]
        if proc_info.get('create_date'):
            create_date = proc_info['create_date']
            proc_info['create_date'] = create_date.isoformat() if hasattr(create_date, 'isoformat') else str(create_date)
        
        if proc_info.get('modify_date'):
            modify_date = proc_info['modify_date']
            proc_info['modify_date'] = modify_date.isoformat() if hasattr(modify_date, 'isoformat') else str(modify_date)
        
        # Build result
        result = {
            "schema_name": schema_name,
            "procedure_name": procedure_name_only,
            "full_name": f"{schema_name}.{procedure_name_only}",
            "definition": proc_info.get('procedure_definition', ''),
            "created": proc_info.get('create_date'),
            "last_modified": proc_info.get('modify_date'),
            "is_system_procedure": proc_info.get('is_ms_shipped', False),
            "parameters": params_result
        }
        
        logger.info(f"Retrieved definition for procedure {schema_name}.{procedure_name_only}")
        return result
    
    except Exception as e:
        logger.error(f"Error in get_procedure_definition: {e}")
        return {
            "error": "Failed to get procedure definition",
            "details": str(e)
        }
