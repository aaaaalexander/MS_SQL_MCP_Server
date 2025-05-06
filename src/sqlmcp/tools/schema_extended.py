"""
Extended schema exploration tools for SQL MCP Server.

This module provides additional tools for exploring database schema beyond the
basic list_tables and get_table_schema functionality.
"""
import logging
import asyncio
from typing import Dict, List, Any, Optional
import time

# Configure logging
logger = logging.getLogger("DB_USER_SchemaExt")

# Import the MCP server
try:
    from mcp.server.fastmcp import FastMCP
    logger.info("Successfully imported FastMCP for extended schema tools.")
except ImportError as e:
    logger.error(f"Failed to import FastMCP: {e}")

# These will be set during registration
mcp = None
_get_db_connection_blocking = None
_execute_query_blocking = None
ALLOWED_SCHEMAS = ["dbo"]

def register_tools(mcp_instance, db_connection=None, db_connection_blocking=None, 
                  execute_query_blocking=None, allowed_schemas=None):
    """Register all tools in this module with the MCP instance."""
    global mcp, _get_db_connection_blocking, _execute_query_blocking, ALLOWED_SCHEMAS
    
    mcp = mcp_instance
    _get_db_connection_blocking = db_connection_blocking
    _execute_query_blocking = execute_query_blocking
    
    if allowed_schemas:
        ALLOWED_SCHEMAS = allowed_schemas
    
    # Register tools manually
    mcp.add_tool(list_schemas)
    mcp.add_tool(get_sample_data)
    mcp.add_tool(search_schema_objects)
    mcp.add_tool(find_related_tables)
    mcp.add_tool(get_query_examples)
    
    logger.info("Registered extended schema tools with MCP instance")
    return True

async def list_schemas() -> List[str]:
    """
    Lists all available schemas in the database that are allowed for access.
    
    Returns:
        A list of schema names.
    """
    logger.info("Handling list_schemas request")
    
    try:
        # Query to get schemas
        query = "SELECT DISTINCT schema_name FROM information_schema.schemata ORDER BY schema_name"
        
        schemas_result = await asyncio.to_thread(_execute_query_blocking, query)
        
        # Extract schema names and filter by allowed schemas
        all_schemas = [schema['schema_name'] for schema in schemas_result if schema['schema_name'] in ALLOWED_SCHEMAS]
        
        logger.info(f"Retrieved {len(all_schemas)} allowed schemas")
        return all_schemas
    
    except Exception as e:
        logger.error(f"Error in list_schemas: {e}")
        return []

async def get_sample_data(table_name: str, limit: int = 5) -> Dict[str, Any]:
    """
    Get sample data from a table.
    
    Args:
        table_name: Table name (e.g., 'dbo.Customers' or 'Customers' for default dbo)
        limit: Number of rows to return (default: 5)
        
    Returns:
        Dictionary containing sample data and metadata
    """
    logger.info(f"Handling get_sample_data: table_name={table_name}, limit={limit}")
    
    # Parse schema and table name
    parts = table_name.split('.')
    if len(parts) == 2:
        schema_name, table_name_only = parts
    else:
        schema_name = 'dbo'  # Default schema
        table_name_only = parts[0]
    
    # Check if schema is allowed
    if schema_name not in ALLOWED_SCHEMAS:
        return {
            "error": f"Schema '{schema_name}' is not allowed",
            "details": f"The schema must be one of: {', '.join(ALLOWED_SCHEMAS)}"
        }
    
    try:
        # Verify table exists
        validate_query = "SELECT COUNT(*) AS count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
        
        validation_result = await asyncio.to_thread(
            _execute_query_blocking,
            validate_query,
            (schema_name, table_name_only)
        )
        
        if not validation_result or validation_result[0].get('count', 0) == 0:
            return {
                "error": f"Table '{schema_name}.{table_name_only}' not found",
                "details": "The specified table does not exist or is not accessible"
            }
        
        # Build sample query
        sample_query = f"SELECT TOP {limit} * FROM [{schema_name}].[{table_name_only}]"
        
        sample_data = await asyncio.to_thread(_execute_query_blocking, sample_query, max_rows=limit)
        
        # Get column info
        columns_query = """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        
        columns_info = await asyncio.to_thread(
            _execute_query_blocking,
            columns_query,
            (schema_name, table_name_only)
        )
        
        # Return sample data with metadata
        return {
            "table_name": f"{schema_name}.{table_name_only}",
            "sample_data": sample_data,
            "sample_size": len(sample_data),
            "columns_info": columns_info,
            "limit": limit
        }
    
    except Exception as e:
        logger.error(f"Error in get_sample_data: {e}")
        return {
            "error": "Failed to get sample data",
            "details": str(e)
        }

async def search_schema_objects(
    search_term: str,
    object_types: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Searches for tables, views, or columns matching a specific search term across the database.
    
    Args:
        search_term: The keyword to search for (supports partial matches)
        object_types: Types of objects to search (default: ['TABLE', 'VIEW', 'COLUMN'])
        
    Returns:
        A dictionary containing search matches
    """
    logger.info(f"Handling search_schema_objects: search_term={search_term}")
    
    # Set default object types if not provided
    if object_types is None:
        object_types = ['TABLE', 'VIEW', 'COLUMN']
    
    search_results = {
        'tables': [],
        'views': [],
        'columns': []
    }
    
    try:
        # Search for tables
        if 'TABLE' in object_types:
            table_query = """
            SELECT t.TABLE_SCHEMA, t.TABLE_NAME, 'TABLE' as OBJECT_TYPE
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_TYPE = 'BASE TABLE' 
            AND (t.TABLE_NAME LIKE ? OR t.TABLE_SCHEMA LIKE ?)
            AND t.TABLE_SCHEMA IN (SELECT value FROM STRING_SPLIT(?, ','))
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
            """
            
            tables = await asyncio.to_thread(
                _execute_query_blocking,
                table_query, 
                (f'%{search_term}%', f'%{search_term}%', ','.join(ALLOWED_SCHEMAS))
            )
            search_results['tables'] = tables
        
        # Search for views
        if 'VIEW' in object_types:
            view_query = """
            SELECT t.TABLE_SCHEMA, t.TABLE_NAME, 'VIEW' as OBJECT_TYPE
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_TYPE = 'VIEW' 
            AND (t.TABLE_NAME LIKE ? OR t.TABLE_SCHEMA LIKE ?)
            AND t.TABLE_SCHEMA IN (SELECT value FROM STRING_SPLIT(?, ','))
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
            """
            
            views = await asyncio.to_thread(
                _execute_query_blocking,
                view_query, 
                (f'%{search_term}%', f'%{search_term}%', ','.join(ALLOWED_SCHEMAS))
            )
            search_results['views'] = views
        
        # Search for columns
        if 'COLUMN' in object_types:
            column_query = """
            SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, 
                c.DATA_TYPE, 'COLUMN' as OBJECT_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.COLUMN_NAME LIKE ?
            AND c.TABLE_SCHEMA IN (SELECT value FROM STRING_SPLIT(?, ','))
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
            """
            
            columns = await asyncio.to_thread(
                _execute_query_blocking,
                column_query,
                (f'%{search_term}%', ','.join(ALLOWED_SCHEMAS))
            )
            search_results['columns'] = columns
        
        # Calculate total matches
        total_matches = len(search_results['tables']) + len(search_results['views']) + len(search_results['columns'])
        
        logger.info(f"Search for '{search_term}' found {total_matches} matches")
        return {
            'search_term': search_term,
            'results': search_results,
            'total_matches': total_matches
        }
    
    except Exception as e:
        logger.error(f"Error in search_schema_objects: {e}")
        return {
            "error": "Failed to search schema objects",
            "details": str(e)
        }

async def find_related_tables(table_name: str) -> Dict[str, Any]:
    """
    Identifies tables directly related to the given table via foreign keys (both incoming and outgoing).
    
    Args:
        table_name: The name of the table to find relationships for
        
    Returns:
        A dictionary containing related tables and their relationship details
    """
    logger.info(f"Handling find_related_tables: table_name={table_name}")
    
    # Parse schema and table name
    parts = table_name.split('.')
    if len(parts) == 2:
        schema_name, table_name_only = parts
    else:
        schema_name = 'dbo'  # Default schema
        table_name_only = parts[0]
    
    # Check if schema is allowed
    if schema_name not in ALLOWED_SCHEMAS:
        return {
            "error": f"Schema '{schema_name}' is not allowed",
            "details": f"The schema must be one of: {', '.join(ALLOWED_SCHEMAS)}"
        }
    
    try:
        # Verify table exists
        validate_query = "SELECT COUNT(*) AS count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
        
        validation_result = await asyncio.to_thread(
            _execute_query_blocking,
            validate_query,
            (schema_name, table_name_only)
        )
        
        if not validation_result or validation_result[0].get('count', 0) == 0:
            return {
                "error": f"Table '{schema_name}.{table_name_only}' not found",
                "details": "The specified table does not exist or is not accessible"
            }
        
        relationships = []
        
        # Outgoing relationships (this table references other tables)
        outgoing_fk_query = """
        SELECT 
            fk.name AS constraint_name,
            OBJECT_NAME(fk.parent_object_id) AS source_table,
            SCHEMA_NAME(s1.schema_id) AS source_schema,
            COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS source_column,
            OBJECT_NAME(fk.referenced_object_id) AS target_table,
            SCHEMA_NAME(s2.schema_id) AS target_schema,
            COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS target_column
        FROM 
            sys.foreign_keys fk
        JOIN 
            sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        JOIN 
            sys.tables t1 ON fk.parent_object_id = t1.object_id
        JOIN 
            sys.schemas s1 ON t1.schema_id = s1.schema_id
        JOIN 
            sys.tables t2 ON fk.referenced_object_id = t2.object_id
        JOIN 
            sys.schemas s2 ON t2.schema_id = s2.schema_id
        WHERE 
            s1.name = ? AND t1.name = ?
        """
        
        outgoing_relationships = await asyncio.to_thread(
            _execute_query_blocking,
            outgoing_fk_query,
            (schema_name, table_name_only)
        )
        
        for rel in outgoing_relationships:
            relationships.append({
                'related_table': f"{rel['target_schema']}.{rel['target_table']}",
                'relationship_type': 'outgoing',
                'join_condition': f"{schema_name}.{table_name_only}.{rel['source_column']} = {rel['target_schema']}.{rel['target_table']}.{rel['target_column']}",
                'description': f"{schema_name}.{table_name_only} has a foreign key to {rel['target_schema']}.{rel['target_table']}",
                'constraint_name': rel['constraint_name']
            })
        
        # Incoming relationships (other tables reference this table)
        incoming_fk_query = """
        SELECT 
            fk.name AS constraint_name,
            OBJECT_NAME(fk.parent_object_id) AS source_table,
            SCHEMA_NAME(s1.schema_id) AS source_schema,
            COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS source_column,
            OBJECT_NAME(fk.referenced_object_id) AS target_table,
            SCHEMA_NAME(s2.schema_id) AS target_schema,
            COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS target_column
        FROM 
            sys.foreign_keys fk
        JOIN 
            sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        JOIN 
            sys.tables t1 ON fk.parent_object_id = t1.object_id
        JOIN 
            sys.schemas s1 ON t1.schema_id = s1.schema_id
        JOIN 
            sys.tables t2 ON fk.referenced_object_id = t2.object_id
        JOIN 
            sys.schemas s2 ON t2.schema_id = s2.schema_id
        WHERE 
            s2.name = ? AND t2.name = ?
        """
        
        incoming_relationships = await asyncio.to_thread(
            _execute_query_blocking,
            incoming_fk_query,
            (schema_name, table_name_only)
        )
        
        for rel in incoming_relationships:
            relationships.append({
                'related_table': f"{rel['source_schema']}.{rel['source_table']}",
                'relationship_type': 'incoming',
                'join_condition': f"{rel['source_schema']}.{rel['source_table']}.{rel['source_column']} = {schema_name}.{table_name_only}.{rel['target_column']}",
                'description': f"{rel['source_schema']}.{rel['source_table']} has a foreign key to {schema_name}.{table_name_only}",
                'constraint_name': rel['constraint_name']
            })
        
        logger.info(f"Found {len(relationships)} relationships for {schema_name}.{table_name_only}")
        return {
            'table_name': f"{schema_name}.{table_name_only}",
            'relationships': relationships,
            'relationship_count': len(relationships)
        }
            
    except Exception as e:
        logger.error(f"Error in find_related_tables: {e}")
        return {
            "error": "Failed to find related tables",
            "details": str(e)
        }

async def get_query_examples(table_name: str) -> Dict[str, Any]:
    """
    Generates example SQL queries for a specific table.
    
    Args:
        table_name: The name of the table to generate examples for
        
    Returns:
        A dictionary containing example queries
    """
    logger.info(f"Handling get_query_examples: table_name={table_name}")
    
    # Parse schema and table name
    parts = table_name.split('.')
    if len(parts) == 2:
        schema_name, table_name_only = parts
    else:
        schema_name = 'dbo'  # Default schema
        table_name_only = parts[0]
    
    # Check if schema is allowed
    if schema_name not in ALLOWED_SCHEMAS:
        return {
            "error": f"Schema '{schema_name}' is not allowed",
            "details": f"The schema must be one of: {', '.join(ALLOWED_SCHEMAS)}"
        }
    
    try:
        # Get table schema information
        schema_query = """
        SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE,
               CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY
        FROM INFORMATION_SCHEMA.COLUMNS c LEFT JOIN (
            SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA AND tc.TABLE_NAME = ku.TABLE_NAME
        ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA AND c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
        """
        
        columns = await asyncio.to_thread(
            _execute_query_blocking,
            schema_query,
            (schema_name, table_name_only)
        )
        
        if not columns:
            return {
                "error": f"Table '{schema_name}.{table_name_only}' not found or has no columns",
                "details": "Could not retrieve column information for the specified table"
            }
        
        # Get foreign key relationships
        fk_query = """
        SELECT 
            fk.name constraint_name,
            OBJECT_NAME(fk.parent_object_id) table_name,
            COL_NAME(fkc.parent_object_id, fkc.parent_column_id) column_name,
            OBJECT_NAME(fk.referenced_object_id) referenced_table_name,
            COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) referenced_column_name
        FROM sys.foreign_keys fk JOIN sys.foreign_key_columns fkc ON fk.OBJECT_ID = fkc.constraint_object_id
        JOIN sys.tables t ON fk.parent_object_id = t.object_id JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND OBJECT_NAME(fk.parent_object_id) = ?
        """
        
        foreign_keys = await asyncio.to_thread(
            _execute_query_blocking,
            fk_query,
            (schema_name, table_name_only)
        )
        
        # Build the full table name
        full_table_name = f"[{schema_name}].[{table_name_only}]"
        
        # Extract column names and primary key columns
        primary_keys = [col['COLUMN_NAME'] for col in columns if col['IS_PRIMARY_KEY'] == 'YES']
        
        # Generate example queries
        examples = {}
        
        # Simple SELECT example
        examples["basic_select"] = f"SELECT TOP 10 * FROM {full_table_name}"
        
        # SELECT with specific columns
        if len(columns) > 3:
            # Get first 3 columns for the example
            col_names = [f"[{col['COLUMN_NAME']}]" for col in columns[:3]]
            examples["select_columns"] = f"SELECT {', '.join(col_names)} FROM {full_table_name}"
        
        # SELECT with WHERE clause
        if columns:
            # Use the first non-primary key column for WHERE example
            non_pk_cols = [col for col in columns if col['IS_PRIMARY_KEY'] == 'NO']
            if non_pk_cols:
                where_col = non_pk_cols[0]
                col_name = f"[{where_col['COLUMN_NAME']}]"
                examples["select_where"] = f"SELECT * FROM {full_table_name} WHERE {col_name} = ?"
            elif primary_keys:
                # If no non-PK columns, use first PK column
                col_name = f"[{primary_keys[0]}]"
                examples["select_where"] = f"SELECT * FROM {full_table_name} WHERE {col_name} = ?"
        
        # SELECT with ORDER BY
        if columns:
            order_col = columns[0]['COLUMN_NAME']
            examples["select_order"] = f"SELECT * FROM {full_table_name} ORDER BY [{order_col}] DESC"
        
        # SELECT with GROUP BY (if there are columns suitable for aggregation)
        num_columns = [col for col in columns if col['DATA_TYPE'] in ('int', 'float', 'decimal', 'money', 'bigint', 'smallint', 'tinyint', 'numeric')]
        text_columns = [col for col in columns if col['DATA_TYPE'] in ('varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext')]
        
        if num_columns and text_columns:
            group_col = text_columns[0]['COLUMN_NAME']
            agg_col = num_columns[0]['COLUMN_NAME']
            examples["select_group"] = f"SELECT [{group_col}], AVG([{agg_col}]) as avg_{agg_col} FROM {full_table_name} GROUP BY [{group_col}]"
        
        # SELECT with JOIN (if the table has foreign keys)
        if foreign_keys:
            rel = foreign_keys[0]
            examples["select_join"] = (
                f"SELECT a.*, b.[{rel['referenced_column_name']}] "
                f"FROM {full_table_name} a "
                f"JOIN [{rel['referenced_table_name']}] b ON a.[{rel['column_name']}] = b.[{rel['referenced_column_name']}]"
            )
        
        # COUNT query example
        examples["count_records"] = f"SELECT COUNT(*) AS total_records FROM {full_table_name}"
        
        # INSERT example (using first 3 columns or all if less than 3)
        insert_columns = columns[:min(3, len(columns))]
        insert_col_names = ", ".join([f"[{col['COLUMN_NAME']}]" for col in insert_columns])
        insert_placeholders = ", ".join(["?" for _ in insert_columns])
        examples["insert"] = f"INSERT INTO {full_table_name} ({insert_col_names}) VALUES ({insert_placeholders})"
        
        logger.info(f"Generated query examples for {schema_name}.{table_name_only}")
        return {
            "table_name": f"{schema_name}.{table_name_only}",
            "examples": examples
        }
    
    except Exception as e:
        logger.error(f"Error in get_query_examples: {e}")
        return {
            "error": "Failed to generate query examples",
            "details": str(e)
        }
