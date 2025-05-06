"""
Tools for exploring database schema information.
"""
from mcp.server.fastmcp import Context
import logging
from typing import Dict, List, Any, Optional

from DB_USER.server import server  # Import server to use decorators

logger = logging.getLogger(__name__)

@server.tool()
async def get_table_schema(ctx: Context, table_name: str) -> Dict[str, Any]:
    """
    Retrieve detailed schema information for a specified table.
    
    Args:
        ctx: MCP context containing database connection
        table_name: The name of the table (can include schema as 'schema.table')
    
    Returns:
        Dictionary with table schema details including columns, constraints, and indexes
    """
    db = ctx.lifespan_context.db_pool
    
    # Parse schema and table name
    parts = table_name.split('.')
    if len(parts) == 2:
        schema_name, table_name = parts
    else:
        schema_name = 'dbo'  # Default schema
        table_name = parts[0]
    
    # Validate table exists
    validate_query = """
    SELECT COUNT(*) AS table_exists 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """
    
    result = await db.execute_query(validate_query, {'schema': schema_name, 'table': table_name})
    if not result or result[0].get('table_exists', 0) == 0:
        return {"error": f"Table {schema_name}.{table_name} does not exist"}
    
    # Get column information
    columns_query = """
    SELECT 
        c.COLUMN_NAME, 
        c.DATA_TYPE, 
        c.CHARACTER_MAXIMUM_LENGTH,
        c.NUMERIC_PRECISION, 
        c.NUMERIC_SCALE,
        c.IS_NULLABLE, 
        c.COLUMN_DEFAULT,
        CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY
    FROM INFORMATION_SCHEMA.COLUMNS c
    LEFT JOIN (
        SELECT ku.TABLE_CATALOG, ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
            ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
            AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            AND tc.CONSTRAINT_SCHEMA = ku.CONSTRAINT_SCHEMA
    ) pk 
        ON c.TABLE_CATALOG = pk.TABLE_CATALOG
        AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
        AND c.TABLE_NAME = pk.TABLE_NAME
        AND c.COLUMN_NAME = pk.COLUMN_NAME
    WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
    ORDER BY c.ORDINAL_POSITION
    """
    
    columns = await db.execute_query(columns_query, {'schema': schema_name, 'table': table_name})
    
    # Get foreign key information
    fk_query = """
    SELECT
        fk.name AS constraint_name,
        OBJECT_NAME(fk.parent_object_id) AS table_name,
        COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
        OBJECT_NAME(fk.referenced_object_id) AS referenced_table_name,
        COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS referenced_column_name
    FROM 
        sys.foreign_keys AS fk
    INNER JOIN 
        sys.foreign_key_columns AS fkc 
        ON fk.OBJECT_ID = fkc.constraint_object_id
    INNER JOIN 
        sys.tables t 
        ON fk.parent_object_id = t.object_id
    INNER JOIN 
        sys.schemas s 
        ON t.schema_id = s.schema_id
    WHERE 
        s.name = ? AND OBJECT_NAME(fk.parent_object_id) = ?
    """
    
    foreign_keys = await db.execute_query(fk_query, {'schema': schema_name, 'table': table_name})
    
    # Combine all information
    result = {
        "table_name": f"{schema_name}.{table_name}",
        "columns": columns,
        "foreign_keys": foreign_keys
    }
    
    logger.info(f"Retrieved schema for table {schema_name}.{table_name}")
    return result

@server.tool()
async def list_tables(
    ctx: Context,
    schema: Optional[str] = None, 
    include_views: bool = False
) -> List[Dict[str, Any]]:
    """
    List all tables in the database with optional filtering.
    
    Args:
        ctx: MCP context containing database connection
        schema: Optional schema name to filter by
        include_views: Whether to include views in the results
    
    Returns:
        List of dictionaries with table information
    """
    db = ctx.lifespan_context.db_pool
    
    # Build query based on parameters
    query = """
    SELECT 
        t.TABLE_SCHEMA, 
        t.TABLE_NAME, 
        t.TABLE_TYPE,
        (
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.COLUMNS c 
            WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
        ) AS column_count
    FROM 
        INFORMATION_SCHEMA.TABLES t
    WHERE 1=1
    """
    
    params = {}
    
    # Add schema filter if provided
    if schema:
        query += " AND t.TABLE_SCHEMA = :schema"
        params['schema'] = schema
    
    # Add table type filter if views should be excluded
    if not include_views:
        query += " AND t.TABLE_TYPE = 'BASE TABLE'"
    
    # Add order by
    query += " ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME"
    
    tables = await db.execute_query(query, params)
    
    logger.info(f"Retrieved {len(tables)} tables from database")
    return tables

@server.tool()
async def find_foreign_keys(
    ctx: Context,
    table_name: str, 
    recursive: bool = False
) -> Dict[str, Any]:
    """
    Find foreign key relationships for a table.
    
    Args:
        ctx: MCP context containing database connection
        table_name: The name of the table (can include schema as 'schema.table')
        recursive: Whether to recursively find relationships
    
    Returns:
        Dictionary with incoming and outgoing foreign key relationships
    """
    db = ctx.lifespan_context.db_pool
    
    # Parse schema and table name
    parts = table_name.split('.')
    if len(parts) == 2:
        schema_name, table_name = parts
    else:
        schema_name = 'dbo'  # Default schema
        table_name = parts[0]
    
    # Get outgoing foreign keys (this table references other tables)
    outgoing_query = """
    SELECT
        fk.name AS constraint_name,
        OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS referenced_schema,
        OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
        COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
        COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS referenced_column
    FROM 
        sys.foreign_keys AS fk
    INNER JOIN 
        sys.foreign_key_columns AS fkc 
        ON fk.OBJECT_ID = fkc.constraint_object_id
    INNER JOIN 
        sys.tables t 
        ON fk.parent_object_id = t.object_id
    INNER JOIN 
        sys.schemas s 
        ON t.schema_id = s.schema_id
    WHERE 
        s.name = ? AND OBJECT_NAME(fk.parent_object_id) = ?
    """
    
    outgoing = await db.execute_query(outgoing_query, {'schema': schema_name, 'table': table_name})
    
    # Get incoming foreign keys (other tables reference this table)
    incoming_query = """
    SELECT
        fk.name AS constraint_name,
        OBJECT_SCHEMA_NAME(fk.parent_object_id) AS referencing_schema,
        OBJECT_NAME(fk.parent_object_id) AS referencing_table,
        COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS referencing_column,
        COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS referenced_column
    FROM 
        sys.foreign_keys AS fk
    INNER JOIN 
        sys.foreign_key_columns AS fkc 
        ON fk.OBJECT_ID = fkc.constraint_object_id
    INNER JOIN 
        sys.tables t 
        ON fk.referenced_object_id = t.object_id
    INNER JOIN 
        sys.schemas s 
        ON t.schema_id = s.schema_id
    WHERE 
        s.name = ? AND OBJECT_NAME(fk.referenced_object_id) = ?
    """
    
    incoming = await db.execute_query(incoming_query, {'schema': schema_name, 'table': table_name})
    
    # Build result
    result = {
        "table": f"{schema_name}.{table_name}",
        "outgoing_relationships": outgoing,
        "incoming_relationships": incoming
    }
    
    # If recursive, add child relationships
    if recursive and (outgoing or incoming):
        # Implementation for recursive functionality would go here
        # This would recursively call this function for each related table
        pass
    
    logger.info(f"Found {len(outgoing)} outgoing and {len(incoming)} incoming relationships for {schema_name}.{table_name}")
    return result
