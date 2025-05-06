"""
SQL MCP Server using the FastMCP interface from the SDK.
This version properly loads all tools BEFORE starting the server.
"""
import sys
import json
import logging
import traceback
import os
import pyodbc
import asyncio
import time
import importlib
from typing import Dict, List, Any, Optional

# Add the project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger("DB_USER_FastMCP")
logger.info(f"Updated sys.path to include project root: {os.path.dirname(os.path.abspath(__file__))}")

# SDK Imports
try:
    from mcp.server.fastmcp import FastMCP
    logger.info("Successfully imported FastMCP.")
except ImportError as e:
    logger.error(f"Failed to import FastMCP from 'mcp.server.fastmcp': {e}")
    logger.error("Please ensure 'mcp' package (modelcontextprotocol-server) is installed correctly.")
    sys.exit(1)

# Configuration Loading - Priority order: DB_ (standard), SQLMCP_ (transitional), DB_USER_ (legacy)
DB_SERVER = os.environ.get("DB_SERVER", os.environ.get("SQLMCP_DB_SERVER", os.environ.get("DB_USER_DB_SERVER", "localhost")))
DB_NAME = os.environ.get("DB_NAME", os.environ.get("SQLMCP_DB_NAME", os.environ.get("DB_USER_DB_NAME", "database")))
DB_USERNAME = os.environ.get("DB_USERNAME", os.environ.get("SQLMCP_DB_USERNAME", os.environ.get("DB_USER_DB_USERNAME", "")))
DB_PASSWORD = os.environ.get("DB_PASSWORD", os.environ.get("SQLMCP_DB_PASSWORD", os.environ.get("DB_USER_DB_PASSWORD", "")))

# Log which prefix was actually used (helps with debugging)
if os.environ.get("DB_SERVER"):
    logger.info("Using DB_ prefix for configuration.")
elif os.environ.get("SQLMCP_DB_SERVER"):
    logger.info("Using SQLMCP_ prefix for configuration.")
elif os.environ.get("DB_USER_DB_SERVER"):
    logger.info("Using DB_USER_ prefix for configuration.")
else:
    logger.info("No specific environment prefix found, using defaults.")

try:
    # Try all prefixes in priority order: DB_, SQLMCP_, DB_USER_
    allowed_schemas_str = os.environ.get("DB_ALLOWED_SCHEMAS", 
                                      os.environ.get("SQLMCP_ALLOWED_SCHEMAS", 
                                                  os.environ.get("DB_USER_ALLOWED_SCHEMAS", '["dbo"]')))
    
    ALLOWED_SCHEMAS = json.loads(allowed_schemas_str)
    if not isinstance(ALLOWED_SCHEMAS, list):
        raise ValueError("ALLOWED_SCHEMAS must be a JSON list of strings")
    logger.info(f"Allowed schemas loaded: {ALLOWED_SCHEMAS}")
    
    # Log which prefix was used for schemas
    if os.environ.get("DB_ALLOWED_SCHEMAS"):
        logger.info("Using DB_ prefix for allowed schemas.")
    elif os.environ.get("SQLMCP_ALLOWED_SCHEMAS"):
        logger.info("Using SQLMCP_ prefix for allowed schemas.")
    elif os.environ.get("DB_USER_ALLOWED_SCHEMAS"):
        logger.info("Using DB_USER_ prefix for allowed schemas.")
    else:
        logger.info("Using default allowed schemas.")
except (json.JSONDecodeError, ValueError) as e:
    logger.error(f"Invalid ALLOWED_SCHEMAS format: {e}. Using default ['dbo'].")
    ALLOWED_SCHEMAS = ["dbo"]

# Database Connection Logic
_conn: Optional[pyodbc.Connection] = None

def _get_db_connection_blocking() -> pyodbc.Connection:
    """Blocking function to get/create pyodbc connection."""
    global _conn
    is_alive = False
    if _conn:
        try:
            _conn.cursor().execute("SELECT 1").fetchall()
            is_alive = True
        except pyodbc.Error:
            logger.warning("Existing connection seems dead. Reconnecting.")
            try: _conn.close()
            except pyodbc.Error: pass
            _conn = None

    if _conn is None or not is_alive:
        logger.info(f"Creating new connection to {DB_SERVER}/{DB_NAME}")
        conn_str = f"Driver={{SQL Server}};Server={DB_SERVER};Database={DB_NAME};UID={DB_USERNAME};PWD={DB_PASSWORD};"
        alt_server_name = DB_SERVER.replace('\\\\', '').replace('\\', '\\')
        alt_conn_str = f"Driver={{SQL Server}};Server={alt_server_name};Database={DB_NAME};UID={DB_USERNAME};PWD={DB_PASSWORD};"

        connected = False
        last_error = None
        for fmt_name, c_str in [("standard", conn_str), ("alternative", alt_conn_str)]:
            try:
                _conn = pyodbc.connect(c_str, autocommit=True, timeout=10)
                logger.info(f"Connection successful with {fmt_name} format")
                connected = True
                break
            except pyodbc.Error as e:
                logger.warning(f"{fmt_name.capitalize()} connection failed: {str(e)}")
                last_error = e

        if not connected:
            logger.error(f"All connection attempts failed.")
            raise ConnectionError(f"Database connection failed: {last_error}")

    if _conn is None: raise ConnectionError("Failed to establish database connection.")
    return _conn

def _execute_query_blocking(query: str, params: Optional[tuple] = None, max_rows: int = 1000) -> List[Dict[str, Any]]:
    """Blocking function to execute pyodbc query."""
    logger.debug(f"Executing query: {query[:100]}... with params: {params}")
    try:
        connection = _get_db_connection_blocking()
        cursor = connection.cursor()
        cursor.execute(query, params if params else [])

        results = []
        if cursor.description:
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchmany(max_rows) if max_rows > 0 else cursor.fetchall()
            for row in rows:
                results.append(dict(zip(columns, row)))
        else:
            logger.debug("Query did not return rows.")

        cursor.close()
        logger.debug(f"Query successful, {len(results)} rows fetched.")
        return results
    except pyodbc.Error as db_err:
        logger.error(f"Query execution failed: {str(db_err)}")
        raise ValueError(f"Query failed: {str(db_err)}")
    except Exception as e:
        logger.error(f"Unexpected error during query execution: {str(e)}")
        raise ValueError(f"Internal server error during query: {str(e)}")

def is_safe_query(query):
    """Validate if the query is a safe SELECT statement."""
    query = query.strip().upper()
    if not query.startswith("SELECT"): return False
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "EXEC", "EXECUTE", "SP_", "XP_"]
    for word in forbidden:
        if word in query: return False
    return True

# Create FastMCP Server Instance
mcp = FastMCP(
    title="SQL MCP Server", 
    version="0.4.0",
    description=f"Provides tools to query the {DB_NAME} database on {DB_SERVER}, including basic-friendly tools. Allowed schemas: {ALLOWED_SCHEMAS}"
)

# IMPORTANT: Import tools loader BEFORE defining built-in tools
try:
    from src.sqlmcp import tools_loader
    logger.info("Successfully imported tools_loader module")
except ImportError as e:
    logger.error(f"Failed to import tools_loader module: {e}")
    logger.error(traceback.format_exc())
    sys.exit(1)

# Define built-in tools
@mcp.tool()
async def list_tables(schema: Optional[str] = None, include_views: bool = False) -> List[Dict[str, Any]]:
    """
    List tables/views in allowed schemas.
    Args:
        schema: Optional schema name to filter by. Must be one of allowed schemas.
        include_views: Whether to include views (default: false).
    """
    logger.info(f"Handling list_tables: schema={schema}, include_views={include_views}")

    query = "SELECT t.TABLE_SCHEMA, t.TABLE_NAME, t.TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES t WHERE 1=1"
    query_params_list = []

    if schema:
        if schema not in ALLOWED_SCHEMAS:
             raise ValueError(f"Schema '{schema}' is not in the allowed list: {ALLOWED_SCHEMAS}")
        query += " AND t.TABLE_SCHEMA = ?"
        query_params_list.append(schema)
    elif ALLOWED_SCHEMAS:
        schema_placeholders = ", ".join(["?" for _ in ALLOWED_SCHEMAS])
        query += f" AND t.TABLE_SCHEMA IN ({schema_placeholders})"
        query_params_list.extend(ALLOWED_SCHEMAS)
    else:
         raise ValueError("No allowed schemas configured.")

    if not include_views: query += " AND t.TABLE_TYPE = 'BASE TABLE'"
    query += " ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME"

    try:
        tables = await asyncio.to_thread(
            _execute_query_blocking,
            query,
            tuple(query_params_list)
        )
        logger.info(f"Retrieved {len(tables)} tables/views")
        return tables
    except Exception as e:
         logger.error(f"Error in list_tables handler: {e}")
         raise ValueError(f"Failed to list tables: {e}") from e

@mcp.tool()
async def get_table_schema(table_name: str) -> Dict[str, Any]:
    """
    Get schema (columns, FKs) for a table in allowed schemas.
    Args:
        table_name: Table name (e.g., 'dbo.my_table' or 'my_table' for default dbo).
    """
    logger.info(f"Handling get_table_schema: table_name={table_name}")

    if not table_name: raise ValueError("table_name parameter is required.")

    parts = table_name.split('.')
    schema_name, table_name_only = (parts[0], parts[1]) if len(parts) == 2 else ('dbo', parts[0])

    if schema_name not in ALLOWED_SCHEMAS:
        raise ValueError(f"Schema '{schema_name}' is not allowed.")

    # Define queries
    validate_query = "SELECT COUNT(*) AS count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
    columns_query = """
    SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION,
           c.NUMERIC_SCALE, c.IS_NULLABLE, c.COLUMN_DEFAULT,
           CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY
    FROM INFORMATION_SCHEMA.COLUMNS c LEFT JOIN (SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
           FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
           ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME AND tc.CONSTRAINT_SCHEMA = ku.CONSTRAINT_SCHEMA
    ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA AND c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME
    WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? ORDER BY c.ORDINAL_POSITION
    """
    fk_query = """
    SELECT fk.name constraint_name, OBJECT_NAME(fk.parent_object_id) table_name,
           COL_NAME(fkc.parent_object_id, fkc.parent_column_id) column_name,
           OBJECT_NAME(fk.referenced_object_id) referenced_table_name,
           COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) referenced_column_name
    FROM sys.foreign_keys fk JOIN sys.foreign_key_columns fkc ON fk.OBJECT_ID = fkc.constraint_object_id
    JOIN sys.tables t ON fk.parent_object_id = t.object_id JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = ? AND OBJECT_NAME(fk.parent_object_id) = ?
    """

    try:
        # Validate table exists
        validation_result = await asyncio.to_thread(
            _execute_query_blocking, validate_query, (schema_name, table_name_only)
        )
        if not validation_result or validation_result[0].get('count', 0) == 0:
            raise ValueError(f"Table '{schema_name}.{table_name_only}' not found or not allowed.")

        # Get columns and FKs
        columns = await asyncio.to_thread(
            _execute_query_blocking, columns_query, (schema_name, table_name_only)
        )
        foreign_keys = await asyncio.to_thread(
            _execute_query_blocking, fk_query, (schema_name, table_name_only)
        )

        result = {"table_name": f"{schema_name}.{table_name_only}", "columns": columns, "foreign_keys": foreign_keys}
        logger.info(f"Retrieved schema for table {schema_name}.{table_name_only}")
        return result
    except Exception as e:
         logger.error(f"Error in get_table_schema handler: {e}")
         raise ValueError(f"Failed to get table schema: {e}") from e

@mcp.tool()
async def execute_select(query: str, limit: int = 100, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Execute a safe SELECT query.
    Args:
        query: SQL SELECT query (DML/DDL forbidden).
        limit: Max rows to return (default: 100).
        parameters: Query parameters {name: value} (for ? placeholders in order).
    """
    logger.info(f"Handling execute_select: limit={limit}")
    query_params_dict = parameters if parameters is not None else {}

    if not query: raise ValueError("Query parameter is required.")
    if limit < 0: raise ValueError("Limit must be non-negative.")
    if not isinstance(query_params_dict, dict): raise ValueError("Parameters must be a dictionary/object.")

    if not is_safe_query(query):
        raise ValueError("Query validation failed. Only SELECT queries are allowed.")

    # Apply TOP clause
    processed_query = query.strip()
    if limit > 0 and not processed_query.upper().startswith("SELECT TOP "):
         if processed_query.upper().startswith("SELECT"):
              processed_query = f"SELECT TOP {limit} " + processed_query[len("SELECT"):].strip()

    try:
        start_time = time.monotonic()
        results = await asyncio.to_thread(
            _execute_query_blocking,
            processed_query,
            tuple(query_params_dict.values()) if query_params_dict else None,
            max_rows=limit
        )
        execution_time = time.monotonic() - start_time

        # Format response
        columns_meta = []
        if results:
            first_row = results[0]
            for col_name in first_row.keys():
                value = first_row[col_name]
                col_type = type(value).__name__ if value is not None else "unknown"
                columns_meta.append({"name": col_name, "type": col_type})

        response = {
            "success": True, 
            "row_count": len(results), 
            "columns": columns_meta,
            "results": results, 
            "execution_time_seconds": round(execution_time, 3),
            "limit_applied": limit
        }
        logger.info(f"Query executed successfully: {len(results)} rows in {execution_time:.3f}s")
        return response
    except Exception as e:
         logger.error(f"Error in execute_select handler: {e}")
         raise ValueError(f"Failed to execute query: {e}") from e

@mcp.tool()
async def find_foreign_keys(table_name: str) -> List[Dict[str, Any]]:
    """Find foreign key relationships for a table."""
    logger.info(f"Handling find_foreign_keys: table_name={table_name}")
    
    # Parse schema and table name
    parts = table_name.split('.')
    schema_name, table_name_only = (parts[0], parts[1]) if len(parts) == 2 else ('dbo', parts[0])
    
    if schema_name not in ALLOWED_SCHEMAS:
        raise ValueError(f"Schema '{schema_name}' is not allowed.")
    
    # Query for foreign keys
    fk_query = """
    SELECT 
        fk.name AS constraint_name,
        OBJECT_NAME(fk.parent_object_id) AS table_name,
        COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
        OBJECT_NAME(fk.referenced_object_id) AS referenced_table_name,
        COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS referenced_column_name
    FROM sys.foreign_keys fk 
    JOIN sys.foreign_key_columns fkc ON fk.OBJECT_ID = fkc.constraint_object_id
    JOIN sys.tables t ON fk.parent_object_id = t.object_id 
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = ? AND OBJECT_NAME(fk.parent_object_id) = ?
    """
    
    try:
        foreign_keys = await asyncio.to_thread(
            _execute_query_blocking, fk_query, (schema_name, table_name_only)
        )
        logger.info(f"Retrieved {len(foreign_keys)} foreign keys for {schema_name}.{table_name_only}")
        return foreign_keys
    except Exception as e:
        logger.error(f"Error in find_foreign_keys handler: {e}")
        raise ValueError(f"Failed to find foreign keys: {e}") from e

if __name__ == "__main__":
    logger.info("Starting SQL MCP Server with All Tools (FastMCP Version)")
    logger.info(f"Using DB: {DB_SERVER}/{DB_NAME}")
    logger.info(f"Allowed Schemas: {ALLOWED_SCHEMAS}")
    
    # IMPORTANT: Register all tools BEFORE starting the server
    logger.info("Registering all tools...")
    
    # Call the tools_loader to register all tools
    success = tools_loader.register_all_tools(
        mcp=mcp,
        db_connection=None,  # No async version in this file
        db_connection_blocking=_get_db_connection_blocking,
        execute_query_blocking=_execute_query_blocking,
        allowed_schemas=ALLOWED_SCHEMAS,
        is_safe_query=is_safe_query
    )
    
    if not success:
        logger.warning("Some tools could not be registered. Server will continue with limited functionality.")
    
    # List all registered tools to verify
    tool_names = tools_loader.list_registered_tools(mcp)
    logger.info(f"Registered tools ({len(tool_names)}): {', '.join(tool_names)}")
    
    # Check if basic tools are registered
    basic_tools = ['query_table', 'summarize_data', 'export_data']
    missing_tools = [tool for tool in basic_tools if tool not in tool_names]
    
    if missing_tools:
        logger.warning(f"Some basic tools are not registered: {', '.join(missing_tools)}")
    else:
        logger.info("All basic tools are registered successfully!")
    
    # Run the server
    try:
        mcp.run()
    except Exception as e:
         logger.error(f"FATAL: Error running FastMCP server: {e}")
         logger.error(traceback.format_exc())
         sys.exit(1)
    finally:
         logger.info("FastMCP server stopped.")
         if _conn:
             try: 
                 _conn.close()
                 logger.info("Database connection closed.")
             except Exception as e_close: 
                 logger.warning(f"Error closing DB on exit: {e_close}")

    logger.info("Exiting server script.")
    sys.exit(0)
