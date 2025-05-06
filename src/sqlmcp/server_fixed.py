"""
Main MCP server implementation for SQL Server exploration.
"""
from mcp.server.fastmcp import FastMCP, Context
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass
import asyncio
import logging
import sys

from DB_USER.config import Settings, load_settings, DEFAULT_SQL_SERVER, DEFAULT_SQL_DATABASE
from DB_USER.db.connection import DBConnectionPool
from DB_USER.utils.logging import setup_logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
    force=True
)

logger = logging.getLogger(__name__)
logger.info("Server module loaded")
setup_logging()

# Load settings
settings = load_settings()

@dataclass
class AppContext:
    """Application context with database connection pool."""
    db_pool: DBConnectionPool

@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Initialize and manage application lifecycle."""
    logger.info("Starting SQL Explorer MCP Server")
    
    if not settings:
        error_msg = "Cannot initialize database connection: Missing configuration"
        logger.error(error_msg)
        print(error_msg, file=sys.stderr)
        # Return empty context - server will start but tools/resources will fail
        yield AppContext(db_pool=None)
        return
    
    # Initialize connection pool
    db_pool = DBConnectionPool(
        server=settings.db_server,
        database=settings.db_name,
        authentication=settings.auth_method,
        username=settings.db_username,
        password=settings.db_password,
        pool_size=settings.connection_pool_size,
        timeout=settings.connection_timeout
    )
    
    try:
        # Connect to database
        await db_pool.initialize()
        logger.info(f"Connected to database {settings.db_name} on {settings.db_server}")
        
        # Yield context to server
        yield AppContext(db_pool=db_pool)
    except Exception as e:
        error_msg = f"Failed to initialize database connection: {str(e)}"
        logger.error(error_msg)
        print(error_msg, file=sys.stderr)
        # Return empty context - server will start but tools/resources will fail
        yield AppContext(db_pool=None)
    finally:
        # Cleanup on shutdown
        if db_pool:
            logger.info("Shutting down SQL Explorer MCP Server")
            await db_pool.close()

# Create MCP server with context
server = FastMCP(
    "SQL Explorer MCP Server", 
    lifespan=app_lifespan,
    description=f"Explore and query SQL database ({DEFAULT_SQL_DATABASE}) on {DEFAULT_SQL_SERVER}. Use SQL authentication with the provided credentials.",
    dependencies=["pyodbc", "pandas"]
)

# Helper functions for tools to use
async def get_db_connection(ctx: Context):
    """Get database connection from context."""
    return ctx.lifespan_context.db_pool

def _get_db_connection_blocking():
    """Get database connection (blocking version)."""
    # This is a placeholder - tools should be rewritten to use async version
    from DB_USER.db.connection import get_connection
    return get_connection(
        server=settings.db_server,
        database=settings.db_name,
        authentication=settings.auth_method,
        username=settings.db_username,
        password=settings.db_password
    )

def _execute_query_blocking(query, params=None):
    """Execute query (blocking version)."""
    # This is a placeholder - tools should be rewritten to use async version
    conn = _get_db_connection_blocking()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or [])
        
        # Convert to list of dictionaries
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        return results
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return []
    finally:
        if conn:
            conn.close()

# Import and register all tools
def register_all_tools():
    """Register all tools with the MCP server."""
    # Import tool modules
    from DB_USER.tools import schema, query, analyze, metadata, schema_extended
    from DB_USER.utils.security import is_safe_query
    from DB_USER.tools import schema_extended_adapter
    
    # Define allowed schemas
    ALLOWED_SCHEMAS = ["dbo"]
    
    # Register schema tools
    schema.register_tools(server, get_db_connection)
    
    # Register query tools
    query.register_tools(server, get_db_connection)
    
    # Register analyze tools
    analyze.register_tools(server, get_db_connection, _get_db_connection_blocking, _execute_query_blocking)
    
    # Register metadata tools
    metadata.register_tools(server, get_db_connection, _get_db_connection_blocking, _execute_query_blocking)
    
    # Register schema_extended tools
    schema_extended.register_tools(server, _get_db_connection_blocking, _execute_query_blocking)
    
    # Register schema_extended_adapter tools
    schema_extended_adapter.register_tools(
        server, 
        _get_db_connection_blocking, 
        _execute_query_blocking,
        ALLOWED_SCHEMAS,
        is_safe_query
    )
    
    logger.info("All tools registered successfully")

def start_server():
    """Start the MCP server."""
    import uvicorn
    
    if not settings:
        logger.error("Cannot start server: Missing configuration")
        return
    
    # Register all tools before starting server
    register_all_tools()
        
    logger.info(f"Starting server on {settings.host}:{settings.port}")
    try:
        uvicorn.run(
            "DB_USER.server:server",
            host=settings.host,
            port=settings.port,
            reload=settings.debug,
            log_level=settings.log_level.lower()
        )
    except Exception as e:
        logger.error(f"Error starting server: {str(e)}")

if __name__ == "__main__":
    start_server()
