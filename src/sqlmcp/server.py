"""
Main MCP server implementation for SQL Server exploration.
"""
from mcp.server.fastmcp import FastMCP, Context
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass
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

# Create MCP server with context - using standard MCP approach
server = FastMCP(
    "SQL Explorer MCP Server", 
    lifespan=app_lifespan,
    description=f"Explore and query SQL database ({DEFAULT_SQL_DATABASE}) on {DEFAULT_SQL_SERVER}. Use SQL authentication with the provided credentials.",
    dependencies=["pyodbc", "pandas"]
)

def start_server():
    """Start the MCP server."""
    import uvicorn
    
    if not settings:
        logger.error("Cannot start server: Missing configuration")
        return
        
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
