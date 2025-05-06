"""
Simple MCP server test for SQL Server connection.
This file is completely separate from the main project to isolate testing.
"""
import os
import sys
import logging
import pyodbc
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP, Context

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr
)

logger = logging.getLogger("simple_test")

# Set environment variables
os.environ["DB_USER_DB_USERNAME"] = "DB_USER"
os.environ["DB_USER_DB_PASSWORD"] = "YOUR_PASSWORD_HERE"

# SQL Server connection settings
SQL_SERVER = r"\\DB_SERVER\CCDATA"
SQL_DATABASE = "DATABASE_NAME"

@dataclass
class DBConnection:
    """Simple database connection wrapper."""
    connection: pyodbc.Connection
    
    async def execute_query(self, query: str, params=None) -> list:
        """Execute a SQL query and return results as a list of dictionaries."""
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            # Convert to list of dictionaries
            columns = [column[0] for column in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
                
            return results
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

@dataclass
class AppContext:
    """Application context with database connection."""
    db: DBConnection

@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Manage the lifecycle of the MCP server."""
    logger.info("Starting simple test server")
    
    # Connect to database
    try:
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={os.environ['DB_USER_DB_USERNAME']};"
            f"PWD={os.environ['DB_USER_DB_PASSWORD']};"
            f"Trusted_Connection=no;"
        )
        
        connection = pyodbc.connect(connection_string)
        db = DBConnection(connection)
        logger.info(f"Connected to database {SQL_DATABASE} on {SQL_SERVER}")
        
        yield AppContext(db=db)
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        # Return empty context to allow the server to start
        yield AppContext(db=None)
    finally:
        logger.info("Shutting down simple test server")
        # Clean up resources

# Create the server
server = FastMCP(
    "Simple SQL Test",
    lifespan=lifespan,
    description="Simple MCP server for testing SQL Server connection"
)

@server.tool()
async def test_connection(ctx: Context) -> str:
    """Test connection to SQL Server."""
    logger.info("Testing connection")
    
    if not ctx.lifespan_context.db:
        return "No database connection available"
    
    try:
        result = await ctx.lifespan_context.db.execute_query("SELECT @@VERSION AS version")
        return f"Connection successful: {result[0]['version']}"
    except Exception as e:
        return f"Connection failed: {str(e)}"

@server.resource("schema://{schema_name}")
async def get_schema(schema_name: str, ctx: Context) -> str:
    """Get schema information."""
    logger.info(f"Getting schema information for {schema_name}")
    
    if not ctx.lifespan_context.db:
        return "No database connection available"
    
    try:
        query = """
        SELECT 
            TABLE_NAME,
            TABLE_TYPE
        FROM 
            INFORMATION_SCHEMA.TABLES
        WHERE 
            TABLE_SCHEMA = ?
        ORDER BY
            TABLE_NAME
        """
        
        tables = await ctx.lifespan_context.db.execute_query(query, [schema_name])
        
        result = f"# Tables in {schema_name} schema\n\n"
        for table in tables:
            table_name = table.get('TABLE_NAME')
            table_type = table.get('TABLE_TYPE')
            result += f"- {table_name} ({table_type})\n"
        
        return result
    except Exception as e:
        return f"Failed to get schema information: {str(e)}"

# Create stdin middleware to handle 'text' prefix
class StdinMiddleware:
    """Middleware to handle 'text' prefix in JSON messages."""
    def __init__(self):
        self.original_stdin = sys.stdin
        sys.stdin = self
        logger.info("Installed protocol adaptation middleware")
    
    def readline(self):
        """Read a line and remove 'text' prefix if present."""
        line = self.original_stdin.readline()
        if not line:
            return line
            
        # Check for 'text' prefix
        if isinstance(line, bytes) and line.startswith(b'text'):
            logger.info("Removed 'text' prefix from bytes message")
            return line[4:]
        elif isinstance(line, str) and line.startswith('text'):
            logger.info("Removed 'text' prefix from string message")
            return line[4:]
            
        return line
    
    def __getattr__(self, name):
        """Pass through other attribute access to the original stdin."""
        return getattr(self.original_stdin, name)

if __name__ == "__main__":
    # Install middleware
    middleware = StdinMiddleware()
    
    try:
        # Run the server
        import uvicorn
        uvicorn.run(server, host="127.0.0.1", port=8000)
    except ValueError as e:
        if "Mismatch between URI parameters" in str(e):
            logger.error("Parameter order mismatch. MCP requires function parameters to match URI pattern exactly.")
            logger.error(f"Error details: {str(e)}")
        else:
            logger.error(f"Error: {str(e)}")
