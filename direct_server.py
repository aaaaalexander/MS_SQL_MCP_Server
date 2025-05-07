"""
Direct SQL MCP Server using plain FastAPI.

This script starts a simple FastAPI server that wraps the SQL MCP tools.
"""

import os
import sys
import json
import logging
from pathlib import Path
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SQL_MCP_Server")

# Create FastAPI app
app = FastAPI(
    title="SQL MCP Server",
    version="0.4.0",
    description="Provides tools to query SQL Server databases"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Import base tools from the server script
from sql_mcp_server import (
    list_tables, get_table_schema, execute_select, find_foreign_keys,
    _get_db_connection_blocking, _execute_query_blocking
)

# Register available tools
available_tools = {
    "list_tables": list_tables,
    "get_table_schema": get_table_schema,
    "execute_select": execute_select,
    "find_foreign_keys": find_foreign_keys
}

# Try to import and register additional tools
try:
    # Analyze tools
    from src.sqlmcp.tools.analyze_fixed import analyze_table_data, find_duplicate_records
    available_tools["analyze_table_data"] = analyze_table_data
    available_tools["find_duplicate_records"] = find_duplicate_records
    logger.info("Registered analyze tools")
except Exception as e:
    logger.error(f"Failed to register analyze tools: {e}")

try:
    # Metadata tools
    from src.sqlmcp.tools.metadata_fixed import get_database_info, list_stored_procedures, get_procedure_definition
    available_tools["get_database_info"] = get_database_info
    available_tools["list_stored_procedures"] = list_stored_procedures
    available_tools["get_procedure_definition"] = get_procedure_definition
    logger.info("Registered metadata tools")
except Exception as e:
    logger.error(f"Failed to register metadata tools: {e}")

try:
    # Schema extended tools
    from src.sqlmcp.tools.schema_extended import list_schemas, get_sample_data, search_schema_objects, find_related_tables, get_query_examples
    available_tools["list_schemas"] = list_schemas
    available_tools["get_sample_data"] = get_sample_data
    available_tools["search_schema_objects"] = search_schema_objects
    available_tools["find_related_tables"] = find_related_tables
    available_tools["get_query_examples"] = get_query_examples
    logger.info("Registered schema extended tools")
except Exception as e:
    logger.error(f"Failed to register schema extended tools: {e}")

# Basic advanced tools are not registered here due to complexity

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "SQL MCP Server is running",
        "available_tools": list(available_tools.keys())
    }

@app.post("/tools/{tool_name}")
async def call_tool(tool_name: str, request: Request):
    """Call a specific tool with parameters"""
    if tool_name not in available_tools:
        raise HTTPException(status_code=404, detail=f"Tool {tool_name} not found")
    
    # Get the tool function
    tool_function = available_tools[tool_name]
    
    # Parse request body
    try:
        if await request.body():
            params = await request.json()
        else:
            params = {}
    except Exception:
        params = {}
    
    # Log the request
    logger.info(f"Calling tool: {tool_name} with params: {json.dumps(params)}")
    
    # Execute the tool function
    try:
        result = await tool_function(**params)
        return result
    except Exception as e:
        logger.error(f"Error executing {tool_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def main():
    """Main function"""
    # Get host and port
    host = os.environ.get("DB_HOST", "127.0.0.1")
    port = int(os.environ.get("DB_PORT", "8000"))
    
    print(f"SQL MCP Server - Direct FastAPI Server")
    print(f"---------------------------------------")
    print(f"Database: {os.environ.get('DB_SERVER')}/{os.environ.get('DB_NAME')}")
    print(f"Available tools: {', '.join(available_tools.keys())}")
    print(f"Server running at: http://{host}:{port}")
    
    # Start the server
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    main()
