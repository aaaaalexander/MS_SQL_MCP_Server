# SQL MCP Server Adapter Modules

This documentation explains the adapter pattern used in the SQL MCP Server project and how to create or modify adapter modules.

## Adapter Pattern Overview

The adapter pattern is used to provide enhanced functionality around existing tool modules. In this project, adapters:

1. Add error handling and validation to core tools
2. Standardize response formats for consistency
3. Provide additional metadata and execution statistics
4. Handle dependencies and configuration in a standardized way

## Simplified Adapter Module

The `simplified_adapter.py` module provides an example of how to create a clean adapter implementation that avoids common issues.

### Key Design Principles

1. **Parameter Naming**: Use distinct parameter names for registration functions to avoid conflicts with global variables.
2. **Clear Dependencies**: Explicitly import tools from their modules rather than relying on globals.
3. **Single Responsibility**: Each adapter function performs a specific task with proper error handling.
4. **Enhanced Responses**: Add useful metadata like execution time to responses.

### Usage

To register the simplified adapter tools:

```python
from src.sqlmcp.tools import simplified_adapter

simplified_adapter.register_adapter_tools(
    mcp_instance=mcp,                   # The MCP instance to register tools with
    db_conn_func=db_connection,         # Async database connection function
    db_conn_blocking=db_conn_blocking,  # Blocking database connection function
    exec_query_blocking=exec_query_blocking,  # Query execution function
    schemas=allowed_schemas,            # List of allowed schemas
    query_checker=is_safe_query         # Function to validate query safety
)
```

## Creating New Adapter Modules

When creating a new adapter module, follow these guidelines:

1. **Avoid Global Variable Conflicts**:
   - Never use the same name for a parameter and a global variable
   - Use clear, distinct parameter names in registration functions

2. **Proper Error Handling**:
   - Wrap all tool calls in try/except blocks
   - Log errors with sufficient detail
   - Return standardized error responses

3. **Consistent Response Format**:
   - Use a consistent structure for success and error responses
   - Include metadata like execution time, counts, etc.
   - Format all responses as dictionaries with clear keys

4. **Efficient Resource Usage**:
   - Only import what you need
   - Avoid redundant function calls
   - Reuse functionality rather than duplicating code

## Example: Minimal Adapter Function

```python
async def enhanced_tool_example(parameter1: str, parameter2: int = 10) -> Dict[str, Any]:
    """Enhanced version of a core tool with better error handling."""
    logger.info(f"Handling enhanced_tool_example: parameter1={parameter1}, parameter2={parameter2}")
    
    try:
        # Time the execution
        start_time = time.monotonic()
        
        # Call the original tool
        result = await original_module.original_tool(parameter1, parameter2)
        
        # Calculate execution time
        execution_time = time.monotonic() - start_time
        
        # Enhance the response
        if isinstance(result, dict) and "error" not in result:
            result["execution_time_seconds"] = round(execution_time, 3)
            result["additional_metadata"] = "Some useful information"
        
        return result
    except Exception as e:
        logger.error(f"Error in enhanced_tool_example: {e}")
        return {
            "error": "Failed to execute tool",
            "details": str(e),
            "parameters": {"parameter1": parameter1, "parameter2": parameter2}
        }
```

## Troubleshooting

If you encounter issues with adapter modules:

1. Check for parameter naming conflicts in registration functions
2. Verify that all required tools are properly imported
3. Ensure that global variables are properly set before use
4. Check that each tool is registered only once
5. Use explicit typing for better IDE support

For more detailed information, see `ADAPTER_FIX.md` in the project root.