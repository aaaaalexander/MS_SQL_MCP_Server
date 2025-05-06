"""Check MCP version and details."""
import sys
import importlib
import inspect

def check_module_availability(module_name):
    """Check if a module is available and get its version."""
    try:
        module = importlib.import_module(module_name)
        version = getattr(module, "__version__", "unknown")
        print(f"{module_name} is available (version: {version})")
        return module
    except ImportError:
        print(f"{module_name} is not available")
        return None

def check_mcp_resource_details():
    """Check details of the MCP resource function signature."""
    try:
        from mcp.server.fastmcp.server import _ResourceFunction
        
        print("\nResource function details:")
        print(f"Class: {_ResourceFunction}")
        print(f"Signature: {inspect.signature(_ResourceFunction.__init__)}")
        
        # Check FastMCP resource method
        from mcp.server.fastmcp import FastMCP
        print("\nFastMCP.resource method details:")
        print(f"Method: {FastMCP.resource}")
        print(f"Signature: {inspect.signature(FastMCP.resource)}")
    except ImportError as e:
        print(f"Error checking MCP resource details: {e}")
    except Exception as e:
        print(f"Error: {e}")

def main():
    """Check module versions and details."""
    print(f"Python version: {sys.version}")
    print(f"Python executable: {sys.executable}")
    print()
    
    # Check key modules
    check_module_availability("mcp")
    check_module_availability("pydantic")
    check_module_availability("pyodbc")
    
    # Check MCP resource details
    check_mcp_resource_details()

if __name__ == "__main__":
    main()
