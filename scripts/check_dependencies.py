#!/usr/bin/env python
"""
Dependency Compatibility Checker for SQL MCP Server

This script checks installed package versions against requirements
and verifies compatibility between critical dependencies.
"""
import importlib.metadata
import importlib.util
import sys
import subprocess
import os
import pkg_resources

# Colors for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_status(message, status, details=None):
    """Print a status message with color."""
    if status == "OK":
        status_color = f"{Colors.GREEN}[OK]{Colors.ENDC}"
    elif status == "WARNING":
        status_color = f"{Colors.WARNING}[WARNING]{Colors.ENDC}"
    elif status == "ERROR":
        status_color = f"{Colors.FAIL}[ERROR]{Colors.ENDC}"
    elif status == "INFO":
        status_color = f"{Colors.BLUE}[INFO]{Colors.ENDC}"
    else:
        status_color = status
        
    print(f"{status_color} {message}")
    if details:
        print(f"     {Colors.CYAN}{details}{Colors.ENDC}")

def get_installed_version(package_name):
    """Get the installed version of a package."""
    try:
        return importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        return None

def check_importable(module_name, class_name=None):
    """Check if a module and optional class can be imported."""
    try:
        module = importlib.import_module(module_name)
        if class_name:
            try:
                getattr(module, class_name)
                return True, None
            except AttributeError:
                return False, f"Module {module_name} found, but {class_name} class is missing"
        return True, None
    except ImportError as e:
        return False, str(e)

def check_requirements():
    """Check all requirements from requirements.txt."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}Checking package requirements...{Colors.ENDC}")
    
    # Read requirements file
    req_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "requirements.txt")
    if not os.path.exists(req_file):
        print_status("requirements.txt file not found", "ERROR")
        return
        
    with open(req_file, "r") as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    
    # Check each requirement
    for req in requirements:
        # Skip empty lines and comments
        if not req:
            continue
            
        # Parse requirement
        try:
            req_spec = pkg_resources.Requirement.parse(req)
            package_name = req_spec.name
            version_specs = req_spec.specs
        except:
            print_status(f"Failed to parse requirement: {req}", "WARNING")
            continue
            
        # Check if package is installed
        installed_version = get_installed_version(package_name)
        if not installed_version:
            print_status(f"Package {package_name} is not installed", "ERROR", f"Required by: {req}")
            continue
            
        # Check version compatibility
        if version_specs:
            # Convert version spec to string for readability
            specs_str = ", ".join([f"{op} {ver}" for op, ver in version_specs])
            try:
                if pkg_resources.parse_version(installed_version) in req_spec:
                    print_status(f"Package {package_name} version {installed_version}", "OK", f"Requirement: {specs_str}")
                else:
                    print_status(f"Package {package_name} version {installed_version} does not satisfy requirement", "ERROR", f"Required: {specs_str}")
            except:
                print_status(f"Error checking version of {package_name}", "ERROR")
        else:
            print_status(f"Package {package_name} version {installed_version}", "OK", "No version constraint specified")

def check_critical_modules():
    """Check critical module imports."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}Checking critical module imports...{Colors.ENDC}")
    
    # Check pydantic
    pydantic_version = get_installed_version("pydantic")
    if pydantic_version:
        print_status(f"pydantic installed (v{pydantic_version})", "OK")
        
        # Check TypeAdapter
        can_import, error = check_importable("pydantic", "TypeAdapter")
        if can_import:
            print_status("pydantic.TypeAdapter is available", "OK")
        else:
            print_status("pydantic.TypeAdapter is not available", "ERROR", f"Error: {error}")
            print_status("This class was introduced in pydantic v2.0", "INFO")
    else:
        print_status("pydantic is not installed", "ERROR")
    
    # Check MCP
    mcp_version = get_installed_version("mcp")
    if mcp_version:
        print_status(f"mcp installed (v{mcp_version})", "OK")
        
        # Check FastMCP
        can_import, error = check_importable("mcp.server.fastmcp", "FastMCP")
        if can_import:
            print_status("mcp.server.fastmcp.FastMCP is available", "OK")
        else:
            print_status("mcp.server.fastmcp.FastMCP is not available", "ERROR", f"Error: {error}")
    else:
        print_status("mcp (modelcontextprotocol-server) is not installed", "ERROR")
    
    # Check pyodbc
    pyodbc_version = get_installed_version("pyodbc")
    if pyodbc_version:
        print_status(f"pyodbc installed (v{pyodbc_version})", "OK")
        
        can_import, error = check_importable("pyodbc")
        if can_import:
            print_status("pyodbc module is importable", "OK")
        else:
            print_status("pyodbc module is not importable", "ERROR", f"Error: {error}")
            print_status("This may indicate missing ODBC drivers", "INFO")
    else:
        print_status("pyodbc is not installed", "ERROR")

def check_system():
    """Check system environment."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}Checking system environment...{Colors.ENDC}")
    
    # Python version
    py_version = sys.version
    print_status(f"Python version: {py_version.split()[0]}", "INFO")
    if sys.version_info < (3, 8):
        print_status("Python version must be at least 3.8", "ERROR")
    
    # Virtual environment
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print_status(f"Using virtual environment: {sys.prefix}", "OK")
    else:
        print_status("Not using a virtual environment", "WARNING", "A virtual environment is recommended")
    
    # ODBC drivers
    if sys.platform == 'win32':
        try:
            import pyodbc
            drivers = pyodbc.drivers()
            if drivers:
                sql_server_drivers = [d for d in drivers if 'SQL Server' in d]
                if sql_server_drivers:
                    print_status(f"Found SQL Server ODBC drivers: {', '.join(sql_server_drivers)}", "OK")
                else:
                    print_status("No SQL Server ODBC drivers found", "ERROR", f"Available drivers: {', '.join(drivers)}")
            else:
                print_status("No ODBC drivers found", "ERROR")
        except:
            print_status("Failed to check ODBC drivers", "ERROR")
    else:
        print_status("ODBC driver check is only available on Windows", "INFO")

def print_dependency_report():
    """Print a comprehensive dependency report."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}SQL MCP Server Dependency Report{Colors.ENDC}")
    print(f"{Colors.HEADER}==============================={Colors.ENDC}")
    
    check_system()
    check_requirements()
    check_critical_modules()
    
    print(f"\n{Colors.HEADER}{Colors.BOLD}Dependency Analysis Complete{Colors.ENDC}")
    print(f"If you are experiencing issues, please check the error messages above.")
    print(f"For detailed resolution steps, see {Colors.UNDERLINE}DEPENDENCY_RESOLUTION_PLAN.md{Colors.ENDC}")

if __name__ == "__main__":
    print_dependency_report()
