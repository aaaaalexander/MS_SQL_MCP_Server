#!/usr/bin/env python
"""
Simple diagnostic script to verify environment variable loading.
Run this to check if environment variables are being loaded correctly from .env file.

Usage:
  python scripts/check_env_vars.py
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import json

# Add the project root to path to ensure imports work
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables from .env file
env_file = project_root / ".env"
if env_file.exists():
    print(f"Loading environment variables from {env_file}")
    load_dotenv(env_file)
else:
    print(f"Warning: .env file not found at {env_file}")
    
print("\n=== Environment Variable Check ===\n")

# Check for SQLMCP_ prefixed variables
sqlmcp_vars = {
    "SQLMCP_DB_SERVER": os.environ.get("SQLMCP_DB_SERVER"),
    "SQLMCP_DB_NAME": os.environ.get("SQLMCP_DB_NAME"),
    "SQLMCP_DB_USERNAME": os.environ.get("SQLMCP_DB_USERNAME"),
    "SQLMCP_DB_PASSWORD": os.environ.get("SQLMCP_DB_PASSWORD", "**HIDDEN**") and "**HIDDEN**",
    "SQLMCP_ALLOWED_SCHEMAS": os.environ.get("SQLMCP_ALLOWED_SCHEMAS"),
    "SQLMCP_DEBUG": os.environ.get("SQLMCP_DEBUG"),
    "SQLMCP_LOG_LEVEL": os.environ.get("SQLMCP_LOG_LEVEL"),
}

# Check for DB_USER_ prefixed variables
db_user_vars = {
    "DB_USER_DB_SERVER": os.environ.get("DB_USER_DB_SERVER"),
    "DB_USER_DB_NAME": os.environ.get("DB_USER_DB_NAME"),
    "DB_USER_DB_USERNAME": os.environ.get("DB_USER_DB_USERNAME"),
    "DB_USER_DB_PASSWORD": os.environ.get("DB_USER_DB_PASSWORD", "**HIDDEN**") and "**HIDDEN**",
    "DB_USER_ALLOWED_SCHEMAS": os.environ.get("DB_USER_ALLOWED_SCHEMAS"),
}

print("SQLMCP_ Variables:")
for var, value in sqlmcp_vars.items():
    status = "✅ SET" if value is not None else "❌ NOT SET"
    print(f"  {var}: {status}")
    
print("\nDB_USER_ Variables:")
for var, value in db_user_vars.items():
    status = "✅ SET" if value is not None else "❌ NOT SET"
    print(f"  {var}: {status}")

print("\n=== SQLMCP_ Values ===\n")
for var, value in sqlmcp_vars.items():
    if "PASSWORD" not in var:
        print(f"  {var}: {value}")
    else:
        print(f"  {var}: {value and '**HIDDEN**'}")

# Try parsing allowed schemas
schemas_var = os.environ.get("SQLMCP_ALLOWED_SCHEMAS")
if schemas_var:
    try:
        schemas = json.loads(schemas_var)
        print(f"\nParsed SQLMCP_ALLOWED_SCHEMAS: {schemas} (type: {type(schemas).__name__})")
    except json.JSONDecodeError as e:
        print(f"\nError parsing SQLMCP_ALLOWED_SCHEMAS: {e}")

print("\n=== Environment Check Complete ===")
