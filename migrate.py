"""
Migration script for SQL MCP Server.

This script implements the migration plan by:
1. Backing up original files
2. Copying fixed versions to replace originals
3. Updating server registration
"""
import os
import shutil
import sys
from pathlib import Path

# Base paths
PROJECT_DIR = Path(__file__).parent
SRC_DIR = PROJECT_DIR / 'src' / 'DB_USER'
TOOLS_DIR = SRC_DIR / 'tools'
BACKUP_DIR = PROJECT_DIR / 'backup'

# Ensure backup directory exists
BACKUP_DIR.mkdir(exist_ok=True)

# Files to backup and replace
FILES_TO_MIGRATE = [
    ('metadata.py', 'metadata_fixed.py'),
    ('query.py', 'query_fixed.py'),
    ('schema_extended_adapter.py', 'schema_extended_adapter_fixed.py'),
    ('__init__.py', '__init__fixed.py')
]

def backup_files():
    """Backup original files."""
    print("Backing up original files...")
    for original, _ in FILES_TO_MIGRATE:
        original_path = TOOLS_DIR / original
        backup_path = BACKUP_DIR / f"{original}.bak"
        
        if original_path.exists():
            try:
                shutil.copy2(original_path, backup_path)
                print(f"  Backed up {original} to {backup_path}")
            except Exception as e:
                print(f"  Error backing up {original}: {e}")
                return False
        else:
            print(f"  Warning: Original file {original} not found, skipping backup")
    
    return True

def replace_files():
    """Replace original files with fixed versions."""
    print("Replacing files with fixed versions...")
    for original, fixed in FILES_TO_MIGRATE:
        original_path = TOOLS_DIR / original
        fixed_path = TOOLS_DIR / fixed
        
        if fixed_path.exists():
            try:
                shutil.copy2(fixed_path, original_path)
                print(f"  Replaced {original} with {fixed}")
            except Exception as e:
                print(f"  Error replacing {original}: {e}")
                return False
        else:
            print(f"  Error: Fixed file {fixed} not found")
            return False
    
    return True

def update_server():
    """Update server registration for schema_extended_adapter."""
    server_path = SRC_DIR / 'server_fixed.py'
    
    if not server_path.exists():
        print(f"Error: Server file {server_path} not found")
        return False
    
    try:
        with open(server_path, 'r') as f:
            content = f.read()
        
        # Check if the adapter is already registered
        if 'schema_extended_adapter.register_tools' in content:
            print("  Schema adapter already registered in server file")
            return True
        
        # Update the register_all_tools function to include schema_extended_adapter
        import_line = 'from DB_USER.utils.security import is_safe_query'
        if import_line not in content:
            print("  Error: Could not find expected import line in server file")
            return False
        
        new_import = import_line + '\nfrom DB_USER.tools import schema_extended_adapter'
        content = content.replace(import_line, new_import)
        
        # Find position to add registration code
        register_end = content.find('logger.info("All tools registered successfully")')
        if register_end == -1:
            print("  Error: Could not find registration function end marker")
            return False
        
        # Prepare the registration code
        registration_code = """    # Register schema_extended_adapter tools
    schema_extended_adapter.register_tools(
        server, 
        _get_db_connection_blocking, 
        _execute_query_blocking,
        ALLOWED_SCHEMAS,
        is_safe_query
    )
    
    """
        
        # Insert registration code before the logger.info line
        updated_content = content[:register_end] + registration_code + content[register_end:]
        
        # Write updated file
        with open(server_path, 'w') as f:
            f.write(updated_content)
        
        print("  Updated server file to register schema_extended_adapter")
        return True
    
    except Exception as e:
        print(f"  Error updating server file: {e}")
        return False

def main():
    """Execute the migration steps."""
    print("Starting SQL MCP Server migration...")
    
    # Step 1: Backup files
    if not backup_files():
        print("Migration aborted due to backup failure")
        return False
    
    # Step 2: Replace files
    if not replace_files():
        print("Migration aborted due to file replacement failure")
        print("Restoring backups is recommended")
        return False
    
    # Step 3: Update server registration
    if not update_server():
        print("Migration had issues updating the server file")
        print("Please check the server_fixed.py file manually")
    
    print("Migration completed successfully!")
    print("Next steps:")
    print("1. Run tests using run_fixed_sql_mcp.bat")
    print("2. Check server logs for any errors")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
