"""
Security utilities for SQL query validation and parameter sanitization.
"""
import re
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# Prohibited SQL keywords that might allow harmful operations
PROHIBITED_KEYWORDS = [
    r'\bINSERT\b',
    r'\bUPDATE\b',
    r'\bDELETE\b',
    r'\bDROP\b',
    r'\bALTER\b',
    r'\bCREATE\b',
    r'\bTRUNCATE\b',
    r'\bEXEC\b',
    r'\bEXECUTE\b',
    r'\bsp_\w+\b',  # Stored procedures
    r'\bxp_\w+\b',  # Extended stored procedures
    r'\bINTO\s+OUTFILE\b',
    r'\bINTO\s+DUMPFILE\b'
]

# Compiled regex patterns for better performance
PROHIBITED_PATTERNS = [re.compile(pattern, re.IGNORECASE) for pattern in PROHIBITED_KEYWORDS]

def is_safe_query(query: str) -> bool:
    """
    Validate if a SQL query is safe to execute.
    
    Args:
        query: The SQL query to validate
        
    Returns:
        Boolean indicating if the query is safe
    """
    # Check if query is a SELECT query
    if not re.match(r'^\s*SELECT\b', query, re.IGNORECASE):
        logger.warning(f"Query rejected: Not a SELECT query")
        return False
    
    # Check for prohibited keywords
    for pattern in PROHIBITED_PATTERNS:
        if pattern.search(query):
            logger.warning(f"Query rejected: Contains prohibited keyword")
            return False
    
    # Check for multiple statements (SQL injection risk)
    if re.search(r';\s*\w', query):
        logger.warning(f"Query rejected: Multiple statements detected")
        return False
    
    # Check for comment indicators that might be used to bypass checks
    if re.search(r'--\s*$|\/\*|\*\/', query):
        logger.warning(f"Query rejected: Comment indicators detected")
        return False
    
    return True

def sanitize_parameters(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize query parameters to prevent SQL injection.
    
    Args:
        params: Dictionary of parameter names and values
        
    Returns:
        Dictionary with sanitized parameter values
    """
    if not params:
        return {}
    
    sanitized = {}
    
    for key, value in params.items():
        # Sanitize strings
        if isinstance(value, str):
            # Remove special characters that might be used for injection
            sanitized[key] = re.sub(r'[\'";\\]', '', value)
        else:
            # Non-string values are inherently safer when using parameterized queries
            sanitized[key] = value
    
    return sanitized

def validate_table_name(table_name: str) -> bool:
    """
    Validate a table name is safe to use in queries.
    
    Args:
        table_name: The table name to validate
        
    Returns:
        Boolean indicating if the table name is safe
    """
    # Check for SQL injection attempts in table name
    if re.search(r'[\'";\\]', table_name):
        return False
    
    # Validate format (allow schema.table notation)
    pattern = r'^(\w+\.)?[\w\$]+$'
    return bool(re.match(pattern, table_name))

def validate_column_name(column_name: str) -> bool:
    """
    Validate a column name is safe to use in queries.
    
    Args:
        column_name: The column name to validate
        
    Returns:
        Boolean indicating if the column name is safe
    """
    # Check for SQL injection attempts in column name
    if re.search(r'[\'";\\]', column_name):
        return False
    
    # Validate format
    pattern = r'^[\w\$]+$'
    return bool(re.match(pattern, column_name))

def validate_access(operation: str, object_name: str, allowed_schemas: List[str]) -> bool:
    """
    Validate access permissions for an operation on a database object.
    
    Args:
        operation: The operation type (e.g., 'SELECT', 'DESCRIBE')
        object_name: The name of the database object
        allowed_schemas: List of schemas the user is allowed to access
        
    Returns:
        Boolean indicating if access is allowed
    """
    # Parse schema from object name
    parts = object_name.split('.')
    if len(parts) == 2:
        schema_name = parts[0]
    else:
        schema_name = 'dbo'  # Default schema
    
    # Check if schema is in allowed list
    if schema_name not in allowed_schemas:
        logger.warning(f"Access denied: Schema '{schema_name}' not in allowed list")
        return False
    
    # Additional access control logic could be implemented here
    # For example, checking specific table permissions
    
    return True
