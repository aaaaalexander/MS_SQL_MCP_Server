"""
Configuration management for the SQL MCP server.
"""
from pydantic_settings import BaseSettings
from pydantic import Field, validator
from typing import Optional, Dict, Any, List
import os
import sys
import json
import logging

logger = logging.getLogger(__name__)

# Default configuration values - can be overridden by environment variables
DEFAULT_SQL_SERVER = "localhost"
DEFAULT_SQL_DATABASE = "database"
DEFAULT_SQL_AUTH_METHOD = "sql"

class Settings(BaseSettings):
    """Application settings with validation."""
    
    # Database connection settings - configurable via environment variables
    db_server: str = Field(DEFAULT_SQL_SERVER, description="SQL Server hostname or IP")
    db_name: str = Field(DEFAULT_SQL_DATABASE, description="Database name")
    auth_method: str = Field(DEFAULT_SQL_AUTH_METHOD, description="Authentication method: 'sql' or 'windows'")
    
    # Configurable credentials - Must be provided via environment variables
    db_username: str = Field(..., description="Database username (for SQL auth)")
    db_password: str = Field(..., description="Database password (for SQL auth)")
    
    # Connection settings
    connection_timeout: int = Field(30, description="Connection timeout in seconds")
    connection_pool_size: int = Field(5, description="Size of connection pool")
    
    # Server settings
    host: str = Field("127.0.0.1", description="Host to bind server to")
    port: int = Field(8000, description="Port to bind server to")
    debug: bool = Field(False, description="Enable debug mode")
    
    # Security settings
    allowed_schemas: List[str] = Field(default_factory=lambda: ["dbo"], description="Allowed database schemas")
    max_rows: int = Field(1000, description="Maximum rows to return from a query")
    query_timeout: int = Field(30, description="Query timeout in seconds")
    read_only: bool = Field(True, description="Allow only read operations")
    
    # Logging settings
    log_level: str = Field("INFO", description="Logging level")
    log_format: str = Field(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format string"
    )
    
    class Config:
        """Pydantic configuration."""
        # No env_prefix - we'll handle this in model_config to support multiple prefixes
        env_file = ".env"  # Enable .env file loading
        env_file_encoding = "utf-8"
        
    model_config = {
        "env_nested_delimiter": "__",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }
        
    @validator("auth_method")
    def validate_auth_method(cls, v):
        """Validate authentication method."""
        if v != "sql":
            raise ValueError("Only SQL authentication is supported")
        return v
        
    @validator("log_level")
    def validate_log_level(cls, v):
        """Validate log level."""
        allowed_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v not in allowed_levels:
            raise ValueError(f"Log level must be one of {allowed_levels}")
        return v
        
    @validator("allowed_schemas", pre=True)
    def parse_allowed_schemas(cls, v):
        """Parse allowed_schemas from various formats."""
        if isinstance(v, str):
            try:
                # Try to parse as JSON
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return parsed
                else:
                    logger.warning(f"Invalid JSON for allowed_schemas: {v}, using default")
                    return ["dbo"]
            except json.JSONDecodeError:
                # Try to parse as comma-separated string
                return [s.strip() for s in v.split(",")]
        elif v is None:
            return ["dbo"]  # Default
        return v

def get_env_var(name: str, default=None) -> Any:
    """Get environment variable with both SQLMCP_ and DB_USER_ prefix support."""
    # Try SQLMCP_ prefix first (new standard)
    value = os.environ.get(f"SQLMCP_{name}", None)
    
    # Fall back to DB_USER_ prefix if SQLMCP_ not found
    if value is None:
        value = os.environ.get(f"DB_USER_{name}", default)
        
    return value

def load_settings() -> Optional[Settings]:
    """Load and validate application settings with multi-prefix support."""
    try:
        # Set environment variables with prefix priority: SQLMCP_ > DB_USER_
        # This ensures pydantic settings can find the variables without prefixes
        for env_var in [
            "DB_SERVER", "DB_NAME", "DB_USERNAME", "DB_PASSWORD", 
            "ALLOWED_SCHEMAS", "HOST", "PORT", "DEBUG", "LOG_LEVEL", 
            "CONNECTION_TIMEOUT", "CONNECTION_POOL_SIZE", "AUTH_METHOD",
            "MAX_ROWS", "QUERY_TIMEOUT", "READ_ONLY"
        ]:
            value = get_env_var(env_var)
            if value is not None:
                # Set the environment variable without prefix for pydantic to find
                os.environ[env_var] = value
                
        # Handle ALLOWED_SCHEMAS specifically since it needs special parsing
        schemas_str = get_env_var("ALLOWED_SCHEMAS")
        if schemas_str:
            try:
                json.loads(schemas_str)  # Validate it's proper JSON
                os.environ["ALLOWED_SCHEMAS"] = schemas_str
            except Exception:
                logger.warning(f"Invalid ALLOWED_SCHEMAS format: {schemas_str}. Using default.")
                os.environ["ALLOWED_SCHEMAS"] = '["dbo"]'
        else:
            os.environ["ALLOWED_SCHEMAS"] = '["dbo"]'
            
        settings = Settings()
        logger.info(f"Loaded configuration for database {settings.db_name} on {settings.db_server}")
        logger.info(f"Allowed database schemas: {settings.allowed_schemas}")
        logger.info(f"Using SQL auth with username: {settings.db_username}")
        
        return settings
    except Exception as e:
        error_msg = f"Failed to load configuration: {str(e)}"
        logger.error(error_msg)
        
        # More detailed error message for missing credentials
        if "db_username" in str(e) or "db_password" in str(e):
            cred_error = (
                "SQL credentials not found. Make sure to provide username and password "
                "via environment variables (SQLMCP_DB_USERNAME/SQLMCP_DB_PASSWORD or "
                "DB_USER_DB_USERNAME/DB_USER_DB_PASSWORD) or Claude Desktop configuration."
            )
            logger.error(cred_error)
            print(cred_error, file=sys.stderr)
        
        return None