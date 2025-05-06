"""
Logging configuration and utilities.
"""
import logging
import os
import sys
import re
from typing import Optional
from datetime import datetime

def setup_logging(
    log_level: str = "INFO",
    log_format: Optional[str] = None,
    log_file: Optional[str] = None
) -> None:
    """
    Configure application logging.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Custom log format string
        log_file: Path to log file (if None, logs to console only)
    """
    # Set default format if not provided
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Convert log level string to constant
    level = getattr(logging, log_level.upper())
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler if log file specified
    if log_file:
        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Add file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    logging.info(f"Logging initialized at {datetime.now().isoformat()}")

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Logger name (typically module name)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)

class QueryLogger:
    """
    Specialized logger for SQL queries with sensitive data handling.
    """
    
    def __init__(self, logger_name: str = "DB_USER.query"):
        """
        Initialize query logger.
        
        Args:
            logger_name: Logger name
        """
        self.logger = logging.getLogger(logger_name)
    
    def log_query(
        self, 
        query: str, 
        params: Optional[dict] = None, 
        execution_time: Optional[float] = None,
        rows_affected: Optional[int] = None,
        error: Optional[Exception] = None
    ) -> None:
        """
        Log a SQL query with appropriate handling of sensitive data.
        
        Args:
            query: The SQL query text
            params: Query parameters (will be sanitized)
            execution_time: Query execution time in seconds
            rows_affected: Number of rows affected/returned
            error: Exception if query failed
        """
        # Sanitize query for logging (remove sensitive data)
        sanitized_query = self._sanitize_query(query)
        
        # Sanitize parameters
        sanitized_params = self._sanitize_params(params) if params else None
        
        # Build log message
        message = f"SQL Query: {sanitized_query}"
        
        if sanitized_params:
            message += f" | Params: {sanitized_params}"
        
        if execution_time is not None:
            message += f" | Time: {execution_time:.3f}s"
        
        if rows_affected is not None:
            message += f" | Rows: {rows_affected}"
        
        # Log at appropriate level
        if error:
            self.logger.error(f"{message} | Error: {str(error)}")
        else:
            self.logger.debug(message)
    
    def _sanitize_query(self, query: str) -> str:
        """
        Sanitize SQL query for logging by removing sensitive data.
        
        Args:
            query: The SQL query to sanitize
            
        Returns:
            Sanitized query string
        """
        # Replace password values
        sanitized = query
        
        # Hide passwords in connection strings or queries
        sanitized = re.sub(
            r"(PWD|PASSWORD)\s*=\s*['\"]?[^'\";,\s]+['\"]?", 
            r"\1=*****", 
            sanitized, 
            flags=re.IGNORECASE
        )
        
        return sanitized
    
    def _sanitize_params(self, params: dict) -> dict:
        """
        Sanitize query parameters for logging by removing sensitive data.
        
        Args:
            params: Query parameters to sanitize
            
        Returns:
            Sanitized parameters
        """
        sanitized = {}
        
        for key, value in params.items():
            # Mask potential password/key fields
            if any(sensitive in key.lower() for sensitive in ['password', 'secret', 'key', 'token']):
                sanitized[key] = '*****'
            else:
                sanitized[key] = value
        
        return sanitized
