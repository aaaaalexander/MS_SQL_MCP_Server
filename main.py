"""
Main entry point for the SQL MCP Server with Model Context Protocol integration.
"""
import sys
import os
import logging
import traceback
import json
import re
import time

# Configure logging to stderr
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr
)

logger = logging.getLogger("DB_USER")

# Add the src directory to the path so imports work correctly
src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
sys.path.insert(0, src_dir)

# Store original stdin/stdout for reference
original_stdin = sys.stdin
original_stdout = sys.stdout

def set_default_env_vars():
    """Set default environment variables if they don't exist."""
    defaults = {
        "DB_USER_ALLOWED_SCHEMAS": '["dbo"]',
        "DB_USER_DEBUG": "true",
        "DB_USER_LOG_LEVEL": "DEBUG"
    }
    
    for key, value in defaults.items():
        if key not in os.environ:
            os.environ[key] = value
            logger.info(f"Set default environment variable: {key}={value}")
    
    logger.info(f"Environment variables: {get_env_vars()}")

def get_env_vars():
    """Get DB_USER environment variables for logging."""
    return {k: v if 'PASSWORD' not in k else '[REDACTED]' 
            for k, v in os.environ.items() 
            if k.startswith('DB_USER_')}

class RobustStdinMiddleware:
    """Enhanced middleware to handle various JSON message formats."""
    def __init__(self):
        self.original_stdin = sys.stdin
        sys.stdin = self
        logger.info("Installed protocol adaptation middleware")
        self.debug_count = 3
    
    def readline(self):
        """Read a line with enhanced error handling and preprocessing."""
        try:
            # Read a line from original stdin
            line = self.original_stdin.readline()
            
            # Debug the first few lines
            if self.debug_count > 0:
                self.debug_count -= 1
                self.debug_input(line)
            
            if not line:
                return line
                
            # Process the line to handle various format issues
            processed_line = self.process_line(line)
            
            # If processing changed the line, log it
            if processed_line != line:
                logger.info("Input line was preprocessed")
                
            return processed_line
            
        except Exception as e:
            logger.error(f"Error in stdin middleware: {str(e)}")
            logger.error(traceback.format_exc())
            return line  # Return original line if processing fails
    
    def process_line(self, line):
        """Process a line to handle various format issues."""
        if not line:
            return line
            
        # Convert bytes to string if needed
        if isinstance(line, bytes):
            try:
                line_str = line.decode('utf-8')
                
                # Handle 'text' prefix in bytes
                if line.startswith(b'text'):
                    logger.info("Removing 'text' prefix from bytes message")
                    return line[4:]
                    
            except UnicodeDecodeError:
                logger.warning("Failed to decode bytes to string")
                return line
        else:
            line_str = line
            
            # Handle 'text' prefix in string
            if line_str.startswith('text'):
                logger.info("Removing 'text' prefix from string message")
                line_str = line_str[4:]
                return line_str
            
        # Check for other prefixes using regex for known prefix patterns
        if isinstance(line_str, str):
            # Handle common prefixes with a more specific pattern
            prefix_match = re.match(r'^(\d{4}-\d{2}-\d{2}.*?|[a-z]+\s+)(\{.*)', line_str)
            if prefix_match:
                logger.info(f"Detected protocol prefix - extracting JSON part")
                json_part = prefix_match.group(2)
                # Validate the extracted JSON
                try:
                    json.loads(json_part)
                    logger.debug("Successfully extracted valid JSON from prefixed message")
                    return json_part
                except json.JSONDecodeError:
                    logger.warning("Extracted text is not valid JSON")
        
        # Validate JSON or try to extract it
        if isinstance(line_str, str):
            # First check if the entire string is valid JSON
            try:
                json.loads(line_str)
                logger.debug("Line contains valid JSON")
                return line_str
            except json.JSONDecodeError:
                logger.warning("Line does not contain valid JSON")
                
                # Try to extract JSON-like content
                try:
                    # Look for JSON objects (either complete or partial)
                    json_match = re.search(r'(\{(?:[^{}]|(?1))*\})', line_str)
                    if json_match:
                        potential_json = json_match.group(1)
                        try:
                            json.loads(potential_json)  # Validate
                            logger.info("Found and extracted valid JSON within message")
                            return potential_json
                        except json.JSONDecodeError:
                            logger.warning("Extracted potential JSON is invalid")
                except Exception as ex:
                    logger.error(f"Error during JSON extraction: {str(ex)}")
        
        return line  # Return original or decoded line if no processing was needed
    
    def debug_input(self, data, prefix="INPUT"):
        """Debug input data in various formats."""
        try:
            if data:
                logger.debug(f"--- {prefix} DEBUG ---")
                
                # Debug as raw
                if isinstance(data, bytes):
                    logger.debug(f"{prefix} as bytes: {data[:50]}...")
                    
                    # Try to decode
                    try:
                        as_str = data.decode('utf-8')
                        logger.debug(f"{prefix} decoded as UTF-8: {as_str[:50]}...")
                    except UnicodeDecodeError:
                        logger.debug(f"{prefix} could not be decoded as UTF-8")
                else:
                    logger.debug(f"{prefix} as string: {data[:50]}...")
                
                # Try to parse as JSON
                if isinstance(data, str):
                    try:
                        json.loads(data)
                        logger.debug(f"{prefix} is valid JSON")
                    except json.JSONDecodeError:
                        logger.debug(f"{prefix} is not valid JSON")
                
        except Exception as e:
            logger.error(f"Error debugging input: {str(e)}")
    
    def __getattr__(self, name):
        """Pass through other attribute access to the original stdin."""
        return getattr(self.original_stdin, name)

class JSONOnlyStdoutWrapper:
    """
    Wrapper for stdout that allows JSON-RPC messages to pass through.
    """
    def __init__(self):
        self.original_stdout = sys.stdout
        sys.stdout = self
        logger.info("Installed JSON-only stdout wrapper")
        self.buffer = ""
    
    def write(self, data):
        """Write data to stdout only if it's valid JSON, otherwise log to stderr."""
        if not data:
            return 0
            
        # Check if data has timestamp patterns that indicate log messages
        if isinstance(data, str) and re.match(r'\d{4}-\d{2}-\d{2}', data):
            # This is a log message, redirect to stderr
            sys.stderr.write(data)
            return len(data)
            
        # For JSON-like data, pass it through directly
        if isinstance(data, str) and (data.strip().startswith('{') or data.strip().startswith('[')):
            try:
                # Try to parse as JSON to validate
                json.loads(data.strip())
                logger.debug("Writing JSON message to stdout")
                return self.original_stdout.write(data)
            except json.JSONDecodeError:
                # Not valid JSON, might be a partial message, buffer it
                logger.debug("Found JSON-like data but not valid JSON")
                self.buffer += data.strip()
                
                # Check if buffer now contains valid JSON
                try:
                    json.loads(self.buffer)
                    logger.debug("Writing buffered JSON message to stdout")
                    result = self.original_stdout.write(self.buffer)
                    self.buffer = ""  # Clear buffer after writing
                    return result
                except json.JSONDecodeError:
                    # Still not valid JSON
                    if len(self.buffer) > 10000:
                        logger.warning("Buffer too large with no valid JSON, clearing")
                        self.buffer = ""
                    return len(data)
                
        # For anything else, send to stderr for debugging
        logger.debug("Non-JSON data detected")
        sys.stderr.write(data)
        return len(data)
    
    def flush(self):
        """Pass through flush to original stdout."""
        return self.original_stdout.flush()
    
    def __getattr__(self, name):
        """Pass through other attribute access to the original stdout."""
        return getattr(self.original_stdout, name)

if __name__ == "__main__":
    try:
        logger.info("Starting SQL MCP Server")
        
        # Install middleware
        stdin_middleware = RobustStdinMiddleware()
        stdout_wrapper = JSONOnlyStdoutWrapper()  
        
        # Set default environment variables
        set_default_env_vars()
        
        # Check for credentials and use values from .env file if missing
        if not os.environ.get("DB_USER_DB_USERNAME") or not os.environ.get("DB_USER_DB_PASSWORD"):
            # Try to load from .env file first
            env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
            if os.path.exists(env_path):
                logger.info(f"Loading credentials from .env file: {env_path}")
                try:
                    with open(env_path, 'r') as f:
                        for line in f:
                            if line.strip() and not line.startswith('#'):
                                key, value = line.strip().split('=', 1)
                                if key and value and key not in os.environ:
                                    os.environ[key] = value
                    logger.info("Successfully loaded credentials from .env file")
                except Exception as e:
                    logger.warning(f"Error loading .env file: {str(e)}")
        
        # Import MCP server modules
        from DB_USER.server import server, start_server
        
        # Import tools modules
        import DB_USER.tools.schema
        import DB_USER.tools.query
        
        # Run the server
        start_server()
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)