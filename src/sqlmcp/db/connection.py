"""
Database connection pool management.
"""
import asyncio
import logging
import time
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

class DBConnectionPool:
    """Manages a pool of database connections with monitoring and health checks."""
    
    def __init__(
        self,
        server: str,
        database: str,
        authentication: str = "sql",
        username: Optional[str] = None,
        password: Optional[str] = None,
        pool_size: int = 5,
        timeout: int = 30,
    ):
        self.server = server
        self.database = database
        self.authentication = authentication
        self.username = username
        self.password = password
        self.pool_size = pool_size
        self.timeout = timeout
        
        self.pool = []
        self.in_use = {}
        self.lock = asyncio.Lock()
        self.initialized = False
        
        # Connection metrics
        self.connection_attempts = 0
        self.successful_connections = 0
        self.failed_connections = 0
        self.queries_executed = 0
        self.last_error = None
    
    async def initialize(self) -> None:
        """Initialize the connection pool."""
        async with self.lock:
            if self.initialized:
                return
            
            for _ in range(self.pool_size):
                try:
                    conn = await self._create_connection()
                    self.pool.append(conn)
                    self.successful_connections += 1
                except Exception as e:
                    self.failed_connections += 1
                    self.last_error = str(e)
                    logger.error(f"Failed to create connection: {str(e)}")
                    raise
            
            self.initialized = True
            logger.info(f"Initialized connection pool with {len(self.pool)} connections")
    
    async def _create_connection(self):
        """Create a new database connection."""
        self.connection_attempts += 1
        
        # Construct connection string based on authentication method
        if self.authentication == "windows":
            conn_str = f"Driver={{SQL Server}};Server={self.server};Database={self.database};Trusted_Connection=yes;"
        else:
            conn_str = f"Driver={{SQL Server}};Server={self.server};Database={self.database};UID={self.username};PWD={self.password};"
        
        # Create connection (wrapped in asyncio to make non-blocking)
        # Note: In a real implementation, you would import pyodbc or pymssql here
        # For this framework, we're just creating a placeholder
        loop = asyncio.get_event_loop()
        
        # Mock connection creation - replace with actual pyodbc in real implementation
        # conn = await loop.run_in_executor(None, lambda: pyodbc.connect(conn_str))
        conn = {"connection_string": conn_str, "created_at": time.time()}
        
        logger.debug(f"Created new connection to {self.server}/{self.database}")
        return conn
    
    async def get_connection(self):
        """Get a connection from the pool or create a new one if needed."""
        async with self.lock:
            if not self.initialized:
                await self.initialize()
            
            if not self.pool:
                # No available connections, create a new one if under limit
                if len(self.in_use) < self.pool_size * 2:  # Allow creating up to 2x pool size
                    conn = await self._create_connection()
                    conn_id = id(conn)
                    self.in_use[conn_id] = (conn, time.time())
                    return conn
                else:
                    # Wait for a connection to become available
                    raise TimeoutError("No available connections in the pool")
            
            # Get connection from pool
            conn = self.pool.pop(0)
            conn_id = id(conn)
            self.in_use[conn_id] = (conn, time.time())
            return conn
    
    async def release_connection(self, conn) -> None:
        """Return a connection to the pool."""
        conn_id = id(conn)
        async with self.lock:
            if conn_id in self.in_use:
                del self.in_use[conn_id]
                
                # Check if connection is still valid
                try:
                    # In a real implementation, this would execute a test query
                    # cursor = conn.cursor()
                    # cursor.execute("SELECT 1")
                    # cursor.fetchall()
                    # cursor.close()
                    self.pool.append(conn)
                except Exception as e:
                    logger.warning(f"Discarding broken connection: {str(e)}")
                    # try:
                    #     conn.close()
                    # except:
                    #     pass
    
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None, 
                           timeout: Optional[int] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries."""
        conn = await self.get_connection()
        try:
            # Set timeout if specified
            # if timeout:
            #     conn.timeout = timeout
            
            # cursor = conn.cursor()
            
            start_time = time.time()
            # if params:
            #     cursor.execute(query, params)
            # else:
            #     cursor.execute(query)
            
            # Mock execution
            # In a real implementation, this would be the actual query execution
            await asyncio.sleep(0.1)  # Simulate query execution time
            
            # Mock results
            # In a real implementation, this would fetch results from the cursor
            if "SELECT" in query.upper():
                # Simulate query results
                columns = ["id", "name", "value"]
                results = [
                    {"id": 1, "name": "Test 1", "value": 100},
                    {"id": 2, "name": "Test 2", "value": 200}
                ]
            else:
                results = []
            
            execution_time = time.time() - start_time
            logger.debug(f"Query executed in {execution_time:.3f}s, returned {len(results)} rows")
            
            self.queries_executed += 1
            return results
        except Exception as e:
            self.last_error = str(e)
            logger.error(f"Query execution error: {str(e)}")
            raise
        finally:
            await self.release_connection(conn)
    
    async def close(self) -> None:
        """Close all connections in the pool."""
        async with self.lock:
            # Close all pool connections
            for conn in self.pool:
                try:
                    # conn.close()
                    pass  # In a real implementation, this would close connections
                except Exception as e:
                    logger.warning(f"Error closing connection: {str(e)}")
            
            # Close all in-use connections
            for conn_id, (conn, _) in self.in_use.items():
                try:
                    # conn.close()
                    pass  # In a real implementation, this would close connections
                except Exception as e:
                    logger.warning(f"Error closing connection: {str(e)}")
            
            self.pool = []
            self.in_use = {}
            self.initialized = False
            logger.info("Connection pool closed")
    
    async def health_check(self) -> Dict[str, Any]:
        """Return health status of the connection pool."""
        return {
            "pool_size": len(self.pool),
            "in_use": len(self.in_use),
            "connection_attempts": self.connection_attempts,
            "successful_connections": self.successful_connections,
            "failed_connections": self.failed_connections,
            "queries_executed": self.queries_executed,
            "last_error": self.last_error,
            "server": self.server,
            "database": self.database
        }
