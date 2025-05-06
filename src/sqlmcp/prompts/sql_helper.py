"""
SQL helper prompts for common SQL operations.
"""
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.prompts import base

from DB_USER.server import server  # Import server to use decorators

@server.prompt()
def analyze_query(query: str) -> str:
    """
    Create a prompt for analyzing a SQL query.
    
    Args:
        query: SQL query to analyze
    
    Returns:
        A prompt asking to analyze the query
    """
    return f"""Please analyze this SQL query:

```sql
{query}
```

Provide the following details:
1. A brief explanation of what the query does
2. Any performance concerns or optimization opportunities
3. Potential issues with the query structure or syntax
4. Suggestions for improvements
"""

@server.prompt()
def suggest_index(table_name: str, query_pattern: str) -> str:
    """
    Create a prompt for suggesting an index given a query pattern.
    
    Args:
        table_name: Name of the table
        query_pattern: Common query pattern used on the table
    
    Returns:
        A prompt asking to suggest an appropriate index
    """
    return f"""Based on the following information:

Table: {table_name}
Common query pattern: {query_pattern}

Please suggest an appropriate index design that would improve performance.
Explain why this index would be beneficial and any trade-offs to consider.
Include a sample SQL statement to create the index.
"""

@server.prompt()
def generate_query(description: str, table_info: str = None) -> list[base.Message]:
    """
    Create a conversation prompt for generating a SQL query based on a description.
    
    Args:
        description: Text description of what the query should do
        table_info: Optional information about relevant tables and schema
        
    Returns:
        A multi-message prompt for generating a query
    """
    messages = [
        base.SystemMessage(
            "You are a SQL expert helping to write optimized SQL Server queries."
        ),
        base.UserMessage(f"I need to write a SQL query that {description}")
    ]
    
    if table_info:
        messages.append(
            base.UserMessage(f"Here is information about the tables involved:\n\n{table_info}")
        )
    
    messages.append(
        base.AssistantMessage(
            "I'll help you create an efficient SQL query for this need. Let me clarify a few details first:"
        )
    )
    
    return messages
