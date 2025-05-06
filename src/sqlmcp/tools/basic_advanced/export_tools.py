"""
Export Tools for SQL MCP Server.

This module provides tools for exporting SQL data in various formats for basic users.
"""
import logging
import asyncio
import json
import csv
from io import StringIO
from typing import Dict, List, Any, Optional, Union

# Configure logging
logger = logging.getLogger("DB_USER_ExportTools")

# These will be set by the registration function
mcp = None
get_db_connection = None
_get_db_connection_blocking = None
_execute_query_blocking = None
is_safe_query = None


def register(mcp_instance, db_connection_function=None, db_connection_blocking=None, 
             execute_query_blocking=None, safe_query_function=None):
    """Register this module's functions with the MCP instance."""
    global mcp, get_db_connection, _get_db_connection_blocking, _execute_query_blocking, is_safe_query
    
    mcp = mcp_instance
    get_db_connection = db_connection_function
    _get_db_connection_blocking = db_connection_blocking
    _execute_query_blocking = execute_query_blocking
    if safe_query_function:
        is_safe_query = safe_query_function
    
    # Register tools manually
    mcp.add_tool(export_data)
    
    logger.info("Registered basic advanced export tools with MCP instance")


async def export_data(
    query: str,
    format: str = "csv",
    parameters: Optional[Dict[str, Any]] = None,
    limit: int = 1000,
    include_headers: bool = True
) -> Dict[str, Any]:
    """
    Export SQL query results in various formats.
    
    Args:
        query: SQL SELECT query to execute
        format: Export format ('csv', 'json', 'markdown', 'html')
        parameters: Optional query parameters
        limit: Maximum number of rows to export
        include_headers: Whether to include column headers in formats like CSV
        
    Returns:
        Dictionary with the exported data and metadata.
    """
    logger.info(f"Handling export_data: format={format}, limit={limit}")
    
    # Validate format
    format = format.lower()
    valid_formats = {"csv", "json", "markdown", "html"}
    if format not in valid_formats:
        return {
            "error": "Invalid export format",
            "details": f"Format must be one of: {', '.join(valid_formats)}"
        }
    
    # Validate query
    if not query.strip().upper().startswith("SELECT"):
        return {
            "error": "Invalid query",
            "details": "Only SELECT queries are allowed for export"
        }
    
    if is_safe_query and not is_safe_query(query):
        return {
            "error": "Unsafe query",
            "details": "The query contains unsafe operations"
        }
    
    try:
        # Execute the query
        if _execute_query_blocking:
            # Prepare parameters
            param_values = []
            if parameters:
                param_values = list(parameters.values())
            
            # Apply limit
            if limit > 0:
                # Check if query already has a TOP or LIMIT clause
                upper_query = query.upper()
                if not ("TOP " in upper_query or "LIMIT " in upper_query or "FETCH FIRST" in upper_query):
                    if upper_query.startswith("SELECT"):
                        query = f"SELECT TOP {limit} " + query[len("SELECT"):].strip()
            
            # Execute the query
            results = await asyncio.to_thread(_execute_query_blocking, query, tuple(param_values), limit)
            
            # If no results, return an appropriate message
            if not results:
                return {
                    "success": True,
                    "format": format,
                    "row_count": 0,
                    "message": "Query returned no results",
                    "data": None
                }
            
            # Format the data
            formatted_data = None
            if format == "csv":
                formatted_data = _format_as_csv(results, include_headers)
            elif format == "json":
                formatted_data = json.dumps(results, default=str, indent=2)
            elif format == "markdown":
                formatted_data = _format_as_markdown(results)
            elif format == "html":
                formatted_data = _format_as_html(results)
            
            return {
                "success": True,
                "format": format,
                "row_count": len(results),
                "columns": list(results[0].keys()) if results else [],
                "data": formatted_data
            }
        else:
            return {
                "error": "Query execution function not available",
                "details": "The query execution function has not been properly configured"
            }
    except Exception as e:
        logger.error(f"Error exporting data: {e}")
        return {
            "error": "Export failed",
            "details": str(e),
            "query": query
        }


def _format_as_csv(results: List[Dict[str, Any]], include_headers: bool) -> str:
    """Format query results as CSV."""
    if not results:
        return ""
    
    output = StringIO()
    writer = csv.writer(output)
    
    if include_headers:
        writer.writerow(results[0].keys())
    
    for row in results:
        writer.writerow(row.values())
    
    return output.getvalue()


def _format_as_markdown(results: List[Dict[str, Any]]) -> str:
    """Format query results as Markdown table."""
    if not results:
        return ""
    
    columns = list(results[0].keys())
    
    # Create header
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(["---" for _ in columns]) + " |"
    
    # Create rows
    rows = []
    for row in results:
        formatted_row = "| " + " | ".join([str(row[col]) for col in columns]) + " |"
        rows.append(formatted_row)
    
    # Combine all parts
    return "\n".join([header, separator] + rows)


def _format_as_html(results: List[Dict[str, Any]]) -> str:
    """Format query results as HTML table."""
    if not results:
        return ""
    
    columns = list(results[0].keys())
    
    # Start table
    html = ["<table>", "<thead>", "<tr>"]
    
    # Add headers
    for col in columns:
        html.append(f"  <th>{col}</th>")
    html.append("</tr>")
    html.append("</thead>")
    
    # Add rows
    html.append("<tbody>")
    for row in results:
        html.append("<tr>")
        for col in columns:
            html.append(f"  <td>{row[col]}</td>")
        html.append("</tr>")
    html.append("</tbody>")
    
    # Close table
    html.append("</table>")
    
    return "\n".join(html)