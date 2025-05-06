"""
Usage Digest tools for SQL MCP Server.

This module provides tools for tracking and analyzing the most frequently used
tables and fields in a database, creating a digest of important database objects
based on actual usage patterns to help users unfamiliar with SQL.
"""
import logging
import asyncio
import json
import os
import datetime
from typing import Dict, List, Any, Optional, Set, Tuple
import time

# Configure logging
logger = logging.getLogger("DB_USER_UsageDigest")

# These will be set from the main server file
mcp = None
get_db_connection = None
_get_db_connection_blocking = None
_execute_query_blocking = None

# Constants
DEFAULT_USAGE_DIGEST_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'resources', 'usage_digest')
DEFAULT_USAGE_DIGEST_FILE = 'usage_digest.json'
DEFAULT_MIN_QUERY_COUNT = 5  # Minimum query count to consider a table/field important
DEFAULT_MAX_RESULTS = 50  # Maximum number of results to return
DEFAULT_DAYS_HISTORY = 30  # Number of days of history to analyze

def register_tools(mcp_instance, db_connection_function, db_connection_blocking, execute_query_blocking):
    """Register this module's functions with the MCP instance."""
    global mcp, get_db_connection, _get_db_connection_blocking, _execute_query_blocking
    mcp = mcp_instance
    get_db_connection = db_connection_function
    _get_db_connection_blocking = db_connection_blocking
    _execute_query_blocking = execute_query_blocking
    
    # Register tools manually
    mcp.add_tool(get_usage_digest)
    mcp.add_tool(update_usage_digest)
    mcp.add_tool(get_table_importance)
    mcp.add_tool(suggest_important_joins)
    mcp.add_tool(export_usage_report)
    
    logger.info("Registered usage digest tools with MCP instance")
    
    # Create resource directory if it doesn't exist
    os.makedirs(DEFAULT_USAGE_DIGEST_PATH, exist_ok=True)

def _get_digest_file_path() -> str:
    """Get the path to the usage digest file."""
    return os.path.join(DEFAULT_USAGE_DIGEST_PATH, DEFAULT_USAGE_DIGEST_FILE)

def _load_usage_digest() -> Dict[str, Any]:
    """Load the usage digest from file or create a new one if it doesn't exist."""
    file_path = _get_digest_file_path()
    
    if not os.path.exists(file_path):
        # Initialize empty digest
        digest = {
            "last_updated": datetime.datetime.now().isoformat(),
            "tables": {},
            "fields": {},
            "joins": {},
            "usage_count": 0
        }
        
        # Save the empty digest
        with open(file_path, 'w') as f:
            json.dump(digest, f, indent=2)
        
        return digest
    
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading usage digest: {e}")
        # Return empty digest on error
        return {
            "last_updated": datetime.datetime.now().isoformat(),
            "tables": {},
            "fields": {},
            "joins": {},
            "usage_count": 0
        }

def _save_usage_digest(digest: Dict[str, Any]) -> None:
    """Save the usage digest to file."""
    file_path = _get_digest_file_path()
    
    # Update last_updated timestamp
    digest["last_updated"] = datetime.datetime.now().isoformat()
    
    try:
        with open(file_path, 'w') as f:
            json.dump(digest, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving usage digest: {e}")

async def get_usage_digest(
    min_query_count: int = DEFAULT_MIN_QUERY_COUNT,
    max_results: int = DEFAULT_MAX_RESULTS
) -> Dict[str, Any]:
    """
    Get the digest of most frequently used tables and fields in the database.
    
    Args:
        min_query_count: Minimum query count to include a table/field (default: 5)
        max_results: Maximum number of results to return (default: 50)
        
    Returns:
        Dictionary containing usage digest information
    """
    logger.info(f"Handling get_usage_digest: min_query_count={min_query_count}, max_results={max_results}")
    
    try:
        # Load current usage digest
        digest = _load_usage_digest()
        
        # Filter tables and fields by min_query_count
        filtered_tables = {
            table: info
            for table, info in digest["tables"].items()
            if info["query_count"] >= min_query_count
        }
        
        filtered_fields = {
            field: info
            for field, info in digest["fields"].items()
            if info["query_count"] >= min_query_count
        }
        
        filtered_joins = {
            join: info
            for join, info in digest["joins"].items()
            if info["query_count"] >= min_query_count
        }
        
        # Sort tables and fields by query_count (descending)
        sorted_tables = dict(sorted(
            filtered_tables.items(),
            key=lambda item: item[1]["query_count"],
            reverse=True
        )[:max_results])
        
        sorted_fields = dict(sorted(
            filtered_fields.items(),
            key=lambda item: item[1]["query_count"],
            reverse=True
        )[:max_results])
        
        sorted_joins = dict(sorted(
            filtered_joins.items(),
            key=lambda item: item[1]["query_count"],
            reverse=True
        )[:max_results])
        
        # Prepare result
        result = {
            "last_updated": digest["last_updated"],
            "total_queries_analyzed": digest["usage_count"],
            "most_used_tables": sorted_tables,
            "most_used_fields": sorted_fields,
            "most_used_joins": sorted_joins,
            "metadata": {
                "min_query_count": min_query_count,
                "max_results": max_results
            }
        }
        
        logger.info(f"Retrieved usage digest with {len(sorted_tables)} tables, {len(sorted_fields)} fields")
        return result
        
    except Exception as e:
        logger.error(f"Error in get_usage_digest: {e}")
        return {
            "error": "Failed to retrieve usage digest",
            "details": str(e)
        }

async def update_usage_digest(
    days_history: int = DEFAULT_DAYS_HISTORY,
    force_refresh: bool = False
) -> Dict[str, Any]:
    """
    Update the usage digest by analyzing query history from system views.
    
    Args:
        days_history: Number of days of history to analyze (default: 30)
        force_refresh: Whether to force a refresh even if recently updated (default: False)
        
    Returns:
        Dictionary containing update status information
    """
    logger.info(f"Handling update_usage_digest: days_history={days_history}, force_refresh={force_refresh}")
    
    try:
        # Check if digest was recently updated (within the last 24 hours)
        current_digest = _load_usage_digest()
        last_updated = datetime.datetime.fromisoformat(current_digest["last_updated"])
        now = datetime.datetime.now()
        
        if not force_refresh and (now - last_updated).total_seconds() < 86400:  # 24 hours in seconds
            return {
                "status": "skipped",
                "message": f"Usage digest was recently updated ({last_updated.isoformat()}). Use force_refresh=True to update anyway.",
                "last_updated": current_digest["last_updated"]
            }
        
        # Query SQL Server DMVs to get query statistics
        # We'll use sys.dm_exec_query_stats, sys.dm_exec_sql_text, and sys.objects
        
        # Query to get the most executed queries
        query_stats_query = f"""
        SELECT TOP 1000
            qt.text AS query_text,
            qs.execution_count,
            qs.total_logical_reads,
            qs.total_logical_writes,
            qs.last_execution_time
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
        WHERE 
            qs.last_execution_time > DATEADD(day, -{days_history}, GETDATE())
            AND qt.text NOT LIKE '%sys.%'
            AND qt.text NOT LIKE '%@%'  -- Filter out parameterized/system queries
            AND qt.text NOT LIKE '%dm_%'
            AND qt.text NOT LIKE '%information_schema%'
        ORDER BY qs.execution_count DESC
        """
        
        query_stats = await asyncio.to_thread(_execute_query_blocking, query_stats_query)
        
        if not query_stats:
            return {
                "status": "error",
                "message": "No query statistics found",
                "last_updated": current_digest["last_updated"]
            }
        
        # Initialize new digest
        new_digest = {
            "last_updated": now.isoformat(),
            "tables": {},
            "fields": {},
            "joins": {},
            "usage_count": len(query_stats)
        }
        
        # Extract table and field information from queries
        for query_stat in query_stats:
            query_text = query_stat.get("query_text", "").upper()
            execution_count = query_stat.get("execution_count", 1)
            
            # Skip empty queries
            if not query_text or len(query_text) < 10:
                continue
                
            # Extract tables
            tables = _extract_tables_from_query(query_text)
            
            # Extract fields
            fields = _extract_fields_from_query(query_text)
            
            # Extract joins
            joins = _extract_joins_from_query(query_text)
            
            # Update tables in digest
            for table in tables:
                if table in new_digest["tables"]:
                    new_digest["tables"][table]["query_count"] += execution_count
                    new_digest["tables"][table]["last_seen"] = now.isoformat()
                else:
                    # Get actual table information from database
                    table_parts = table.split(".")
                    schema = table_parts[0] if len(table_parts) > 1 else "dbo"
                    table_name = table_parts[1] if len(table_parts) > 1 else table_parts[0]
                    
                    table_info_query = """
                    SELECT 
                        t.name AS table_name,
                        s.name AS schema_name,
                        i.rows AS row_count,
                        COUNT(c.name) AS column_count,
                        MAX(o.create_date) AS create_date,
                        MAX(o.modify_date) AS modify_date
                    FROM sys.tables t
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    JOIN sys.columns c ON t.object_id = c.object_id
                    JOIN sys.sysindexes i ON t.object_id = i.id AND i.indid < 2
                    JOIN sys.objects o ON t.object_id = o.object_id
                    WHERE s.name = ? AND t.name = ?
                    GROUP BY t.name, s.name, i.rows
                    """
                    
                    table_info_result = await asyncio.to_thread(
                        _execute_query_blocking,
                        table_info_query,
                        (schema, table_name)
                    )
                    
                    if table_info_result:
                        table_info = table_info_result[0]
                        new_digest["tables"][table] = {
                            "schema": schema,
                            "name": table_name,
                            "query_count": execution_count,
                            "row_count": table_info.get("row_count", 0),
                            "column_count": table_info.get("column_count", 0),
                            "create_date": table_info.get("create_date").isoformat() if table_info.get("create_date") else None,
                            "modify_date": table_info.get("modify_date").isoformat() if table_info.get("modify_date") else None,
                            "first_seen": now.isoformat(),
                            "last_seen": now.isoformat()
                        }
                    else:
                        # Table might be a view or temporary table
                        new_digest["tables"][table] = {
                            "schema": schema,
                            "name": table_name,
                            "query_count": execution_count,
                            "first_seen": now.isoformat(),
                            "last_seen": now.isoformat()
                        }
            
            # Update fields in digest
            for field in fields:
                if field in new_digest["fields"]:
                    new_digest["fields"][field]["query_count"] += execution_count
                    new_digest["fields"][field]["last_seen"] = now.isoformat()
                else:
                    # Parse field and table
                    parts = field.split(".")
                    if len(parts) >= 2:
                        field_name = parts[-1]
                        table_ref = ".".join(parts[:-1])
                    else:
                        field_name = parts[0]
                        table_ref = None
                    
                    new_digest["fields"][field] = {
                        "name": field_name,
                        "table": table_ref,
                        "query_count": execution_count,
                        "first_seen": now.isoformat(),
                        "last_seen": now.isoformat()
                    }
            
            # Update joins in digest
            for join in joins:
                join_key = f"{join[0]}|{join[1]}"
                if join_key in new_digest["joins"]:
                    new_digest["joins"][join_key]["query_count"] += execution_count
                    new_digest["joins"][join_key]["last_seen"] = now.isoformat()
                else:
                    new_digest["joins"][join_key] = {
                        "table1": join[0],
                        "table2": join[1],
                        "query_count": execution_count,
                        "join_fields": join[2] if len(join) > 2 else None,
                        "first_seen": now.isoformat(),
                        "last_seen": now.isoformat()
                    }
        
        # Merge with existing digest (preserving history)
        merged_digest = _merge_digests(current_digest, new_digest)
        
        # Save updated digest
        _save_usage_digest(merged_digest)
        
        return {
            "status": "success",
            "message": f"Updated usage digest with {len(query_stats)} queries from the last {days_history} days",
            "tables_analyzed": len(merged_digest["tables"]),
            "fields_analyzed": len(merged_digest["fields"]),
            "joins_analyzed": len(merged_digest["joins"]),
            "last_updated": now.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in update_usage_digest: {e}")
        return {
            "error": "Failed to update usage digest",
            "details": str(e)
        }

def _extract_tables_from_query(query_text: str) -> Set[str]:
    """Extract table names from a SQL query."""
    tables = set()
    
    # Look for patterns like "FROM [schema].[table]" or "JOIN [schema].[table]"
    query_parts = query_text.split()
    
    for i, part in enumerate(query_parts):
        if part in ("FROM", "JOIN", "INTO", "UPDATE") and i + 1 < len(query_parts):
            table_ref = query_parts[i + 1].strip('[](){},;')
            if table_ref and table_ref.upper() not in ("SELECT", "WITH", "AS", "ON"):
                # Clean up the table name
                table_ref = table_ref.replace('[', '').replace(']', '')
                tables.add(table_ref)
    
    return tables

def _extract_fields_from_query(query_text: str) -> Set[str]:
    """Extract field names from a SQL query."""
    fields = set()
    
    # Split the query into smaller chunks for easier parsing
    # Focus on SELECT, WHERE, GROUP BY, ORDER BY clauses
    clauses = {
        "SELECT": "",
        "WHERE": "",
        "GROUP BY": "",
        "ORDER BY": ""
    }
    
    current_clause = None
    
    # Extract clauses
    for line in query_text.split("\n"):
        line = line.strip()
        
        for clause in clauses.keys():
            if line.startswith(clause) or line.startswith(clause.lower()):
                current_clause = clause
                line = line[len(clause):].strip()
                break
        
        if current_clause:
            clauses[current_clause] += " " + line
    
    # Process SELECT clause
    if clauses["SELECT"]:
        # Handle column selects like "SELECT a, b, c, t.d"
        select_items = clauses["SELECT"].split(",")
        for item in select_items:
            item = item.strip()
            
            # Skip * selections
            if item == "*" or item.endswith(".*"):
                continue
            
            # Handle "AS" aliases
            if " AS " in item.upper():
                item = item.split(" AS ")[0].strip()
            
            # Ignore functions like COUNT(), SUM(), etc.
            if "(" not in item:
                # Handle qualified field names (table.field)
                if "." in item:
                    fields.add(item)
                else:
                    fields.add(item)
    
    # Process WHERE clause - look for field comparisons
    if clauses["WHERE"]:
        where_parts = clauses["WHERE"].replace(" AND ", " ").replace(" OR ", " ").split()
        for part in where_parts:
            if "." in part and "(" not in part:
                field = part.split("=")[0].strip() if "=" in part else part.strip()
                field = field.strip('[](){},;')
                if field:
                    fields.add(field)
    
    # Clean up field names
    clean_fields = set()
    for field in fields:
        # Remove brackets, quotes, etc.
        clean_field = field.replace('[', '').replace(']', '').replace('"', '').replace("'", "")
        # Remove table alias if it's just a single character followed by a dot
        if len(clean_field) > 2 and clean_field[1] == '.' and clean_field[0].isalpha():
            clean_field = clean_field[2:]
        clean_fields.add(clean_field)
    
    return clean_fields

def _extract_joins_from_query(query_text: str) -> List[Tuple[str, str, Optional[str]]]:
    """Extract join information from a SQL query."""
    joins = []
    
    # Look for patterns like "TableA JOIN TableB ON TableA.FieldX = TableB.FieldY"
    join_types = ["JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN"]
    
    # Split the query into lines for easier processing
    lines = query_text.split("\n")
    
    current_tables = set()
    from_encountered = False
    
    for line in lines:
        line = line.strip().upper()
        
        # Capture table after FROM
        if "FROM" in line and not from_encountered:
            from_encountered = True
            parts = line.split("FROM", 1)[1].strip().split()
            if parts:
                table = parts[0].strip('[](){},;')
                if table:
                    current_tables.add(table.replace('[', '').replace(']', ''))
        
        # Look for JOIN statements
        for join_type in join_types:
            if join_type in line:
                join_parts = line.split(join_type, 1)[1].strip().split()
                if join_parts:
                    joined_table = join_parts[0].strip('[](){},;')
                    joined_table = joined_table.replace('[', '').replace(']', '')
                    
                    # Look for the ON clause to get the join fields
                    join_fields = None
                    if "ON" in line:
                        on_clause = line.split("ON", 1)[1].strip()
                        if "=" in on_clause:
                            join_fields = on_clause.split("AND")[0].strip()
                    
                    # Add joins for each previously encountered table
                    for table in current_tables:
                        joins.append((table, joined_table, join_fields))
                    
                    # Add the joined table to our set
                    current_tables.add(joined_table)
    
    return joins

def _merge_digests(old_digest: Dict[str, Any], new_digest: Dict[str, Any]) -> Dict[str, Any]:
    """Merge an old digest with a new one, preserving historical information."""
    merged = {
        "last_updated": new_digest["last_updated"],
        "usage_count": old_digest.get("usage_count", 0) + new_digest["usage_count"],
        "tables": {},
        "fields": {},
        "joins": {}
    }
    
    # Merge tables
    for table, info in old_digest.get("tables", {}).items():
        if table in new_digest["tables"]:
            # Table exists in both, update counts and keep oldest first_seen
            merged["tables"][table] = new_digest["tables"][table].copy()
            merged["tables"][table]["query_count"] += info.get("query_count", 0)
            
            # Keep earliest first_seen date
            if "first_seen" in info:
                old_first_seen = datetime.datetime.fromisoformat(info["first_seen"])
                new_first_seen = datetime.datetime.fromisoformat(merged["tables"][table]["first_seen"])
                if old_first_seen < new_first_seen:
                    merged["tables"][table]["first_seen"] = info["first_seen"]
        else:
            # Table only in old digest
            merged["tables"][table] = info.copy()
    
    # Add tables only in new digest
    for table, info in new_digest["tables"].items():
        if table not in merged["tables"]:
            merged["tables"][table] = info.copy()
    
    # Merge fields
    for field, info in old_digest.get("fields", {}).items():
        if field in new_digest["fields"]:
            # Field exists in both, update counts
            merged["fields"][field] = new_digest["fields"][field].copy()
            merged["fields"][field]["query_count"] += info.get("query_count", 0)
            
            # Keep earliest first_seen date
            if "first_seen" in info:
                old_first_seen = datetime.datetime.fromisoformat(info["first_seen"])
                new_first_seen = datetime.datetime.fromisoformat(merged["fields"][field]["first_seen"])
                if old_first_seen < new_first_seen:
                    merged["fields"][field]["first_seen"] = info["first_seen"]
        else:
            # Field only in old digest
            merged["fields"][field] = info.copy()
    
    # Add fields only in new digest
    for field, info in new_digest["fields"].items():
        if field not in merged["fields"]:
            merged["fields"][field] = info.copy()
    
    # Merge joins
    for join, info in old_digest.get("joins", {}).items():
        if join in new_digest["joins"]:
            # Join exists in both, update counts
            merged["joins"][join] = new_digest["joins"][join].copy()
            merged["joins"][join]["query_count"] += info.get("query_count", 0)
            
            # Keep earliest first_seen date
            if "first_seen" in info:
                old_first_seen = datetime.datetime.fromisoformat(info["first_seen"])
                new_first_seen = datetime.datetime.fromisoformat(merged["joins"][join]["first_seen"])
                if old_first_seen < new_first_seen:
                    merged["joins"][join]["first_seen"] = info["first_seen"]
        else:
            # Join only in old digest
            merged["joins"][join] = info.copy()
    
    # Add joins only in new digest
    for join, info in new_digest["joins"].items():
        if join not in merged["joins"]:
            merged["joins"][join] = info.copy()
    
    return merged

async def get_table_importance(
    table_name: str
) -> Dict[str, Any]:
    """
    Get the importance metrics for a specific table based on usage patterns.
    
    Args:
        table_name: Table name (format: 'schema.table' or just 'table' for default 'dbo' schema)
        
    Returns:
        Dictionary containing table importance information
    """
    logger.info(f"Handling get_table_importance: table_name={table_name}")
    
    try:
        # Parse schema and table name
        parts = table_name.split('.')
        if len(parts) == 2:
            schema_name, table_name_only = parts
        else:
            schema_name = 'dbo'  # Default schema
            table_name_only = parts[0]
        
        full_table_name = f"{schema_name}.{table_name_only}"
        
        # Load current usage digest
        digest = _load_usage_digest()
        
        # Try different formats of the table name
        table_variants = [
            full_table_name,
            table_name_only,
            f"[{schema_name}].[{table_name_only}]",
            f"[{table_name_only}]"
        ]
        
        table_info = None
        for variant in table_variants:
            if variant in digest["tables"]:
                table_info = digest["tables"][variant]
                break
        
        if not table_info:
            # Table not found in digest, get basic info from database
            validate_query = """
            SELECT 
                t.name AS table_name,
                s.name AS schema_name,
                i.rows AS row_count,
                COUNT(c.name) AS column_count
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.columns c ON t.object_id = c.object_id
            JOIN sys.sysindexes i ON t.object_id = i.id AND i.indid < 2
            WHERE s.name = ? AND t.name = ?
            GROUP BY t.name, s.name, i.rows
            """
            
            validation_result = await asyncio.to_thread(
                _execute_query_blocking,
                validate_query,
                (schema_name, table_name_only)
            )
            
            if not validation_result:
                return {
                    "error": f"Table '{schema_name}.{table_name_only}' not found",
                    "details": "The specified table does not exist in the database or usage digest"
                }
            
            # Create basic table info
            table_info = {
                "schema": schema_name,
                "name": table_name_only,
                "query_count": 0,
                "row_count": validation_result[0].get("row_count", 0),
                "column_count": validation_result[0].get("column_count", 0),
                "first_seen": None,
                "last_seen": None
            }
        
        # Get related fields from the digest
        related_fields = {}
        for field, field_info in digest["fields"].items():
            field_table = field_info.get("table")
            if field_table and any(variant.lower() == field_table.lower() for variant in table_variants):
                related_fields[field] = field_info
        
        # Get related joins
        related_joins = {}
        for join_key, join_info in digest["joins"].items():
            table1 = join_info.get("table1", "")
            table2 = join_info.get("table2", "")
            
            if any(variant.lower() == table1.lower() for variant in table_variants) or \
               any(variant.lower() == table2.lower() for variant in table_variants):
                related_joins[join_key] = join_info
        
        # Calculate table importance metrics
        total_queries = digest.get("usage_count", 1)  # Avoid division by zero
        importance_score = min(10, (table_info.get("query_count", 0) / total_queries) * 100)
        
        # Get top 10 most queried fields
        top_fields = sorted(
            related_fields.items(),
            key=lambda item: item[1].get("query_count", 0),
            reverse=True
        )[:10]
        
        # Get top 5 most common joins
        top_joins = sorted(
            related_joins.items(),
            key=lambda item: item[1].get("query_count", 0),
            reverse=True
        )[:5]
        
        # Prepare result
        result = {
            "table_name": full_table_name,
            "importance_metrics": {
                "query_count": table_info.get("query_count", 0),
                "total_queries_analyzed": total_queries,
                "usage_percentage": round((table_info.get("query_count", 0) / total_queries) * 100, 2),
                "importance_score": round(importance_score, 1),
                "row_count": table_info.get("row_count", 0),
                "column_count": table_info.get("column_count", 0),
                "first_seen": table_info.get("first_seen"),
                "last_seen": table_info.get("last_seen")
            },
            "most_queried_fields": {field: info for field, info in top_fields},
            "common_joins": {join: info for join, info in top_joins},
            "description": _generate_table_description(table_info, related_fields, related_joins)
        }
        
        logger.info(f"Retrieved importance metrics for {full_table_name}")
        return result
        
    except Exception as e:
        logger.error(f"Error in get_table_importance: {e}")
        return {
            "error": "Failed to get table importance",
            "details": str(e)
        }

def _generate_table_description(
    table_info: Dict[str, Any],
    related_fields: Dict[str, Dict[str, Any]],
    related_joins: Dict[str, Dict[str, Any]]
) -> str:
    """Generate a natural language description of the table's usage and importance."""
    table_name = f"{table_info.get('schema', 'dbo')}.{table_info.get('name', '')}"
    query_count = table_info.get("query_count", 0)
    
    if query_count == 0:
        return f"The table {table_name} has not been queried or analyzed yet."
    
    # Describe table usage
    description = f"The table {table_name} contains approximately {table_info.get('row_count', 0):,} rows with {table_info.get('column_count', 0)} columns. "
    
    if query_count > 0:
        description += f"It has been queried {query_count:,} times in the analyzed history. "
        
    # Describe key fields
    if related_fields:
        top_fields = sorted(
            related_fields.items(),
            key=lambda item: item[1].get("query_count", 0),
            reverse=True
        )[:5]
        
        field_names = [field_info.get("name", field) for field, field_info in top_fields]
        description += f"The most commonly queried fields are {', '.join(field_names)}. "
    
    # Describe common joins
    if related_joins:
        top_joins = sorted(
            related_joins.items(),
            key=lambda item: item[1].get("query_count", 0),
            reverse=True
        )[:3]
        
        join_descriptions = []
        for _, join_info in top_joins:
            table1 = join_info.get("table1", "")
            table2 = join_info.get("table2", "")
            
            # Make sure the current table is first in the description
            if table_name.lower() in table2.lower():
                table1, table2 = table2, table1
            
            join_descriptions.append(f"{table2}")
        
        if join_descriptions:
            description += f"This table is commonly joined with {', '.join(join_descriptions)}."
    
    return description

async def suggest_important_joins(
    table_name: str,
    max_suggestions: int = 5
) -> Dict[str, Any]:
    """
    Suggest important table joins based on usage patterns.
    
    Args:
        table_name: Table name (format: 'schema.table' or just 'table' for default 'dbo' schema)
        max_suggestions: Maximum number of join suggestions to return (default: 5)
        
    Returns:
        Dictionary containing join suggestions
    """
    logger.info(f"Handling suggest_important_joins: table_name={table_name}, max_suggestions={max_suggestions}")
    
    try:
        # Parse schema and table name
        parts = table_name.split('.')
        if len(parts) == 2:
            schema_name, table_name_only = parts
        else:
            schema_name = 'dbo'  # Default schema
            table_name_only = parts[0]
        
        full_table_name = f"{schema_name}.{table_name_only}"
        
        # Verify table exists
        validate_query = "SELECT COUNT(*) AS count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
        
        validation_result = await asyncio.to_thread(
            _execute_query_blocking,
            validate_query,
            (schema_name, table_name_only)
        )
        
        if not validation_result or validation_result[0].get("count", 0) == 0:
            return {
                "error": f"Table '{full_table_name}' not found",
                "details": "The specified table does not exist in the database"
            }
        
        # Load current usage digest
        digest = _load_usage_digest()
        
        # Find all potential join relationships
        # 1. From the usage digest
        table_variants = [
            full_table_name,
            table_name_only,
            f"[{schema_name}].[{table_name_only}]",
            f"[{table_name_only}]"
        ]
        
        usage_joins = []
        for join_key, join_info in digest.get("joins", {}).items():
            table1 = join_info.get("table1", "")
            table2 = join_info.get("table2", "")
            
            if any(variant.lower() == table1.lower() for variant in table_variants):
                usage_joins.append({
                    "primary_table": table1,
                    "secondary_table": table2,
                    "join_condition": join_info.get("join_fields"),
                    "query_count": join_info.get("query_count", 0),
                    "source": "usage_history"
                })
            elif any(variant.lower() == table2.lower() for variant in table_variants):
                usage_joins.append({
                    "primary_table": table2,
                    "secondary_table": table1,
                    "join_condition": join_info.get("join_fields"),
                    "query_count": join_info.get("query_count", 0),
                    "source": "usage_history"
                })
        
        # 2. From foreign key relationships
        fk_query = """
        SELECT
            pk_schema = pk_tab.TABLE_SCHEMA,
            pk_table = pk_tab.TABLE_NAME,
            pk_column = pk_col.COLUMN_NAME,
            fk_schema = fk_tab.TABLE_SCHEMA,
            fk_table = fk_tab.TABLE_NAME,
            fk_column = fk_col.COLUMN_NAME
        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS ref_const
        INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS fk_const ON ref_const.CONSTRAINT_NAME = fk_const.CONSTRAINT_NAME
        INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk_const ON ref_const.UNIQUE_CONSTRAINT_NAME = pk_const.CONSTRAINT_NAME
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk_col ON fk_const.CONSTRAINT_NAME = fk_col.CONSTRAINT_NAME
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk_col ON pk_const.CONSTRAINT_NAME = pk_col.CONSTRAINT_NAME AND pk_col.ORDINAL_POSITION = fk_col.ORDINAL_POSITION
        INNER JOIN INFORMATION_SCHEMA.TABLES fk_tab ON fk_const.TABLE_NAME = fk_tab.TABLE_NAME AND fk_const.TABLE_SCHEMA = fk_tab.TABLE_SCHEMA
        INNER JOIN INFORMATION_SCHEMA.TABLES pk_tab ON pk_const.TABLE_NAME = pk_tab.TABLE_NAME AND pk_const.TABLE_SCHEMA = pk_tab.TABLE_SCHEMA
        WHERE (pk_tab.TABLE_SCHEMA = ? AND pk_tab.TABLE_NAME = ?) OR (fk_tab.TABLE_SCHEMA = ? AND fk_tab.TABLE_NAME = ?)
        """
        
        fk_result = await asyncio.to_thread(
            _execute_query_blocking,
            fk_query,
            (schema_name, table_name_only, schema_name, table_name_only)
        )
        
        fk_joins = []
        for fk in fk_result:
            pk_full_table = f"{fk.get('pk_schema')}.{fk.get('pk_table')}"
            fk_full_table = f"{fk.get('fk_schema')}.{fk.get('fk_table')}"
            
            if full_table_name.lower() == pk_full_table.lower():
                # Current table is the primary key table
                join_condition = f"{pk_full_table}.{fk.get('pk_column')} = {fk_full_table}.{fk.get('fk_column')}"
                fk_joins.append({
                    "primary_table": pk_full_table,
                    "secondary_table": fk_full_table,
                    "join_condition": join_condition,
                    "query_count": 0,  # Not based on usage
                    "source": "foreign_key"
                })
            elif full_table_name.lower() == fk_full_table.lower():
                # Current table is the foreign key table
                join_condition = f"{fk_full_table}.{fk.get('fk_column')} = {pk_full_table}.{fk.get('pk_column')}"
                fk_joins.append({
                    "primary_table": fk_full_table,
                    "secondary_table": pk_full_table,
                    "join_condition": join_condition,
                    "query_count": 0,  # Not based on usage
                    "source": "foreign_key"
                })
        
        # Combine both sources
        all_joins = usage_joins + fk_joins
        
        # For each foreign key join, check if it's already in usage_joins and update query_count if found
        for fk_join in fk_joins:
            for usage_join in usage_joins:
                if (fk_join["primary_table"].lower() == usage_join["primary_table"].lower() and 
                    fk_join["secondary_table"].lower() == usage_join["secondary_table"].lower()):
                    fk_join["query_count"] = usage_join["query_count"]
                    break
        
        # Sort by query count (descending)
        sorted_joins = sorted(all_joins, key=lambda x: x["query_count"], reverse=True)
        
        # Generate example queries for each join
        join_suggestions = []
        for join in sorted_joins[:max_suggestions]:
            primary_table = join["primary_table"]
            secondary_table = join["secondary_table"]
            join_condition = join["join_condition"]
            query_count = join["query_count"]
            source = join["source"]
            
            # Get some field names from the secondary table for the example query
            fields_query = """
            SELECT TOP 5 COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """
            
            sec_table_parts = secondary_table.replace('[', '').replace(']', '').split('.')
            sec_schema = sec_table_parts[0] if len(sec_table_parts) > 1 else "dbo"
            sec_table = sec_table_parts[1] if len(sec_table_parts) > 1 else sec_table_parts[0]
            
            fields_result = await asyncio.to_thread(
                _execute_query_blocking,
                fields_query,
                (sec_schema, sec_table)
            )
            
            field_list = ", ".join([f"b.{field['COLUMN_NAME']}" for field in fields_result])
            
            # If no join condition was found in the usage history, try to infer one
            if not join_condition and source == "usage_history":
                # Look for similar field names between tables
                pri_table_parts = primary_table.replace('[', '').replace(']', '').split('.')
                pri_schema = pri_table_parts[0] if len(pri_table_parts) > 1 else "dbo"
                pri_table = pri_table_parts[1] if len(pri_table_parts) > 1 else pri_table_parts[0]
                
                pri_fields_query = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """
                
                pri_fields_result = await asyncio.to_thread(
                    _execute_query_blocking,
                    pri_fields_query,
                    (pri_schema, pri_table)
                )
                
                pri_field_names = [field['COLUMN_NAME'].lower() for field in pri_fields_result]
                
                sec_fields_query = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """
                
                sec_fields_result = await asyncio.to_thread(
                    _execute_query_blocking,
                    sec_fields_query,
                    (sec_schema, sec_table)
                )
                
                sec_field_names = [field['COLUMN_NAME'].lower() for field in sec_fields_result]
                
                # Look for exact matches (e.g., ID = ID) or pattern matches (table_id = id)
                for pri_field in pri_field_names:
                    if pri_field in sec_field_names:
                        join_condition = f"{primary_table}.{pri_field} = {secondary_table}.{pri_field}"
                        break
                
                if not join_condition:
                    # Look for common patterns like "table_id = id"
                    for pri_field in pri_field_names:
                        for sec_field in sec_field_names:
                            if pri_field.endswith('_' + sec_field) or sec_field.endswith('_' + pri_field):
                                join_condition = f"{primary_table}.{pri_field} = {secondary_table}.{sec_field}"
                                break
                
                if not join_condition:
                    # Default to a placeholder if no condition can be inferred
                    join_condition = f"{primary_table}.id = {secondary_table}.{pri_table}_id"
            
            # Create example query
            example_query = f"""
SELECT a.*, {field_list}
FROM {primary_table} a
JOIN {secondary_table} b ON {join_condition}
-- Uncomment to add filters
-- WHERE a.some_field = 'some_value'
-- ORDER BY a.id
            """.strip()
            
            join_suggestions.append({
                "primary_table": primary_table,
                "secondary_table": secondary_table,
                "join_condition": join_condition,
                "query_count": query_count,
                "source": source,
                "example_query": example_query
            })
        
        # Prepare result
        result = {
            "table_name": full_table_name,
            "join_suggestions": join_suggestions,
            "metadata": {
                "max_suggestions": max_suggestions,
                "total_suggestions_found": len(all_joins),
                "usage_based_suggestions": len(usage_joins),
                "foreign_key_suggestions": len(fk_joins)
            }
        }
        
        logger.info(f"Generated {len(join_suggestions)} join suggestions for {full_table_name}")
        return result
        
    except Exception as e:
        logger.error(f"Error in suggest_important_joins: {e}")
        return {
            "error": "Failed to suggest important joins",
            "details": str(e)
        }

async def export_usage_report(
    export_format: str = "csv",
    output_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Export the usage digest information to a file format suitable for DTS.
    
    Args:
        export_format: Format to export (csv, json, xml, default: csv)
        output_path: Optional path to save the export file
        
    Returns:
        Dictionary containing export status information
    """
    logger.info(f"Handling export_usage_report: export_format={export_format}, output_path={output_path}")
    
    try:
        # Load usage digest
        digest = _load_usage_digest()
        
        # Determine output path
        if not output_path:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = os.path.join(DEFAULT_USAGE_DIGEST_PATH, "exports")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"usage_report_{timestamp}.{export_format}")
        
        # Create export data structure
        export_data = {
            "metadata": {
                "export_date": datetime.datetime.now().isoformat(),
                "last_updated": digest["last_updated"],
                "total_queries_analyzed": digest.get("usage_count", 0)
            },
            "tables": [],
            "fields": [],
            "joins": []
        }
        
        # Format tables data
        for table_name, table_info in digest.get("tables", {}).items():
            export_data["tables"].append({
                "table_name": table_name,
                "schema": table_info.get("schema", "dbo"),
                "name": table_info.get("name", ""),
                "query_count": table_info.get("query_count", 0),
                "row_count": table_info.get("row_count", 0),
                "column_count": table_info.get("column_count", 0),
                "first_seen": table_info.get("first_seen", ""),
                "last_seen": table_info.get("last_seen", "")
            })
        
        # Format fields data
        for field_name, field_info in digest.get("fields", {}).items():
            export_data["fields"].append({
                "field_name": field_name,
                "table": field_info.get("table", ""),
                "name": field_info.get("name", ""),
                "query_count": field_info.get("query_count", 0),
                "first_seen": field_info.get("first_seen", ""),
                "last_seen": field_info.get("last_seen", "")
            })
        
        # Format joins data
        for join_key, join_info in digest.get("joins", {}).items():
            export_data["joins"].append({
                "join_key": join_key,
                "table1": join_info.get("table1", ""),
                "table2": join_info.get("table2", ""),
                "join_fields": join_info.get("join_fields", ""),
                "query_count": join_info.get("query_count", 0),
                "first_seen": join_info.get("first_seen", ""),
                "last_seen": join_info.get("last_seen", "")
            })
        
        # Export based on format
        if export_format.lower() == "json":
            with open(output_path, 'w') as f:
                json.dump(export_data, f, indent=2)
        
        elif export_format.lower() == "csv":
            # Create separate CSV files for tables, fields, and joins
            base_path = os.path.splitext(output_path)[0]
            
            # Tables CSV
            tables_path = f"{base_path}_tables.csv"
            with open(tables_path, 'w') as f:
                # Write header
                header = "table_name,schema,name,query_count,row_count,column_count,first_seen,last_seen\n"
                f.write(header)
                
                # Write data
                for table in export_data["tables"]:
                    row = f"{table['table_name']},{table['schema']},{table['name']},{table['query_count']},{table['row_count']},{table['column_count']},{table['first_seen']},{table['last_seen']}\n"
                    f.write(row)
            
            # Fields CSV
            fields_path = f"{base_path}_fields.csv"
            with open(fields_path, 'w') as f:
                # Write header
                header = "field_name,table,name,query_count,first_seen,last_seen\n"
                f.write(header)
                
                # Write data
                for field in export_data["fields"]:
                    row = f"{field['field_name']},{field['table']},{field['name']},{field['query_count']},{field['first_seen']},{field['last_seen']}\n"
                    f.write(row)
            
            # Joins CSV
            joins_path = f"{base_path}_joins.csv"
            with open(joins_path, 'w') as f:
                # Write header
                header = "join_key,table1,table2,join_fields,query_count,first_seen,last_seen\n"
                f.write(header)
                
                # Write data
                for join in export_data["joins"]:
                    # Escape commas in join_fields if present
                    join_fields = f'"{join["join_fields"]}"' if join["join_fields"] and "," in join["join_fields"] else join["join_fields"]
                    row = f"{join['join_key']},{join['table1']},{join['table2']},{join_fields},{join['query_count']},{join['first_seen']},{join['last_seen']}\n"
                    f.write(row)
            
            # Update output path to include all files
            output_path = f"{base_path}_*.csv"
        
        elif export_format.lower() == "xml":
            with open(output_path, 'w') as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write('<UsageDigest>\n')
                
                # Metadata
                f.write('  <Metadata>\n')
                for key, value in export_data["metadata"].items():
                    f.write(f'    <{key}>{value}</{key}>\n')
                f.write('  </Metadata>\n')
                
                # Tables
                f.write('  <Tables>\n')
                for table in export_data["tables"]:
                    f.write('    <Table>\n')
                    for key, value in table.items():
                        f.write(f'      <{key}>{value}</{key}>\n')
                    f.write('    </Table>\n')
                f.write('  </Tables>\n')
                
                # Fields
                f.write('  <Fields>\n')
                for field in export_data["fields"]:
                    f.write('    <Field>\n')
                    for key, value in field.items():
                        f.write(f'      <{key}>{value}</{key}>\n')
                    f.write('    </Field>\n')
                f.write('  </Fields>\n')
                
                # Joins
                f.write('  <Joins>\n')
                for join in export_data["joins"]:
                    f.write('    <Join>\n')
                    for key, value in join.items():
                        f.write(f'      <{key}>{value}</{key}>\n')
                    f.write('    </Join>\n')
                f.write('  </Joins>\n')
                
                f.write('</UsageDigest>\n')
        
        else:
            return {
                "error": f"Unsupported export format: {export_format}",
                "details": "Supported formats are: csv, json, xml"
            }
        
        # Prepare result
        result = {
            "status": "success",
            "message": f"Exported usage report to {output_path}",
            "export_format": export_format,
            "output_path": output_path,
            "export_data": {
                "tables_count": len(export_data["tables"]),
                "fields_count": len(export_data["fields"]),
                "joins_count": len(export_data["joins"])
            }
        }
        
        logger.info(f"Exported usage report to {output_path}")
        return result
        
    except Exception as e:
        logger.error(f"Error in export_usage_report: {e}")
        return {
            "error": "Failed to export usage report",
            "details": str(e)
        }
