"""
Advanced Inspector Tools for SQL MCP Server.

This module advances existing analysis and exploration tools to make them
more accessible and comprehensive for basic SQL users.
"""
import logging
import asyncio
from typing import Dict, List, Any, Optional, Union, Tuple
import time
import json
import datetime

# Configure logging
logger = logging.getLogger("DB_USER_BasicInspector")

# These will be imported from existing tools
analyze_table_data = None
get_table_schema = None
advanced_get_sample_data = None
advanced_search_schema_objects = None
advanced_find_related_tables = None
_get_db_connection_blocking = None
_execute_query_blocking = None
is_safe_query = None

# Set by registration function
mcp = None

# The register function that was missing
def register(mcp_instance, tool_dependencies=None, db_connection_blocking=None, execute_query_blocking=None, safe_query_func=None):
    """Register this module's functions with the MCP instance."""
    global mcp, analyze_table_data, get_table_schema, advanced_get_sample_data, advanced_search_schema_objects
    global advanced_find_related_tables, _get_db_connection_blocking, _execute_query_blocking, is_safe_query
    
    mcp = mcp_instance
    
    # Import dependencies from the provided dictionary
    if tool_dependencies is None:
        tool_dependencies = {}
    
    analyze_table_data = tool_dependencies.get('analyze_table_data')
    get_table_schema = tool_dependencies.get('get_table_schema')
    advanced_get_sample_data = tool_dependencies.get('advanced_get_sample_data')
    advanced_search_schema_objects = tool_dependencies.get('advanced_search_schema_objects')
    advanced_find_related_tables = tool_dependencies.get('advanced_find_related_tables')
    
    # Set connection and execution functions
    _get_db_connection_blocking = db_connection_blocking
    _execute_query_blocking = execute_query_blocking
    if safe_query_func:
        is_safe_query = safe_query_func
    
    # Register tools manually
    mcp.add_tool(analyze_table_data_advanced)
    mcp.add_tool(search_schema_objects_advanced)
    mcp.add_tool(find_related_tables_advanced)
    
    logger.info("Registered basic advanced inspector tools with MCP instance")


async def analyze_table_data_advanced(
    table_name: str,
    column_names: Optional[List[str]] = None,
    sample_size: int = 1000,
    include_schema: bool = True,
    include_samples: bool = True,
    include_common_values: bool = True,
    max_samples: int = 10
) -> Dict[str, Any]:
    """
    Get a comprehensive overview of a table with samples, structure, and key statistics.
    
    Args:
        table_name: Table name to explore (format: 'schema.table' or just 'table')
        column_names: Optional list of specific columns to analyze. If not provided, analyzes all columns.
        sample_size: Number of rows to sample for analysis (default: 1000, 0 for all rows)
        include_schema: Include table schema information (columns, keys, etc.)
        include_samples: Include sample data rows
        include_common_values: Include most common values for string/categorical columns
        max_samples: Maximum number of sample rows to include
        
    Returns:
        Dictionary with table overview information including structure, 
        samples, row count, and key statistics.
    """
    logger.info(f"Handling analyze_table_data_advanced: table={table_name}, include_schema={include_schema}, include_samples={include_samples}")
    
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
        
        # Get table schema if requested
        schema_info = None
        if include_schema:
            # Get column information
            columns_query = """
            SELECT 
                c.COLUMN_NAME, 
                c.DATA_TYPE, 
                c.CHARACTER_MAXIMUM_LENGTH, 
                c.NUMERIC_PRECISION, 
                c.NUMERIC_SCALE,
                c.IS_NULLABLE,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT 
                    ku.TABLE_SCHEMA,
                    ku.TABLE_NAME,
                    ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA AND c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
            """
            
            columns = await asyncio.to_thread(
                _execute_query_blocking,
                columns_query,
                (schema_name, table_name_only)
            )
            
            # Get foreign key information
            fk_query = """
            SELECT 
                fk.name AS constraint_name,
                COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
                OBJECT_NAME(fk.referenced_object_id) AS referenced_table_name,
                COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS referenced_column_name
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ?
            """
            
            foreign_keys = await asyncio.to_thread(
                _execute_query_blocking,
                fk_query,
                (schema_name, table_name_only)
            )
            
            # Get index information
            index_query = """
            SELECT 
                i.name AS index_name,
                i.type_desc AS index_type,
                i.is_unique,
                STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS indexed_columns
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.tables t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ? AND i.name IS NOT NULL
            GROUP BY i.name, i.type_desc, i.is_unique
            """
            
            try:
                indexes = await asyncio.to_thread(
                    _execute_query_blocking,
                    index_query,
                    (schema_name, table_name_only)
                )
            except Exception as e:
                # If STRING_AGG is not supported, try a simpler query
                logger.warning(f"Error in index query (STRING_AGG not supported?): {e}")
                index_query = """
                SELECT 
                    i.name AS index_name,
                    i.type_desc AS index_type,
                    i.is_unique
                FROM sys.indexes i
                JOIN sys.tables t ON i.object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = ? AND t.name = ? AND i.name IS NOT NULL
                """
                
                indexes = await asyncio.to_thread(
                    _execute_query_blocking,
                    index_query,
                    (schema_name, table_name_only)
                )
            
            schema_info = {
                "columns": columns,
                "foreign_keys": foreign_keys,
                "indexes": indexes
            }
        
        # Get row count
        count_query = f"SELECT COUNT(*) AS total_rows FROM [{schema_name}].[{table_name_only}]"
        count_result = await asyncio.to_thread(_execute_query_blocking, count_query)
        row_count = count_result[0]["total_rows"] if count_result else 0
        
        # Get sample data if requested
        sample_data = None
        if include_samples:
            # Build column list
            column_list = "*"
            if column_names:
                column_list = ", ".join([f"[{col}]" for col in column_names])
            
            # Use TOP or sample based on sample_size
            if sample_size > 0:
                sample_query = f"SELECT TOP ({max_samples}) {column_list} FROM [{schema_name}].[{table_name_only}]"
            else:
                sample_query = f"SELECT TOP ({max_samples}) {column_list} FROM [{schema_name}].[{table_name_only}]"
            
            sample_data = await asyncio.to_thread(_execute_query_blocking, sample_query)
        
        # Analyze column distributions if requested
        column_stats = None
        if include_common_values:
            column_stats = {}
            
            # Get columns to analyze (all or specified)
            columns_to_analyze = column_names if column_names else [col["COLUMN_NAME"] for col in columns] if include_schema else []
            
            # Limit to reasonable number to avoid excessive queries
            columns_to_analyze = columns_to_analyze[:20] if len(columns_to_analyze) > 20 else columns_to_analyze
            
            for column in columns_to_analyze:
                # Get basic stats
                stats_query = f"""
                SELECT 
                    COUNT(*) AS total,
                    COUNT(DISTINCT [{column}]) AS distinct_count,
                    COUNT(CASE WHEN [{column}] IS NULL THEN 1 END) AS null_count
                FROM [{schema_name}].[{table_name_only}]
                """
                
                stats_result = await asyncio.to_thread(_execute_query_blocking, stats_query)
                
                # Get top values
                top_values_query = f"""
                SELECT TOP 5 [{column}] AS value, COUNT(*) AS frequency
                FROM [{schema_name}].[{table_name_only}]
                WHERE [{column}] IS NOT NULL
                GROUP BY [{column}]
                ORDER BY COUNT(*) DESC
                """
                
                try:
                    top_values = await asyncio.to_thread(_execute_query_blocking, top_values_query)
                    
                    # Add to column stats
                    column_stats[column] = {
                        "total": stats_result[0]["total"],
                        "distinct_count": stats_result[0]["distinct_count"],
                        "null_count": stats_result[0]["null_count"],
                        "null_percentage": round((stats_result[0]["null_count"] / stats_result[0]["total"]) * 100, 2) if stats_result[0]["total"] > 0 else 0,
                        "top_values": top_values
                    }
                except Exception as e:
                    logger.warning(f"Error getting top values for column '{column}': {e}")
                    column_stats[column] = {
                        "total": stats_result[0]["total"],
                        "distinct_count": stats_result[0]["distinct_count"],
                        "null_count": stats_result[0]["null_count"],
                        "null_percentage": round((stats_result[0]["null_count"] / stats_result[0]["total"]) * 100, 2) if stats_result[0]["total"] > 0 else 0,
                        "error": str(e)
                    }
        
        # Combine all results
        results = {
            "table_name": full_table_name,
            "row_count": row_count,
            "analysis_timestamp": datetime.datetime.now().isoformat()
        }
        
        if schema_info:
            results["schema"] = schema_info
        
        if sample_data:
            results["sample_data"] = sample_data
        
        if column_stats:
            results["column_stats"] = column_stats
        
        logger.info(f"Completed advanced table analysis for {full_table_name}")
        return results
        
    except Exception as e:
        logger.error(f"Error in analyze_table_data_advanced: {e}")
        return {
            "table_name": table_name,
            "error": "Failed to analyze table data",
            "details": str(e)
        }


async def search_schema_objects_advanced(
    search_term: str,
    object_types: Optional[List[str]] = None,
    include_row_counts: bool = False,
    include_relationships: bool = False,
    include_descriptions: bool = True
) -> Dict[str, Any]:
    """
    Search for database objects with advanced metadata and organization.
    
    Args:
        search_term: Term to search for
        object_types: Types of objects to search for
        include_row_counts: Include approximate row counts for tables
        include_relationships: Include basic relationship information
        include_descriptions: Include simplified descriptions
        
    Returns:
        Dictionary with search results and advanced metadata
    """
    logger.info(f"Handling search_schema_objects_advanced: term={search_term}, include_row_counts={include_row_counts}")
    
    try:
        # Set default object types if not provided
        if object_types is None:
            object_types = ['TABLE', 'VIEW', 'COLUMN', 'PROCEDURE', 'FUNCTION']
        
        results = {
            "search_term": search_term,
            "object_types": object_types,
            "timestamp": datetime.datetime.now().isoformat(),
            "results": {}
        }
        
        # Initialize result categories
        for obj_type in object_types:
            results["results"][obj_type.lower()] = []
        
        # Search for tables
        if 'TABLE' in object_types:
            table_query = """
            SELECT 
                t.TABLE_SCHEMA, 
                t.TABLE_NAME, 
                'TABLE' as OBJECT_TYPE,
                t.TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_TYPE = 'BASE TABLE' 
            AND (t.TABLE_NAME LIKE ? OR t.TABLE_SCHEMA LIKE ?)
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
            """
            
            tables = await asyncio.to_thread(
                _execute_query_blocking,
                table_query, 
                (f'%{search_term}%', f'%{search_term}%')
            )
            
            # Add row counts if requested
            if include_row_counts and tables:
                for i, table in enumerate(tables):
                    try:
                        count_query = f"SELECT COUNT(*) AS row_count FROM [{table['TABLE_SCHEMA']}].[{table['TABLE_NAME']}]"
                        count_result = await asyncio.to_thread(_execute_query_blocking, count_query)
                        if count_result:
                            tables[i]['row_count'] = count_result[0]['row_count']
                    except Exception as e:
                        logger.warning(f"Error getting row count for {table['TABLE_SCHEMA']}.{table['TABLE_NAME']}: {e}")
                        tables[i]['row_count'] = "Error"
            
            # Add descriptions if requested
            if include_descriptions and tables:
                for i, table in enumerate(tables):
                    # Get column count as part of description
                    try:
                        col_query = f"""
                        SELECT COUNT(*) AS column_count
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{table['TABLE_SCHEMA']}' AND TABLE_NAME = '{table['TABLE_NAME']}'
                        """
                        col_result = await asyncio.to_thread(_execute_query_blocking, col_query)
                        if col_result:
                            column_count = col_result[0]['column_count']
                            tables[i]['column_count'] = column_count
                            
                            # Generate description
                            description = f"Table with {column_count} columns"
                            if include_row_counts and 'row_count' in tables[i]:
                                description += f" and {tables[i]['row_count']} rows"
                            tables[i]['description'] = description
                    except Exception as e:
                        logger.warning(f"Error generating description for {table['TABLE_SCHEMA']}.{table['TABLE_NAME']}: {e}")
            
            results["results"]["table"] = tables
        
        # Search for views
        if 'VIEW' in object_types:
            view_query = """
            SELECT 
                t.TABLE_SCHEMA, 
                t.TABLE_NAME, 
                'VIEW' as OBJECT_TYPE
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_TYPE = 'VIEW' 
            AND (t.TABLE_NAME LIKE ? OR t.TABLE_SCHEMA LIKE ?)
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
            """
            
            views = await asyncio.to_thread(
                _execute_query_blocking,
                view_query, 
                (f'%{search_term}%', f'%{search_term}%')
            )
            
            # Add descriptions if requested
            if include_descriptions and views:
                for i, view in enumerate(views):
                    # Get column count
                    try:
                        col_query = f"""
                        SELECT COUNT(*) AS column_count
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{view['TABLE_SCHEMA']}' AND TABLE_NAME = '{view['TABLE_NAME']}'
                        """
                        col_result = await asyncio.to_thread(_execute_query_blocking, col_query)
                        if col_result:
                            column_count = col_result[0]['column_count']
                            views[i]['column_count'] = column_count
                            views[i]['description'] = f"View with {column_count} columns"
                    except Exception as e:
                        logger.warning(f"Error generating description for {view['TABLE_SCHEMA']}.{view['TABLE_NAME']}: {e}")
            
            results["results"]["view"] = views
        
        # Search for columns
        if 'COLUMN' in object_types:
            column_query = """
            SELECT 
                c.TABLE_SCHEMA, 
                c.TABLE_NAME, 
                c.COLUMN_NAME, 
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.IS_NULLABLE, 
                'COLUMN' as OBJECT_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.COLUMN_NAME LIKE ?
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
            """
            
            columns = await asyncio.to_thread(
                _execute_query_blocking,
                column_query,
                (f'%{search_term}%')
            )
            
            # Add descriptions if requested
            if include_descriptions and columns:
                for i, column in enumerate(columns):
                    # Generate description based on data type
                    data_type = column['DATA_TYPE']
                    max_length = column['CHARACTER_MAXIMUM_LENGTH']
                    is_nullable = column['IS_NULLABLE']
                    
                    type_desc = data_type
                    if max_length is not None and max_length > 0:
                        type_desc += f"({max_length})"
                    
                    nullable_desc = "NULL" if is_nullable == 'YES' else "NOT NULL"
                    
                    columns[i]['description'] = f"{type_desc} {nullable_desc}"
            
            results["results"]["column"] = columns
        
        # Search for procedures if requested
        if 'PROCEDURE' in object_types:
            try:
                proc_query = """
                SELECT 
                    ROUTINE_SCHEMA,
                    ROUTINE_NAME,
                    'PROCEDURE' as OBJECT_TYPE,
                    CREATED,
                    LAST_ALTERED
                FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_TYPE = 'PROCEDURE'
                AND (ROUTINE_NAME LIKE ? OR ROUTINE_SCHEMA LIKE ?)
                ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
                """
                
                procedures = await asyncio.to_thread(
                    _execute_query_blocking,
                    proc_query,
                    (f'%{search_term}%', f'%{search_term}%')
                )
                
                if include_descriptions and procedures:
                    for i, proc in enumerate(procedures):
                        # Get parameter information for description
                        try:
                            param_query = f"""
                            SELECT 
                                PARAMETER_NAME,
                                DATA_TYPE,
                                PARAMETER_MODE
                            FROM INFORMATION_SCHEMA.PARAMETERS
                            WHERE SPECIFIC_SCHEMA = '{proc['ROUTINE_SCHEMA']}' 
                            AND SPECIFIC_NAME = '{proc['ROUTINE_NAME']}'
                            ORDER BY ORDINAL_POSITION
                            """
                            
                            params = await asyncio.to_thread(_execute_query_blocking, param_query)
                            param_count = len(params) if params else 0
                            
                            procedures[i]['parameter_count'] = param_count
                            procedures[i]['description'] = f"Stored procedure with {param_count} parameters"
                        except Exception as e:
                            logger.warning(f"Error getting parameters for {proc['ROUTINE_SCHEMA']}.{proc['ROUTINE_NAME']}: {e}")
                
                results["results"]["procedure"] = procedures
            except Exception as e:
                logger.warning(f"Error searching for procedures: {e}")
                results["results"]["procedure"] = []
        
        # Add relationships if requested
        if include_relationships and ('TABLE' in object_types or 'COLUMN' in object_types):
            # Get all foreign key relationships
            try:
                fk_query = """
                SELECT 
                    fk.name AS constraint_name,
                    OBJECT_NAME(fk.parent_object_id) AS source_table,
                    SCHEMA_NAME(s1.schema_id) AS source_schema,
                    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS source_column,
                    OBJECT_NAME(fk.referenced_object_id) AS target_table,
                    SCHEMA_NAME(s2.schema_id) AS target_schema,
                    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS target_column
                FROM 
                    sys.foreign_keys fk
                JOIN 
                    sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                JOIN 
                    sys.tables t1 ON fk.parent_object_id = t1.object_id
                JOIN 
                    sys.schemas s1 ON t1.schema_id = s1.schema_id
                JOIN 
                    sys.tables t2 ON fk.referenced_object_id = t2.object_id
                JOIN 
                    sys.schemas s2 ON t2.schema_id = s2.schema_id
                """
                
                fk_relationships = await asyncio.to_thread(_execute_query_blocking, fk_query)
                
                # Match relationships to tables in results
                if 'TABLE' in object_types and 'table' in results["results"]:
                    for i, table in enumerate(results["results"]["table"]):
                        table_name = table['TABLE_NAME']
                        schema_name = table['TABLE_SCHEMA']
                        
                        # Find relationships where this table is source or target
                        related_tables = []
                        
                        for fk in fk_relationships:
                            # Table is source (has foreign key to another table)
                            if fk['source_schema'] == schema_name and fk['source_table'] == table_name:
                                related_tables.append({
                                    "table": f"{fk['target_schema']}.{fk['target_table']}",
                                    "relationship": f"References via FK {fk['constraint_name']}",
                                    "join_condition": f"{schema_name}.{table_name}.{fk['source_column']} = {fk['target_schema']}.{fk['target_table']}.{fk['target_column']}"
                                })
                            
                            # Table is target (referenced by another table)
                            if fk['target_schema'] == schema_name and fk['target_table'] == table_name:
                                related_tables.append({
                                    "table": f"{fk['source_schema']}.{fk['source_table']}",
                                    "relationship": f"Referenced by FK {fk['constraint_name']}",
                                    "join_condition": f"{fk['source_schema']}.{fk['source_table']}.{fk['source_column']} = {schema_name}.{table_name}.{fk['target_column']}"
                                })
                        
                        if related_tables:
                            results["results"]["table"][i]['related_tables'] = related_tables
                
                # Match relationships to columns in results
                if 'COLUMN' in object_types and 'column' in results["results"]:
                    for i, column in enumerate(results["results"]["column"]):
                        col_name = column['COLUMN_NAME']
                        table_name = column['TABLE_NAME']
                        schema_name = column['TABLE_SCHEMA']
                        
                        # Find relationships where this column is involved
                        related_columns = []
                        
                        for fk in fk_relationships:
                            # Column is source (foreign key)
                            if fk['source_schema'] == schema_name and fk['source_table'] == table_name and fk['source_column'] == col_name:
                                related_columns.append({
                                    "table_column": f"{fk['target_schema']}.{fk['target_table']}.{fk['target_column']}",
                                    "relationship": "References (FK)",
                                    "constraint_name": fk['constraint_name']
                                })
                            
                            # Column is target (referenced by foreign key)
                            if fk['target_schema'] == schema_name and fk['target_table'] == table_name and fk['target_column'] == col_name:
                                related_columns.append({
                                    "table_column": f"{fk['source_schema']}.{fk['source_table']}.{fk['source_column']}",
                                    "relationship": "Referenced by (FK)",
                                    "constraint_name": fk['constraint_name']
                                })
                        
                        if related_columns:
                            results["results"]["column"][i]['related_columns'] = related_columns
            
            except Exception as e:
                logger.warning(f"Error getting relationships: {e}")
                results["relationships_error"] = str(e)
        
        # Count total matches
        total_matches = 0
        for obj_type in results["results"]:
            total_matches += len(results["results"][obj_type])
        
        results["total_matches"] = total_matches
        
        if total_matches == 0:
            results["message"] = f"No objects found matching '{search_term}'"
        else:
            results["message"] = f"Found {total_matches} objects matching '{search_term}'"
        
        logger.info(f"Advanced search for '{search_term}' found {total_matches} matches")
        return results
        
    except Exception as e:
        logger.error(f"Error in search_schema_objects_advanced: {e}")
        return {
            "search_term": search_term,
            "error": "Failed to search database objects",
            "details": str(e)
        }


async def find_related_tables_advanced(
    table_name: str,
    include_sample_joins: bool = True,
    max_relation_depth: int = 1,
    include_example_rows: bool = False,
    max_examples: int = 3
) -> Dict[str, Any]:
    """
    Explore relationships between tables with advanced context and examples.
    
    Args:
        table_name: Starting table
        include_sample_joins: Include example queries showing joins
        max_relation_depth: How many relationship levels to explore (1=direct only)
        include_example_rows: Include example rows showing joined data
        max_examples: Maximum number of example rows to include
        
    Returns:
        Dictionary with related tables, relationship types, join columns,
        and optional example join queries and joined data samples.
    """
    logger.info(f"Handling find_related_tables_advanced: table={table_name}, depth={max_relation_depth}")
    
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
        
        # Get table structure for context
        table_info_query = """
        SELECT 
            c.COLUMN_NAME, 
            c.DATA_TYPE, 
            c.IS_NULLABLE,
            CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN (
            SELECT 
                ku.TABLE_SCHEMA,
                ku.TABLE_NAME,
                ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA AND c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
        """
        
        columns = await asyncio.to_thread(
            _execute_query_blocking,
            table_info_query,
            (schema_name, table_name_only)
        )
        
        # Initialize results
        results = {
            "table_name": full_table_name,
            "timestamp": datetime.datetime.now().isoformat(),
            "columns": columns,
            "relationships": {
                "outgoing": [],  # This table references other tables
                "incoming": [],  # Other tables reference this table
                "inferred": []   # Potential relationships based on naming conventions
            }
        }
        
        # Keep track of all tables we've found (to avoid circular references in deeper searches)
        processed_tables = set([full_table_name.lower()])
        
        # Process each depth level
        tables_to_process = [(schema_name, table_name_only, 0)]  # (schema, table, current_depth)
        
        while tables_to_process:
            current_schema, current_table, current_depth = tables_to_process.pop(0)
            
            if current_depth > max_relation_depth:
                continue  # Skip if we're beyond the requested depth
            
            # Only process the root table's relationships if this is depth 0
            is_root_table = current_depth == 0
            
            # Get outgoing relationships (foreign keys from this table)
            outgoing_fk_query = """
            SELECT 
                fk.name AS constraint_name,
                OBJECT_NAME(fk.parent_object_id) AS source_table,
                SCHEMA_NAME(s1.schema_id) AS source_schema,
                COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS source_column,
                OBJECT_NAME(fk.referenced_object_id) AS target_table,
                SCHEMA_NAME(s2.schema_id) AS target_schema,
                COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS target_column
            FROM 
                sys.foreign_keys fk
            JOIN 
                sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN 
                sys.tables t1 ON fk.parent_object_id = t1.object_id
            JOIN 
                sys.schemas s1 ON t1.schema_id = s1.schema_id
            JOIN 
                sys.tables t2 ON fk.referenced_object_id = t2.object_id
            JOIN 
                sys.schemas s2 ON t2.schema_id = s2.schema_id
            WHERE 
                s1.name = ? AND t1.name = ?
            """
            
            outgoing_relationships = await asyncio.to_thread(
                _execute_query_blocking,
                outgoing_fk_query,
                (current_schema, current_table)
            )
            
            # Add to results if root table
            if is_root_table:
                for rel in outgoing_relationships:
                    relationship = {
                        'related_table': f"{rel['target_schema']}.{rel['target_table']}",
                        'relationship_type': 'outgoing',
                        'join_condition': f"{current_schema}.{current_table}.{rel['source_column']} = {rel['target_schema']}.{rel['target_table']}.{rel['target_column']}",
                        'description': f"{current_schema}.{current_table} has a foreign key to {rel['target_schema']}.{rel['target_table']}",
                        'constraint_name': rel['constraint_name'],
                        'source_column': rel['source_column'],
                        'target_column': rel['target_column']
                    }
                    
                    # Add sample join query if requested
                    if include_sample_joins:
                        # Get column lists for both tables
                        source_cols = [col['COLUMN_NAME'] for col in columns]
                        source_cols_str = ", ".join([f"a.[{col}]" for col in source_cols])
                        
                        target_cols_query = f"""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{rel['target_schema']}' AND TABLE_NAME = '{rel['target_table']}'
                        ORDER BY ORDINAL_POSITION
                        """
                        
                        target_cols = await asyncio.to_thread(_execute_query_blocking, target_cols_query)
                        target_cols_str = ", ".join([f"b.[{col['COLUMN_NAME']}]" for col in target_cols])
                        
                        # Create sample join query
                        join_query = f"""
SELECT {source_cols_str}, {target_cols_str}
FROM [{current_schema}].[{current_table}] a
JOIN [{rel['target_schema']}].[{rel['target_table']}] b ON a.[{rel['source_column']}] = b.[{rel['target_column']}]
-- Add WHERE clause here if needed
-- ORDER BY a.[{source_cols[0]}]
LIMIT 10
                        """.strip()
                        
                        relationship['sample_join_query'] = join_query
                    
                    # Add example rows if requested
                    if include_example_rows:
                        try:
                            example_query = f"""
                            SELECT TOP {max_examples} a.*, b.*
                            FROM [{current_schema}].[{current_table}] a
                            JOIN [{rel['target_schema']}].[{rel['target_table']}] b 
                                ON a.[{rel['source_column']}] = b.[{rel['target_column']}]
                            """
                            
                            example_rows = await asyncio.to_thread(_execute_query_blocking, example_query)
                            relationship['example_rows'] = example_rows
                        except Exception as e:
                            logger.warning(f"Error getting example rows for join: {e}")
                            relationship['example_rows_error'] = str(e)
                    
                    results["relationships"]["outgoing"].append(relationship)
                    
                    # Add to tables to process for next depth
                    if current_depth < max_relation_depth:
                        related_table_full = f"{rel['target_schema']}.{rel['target_table']}".lower()
                        if related_table_full not in processed_tables:
                            processed_tables.add(related_table_full)
                            tables_to_process.append((rel['target_schema'], rel['target_table'], current_depth + 1))
            
            # Get incoming relationships (foreign keys to this table)
            incoming_fk_query = """
            SELECT 
                fk.name AS constraint_name,
                OBJECT_NAME(fk.parent_object_id) AS source_table,
                SCHEMA_NAME(s1.schema_id) AS source_schema,
                COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS source_column,
                OBJECT_NAME(fk.referenced_object_id) AS target_table,
                SCHEMA_NAME(s2.schema_id) AS target_schema,
                COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS target_column
            FROM 
                sys.foreign_keys fk
            JOIN 
                sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN 
                sys.tables t1 ON fk.parent_object_id = t1.object_id
            JOIN 
                sys.schemas s1 ON t1.schema_id = s1.schema_id
            JOIN 
                sys.tables t2 ON fk.referenced_object_id = t2.object_id
            JOIN 
                sys.schemas s2 ON t2.schema_id = s2.schema_id
            WHERE 
                s2.name = ? AND t2.name = ?
            """
            
            incoming_relationships = await asyncio.to_thread(
                _execute_query_blocking,
                incoming_fk_query,
                (current_schema, current_table)
            )
            
            # Add to results if root table
            if is_root_table:
                for rel in incoming_relationships:
                    relationship = {
                        'related_table': f"{rel['source_schema']}.{rel['source_table']}",
                        'relationship_type': 'incoming',
                        'join_condition': f"{rel['source_schema']}.{rel['source_table']}.{rel['source_column']} = {current_schema}.{current_table}.{rel['target_column']}",
                        'description': f"{rel['source_schema']}.{rel['source_table']} has a foreign key to {current_schema}.{current_table}",
                        'constraint_name': rel['constraint_name'],
                        'source_column': rel['source_column'],
                        'target_column': rel['target_column']
                    }
                    
                    # Add sample join query if requested
                    if include_sample_joins:
                        # Get column lists for both tables
                        target_cols = [col['COLUMN_NAME'] for col in columns]
                        target_cols_str = ", ".join([f"b.[{col}]" for col in target_cols])
                        
                        source_cols_query = f"""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{rel['source_schema']}' AND TABLE_NAME = '{rel['source_table']}'
                        ORDER BY ORDINAL_POSITION
                        """
                        
                        source_cols = await asyncio.to_thread(_execute_query_blocking, source_cols_query)
                        source_cols_str = ", ".join([f"a.[{col['COLUMN_NAME']}]" for col in source_cols])
                        
                        # Create sample join query
                        join_query = f"""
SELECT {source_cols_str}, {target_cols_str}
FROM [{rel['source_schema']}].[{rel['source_table']}] a
JOIN [{current_schema}].[{current_table}] b ON a.[{rel['source_column']}] = b.[{rel['target_column']}]
-- Add WHERE clause here if needed
-- ORDER BY a.[{source_cols[0]['COLUMN_NAME']}]
LIMIT 10
                        """.strip()
                        
                        relationship['sample_join_query'] = join_query
                    
                    # Add example rows if requested
                    if include_example_rows:
                        try:
                            example_query = f"""
                            SELECT TOP {max_examples} a.*, b.*
                            FROM [{rel['source_schema']}].[{rel['source_table']}] a
                            JOIN [{current_schema}].[{current_table}] b 
                                ON a.[{rel['source_column']}] = b.[{rel['target_column']}]
                            """
                            
                            example_rows = await asyncio.to_thread(_execute_query_blocking, example_query)
                            relationship['example_rows'] = example_rows
                        except Exception as e:
                            logger.warning(f"Error getting example rows for join: {e}")
                            relationship['example_rows_error'] = str(e)
                    
                    results["relationships"]["incoming"].append(relationship)
                    
                    # Add to tables to process for next depth
                    if current_depth < max_relation_depth:
                        related_table_full = f"{rel['source_schema']}.{rel['source_table']}".lower()
                        if related_table_full not in processed_tables:
                            processed_tables.add(related_table_full)
                            tables_to_process.append((rel['source_schema'], rel['source_table'], current_depth + 1))
        
        # If no relationships found via foreign keys, try to infer some based on naming conventions
        if is_root_table and not results["relationships"]["outgoing"] and not results["relationships"]["incoming"]:
            # Look for tables with columns that might reference this table
            # Common patterns: table_id, tableId, id_table, etc.
            
            # Get primary key columns
            pk_columns = [col['COLUMN_NAME'] for col in columns if col.get('IS_PRIMARY_KEY') == 'YES']
            
            if pk_columns:
                primary_key = pk_columns[0]  # Use first PK if multiple
                
                # Look for other tables with columns named like 'table_name_id' or 'tableNameId'
                potential_fk_patterns = [
                    f"{table_name_only}ID",
                    f"{table_name_only}_ID", 
                    f"{table_name_only}Id", 
                    f"{table_name_only}_Id",
                    f"{table_name_only}_id",
                    f"ID{table_name_only}",
                    f"Id{table_name_only}"
                ]
                
                for pattern in potential_fk_patterns:
                    inferred_query = f"""
                    SELECT 
                        c.TABLE_SCHEMA, 
                        c.TABLE_NAME, 
                        c.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    JOIN INFORMATION_SCHEMA.TABLES t 
                        ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
                    WHERE 
                        (c.COLUMN_NAME LIKE '{pattern}' OR c.COLUMN_NAME LIKE '{pattern}%') 
                        AND t.TABLE_TYPE = 'BASE TABLE'
                        AND NOT (c.TABLE_SCHEMA = '{schema_name}' AND c.TABLE_NAME = '{table_name_only}')
                    """
                    
                    inferred_results = await asyncio.to_thread(_execute_query_blocking, inferred_query)
                    
                    for inferred in inferred_results:
                        inferred_table = f"{inferred['TABLE_SCHEMA']}.{inferred['TABLE_NAME']}"
                        if inferred_table.lower() not in processed_tables:
                            results["relationships"]["inferred"].append({
                                'related_table': inferred_table,
                                'relationship_type': 'inferred',
                                'possible_join_column': inferred['COLUMN_NAME'],
                                'description': f"Table {inferred_table} has column {inferred['COLUMN_NAME']} which might reference {full_table_name}",
                                'confidence': "Medium"
                            })
                
                # Also look for columns in other tables with the same name as the primary key
                pk_query = f"""
                SELECT 
                    c.TABLE_SCHEMA, 
                    c.TABLE_NAME, 
                    c.COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS c
                JOIN INFORMATION_SCHEMA.TABLES t 
                    ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
                WHERE 
                    c.COLUMN_NAME = '{primary_key}'
                    AND t.TABLE_TYPE = 'BASE TABLE'
                    AND NOT (c.TABLE_SCHEMA = '{schema_name}' AND c.TABLE_NAME = '{table_name_only}')
                """
                
                pk_matches = await asyncio.to_thread(_execute_query_blocking, pk_query)
                
                for match in pk_matches:
                    inferred_table = f"{match['TABLE_SCHEMA']}.{match['TABLE_NAME']}"
                    if inferred_table.lower() not in processed_tables:
                        results["relationships"]["inferred"].append({
                            'related_table': inferred_table,
                            'relationship_type': 'inferred',
                            'possible_join_column': match['COLUMN_NAME'],
                            'description': f"Table {inferred_table} has column with same name as primary key of {full_table_name}",
                            'confidence': "Low"
                        })
        
        # Get total count of relationships
        total_relationships = len(results["relationships"]["outgoing"]) + len(results["relationships"]["incoming"]) + len(results["relationships"]["inferred"])
        results["total_relationships"] = total_relationships
        
        if total_relationships == 0:
            results["message"] = f"No relationships found for table {full_table_name}"
        else:
            results["message"] = f"Found {total_relationships} relationships for table {full_table_name}"
        
        logger.info(f"Found {total_relationships} relationships for {full_table_name}")
        return results
    
    except Exception as e:
        logger.error(f"Error in find_related_tables_advanced: {e}")
        return {
            "table_name": table_name,
            "error": "Failed to find related tables",
            "details": str(e)
        }