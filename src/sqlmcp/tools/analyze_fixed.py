"""
Data analysis tools for SQL MCP Server.

This module provides tools for analyzing database table data, finding patterns,
and identifying potential data issues.
"""
import logging
import asyncio
from typing import Dict, List, Any, Optional
import time

# Configure logging
logger = logging.getLogger("DB_USER_Analyze")

# These will be set from the main server file
mcp = None
get_db_connection = None
_get_db_connection_blocking = None
_execute_query_blocking = None

def register_tools(mcp_instance, db_connection_function, db_connection_blocking, execute_query_blocking):
    """Register this module's functions with the MCP instance."""
    global mcp, get_db_connection, _get_db_connection_blocking, _execute_query_blocking
    mcp = mcp_instance
    get_db_connection = db_connection_function
    _get_db_connection_blocking = db_connection_blocking
    _execute_query_blocking = execute_query_blocking
    
    # Register tools manually
    mcp.add_tool(analyze_table_data)
    mcp.add_tool(find_duplicate_records)
    
    logger.info("Registered analyze tools with MCP instance")

async def analyze_table_data(
    table_name: str,
    column_names: Optional[List[str]] = None,
    sample_size: int = 1000
) -> Dict[str, Any]:
    """
    Analyze table data to provide insights on column distributions and statistics.
    
    Args:
        table_name: Table name (format: 'schema.table' or just 'table' for default 'dbo' schema)
        column_names: Optional list of specific columns to analyze. If not provided, analyzes all columns.
        sample_size: Number of rows to sample for analysis (default: 1000, 0 for all rows)
        
    Returns:
        Dictionary containing analysis results for each analyzed column
    """
    logger.info(f"Handling analyze_table_data: table_name={table_name}, columns={column_names}, sample_size={sample_size}")
    
    # Parse schema and table name
    parts = table_name.split('.')
    if len(parts) == 2:
        schema_name, table_name_only = parts
    else:
        schema_name = 'dbo'  # Default schema
        table_name_only = parts[0]
    
    # Verify table exists
    validate_query = "SELECT COUNT(*) AS count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
    
    try:
        # Validate table exists using the existing blocking function wrapped in asyncio.to_thread
        validation_result = await asyncio.to_thread(
            _execute_query_blocking,
            validate_query,
            (schema_name, table_name_only)
        )
        
        if not validation_result or validation_result[0].get('count', 0) == 0:
            return {
                "error": f"Table '{schema_name}.{table_name_only}' not found",
                "details": "The specified table does not exist or is not accessible"
            }
        
        # Get column information
        columns_query = """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        
        columns_info = await asyncio.to_thread(
            _execute_query_blocking,
            columns_query,
            (schema_name, table_name_only)
        )
        
        # Filter columns if specific ones requested
        if column_names:
            columns_info = [col for col in columns_info if col['COLUMN_NAME'] in column_names]
            
            # Check if all requested columns exist
            found_columns = set(col['COLUMN_NAME'] for col in columns_info)
            missing_columns = set(column_names) - found_columns
            
            if missing_columns:
                return {
                    "error": "Invalid column names",
                    "details": f"The following columns do not exist in the table: {', '.join(missing_columns)}"
                }
        
        # Get total row count
        count_query = f"SELECT COUNT(*) AS row_count FROM [{schema_name}].[{table_name_only}]"
        count_result = await asyncio.to_thread(_execute_query_blocking, count_query)
        total_rows = count_result[0]['row_count'] if count_result else 0
        
        # Prepare analysis results
        analysis_results = {
            "table_name": f"{schema_name}.{table_name_only}",
            "total_rows": total_rows,
            "analyzed_rows": min(sample_size, total_rows) if sample_size > 0 else total_rows,
            "column_analysis": {}
        }
        
        # Analyze each column
        for column in columns_info:
            column_name = column['COLUMN_NAME']
            data_type = column['DATA_TYPE']
            
            # Get sample limit clause
            sample_clause = f"TOP {sample_size}" if sample_size > 0 else ""
            
            # Analyze null values
            null_query = f"""
            SELECT COUNT(*) AS null_count
            FROM (
                SELECT {sample_clause} *
                FROM [{schema_name}].[{table_name_only}]
                {f"ORDER BY NEWID()" if sample_size > 0 else ""}
            ) AS sample_data
            WHERE [{column_name}] IS NULL
            """
            
            null_result = await asyncio.to_thread(_execute_query_blocking, null_query)
            null_count = null_result[0]['null_count'] if null_result else 0
            
            # Initialize column analysis
            column_analysis = {
                "data_type": data_type,
                "null_count": null_count,
                "null_percentage": round((null_count / min(sample_size, total_rows) * 100), 2) if total_rows > 0 else 0
            }
            
            # Analyze distinct values
            distinct_query = f"""
            SELECT COUNT(DISTINCT [{column_name}]) AS distinct_count
            FROM (
                SELECT {sample_clause} *
                FROM [{schema_name}].[{table_name_only}]
                {f"ORDER BY NEWID()" if sample_size > 0 else ""}
            ) AS sample_data
            """
            
            distinct_result = await asyncio.to_thread(_execute_query_blocking, distinct_query)
            distinct_count = distinct_result[0]['distinct_count'] if distinct_result else 0
            
            column_analysis["distinct_values"] = distinct_count
            
            # Get frequency distribution for top values (non-numeric types)
            if data_type.lower() in ('char', 'varchar', 'nchar', 'nvarchar', 'text', 'ntext'):
                frequency_query = f"""
                SELECT TOP 10 [{column_name}] AS value, COUNT(*) AS frequency
                FROM (
                    SELECT {sample_clause} *
                    FROM [{schema_name}].[{table_name_only}]
                    {f"ORDER BY NEWID()" if sample_size > 0 else ""}
                ) AS sample_data
                WHERE [{column_name}] IS NOT NULL
                GROUP BY [{column_name}]
                ORDER BY COUNT(*) DESC
                """
                
                frequency_result = await asyncio.to_thread(_execute_query_blocking, frequency_query)
                
                column_analysis["top_values"] = [{
                    "value": str(row['value']),
                    "frequency": row['frequency'],
                    "percentage": round((row['frequency'] / (min(sample_size, total_rows) - null_count) * 100), 2) if (min(sample_size, total_rows) - null_count) > 0 else 0
                } for row in frequency_result]
                
                # Get length statistics for string columns
                length_query = f"""
                SELECT 
                    MIN(LEN([{column_name}])) AS min_length,
                    MAX(LEN([{column_name}])) AS max_length,
                    AVG(CAST(LEN([{column_name}]) AS FLOAT)) AS avg_length
                FROM (
                    SELECT {sample_clause} *
                    FROM [{schema_name}].[{table_name_only}]
                    {f"ORDER BY NEWID()" if sample_size > 0 else ""}
                ) AS sample_data
                WHERE [{column_name}] IS NOT NULL
                """
                
                length_result = await asyncio.to_thread(_execute_query_blocking, length_query)
                
                if length_result:
                    column_analysis["length_stats"] = {
                        "min": length_result[0]['min_length'],
                        "max": length_result[0]['max_length'],
                        "avg": round(length_result[0]['avg_length'], 2)
                    }
            
            # Get numeric statistics (numeric types)
            elif data_type.lower() in ('tinyint', 'smallint', 'int', 'bigint', 'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney'):
                stats_query = f"""
                SELECT 
                    MIN([{column_name}]) AS min_value,
                    MAX([{column_name}]) AS max_value,
                    AVG(CAST([{column_name}] AS FLOAT)) AS avg_value,
                    SUM([{column_name}]) AS sum_value
                FROM (
                    SELECT {sample_clause} *
                    FROM [{schema_name}].[{table_name_only}]
                    {f"ORDER BY NEWID()" if sample_size > 0 else ""}
                ) AS sample_data
                WHERE [{column_name}] IS NOT NULL
                """
                
                stats_result = await asyncio.to_thread(_execute_query_blocking, stats_query)
                
                if stats_result:
                    column_analysis["numeric_stats"] = {
                        "min": stats_result[0]['min_value'],
                        "max": stats_result[0]['max_value'],
                        "avg": round(stats_result[0]['avg_value'], 2) if stats_result[0]['avg_value'] is not None else None,
                        "sum": stats_result[0]['sum_value']
                    }
            
            # Get date statistics (date types)
            elif data_type.lower() in ('date', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset'):
                date_stats_query = f"""
                SELECT 
                    MIN([{column_name}]) AS min_date,
                    MAX([{column_name}]) AS max_date,
                    DATEDIFF(day, MIN([{column_name}]), MAX([{column_name}])) AS date_range_days
                FROM (
                    SELECT {sample_clause} *
                    FROM [{schema_name}].[{table_name_only}]
                    {f"ORDER BY NEWID()" if sample_size > 0 else ""}
                ) AS sample_data
                WHERE [{column_name}] IS NOT NULL
                """
                
                date_stats_result = await asyncio.to_thread(_execute_query_blocking, date_stats_query)
                
                if date_stats_result:
                    column_analysis["date_stats"] = {
                        "min_date": date_stats_result[0]['min_date'].isoformat() if date_stats_result[0]['min_date'] else None,
                        "max_date": date_stats_result[0]['max_date'].isoformat() if date_stats_result[0]['max_date'] else None,
                        "date_range_days": date_stats_result[0]['date_range_days']
                    }
            
            # Add column analysis to results
            analysis_results["column_analysis"][column_name] = column_analysis
        
        logger.info(f"Completed data analysis for {schema_name}.{table_name_only}")
        return analysis_results
        
    except Exception as e:
        logger.error(f"Error in analyze_table_data: {e}")
        return {
            "error": "Failed to analyze table data",
            "details": str(e)
        }

async def find_duplicate_records(
    table_name: str,
    column_names: List[str],
    sample_size: int = 1000,
    min_duplicates: int = 2
) -> Dict[str, Any]:
    """
    Find potential duplicate records in a table based on specified columns.
    
    Args:
        table_name: Table name (format: 'schema.table' or just 'table' for default 'dbo' schema)
        column_names: List of column names to check for duplicates
        sample_size: Maximum number of rows to sample (default: 1000, 0 for all rows)
        min_duplicates: Minimum number of duplicates to qualify for reporting (default: 2)
        
    Returns:
        Dictionary containing duplicate groups found
    """
    logger.info(f"Handling find_duplicate_records: table_name={table_name}, columns={column_names}, sample_size={sample_size}")
    
    # Validate inputs
    if not column_names:
        return {
            "error": "No columns specified",
            "details": "You must specify at least one column to check for duplicates"
        }
    
    # Parse schema and table name
    parts = table_name.split('.')
    if len(parts) == 2:
        schema_name, table_name_only = parts
    else:
        schema_name = 'dbo'  # Default schema
        table_name_only = parts[0]
    
    # Verify table exists
    validate_query = "SELECT COUNT(*) AS count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
    
    try:
        # Validate table exists using existing blocking function wrapped in asyncio.to_thread
        validation_result = await asyncio.to_thread(
            _execute_query_blocking,
            validate_query,
            (schema_name, table_name_only)
        )
        
        if not validation_result or validation_result[0].get('count', 0) == 0:
            return {
                "error": f"Table '{schema_name}.{table_name_only}' not found",
                "details": "The specified table does not exist or is not accessible"
            }
        
        # Verify columns exist
        columns_query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """
        
        columns_info = await asyncio.to_thread(
            _execute_query_blocking,
            columns_query,
            (schema_name, table_name_only)
        )
        
        valid_columns = [col['COLUMN_NAME'] for col in columns_info]
        
        # Check if all requested columns exist
        invalid_columns = [col for col in column_names if col not in valid_columns]
        
        if invalid_columns:
            return {
                "error": "Invalid column names",
                "details": f"The following columns do not exist in the table: {', '.join(invalid_columns)}"
            }
        
        # Format column list for SQL
        column_list = ", ".join([f"[{col}]" for col in column_names])
        
        # Determine if we need a primary key for the results
        pk_query = """
        SELECT ku.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
            ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
            AND tc.TABLE_NAME = ku.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            AND tc.TABLE_SCHEMA = ?
            AND tc.TABLE_NAME = ?
        """
        
        pk_result = await asyncio.to_thread(
            _execute_query_blocking,
            pk_query,
            (schema_name, table_name_only)
        )
        
        pk_columns = [row['COLUMN_NAME'] for row in pk_result]
        
        # Add primary key columns to select clause (if not already included)
        additional_columns = [col for col in pk_columns if col not in column_names]
        select_column_list = column_list
        
        if additional_columns:
            select_column_list = f"{column_list}, {', '.join([f'[{col}]' for col in additional_columns])}"
        
        # Get sample limit clause
        sample_clause = f"TOP {sample_size}" if sample_size > 0 else ""
        
        # Build the query to find duplicates
        duplicates_query = f"""
        WITH sample_data AS (
            SELECT {sample_clause} {select_column_list}
            FROM [{schema_name}].[{table_name_only}]
            {f"ORDER BY NEWID()" if sample_size > 0 else ""}
        ),
        duplicate_groups AS (
            SELECT {column_list}, COUNT(*) AS duplicate_count
            FROM sample_data
            GROUP BY {column_list}
            HAVING COUNT(*) >= {min_duplicates}
        )
        SELECT s.*
        FROM sample_data s
        INNER JOIN duplicate_groups d ON {' AND '.join([f"s.[{col}] = d.[{col}]" for col in column_names])}
        ORDER BY {column_list}
        """
        
        # Execute the query
        duplicates_result = await asyncio.to_thread(_execute_query_blocking, duplicates_query)
        
        # If no duplicates found
        if not duplicates_result:
            return {
                "table_name": f"{schema_name}.{table_name_only}",
                "columns_checked": column_names,
                "duplicate_groups_found": 0,
                "message": "No duplicate records found based on the specified columns"
            }
        
        # Process results into groups
        duplicate_groups = {}
        
        for row in duplicates_result:
            # Create a key for grouping
            group_key = "_".join([str(row[col]) for col in column_names])
            
            # Initialize group if not exists
            if group_key not in duplicate_groups:
                duplicate_groups[group_key] = {
                    "key_values": {col: row[col] for col in column_names},
                    "records": []
                }
            
            # Add record to group
            record = {col: row[col] for col in row}
            duplicate_groups[group_key]["records"].append(record)
        
        # Format results
        groups_list = list(duplicate_groups.values())
        
        result = {
            "table_name": f"{schema_name}.{table_name_only}",
            "columns_checked": column_names,
            "duplicate_groups_found": len(groups_list),
            "duplicate_groups": groups_list
        }
        
        logger.info(f"Found {len(groups_list)} duplicate groups in {schema_name}.{table_name_only}")
        return result
        
    except Exception as e:
        logger.error(f"Error in find_duplicate_records: {e}")
        return {
            "error": "Failed to find duplicate records",
            "details": str(e)
        }
