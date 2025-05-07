"""
Data analysis tools for SQL MCP Server.

This module provides tools for analyzing database table data, finding patterns,
and identifying potential data issues.
"""
import logging
import asyncio
from typing import Dict, List, Any, Optional
import time
import math

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
            
            # Use a more robust approach for getting sample data
            sample_query = f"""
            SELECT {sample_clause} [{column_name}]
            FROM [{schema_name}].[{table_name_only}]
            {f"ORDER BY NEWID()" if sample_size > 0 else ""}
            """
            
            try:
                sample_data = await asyncio.to_thread(_execute_query_blocking, sample_query)
                
                # Count null values
                null_count = sum(1 for row in sample_data if row[column_name] is None)
                
                # Initialize column analysis
                column_analysis = {
                    "data_type": data_type,
                    "null_count": null_count
                }
                
                # Calculate null percentage safely
                analyzed_rows = len(sample_data)
                if analyzed_rows > 0:
                    column_analysis["null_percentage"] = round((null_count / analyzed_rows * 100), 2)
                else:
                    column_analysis["null_percentage"] = 0
                
                # Count distinct values (without using SQL which might overflow)
                non_null_values = [row[column_name] for row in sample_data if row[column_name] is not None]
                distinct_values = set()
                for val in non_null_values:
                    # Convert to string to handle any type
                    if val is not None:
                        try:
                            distinct_values.add(str(val AS FLOAT))
                        except:
                            # Skip values that can't be converted to string
                            pass
                
                column_analysis["distinct_values"] = len(distinct_values)
                
                # Analyze based on data type
                if data_type.lower() in ('char', 'varchar', 'nchar', 'nvarchar', 'text', 'ntext'):
                    # Text analysis
                    try:
                        # Frequency distribution for string values
                        value_counts = {}
                        for val in non_null_values:
                            str_val = str(val)
                            if str_val in value_counts:
                                value_counts[str_val] += 1
                            else:
                                value_counts[str_val] = 1
                        
                        # Sort by frequency and get top 10
                        top_values = sorted(value_counts.items(), key=lambda x: x[1], reverse=True)[:10]
                        
                        column_analysis["top_values"] = [{
                            "value": value,
                            "frequency": count,
                            "percentage": round((count / len(non_null_values) * 100), 2) if non_null_values else 0
                        } for value, count in top_values]
                        
                        # Length statistics
                        if non_null_values:
                            lengths = [len(str(val AS FLOAT)) for val in non_null_values if val is not None]
                            if lengths:
                                column_analysis["length_stats"] = {
                                    "min": min(lengths),
                                    "max": max(lengths),
                                    "avg": round(sum(lengths) / len(lengths), 2)
                                }
                    except Exception as e:
                        logger.warning(f"Error analyzing text column {column_name}: {e}")
                
                elif data_type.lower() in ('tinyint', 'smallint', 'int', 'bigint', 'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney'):
                    # Numeric analysis
                    try:
                        # Convert to float for calculations to avoid overflow
                        numeric_values = []
                        for val in non_null_values:
                            try:
                                numeric_values.append(float(val AS FLOAT))
                            except (ValueError, TypeError):
                                # Skip values that can't be converted to float
                                pass
                        
                        if numeric_values:
                            column_analysis["numeric_stats"] = {
                                "min": min(numeric_values),
                                "max": max(numeric_values),
                                "avg": round(sum(numeric_values) / len(numeric_values), 2),
                                # Use safe sum calculation
                                "sum": round(sum(numeric_values), 2)
                            }
                    except Exception as e:
                        logger.warning(f"Error analyzing numeric column {column_name}: {e}")
                        # Provide a fallback using SQL but with DECIMAL casting to avoid overflow
                        try:
                            safe_stats_query = f"""
                            SELECT 
                                MIN(CAST([{column_name}] AS DECIMAL(38,4 AS FLOAT))) AS min_value,
                                MAX(CAST([{column_name}] AS DECIMAL(38,4 AS FLOAT))) AS max_value,
                                AVG(CAST(CAST(CAST(CAST([{column_name}] AS DECIMAL(38,4 AS FLOAT))) AS avg_value
                            FROM (
                                SELECT {sample_clause} *
                                FROM [{schema_name}].[{table_name_only}]
                                {f"ORDER BY NEWID()" if sample_size > 0 else ""}
                            ) AS sample_data
                            WHERE [{column_name}] IS NOT NULL
                            """
                            
                            safe_stats_result = await asyncio.to_thread(_execute_query_blocking, safe_stats_query)
                            
                            if safe_stats_result:
                                column_analysis["numeric_stats"] = {
                                    "min": safe_stats_result[0]['min_value'],
                                    "max": safe_stats_result[0]['max_value'],
                                    "avg": round(safe_stats_result[0]['avg_value'], 2) if safe_stats_result[0]['avg_value'] is not None else None,
                                    # Sum not included due to potential overflow
                                }
                        except Exception as e2:
                            logger.error(f"Error in fallback numeric analysis for {column_name}: {e2}")
                            column_analysis["numeric_stats"] = {
                                "error": "Could not calculate numeric statistics due to potential overflow or data type issues"
                            }
                
                elif data_type.lower() in ('date', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset'):
                    # Date analysis
                    try:
                        if non_null_values:
                            # Since we already have the data, we don't need to query again
                            date_objects = []
                            for val in non_null_values:
                                if hasattr(val, 'isoformat'):  # Check if it's a date object
                                    date_objects.append(val)
                            
                            if date_objects:
                                min_date = min(date_objects)
                                max_date = max(date_objects)
                                # Calculate date range in days safely
                                try:
                                    if hasattr(max_date, 'toordinal') and hasattr(min_date, 'toordinal'):
                                        date_range_days = max_date.toordinal() - min_date.toordinal()
                                    else:
                                        # Fallback using total_seconds for datetime objects
                                        date_range_days = (max_date - min_date).total_seconds() / (24*60*60)
                                except Exception:
                                    date_range_days = None
                                
                                column_analysis["date_stats"] = {
                                    "min_date": min_date.isoformat() if hasattr(min_date, 'isoformat') else str(min_date),
                                    "max_date": max_date.isoformat() if hasattr(max_date, 'isoformat') else str(max_date),
                                    "date_range_days": date_range_days
                                }
                    except Exception as e:
                        logger.warning(f"Error analyzing date column {column_name}: {e}")
                        # We can try to get date statistics using SQL as fallback
                        try:
                            date_stats_query = f"""
                            SELECT 
                                MIN([{column_name}]) AS min_date,
                                MAX([{column_name}]) AS max_date,
                                DATEDIFF(day, MIN([{column_name}]), MAX([{column_name}] AS FLOAT)) AS date_range_days
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
                        except Exception as e2:
                            logger.error(f"Error in fallback date analysis for {column_name}: {e2}")
                
            except Exception as e:
                logger.error(f"Error analyzing column {column_name}: {e}")
                column_analysis = {
                    "data_type": data_type,
                    "error": f"Failed to analyze column: {str(e)}"
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
        groups_list = list(duplicate_groups.values( AS FLOAT))
        
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
