# Add this at the end of your sql_mcp_server.py file, where other tools are imported

try:
    # Import novice-friendly tools
    import src.DB_USER.tools.novice_enhanced
    
    # Initialize dependencies
    src.DB_USER.tools.novice_enhanced.mcp = mcp
    src.DB_USER.tools.novice_enhanced.get_db_connection = None  # Async version not needed
    src.DB_USER.tools.novice_enhanced._get_db_connection_blocking = _get_db_connection_blocking
    src.DB_USER.tools.novice_enhanced._execute_query_blocking = _execute_query_blocking
    src.DB_USER.tools.novice_enhanced.is_safe_query = is_safe_query
    
    # Register tools
    src.DB_USER.tools.novice_enhanced.register_tools(
        mcp, 
        None,  # Async db_connection_function
        _get_db_connection_blocking, 
        _execute_query_blocking,
        is_safe_query
    )
    
    logger.info("Successfully imported and registered novice-enhanced tools")
except Exception as e:
    logger.error(f"Failed to import novice-enhanced tools: {str(e)}")
    logger.error(traceback.format_exc())
