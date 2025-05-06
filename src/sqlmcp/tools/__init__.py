"""
Tools module initialization.
"""
import logging

logger = logging.getLogger("DB_USER_Tools")

# Define all exported functions
__all__ = [
    # Core schema tools
    'get_table_schema', 
    'list_tables', 
    'find_foreign_keys',
    
    # Core query tools
    'execute_select',
    'get_sample_data',
    'explain_query',
    
    # Analysis tools
    'analyze_table_data',
    'find_duplicate_records',
    
    # Metadata tools
    'get_database_info',
    'list_stored_procedures',
    'get_procedure_definition',
    
    # Extended schema tools
    'list_schemas',
    'get_table_sample',
    'search_schema_objects',
    'find_related_tables',
    'get_query_examples',
    
    # Enhanced schema adapter tools
    'enhanced_list_schemas',
    'enhanced_get_sample_data',
    'enhanced_search_schema_objects',
    'enhanced_find_related_tables',
    'enhanced_get_query_examples',
    'schema_adapter',
    
    # Novice enhanced tools
    'novice_enhanced',
    'digest'
]

# Note: Actual imports will be handled by the server module when registering tools
# This allows proper dependency injection and avoids circular imports
