"""
Resources module initialization.
"""

# Import specific resources
from DB_USER.resources.table_resources import get_schema, get_table_details

__all__ = [
    'get_schema',
    'get_table_details'
]
