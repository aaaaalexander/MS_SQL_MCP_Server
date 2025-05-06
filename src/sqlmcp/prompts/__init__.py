"""
Prompts module initialization.
"""

# Import prompts
from DB_USER.prompts.sql_helper import analyze_query, suggest_index, generate_query

__all__ = [
    'analyze_query', 
    'suggest_index',
    'generate_query'
]
