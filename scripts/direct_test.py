"""
Direct test script for DB_USER server.
This script sets the environment variables directly in Python.
"""
import os
import sys
import subprocess

# Set environment variables
os.environ["DB_USER_DB_USERNAME"] = "DB_USER"
os.environ["DB_USER_DB_PASSWORD"] = "YOUR_PASSWORD_HERE"
os.environ["DB_USER_ALLOWED_SCHEMAS"] = '["dbo"]'
os.environ["DB_USER_DEBUG"] = "true"
os.environ["DB_USER_LOG_LEVEL"] = "DEBUG"

# Print set environment variables
print("Environment variables set:")
DB_USER_vars = {k: v if 'PASSWORD' not in k else '[REDACTED]' 
               for k, v in os.environ.items() 
               if k.startswith('DB_USER_')}
print(DB_USER_vars)

# Get the path to the main.py file
main_script = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "main.py")

# Run the main script
print(f"Running {main_script}...")
subprocess.run([sys.executable, main_script], check=True)
