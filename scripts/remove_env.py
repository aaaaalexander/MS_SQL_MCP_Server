"""Remove .env file."""
import os
import sys

env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.exists(env_path):
    try:
        os.remove(env_path)
        print(f"Successfully removed {env_path}")
    except Exception as e:
        print(f"Error removing {env_path}: {str(e)}")
else:
    print(f"{env_path} does not exist")
