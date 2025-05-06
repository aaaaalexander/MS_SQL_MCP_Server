#!/usr/bin/env python
"""
Generate a test .env file with safe default values and a random password.
This script helps developers set up a testing environment without exposing
sensitive credentials.
"""
import os
import random
import string
import sys
import shutil
import argparse
from pathlib import Path

def generate_password(length=16):
    """Generate a secure random password."""
    characters = string.ascii_letters + string.digits + "!@#$%^&*()-_=+[]{}|;:,.<>?"
    # Ensure at least one of each character type
    password = [
        random.choice(string.ascii_lowercase),
        random.choice(string.ascii_uppercase),
        random.choice(string.digits),
        random.choice("!@#$%^&*()-_=+[]{}|;:,.<>?")
    ]
    # Fill the rest with random characters
    password.extend(random.choice(characters) for _ in range(length - 4))
    # Shuffle the password
    random.shuffle(password)
    return ''.join(password)

def main():
    """Create a test .env file with safe values."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate test environment file')
    parser.add_argument('--force', action='store_true', help='Force overwrite existing .env file')
    args = parser.parse_args()
    
    project_root = Path(__file__).parent.parent.absolute()
    env_example_path = project_root / ".env.example"
    env_path = project_root / ".env"
    
    # Check if .env.example exists
    if not env_example_path.exists():
        print(f"Error: .env.example not found at {env_example_path}")
        return 1
    
    # Check if .env already exists
    if env_path.exists() and not args.force:
        overwrite = input(".env file already exists. Overwrite? (y/n): ").lower()
        if overwrite != 'y':
            print("Operation cancelled.")
            return 0
    
    # Start with the example file
    shutil.copy(env_example_path, env_path)
    
    # Read the .env file
    with open(env_path, 'r') as f:
        env_content = f.read()
    
    # Replace placeholder values with test values
    test_values = {
        "your_server_name": "localhost",
        "your_database_name": "test_db",
        "your_username": "test_user",
        "your_password": generate_password()
    }
    
    for placeholder, value in test_values.items():
        env_content = env_content.replace(placeholder, value)
    
    # Write the updated content back to the .env file
    with open(env_path, 'w') as f:
        f.write(env_content)
    
    print(f"Test .env file created at {env_path}")
    print("Generated values:")
    for key, value in test_values.items():
        if key == "your_password":
            masked_value = value[:2] + "*" * (len(value) - 4) + value[-2:]
            print(f"- Password: {masked_value}")
        else:
            print(f"- {key.replace('your_', '').replace('_', ' ').title()}: {value}")
    
    print("\nNote: These are test values only. Use appropriate credentials for production.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
