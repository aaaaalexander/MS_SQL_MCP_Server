#!/usr/bin/env python
"""
Sanitize code by searching for and replacing sensitive information like company names,
server paths, or other identifiable data that should not be included in a public release.

This script helps prepare the codebase for public GitHub release by ensuring no sensitive
information is accidentally included.
"""
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Pattern, Set, Tuple

# Terms to find and replace - add specific terms as needed
REPLACEMENTS = {
    # Company and server names
    r"(?i)GATEWAY[-_]?DS": "DB_SERVER",
    r"(?i)GATEWAY[-_]?FS": "FILE_SERVER",
    r"(?i)GATEWAY[-_]?CI": "COMPANY_NAME",
    r"(?i)GATEWAY\s+Communications": "COMPANY_NAME",
    r"(?i)\\\\DB_SERVER\\": "SERVER_PATH",
    
    # Database names
    r"(?i)DATABASE_NAME": "DATABASE_NAME",
    r"(?i)MAIN_TABLE": "MAIN_TABLE",
    r"(?i)TRANSCRIPT_TABLE": "TRANSCRIPT_TABLE",
    r"(?i)TRANSACTION_TABLE": "TRANSACTION_TABLE",
    r"(?i)PERFORMANCE_TEMP_TABLE": "PERFORMANCE_TEMP_TABLE",
    r"(?i)CAMPAIGN_PERFORMANCE_TABLE": "CAMPAIGN_PERFORMANCE_TABLE",
    r"(?i)SMS_QUEUE_TABLE": "SMS_QUEUE_TABLE",
    r"(?i)SMS_TABLE": "SMS_TABLE",
    
    # Emails and domains
    r"(?i)mark@COMPANY_NAME\.com": "user@example.com",
    r"(?i)dev@example\.com": "dev@example.com",  # Already sanitized
    r"(?i)@COMPANY_NAME\.com": "@example.com",
    
    # Connection strings
    r"(?i)xy6syj-2GgixrWVQLApt3qHK\*DfV": "YOUR_PASSWORD_HERE",
    r"(?i)DB_USER": "DB_USER",
}

# Files and directories to exclude
EXCLUDED_PATHS = {
    ".git",
    ".venv",
    "__pycache__",
    "venv",
    "node_modules",
    ".idea",
    ".vs",
}

# File extensions to scan
INCLUDED_EXTENSIONS = {
    ".py", ".md", ".txt", ".json", ".bat", ".cmd", ".js", ".html", ".css", 
    ".yml", ".yaml", ".toml", ".ini", ".cfg", ".config", ".xml", ".sh"
}

def should_process_file(file_path: Path) -> bool:
    """Determine if a file should be processed based on path and extension."""
    # Check if any part of the path is in excluded paths
    for part in file_path.parts:
        if part in EXCLUDED_PATHS:
            return False
    
    # Check file extension
    return file_path.suffix.lower() in INCLUDED_EXTENSIONS

def scan_file(file_path: Path) -> Dict[str, List[int]]:
    """
    Scan a file for sensitive terms and return a dictionary of matches.
    
    Args:
        file_path: Path object to the file to scan
    
    Returns:
        Dictionary mapping regex patterns to line numbers where they match
    """
    matches: Dict[str, List[int]] = {}
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
            
        for i, line in enumerate(lines, 1):
            for pattern in REPLACEMENTS.keys():
                if re.search(pattern, line):
                    if pattern not in matches:
                        matches[pattern] = []
                    matches[pattern].append(i)
    except Exception as e:
        print(f"Error scanning {file_path}: {e}")
    
    return matches

def scan_directory(directory: Path) -> Dict[Path, Dict[str, List[int]]]:
    """
    Recursively scan a directory for sensitive terms.
    
    Args:
        directory: Path object to the directory to scan
    
    Returns:
        Dictionary mapping file paths to dictionaries of matches
    """
    results: Dict[Path, Dict[str, List[int]]] = {}
    
    for root, dirs, files in os.walk(directory):
        # Skip excluded directories
        dirs[:] = [d for d in dirs if d not in EXCLUDED_PATHS]
        
        for file in files:
            file_path = Path(root) / file
            if should_process_file(file_path):
                matches = scan_file(file_path)
                if matches:
                    results[file_path] = matches
    
    return results

def replace_in_file(file_path: Path, dry_run: bool = True) -> Tuple[int, int]:
    """
    Replace sensitive terms in a file with sanitized versions.
    
    Args:
        file_path: Path to the file to process
        dry_run: If True, just report what would change without modifying files
    
    Returns:
        Tuple of (lines_modified, terms_replaced)
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
        
        lines_modified = 0
        terms_replaced = 0
        modified_content = content
        
        # Track which lines were modified
        line_numbers = set()
        
        for pattern, replacement in REPLACEMENTS.items():
            # Count initial matches
            matches = re.findall(pattern, content)
            match_count = len(matches)
            
            if match_count > 0:
                # Find line numbers of matches
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if re.search(pattern, line):
                        line_numbers.add(i + 1)
                
                # Replace matches
                modified_content = re.sub(pattern, replacement, modified_content)
                terms_replaced += match_count
        
        lines_modified = len(line_numbers)
        
        if not dry_run and terms_replaced > 0:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(modified_content)
        
        return lines_modified, terms_replaced
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return 0, 0

def process_directory(directory: Path, dry_run: bool = True) -> Tuple[int, int, int]:
    """
    Process all files in a directory, replacing sensitive terms.
    
    Args:
        directory: Path to the directory to process
        dry_run: If True, just report what would change without modifying files
    
    Returns:
        Tuple of (files_modified, lines_modified, terms_replaced)
    """
    files_modified = 0
    total_lines_modified = 0
    total_terms_replaced = 0
    
    for root, dirs, files in os.walk(directory):
        # Skip excluded directories
        dirs[:] = [d for d in dirs if d not in EXCLUDED_PATHS]
        
        for file in files:
            file_path = Path(root) / file
            if should_process_file(file_path):
                lines_modified, terms_replaced = replace_in_file(file_path, dry_run)
                if terms_replaced > 0:
                    files_modified += 1
                    total_lines_modified += lines_modified
                    total_terms_replaced += terms_replaced
                    
                    status = "Would modify" if dry_run else "Modified"
                    print(f"{status} {file_path}: {lines_modified} lines, {terms_replaced} terms")
    
    return files_modified, total_lines_modified, total_terms_replaced

def main():
    """Main entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Sanitize code by replacing sensitive information")
    parser.add_argument("--scan", action="store_true", help="Scan only, don't replace")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default is dry run)")
    parser.add_argument("--dir", default=None, help="Directory to process (default: project root)")
    args = parser.parse_args()
    
    # Determine project root directory
    if args.dir:
        directory = Path(args.dir)
    else:
        # Assume this script is in the scripts/ directory
        script_dir = Path(__file__).parent
        directory = script_dir.parent
    
    print(f"Processing directory: {directory}")
    print(f"Mode: {'Scan only' if args.scan else 'Dry run' if not args.apply else 'Apply changes'}")
    print()
    
    if args.scan:
        # Scan mode - just report matches
        results = scan_directory(directory)
        
        total_files = len(results)
        total_matches = sum(len(matches) for matches in results.values())
        
        print(f"Found {total_matches} potential sensitive terms in {total_files} files")
        print()
        
        for file_path, matches in sorted(results.items()):
            rel_path = file_path.relative_to(directory)
            print(f"{rel_path}:")
            for pattern, lines in sorted(matches.items()):
                print(f"  - {pattern}: lines {', '.join(map(str, lines))}")
        
        return 0
    else:
        # Replace mode
        dry_run = not args.apply
        files_modified, lines_modified, terms_replaced = process_directory(directory, dry_run)
        
        action = "Would modify" if dry_run else "Modified"
        print()
        print(f"{action} {files_modified} files, {lines_modified} lines, {terms_replaced} terms")
        
        if dry_run and files_modified > 0:
            print()
            print("This was a dry run. Use --apply to make these changes.")
        
        return 0

if __name__ == "__main__":
    sys.exit(main())
