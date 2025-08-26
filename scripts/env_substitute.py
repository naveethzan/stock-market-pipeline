#!/usr/bin/env python3
"""
Environment variable substitution script.
Replaces ${env:VARIABLE_NAME} patterns in JSON files with actual environment variable values.
This replaces the need for envsubst which may not be available on all systems.
"""

import os
import sys
import re
import json
from pathlib import Path


def substitute_env_vars(content: str) -> str:
    """Replace ${env:VARIABLE_NAME} patterns with environment variable values."""
    
    def replace_var(match):
        var_name = match.group(1)
        value = os.environ.get(var_name, f"${{{var_name}}}")  # Keep original if not found
        return value
    
    # Pattern to match ${env:VARIABLE_NAME}
    pattern = r'\$\{env:([^}]+)\}'
    result = re.sub(pattern, replace_var, content)
    
    return result


def main():
    if len(sys.argv) != 3:
        print("Usage: python env_substitute.py <input_file> <output_file>")
        sys.exit(1)
    
    input_file = Path(sys.argv[1])
    output_file = Path(sys.argv[2])
    
    if not input_file.exists():
        print(f"Error: Input file {input_file} does not exist")
        sys.exit(1)
    
    try:
        # Read input file
        with open(input_file, 'r') as f:
            content = f.read()
        
        # Substitute environment variables
        substituted_content = substitute_env_vars(content)
        
        # Validate JSON if it's a JSON file
        if input_file.suffix.lower() == '.json':
            try:
                json.loads(substituted_content)
            except json.JSONDecodeError as e:
                print(f"Error: Result is not valid JSON: {e}")
                sys.exit(1)
        
        # Write output file
        with open(output_file, 'w') as f:
            f.write(substituted_content)
        
        print(f"Environment substitution complete: {input_file} -> {output_file}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()