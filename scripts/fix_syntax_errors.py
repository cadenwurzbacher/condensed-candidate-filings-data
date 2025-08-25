#!/usr/bin/env python3
"""
Fix Syntax Errors in State Cleaners

This script fixes the syntax errors that were introduced when removing
stable ID generation from state cleaners.
"""

import os
import re
from pathlib import Path

def fix_syntax_errors(file_path):
    """Fix syntax errors in a state cleaner file"""
    print(f"Processing: {file_path}")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    original_content = content
    
    # Fix stray return statements outside functions
    # Pattern: return df\n\n    def (this indicates a stray return)
    content = re.sub(r'return df\n\n    def ', '    def ', content)
    
    # Fix any other stray return statements that might be outside functions
    # Look for return statements that are not properly indented
    lines = content.split('\n')
    fixed_lines = []
    in_function = False
    function_indent = 0
    
    for line in lines:
        stripped = line.strip()
        
        # Check if we're entering a function
        if stripped.startswith('def '):
            in_function = True
            function_indent = len(line) - len(line.lstrip())
            fixed_lines.append(line)
        # Check if we're exiting a function (empty line or less indented line)
        elif in_function and (not stripped or (len(line) - len(line.lstrip())) <= function_indent):
            in_function = False
            fixed_lines.append(line)
        # If we see a return statement outside a function, skip it
        elif stripped == 'return df' and not in_function:
            print(f"  Removing stray return statement: {line}")
            continue
        else:
            fixed_lines.append(line)
    
    content = '\n'.join(fixed_lines)
    
    if content != original_content:
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"  ✅ Fixed: {file_path}")
        return True
    else:
        print(f"  ⚠️  No syntax errors found: {file_path}")
        return False

def main():
    """Fix syntax errors in all state cleaners"""
    print("🔧 Fixing Syntax Errors in State Cleaners")
    print("=" * 60)
    
    # Path to state cleaners
    state_cleaners_dir = Path("src/pipeline/state_cleaners")
    
    if not state_cleaners_dir.exists():
        print(f"❌ State cleaners directory not found: {state_cleaners_dir}")
        return
    
    # Find all state cleaner files
    state_cleaner_files = list(state_cleaners_dir.glob("*_cleaner.py"))
    
    print(f"Found {len(state_cleaner_files)} state cleaner files")
    print()
    
    fixed_count = 0
    
    for file_path in state_cleaner_files:
        try:
            if fix_syntax_errors(file_path):
                fixed_count += 1
        except Exception as e:
            print(f"  ❌ Error processing {file_path}: {e}")
    
    print()
    print(f"✅ Fixed syntax errors in {fixed_count} out of {len(state_cleaner_files)} files")
    print()
    print("🎯 Next Steps:")
    print("1. Syntax errors should be fixed")
    print("2. Try running the pipeline again")
    print("3. All state cleaners should work without stable ID generation")

if __name__ == "__main__":
    main()
