#!/usr/bin/env python3
"""
Fix all syntax errors in state cleaner files.
This script addresses the common issues introduced by previous modifications.
"""

import os
import re
import glob
from pathlib import Path

def fix_syntax_errors(file_path):
    """Fix syntax errors in a single file."""
    print(f"Fixing {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    # Fix 1: Remove stray "return df" statements that are outside functions
    # Pattern: "return df" followed by empty lines and then a function definition
    content = re.sub(r'(\s+return df\s+)\n\s+return df\s*\n', r'\1\n', content)
    
    # Fix 2: Remove any "return df" that's not properly indented within a function
    # This is a more aggressive fix for the common pattern we're seeing
    lines = content.split('\n')
    fixed_lines = []
    in_function = False
    function_indent = 0
    
    for i, line in enumerate(lines):
        stripped = line.strip()
        
        # Check if we're entering a function
        if stripped.startswith('def '):
            in_function = True
            function_indent = len(line) - len(line.lstrip())
            fixed_lines.append(line)
            continue
        
        # Check if we're exiting a function (next function or class definition)
        if stripped.startswith('class ') or (stripped.startswith('def ') and in_function):
            in_function = False
            function_indent = 0
            fixed_lines.append(line)
            continue
        
        # Check if this line is a "return df" that might be outside a function
        if stripped == 'return df':
            # If we're not in a function or this line has wrong indentation
            if not in_function or (len(line) - len(line.lstrip())) <= function_indent:
                print(f"  Removing stray 'return df' at line {i+1}")
                continue  # Skip this line
        
        fixed_lines.append(line)
    
    content = '\n'.join(fixed_lines)
    
    # Fix 3: Remove any remaining problematic patterns
    # Pattern: multiple return statements in a row
    content = re.sub(r'(\s+return df\s+)\n\s+return df\s*\n', r'\1\n', content)
    
    # Fix 4: Ensure proper spacing around function definitions
    content = re.sub(r'(\s+return df\s+)\n\s+def ', r'\1\n\ndef ', content)
    
    # Only write if content changed
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✅ Fixed {file_path}")
        return True
    else:
        print(f"  ✅ No changes needed for {file_path}")
        return False

def main():
    """Main function to fix all state cleaner files."""
    print("🔧 Fixing all syntax errors in state cleaner files...")
    print("=" * 60)
    
    # Get all state cleaner files
    state_cleaner_dir = Path("src/pipeline/state_cleaners")
    state_cleaner_files = list(state_cleaner_dir.glob("*.py"))
    
    if not state_cleaner_files:
        print("❌ No state cleaner files found!")
        return
    
    print(f"Found {len(state_cleaner_files)} state cleaner files")
    print()
    
    fixed_count = 0
    for file_path in state_cleaner_files:
        if fix_syntax_errors(file_path):
            fixed_count += 1
    
    print()
    print("=" * 60)
    print(f"🎉 Syntax fix complete!")
    print(f"✅ Fixed {fixed_count} files")
    print(f"📁 Total files processed: {len(state_cleaner_files)}")
    
    if fixed_count > 0:
        print("\n🔍 Next steps:")
        print("1. Test the pipeline: python run_pipeline.py")
        print("2. If more errors, run this script again")
    else:
        print("\n🎯 All files are already clean!")

if __name__ == "__main__":
    main()
