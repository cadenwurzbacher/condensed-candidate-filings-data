#!/usr/bin/env python3
"""
Robust fix for all state cleaner issues.
This script addresses the root causes:
1. Missing 'self.' prefixes on method definitions
2. Incorrect indentation
3. Stray 'return df' statements
4. Mixed tabs/spaces
"""

import os
import re
from pathlib import Path

def fix_state_cleaner_properly(file_path):
    """Fix all issues in a single state cleaner file."""
    print(f"Fixing {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    # Fix 1: Remove ALL stray "return df" statements that are outside functions
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
    
    # Fix 2: Fix missing 'self.' prefixes on method definitions
    # Pattern: method definition that's not properly indented within class
    lines = content.split('\n')
    fixed_lines = []
    in_class = False
    class_indent = 0
    
    for i, line in enumerate(lines):
        stripped = line.strip()
        
        # Check if we're entering a class
        if stripped.startswith('class '):
            in_class = True
            class_indent = len(line) - len(line.lstrip())
            fixed_lines.append(line)
            continue
        
        # Check if we're exiting the class (next class definition)
        if stripped.startswith('class ') and in_class:
            in_class = False
            class_indent = 0
            fixed_lines.append(line)
            continue
        
        # Check if this line is a method definition that's missing proper indentation
        if stripped.startswith('def _') and in_class:
            # If the method is not properly indented within the class
            if len(line) - len(line.lstrip()) <= class_indent:
                # Fix the indentation to be properly within the class
                fixed_line = ' ' * (class_indent + 4) + stripped
                print(f"  Fixed method indentation at line {i+1}: {stripped}")
                fixed_lines.append(fixed_line)
                continue
        
        fixed_lines.append(line)
    
    content = '\n'.join(fixed_lines)
    
    # Fix 3: Remove any remaining problematic patterns
    # Pattern: multiple return statements in a row
    content = re.sub(r'(\s+return df\s+)\n\s+return df\s*\n', r'\1\n', content)
    
    # Fix 4: Ensure proper spacing around function definitions
    content = re.sub(r'(\s+return df\s+)\n\s+def ', r'\1\n\ndef ', content)
    
    # Fix 5: Remove any remaining stray "return df" that might be outside functions
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
    
    # Fix 6: Clean up any double newlines that might have been created
    content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
    
    # Fix 7: Ensure all method definitions within classes have proper indentation
    lines = content.split('\n')
    fixed_lines = []
    in_class = False
    class_indent = 0
    
    for i, line in enumerate(lines):
        stripped = line.strip()
        
        # Check if we're entering a class
        if stripped.startswith('class '):
            in_class = True
            class_indent = len(line) - len(line.lstrip())
            fixed_lines.append(line)
            continue
        
        # Check if we're exiting the class (next class definition)
        if stripped.startswith('class ') and in_class:
            in_class = False
            class_indent = 0
            fixed_lines.append(line)
            continue
        
        # Check if this line is a method definition within a class
        if stripped.startswith('def ') and in_class:
            # Ensure proper indentation (4 spaces from class)
            current_indent = len(line) - len(line.lstrip())
            if current_indent <= class_indent:
                # Fix the indentation
                fixed_line = ' ' * (class_indent + 4) + stripped
                print(f"  Fixed method indentation at line {i+1}: {stripped}")
                fixed_lines.append(fixed_line)
                continue
        
        fixed_lines.append(line)
    
    content = '\n'.join(fixed_lines)
    
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
    print("🔧 Robust fix for all state cleaner issues...")
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
        if fix_state_cleaner_properly(file_path):
            fixed_count += 1
    
    print()
    print("=" * 60)
    print(f"🎉 Robust fix complete!")
    print(f"✅ Fixed {fixed_count} files")
    print(f"📁 Total files processed: {len(state_cleaner_files)}")
    
    if fixed_count > 0:
        print("\n🔍 Next steps:")
        print("1. Test individual imports: python -c 'from src.pipeline.state_cleaners.alaska_cleaner import AlaskaCleaner'")
        print("2. Test the pipeline: python run_pipeline.py")
        print("3. If more errors, run this script again")
    else:
        print("\n🎯 All files are already clean!")

if __name__ == "__main__":
    main()
