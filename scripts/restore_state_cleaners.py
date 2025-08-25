#!/usr/bin/env python3
"""
Restore State Cleaners Structure

This script restores the proper structure of state cleaners after
the stable ID removal caused syntax errors.
"""

import os
import re
from pathlib import Path

def restore_state_cleaner(file_path):
    """Restore proper structure of a state cleaner file"""
    print(f"Processing: {file_path}")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    original_content = content
    
    # Fix common issues that were introduced
    
    # 1. Fix function definitions that are not properly indented
    # Look for def statements that are not at the class level
    lines = content.split('\n')
    fixed_lines = []
    in_class = False
    class_indent = 0
    
    for i, line in enumerate(lines):
        stripped = line.strip()
        
        # Check if we're entering a class
        if stripped.startswith('class ') and stripped.endswith(':'):
            in_class = True
            class_indent = len(line) - len(line.lstrip())
            fixed_lines.append(line)
        # Check if we're exiting the class (end of file or new class)
        elif in_class and (stripped.startswith('class ') or i == len(lines) - 1):
            if stripped.startswith('class '):
                in_class = False
            fixed_lines.append(line)
        # Fix function definitions that should be at class level
        elif in_class and stripped.startswith('def ') and len(line) - len(line.lstrip()) != class_indent + 4:
            # This function should be indented 4 spaces from class level
            fixed_line = ' ' * (class_indent + 4) + stripped
            fixed_lines.append(fixed_line)
        else:
            fixed_lines.append(line)
    
    content = '\n'.join(fixed_lines)
    
    # 2. Remove any remaining stray return statements outside functions
    content = re.sub(r'^\s*return df\s*$', '', content, flags=re.MULTILINE)
    
    # 3. Clean up multiple empty lines
    content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
    
    if content != original_content:
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"  ✅ Restored: {file_path}")
        return True
    else:
        print(f"  ⚠️  No changes needed: {file_path}")
        return False

def main():
    """Restore all state cleaners"""
    print("🔧 Restoring State Cleaners Structure")
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
    
    restored_count = 0
    
    for file_path in state_cleaner_files:
        try:
            if restore_state_cleaner(file_path):
                restored_count += 1
        except Exception as e:
            print(f"  ❌ Error processing {file_path}: {e}")
    
    print()
    print(f"✅ Restored {restored_count} out of {len(state_cleaner_files)} files")
    print()
    print("🎯 Next Steps:")
    print("1. State cleaner structure should be restored")
    print("2. Try running the pipeline again")
    print("3. All state cleaners should work without stable ID generation")

if __name__ == "__main__":
    main()
