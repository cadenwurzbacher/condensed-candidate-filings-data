#!/usr/bin/env python3
"""
Final comprehensive fix for all state cleaner issues.
This script will restore all state cleaners to a working state.
"""

import os
import re
from pathlib import Path

def fix_state_cleaner_final(file_path):
    """Fix all issues in a single state cleaner file with a comprehensive approach."""
    print(f"🔧 Final fix for {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    # Step 1: Remove stable ID generation methods completely
    pattern = r'def _generate_stable_ids\(self, df: pd\.DataFrame\) -> pd\.DataFrame:.*?return df'
    content = re.sub(pattern, '', content, flags=re.DOTALL)
    
    # Step 2: Remove any calls to _generate_stable_ids
    content = re.sub(r'\s*cleaned_df = self\._generate_stable_ids\(cleaned_df\)\s*\n?', '', content)
    content = re.sub(r'\s*df = self\._generate_stable_ids\(df\)\s*\n?', '', content)
    content = re.sub(r'\s*[a-zA-Z_][a-zA-Z0-9_]* = self\._generate_stable_ids\([a-zA-Z_][a-zA-Z0-9_]*\)\s*\n?', '', content)
    
    # Step 3: Fix function calls that are missing self. prefixes
    # Pattern: df['column'].apply(function_name) -> df['column'].apply(self.function_name)
    content = re.sub(r'df\[[\'"]\w+[\'"]\]\.apply\((\w+)\)', r'df[\1].apply(self.\1)', content)
    
    # Step 4: Fix the core structure - ensure proper class and method indentation
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
        
        # Check if we're exiting the class (next class definition or end of file)
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
                print(f"  Fixed method indentation at line {i+1}: {stripped[:50]}...")
                fixed_lines.append(fixed_line)
                continue
        
        # Check if this line is a standalone function that should be a method
        if stripped.startswith('def ') and not in_class:
            # This function should probably be a method of the class
            # Let's check if there's a class above it
            if i > 0 and 'class ' in lines[i-1]:
                # This is likely a method that got misaligned
                in_class = True
                class_indent = len(lines[i-1]) - len(lines[i-1].lstrip())
                fixed_line = ' ' * (class_indent + 4) + stripped
                print(f"  Fixed standalone function to method at line {i+1}: {stripped[:50]}...")
                fixed_lines.append(fixed_line)
                continue
        
        # Check for stray 'return df' statements outside functions
        if stripped == 'return df':
            # Check if we're in a function context
            in_function = False
            function_indent = 0
            
            # Look backwards to see if we're in a function
            for j in range(i-1, -1, -1):
                prev_line = lines[j]
                prev_stripped = prev_line.strip()
                
                if prev_stripped.startswith('def '):
                    function_indent = len(prev_line) - len(prev_line.lstrip())
                    in_function = True
                    break
                elif prev_stripped.startswith('class '):
                    break
            
            # If we're not in a function or this line has wrong indentation, remove it
            if not in_function or (len(line) - len(line.lstrip())) <= function_indent:
                print(f"  Removed stray 'return df' at line {i+1}")
                continue
        
        # Add the line as-is
        fixed_lines.append(line)
    
    content = '\n'.join(fixed_lines)
    
    # Step 5: Clean up any double newlines that might have been created
    content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
    
    # Step 6: Ensure consistent indentation (4 spaces)
    lines = content.split('\n')
    fixed_lines = []
    
    for line in lines:
        # Convert tabs to spaces
        line = line.expandtabs(4)
        
        # Ensure consistent indentation
        if line.strip():  # Skip empty lines
            # Count leading spaces
            leading_spaces = len(line) - len(line.lstrip())
            # Round to nearest 4-space increment
            rounded_spaces = (leading_spaces // 4) * 4
            # Reconstruct line with proper indentation
            fixed_line = ' ' * rounded_spaces + line.lstrip()
            fixed_lines.append(fixed_line)
        else:
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
    """Main function to fix all state cleaner files comprehensively."""
    print("🔧 Final comprehensive fix for all state cleaner issues...")
    print("=" * 70)
    
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
        if fix_state_cleaner_final(file_path):
            fixed_count += 1
    
    print()
    print("=" * 70)
    print(f"🎉 Final comprehensive fix complete!")
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
