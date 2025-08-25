#!/usr/bin/env python3
"""
Targeted fix for Arizona cleaner specific issues.
"""

def fix_arizona_cleaner():
    """Fix specific issues in Arizona cleaner."""
    file_path = "src/pipeline/state_cleaners/arizona_cleaner.py"
    
    print(f"🔧 Fixing {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Fix 1: Add missing self. prefixes
    content = content.replace("process_office_district", "self.process_office_district")
    content = content.replace("clean_name", "self.clean_name")
    
    # Fix 2: Fix indentation for methods that are not properly indented
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
        
        # Check if we're exiting the class
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
        
        fixed_lines.append(line)
    
    content = '\n'.join(fixed_lines)
    
    # Write the fixed content
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"  ✅ Fixed {file_path}")

if __name__ == "__main__":
    fix_arizona_cleaner()
