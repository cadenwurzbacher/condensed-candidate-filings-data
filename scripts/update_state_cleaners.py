#!/usr/bin/env python3
"""
Update State Cleaners

Update all state cleaner files to use 'full_name_display' instead of 'candidate_name'.
This eliminates redundancy and standardizes on the single name field.
"""

import os
import re
from pathlib import Path

def update_file(file_path: str) -> bool:
    """Update a single file to replace candidate_name with full_name_display."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Replace candidate_name with full_name_display in various contexts
        replacements = [
            # Column definitions
            (r"'candidate_name'", "'full_name_display'"),
            (r'"candidate_name"', '"full_name_display"'),
            (r'candidate_name', 'full_name_display'),
            
            # Variable names (but be careful not to replace everything)
            (r'df\[\'candidate_name\'\]', "df['full_name_display']"),
            (r'df\["candidate_name"\]', 'df["full_name_display"]'),
            (r'row\[\'candidate_name\'\]', "row['full_name_display']"),
            (r'row\["candidate_name"\]', 'row["full_name_display"]'),
            
            # Comments and documentation
            (r'# candidate_name', '# full_name_display'),
            (r'"""candidate_name', '"""full_name_display'),
        ]
        
        for old_pattern, new_pattern in replacements:
            content = re.sub(old_pattern, new_pattern, content)
        
        # Only write if content changed
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        
        return False
        
    except Exception as e:
        print(f"  ❌ Error updating {file_path}: {e}")
        return False

def main():
    """Update all state cleaner files."""
    print("🔄 Updating State Cleaners to Use full_name_display...")
    
    # Find all state cleaner files
    state_cleaners_dir = Path("../src/pipeline/state_cleaners")
    
    if not state_cleaners_dir.exists():
        print("❌ State cleaners directory not found")
        return
    
    # Get all Python files in state_cleaners directory
    python_files = list(state_cleaners_dir.glob("*.py"))
    
    if not python_files:
        print("❌ No Python files found in state_cleaners directory")
        return
    
    print(f"📁 Found {len(python_files)} Python files to update")
    
    updated_files = []
    total_files = len(python_files)
    
    for i, file_path in enumerate(python_files, 1):
        print(f"\n[{i}/{total_files}] Processing: {file_path.name}")
        
        if file_path.name == "__init__.py":
            print("  ℹ️  Skipping __init__.py")
            continue
        
        if update_file(str(file_path)):
            updated_files.append(file_path.name)
            print(f"  ✅ Updated: {file_path.name}")
        else:
            print(f"  ℹ️  No changes needed: {file_path.name}")
    
    print(f"\n🎉 Update Complete!")
    print(f"📊 Summary:")
    print(f"  Total files processed: {total_files}")
    print(f"  Files updated: {len(updated_files)}")
    print(f"  Files unchanged: {total_files - len(updated_files)}")
    
    if updated_files:
        print(f"\n✅ Updated files:")
        for file_name in updated_files:
            print(f"  • {file_name}")
    
    print(f"\n📋 What Changed:")
    print(f"  • 'candidate_name' → 'full_name_display' in all state cleaners")
    print(f"  • Eliminates redundancy between candidate_name and full_name_display")
    print(f"  • Standardizes on single name field for consistency")
    print(f"  • Your pipeline will now use full_name_display exclusively")

if __name__ == "__main__":
    main()
