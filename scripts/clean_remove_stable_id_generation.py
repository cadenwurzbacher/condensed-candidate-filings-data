#!/usr/bin/env python3
"""
Cleanly remove stable ID generation methods from state cleaner files.
This script only removes the specific methods that generate stable IDs,
without touching any other code structure.
"""

import os
import re
from pathlib import Path

def remove_stable_id_generation(file_path):
    """Remove stable ID generation methods from a single file."""
    print(f"Processing {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    # Remove the _generate_stable_ids method completely
    # Pattern: def _generate_stable_ids(self, df: pd.DataFrame) -> pd.DataFrame: ... return df
    pattern = r'def _generate_stable_ids\(self, df: pd\.DataFrame\) -> pd\.DataFrame:.*?return df'
    content = re.sub(pattern, '', content, flags=re.DOTALL)
    
    # Remove any remaining calls to _generate_stable_ids
    content = re.sub(r'\s*cleaned_df = self\._generate_stable_ids\(cleaned_df\)\s*\n?', '', content)
    content = re.sub(r'\s*df = self\._generate_stable_ids\(df\)\s*\n?', '', content)
    
    # Remove any remaining calls to _generate_stable_ids with different variable names
    content = re.sub(r'\s*[a-zA-Z_][a-zA-Z0-9_]* = self\._generate_stable_ids\([a-zA-Z_][a-zA-Z0-9_]*\)\s*\n?', '', content)
    
    # Clean up any double newlines that might have been created
    content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
    
    # Only write if content changed
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✅ Removed stable ID generation from {file_path}")
        return True
    else:
        print(f"  ✅ No stable ID generation found in {file_path}")
        return False

def main():
    """Main function to remove stable ID generation from all state cleaner files."""
    print("🧹 Removing stable ID generation from state cleaner files...")
    print("=" * 60)
    
    # Get all state cleaner files
    state_cleaner_dir = Path("src/pipeline/state_cleaners")
    state_cleaner_files = list(state_cleaner_dir.glob("*.py"))
    
    if not state_cleaner_files:
        print("❌ No state cleaner files found!")
        return
    
    print(f"Found {len(state_cleaner_files)} state cleaner files")
    print()
    
    modified_count = 0
    for file_path in state_cleaner_files:
        if remove_stable_id_generation(file_path):
            modified_count += 1
    
    print()
    print("=" * 60)
    print(f"🎉 Stable ID generation removal complete!")
    print(f"✅ Modified {modified_count} files")
    print(f"📁 Total files processed: {len(state_cleaner_files)}")
    
    if modified_count > 0:
        print("\n🔍 Next steps:")
        print("1. Test the pipeline: python run_pipeline.py")
        print("2. If successful, the pipeline should now work correctly")
    else:
        print("\n🎯 No files needed modification!")

if __name__ == "__main__":
    main()
