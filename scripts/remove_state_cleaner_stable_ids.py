#!/usr/bin/env python3
"""
Remove Stable ID Generation from State Cleaners

This script removes all stable_id generation code from state cleaners
to fix the inconsistency issue. Stable IDs should only be generated
by the Main Pipeline in Phase 2.
"""

import os
import re
from pathlib import Path

def remove_stable_id_generation(file_path):
    """Remove stable ID generation from a state cleaner file"""
    print(f"Processing: {file_path}")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    original_content = content
    
    # Remove the entire _generate_stable_ids method
    # Pattern: def _generate_stable_ids(self, df: pd.DataFrame) -> pd.DataFrame: ... return df
    pattern = r'def _generate_stable_ids\(self, df: pd\.DataFrame\) -> pd\.DataFrame:.*?return df'
    content = re.sub(pattern, 'return df', content, flags=re.DOTALL)
    
    # Remove any remaining stable_id assignments
    content = re.sub(r'df\[\'stable_id\'\]\s*=\s*.*?\n', '', content)
    
    # Remove any generate_stable_id function calls
    content = re.sub(r'df\.apply\(generate_stable_id, axis=1\)', '', content)
    
    # Clean up any empty lines that might be left
    content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
    
    if content != original_content:
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"  ✅ Updated: {file_path}")
        return True
    else:
        print(f"  ⚠️  No changes needed: {file_path}")
        return False

def main():
    """Remove stable ID generation from all state cleaners"""
    print("🔧 Removing Stable ID Generation from State Cleaners")
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
    
    updated_count = 0
    
    for file_path in state_cleaner_files:
        try:
            if remove_stable_id_generation(file_path):
                updated_count += 1
        except Exception as e:
            print(f"  ❌ Error processing {file_path}: {e}")
    
    print()
    print(f"✅ Updated {updated_count} out of {len(state_cleaner_files)} files")
    print()
    print("🎯 Next Steps:")
    print("1. State cleaners will no longer generate stable IDs")
    print("2. Main Pipeline will be the ONLY source of stable IDs")
    print("3. Stable IDs will be consistent throughout the pipeline")
    print("4. Test the pipeline to ensure stable IDs work correctly")

if __name__ == "__main__":
    main()
