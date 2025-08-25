#!/usr/bin/env python3
"""
Test script to check which state cleaners can be imported successfully.
"""

import sys
import os
from pathlib import Path

# Add src to path
current_dir = Path(__file__).parent.parent
sys.path.append(str(current_dir / "src"))

def test_state_cleaner_imports():
    """Test importing each state cleaner and report results."""
    print("🧪 Testing State Cleaner Imports")
    print("=" * 60)
    
    # Get all state cleaner files
    state_cleaner_dir = Path("src/pipeline/state_cleaners")
    state_cleaner_files = list(state_cleaner_dir.glob("*.py"))
    
    working_cleaners = []
    broken_cleaners = []
    
    for file_path in state_cleaner_files:
        if file_path.name == "__init__.py":
            continue
            
        # Extract the cleaner name from the filename
        cleaner_name = file_path.stem.replace("_cleaner", "")
        class_name = "".join(word.capitalize() for word in cleaner_name.split("_")) + "Cleaner"
        
        try:
            # Try to import the cleaner
            module_path = f"pipeline.state_cleaners.{file_path.stem}"
            module = __import__(module_path, fromlist=[class_name])
            
            # Try to get the class
            cleaner_class = getattr(module, class_name)
            print(f"✅ {cleaner_name}: {class_name} imported successfully")
            working_cleaners.append(cleaner_name)
            
        except Exception as e:
            print(f"❌ {cleaner_name}: {class_name} failed - {type(e).__name__}: {str(e)[:100]}...")
            broken_cleaners.append(cleaner_name)
    
    print("\n" + "=" * 60)
    print(f"📊 Results Summary:")
    print(f"✅ Working: {len(working_cleaners)}/{len(working_cleaners) + len(broken_cleaners)}")
    print(f"❌ Broken: {len(broken_cleaners)}/{len(working_cleaners) + len(broken_cleaners)}")
    
    if working_cleaners:
        print(f"\n✅ Working Cleaners:")
        for cleaner in working_cleaners:
            print(f"  - {cleaner}")
    
    if broken_cleaners:
        print(f"\n❌ Broken Cleaners:")
        for cleaner in broken_cleaners:
            print(f"  - {cleaner}")
    
    return working_cleaners, broken_cleaners

if __name__ == "__main__":
    working, broken = test_state_cleaner_imports()
    
    if not broken:
        print("\n🎉 All state cleaners are working!")
        print("Ready to run the full pipeline!")
    else:
        print(f"\n⚠️  {len(broken)} state cleaners still have issues.")
        print("Consider running the fix scripts again or focusing on specific files.")
