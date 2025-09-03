#!/usr/bin/env python3
"""
Test script to verify structured directory clearing functionality
"""

import sys
import os
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.append(str(src_path))

from pipeline.main_pipeline import MainPipeline

def test_structured_cleanup():
    """Test the structured directory clearing functionality"""
    print("🧪 Testing structured directory clearing...")
    
    # Create a test pipeline instance
    pipeline = MainPipeline()
    
    # Create some test files in structured directory
    structured_dir = Path(pipeline.structured_dir)
    structured_dir.mkdir(parents=True, exist_ok=True)
    
    # Create some dummy files
    test_files = [
        "alaska_structured_20240101_120000.xlsx",
        "arizona_structured_20240101_120000.xlsx",
        "colorado_structured_20240101_120000.xlsx"
    ]
    
    print(f"📁 Creating test files in {structured_dir}...")
    for filename in test_files:
        test_file = structured_dir / filename
        test_file.write_text("test content")
        print(f"   Created: {filename}")
    
    # List files before clearing
    files_before = list(structured_dir.glob("*"))
    print(f"\n📋 Files before clearing: {len(files_before)}")
    for file in files_before:
        print(f"   - {file.name}")
    
    # Test the clearing function
    print(f"\n🧹 Testing _clear_structured_directory()...")
    pipeline._clear_structured_directory()
    
    # List files after clearing
    files_after = list(structured_dir.glob("*"))
    print(f"\n📋 Files after clearing: {len(files_after)}")
    for file in files_after:
        print(f"   - {file.name}")
    
    if len(files_after) == 0:
        print("\n✅ SUCCESS: Structured directory was cleared completely!")
    else:
        print(f"\n❌ FAILED: {len(files_after)} files remain in structured directory")
    
    print("\n🧪 Test completed!")

if __name__ == "__main__":
    test_structured_cleanup()
