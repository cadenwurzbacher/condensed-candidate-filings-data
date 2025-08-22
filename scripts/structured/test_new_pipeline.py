#!/usr/bin/env python3
"""
Test script for the new 5-phase pipeline structure

This script tests the new pipeline architecture:
1. Structural parsing (extract data from messy files)
2. ID generation (create stable IDs from structured data)
3. State cleaning (improve data quality within each state)
4. National standards (cross-state consistency)
5. Output generation
"""

import sys
import os
import logging
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.pipeline.main_pipeline import MainPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_new_pipeline():
    """Test the new 5-phase pipeline"""
    print("🧪 Testing new 5-phase pipeline structure...")
    
    try:
        # Initialize pipeline
        pipeline = MainPipeline(data_dir="data")
        
        # Run the new pipeline
        print("🚀 Running new pipeline...")
        final_data = pipeline.run_pipeline()
        
        if final_data.empty:
            print("❌ Pipeline returned empty dataset")
            return False
        
        print(f"✅ Pipeline completed successfully!")
        print(f"📊 Final dataset: {len(final_data)} records")
        print(f"🏛️ States represented: {final_data['state'].nunique()}")
        print(f"📋 Columns: {list(final_data.columns)}")
        
        # Check for stable IDs
        if 'stable_id' in final_data.columns:
            stable_ids = final_data['stable_id'].dropna()
            print(f"🆔 Stable IDs generated: {len(stable_ids)}")
            print(f"🆔 Unique stable IDs: {stable_ids.nunique()}")
        else:
            print("❌ No stable_id column found")
        
        # Check for first ingestion dates
        if 'first_ingestion_date' in final_data.columns:
            ingestion_dates = final_data['first_ingestion_date'].dropna()
            print(f"📅 First ingestion dates: {len(ingestion_dates)}")
        else:
            print("❌ No first_ingestion_date column found")
        
        # Show sample data
        print("\n📋 Sample data:")
        print(final_data.head())
        
        return True
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_new_pipeline()
    if success:
        print("\n🎉 New pipeline test completed successfully!")
    else:
        print("\n💥 New pipeline test failed!")
        sys.exit(1)
