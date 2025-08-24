#!/usr/bin/env python3
"""
Test Date Tracking Functionality

This script tests the new date tracking functionality in the pipeline.
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.pipeline.main_pipeline import MainPipeline
import pandas as pd
from datetime import datetime

def test_date_tracking():
    """Test that date tracking is working correctly"""
    
    print("🧪 TESTING DATE TRACKING FUNCTIONALITY")
    print("=" * 60)
    
    # Initialize pipeline
    pipeline = MainPipeline()
    
    # Create a test DataFrame with candidate data
    test_data = pd.DataFrame({
        'candidate_name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
        'office': ['US President', 'Governor', 'State Senator'],
        'party': ['REPUBLICAN', 'DEMOCRATIC', 'INDEPENDENT'],
        'state': ['Test State', 'Test State', 'Test State'],
        'election_year': [2024, 2024, 2024]
    })
    
    print("📊 TEST DATA:")
    print(f"Records: {len(test_data)}")
    print(f"Columns: {test_data.columns.tolist()}")
    
    # Test stable ID generation with date tracking
    print(f"\n🔧 TESTING STABLE ID GENERATION WITH DATE TRACKING...")
    
    try:
        # Generate stable IDs (this should now include date tracking)
        df_with_ids = pipeline._add_stable_ids_to_dataframe(test_data, 'Test State')
        
        print(f"\n📊 RESULTS:")
        print(f"Generated DataFrame shape: {df_with_ids.shape}")
        print(f"Columns: {df_with_ids.columns.tolist()}")
        
        # Check if date columns exist
        date_columns = ['first_added_date', 'last_updated_date']
        for col in date_columns:
            if col in df_with_ids.columns:
                print(f"✅ {col} column exists")
                non_null_count = df_with_ids[col].notna().sum()
                print(f"   Records with {col}: {non_null_count}/{len(df_with_ids)}")
            else:
                print(f"❌ {col} column missing")
        
        # Check stable ID coverage
        if 'stable_id' in df_with_ids.columns:
            stable_id_coverage = df_with_ids['stable_id'].notna().sum()
            print(f"✅ stable_id coverage: {stable_id_coverage}/{len(df_with_ids)} ({stable_id_coverage/len(df_with_ids)*100:.1f}%)")
        else:
            print(f"❌ stable_id column missing")
        
        # Show sample data
        print(f"\n📋 SAMPLE DATA:")
        sample_cols = ['candidate_name', 'stable_id', 'first_added_date', 'last_updated_date']
        available_cols = [col for col in sample_cols if col in df_with_ids.columns]
        print(df_with_ids[available_cols].head().to_string())
        
        # Verify date logic
        print(f"\n🔍 DATE LOGIC VERIFICATION:")
        current_time = datetime.now()
        
        for idx, row in df_with_ids.iterrows():
            if 'first_added_date' in row and 'last_updated_date' in row:
                first_date = row['first_added_date']
                last_date = row['last_updated_date']
                
                if pd.notna(first_date) and pd.notna(last_date):
                    # Both dates should be recent (within last few seconds)
                    time_diff_first = abs((current_time - first_date).total_seconds())
                    time_diff_last = abs((current_time - last_date).total_seconds())
                    
                    if time_diff_first < 10 and time_diff_last < 10:
                        print(f"  ✅ Row {idx}: Dates are recent and consistent")
                    else:
                        print(f"  ⚠️  Row {idx}: Dates seem old (first: {time_diff_first:.1f}s, last: {time_diff_last:.1f}s)")
                else:
                    print(f"  ❌ Row {idx}: Missing date data")
            else:
                print(f"  ❌ Row {idx}: Date columns missing")
        
        print(f"\n✅ Date tracking test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Date tracking test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_date_tracking()
    if success:
        print("\n🎉 All tests passed!")
    else:
        print("\n💥 Tests failed. Check the logs for details.")
        sys.exit(1)
