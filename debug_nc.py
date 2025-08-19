#!/usr/bin/env python3
"""Debug North Carolina cleaner error."""

import pandas as pd
import sys
sys.path.append('src')

def debug_nc_cleaner():
    print("🔍 DEBUGGING NORTH CAROLINA CLEANER ERROR")
    print("=" * 50)
    
    try:
        # Test with a single file first
        print("1. Testing with single North Carolina file...")
        test_file = 'data/raw/north_carolina_candidates_2024.csv'
        
        if not pd.io.common.file_exists(test_file):
            print(f"❌ Test file not found: {test_file}")
            return
            
        df = pd.read_csv(test_file)
        print(f"✅ Loaded {len(df)} records from test file")
        print(f"Columns: {list(df.columns)}")
        
        # Check if we have the expected columns
        required_cols = ['name_on_ballot', 'contest_name']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"⚠️  Missing required columns: {missing_cols}")
            print(f"Available columns: {list(df.columns)}")
            return
        
        print(f"\n2. Testing name parsing...")
        print(f"Sample names: {df['name_on_ballot'].head(5).tolist()}")
        
        # Try to import and run the cleaner
        print(f"\n3. Testing North Carolina cleaner...")
        from src.pipeline.state_cleaners.north_carolina_cleaner import clean_north_carolina_candidates
        
        cleaned_df = clean_north_carolina_candidates(test_file, 'data/processed')
        print(f"✅ Successfully cleaned {len(cleaned_df)} records")
        
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_nc_cleaner()
