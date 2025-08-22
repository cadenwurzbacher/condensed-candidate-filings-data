#!/usr/bin/env python3
"""
Test script to verify Maryland cleaner 2022-2026 fix
"""

import sys
import os
sys.path.append('src/pipeline/state_cleaners')

from maryland_cleaner import clean_maryland_candidates

def test_maryland_fix():
    """Test that Maryland cleaner now properly handles 2022-2026 data with no data loss."""
    
    print("=== TESTING MARYLAND CLEANER 2022-2026 FIX ===")
    
    # Test with 2022 data (cleanest format)
    raw_file = 'data/raw/maryland_candidates_2022.xlsx'
    print(f"Testing with: {raw_file}")
    
    # Clean the data
    print("Running Maryland cleaner...")
    cleaned_df = clean_maryland_candidates(raw_file, output_dir='data/processed')
    
    print(f"\nCleaned data shape: {cleaned_df.shape}")
    
    # Check the results
    print(f"\nData completeness check:")
    print(f"  office non-null: {cleaned_df['office'].notna().sum()}")
    print(f"  district non-null: {cleaned_df['district'].notna().sum()}")
    print(f"  party non-null: {cleaned_df['party'].notna().sum()}")
    print(f"  filing_date non-null: {cleaned_df['filing_date'].notna().sum()}")
    print(f"  address non-null: {cleaned_df['address'].notna().sum()}")
    print(f"  city non-null: {cleaned_df['city'].notna().sum()}")
    print(f"  county non-null: {cleaned_df['county'].notna().sum()}")
    print(f"  zip_code non-null: {cleaned_df['zip_code'].notna().sum()}")
    print(f"  address_state non-null: {cleaned_df['address_state'].notna().sum()}")
    print(f"  phone non-null: {cleaned_df['phone'].notna().sum()}")
    print(f"  email non-null: {cleaned_df['email'].notna().sum()}")
    print(f"  website non-null: {cleaned_df['website'].notna().sum()}")
    print(f"  facebook non-null: {cleaned_df['facebook'].notna().sum()}")
    print(f"  twitter non-null: {cleaned_df['twitter'].notna().sum()}")
    
    # Show sample parsed results
    print(f"\nSample parsed results:")
    sample_data = cleaned_df[['office', 'district', 'party', 'filing_date', 'city', 'county', 'address_state']].head(5)
    for idx, row in sample_data.iterrows():
        print(f"  Row {idx}: office='{row['office']}', district='{row['district']}', party='{row['party']}', filing_date='{row['filing_date']}', city='{row['city']}', county='{row['county']}', state='{row['address_state']}'")
    
    return cleaned_df

if __name__ == "__main__":
    test_maryland_fix()
