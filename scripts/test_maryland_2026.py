#!/usr/bin/env python3
"""
Test script to verify Maryland cleaner with 2026 data (has address info)
"""

import sys
import os
sys.path.append('src/pipeline/state_cleaners')

from maryland_cleaner import clean_maryland_candidates

def test_maryland_2026():
    """Test Maryland cleaner with 2026 data to verify address parsing."""
    
    print("=== TESTING MARYLAND CLEANER WITH 2026 DATA ===")
    
    # Test with 2026 data (has address info)
    raw_file = 'data/raw/maryland_candidates_2026.xlsx'
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
    sample_data = cleaned_df[['office', 'district', 'party', 'filing_date', 'city', 'county', 'address_state', 'address']].head(5)
    for idx, row in sample_data.iterrows():
        print(f"  Row {idx}: office='{row['office']}', district='{row['district']}', party='{row['party']}', filing_date='{row['filing_date']}', city='{row['city']}', county='{row['county']}', state='{row['address_state']}', address='{row['address']}'")
    
    return cleaned_df

if __name__ == "__main__":
    test_maryland_2026()
