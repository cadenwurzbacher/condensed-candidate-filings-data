#!/usr/bin/env python3
"""
Test script to verify Montana cleaner fix
"""

import sys
import os
sys.path.append('src/pipeline/state_cleaners')

from montana_cleaner import clean_montana_candidates

def test_montana_fix():
    """Test that Montana cleaner now properly maps all available data with no loss."""
    
    print("=== TESTING MONTANA CLEANER FIX ===")
    
    # Load raw data
    raw_file = 'data/raw/montana_candidates_2020_2024.xlsx'
    print(f"Testing with: {raw_file}")
    
    # Clean the data
    print("Running Montana cleaner...")
    cleaned_df = clean_montana_candidates(raw_file, output_dir='data/processed')
    
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
    
    # Show sample parsed results
    print(f"\nSample parsed results:")
    sample_data = cleaned_df[['office', 'district', 'party', 'filing_date', 'city', 'address_state', 'address']].head(5)
    for idx, row in sample_data.iterrows():
        print(f"  Row {idx}: office='{row['office']}', district='{row['district']}', party='{row['party']}', filing_date='{row['filing_date']}', city='{row['city']}', state='{row['address_state']}', address='{row['address']}'")
    
    return cleaned_df

if __name__ == "__main__":
    test_montana_fix()
