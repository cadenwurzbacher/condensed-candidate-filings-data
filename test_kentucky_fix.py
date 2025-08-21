#!/usr/bin/env python3
"""
Test script to verify Kentucky cleaner location parsing fix
"""

import sys
import os
sys.path.append('src/pipeline/state_cleaners')

from kentucky_cleaner import clean_kentucky_candidates

def test_kentucky_fix():
    """Test that Kentucky cleaner now properly parses location field."""
    
    print("=== TESTING KENTUCKY CLEANER LOCATION PARSING FIX ===")
    
    # Load raw data
    raw_file = 'data/raw/kentucky_candidates_1983_2024.xlsx'
    print(f"Loading raw data from: {raw_file}")
    
    # Clean the data
    print("Running Kentucky cleaner...")
    cleaned_df = clean_kentucky_candidates(raw_file, output_dir='data/processed')
    
    print(f"\nCleaned data shape: {cleaned_df.shape}")
    
    # Check the results
    print(f"\nLocation parsing results:")
    print(f"  city non-null: {cleaned_df['city'].notna().sum()}")
    print(f"  county non-null: {cleaned_df['county'].notna().sum()}")
    print(f"  district non-null: {cleaned_df['district'].notna().sum()}")
    print(f"  address non-null: {cleaned_df['address'].notna().sum()}")
    print(f"  address_state values: {cleaned_df['address_state'].value_counts()}")
    
    # Show sample parsed results
    print(f"\nSample location parsing:")
    sample_data = cleaned_df[['city', 'county', 'district', 'address', 'address_state']].head(10)
    for idx, row in sample_data.iterrows():
        print(f"  Row {idx}: city='{row['city']}', county='{row['county']}', district='{row['district']}', address='{row['address']}', state='{row['address_state']}'")
    
    return cleaned_df

if __name__ == "__main__":
    test_kentucky_fix()
