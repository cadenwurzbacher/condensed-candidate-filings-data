#!/usr/bin/env python3
"""
Test script to verify Kansas cleaner address_state and phone fixes
"""

import sys
import os
sys.path.append('src/pipeline/state_cleaners')

from kansas_cleaner import clean_kansas_candidates

def test_kansas_fix():
    """Test that Kansas cleaner now properly sets address_state to 'KS' and handles phone correctly."""
    
    print("=== TESTING KANSAS CLEANER FIXES ===")
    
    # Load raw data
    raw_file = 'data/raw/kansas_candidates_2002_2026.xlsx'
    print(f"Loading raw data from: {raw_file}")
    
    # Clean the data
    print("Running Kansas cleaner...")
    cleaned_df = clean_kansas_candidates(raw_file, output_dir='data/processed')
    
    print(f"\nCleaned data shape: {cleaned_df.shape}")
    
    # Check the results
    print(f"\nFix results:")
    print(f"  address_state values: {cleaned_df['address_state'].value_counts()}")
    print(f"  address_state null count: {cleaned_df['address_state'].isna().sum()}")
    
    # Check phone field
    print(f"\nPhone field check:")
    print(f"  phone non-null count: {cleaned_df['phone'].notna().sum()}")
    print(f"  phone null count: {cleaned_df['phone'].isna().sum()}")
    print(f"  phone completion: {(cleaned_df['phone'].notna().sum()/len(cleaned_df))*100:.1f}%")
    
    # Check county field
    print(f"\nCounty field check:")
    print(f"  county null count: {cleaned_df['county'].isna().sum()}")
    print(f"  county non-null count: {cleaned_df['county'].notna().sum()}")
    
    # Show sample phone values
    print(f"\nSample phone values:")
    print(cleaned_df['phone'].dropna().head(10))
    
    return cleaned_df

if __name__ == "__main__":
    test_kansas_fix()
