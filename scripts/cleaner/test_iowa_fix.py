#!/usr/bin/env python3
"""
Test script to verify Iowa cleaner format fix
"""

import sys
import os
sys.path.append('src/pipeline/state_cleaners')

from iowa_cleaner import clean_iowa_candidates

def test_iowa_fix():
    """Test that Iowa cleaner now properly handles the weird format."""
    
    print("=== TESTING IOWA CLEANER FORMAT FIX ===")
    
    # Load raw data
    raw_file = 'data/raw/iowa_candidates_2024.xlsx'
    print(f"Loading raw data from: {raw_file}")
    
    # Clean the data
    print("Running Iowa cleaner...")
    cleaned_df = clean_iowa_candidates(raw_file, output_dir='data/processed')
    
    print(f"\nCleaned data shape: {cleaned_df.shape}")
    
    # Check the results
    print(f"\nFormat fix results:")
    print(f"  Original raw rows: 403")
    print(f"  After filtering 'No Candidate': {len(cleaned_df)}")
    print(f"  Rows removed: {403 - len(cleaned_df)}")
    
    # Check office field completion
    print(f"\nOffice field completion:")
    print(f"  Non-null office values: {cleaned_df['office'].notna().sum()}")
    print(f"  Null office values: {cleaned_df['office'].isna().sum()}")
    
    # Show sample office values
    print(f"\nSample office values:")
    print(cleaned_df['office'].value_counts().head(10))
    
    # Check for any remaining "-" values
    print(f"\nChecking for remaining '-' values:")
    for col in ['phone', 'email', 'address']:
        if col in cleaned_df.columns:
            dash_count = (cleaned_df[col] == '-').sum()
            print(f"  {col}: {dash_count} '-' values")
    
    return cleaned_df

if __name__ == "__main__":
    test_iowa_fix()
