#!/usr/bin/env python3
"""
Test script to verify Indiana cleaner filing_date fix
"""

import sys
import os
sys.path.append('src/pipeline/state_cleaners')

from indiana_cleaner import clean_indiana_candidates

def test_indiana_fix():
    """Test that Indiana cleaner now properly maps Date Filed to filing_date."""
    
    print("=== TESTING INDIANA CLEANER FILING_DATE FIX ===")
    
    # Load raw data
    raw_file = 'data/raw/indiana_candidates_2024.xlsx'
    print(f"Loading raw data from: {raw_file}")
    
    # Clean the data
    print("Running Indiana cleaner...")
    cleaned_df = clean_indiana_candidates(raw_file, output_dir='data/processed')
    
    print(f"\nCleaned data shape: {cleaned_df.shape}")
    
    # Check filing_date field
    print(f"\nFiling date check:")
    print(f"  Raw 'Date Filed' non-null: {cleaned_df['original_filing_date'].notna().sum()}")
    print(f"  Processed 'filing_date' non-null: {cleaned_df['filing_date'].notna().sum()}")
    
    # Show sample values
    print(f"\nSample raw Date Filed values:")
    print(cleaned_df['original_filing_date'].head())
    
    print(f"\nSample processed filing_date values:")
    print(cleaned_df['filing_date'].head())
    
    # Check if they match
    if cleaned_df['original_filing_date'].equals(cleaned_df['filing_date']):
        print(f"\n✅ SUCCESS: Date Filed properly mapped to filing_date!")
    else:
        print(f"\n❌ FAILURE: Date Filed not properly mapped to filing_date")
    
    return cleaned_df

if __name__ == "__main__":
    test_indiana_fix()
