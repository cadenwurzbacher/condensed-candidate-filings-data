#!/usr/bin/env python3
"""
Test script to check Oregon data issues and test fixes.
"""

import sys
import os
sys.path.append('.')

import pandas as pd
from src.pipeline.state_cleaners.oregon_cleaner import clean_oregon_candidates

def test_oregon_fix():
    """Test Oregon cleaner fixes."""
    print("=== TESTING OREGON CLEANER FIX ===")
    
    # Test with Oregon data
    input_file = "data/raw/oregon_candidates_2008_2024.xlsx"
    print(f"Testing with: {input_file}")
    
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found!")
        return
    
    # Check raw data structure
    print("\n--- RAW DATA STRUCTURE ---")
    raw_df = pd.read_excel(input_file)
    print(f"Raw data shape: {raw_df.shape}")
    print(f"Raw columns: {raw_df.columns.tolist()}")
    
    # Check sample data
    print("\n--- SAMPLE RAW DATA ---")
    print(raw_df.head(3).to_string())
    
    # Check key columns
    print("\n--- KEY COLUMNS CHECK ---")
    key_cols = ['Date Filed', 'Mailing Address', 'Office', 'Name', 'Party']
    for col in key_cols:
        if col in raw_df.columns:
            non_null = raw_df[col].notna().sum()
            total = len(raw_df)
            print(f"{col}: {non_null}/{total} ({non_null/total*100:.1f}%)")
        else:
            print(f"{col}: NOT FOUND")
    
    # Check specific issues mentioned in audit
    print("\n--- SPECIFIC ISSUES CHECK ---")
    if 'Date Filed' in raw_df.columns:
        print(f"Date Filed sample values: {raw_df['Date Filed'].dropna().head(5).tolist()}")
    
    if 'Mailing Address' in raw_df.columns:
        print(f"Mailing Address sample values: {raw_df['Mailing Address'].dropna().head(5).tolist()}")
    
    # Run cleaner
    print("\n--- RUNNING OREGON CLEANER ---")
    try:
        cleaned_df = clean_oregon_candidates(input_file, "data/processed")
        print("Cleaner completed successfully!")
        
        print(f"\nCleaned data shape: {cleaned_df.shape}")
        
        # Check key fields
        print("\n--- CLEANED DATA QUALITY ---")
        key_fields = ['office', 'district', 'party', 'filing_date', 'address', 'city', 'county', 'zip_code', 'address_state']
        for field in key_fields:
            if field in cleaned_df.columns:
                non_null = cleaned_df[field].notna().sum()
                total = len(cleaned_df)
                print(f"{field}: {non_null}/{total} ({non_null/total*100:.1f}%)")
            else:
                print(f"{field}: NOT FOUND")
        
        # Check address_state specifically
        if 'address_state' in cleaned_df.columns:
            print(f"\nAddress state values:")
            state_counts = cleaned_df['address_state'].value_counts().head(10)
            for state, count in state_counts.items():
                print(f"  {state}: {count}")
        
        # Sample results
        print(f"\nSample parsed results:")
        sample_cols = ['office', 'district', 'party', 'filing_date', 'city', 'address_state', 'address', 'county']
        available_cols = [col for col in sample_cols if col in cleaned_df.columns]
        if available_cols:
            sample_data = cleaned_df[available_cols].head(3)
            for idx, row in sample_data.iterrows():
                print(f"  Row {idx}: " + ", ".join([f"{col}='{row[col]}'" for col in available_cols]))
        
    except Exception as e:
        print(f"Error running cleaner: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_oregon_fix()
