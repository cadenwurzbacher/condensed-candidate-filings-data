#!/usr/bin/env python3
"""
Data Completeness Audit Script
Checks that processed columns have the right number of values compared to raw data
"""

import pandas as pd
import os
from pathlib import Path

def audit_state_data_completeness(state_name, raw_file, processed_file):
    """Audit data completeness for a specific state."""
    print(f"\n{'='*60}")
    print(f"=== {state_name.upper()} DATA COMPLETENESS AUDIT ===")
    print(f"{'='*60}")
    
    try:
        # Load raw data
        if raw_file.endswith('.csv'):
            raw = pd.read_csv(raw_file)
        elif raw_file.endswith('.xls'):
            try:
                raw = pd.read_html(raw_file)[0]
            except:
                raw = pd.read_excel(raw_file, engine='xlrd')
        else:
            raw = pd.read_excel(raw_file)
        
        # Load processed data
        processed = pd.read_excel(processed_file)
        
        print(f"Raw file: {os.path.basename(raw_file)}")
        print(f"Raw records: {len(raw):,}")
        print(f"Raw columns: {len(raw.columns)}")
        
        print(f"\nProcessed file: {os.path.basename(processed_file)}")
        print(f"Processed records: {len(processed):,}")
        print(f"Processed columns: {len(processed.columns)}")
        
        # Check raw data completeness
        print(f"\n📊 RAW DATA COMPLETENESS:")
        for col in raw.columns:
            non_null = raw[col].notna().sum()
            pct = (non_null / len(raw)) * 100 if len(raw) > 0 else 0
            print(f"  {col}: {non_null:,}/{len(raw):,} ({pct:.1f}%)")
        
        # Check key processed columns
        key_columns = ['address', 'city', 'zip_code', 'county', 'address_state', 'phone', 'email', 'website']
        print(f"\n📊 PROCESSED DATA COMPLETENESS (Key Columns):")
        for col in key_columns:
            if col in processed.columns:
                non_null = processed[col].notna().sum()
                pct = (non_null / len(processed)) * 100 if len(processed) > 0 else 0
                print(f"  {col}: {non_null:,}/{len(processed):,} ({pct:.1f}%)")
            else:
                print(f"  {col}: COLUMN NOT FOUND")
        
        # Check for potential data loss
        print(f"\n🔍 DATA LOSS ANALYSIS:")
        
        # Check if address data was preserved
        if 'Address' in raw.columns or 'address' in raw.columns:
            raw_addr_col = 'Address' if 'Address' in raw.columns else 'address'
            raw_addr_count = raw[raw_addr_col].notna().sum()
            proc_addr_count = processed['address'].notna().sum()
            
            if raw_addr_count > 0:
                addr_preserved = (proc_addr_count / raw_addr_count) * 100
                print(f"  Address preservation: {proc_addr_count:,}/{raw_addr_count:,} ({addr_preserved:.1f}%)")
                
                if addr_preserved < 90:
                    print(f"    ⚠️  WARNING: Significant address data loss detected!")
            else:
                print(f"  Address preservation: No address data in raw file")
        
        # Check if phone data was preserved
        if 'Phone' in raw.columns or 'phone' in raw.columns:
            raw_phone_col = 'Phone' if 'Phone' in raw.columns else 'phone'
            raw_phone_count = raw[raw_phone_col].notna().sum()
            proc_phone_count = processed['phone'].notna().sum()
            
            if raw_phone_count > 0:
                phone_preserved = (proc_phone_count / raw_phone_count) * 100
                print(f"  Phone preservation: {proc_phone_count:,}/{raw_phone_count:,} ({phone_preserved:.1f}%)")
                
                if phone_preserved < 90:
                    print(f"    ⚠️  WARNING: Significant phone data loss detected!")
            else:
                print(f"  Phone preservation: No phone data in raw file")
        
        # Check if email data was preserved
        if 'Email' in raw.columns or 'email' in raw.columns:
            raw_email_col = 'Email' if 'Email' in raw.columns else 'email'
            raw_email_count = raw[raw_email_col].notna().sum()
            proc_email_count = processed['email'].notna().sum()
            
            if raw_email_count > 0:
                email_preserved = (proc_email_count / raw_email_count) * 100
                print(f"  Email preservation: {proc_email_count:,}/{raw_email_count:,} ({email_preserved:.1f}%)")
                
                if email_preserved < 90:
                    print(f"    ⚠️  WARNING: Significant email data loss detected!")
            else:
                print(f"  Email preservation: No email data in raw file")
        
        print(f"\n✅ {state_name.upper()} AUDIT COMPLETED")
        
    except Exception as e:
        print(f"❌ ERROR auditing {state_name}: {e}")

def main():
    """Run audits for all states."""
    print("🔍 STARTING DATA COMPLETENESS AUDIT")
    print("This will check that processed columns have the right number of values")
    
    # Define state mappings (raw file, processed file)
    state_mappings = [
        ("Alaska", "data/raw/alaska_candidates_2022_2024.xlsx", "data/processed/alaska_candidates_2022_2024_cleaned_20250820_131540.xlsx"),
        ("Arizona", "data/raw/arizona_candidates_2024.xlsx", "data/processed/arizona_candidates_2024_cleaned_20250820_131540.xlsx"),
        ("Arkansas", "data/raw/arkansas_candidates_2024.xlsx", "data/processed/arkansas_candidates_2024_cleaned_20250820_131540.xlsx"),
        ("Colorado", "data/raw/colorado_candidates_2024.csv", "data/processed/colorado_candidates_2024_cleaned_20250820_131540.xlsx"),
        ("Delaware", "data/raw/delaware_candidates_2008_2024.xlsx", "data/processed/delaware_candidates_2008_2024_cleaned_20250820_131540.xlsx"),
        ("Georgia", "data/raw/georgia_candidates_2024.xlsx", "data/processed/georgia_candidates_2024_cleaned_20250820_131541.xlsx"),
        ("Idaho", "data/raw/idaho_candidates_2024.csv", "data/processed/idaho_candidates_2024_cleaned_20250820_131542.xlsx"),
        ("Illinois", "data/raw/illinois_candidates_1998_2024.xlsx", "data/processed/illinois_candidates_1998_2024_cleaned_20250820_131544.xlsx"),
        ("Indiana", "data/raw/indiana_candidates_2024.xlsx", "data/processed/indiana_candidates_2024_cleaned_20250820_131548.xlsx"),
        ("Iowa", "data/raw/iowa_candidates_2024.xlsx", "data/processed/iowa_candidates_2024_cleaned_20250820_131550.xlsx"),
        ("Kansas", "data/raw/kansas_candidates_2002_2026.xlsx", "data/processed/kansas_merged_temp_cleaned_20250820_131554.xlsx"),
        ("Kentucky", "data/raw/kentucky_candidates_1983_2024.xlsx", "data/processed/kentucky_candidates_1983_2024_cleaned_20250820_131629.xlsx"),
        ("Louisiana", "data/raw/louisiana_candidates_2016_2025.xlsx", "data/processed/louisiana_candidates_2016_2025_cleaned_20250820_131709.xlsx"),
        ("Maryland", "data/raw/maryland_candidates_2016.xlsx", "data/processed/maryland_merged_temp_cleaned_20250820_131721.xlsx"),
        ("Missouri", "data/raw/missouri_candidates_2024.xlsx", "data/processed/missouri_candidates_2024_cleaned_20250820_131722.xlsx"),
        ("Montana", "data/raw/montana_candidates_2020_2024.xlsx", "data/processed/montana_candidates_2020_2024_cleaned_20250820_131722.xlsx"),
        ("Nebraska", "data/raw/nebraska_candidates_2024.xlsx", "data/processed/nebraska_candidates_2024_cleaned_20250820_131722.xlsx"),
        ("New Mexico", "data/raw/new_mexico_candidates_2024.xls", "data/processed/new_mexico_candidates_2024_cleaned_20250820_131722.xlsx"),
        ("New York", "data/raw/new_york_candidates_1982_2024.csv", "data/processed/new_york_candidates_1982_2024_cleaned_20250820_132117.xlsx"),
        ("North Carolina", "data/raw/north_carolina_candidates_2024.csv", "data/processed/north_carolina_merged_temp_cleaned_20250820_132154.xlsx"),
        ("Oklahoma", "data/raw/oklahoma_candidates_2024.xlsx", "data/processed/oklahoma_candidates_2024_cleaned_20250820_132206.xlsx"),
        ("Oregon", "data/raw/oregon_candidates_2008_2024.xlsx", "data/processed/oregon_candidates_2008_2024_cleaned_20250820_132206.xlsx"),
        ("Pennsylvania", "data/raw/pennsylvania_candidates_2024.xlsx", "data/processed/pennsylvania_merged_temp_cleaned_20250820_132209.xlsx"),
        ("South Carolina", "data/raw/south_carolina_candidates_2018_2024.xlsx", "data/processed/south_carolina_candidates_2018_2024_cleaned_20250820_132212.xlsx"),
        ("South Dakota", "data/raw/south_dakota_candidates_2024.xls", "data/processed/south_dakota_candidates_2024_cleaned_20250820_132214.xlsx"),
        ("Vermont", "data/raw/vermont_candidates_2024.xlsx", "data/processed/vermont_candidates_2024_cleaned_20250820_132214.xlsx"),
        ("Virginia", "data/raw/virginia_candidates_2024.xlsx", "data/processed/virginia_merged_temp_cleaned_20250820_132223.xlsx"),
        ("Washington", "data/raw/washington_candidates_all_2008_2025.xlsx", "data/processed/washington_candidates_all_2008_2025_cleaned_20250820_132226.xlsx"),
        ("Wyoming", "data/raw/wyoming_candidates_2024.xlsx", "data/processed/wyoming_candidates_2024_cleaned_20250820_132228.xlsx"),
    ]
    
    for state_name, raw_file, processed_file in state_mappings:
        if os.path.exists(raw_file) and os.path.exists(processed_file):
            audit_state_data_completeness(state_name, raw_file, processed_file)
        else:
            print(f"\n❌ SKIPPING {state_name.upper()}: Files not found")
            if not os.path.exists(raw_file):
                print(f"  Raw file missing: {raw_file}")
            if not os.path.exists(processed_file):
                print(f"  Processed file missing: {processed_file}")
    
    print(f"\n🎉 DATA COMPLETENESS AUDIT COMPLETED")
    print("Check the results above for any data loss issues")

if __name__ == "__main__":
    main()
