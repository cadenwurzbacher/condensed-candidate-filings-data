#!/usr/bin/env python3
"""
Analyze data quality issues by comparing raw data with processed data.
"""

import pandas as pd
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_data_issues():
    """Analyze issues in the final data file."""
    
    # Load the NEW data
    df = pd.read_csv('data/final/candidate_filings_FINAL_20250901_211325.csv')
    
    print("=== NEW PIPELINE OUTPUT ANALYSIS ===")
    print(f"Total columns: {len(df.columns)}")
    print(f"Total records: {len(df)}")
    
    print("\n=== CURRENT COLUMN ORDER ===")
    print("Columns:")
    for i, col in enumerate(df.columns):
        print(f"{i+1:2d}. {col}")
    
    print("\n=== PREFERRED COLUMN ORDER ===")
    preferred_order = [
        'stable_id',
        'state',
        'full_name_display',
        'prefix',
        'first_name',
        'middle_name',
        'last_name',
        'suffix',
        'nickname',
        'office',
        'district',
        'party',
        'street_address',
        'city',
        'address_state',
        'zip_code',
        'phone',
        'email',
        'facebook',
        'twitter',
        'filing_date',
        'election_date',
        'first_added_date',
        'last_updated_date'
    ]
    
    print("Preferred order:")
    for i, col in enumerate(preferred_order):
        status = "✅" if col in df.columns else "❌"
        print(f"{i+1:2d}. {col} {status}")
    
    print("\n=== STATE STANDARDIZATION CHECK ===")
    print("Sample states:")
    print(df['state'].head(10).tolist())
    print("\nUnique states:")
    print(df['state'].unique()[:10])
    
    print("\n=== STREET_ADDRESS COLUMN CHECK ===")
    print("Sample street_address:")
    print(df['street_address'].head(5).tolist())
    
    print("\n=== RAW VS PROCESSED DATA ANALYSIS ===\n")
    
    # Sample analysis
    sample = df.head(10)
    
    for idx, row in sample.iterrows():
        try:
            raw = json.loads(row['raw_data']) if pd.notna(row['raw_data']) else {}
            
            print(f"Record {idx}:")
            print(f"  Raw Office: '{raw.get('Office', 'N/A')}' -> Processed: '{row['office']}'")
            print(f"  Raw Name: '{raw.get('Name', 'N/A')}' -> Processed: '{row['full_name_display']}'")
            print(f"  Raw Address: '{raw.get('Address', 'N/A')}' -> Processed: '{row['street_address']}'")
            print(f"  Raw Party: '{raw.get('Party', 'N/A')}' -> Processed: '{row['party']}'")
            print(f"  Parsed City: '{row['city']}' | Parsed State: '{row['address_state']}' | Parsed ZIP: '{row['zip_code']}'")
            print("  ---")
            
        except Exception as e:
            print(f"Error processing record {idx}: {e}")
    
    print("\n=== NAME PARSING ANALYSIS ===")
    
    # Name parsing analysis
    sample = df.head(10)
    for idx, row in sample.iterrows():
        try:
            raw = json.loads(row['raw_data']) if pd.notna(row['raw_data']) else {}
            raw_name = raw.get('Name', 'N/A')
            
            print(f"Record {idx}: Raw: '{raw_name}'")
            print(f"  -> First: '{row['first_name']}' Middle: '{row['middle_name']}' Last: '{row['last_name']}' Suffix: '{row['suffix']}'")
            
        except Exception as e:
            print(f"Error processing record {idx}: {e}")
    
    print("\n=== COLUMN ISSUES ===")
    
    # Check for duplicate/conflicting columns
    address_columns = [col for col in df.columns if 'address' in col.lower()]
    city_columns = [col for col in df.columns if 'city' in col.lower()]
    state_columns = [col for col in df.columns if 'state' in col.lower()]
    zip_columns = [col for col in df.columns if 'zip' in col.lower()]
    
    print(f"Address columns: {address_columns}")
    print(f"City columns: {city_columns}")
    print(f"State columns: {state_columns}")
    print(f"ZIP columns: {zip_columns}")
    
    print("\n=== DATA QUALITY ISSUES ===")
    
    # Check for specific issues
    issues = []
    
    # 1. Empty party data
    empty_party = df['party'].isna().sum()
    if empty_party > 0:
        issues.append(f"Empty party records: {empty_party} ({empty_party/len(df)*100:.1f}%)")
    
    # 2. Empty county data
    empty_county = df['county'].isna().sum()
    if empty_county > 0:
        issues.append(f"Empty county records: {empty_county} ({empty_county/len(df)*100:.1f}%)")
    
    # 3. Empty district data
    empty_district = df['district'].isna().sum()
    if empty_district > 0:
        issues.append(f"Empty district records: {empty_district} ({empty_district/len(df)*100:.1f}%)")
    
    # 4. Empty parsed address components
    empty_city = df['city'].isna().sum()
    if empty_city > 0:
        issues.append(f"Empty city records: {empty_city} ({empty_city/len(df)*100:.1f}%)")
    
    empty_zip = df['zip_code'].isna().sum()
    if empty_zip > 0:
        issues.append(f"Empty ZIP records: {empty_zip} ({empty_zip/len(df)*100:.1f}%)")
    
    # 5. Case issues
    all_caps_states = df[df['state'].str.isupper()]['state'].nunique() if 'state' in df.columns else 0
    if all_caps_states > 0:
        issues.append(f"All caps states: {all_caps_states} states")
    
    # 6. Duplicate columns
    if len(address_columns) > 1:
        issues.append(f"Multiple address columns: {address_columns}")
    
    if len(city_columns) > 1:
        issues.append(f"Multiple city columns: {city_columns}")
    
    if len(state_columns) > 1:
        issues.append(f"Multiple state columns: {state_columns}")
    
    if len(zip_columns) > 1:
        issues.append(f"Multiple ZIP columns: {zip_columns}")
    
    # Print issues
    for issue in issues:
        print(f"  ❌ {issue}")
    
    print(f"\nTotal issues found: {len(issues)}")
    
    return issues

if __name__ == "__main__":
    analyze_data_issues()
