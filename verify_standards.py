#!/usr/bin/env python3
"""
Comprehensive verification script to test all new standards.
"""

import pandas as pd
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_all_standards():
    """Verify all new standards are working correctly."""
    
    # Load the latest data
    df = pd.read_csv('data/final/candidate_filings_FINAL_20250903_134419.csv')
    
    print("=== COMPREHENSIVE STANDARDS VERIFICATION ===\n")
    
    # Test 1: State Standardization
    print("1. STATE STANDARDIZATION VERIFICATION")
    print("=" * 50)
    sample_states = df['state'].head(10).tolist()
    print(f"Sample states: {sample_states}")
    
    # Check for any remaining all-caps states
    all_caps_states = df[df['state'].str.isupper()]['state'].unique()
    if len(all_caps_states) > 0:
        print(f"❌ Found {len(all_caps_states)} all-caps states: {all_caps_states[:5]}")
    else:
        print("✅ All states properly formatted (no all-caps)")
    
    print()
    
    # Test 2: Address Parsing Verification
    print("2. ADDRESS PARSING VERIFICATION")
    print("=" * 50)
    
    # Check if street_address column has data
    non_empty_addresses = df['street_address'].notna().sum()
    total_records = len(df)
    print(f"Records with street_address: {non_empty_addresses}/{total_records} ({non_empty_addresses/total_records*100:.1f}%)")
    
    # Sample address parsing results
    sample_addresses = df[df['street_address'].notna()].head(5)
    if not sample_addresses.empty:
        print("\nSample address parsing results:")
        for idx, row in sample_addresses.iterrows():
            try:
                raw = json.loads(row['raw_data']) if pd.notna(row['raw_data']) else {}
                raw_address = raw.get('Address', 'N/A')
                print(f"  Raw: '{raw_address}'")
                print(f"  -> Street: '{row['street_address']}' | City: '{row['city']}' | State: '{row['address_state']}' | ZIP: '{row['zip_code']}'")
                print()
            except Exception as e:
                print(f"  Error processing record {idx}: {e}")
    else:
        print("❌ No street_address data found")
    
    # Check for duplicate address columns
    address_columns = [col for col in df.columns if 'address' in col.lower()]
    print(f"Address columns: {address_columns}")
    if len(address_columns) > 2:  # street_address, address_state are expected
        print("❌ Multiple address columns found")
    else:
        print("✅ Address columns properly organized")
    
    print()
    
    # Test 3: Phone Standardization Verification
    print("3. PHONE STANDARDIZATION VERIFICATION")
    print("=" * 50)
    
    # Check phone numbers for non-digit characters
    if 'phone' in df.columns:
        non_empty_phones = df['phone'].notna().sum()
        print(f"Records with phone numbers: {non_empty_phones}/{total_records}")
        
        if non_empty_phones > 0:
            # Sample phone numbers
            sample_phones = df[df['phone'].notna()]['phone'].head(10).tolist()
            print(f"Sample phone numbers: {sample_phones}")
            
            # Check for non-digit characters
            non_digit_phones = []
            for phone in sample_phones:
                if isinstance(phone, str) and any(not c.isdigit() for c in phone):
                    non_digit_phones.append(phone)
            
            if non_digit_phones:
                print(f"❌ Found {len(non_digit_phones)} phone numbers with non-digit characters: {non_digit_phones[:3]}")
            else:
                print("✅ All sample phone numbers contain only digits")
        else:
            print("ℹ️ No phone numbers found in data")
    else:
        print("ℹ️ No phone column found")
    
    print()
    
    # Test 4: Alaska-Specific Office Mapping Verification
    print("4. ALASKA-SPECIFIC OFFICE MAPPING VERIFICATION")
    print("=" * 50)
    
    # Look for Alaska House District offices
    alaska_data = df[df['state'] == 'Alaska']
    alaska_house_offices = alaska_data[alaska_data['office'].str.contains('House', case=False, na=False)]
    
    print(f"Alaska records: {len(alaska_data)}")
    print(f"Alaska House offices: {len(alaska_house_offices)}")
    
    if not alaska_house_offices.empty:
        print("\nSample Alaska House offices:")
        for idx, row in alaska_house_offices.head(5).iterrows():
            try:
                raw = json.loads(row['raw_data']) if pd.notna(row['raw_data']) else {}
                raw_office = raw.get('Office', 'N/A')
                print(f"  Raw: '{raw_office}' -> Processed: '{row['office']}' | District: '{row['district']}'")
            except Exception as e:
                print(f"  Error processing record {idx}: {e}")
    else:
        print("ℹ️ No Alaska House offices found")
    
    print()
    
    # Test 5: Party Letter Code Removal Verification
    print("5. PARTY LETTER CODE REMOVAL VERIFICATION")
    print("=" * 50)
    
    # Look for party letter codes in names
    party_codes_found = []
    sample_records = df.head(20)
    
    for idx, row in sample_records.iterrows():
        try:
            raw = json.loads(row['raw_data']) if pd.notna(row['raw_data']) else {}
            raw_name = raw.get('Name', 'N/A')
            
            # Check for party letter codes
            party_codes = ['(R)', '(r)', '(D)', '(d)', '(G)', '(g)', '(L)', '(l)', '(I)', '(i)']
            found_codes = [code for code in party_codes if code in raw_name]
            
            if found_codes:
                party_codes_found.append({
                    'raw_name': raw_name,
                    'processed_name': row['full_name_display'],
                    'party': row['party'],
                    'codes': found_codes
                })
        except Exception as e:
            continue
    
    if party_codes_found:
        print(f"Found {len(party_codes_found)} records with party letter codes:")
        for record in party_codes_found[:5]:
            print(f"  Raw: '{record['raw_name']}'")
            print(f"  -> Processed: '{record['processed_name']}' | Party: '{record['party']}' | Codes: {record['codes']}")
            print()
    else:
        print("ℹ️ No party letter codes found in sample records")
    
    print()
    
    # Test 6: Overall Data Quality Summary
    print("6. OVERALL DATA QUALITY SUMMARY")
    print("=" * 50)
    
    print(f"Total records: {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    
    # Check for missing data
    missing_data = {
        'party': df['party'].isna().sum(),
        'district': df['district'].isna().sum(),
        'city': df['city'].isna().sum(),
        'zip_code': df['zip_code'].isna().sum(),
        'street_address': df['street_address'].isna().sum()
    }
    
    print("\nMissing data summary:")
    for col, missing in missing_data.items():
        percentage = missing / len(df) * 100
        print(f"  {col}: {missing:,} ({percentage:.1f}%)")
    
    # Check column order
    expected_order = [
        'stable_id', 'state', 'full_name_display', 'prefix', 'first_name',
        'middle_name', 'last_name', 'suffix', 'nickname', 'office',
        'district', 'party', 'street_address', 'city', 'address_state', 'zip_code',
        'phone', 'email', 'facebook', 'twitter', 'filing_date',
        'election_date', 'first_added_date', 'last_updated_date'
    ]
    
    print(f"\nColumn order check:")
    for i, col in enumerate(expected_order):
        if col in df.columns:
            actual_pos = list(df.columns).index(col) + 1
            if actual_pos == i + 1:
                print(f"  ✅ {col} (position {actual_pos})")
            else:
                print(f"  ❌ {col} (expected {i+1}, found {actual_pos})")
        else:
            print(f"  ❌ {col} (missing)")
    
    print("\n=== VERIFICATION COMPLETE ===")

if __name__ == "__main__":
    verify_all_standards()
