#!/usr/bin/env python3
"""Check Kentucky's data structure to understand address parsing needs."""

import pandas as pd

def check_kentucky():
    """Check Kentucky's data structure."""
    
    # Load the final dataset
    df = pd.read_excel('data/final/candidate_filings_20250821_165831.xlsx')
    ky_data = df[df['state'] == 'Kentucky']
    
    print('=== KENTUCKY DATA STRUCTURE ===')
    print(f'Total Kentucky records: {len(ky_data):,}')
    
    print('\nColumns with data:')
    for col in ['city', 'county', 'district', 'address', 'address_state']:
        if col in ky_data.columns:
            non_null = ky_data[col].notna().sum()
            pct = non_null/len(ky_data)*100
            print(f'  {col:12}: {non_null:6,} records ({pct:5.1f}%)')
    
    print(f'\nSample city values:')
    city_samples = ky_data['city'].dropna().unique()[:10]
    for city in city_samples:
        print(f'  "{city}"')
    
    if 'county' in ky_data.columns:
        print(f'\nSample county values:')
        county_samples = ky_data['county'].dropna().unique()[:10]
        for county in county_samples:
            print(f'  "{county}"')
    
    if 'district' in ky_data.columns:
        print(f'\nSample district values:')
        district_samples = ky_data['district'].dropna().unique()[:10]
        for district in district_samples:
            print(f'  "{district}"')
    
    # Check if there are any address-like fields
    print(f'\nAll columns in Kentucky data:')
    for col in ky_data.columns:
        non_null = ky_data[col].notna().sum()
        if non_null > 0:
            pct = non_null/len(ky_data)*100
            print(f'  {col:25}: {non_null:6,} records ({pct:5.1f}%)')

if __name__ == "__main__":
    check_kentucky()
