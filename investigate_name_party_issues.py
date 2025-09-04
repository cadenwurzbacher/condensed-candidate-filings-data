import pandas as pd

# Load the latest pipeline output
df = pd.read_csv('data/final/candidate_filings_FINAL_20250904_154732.csv', dtype={'phone': str, 'zip_code': str})

# Check missing last names
missing_last = df[df['last_name'].isna() & df['full_name_display'].notna()]
print('=== MISSING LAST NAMES ===')
print(f'Missing last names: {len(missing_last)}')

if len(missing_last) > 0:
    print('\nSample records:')
    print(missing_last[['full_name_display', 'first_name', 'middle_name', 'last_name']].head(5).to_string(index=False))
    
    print('\nSample raw data:')
    for i in range(3):
        raw_str = missing_last['raw_data'].iloc[i]
        print(f'Record {i}: {raw_str[:300]}...')

# Check non-standard party names
non_standard_parties = df[~df['party'].isin(['Democratic', 'Republican', 'Nonpartisan', 'Independent', 'Libertarian', 'Green']) & df['party'].notna()]
print('\n=== NON-STANDARD PARTY NAMES ===')
print(f'Non-standard party names: {len(non_standard_parties)}')
print('\nMost common non-standard parties:')
print(non_standard_parties['party'].value_counts().head(10))
