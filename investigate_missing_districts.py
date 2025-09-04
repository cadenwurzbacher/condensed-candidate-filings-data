import pandas as pd

# Load the latest pipeline output
df = pd.read_csv('data/final/candidate_filings_FINAL_20250904_154732.csv', dtype={'phone': str, 'zip_code': str})

# Check Alaska missing districts
ak_missing = df[(df['state'] == 'Alaska') & (df['office'] == 'State House') & (df['district'].isna())]
print('=== ALASKA MISSING DISTRICTS ===')
print(f'Alaska State House missing districts: {len(ak_missing)}')

if len(ak_missing) > 0:
    print('\nSample raw data:')
    for i in range(3):
        raw_str = ak_missing['raw_data'].iloc[i]
        print(f'Record {i}: {raw_str[:300]}...')

# Check North Carolina missing districts
nc_missing = df[(df['state'] == 'North Carolina') & (df['office'] == 'State House') & (df['district'].isna())]
print('\n=== NORTH CAROLINA MISSING DISTRICTS ===')
print(f'North Carolina State House missing districts: {len(nc_missing)}')

if len(nc_missing) > 0:
    print('\nSample raw data:')
    for i in range(3):
        raw_str = nc_missing['raw_data'].iloc[i]
        print(f'Record {i}: {raw_str[:300]}...')

# Check what offices are missing districts
districted_offices = ['US House', 'US Senate', 'State House', 'State Senate']
missing_by_office = df[df['office'].isin(districted_offices) & df['district'].isna()]['office'].value_counts()
print('\n=== MISSING DISTRICTS BY OFFICE ===')
print(missing_by_office)
