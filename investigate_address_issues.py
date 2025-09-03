import pandas as pd

df = pd.read_csv('data/final/candidate_filings_FINAL_20250903_134419.csv')

print('=== ADDRESS PARSING ISSUES INVESTIGATION ===')

# Issue 1: Missing Address States
print('\n1. MISSING ADDRESS STATES:')
print(f'Records with address_state: {df["address_state"].notna().sum()} ({df["address_state"].notna().sum()/len(df)*100:.1f}%)')
print('States with address_state data:')
states_with_address_state = df[df['address_state'].notna()]['state'].value_counts()
print(states_with_address_state.head(10))

print('\nSample records with address_state:')
sample_with_state = df[df['address_state'].notna()].head(3)
for _, row in sample_with_state.iterrows():
    print(f'  {row["state"]}: Street="{row["street_address"]}" | State="{row["address_state"]}"')

print('\nSample records WITHOUT address_state:')
sample_without_state = df[df['address_state'].isna() & df['street_address'].notna()].head(3)
for _, row in sample_without_state.iterrows():
    print(f'  {row["state"]}: Street="{row["street_address"]}" | State="{row["address_state"]}"')

# Issue 2: Street Address but No City
print('\n2. STREET ADDRESS BUT NO CITY:')
no_city_df = df[df['street_address'].notna() & df['city'].isna()]
print(f'Records with street_address but no city: {len(no_city_df)}')
print('States with this issue:')
print(no_city_df['state'].value_counts().head(10))

print('\nSample records with street but no city:')
for _, row in no_city_df.head(5).iterrows():
    print(f'  {row["state"]}: Street="{row["street_address"]}" | City="{row["city"]}" | Raw: {row["raw_data"][:100]}...')

# Issue 3: ZIP Code Formatting
print('\n3. ZIP CODE FORMATTING ISSUES:')
zip_issues = df[df['zip_code'].notna() & df['zip_code'].astype(str).str.contains('.0')]
print(f'Records with ZIP ending in .0: {len(zip_issues)}')
print('Sample ZIP issues:')
print(zip_issues['zip_code'].value_counts().head(10))

print('\nSample records with .0 ZIPs:')
for _, row in zip_issues.head(5).iterrows():
    print(f'  {row["state"]}: ZIP="{row["zip_code"]}" | Street="{row["street_address"]}"')
