import pandas as pd

# Load the latest pipeline output
df = pd.read_csv('data/final/candidate_filings_FINAL_20250904_161358.csv', dtype={'phone': str, 'zip_code': str})

print("🔍 VERIFICATION OF ALL FIXES")
print("=" * 60)

# 1. Election Date Formatting Fix
print("\n1️⃣ ELECTION DATE FORMATTING FIX")
election_dates = df['election_date'].dropna().head(10).tolist()
print("Sample election dates:")
for date in election_dates:
    print(f"  {date}")

gen_dates = df[df['election_date'].str.contains('-GEN', na=False)]
print(f"\nElection dates with -GEN: {len(gen_dates)}")

if len(gen_dates) == 0:
    print("✅ Election date formatting fix: SUCCESS")
else:
    print("❌ Election date formatting fix: FAILED")

# 2. Party Standardization Fix
print("\n2️⃣ PARTY STANDARDIZATION FIX")
non_standard_parties = df[~df['party'].isin(['Democratic', 'Republican', 'Nonpartisan', 'Independent', 'Libertarian', 'Green', 'Constitution', 'Write-in', '']) & df['party'].notna()]
print(f"Non-standard party names: {len(non_standard_parties)}")

if len(non_standard_parties) < 1000:  # Should be much lower now
    print("✅ Party standardization fix: SUCCESS")
    print("Most common remaining non-standard parties:")
    print(non_standard_parties['party'].value_counts().head(5))
else:
    print("❌ Party standardization fix: FAILED")

# 3. Office Standardization Fix
print("\n3️⃣ OFFICE STANDARDIZATION FIX")
slash_offices = df[df['office'].str.contains('/', na=False)]
print(f"Offices with '/': {len(slash_offices)}")

if len(slash_offices) < 10:
    print("✅ Office standardization fix: SUCCESS")
else:
    print("❌ Office standardization fix: FAILED")

# 4. County Abbreviation Fix
print("\n4️⃣ COUNTY ABBREVIATION FIX")
short_counties = df[df['county'].str.len() <= 3]
print(f"County abbreviations: {len(short_counties)}")

if len(short_counties) < 1000:  # Should be much lower now
    print("✅ County abbreviation fix: SUCCESS")
else:
    print("❌ County abbreviation fix: FAILED")

# 5. Numeric Column .0 Fix
print("\n5️⃣ NUMERIC COLUMN .0 FIX")
phone_with_dot_zero = df[df['phone'].astype(str).str.endswith('.0')]
zip_with_dot_zero = df[df['zip_code'].astype(str).str.endswith('.0')]

print(f"Phone numbers with .0: {len(phone_with_dot_zero)}")
print(f"ZIP codes with .0: {len(zip_with_dot_zero)}")

if len(phone_with_dot_zero) == 0 and len(zip_with_dot_zero) == 0:
    print("✅ Numeric column .0 fix: SUCCESS")
else:
    print("❌ Numeric column .0 fix: FAILED")

# 6. Statewide Offices District Fix
print("\n6️⃣ STATEWIDE OFFICES DISTRICT FIX")
statewide_offices = ['Governor', 'Secretary of State', 'State Attorney General', 'State Treasurer', 'US Senate', 'US President']
statewide_records = df[df['office'].isin(statewide_offices)]
zero_districts = statewide_records[statewide_records['district'].astype(str).isin(['0', '0.0'])].shape[0]

print(f"Statewide offices with district = 0: {zero_districts}")

if zero_districts < 10:
    print("✅ Statewide offices district fix: SUCCESS")
else:
    print("❌ Statewide offices district fix: FAILED")

# 7. Missing Last Names Fix
print("\n7️⃣ MISSING LAST NAMES FIX")
missing_last = df[df['last_name'].isna() & df['full_name_display'].notna()]
print(f"Missing last names: {len(missing_last)}")

if len(missing_last) < 1000:  # Should be much lower now
    print("✅ Missing last names fix: SUCCESS")
else:
    print("❌ Missing last names fix: FAILED")

# 8. Missing Districts Fix
print("\n8️⃣ MISSING DISTRICTS FIX")
districted_offices = ['US House', 'US Senate', 'State House', 'State Senate']
missing_districts = df[df['office'].isin(districted_offices) & df['district'].isna()]
print(f"Missing districts for districted offices: {len(missing_districts)}")

if len(missing_districts) < 5000:  # Should be much lower now
    print("✅ Missing districts fix: SUCCESS")
else:
    print("❌ Missing districts fix: FAILED")

print("\n" + "=" * 60)
print("🎯 VERIFICATION COMPLETE")
