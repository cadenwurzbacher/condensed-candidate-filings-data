import pandas as pd
import numpy as np

# Load the latest pipeline output
df = pd.read_csv('data/final/candidate_filings_FINAL_20250904_153842.csv', dtype={'phone': str, 'zip_code': str})

print("🔍 COMPREHENSIVE ANALYSIS OF ALL REMAINING ISSUES")
print("=" * 70)

# 1. STATEWIDE OFFICES DISTRICT ISSUE
print("\n1️⃣ STATEWIDE OFFICES DISTRICT ISSUE")
statewide_offices = ['Governor', 'Secretary of State', 'State Attorney General', 'State Treasurer', 'US Senate', 'US President']
statewide_records = df[df['office'].isin(statewide_offices)]
zero_districts = statewide_records[statewide_records['district'].astype(str).isin(['0', '0.0'])].shape[0]
total_statewide = len(statewide_records)

print(f"Statewide office records: {total_statewide}")
print(f"Records with district = 0 or 0.0: {zero_districts}")

if zero_districts < 10:
    print("✅ Statewide offices district fix: SUCCESS")
else:
    print(f"❌ Statewide offices district fix: FAILED - {zero_districts} still have district = 0")

# 2. KANSAS STATE BOARD OF EDUCATION ISSUE
print("\n2️⃣ KANSAS STATE BOARD OF EDUCATION ISSUE")
ks_df = df[df['state'] == 'Kansas']
ks_board_ed = ks_df[ks_df['office'] == 'State Board of Education']
print(f"Kansas State Board of Education records: {len(ks_board_ed)}")

if len(ks_board_ed) > 0:
    print("✅ Kansas State Board of Education fix: SUCCESS")
else:
    print("❌ Kansas State Board of Education fix: FAILED - No records found")

# 3. ADDRESS DATA QUALITY ISSUES
print("\n3️⃣ ADDRESS DATA QUALITY ISSUES")
address_issues = []

# Missing address states
missing_address_state = df[df['street_address'].notna() & df['address_state'].isna()].shape[0]
total_with_address = df[df['street_address'].notna()].shape[0]
if missing_address_state > 0:
    address_issues.append(f"Missing address_state: {missing_address_state}/{total_with_address} records")

# Street address but no city
street_no_city = df[df['street_address'].notna() & df['city'].isna()].shape[0]
if street_no_city > 0:
    address_issues.append(f"Street address but no city: {street_no_city} records")

# Empty address fields
empty_address = df[df['street_address'].isna() & df['city'].isna() & df['address_state'].isna() & df['zip_code'].isna()].shape[0]
total_records = len(df)
if empty_address > 0:
    address_issues.append(f"Completely empty address: {empty_address}/{total_records} records")

if address_issues:
    print("❌ Address data quality issues found:")
    for issue in address_issues:
        print(f"  - {issue}")
else:
    print("✅ Address data quality: GOOD")

# 4. NAME STANDARDIZATION ISSUES
print("\n4️⃣ NAME STANDARDIZATION ISSUES")
name_issues = []

# All caps names in full_name_display
all_caps_names = df[df['full_name_display'].str.isupper() & df['full_name_display'].notna()].shape[0]
if all_caps_names > 0:
    name_issues.append(f"All caps names: {all_caps_names} records")

# Missing name components
missing_first = df[df['first_name'].isna() & df['full_name_display'].notna()].shape[0]
if missing_first > 0:
    name_issues.append(f"Missing first_name: {missing_first} records")

missing_last = df[df['last_name'].isna() & df['full_name_display'].notna()].shape[0]
if missing_last > 0:
    name_issues.append(f"Missing last_name: {missing_last} records")

# Party information in names
party_in_name = df[df['full_name_display'].str.contains(r'\([RDGLI]\)', case=False, na=False)].shape[0]
if party_in_name > 0:
    name_issues.append(f"Party codes in names: {party_in_name} records")

if name_issues:
    print("❌ Name standardization issues found:")
    for issue in name_issues:
        print(f"  - {issue}")
else:
    print("✅ Name standardization: GOOD")

# 5. OFFICE STANDARDIZATION ISSUES
print("\n5️⃣ OFFICE STANDARDIZATION ISSUES")
office_issues = []

# Offices with "/" that should be standardized
slash_offices = df[df['office'].str.contains('/', na=False)].shape[0]
if slash_offices > 0:
    office_issues.append(f"Offices with '/': {slash_offices} records")

# Non-standard office names
non_standard_offices = df[~df['office'].isin([
    'US President', 'US House', 'US Senate', 'State House', 'State Senate', 
    'City Council', 'County Commission', 'Governor', 'Secretary of State', 
    'Sheriff', 'Mayor', 'State Attorney General', 'State Treasurer', 'State Board of Education'
])].shape[0]
if non_standard_offices > 0:
    office_issues.append(f"Non-standard offices: {non_standard_offices} records")

# Missing districts for districted offices
districted_offices = ['US House', 'US Senate', 'State House', 'State Senate']
missing_districts = df[df['office'].isin(districted_offices) & df['district'].isna()].shape[0]
if missing_districts > 0:
    office_issues.append(f"Missing districts for districted offices: {missing_districts} records")

if office_issues:
    print("❌ Office standardization issues found:")
    for issue in office_issues:
        print(f"  - {issue}")
else:
    print("✅ Office standardization: GOOD")

# 6. PARTY STANDARDIZATION ISSUES
print("\n6️⃣ PARTY STANDARDIZATION ISSUES")
party_issues = []

# Non-standard party names
standard_parties = ['Democratic', 'Republican', 'Nonpartisan', 'Independent', 'Libertarian', 'Green']
non_standard_parties = df[~df['party'].isin(standard_parties) & df['party'].notna()].shape[0]
if non_standard_parties > 0:
    party_issues.append(f"Non-standard party names: {non_standard_parties} records")

# Party codes that should be extracted
party_codes = df[df['party'].str.contains(r'\([RDGLI]\)', case=False, na=False)].shape[0]
if party_codes > 0:
    party_issues.append(f"Party codes not extracted: {party_codes} records")

if party_issues:
    print("❌ Party standardization issues found:")
    for issue in party_issues:
        print(f"  - {issue}")
else:
    print("✅ Party standardization: GOOD")

# 7. DATA TYPE ISSUES
print("\n7️⃣ DATA TYPE ISSUES")
type_issues = []

# Phone numbers should be strings
phone_not_string = df[df['phone'].notna() & ~df['phone'].astype(str).str.isdigit()].shape[0]
if phone_not_string > 0:
    type_issues.append(f"Phone numbers not digits only: {phone_not_string} records")

# ZIP codes should be strings
zip_not_string = df[df['zip_code'].notna() & ~df['zip_code'].astype(str).str.isdigit()].shape[0]
if zip_not_string > 0:
    type_issues.append(f"ZIP codes not digits only: {zip_not_string} records")

if type_issues:
    print("❌ Data type issues found:")
    for issue in type_issues:
        print(f"  - {issue}")
else:
    print("✅ Data types: GOOD")

# 8. STATE-SPECIFIC ISSUES
print("\n8️⃣ STATE-SPECIFIC ISSUES")
state_issues = []

# Check each state for specific issues
for state in df['state'].unique():
    state_df = df[df['state'] == state]
    
    # Arizona: Party abbreviations
    if state == 'Arizona':
        abbreviated_parties = state_df[state_df['party'].str.contains('Dem|Rep', case=False, na=False)].shape[0]
        if abbreviated_parties > 0:
            state_issues.append(f"Arizona: {abbreviated_parties} abbreviated party names")
    
    # Delaware: County abbreviations
    if state == 'Delaware':
        county_abbrevs = state_df[state_df['county'].str.contains(r'^[NSK]', case=False, na=False)].shape[0]
        if county_abbrevs > 0:
            state_issues.append(f"Delaware: {county_abbrevs} abbreviated county names")

if state_issues:
    print("❌ State-specific issues found:")
    for issue in state_issues:
        print(f"  - {issue}")
else:
    print("✅ State-specific issues: GOOD")

# 9. MISSING DATA ISSUES
print("\n9️⃣ MISSING DATA ISSUES")
missing_issues = []

# Critical missing data
missing_office = df[df['office'].isna()].shape[0]
if missing_office > 0:
    missing_issues.append(f"Missing office: {missing_office} records")

missing_state = df[df['state'].isna()].shape[0]
if missing_state > 0:
    missing_issues.append(f"Missing state: {missing_state} records")

missing_name = df[df['full_name_display'].isna()].shape[0]
if missing_name > 0:
    missing_issues.append(f"Missing full_name_display: {missing_name} records")

if missing_issues:
    print("❌ Missing data issues found:")
    for issue in missing_issues:
        print(f"  - {issue}")
else:
    print("✅ Missing data: GOOD")

# 10. DUPLICATE ISSUES
print("\n🔟 DUPLICATE ISSUES")
duplicate_issues = []

# Duplicate stable IDs
duplicate_ids = df[df.duplicated(subset=['stable_id'], keep=False)].shape[0]
if duplicate_ids > 0:
    duplicate_issues.append(f"Duplicate stable IDs: {duplicate_ids} records")

# Duplicate candidate records
duplicate_candidates = df[df.duplicated(subset=['full_name_display', 'state', 'office', 'election_year'], keep=False)].shape[0]
if duplicate_candidates > 0:
    duplicate_issues.append(f"Duplicate candidate records: {duplicate_candidates} records")

if duplicate_issues:
    print("❌ Duplicate issues found:")
    for issue in duplicate_issues:
        print(f"  - {issue}")
else:
    print("✅ Duplicates: GOOD")

print("\n" + "=" * 70)
print("🎯 COMPREHENSIVE ANALYSIS COMPLETE")

# Summary
total_issues = len(address_issues) + len(name_issues) + len(office_issues) + len(party_issues) + len(type_issues) + len(state_issues) + len(missing_issues) + len(duplicate_issues)
print(f"\n📊 SUMMARY: {total_issues} total issues identified")
