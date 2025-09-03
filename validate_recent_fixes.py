import pandas as pd
import numpy as np

# Load the latest pipeline output
df = pd.read_csv('data/final/candidate_filings_FINAL_20250903_171552.csv')

print("🔍 COMPREHENSIVE VALIDATION OF RECENT FIXES")
print("=" * 60)

# 1. KANSAS HOUSE MAPPING FIX
print("\n1️⃣ KANSAS HOUSE MAPPING FIX")
ks_df = df[df['state'] == 'Kansas']
ks_state_house = ks_df[ks_df['office'] == 'State House']
ks_us_house = ks_df[ks_df['office'] == 'US House']

print(f"Kansas State House records: {len(ks_state_house)}")
print(f"Kansas US House records: {len(ks_us_house)}")

if len(ks_state_house) > 0:
    print("✅ Kansas House mapping fix: SUCCESS - State House records found")
    print(f"Sample State House districts: {ks_state_house['district'].dropna().head(5).tolist()}")
else:
    print("❌ Kansas House mapping fix: FAILED - No State House records found")

if len(ks_us_house) < 100:  # Should be much lower now
    print("✅ Kansas US House records reduced significantly")
else:
    print(f"⚠️ Kansas US House records still high: {len(ks_us_house)}")

# 2. WYOMING STATE HOUSE DISTRICT EXTRACTION FIX
print("\n2️⃣ WYOMING STATE HOUSE DISTRICT EXTRACTION FIX")
wy_df = df[df['state'] == 'Wyoming']
wy_state_house = wy_df[wy_df['office'] == 'State House']

if len(wy_state_house) > 0:
    districts_with_data = wy_state_house['district'].notna().sum()
    total_districts = len(wy_state_house)
    success_rate = (districts_with_data / total_districts) * 100
    
    print(f"Wyoming State House records: {total_districts}")
    print(f"Records with district data: {districts_with_data}")
    print(f"Success rate: {success_rate:.1f}%")
    
    if success_rate > 80:
        print("✅ Wyoming district extraction fix: SUCCESS")
        print(f"Sample districts: {wy_state_house['district'].dropna().head(5).tolist()}")
    else:
        print("❌ Wyoming district extraction fix: PARTIAL SUCCESS")
else:
    print("❌ Wyoming district extraction fix: FAILED - No State House records found")

# 3. PHONE NUMBER DATA TYPE FIX
print("\n3️⃣ PHONE NUMBER DATA TYPE FIX")
phone_dtype = df['phone'].dtype
print(f"Phone column data type: {phone_dtype}")

if phone_dtype == 'object' or str(phone_dtype) == 'string':
    print("✅ Phone number data type fix: SUCCESS - String type")
else:
    print(f"❌ Phone number data type fix: FAILED - Still {phone_dtype}")

# 4. ZIP CODE FORMATTING FIX
print("\n4️⃣ ZIP CODE FORMATTING FIX")
zip_with_dot_zero = df['zip_code'].astype(str).str.endswith('.0').sum()
total_zip_codes = df['zip_code'].notna().sum()

print(f"ZIP codes with .0 suffix: {zip_with_dot_zero}")
print(f"Total ZIP codes: {total_zip_codes}")

if zip_with_dot_zero < 100:  # Should be very low now
    print("✅ ZIP code formatting fix: SUCCESS")
else:
    print(f"❌ ZIP code formatting fix: FAILED - {zip_with_dot_zero} still have .0 suffix")

# 5. ALASKA STATE SENATE DISTRICT FIX
print("\n5️⃣ ALASKA STATE SENATE DISTRICT FIX")
ak_df = df[df['state'] == 'Alaska']
ak_state_senate = ak_df[ak_df['office'] == 'State Senate']

if len(ak_state_senate) > 0:
    letter_districts = ak_state_senate['district'].apply(lambda x: isinstance(x, str) and x.isalpha()).sum()
    print(f"Alaska State Senate records: {len(ak_state_senate)}")
    print(f"Records with letter districts: {letter_districts}")
    
    if letter_districts > 0:
        print("✅ Alaska State Senate district fix: SUCCESS")
        print(f"Sample letter districts: {ak_state_senate[ak_state_senate['district'].apply(lambda x: isinstance(x, str) and x.isalpha())]['district'].head(5).tolist()}")
    else:
        print("❌ Alaska State Senate district fix: FAILED - No letter districts found")
else:
    print("❌ Alaska State Senate district fix: FAILED - No State Senate records found")

# 6. ILLINOIS CONGRESS AND STATE SENATE FIX
print("\n6️⃣ ILLINOIS CONGRESS AND STATE SENATE FIX")
il_df = df[df['state'] == 'Illinois']
il_us_house = il_df[il_df['office'] == 'US House']
il_state_senate = il_df[il_df['office'] == 'State Senate']

print(f"Illinois US House records: {len(il_us_house)}")
print(f"Illinois State Senate records: {len(il_state_senate)}")

if len(il_us_house) > 0 and len(il_state_senate) > 0:
    print("✅ Illinois Congress and State Senate fix: SUCCESS")
    print(f"Sample US House districts: {il_us_house['district'].dropna().head(5).tolist()}")
    print(f"Sample State Senate districts: {il_state_senate['district'].dropna().head(5).tolist()}")
else:
    print("❌ Illinois Congress and State Senate fix: FAILED")

# 7. KANSAS SENATE FIX
print("\n7️⃣ KANSAS SENATE FIX")
ks_senate = ks_df[ks_df['office'] == 'State Senate']
print(f"Kansas State Senate records: {len(ks_senate)}")

if len(ks_senate) > 0:
    print("✅ Kansas Senate fix: SUCCESS")
else:
    print("❌ Kansas Senate fix: FAILED")

# 8. STATEWIDE OFFICES DISTRICT FIX
print("\n8️⃣ STATEWIDE OFFICES DISTRICT FIX")
statewide_offices = ['Governor', 'Secretary of State', 'State Attorney General', 'State Treasurer', 'US Senate', 'US President']
statewide_records = df[df['office'].isin(statewide_offices)]
zero_districts = statewide_records[statewide_records['district'] == 0].shape[0]
total_statewide = len(statewide_records)

print(f"Statewide office records: {total_statewide}")
print(f"Records with district = 0: {zero_districts}")

if zero_districts < 10:  # Should be very low now
    print("✅ Statewide offices district fix: SUCCESS")
else:
    print(f"❌ Statewide offices district fix: FAILED - {zero_districts} still have district = 0")

# 9. KANSAS STATE BOARD OF EDUCATION FIX
print("\n9️⃣ KANSAS STATE BOARD OF EDUCATION FIX")
ks_board_ed = ks_df[ks_df['office'] == 'State Board of Education']
print(f"Kansas State Board of Education records: {len(ks_board_ed)}")

if len(ks_board_ed) > 0:
    print("✅ Kansas State Board of Education fix: SUCCESS")
else:
    print("❌ Kansas State Board of Education fix: FAILED")

print("\n" + "=" * 60)
print("🎯 VALIDATION COMPLETE")
