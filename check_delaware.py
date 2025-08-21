import pandas as pd

print("=== DELAWARE DATA COMPLETENESS ===")

# Load raw data
raw = pd.read_excel('data/raw/delaware_candidates_2008_2024.xlsx')
print(f"Raw records: {len(raw)}")
print("Raw data counts:")
for col in raw.columns:
    non_null = raw[col].notna().sum()
    print(f"  {col}: {non_null}/{len(raw)} ({non_null/len(raw)*100:.1f}%)")

# Load processed data
processed = pd.read_excel('data/processed/delaware_candidates_2008_2024_cleaned_20250820_131540.xlsx')
print(f"\nProcessed records: {len(processed)}")
print("Processed key columns:")
for col in ['address', 'city', 'zip_code', 'county', 'address_state', 'phone', 'email', 'website', 'party', 'office', 'district', 'filing_date']:
    non_null = processed[col].notna().sum()
    print(f"  {col}: {non_null}/{len(processed)} ({non_null/len(processed)*100:.1f}%)")

# Check data preservation
print("\nData preservation analysis:")
if 'Name' in raw.columns:
    raw_name_count = raw['Name'].notna().sum()
    proc_name_count = processed['full_name_display'].notna().sum()
    print(f"  Name: {proc_name_count}/{raw_name_count} ({proc_name_count/raw_name_count*100:.1f}% preserved)")

if 'Office' in raw.columns:
    raw_office_count = raw['Office'].notna().sum()
    proc_office_count = processed['office'].notna().sum()
    print(f"  Office: {proc_office_count}/{raw_office_count} ({proc_office_count/raw_office_count*100:.1f}% preserved)")

if 'District' in raw.columns:
    raw_district_count = raw['District'].notna().sum()
    proc_district_count = processed['district'].notna().sum()
    print(f"  District: {proc_district_count}/{raw_district_count} ({proc_district_count/raw_district_count*100:.1f}% preserved)")

if 'County' in raw.columns:
    raw_county_count = raw['County'].notna().sum()
    proc_county_count = processed['county'].notna().sum()
    print(f"  County: {proc_county_count}/{raw_county_count} ({proc_county_count/raw_county_count*100:.1f}% preserved)")

if 'Date Filed' in raw.columns:
    raw_date_count = raw['Date Filed'].notna().sum()
    proc_date_count = processed['filing_date'].notna().sum()
    print(f"  Date Filed: {proc_date_count}/{raw_date_count} ({proc_date_count/raw_date_count*100:.1f}% preserved)")

if 'Website' in raw.columns:
    raw_web_count = raw['Website'].notna().sum()
    proc_web_count = processed['website'].notna().sum()
    print(f"  Website: {proc_web_count}/{raw_web_count} ({proc_web_count/raw_web_count*100:.1f}% preserved)")

# Check that address_state is null (no address data in raw)
print(f"\nAddress state check (should be null):")
print(f"  address_state null count: {processed['address_state'].isna().sum()}/{len(processed)} ({(processed['address_state'].isna().sum()/len(processed))*100:.1f}%)")
