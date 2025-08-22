import pandas as pd

print("=== ARIZONA DATA COMPLETENESS ===")

# Load raw data
raw = pd.read_excel('data/raw/arizona_candidates_2024.xlsx')
print(f"Raw records: {len(raw)}")
print("Raw data counts:")
for col in raw.columns:
    non_null = raw[col].notna().sum()
    print(f"  {col}: {non_null}/{len(raw)} ({non_null/len(raw)*100:.1f}%)")

# Load processed data
processed = pd.read_excel('data/processed/arizona_candidates_2024_cleaned_20250820_131540.xlsx')
print(f"\nProcessed records: {len(processed)}")
print("Processed key columns:")
for col in ['address', 'city', 'zip_code', 'county', 'address_state', 'phone', 'email', 'website', 'party', 'office']:
    non_null = processed[col].notna().sum()
    print(f"  {col}: {non_null}/{len(processed)} ({non_null/len(processed)*100:.1f}%)")

# Check data preservation
print("\nData preservation analysis:")
if 'Office' in raw.columns:
    raw_office_count = raw['Office'].notna().sum()
    proc_office_count = processed['office'].notna().sum()
    print(f"  Office: {proc_office_count}/{raw_office_count} ({proc_office_count/raw_office_count*100:.1f}% preserved)")

if 'Candidate Name' in raw.columns:
    raw_name_count = raw['Candidate Name'].notna().sum()
    proc_name_count = processed['full_name_display'].notna().sum()
    print(f"  Candidate Name: {proc_name_count}/{raw_name_count} ({proc_name_count/raw_name_count*100:.1f}% preserved)")

if 'Party' in raw.columns:
    raw_party_count = raw['Party'].notna().sum()
    proc_party_count = processed['party'].notna().sum()
    print(f"  Party: {proc_party_count}/{raw_party_count} ({proc_party_count/raw_party_count*100:.1f}% preserved)")

if 'Website' in raw.columns:
    raw_web_count = raw['Website'].notna().sum()
    proc_web_count = processed['website'].notna().sum()
    print(f"  Website: {proc_web_count}/{raw_web_count} ({proc_web_count/raw_web_count*100:.1f}% preserved)")

if 'Facebook' in raw.columns:
    raw_fb_count = raw['Facebook'].notna().sum()
    proc_fb_count = processed['facebook'].notna().sum()
    print(f"  Facebook: {proc_fb_count}/{raw_fb_count} ({proc_fb_count/raw_fb_count*100:.1f}% preserved)")

if 'Twitter' in raw.columns:
    raw_tw_count = raw['Twitter'].notna().sum()
    proc_tw_count = processed['twitter'].notna().sum()
    print(f"  Twitter: {proc_tw_count}/{raw_tw_count} ({proc_tw_count/raw_tw_count*100:.1f}% preserved)")

# Check that address_state is null (no address data in raw)
print(f"\nAddress state check (should be null):")
print(f"  address_state null count: {processed['address_state'].isna().sum()}/{len(processed)} ({(processed['address_state'].isna().sum()/len(processed))*100:.1f}%)")
