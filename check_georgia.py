import pandas as pd

print("=== GEORGIA DATA COMPLETENESS ===")

# Load raw data
raw = pd.read_excel('data/raw/georgia_candidates_2024.xlsx')
print(f"Raw records: {len(raw)}")
print("Raw data counts:")
for col in raw.columns:
    non_null = raw[col].notna().sum()
    print(f"  {col}: {non_null}/{len(raw)} ({non_null/len(raw)*100:.1f}%)")

# Load processed data
processed = pd.read_excel('data/processed/georgia_candidates_2024_cleaned_20250820_131541.xlsx')
print(f"\nProcessed records: {len(processed)}")
print("Processed key columns:")
for col in ['address', 'city', 'zip_code', 'county', 'address_state', 'phone', 'email', 'website', 'party', 'office']:
    non_null = processed[col].notna().sum()
    print(f"  {col}: {non_null}/{len(processed)} ({non_null/len(processed)*100:.1f}%)")

# Check data preservation
print("\nData preservation analysis:")
if 'Candidate Name' in raw.columns:
    raw_name_count = raw['Candidate Name'].notna().sum()
    proc_name_count = processed['full_name_display'].notna().sum()
    print(f"  Candidate Name: {proc_name_count}/{raw_name_count} ({proc_name_count/raw_name_count*100:.1f}% preserved)")

if 'Office Name' in raw.columns:
    raw_office_count = raw['Office Name'].notna().sum()
    proc_office_count = processed['office'].notna().sum()
    print(f"  Office Name: {proc_office_count}/{raw_office_count} ({proc_office_count/raw_office_count*100:.1f}% preserved)")

if 'Candidate Party' in raw.columns:
    raw_party_count = raw['Candidate Party'].notna().sum()
    proc_party_count = processed['party'].notna().sum()
    print(f"  Candidate Party: {proc_party_count}/{raw_party_count} ({proc_party_count/raw_party_count*100:.1f}% preserved)")

if 'Incumbent' in raw.columns:
    raw_inc_count = raw['Incumbent'].notna().sum()
    print(f"  Incumbent: {raw_inc_count}/{len(raw)} ({raw_inc_count/len(raw)*100:.1f}% in raw)")

if 'Office Type' in raw.columns:
    raw_type_count = raw['Office Type'].notna().sum()
    print(f"  Office Type: {raw_type_count}/{len(raw)} ({raw_type_count/len(raw)*100:.1f}% in raw)")

# Check address components
if 'Street Number' in raw.columns:
    raw_street_count = raw['Street Number'].notna().sum()
    proc_addr_count = processed['address'].notna().sum()
    print(f"  Street Number → Address: {proc_addr_count}/{raw_street_count} ({proc_addr_count/raw_street_count*100:.1f}% preserved)")

# Check that address_state is null (no address data in raw)
print(f"\nAddress state check (should be null):")
print(f"  address_state null count: {processed['address_state'].isna().sum()}/{len(processed)} ({(processed['address_state'].isna().sum()/len(processed))*100:.1f}%)")
