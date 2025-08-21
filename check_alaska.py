import pandas as pd

print("=== ALASKA DATA COMPLETENESS ===")

# Load raw data
raw = pd.read_excel('data/raw/alaska_candidates_2022_2024.xlsx')
print(f"Raw records: {len(raw)}")
print("Raw data counts:")
for col in raw.columns:
    non_null = raw[col].notna().sum()
    print(f"  {col}: {non_null}/{len(raw)} ({non_null/len(raw)*100:.1f}%)")

# Load processed data
processed = pd.read_excel('data/processed/alaska_candidates_2022_2024_cleaned_20250820_131540.xlsx')
print(f"\nProcessed records: {len(processed)}")
print("Processed key columns:")
for col in ['address', 'city', 'zip_code', 'county', 'address_state', 'phone', 'email', 'website', 'party']:
    non_null = processed[col].notna().sum()
    print(f"  {col}: {non_null}/{len(processed)} ({non_null/len(processed)*100:.1f}%)")

# Check data preservation
print("\nData preservation analysis:")
if 'Address' in raw.columns:
    raw_addr_count = raw['Address'].notna().sum()
    proc_addr_count = processed['address'].notna().sum()
    print(f"  Address: {proc_addr_count}/{raw_addr_count} ({proc_addr_count/raw_addr_count*100:.1f}% preserved)")

if 'Phone Number' in raw.columns:
    raw_phone_count = raw['Phone Number'].notna().sum()
    proc_phone_count = processed['phone'].notna().sum()
    print(f"  Phone: {proc_phone_count}/{raw_phone_count} ({proc_phone_count/raw_phone_count*100:.1f}% preserved)")

if 'Email' in raw.columns:
    raw_email_count = raw['Email'].notna().sum()
    proc_email_count = processed['email'].notna().sum()
    print(f"  Email: {proc_email_count}/{raw_email_count} ({proc_email_count/raw_email_count*100:.1f}% preserved)")

if 'Website' in raw.columns:
    raw_web_count = raw['Website'].notna().sum()
    proc_web_count = processed['website'].notna().sum()
    print(f"  Website: {proc_web_count}/{raw_web_count} ({proc_web_count/raw_web_count*100:.1f}% preserved)")

if 'Party' in raw.columns:
    raw_party_count = raw['Party'].notna().sum()
    proc_party_count = processed['party'].notna().sum()
    print(f"  Party: {proc_party_count}/{raw_party_count} ({proc_party_count/raw_party_count*100:.1f}% preserved)")

if 'Office' in raw.columns:
    raw_office_count = raw['Office'].notna().sum()
    proc_office_count = processed['office'].notna().sum()
    print(f"  Office: {proc_office_count}/{raw_office_count} ({proc_office_count/raw_office_count*100:.1f}% preserved)")

if 'Name' in raw.columns:
    raw_name_count = raw['Name'].notna().sum()
    proc_name_count = processed['full_name_display'].notna().sum()
    print(f"  Name: {proc_name_count}/{raw_name_count} ({proc_name_count/raw_name_count*100:.1f}% preserved)")
