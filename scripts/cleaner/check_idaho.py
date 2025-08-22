import pandas as pd

print("=== IDAHO DATA COMPLETENESS ===")

# Load raw data
raw = pd.read_csv('data/raw/idaho_candidates_2024.csv', encoding='latin-1')
print(f"Raw records: {len(raw)}")
print("Raw data counts:")
for col in raw.columns:
    non_null = raw[col].notna().sum()
    print(f"  {col}: {non_null}/{len(raw)} ({non_null/len(raw)*100:.1f}%)")

# Load processed data
processed = pd.read_excel('data/processed/idaho_candidates_2024_cleaned_20250820_131542.xlsx')
print(f"\nProcessed records: {len(processed)}")
print("Processed key columns:")
for col in ['address', 'city', 'zip_code', 'county', 'address_state', 'phone', 'email', 'website', 'party', 'office', 'district']:
    non_null = processed[col].notna().sum()
    print(f"  {col}: {non_null}/{len(processed)} ({non_null/len(processed)*100:.1f}%)")

# Check data preservation
print("\nData preservation analysis:")
if 'Name' in raw.columns:
    raw_name_count = raw['Name'].notna().sum()
    proc_name_count = processed['full_name_display'].notna().sum()
    print(f"  Name: {proc_name_count}/{raw_name_count} ({proc_name_count/raw_name_count*100:.1f}% preserved)")

if 'Position' in raw.columns:
    raw_office_count = raw['Position'].notna().sum()
    proc_office_count = processed['office'].notna().sum()
    print(f"  Position: {proc_office_count}/{raw_office_count} ({proc_office_count/raw_office_count*100:.1f}% preserved)")

if 'Dist.' in raw.columns:
    raw_district_count = raw['Dist.'].notna().sum()
    proc_district_count = processed['district'].notna().sum()
    print(f"  District: {proc_district_count}/{raw_district_count} ({proc_district_count/raw_district_count*100:.1f}% preserved)")

if 'Seat' in raw.columns:
    raw_seat_count = raw['Seat'].notna().sum()
    print(f"  Seat: {raw_seat_count}/{len(raw)} ({raw_seat_count/len(raw)*100:.1f}% in raw)")

if 'Party' in raw.columns:
    raw_party_count = raw['Party'].notna().sum()
    proc_party_count = processed['party'].notna().sum()
    print(f"  Party: {proc_party_count}/{raw_party_count} ({proc_party_count/raw_party_count*100:.1f}% preserved)")

if 'District Type' in raw.columns:
    raw_type_count = raw['District Type'].notna().sum()
    print(f"  District Type: {raw_type_count}/{len(raw)} ({raw_type_count/len(raw)*100:.1f}% in raw)")

if 'Notes' in raw.columns:
    raw_notes_count = raw['Notes'].notna().sum()
    print(f"  Notes: {raw_notes_count}/{len(raw)} ({raw_notes_count/len(raw)*100:.1f}% in raw)")

# Check that address_state is null (no address data in raw)
print(f"\nAddress state check (should be null):")
print(f"  address_state null count: {processed['address_state'].isna().sum()}/{len(processed)} ({(processed['address_state'].isna().sum()/len(processed))*100:.1f}%)")
