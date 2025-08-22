#!/usr/bin/env python3
"""Check data quality after pipeline run."""

import pandas as pd

def check_data_quality():
    print("🔍 CHECKING DATA QUALITY AFTER PIPELINE RUN")
    print("=" * 60)
    
    # Load final dataset
    df = pd.read_excel('data/final/candidate_filings_20250819_145702.xlsx')
    
    print(f"📊 FINAL DATASET OVERVIEW:")
    print(f"Total records: {len(df):,}")
    print(f"Total columns: {len(df.columns)}")
    
    print(f"\n🔍 DATA QUALITY CHECK:")
    print(f"Records with names: {df['full_name_display'].notna().sum():,} / {len(df):,} ({(df['full_name_display'].notna().sum()/len(df)*100):.1f}%)")
    print(f"Records with offices: {df['office'].notna().sum():,} / {len(df):,} ({(df['office'].notna().sum()/len(df)*100):.1f}%)")
    print(f"Records with parties: {df['party'].notna().sum():,} / {len(df):,} ({(df['party'].notna().sum()/len(df)*100):.1f}%)")
    print(f"Records with addresses: {df['address'].notna().sum():,} / {len(df):,} ({(df['address'].notna().sum()/len(df)*100):.1f}%)")
    
    print(f"\n📊 NORTH CAROLINA PERFORMANCE:")
    nc_data = df[df['state'] == 'North Carolina']
    print(f"NC records: {len(nc_data):,}")
    
    if len(nc_data) > 0:
        years = nc_data['election_year'].dropna().unique()
        print(f"NC election years: {sorted(years)}")
        print(f"NC offices: {nc_data['office'].nunique()} unique offices")
        print(f"NC names: {nc_data['full_name_display'].notna().sum():,} with names")
        print(f"NC party data: {nc_data['party'].notna().sum():,} with party info")
        
        # Check if we got all 11 years
        if len(years) == 11:
            print("✅ SUCCESS: North Carolina has all 11 years!")
        else:
            print(f"⚠️  Expected 11 years, got {len(years)}")
    
    print(f"\n🏛️  STATES SUMMARY:")
    states = df['state'].value_counts()
    print(f"Total states processed: {len(states)}")
    print(f"States with most records:")
    for state, count in states.head(10).items():
        print(f"  {state}: {count:,} records")
    
    print(f"\n🗳️  ELECTION YEARS:")
    years = df['election_year'].dropna().value_counts().sort_index()
    print(f"Year range: {years.index.min()} - {years.index.max()}")
    print(f"Total years: {len(years)}")
    
    print(f"\n📈 OFFICE STANDARDIZATION:")
    if 'office_confidence' in df.columns:
        confidence_counts = df['office_confidence'].value_counts().sort_index()
        print("Office standardization confidence levels:")
        for conf, count in confidence_counts.items():
            print(f"  {conf}: {count:,} records")

if __name__ == "__main__":
    check_data_quality()
