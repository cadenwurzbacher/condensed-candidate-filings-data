#!/usr/bin/env python3
"""Check if North Carolina is in the new final output file."""

import pandas as pd

def check_new_file():
    print("🔍 CHECKING NEW FINAL OUTPUT FILE")
    print("=" * 50)
    
    # Load the new final dataset
    new_file = 'data/final/candidate_filings_20250819_154532.xlsx'
    df = pd.read_excel(new_file)
    
    print(f"📊 NEW FINAL DATASET:")
    print(f"Total records: {len(df):,}")
    print(f"Total states: {len(df['state'].unique())}")
    
    # Check for North Carolina
    nc_data = df[df['state'] == 'North Carolina']
    print(f"\n🏛️  NORTH CAROLINA STATUS:")
    print(f"Records: {len(nc_data):,}")
    
    if len(nc_data) > 0:
        years = sorted(nc_data['election_year'].dropna().unique())
        print(f"✅ SUCCESS: North Carolina included!")
        print(f"Election years: {years}")
        print(f"Total years: {len(years)}")
        
        # Compare with old file
        try:
            old_file = 'data/final/candidate_filings_20250819_145702.xlsx'
            old_df = pd.read_excel(old_file)
            old_nc = old_df[old_df['state'] == 'North Carolina']
            print(f"\n📈 IMPROVEMENT:")
            print(f"Old file NC records: {len(old_nc):,}")
            print(f"New file NC records: {len(nc_data):,}")
            print(f"Difference: +{len(nc_data) - len(old_nc):,} records")
        except:
            print("Could not compare with old file")
    else:
        print("❌ North Carolina still missing")
    
    # Show top states
    print(f"\n🏆 TOP STATES BY RECORD COUNT:")
    states = df['state'].value_counts()
    for state, count in states.head(10).items():
        print(f"  {state}: {count:,} records")

if __name__ == "__main__":
    check_new_file()
