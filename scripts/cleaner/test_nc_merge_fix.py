#!/usr/bin/env python3
"""Test if the fixed North Carolina cleaner can handle merged data."""

import pandas as pd
import glob
import os

def test_nc_merge_fix():
    print("🔍 TESTING FIXED NORTH CAROLINA CLEANER WITH MERGED DATA")
    print("=" * 60)
    
    try:
        # Get all NC files
        nc_files = glob.glob('data/raw/north_carolina_candidates_*.csv') + glob.glob('data/raw/north_carolina_candidates_*.xlsx')
        print(f"Found {len(nc_files)} North Carolina files")
        
        # Load and merge files like the main pipeline does
        all_data = []
        for file_path in nc_files[:5]:  # Test with first 5 files
            try:
                filename = os.path.basename(file_path)
                print(f"\nProcessing {filename}...")
                
                # Handle different file types
                if file_path.endswith('.csv'):
                    # Try multiple encodings
                    df = None
                    encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
                    
                    for encoding in encodings_to_try:
                        try:
                            df = pd.read_csv(file_path, encoding=encoding)
                            print(f"  ✅ Read with {encoding}: {len(df)} records")
                            break
                        except UnicodeDecodeError:
                            continue
                        except Exception as e:
                            print(f"  ❌ Error with {encoding}: {e}")
                            continue
                    
                    if df is None:
                        print(f"  ❌ Failed to read {filename}")
                        continue
                        
                elif file_path.endswith('.xlsx'):
                    df = pd.read_excel(file_path)
                    print(f"  ✅ Read Excel: {len(df)} records")
                else:
                    df = pd.read_excel(file_path)
                    print(f"  ✅ Read other: {len(df)} records")
                
                # Extract election year from filename
                import re
                year_match = re.search(r'(\d{4})', filename)
                if year_match:
                    year = int(year_match.group(1))
                    df['election_year'] = year
                    print(f"  📅 Extracted year: {year}")
                
                # Add source file info
                df['_source_file'] = filename
                all_data.append(df)
                
            except Exception as e:
                print(f"  ❌ Error processing {filename}: {e}")
                continue
        
        if not all_data:
            print("❌ No data loaded")
            return
        
        # Merge all data
        print(f"\nMerging {len(all_data)} datasets...")
        merged_df = pd.concat(all_data, ignore_index=True)
        print(f"✅ Merged {len(merged_df)} total records")
        print(f"Columns: {list(merged_df.columns)}")
        
        # Save merged data to test file
        print(f"\nSaving merged data to test file...")
        merged_df.to_excel('temp_merged_nc.xlsx', index=False)
        print(f"✅ Saved merged data to temp_merged_nc.xlsx")
        
        # Test the fixed cleaner with merged data
        print(f"\nTesting FIXED North Carolina cleaner with merged data...")
        from src.pipeline.state_cleaners.north_carolina_cleaner import clean_north_carolina_candidates
        
        cleaned_df = clean_north_carolina_candidates('temp_merged_nc.xlsx', 'data/processed')
        print(f"🎉 SUCCESS: Fixed cleaner processed {len(cleaned_df)} records!")
        
        # Check if we got all years
        if 'election_year' in cleaned_df.columns:
            years = sorted(cleaned_df['election_year'].dropna().unique())
            print(f"📅 Election years included: {years}")
            print(f"✅ Expected: {len(years)} years, Got: {len(years)} years")
        
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_nc_merge_fix()
