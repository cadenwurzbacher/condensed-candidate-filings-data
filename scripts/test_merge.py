#!/usr/bin/env python3
"""Test merging North Carolina files to reproduce the error."""

import pandas as pd
import glob
import os

def test_nc_merge():
    print("🔍 TESTING NORTH CAROLINA MERGE TO REPRODUCE ERROR")
    print("=" * 60)
    
    try:
        # Get all NC files
        nc_files = glob.glob('data/raw/north_carolina_candidates_*.csv') + glob.glob('data/raw/north_carolina_candidates_*.xlsx')
        print(f"Found {len(nc_files)} North Carolina files")
        
        # Load and merge files like the main pipeline does
        all_data = []
        for file_path in nc_files[:3]:  # Test with first 3 files
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
        merged_df.to_excel('temp_merged.xlsx', index=False)
        print(f"✅ Saved merged data to temp_merged.xlsx")
        
        # Test the cleaner with merged data
        print(f"\nTesting cleaner with merged data...")
        from src.pipeline.state_cleaners.north_carolina_cleaner import clean_north_carolina_candidates
        
        cleaned_df = clean_north_carolina_candidates('temp_merged.xlsx', 'data/processed')
        print(f"✅ Successfully cleaned {len(cleaned_df)} records")
        
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_nc_merge()
