#!/usr/bin/env python3
"""Audit raw vs processed data to identify missing columns and data."""

import sys
import os
import pandas as pd
from pathlib import Path
import glob

# Add src to path
sys.path.append('src')

def audit_raw_vs_processed():
    """Audit raw vs processed data columns and identify missing data."""
    print("🔍 AUDITING RAW VS PROCESSED DATA")
    print("=" * 80)
    
    raw_dir = Path("data/raw")
    processed_dir = Path("data/processed")
    final_dir = Path("data/final")
    
    # Get all raw files
    raw_files = list(raw_dir.glob("*"))
    raw_files = [f for f in raw_files if f.is_file() and not f.name.startswith('.')]
    
    # Get all processed files
    processed_files = list(processed_dir.glob("*_cleaned_*.xlsx"))
    
    # Get final file
    final_files = list(final_dir.glob("candidate_filings_*.xlsx"))
    
    print(f"📁 Found {len(raw_files)} raw files")
    print(f"📁 Found {len(processed_files)} processed files")
    print(f"📁 Found {len(final_files)} final files")
    print()
    
    # Group raw files by state
    state_raw_files = {}
    for raw_file in raw_files:
        # Extract state name from filename
        filename = raw_file.name.lower()
        for state in ['alaska', 'arizona', 'arkansas', 'colorado', 'delaware', 'georgia', 
                     'idaho', 'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 
                     'louisiana', 'maryland', 'missouri', 'montana', 'nebraska', 
                     'new_mexico', 'new_york', 'north_carolina', 'oklahoma', 'oregon', 
                     'pennsylvania', 'south_carolina', 'south_dakota', 'vermont', 
                     'virginia', 'washington', 'west_virginia', 'wyoming']:
            if state.replace('_', '') in filename.replace('_', ''):
                if state not in state_raw_files:
                    state_raw_files[state] = []
                state_raw_files[state].append(raw_file)
                break
    
    # Group processed files by state
    state_processed_files = {}
    for processed_file in processed_files:
        filename = processed_file.name.lower()
        for state in state_raw_files.keys():
            if state.replace('_', '') in filename.replace('_', ''):
                if state not in state_processed_files:
                    state_processed_files[state] = []
                state_processed_files[state].append(processed_file)
                break
    
    print("📊 COLUMN ANALYSIS BY STATE")
    print("=" * 80)
    
    missing_data_summary = {}
    
    for state in sorted(state_raw_files.keys()):
        print(f"\n🏛️  {state.upper()}")
        print("-" * 50)
        
        # Analyze raw data columns
        raw_columns = set()
        raw_sample_data = {}
        
        for raw_file in state_raw_files[state]:
            try:
                # Try different file reading methods
                if raw_file.suffix.lower() == '.csv':
                    # Try multiple encodings for CSV
                    for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                        try:
                            df = pd.read_csv(raw_file, encoding=encoding, nrows=5)
                            raw_columns.update(df.columns)
                            raw_sample_data[raw_file.name] = df.head(2)
                            break
                        except:
                            continue
                elif raw_file.suffix.lower() in ['.xlsx', '.xls']:
                    try:
                        df = pd.read_excel(raw_file, nrows=5)
                        raw_columns.update(df.columns)
                        raw_sample_data[raw_file.name] = df.head(2)
                    except Exception as e:
                        print(f"  ❌ Error reading {raw_file.name}: {e}")
                        continue
                
                print(f"  📄 {raw_file.name}: {len(df.columns)} columns")
                
            except Exception as e:
                print(f"  ❌ Error reading {raw_file.name}: {e}")
                continue
        
        # Analyze processed data columns
        processed_columns = set()
        processed_sample_data = {}
        
        if state in state_processed_files:
            for processed_file in state_processed_files[state]:
                try:
                    df = pd.read_excel(processed_file, nrows=5)
                    processed_columns.update(df.columns)
                    processed_sample_data[processed_file.name] = df.head(2)
                    print(f"  ✅ {processed_file.name}: {len(df.columns)} columns")
                except Exception as e:
                    print(f"  ❌ Error reading {processed_file.name}: {e}")
                    continue
        else:
            print(f"  ⚠️  No processed files found for {state}")
        
        # Compare columns
        if raw_columns and processed_columns:
            missing_in_processed = raw_columns - processed_columns
            extra_in_processed = processed_columns - raw_columns
            
            print(f"  📊 Raw columns: {len(raw_columns)}")
            print(f"  📊 Processed columns: {len(processed_columns)}")
            
            if missing_in_processed:
                print(f"  ❌ Missing in processed: {len(missing_in_processed)} columns")
                for col in sorted(missing_in_processed):
                    print(f"    - {col}")
                
                # Check if missing columns have data
                missing_data_analysis = {}
                for raw_file_name, sample_df in raw_sample_data.items():
                    for col in missing_in_processed:
                        if col in sample_df.columns:
                            non_null_count = sample_df[col].notna().sum()
                            if non_null_count > 0:
                                if col not in missing_data_analysis:
                                    missing_data_analysis[col] = []
                                missing_data_analysis[col].append({
                                    'file': raw_file_name,
                                    'non_null_count': non_null_count,
                                    'sample_values': sample_df[col].dropna().head(3).tolist()
                                })
                
                if missing_data_analysis:
                    print(f"  🔍 Missing columns with data:")
                    for col, data in missing_data_analysis.items():
                        print(f"    - {col}: {len(data)} files with data")
                        for item in data:
                            print(f"      * {item['file']}: {item['non_null_count']} non-null values")
                            print(f"        Sample: {item['sample_values']}")
                
                missing_data_summary[state] = {
                    'missing_columns': list(missing_in_processed),
                    'missing_data_analysis': missing_data_analysis
                }
            
            if extra_in_processed:
                print(f"  ➕ Extra in processed: {len(extra_in_processed)} columns")
                for col in sorted(extra_in_processed):
                    print(f"    + {col}")
        
        elif raw_columns:
            print(f"  ⚠️  Raw data found but no processed data to compare")
            print(f"  📊 Raw columns: {len(raw_columns)}")
            print(f"  📊 Raw columns: {sorted(raw_columns)}")
        else:
            print(f"  ❌ No raw data could be read")
    
    # Analyze final file
    print(f"\n🏁 FINAL FILE ANALYSIS")
    print("=" * 80)
    
    if final_files:
        final_file = final_files[0]  # Get the most recent
        try:
            final_df = pd.read_excel(final_file, nrows=5)
            print(f"📁 Final file: {final_file.name}")
            print(f"📊 Final columns: {len(final_df.columns)}")
            print(f"📊 Final columns: {sorted(final_df.columns)}")
            
            # Check for any missing important columns
            expected_columns = [
                'first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname',
                'full_name_display', 'office', 'district', 'party', 'election_year',
                'election_type', 'address', 'city', 'state', 'zip_code', 'phone', 'email',
                'website', 'filing_date', 'status', 'original_office'
            ]
            
            missing_expected = set(expected_columns) - set(final_df.columns)
            if missing_expected:
                print(f"❌ Missing expected columns: {sorted(missing_expected)}")
            else:
                print("✅ All expected columns present")
                
        except Exception as e:
            print(f"❌ Error reading final file: {e}")
    
    # Summary of missing data
    print(f"\n📋 MISSING DATA SUMMARY")
    print("=" * 80)
    
    if missing_data_summary:
        total_missing_columns = 0
        states_with_missing = 0
        
        for state, summary in missing_data_summary.items():
            if summary['missing_columns']:
                states_with_missing += 1
                total_missing_columns += len(summary['missing_columns'])
                print(f"🏛️  {state.upper()}: {len(summary['missing_columns'])} missing columns")
                for col in summary['missing_columns']:
                    print(f"  - {col}")
        
        print(f"\n📊 Summary:")
        print(f"  States with missing data: {states_with_missing}/{len(state_raw_files)}")
        print(f"  Total missing columns: {total_missing_columns}")
        
        # Identify most common missing columns
        all_missing = []
        for summary in missing_data_summary.values():
            all_missing.extend(summary['missing_columns'])
        
        if all_missing:
            from collections import Counter
            missing_counts = Counter(all_missing)
            print(f"\n🔍 Most commonly missing columns:")
            for col, count in missing_counts.most_common(10):
                print(f"  {col}: missing in {count} states")
    else:
        print("✅ No missing data detected!")
    
    return missing_data_summary

if __name__ == "__main__":
    audit_raw_vs_processed()
