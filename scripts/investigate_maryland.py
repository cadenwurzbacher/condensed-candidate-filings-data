#!/usr/bin/env python3
"""Investigate Maryland's raw data structure to understand the 139 missing columns."""

import sys
import os
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.append('src')

def investigate_maryland():
    """Investigate Maryland's raw data structure."""
    print("🔍 INVESTIGATING MARYLAND'S RAW DATA")
    print("=" * 80)
    
    raw_dir = Path("data/raw")
    processed_dir = Path("data/processed")
    
    # Find Maryland raw files
    maryland_raw_files = list(raw_dir.glob("*maryland*"))
    maryland_processed_files = list(processed_dir.glob("*maryland*"))
    
    print(f"📁 Maryland raw files found: {len(maryland_raw_files)}")
    for f in maryland_raw_files:
        print(f"  - {f.name}")
    
    print(f"📁 Maryland processed files found: {len(maryland_processed_files)}")
    for f in maryland_processed_files:
        print(f"  - {f.name}")
    
    print("\n" + "="*80)
    
    # Analyze each raw file in detail
    for raw_file in maryland_raw_files:
        print(f"\n📄 ANALYZING: {raw_file.name}")
        print("-" * 60)
        
        try:
            # Try to read the file
            if raw_file.suffix.lower() == '.csv':
                # Try multiple encodings
                for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                    try:
                        df = pd.read_csv(raw_file, encoding=encoding)
                        print(f"✅ Successfully read with {encoding} encoding")
                        break
                    except:
                        continue
                else:
                    print(f"❌ Failed to read with any encoding")
                    continue
            elif raw_file.suffix.lower() in ['.xlsx', '.xls']:
                try:
                    df = pd.read_excel(raw_file)
                    print(f"✅ Successfully read Excel file")
                except Exception as e:
                    print(f"❌ Error reading Excel: {e}")
                    continue
            else:
                print(f"❌ Unsupported file type: {raw_file.suffix}")
                continue
            
            print(f"📊 Shape: {df.shape}")
            print(f"📊 Columns: {len(df.columns)}")
            
            # Show first few rows to understand structure
            print(f"\n🔍 First 3 rows:")
            print(df.head(3).to_string())
            
            # Show column info
            print(f"\n📋 Column information:")
            for i, col in enumerate(df.columns):
                non_null_count = df[col].notna().sum()
                null_count = df[col].isna().sum()
                sample_values = df[col].dropna().head(3).tolist()
                
                print(f"  {i+1:3d}. {col}")
                print(f"      Non-null: {non_null_count}, Null: {null_count}")
                if sample_values:
                    print(f"      Sample: {sample_values}")
                print()
            
            # Check for unusual column patterns
            print(f"\n🔍 Column pattern analysis:")
            unnamed_cols = [col for col in df.columns if 'Unnamed:' in str(col)]
            if unnamed_cols:
                print(f"  ⚠️  Found {len(unnamed_cols)} unnamed columns: {unnamed_cols}")
            
            empty_cols = [col for col in df.columns if df[col].isna().all()]
            if empty_cols:
                print(f"  ⚠️  Found {len(empty_cols)} completely empty columns: {empty_cols}")
            
            single_value_cols = [col for col in df.columns if df[col].nunique() == 1]
            if single_value_cols:
                print(f"  ⚠️  Found {len(single_value_cols)} single-value columns: {single_value_cols}")
                for col in single_value_cols:
                    value = df[col].iloc[0]
                    print(f"      {col}: '{value}'")
            
            # Check for duplicate column names
            duplicate_cols = df.columns[df.columns.duplicated()].tolist()
            if duplicate_cols:
                print(f"  ⚠️  Found {len(duplicate_cols)} duplicate column names: {duplicate_cols}")
            
        except Exception as e:
            print(f"❌ Error analyzing {raw_file.name}: {e}")
            import traceback
            traceback.print_exc()
    
    # Now check what Maryland processed file looks like
    if maryland_processed_files:
        print(f"\n" + "="*80)
        print(f"📊 MARYLAND PROCESSED FILE ANALYSIS")
        print("="*80)
        
        processed_file = maryland_processed_files[0]
        try:
            df_processed = pd.read_excel(processed_file)
            print(f"📁 Processed file: {processed_file.name}")
            print(f"📊 Shape: {df_processed.shape}")
            print(f"📊 Columns: {len(df_processed.columns)}")
            print(f"📊 Columns: {sorted(df_processed.columns)}")
            
            # Check if any original columns were preserved
            print(f"\n🔍 Checking for preserved original columns:")
            original_cols = [col for col in df_processed.columns if col.startswith('original_')]
            if original_cols:
                print(f"  ✅ Found {len(original_cols)} original columns: {original_cols}")
            else:
                print(f"  ❌ No original columns preserved")
                
        except Exception as e:
            print(f"❌ Error reading processed file: {e}")

if __name__ == "__main__":
    investigate_maryland()
