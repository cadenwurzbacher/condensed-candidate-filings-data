#!/usr/bin/env python3
"""
Data Audit Script for CandidateFilings Pipeline

This script performs a comprehensive audit of the processed data to check:
1. Column completeness
2. Office standardization quality
3. Name parsing quality
4. District number issues in office names
"""

import pandas as pd
import json
import re
from pathlib import Path

def load_data():
    """Load the final processed data"""
    final_file = Path("data/final/candidate_filings_FINAL_20250827_225338.xlsx")
    if not final_file.exists():
        print(f"❌ Final data file not found: {final_file}")
        return None
    
    print(f"📊 Loading data from: {final_file}")
    df = pd.read_excel(final_file)
    print(f"✅ Loaded {len(df):,} records with {len(df.columns)} columns")
    return df

def analyze_column_completeness(df):
    """Analyze how complete each column is"""
    print("\n" + "="*80)
    print("COLUMN COMPLETENESS ANALYSIS")
    print("="*80)
    
    # Calculate completeness
    completeness = df.isnull().sum().sort_values(ascending=False)
    total_records = len(df)
    
    print(f"{'Column':<30} {'Missing':<10} {'Complete %':<12} {'Sample Values'}")
    print("-" * 80)
    
    for col in df.columns:
        missing = completeness[col]
        complete_pct = ((total_records - missing) / total_records) * 100
        
        # Get sample values (non-null)
        sample_values = df[col].dropna().head(3).tolist()
        sample_str = ", ".join([str(v)[:20] + "..." if len(str(v)) > 20 else str(v) for v in sample_values])
        
        print(f"{col:<30} {missing:<10,} {complete_pct:<11.1f}% {sample_str}")

def analyze_office_standardization(df):
    """Analyze office standardization quality"""
    print("\n" + "="*80)
    print("OFFICE STANDARDIZATION ANALYSIS")
    print("="*80)
    
    # Check for the specific office types mentioned
    office_types = {
        'US President': r'President.*United States|US President|U\.s\. President',
        'US House': r'House.*Representatives.*United States|US House|U\.s\. House',
        'US Senate': r'Senate.*United States|US Senate|U\.s\. Senate',
        'State House': r'State.*House|State.*Representative',
        'State Senate': r'State.*Senate'
    }
    
    for office_type, pattern in office_types.items():
        print(f"\n{office_type.upper()} OFFICES:")
        print("-" * 50)
        
        matches = df[df['office'].str.contains(pattern, case=False, na=False)]
        if len(matches) > 0:
            print(f"Total records: {len(matches):,}")
            
            # Check for district numbers in office names
            district_issues = matches[matches['office'].str.contains(r'\d', na=False)]
            if len(district_issues) > 0:
                print(f"⚠️  {len(district_issues):,} records have district numbers in office name:")
                for office, count in district_issues['office'].value_counts().head(5).items():
                    print(f"   {office}: {count:,}")
            else:
                print("✅ No district numbers found in office names")
            
            # Show top variations
            print(f"Top variations:")
            for office, count in matches['office'].value_counts().head(5).items():
                print(f"   {office}: {count:,}")
        else:
            print("No records found")

def analyze_name_parsing(df):
    """Analyze name parsing quality"""
    print("\n" + "="*80)
    print("NAME PARSING ANALYSIS")
    print("="*80)
    
    # Check states with most records
    print("Top 10 states by record count:")
    state_counts = df['state'].value_counts().head(10)
    for state, count in state_counts.items():
        print(f"   {state}: {count:,}")
    
    # Analyze name parsing for specific states
    sample_states = ['Alaska', 'Kentucky', 'North_Carolina', 'Illinois']
    
    for state in sample_states:
        print(f"\n{state.upper()} NAME PARSING SAMPLE:")
        print("-" * 50)
        
        state_data = df[df['state'] == state].head(5)
        if len(state_data) > 0:
            print(f"Records found: {len(state_data)}")
            
            for idx, row in state_data.iterrows():
                print(f"\nRecord {idx}:")
                print(f"  Full name display: {row['full_name_display']}")
                
                # Try to parse raw data
                if pd.notna(row['raw_data']):
                    try:
                        raw_data = json.loads(row['raw_data'])
                        raw_name = raw_data.get('Name', 'N/A')
                        print(f"  Raw data name: {raw_name}")
                        
                        # Check if names match
                        if str(row['full_name_display']) == str(raw_name):
                            print("  ✅ Names match")
                        else:
                            print("  ❌ Names don't match")
                            
                    except json.JSONDecodeError:
                        print(f"  Raw data (first 100 chars): {str(row['raw_data'])[:100]}...")
                else:
                    print("  Raw data: None")
        else:
            print("No records found for this state")

def analyze_district_issues(df):
    """Analyze district number issues in office names"""
    print("\n" + "="*80)
    print("DISTRICT NUMBER ISSUES IN OFFICE NAMES")
    print("="*80)
    
    # Find offices with district numbers
    district_offices = df[df['office'].str.contains(r'\d', na=False)]
    print(f"Total offices with district numbers: {len(district_offices):,}")
    
    # Check specific problematic patterns
    problematic_patterns = [
        r'US House.*District',
        r'US Senate.*District', 
        r'State House.*District',
        r'State Senate.*District'
    ]
    
    for pattern in problematic_patterns:
        matches = df[df['office'].str.contains(pattern, case=False, na=False)]
        if len(matches) > 0:
            print(f"\n⚠️  {pattern} - {len(matches):,} records:")
            for office, count in matches['office'].value_counts().head(5).items():
                print(f"   {office}: {count:,}")

def main():
    """Main audit function"""
    print("🔍 CANDIDATE FILINGS DATA AUDIT")
    print("="*80)
    
    # Load data
    df = load_data()
    if df is None:
        return
    
    # Run analyses
    analyze_column_completeness(df)
    analyze_office_standardization(df)
    analyze_name_parsing(df)
    analyze_district_issues(df)
    
    print("\n" + "="*80)
    print("AUDIT COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()
