#!/usr/bin/env python3
"""
Office Mapping Analysis Script

This script analyzes all unique offices in the dataset and creates mapping recommendations
for standardization to the target office names.
"""

import pandas as pd
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
    print(f"✅ Loaded {len(df):,} records")
    return df

def analyze_office_distribution(df):
    """Analyze the distribution of offices"""
    print("\n" + "="*80)
    print("OFFICE DISTRIBUTION ANALYSIS")
    print("="*80)
    
    office_counts = df['office'].value_counts()
    print(f"Total unique offices: {len(office_counts):,}")
    print(f"Total records: {len(df):,}")
    
    print(f"\nTop 20 most common offices:")
    print("-" * 60)
    for office, count in office_counts.head(20).items():
        print(f"{count:>8,} | {office}")
    
    return office_counts

def categorize_offices(office_counts):
    """Categorize offices for mapping recommendations"""
    print("\n" + "="*80)
    print("OFFICE CATEGORIZATION & MAPPING RECOMMENDATIONS")
    print("="*80)
    
    # Target office names
    target_offices = {
        'US President': [],
        'US House': [],
        'US Senate': [],
        'State House': [],
        'State Senate': [],
        'Governor': [],
        'State Attorney General': [],
        'State Treasurer': [],
        'Lieutenant Governor': [],
        'Secretary of State': [],
        'City Council': [],
        'City Commission': [],
        'County Commission': [],
        'School Board': []
    }
    
    # Patterns for each target office
    patterns = {
        'US President': [
            r'President.*United States',
            r'US President',
            r'U\.s\. President',
            r'Presidential',
            r'President.*Vice President'
        ],
        'US House': [
            r'House.*Representatives.*United States',
            r'US House.*Representatives',
            r'U\.s\. House.*Representatives',
            r'United States.*House'
        ],
        'US Senate': [
            r'Senate.*United States',
            r'US Senate',
            r'U\.s\. Senate',
            r'United States.*Senate'
        ],
        'State House': [
            r'State.*Representative',
            r'State.*House.*Representatives',
            r'State.*House',
            r'House.*Representatives',
            r'Representative.*State'
        ],
        'State Senate': [
            r'State.*Senate',
            r'Senate.*State'
        ],
        'Governor': [
            r'Governor',
            r'Gubernatorial'
        ],
        'State Attorney General': [
            r'Attorney General',
            r'State.*Attorney General',
            r'Attorney General.*State'
        ],
        'State Treasurer': [
            r'Treasurer',
            r'State.*Treasurer',
            r'Treasurer.*State'
        ],
        'Lieutenant Governor': [
            r'Lieutenant Governor',
            r'Lt\. Governor',
            r'Lt Governor'
        ],
        'Secretary of State': [
            r'Secretary.*State',
            r'State.*Secretary'
        ],
        'City Council': [
            r'City Council.*Member',
            r'Council.*Member',
            r'City Council',
            r'Alderman',
            r'Alderwoman'
        ],
        'City Commission': [
            r'City Commissioner',
            r'City Commission',
            r'Commissioner.*City'
        ],
        'County Commission': [
            r'County Commissioner',
            r'County Commission',
            r'Commissioner.*County'
        ],
        'School Board': [
            r'School Board.*Member',
            r'Board.*Member.*School',
            r'School Board',
            r'Ind School Board.*Member'
        ]
    }
    
    # Categorize each office
    categorized = {}
    uncategorized = []
    
    for office in office_counts.index:
        categorized_flag = False
        
        for target, pattern_list in patterns.items():
            for pattern in pattern_list:
                if re.search(pattern, office, re.IGNORECASE):
                    if target not in categorized:
                        categorized[target] = []
                    categorized[target].append(office)
                    categorized_flag = True
                    break
            if categorized_flag:
                break
        
        if not categorized_flag:
            uncategorized.append(office)
    
    # Display categorization results
    for target in target_offices.keys():
        if target in categorized:
            print(f"\n{target.upper()}:")
            print("-" * 50)
            offices = categorized[target]
            total_records = sum(office_counts[office] for office in offices)
            print(f"Total records: {total_records:,}")
            print("Matching offices:")
            for office in sorted(offices):
                count = office_counts[office]
                print(f"  {count:>6,} | {office}")
        else:
            print(f"\n{target.upper()}:")
            print("-" * 50)
            print("No matching offices found")
    
    # Show uncategorized offices
    print(f"\n{'UNCATEGORIZED OFFICES'.upper()}:")
    print("-" * 50)
    print(f"Total uncategorized offices: {len(uncategorized):,}")
    
    # Group uncategorized by record count
    uncategorized_counts = [(office, office_counts[office]) for office in uncategorized]
    uncategorized_counts.sort(key=lambda x: x[1], reverse=True)
    
    print("\nTop 30 uncategorized offices by record count:")
    for office, count in uncategorized_counts[:30]:
        print(f"  {count:>6,} | {office}")
    
    return categorized, uncategorized

def create_mapping_recommendations(categorized, office_counts):
    """Create specific mapping recommendations"""
    print("\n" + "="*80)
    print("MAPPING RECOMMENDATIONS")
    print("="*80)
    
    print("Recommended office mappings for standardization:")
    print("-" * 80)
    
    for target, offices in categorized.items():
        if offices:
            print(f"\n{target}:")
            for office in sorted(offices):
                count = office_counts[office]
                print(f"  '{office}' -> '{target}'  # {count:,} records")

def main():
    """Main analysis function"""
    print("🔍 OFFICE MAPPING ANALYSIS")
    print("="*80)
    
    # Load data
    df = load_data()
    if df is None:
        return
    
    # Analyze office distribution
    office_counts = analyze_office_distribution(df)
    
    # Categorize offices
    categorized, uncategorized = categorize_offices(office_counts)
    
    # Create mapping recommendations
    create_mapping_recommendations(categorized, office_counts)
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()
