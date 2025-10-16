#!/usr/bin/env python3
"""
Test script for Indiana scraper to verify data quality
"""

import pandas as pd
import os
from datetime import datetime

def test_indiana_data():
    """Test the Indiana candidate data for quality and completeness."""
    
    # Find the most recent Excel file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    excel_files = [f for f in os.listdir(script_dir) if f.endswith('.xlsx') and 'indiana_candidates' in f]
    
    if not excel_files:
        print("No Indiana candidate Excel files found!")
        return
    
    # Get the most recent file
    latest_file = max(excel_files, key=lambda x: os.path.getctime(os.path.join(script_dir, x)))
    file_path = os.path.join(script_dir, latest_file)
    
    print(f"Testing data from: {latest_file}")
    print("=" * 50)
    
    try:
        # Load the data
        df = pd.read_excel(file_path)
        
        print(f"Total candidates: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        print()
        
        # Basic statistics
        print("Data Quality Check:")
        print("-" * 30)
        
        # Check for missing values
        missing_data = df.isnull().sum()
        print("Missing values per column:")
        for col, missing in missing_data.items():
            if missing > 0:
                print(f"  {col}: {missing} ({missing/len(df)*100:.1f}%)")
        
        print()
        
        # Check year distribution
        if 'Year' in df.columns:
            year_counts = df['Year'].value_counts().sort_index()
            print("Candidates by year:")
            for year, count in year_counts.items():
                print(f"  {year}: {count}")
        
        print()
        
        # Check election type distribution
        if 'Election' in df.columns:
            election_counts = df['Election'].value_counts()
            print("Candidates by election type:")
            for election, count in election_counts.items():
                print(f"  {election}: {count}")
        
        print()
        
        # Check office distribution (top 10)
        if 'Office' in df.columns:
            office_counts = df['Office'].value_counts().head(10)
            print("Top 10 offices:")
            for office, count in office_counts.items():
                print(f"  {office}: {count}")
        
        print()
        
        # Check party distribution
        if 'Party' in df.columns:
            party_counts = df['Party'].value_counts().head(10)
            print("Top 10 parties:")
            for party, count in party_counts.items():
                print(f"  {party}: {count}")
        
        print()
        
        # Sample data
        print("Sample candidates:")
        print("-" * 30)
        sample = df.head(5)
        for idx, row in sample.iterrows():
            print(f"  {row.get('Name', 'N/A')} - {row.get('Office', 'N/A')} ({row.get('Year', 'N/A')})")
        
        print()
        
        # Data validation
        print("Data Validation:")
        print("-" * 30)
        
        # Check for duplicate candidates
        if 'Name' in df.columns and 'Office' in df.columns and 'Year' in df.columns:
            duplicates = df.duplicated(subset=['Name', 'Office', 'Year']).sum()
            print(f"Duplicate candidates: {duplicates}")
        
        # Check for empty names
        if 'Name' in df.columns:
            empty_names = df['Name'].isna().sum() + (df['Name'] == '').sum()
            print(f"Empty names: {empty_names}")
        
        # Check for empty offices
        if 'Office' in df.columns:
            empty_offices = df['Office'].isna().sum() + (df['Office'] == '').sum()
            print(f"Empty offices: {empty_offices}")
        
        print()
        print("Test completed successfully!")
        
    except Exception as e:
        print(f"Error testing data: {e}")

if __name__ == "__main__":
    test_indiana_data() 