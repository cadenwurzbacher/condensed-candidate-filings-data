#!/usr/bin/env python3
"""
Test Election Type Standardization

This script demonstrates how election types are converted from strings
to binary columns for better data quality and querying.
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
from src.pipeline.election_type_standardizer import ElectionTypeStandardizer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_data():
    """Create test data with various election types"""
    
    test_data = [
        # Simple cases
        {'candidate_name': 'John Smith', 'election_type': 'Primary'},
        {'candidate_name': 'Jane Doe', 'election_type': 'General'},
        {'candidate_name': 'Bob Wilson', 'election_type': 'Special'},
        
        # Multiple election types
        {'candidate_name': 'Alice Johnson', 'election_type': 'Primary, General'},
        {'candidate_name': 'Charlie Brown', 'election_type': 'Primary; General'},
        {'candidate_name': 'Diana Prince', 'election_type': 'Primary, General, Special'},
        
        # Edge cases
        {'candidate_name': 'Unknown Candidate', 'election_type': None},
        {'candidate_name': 'Empty Candidate', 'election_type': ''},
        {'candidate_name': 'Mixed Case', 'election_type': 'PRIMARY, general'},
        
        # Variations
        {'candidate_name': 'Pri Candidate', 'election_type': 'Pri'},
        {'candidate_name': 'Gen Candidate', 'election_type': 'Gen'},
        {'candidate_name': 'Spec Candidate', 'election_type': 'Spec'},
        
        # Complex cases
        {'candidate_name': 'Complex Case 1', 'election_type': 'Presidential Primary, General Election'},
        {'candidate_name': 'Complex Case 2', 'election_type': 'State Primary; November General'},
        {'candidate_name': 'Complex Case 3', 'election_type': 'Special Election to Fill Vacancy'},
    ]
    
    return pd.DataFrame(test_data)

def test_election_type_standardization():
    """Test the election type standardization"""
    
    print("🧪 TESTING ELECTION TYPE STANDARDIZATION")
    print("=" * 60)
    
    # Create test data
    test_df = create_test_data()
    print(f"📊 Original test data: {len(test_df)} records")
    print("\n📋 Sample records:")
    print(test_df[['candidate_name', 'election_type']].to_string())
    
    # Initialize ElectionTypeStandardizer
    standardizer = ElectionTypeStandardizer()
    
    # Apply standardization
    print(f"\n🔧 Standardizing election types...")
    result_df = standardizer.standardize_election_types(test_df)
    
    print(f"\n📊 Results after standardization:")
    print(f"  Original records: {len(test_df)}")
    print(f"  Final records: {len(result_df)}")
    
    # Show the new binary columns
    print(f"\n📋 New binary election type columns:")
    binary_cols = ['ran_in_primary', 'ran_in_general', 'ran_in_special']
    display_cols = ['candidate_name'] + binary_cols + ['election_type_notes']
    print(result_df[display_cols].to_string())
    
    # Get summary statistics
    summary = standardizer.get_election_summary(result_df)
    print(f"\n📊 Election Type Summary:")
    print(f"  Primary candidates: {summary.get('primary', 0)}")
    print(f"  General candidates: {summary.get('general', 0)}")
    print(f"  Special candidates: {summary.get('special', 0)}")
    print(f"  Primary + General: {summary.get('primary_and_general', 0)}")
    print(f"  Primary + Special: {summary.get('primary_and_special', 0)}")
    print(f"  General + Special: {summary.get('general_and_special', 0)}")
    
    # Show specific examples
    print(f"\n🔍 Specific Examples:")
    
    # Show candidates who ran in multiple election types
    multi_type = result_df[
        (result_df['ran_in_primary'] & result_df['ran_in_general']) |
        (result_df['ran_in_primary'] & result_df['ran_in_special']) |
        (result_df['ran_in_general'] & result_df['ran_in_special'])
    ]
    
    if not multi_type.empty:
        print(f"  Candidates in multiple election types:")
        for _, row in multi_type.iterrows():
            types = []
            if row['ran_in_primary']: types.append('Primary')
            if row['ran_in_general']: types.append('General')
            if row['ran_in_special']: types.append('Special')
            print(f"    {row['candidate_name']}: {', '.join(types)}")
    else:
        print(f"  No candidates in multiple election types")
    
    # Show notes for complex cases
    print(f"\n📝 Notes for complex cases:")
    notes_df = result_df[result_df['election_type_notes'].str.len() > 0]
    if not notes_df.empty:
        for _, row in notes_df.iterrows():
            print(f"  {row['candidate_name']}: {row['election_type_notes']}")
    else:
        print(f"  No complex cases with notes")
    
    print(f"\n🎯 Key Benefits:")
    print(f"  • Easy querying: WHERE ran_in_primary = true")
    print(f"  • Clear semantics: Each column has one purpose")
    print(f"  • Flexible: Can run in multiple election types")
    print(f"  • UI friendly: Simple checkboxes/filters")
    print(f"  • No parsing needed: Direct boolean values")

def main():
    """Main function to run the test"""
    test_election_type_standardization()

if __name__ == "__main__":
    main()
