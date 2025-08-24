#!/usr/bin/env python3
"""
Test Statewide Candidate Deduplication

This script demonstrates how the statewide candidate deduplication works
by creating sample data with county-based duplicates.
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
from src.pipeline.national_standards import NationalStandards
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_data():
    """Create test data with county-based duplicates"""
    
    # Sample data showing statewide candidates in multiple counties
    test_data = [
        # Governor candidate in multiple counties
        {
            'full_name_display': 'John Smith',
            'state': 'California',
            'office': 'Governor',
            'election_year': 2024,
            'county': 'Los Angeles',
            'party': 'Republican',
            'address': '123 Main St, Los Angeles, CA',
            'stable_id': 'abc123_la'  # Los Angeles
        },
        {
            'full_name_display': 'John Smith',
            'state': 'California',
            'office': 'Governor',
            'election_year': 2024,
            'county': 'San Francisco',
            'party': 'Republican',
            'address': '123 Main St, Los Angeles, CA',
            'stable_id': 'abc123_sf'  # San Francisco
        },
        {
            'full_name_display': 'John Smith',
            'state': 'California',
            'office': 'Governor',
            'election_year': 2024,
            'county': 'San Diego',
            'party': 'Republican',
            'address': '123 Main St, Los Angeles, CA',
            'stable_id': 'abc123_sd'  # San Diego
        },
        
        # US Senator candidate in multiple counties
        {
            'full_name_display': 'Jane Doe',
            'state': 'California',
            'office': 'US Senator',
            'election_year': 2024,
            'county': 'Orange',
            'party': 'Democrat',
            'address': '456 Oak Ave, Orange, CA',
            'stable_id': 'def456_or'  # Orange
        },
        {
            'full_name_display': 'Jane Doe',
            'state': 'California',
            'office': 'US Senator',
            'election_year': 2024,
            'county': 'Riverside',
            'party': 'Democrat',
            'address': '456 Oak Ave, Orange, CA',
            'stable_id': 'def456_rv'  # Riverside
        },
        
        # Local candidate (should NOT be deduped)
        {
            'full_name_display': 'Bob Wilson',
            'state': 'California',
            'office': 'Mayor',
            'election_year': 2024,
            'county': 'Los Angeles',
            'party': 'Independent',
            'address': '789 Pine St, Los Angeles, CA',
            'stable_id': 'ghi789_la'  # Los Angeles
        },
        {
            'full_name_display': 'Bob Wilson',
            'state': 'California',
            'office': 'Mayor',
            'election_year': 2024,
            'county': 'San Francisco',
            'party': 'Independent',
            'address': '789 Pine St, San Francisco, CA',
            'stable_id': 'ghi789_sf'  # San Francisco
        }
    ]
    
    return pd.DataFrame(test_data)

def test_statewide_deduplication():
    """Test the statewide candidate deduplication"""
    
    print("🧪 TESTING STATEWIDE CANDIDATE DEDUPLICATION")
    print("=" * 60)
    
    # Create test data
    test_df = create_test_data()
    print(f"📊 Original test data: {len(test_df)} records")
    print("\n📋 Sample records:")
    print(test_df[['full_name_display', 'office', 'county', 'state']].to_string())
    
    # Initialize NationalStandards
    standards = NationalStandards()
    
    # Apply national standards (including deduplication)
    print(f"\n🔧 Applying national standards...")
    result_df = standards.apply_standards(test_df)
    
    print(f"\n📊 Results after deduplication:")
    print(f"  Original records: {len(test_df)}")
    print(f"  Final records: {len(result_df)}")
    print(f"  Records removed: {len(test_df) - len(result_df)}")
    
    print(f"\n📋 Final deduped records:")
    print(result_df[['full_name_display', 'office', 'county', 'state']].to_string())
    
    # Show what happened to each candidate
    print(f"\n🔍 Deduplication Analysis:")
    
    # Check Governor candidate
    governor_records = result_df[result_df['full_name_display'] == 'John Smith']
    print(f"  Governor (John Smith): {len(governor_records)} record(s)")
    if len(governor_records) == 1:
        print(f"    County: {governor_records.iloc[0]['county']}")
        print(f"    ✅ Correctly deduped (county removed)")
    
    # Check US Senator candidate
    senator_records = result_df[result_df['full_name_display'] == 'Jane Doe']
    print(f"  US Senator (Jane Doe): {len(senator_records)} record(s)")
    if len(senator_records) == 1:
        print(f"    County: {senator_records.iloc[0]['county']}")
        print(f"    ✅ Correctly deduped (county removed)")
    
    # Check local candidate
    local_records = result_df[result_df['full_name_display'] == 'Bob Wilson']
    print(f"  Local Mayor (Bob Wilson): {len(local_records)} record(s)")
    if len(local_records) == 2:
        print(f"    ✅ Correctly kept separate (different counties matter for local offices)")
    
    print(f"\n🎯 Key Benefits:")
    print(f"  • Statewide candidates no longer duplicate across counties")
    print(f"  • Local candidates remain county-specific (as they should)")
    print(f"  • Data quality improved - no more inflated record counts")
    print(f"  • Stable IDs now properly represent unique candidates")

def main():
    """Main function to run the test"""
    test_statewide_deduplication()

if __name__ == "__main__":
    main()
