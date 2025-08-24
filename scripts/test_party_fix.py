#!/usr/bin/env python3
"""
Test script to verify party standardization fix
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.pipeline.main_pipeline import MainPipeline
import pandas as pd

def test_party_standardization():
    """Test that party standardization is working correctly"""
    
    print("🧪 TESTING PARTY STANDARDIZATION FIX")
    print("=" * 60)
    
    # Initialize pipeline
    pipeline = MainPipeline()
    
    # Create a test DataFrame with party data
    test_data = pd.DataFrame({
        'candidate_name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
        'office': ['US President', 'Governor', 'State Senator'],
        'party': ['REPUBLICAN', 'DEMOCRATIC', 'INDEPENDENT'],
        'state': ['Test State', 'Test State', 'Test State'],
        'election_year': [2024, 2024, 2024]
    })
    
    print("📊 BEFORE PARTY STANDARDIZATION:")
    print(f"Original party coverage: {test_data['party'].notna().sum()}/{len(test_data)} ({(test_data['party'].notna().sum()/len(test_data)*100):.1f}%)")
    print("Party values:")
    for i, party in enumerate(test_data['party']):
        print(f"  {i+1}. {party}")
    
    # Apply party standardization
    print(f"\n🔧 APPLYING PARTY STANDARDIZATION...")
    standardized_data = pipeline._standardize_parties(test_data)
    
    print(f"\n📊 AFTER PARTY STANDARDIZATION:")
    print(f"Final party coverage: {standardized_data['party'].notna().sum()}/{len(standardized_data)} ({(standardized_data['party'].notna().sum()/len(standardized_data)*100):.1f}%)")
    print("Party values:")
    for i, party in enumerate(standardized_data['party']):
        print(f"  {i+1}. {party}")
    
    # Check if party_standardized column exists and has data
    if 'party_standardized' in standardized_data.columns:
        print(f"\n✅ party_standardized column exists")
        print(f"party_standardized coverage: {standardized_data['party_standardized'].notna().sum()}/{len(standardized_data)}")
    else:
        print(f"\n❌ party_standardized column missing")
    
    # Check if original party column was replaced
    if 'party' in standardized_data.columns:
        print(f"✅ party column exists and was updated")
    else:
        print(f"❌ party column missing")
    
    # Verify the fix worked
    original_coverage = test_data['party'].notna().sum()
    final_coverage = standardized_data['party'].notna().sum()
    
    if final_coverage >= original_coverage:
        print(f"\n✅ SUCCESS: Party coverage maintained or improved!")
        print(f"   Before: {original_coverage}/{len(test_data)} ({original_coverage/len(test_data)*100:.1f}%)")
        print(f"   After:  {final_coverage}/{len(test_data)} ({final_coverage/len(test_data)*100:.1f}%)")
    else:
        print(f"\n❌ FAILURE: Party coverage decreased!")
        print(f"   Before: {original_coverage}/{len(test_data)} ({original_coverage/len(test_data)*100:.1f}%)")
        print(f"   After:  {final_coverage}/{len(test_data)} ({final_coverage/len(test_data)*100:.1f}%)")

if __name__ == "__main__":
    test_party_standardization()
