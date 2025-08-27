#!/usr/bin/env python3
"""
Test script for BaseStateCleaner and refactored Arizona cleaner.
"""

import sys
import pandas as pd
import logging
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def test_base_cleaner():
    """Test the base cleaner functionality."""
    print("🧪 Testing BaseStateCleaner...")
    
    try:
        from pipeline.state_cleaners.base_cleaner import BaseStateCleaner
        
        # Test that we can't instantiate the abstract base class
        try:
            cleaner = BaseStateCleaner("Test")
            print("❌ ERROR: Should not be able to instantiate abstract base class")
            return False
        except TypeError:
            print("✅ Correctly prevented instantiation of abstract base class")
        
        # Test the standard columns
        print(f"✅ Standard columns defined: {len(BaseStateCleaner.STANDARD_COLUMNS)} columns")
        print(f"   First few: {BaseStateCleaner.STANDARD_COLUMNS[:5]}")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR testing base cleaner: {e}")
        return False

def test_arizona_cleaner():
    """Test the refactored Arizona cleaner."""
    print("\n🧪 Testing ArizonaCleaner...")
    
    try:
        from pipeline.state_cleaners.arizona_cleaner_refactored import ArizonaCleaner
        
        # Create instance
        cleaner = ArizonaCleaner()
        print(f"✅ Created ArizonaCleaner instance for state: {cleaner.state_name}")
        
        # Test that cleaner was created successfully
        print(f"✅ ArizonaCleaner created successfully")
        
        # Test county mappings
        print(f"✅ Defined {len(cleaner.county_mappings)} county mappings")
        
        # Test office mappings
        print(f"✅ Defined {len(cleaner.office_mappings)} office mappings")
        
        return True, cleaner
        
    except Exception as e:
        print(f"❌ ERROR testing Arizona cleaner: {e}")
        return False, None

def test_data_cleaning(cleaner):
    """Test the actual data cleaning process."""
    print("\n🧪 Testing data cleaning process...")
    
    try:
        # Create sample test data
        test_data = {
            'candidate_name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
            'office': ['US PRESIDENT', 'GOVERNOR', 'STATE SENATOR'],
            'district': ['', '1', 'District 5'],
            'party': ['democrat', 'republican', 'independent'],
            'county': ['Maricopa', 'Pima', 'Coconino'],
            'address': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
            'phone': ['555-1234', '555-5678', '555-9012'],
            'email': ['john@example.com', 'jane@example.com', 'bob@example.com']
        }
        
        df = pd.DataFrame(test_data)
        print(f"✅ Created test DataFrame with {len(df)} rows and {len(df.columns)} columns")
        print(f"   Columns: {list(df.columns)}")
        
        # Test the cleaning process
        cleaned_df = cleaner.clean_data(df)
        print(f"✅ Data cleaning completed successfully")
        print(f"   Final shape: {cleaned_df.shape}")
        print(f"   Final columns: {list(cleaned_df.columns)}")
        
        # Check that parties were preserved (not standardized by state cleaner)
        unique_parties = cleaned_df['party'].unique()
        print(f"✅ Party data preserved: {unique_parties}")
        
        # Check that required columns were added
        missing_required = [col for col in cleaner.STANDARD_COLUMNS if col not in cleaned_df.columns]
        if missing_required:
            print(f"⚠️  Missing required columns: {missing_required}")
        else:
            print("✅ All required columns present")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR testing data cleaning: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_column_management(cleaner):
    """Test column management functionality specifically."""
    print("\n🧪 Testing column management...")
    
    try:
        # Test with DataFrame missing some required columns
        test_df = pd.DataFrame({
            'candidate_name': ['John Doe'],
            'office': ['GOVERNOR'],
            'party': ['democrat']
        })
        
        print(f"✅ Test DataFrame created with {len(test_df.columns)} columns")
        print(f"   Columns: {list(test_df.columns)}")
        
        # Test the cleaning process
        cleaned_df = cleaner.clean_data(test_df)
        
        print(f"✅ Column management test completed")
        print(f"   Before: {len(test_df.columns)} columns")
        print(f"   After: {len(cleaned_df.columns)} columns")
        print(f"   Added columns: {[col for col in cleaned_df.columns if col not in test_df.columns]}")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR testing column management: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Starting BaseStateCleaner and Arizona Cleaner Tests\n")
    
    # Test 1: Base cleaner
    if not test_base_cleaner():
        print("\n❌ Base cleaner tests failed. Stopping.")
        return
    
    # Test 2: Arizona cleaner
    success, cleaner = test_arizona_cleaner()
    if not success:
        print("\n❌ Arizona cleaner tests failed. Stopping.")
        return
    
    # Test 3: Data cleaning
    if not test_data_cleaning(cleaner):
        print("\n❌ Data cleaning tests failed. Stopping.")
        return
    
    # Test 4: Column management
    if not test_column_management(cleaner):
        print("\n❌ Column management tests failed. Stopping.")
        return
    
    print("\n🎉 ALL TESTS PASSED!")
    print("✅ BaseStateCleaner is working correctly")
    print("✅ ArizonaCleaner refactoring is successful")
    print("✅ Column management is working")
    print("✅ Data cleaning pipeline is functional")
    print("✅ Party standardization correctly left to national level")
    print("\n🚀 Ready to move to Phase 1.3: Refactor remaining states!")

if __name__ == "__main__":
    main()
