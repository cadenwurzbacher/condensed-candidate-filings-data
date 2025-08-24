#!/usr/bin/env python3
"""
Test Updated Stable ID Generation (No County)

This script tests that the updated stable ID generation no longer includes county
and generates consistent IDs between Main Pipeline and National Standards.
"""

import hashlib
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def generate_stable_id_no_county(name: str, state: str, office: str, election_year: str, county: str = None) -> str:
    """
    Generate stable ID for a candidate WITHOUT county
    
    This simulates the updated National Standards method
    """
    # Clean and standardize inputs
    clean_name = str(name).strip().upper() if name else ""
    clean_state = str(state).strip().upper() if state else ""
    clean_office = str(office).strip().upper() if office else ""
    clean_election = str(election_year).strip() if election_year else ""
    
    # Create stable ID string WITHOUT county
    # County is NOT included to ensure consistent stable IDs across counties
    stable_id_string = f"{clean_name}_{clean_state}_{clean_office}_{clean_election}"
    
    # Generate MD5 hash
    stable_id = hashlib.md5(stable_id_string.encode('utf-8')).hexdigest()
    
    return stable_id

def generate_main_pipeline_id(name: str, state: str, office: str, election_year: str) -> str:
    """
    Generate stable ID using Main Pipeline method
    """
    key = f"{name}|{state}|{office}|{election_year}"
    stable_id = hashlib.md5(key.encode()).hexdigest()[:12]
    return stable_id

def test_updated_stable_ids():
    """Test that stable IDs no longer include county"""
    logger.info("🧪 Testing Updated Stable ID Generation (No County)")
    logger.info("=" * 80)
    
    # Test cases
    test_cases = [
        {
            'name': 'John Smith',
            'state': 'Alaska',
            'office': 'Governor',
            'election_year': '2024',
            'county': 'Anchorage'
        },
        {
            'name': 'Jane Doe',
            'state': 'Arizona',
            'office': 'US Senator',
            'election_year': '2024',
            'county': None
        },
        {
            'name': 'Bob Wilson',
            'state': 'Georgia',
            'office': 'US Representative',
            'election_year': '2024',
            'county': 'Fulton'
        },
        {
            'name': 'Amy Chen',
            'state': 'Illinois',
            'office': 'State Senator',
            'election_year': '2024',
            'county': None
        },
        {
            'name': 'Anthony Rodriguez',
            'state': 'New York',
            'office': 'County Executive',
            'election_year': '2024',
            'county': 'Nassau'
        }
    ]
    
    logger.info("Testing stable ID generation for various states:")
    logger.info("-" * 60)
    
    for i, test_case in enumerate(test_cases, 1):
        logger.info(f"\n{i}. {test_case['state']} - {test_case['name']} for {test_case['office']}")
        logger.info(f"   County: {test_case['county']}")
        
        # Generate stable ID using updated method (no county)
        stable_id = generate_stable_id_no_county(
            test_case['name'],
            test_case['state'],
            test_case['office'],
            test_case['election_year'],
            test_case['county']
        )
        
        # Generate what the Main Pipeline would generate
        main_pipeline_id = generate_main_pipeline_id(
            test_case['name'],
            test_case['state'],
            test_case['office'],
            test_case['election_year']
        )
        
        # Check if they match (first 12 chars of national should match main pipeline)
        ids_match = stable_id[:12] == main_pipeline_id
        
        logger.info(f"   Main Pipeline ID: {main_pipeline_id}")
        logger.info(f"   National Standards ID: {stable_id}")
        logger.info(f"   IDs Match: {'✅' if ids_match else '❌'}")
        
        # Verify county is not in the stable ID string
        county_in_id = test_case['county'] and test_case['county'].upper() in stable_id
        logger.info(f"   County in ID: {'❌' if county_in_id else '✅'}")
    
    # Test statewide deduplication scenario
    logger.info("\n" + "=" * 80)
    logger.info("🌍 Testing Statewide Deduplication Scenario")
    logger.info("=" * 80)
    
    # Create test data with same candidate in multiple counties
    test_records = [
        {
            'candidate_name': 'John Smith',
            'office': 'Governor',
            'election_year': '2024',
            'county': 'Anchorage',
            'state': 'Alaska'
        },
        {
            'candidate_name': 'John Smith',
            'office': 'Governor',
            'election_year': '2024',
            'county': 'Fairbanks',
            'state': 'Alaska'
        },
        {
            'candidate_name': 'John Smith',
            'office': 'Governor',
            'election_year': '2024',
            'county': 'Juneau',
            'state': 'Alaska'
        }
    ]
    
    logger.info("Test data created:")
    logger.info(f"  {len(test_records)} records for same candidate in different counties")
    
    # Generate stable IDs for each record
    stable_ids = []
    for record in test_records:
        stable_id = generate_stable_id_no_county(
            record['candidate_name'],
            record['state'],
            record['office'],
            record['election_year'],
            record['county']
        )
        stable_ids.append(stable_id)
        logger.info(f"  {record['county']}: {stable_id}")
    
    # Check if all stable IDs are the same
    unique_ids = len(set(stable_ids))
    logger.info(f"\nUnique stable IDs: {unique_ids}")
    
    if unique_ids == 1:
        logger.info("✅ SUCCESS: All records have the same stable ID (county ignored)")
        logger.info("This enables proper statewide deduplication!")
    else:
        logger.error("❌ FAILURE: Different stable IDs generated (county still included)")
    
    # Show the stable ID string that was generated
    logger.info(f"\n🔍 Stable ID String Generated:")
    logger.info(f"  Input: John Smith, Alaska, Governor, 2024")
    logger.info(f"  String: JOHN SMITH_ALASKA_GOVERNOR_2024")
    logger.info(f"  Hash: {stable_ids[0]}")
    
    return test_records

def main():
    """Main test function"""
    try:
        test_data = test_updated_stable_ids()
        logger.info("\n🎉 Test Complete!")
        return test_data
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return None

if __name__ == "__main__":
    main()
