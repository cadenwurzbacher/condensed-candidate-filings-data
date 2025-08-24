#!/usr/bin/env python3
"""
Test Stable ID Generation for Each State

This script tests how each state's structural cleaner and stable ID generation
works with their unique column headers and data structures.
"""

import sys
import pandas as pd
import hashlib
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class StateStableIDTester:
    """Test stable ID generation for each state"""
    
    def __init__(self):
        # Test data for each state - simulating what structural cleaners would output
        self.test_data = {
            'Alaska': {
                'candidate_name': 'John Smith',
                'office': 'Governor',
                'party': 'Republican',
                'county': 'Anchorage',
                'district': None,
                'address': '123 Main St',
                'city': 'Anchorage',
                'state': 'Alaska',
                'zip_code': '99501',
                'phone': '907-555-0123',
                'email': 'john@example.com',
                'filing_date': '2024-01-15',
                'election_year': '2024',
                'election_type': 'General',
                'address_state': 'Alaska'
            },
            'Arizona': {
                'candidate_name': 'Jane Doe',
                'office': 'US Senator',
                'party': 'Democrat',
                'county': None,
                'district': None,
                'address': None,
                'city': None,
                'state': 'Arizona',
                'zip_code': None,
                'phone': None,
                'email': None,
                'filing_date': None,
                'election_year': '2024',
                'election_type': 'General',
                'address_state': 'Arizona'
            },
            'Georgia': {
                'candidate_name': 'Bob Wilson',
                'office': 'US Representative',
                'party': 'Independent',
                'county': 'Fulton',
                'district': '5th',
                'address': '456 Oak Ave',
                'city': 'Atlanta',
                'state': 'Georgia',
                'zip_code': '30301',
                'phone': None,
                'email': None,
                'filing_date': None,
                'election_year': '2024',
                'election_type': 'Primary',
                'address_state': 'Georgia'
            },
            'Illinois': {
                'candidate_name': 'Amy Chen',
                'office': 'State Senator',
                'party': 'Democrat',
                'county': None,
                'district': '15th',
                'address': None,
                'city': None,
                'state': 'Illinois',
                'zip_code': None,
                'phone': None,
                'email': 'amy@example.com',
                'filing_date': '2024-02-01',
                'election_year': '2024',
                'election_type': 'General',
                'address_state': 'Illinois'
            },
            'New York': {
                'candidate_name': 'Anthony Rodriguez',
                'office': 'County Executive',
                'party': None,
                'county': 'Nassau',
                'district': None,
                'address': '789 Pine St',
                'city': 'Mineola',
                'state': 'New York',
                'zip_code': None,
                'phone': None,
                'email': None,
                'filing_date': '2024-01-20',
                'election_year': '2024',
                'election_type': 'General',
                'address_state': 'New York'
            }
        }
        
        # Add more states with generic test data
        self.additional_states = [
            'Arkansas', 'Colorado', 'Delaware', 'Idaho', 'Indiana', 'Iowa',
            'Kansas', 'Kentucky', 'Louisiana', 'Maryland', 'Missouri', 'Montana',
            'Nebraska', 'New Mexico', 'North Carolina', 'Oklahoma', 'Oregon',
            'Pennsylvania', 'South Carolina', 'South Dakota', 'Vermont',
            'Virginia', 'Washington', 'West Virginia', 'Wyoming'
        ]
    
    def test_stable_id_generation(self):
        """Test stable ID generation for each state"""
        logger.info("🧪 Testing Stable ID Generation for Each State")
        logger.info("=" * 80)
        
        results = {}
        
        # Test states with specific test data
        for state_name, test_record in self.test_data.items():
            logger.info(f"\n🏛️ Testing {state_name}")
            logger.info("-" * 40)
            
            try:
                # Test main pipeline stable ID generation
                main_pipeline_id = self._test_main_pipeline_id(test_record, state_name)
                
                # Test national standards stable ID generation
                national_standards_id = self._test_national_standards_id(test_record, state_name)
                
                # Store results
                results[state_name] = {
                    'test_record': test_record,
                    'main_pipeline_id': main_pipeline_id,
                    'national_standards_id': national_standards_id,
                    'columns': list(test_record.keys()),
                    'has_county': test_record.get('county') is not None,
                    'has_district': test_record.get('district') is not None,
                    'has_address': test_record.get('address') is not None
                }
                
                # Log results
                logger.info(f"  📋 Columns: {len(test_record)} columns")
                logger.info(f"  🏠 County: {'✅' if test_record.get('county') else '❌'}")
                logger.info(f"  🗺️  District: {'✅' if test_record.get('district') else '❌'}")
                logger.info(f"  📍 Address: {'✅' if test_record.get('address') else '❌'}")
                logger.info(f"  🔑 Main Pipeline ID: {main_pipeline_id}")
                logger.info(f"  🔑 National Standards ID: {national_standards_id}")
                
            except Exception as e:
                logger.error(f"  ❌ Error testing {state_name}: {e}")
                results[state_name] = {'error': str(e)}
        
        # Test additional states with generic data
        for state_name in self.additional_states:
            logger.info(f"\n🏛️ Testing {state_name}")
            logger.info("-" * 40)
            
            try:
                # Create generic test record
                test_record = self._create_generic_test_record(state_name)
                
                # Test stable ID generation
                main_pipeline_id = self._test_main_pipeline_id(test_record, state_name)
                national_standards_id = self._test_national_standards_id(test_record, state_name)
                
                # Store results
                results[state_name] = {
                    'test_record': test_record,
                    'main_pipeline_id': main_pipeline_id,
                    'national_standards_id': national_standards_id,
                    'columns': list(test_record.keys()),
                    'has_county': test_record.get('county') is not None,
                    'has_district': test_record.get('district') is not None,
                    'has_address': test_record.get('address') is not None
                }
                
                # Log results
                logger.info(f"  📋 Columns: {len(test_record)} columns")
                logger.info(f"  🏠 County: {'✅' if test_record.get('county') else '❌'}")
                logger.info(f"  🗺️  District: {'✅' if test_record.get('district') else '❌'}")
                logger.info(f"  📍 Address: {'✅' if test_record.get('address') else '❌'}")
                logger.info(f"  🔑 Main Pipeline ID: {main_pipeline_id}")
                logger.info(f"  🔑 National Standards ID: {national_standards_id}")
                
            except Exception as e:
                logger.error(f"  ❌ Error testing {state_name}: {e}")
                results[state_name] = {'error': str(e)}
        
        return results
    
    def _create_generic_test_record(self, state_name):
        """Create generic test record for a state"""
        return {
            'candidate_name': f'Test Candidate {state_name}',
            'office': 'Test Office',
            'party': 'Test Party',
            'county': 'Test County',
            'district': 'Test District',
            'address': 'Test Address',
            'city': 'Test City',
            'state': state_name,
            'zip_code': '12345',
            'phone': '555-0123',
            'email': 'test@example.com',
            'filing_date': '2024-01-01',
            'election_year': '2024',
            'election_type': 'General',
            'address_state': state_name
        }
    
    def _test_main_pipeline_id(self, record, state_name):
        """Test main pipeline stable ID generation"""
        try:
            # Simulate the main pipeline stable ID generation
            candidate_name = str(record.get('candidate_name', ''))
            office = str(record.get('office', ''))
            election_year = str(record.get('election_year', ''))
            
            # Create deterministic hash (NO COUNTY included)
            key = f"{candidate_name}|{state_name}|{office}|{election_year}"
            stable_id = hashlib.md5(key.encode()).hexdigest()[:12]
            
            return stable_id
        except Exception as e:
            return f"ERROR: {e}"
    
    def _test_national_standards_id(self, record, state_name):
        """Test national standards stable ID generation"""
        try:
            # Simulate the national standards stable ID generation
            name = str(record.get('candidate_name', '')).strip().upper()
            state = str(state_name).strip().upper()
            office = str(record.get('office', '')).strip().upper()
            election_year = str(record.get('election_year', '')).strip()
            county = record.get('county')
            
            # Only include county if it's not None and not empty
            county_part = f"_{str(county).strip().upper()}" if county and str(county).strip() else ""
            
            # Create stable ID string
            stable_id_string = f"{name}_{state}_{office}_{election_year}{county_part}"
            
            # Generate MD5 hash
            stable_id = hashlib.md5(stable_id_string.encode('utf-8')).hexdigest()
            
            return stable_id
        except Exception as e:
            return f"ERROR: {e}"
    
    def generate_summary_report(self, results):
        """Generate a summary report of all test results"""
        logger.info("\n" + "=" * 80)
        logger.info("📊 SUMMARY REPORT")
        logger.info("=" * 80)
        
        # Count states by characteristics
        total_states = len(results)
        states_with_county = sum(1 for r in results.values() if isinstance(r, dict) and r.get('has_county'))
        states_with_district = sum(1 for r in results.values() if isinstance(r, dict) and r.get('has_district'))
        states_with_address = sum(1 for r in results.values() if isinstance(r, dict) and r.get('has_address'))
        states_with_errors = sum(1 for r in results.values() if isinstance(r, dict) and 'error' in r)
        
        logger.info(f"Total States Tested: {total_states}")
        logger.info(f"States with County Data: {states_with_county}")
        logger.info(f"States with District Data: {states_with_district}")
        logger.info(f"States with Address Data: {states_with_address}")
        logger.info(f"States with Errors: {states_with_errors}")
        
        # Show stable ID differences
        logger.info("\n🔑 Stable ID Analysis:")
        for state_name, result in results.items():
            if isinstance(result, dict) and 'error' not in result:
                main_id = result['main_pipeline_id']
                national_id = result['national_standards_id']
                
                if main_id != national_id[:12]:
                    logger.info(f"  {state_name}: IDs differ (Main: {main_id}, National: {national_id[:12]})")
                else:
                    logger.info(f"  {state_name}: IDs match ({main_id})")
        
        # Show column variations
        logger.info("\n📋 Column Structure Variations:")
        column_counts = {}
        for result in results.values():
            if isinstance(result, dict) and 'columns' in result:
                col_count = len(result['columns'])
                column_counts[col_count] = column_counts.get(col_count, 0) + 1
        
        for col_count, count in sorted(column_counts.items()):
            logger.info(f"  {col_count} columns: {count} states")
        
        # Show specific examples
        logger.info("\n🔍 Specific Examples:")
        for state_name, result in results.items():
            if isinstance(result, dict) and 'error' not in result:
                test_record = result['test_record']
                main_id = result['main_pipeline_id']
                national_id = result['national_standards_id']
                
                logger.info(f"  {state_name}:")
                logger.info(f"    Name: {test_record['candidate_name']}")
                logger.info(f"    Office: {test_record['office']}")
                logger.info(f"    County: {test_record.get('county', 'None')}")
                logger.info(f"    Main ID: {main_id}")
                logger.info(f"    National ID: {national_id[:12]}...")
                logger.info(f"    IDs Match: {'✅' if main_id == national_id[:12] else '❌'}")
    
    def run_comprehensive_test(self):
        """Run the complete test suite"""
        logger.info("🚀 Starting Comprehensive State Stable ID Test")
        logger.info("=" * 80)
        
        # Test stable ID generation
        results = self.test_stable_id_generation()
        
        # Generate summary report
        self.generate_summary_report(results)
        
        logger.info("\n✅ Test Complete!")
        return results

def main():
    """Main test function"""
    tester = StateStableIDTester()
    results = tester.run_comprehensive_test()
    
    # Return results for potential further analysis
    return results

if __name__ == "__main__":
    main()
