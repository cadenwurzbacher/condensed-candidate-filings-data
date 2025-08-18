#!/usr/bin/env python3
"""
Check Staging Data

This script shows you what's in the staging table without moving anything
to production. Use this to review your data before deciding to promote it.
"""

import sys
import os
sys.path.append('src/config')

from database import DatabaseManager
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_staging_data():
    """Check and display staging data summary."""
    db_manager = DatabaseManager()
    
    print("📋 Checking Staging Data")
    print("=" * 50)
    
    try:
        # Connect to database
        if not db_manager.connect():
            print("❌ Failed to connect to database")
            return False
        
        # Check if staging table exists
        if not db_manager.table_exists('staging_candidates'):
            print("❌ No staging_candidates table found")
            print("   Run the main pipeline first to create staging data")
            return False
        
        # Get staging data summary
        summary_query = """
        SELECT 
            state,
            COUNT(*) as record_count,
            MIN(election_year) as min_year,
            MAX(election_year) as max_year,
            COUNT(DISTINCT office) as unique_offices,
            COUNT(DISTINCT party) as unique_parties
        FROM staging_candidates 
        GROUP BY state 
        ORDER BY state
        """
        
        summary = db_manager.execute_query(summary_query)
        if not summary.empty:
            print("📊 Staging Data Summary:")
            print("=" * 60)
            for _, row in summary.iterrows():
                print(f"  {row['state']}: {row['record_count']} records, "
                      f"Years: {row['min_year']}-{row['max_year']}, "
                      f"Offices: {row['unique_offices']}, Parties: {row['unique_parties']}")
            
            total_records = db_manager.get_staging_record_count()
            print("=" * 60)
            print(f"  Total Records: {total_records}")
            print("=" * 60)
            
            # Show some sample data
            print("\n📝 Sample Records:")
            sample_query = "SELECT state, full_name_display, office, party FROM staging_candidates LIMIT 5"
            sample_data = db_manager.execute_query(sample_query)
            
            if not sample_data.empty:
                for _, row in sample_data.iterrows():
                    print(f"  {row['state']}: {row['full_name_display']} - {row['office']} ({row['party']})")
            
            print("\n✅ Staging data looks good!")
            print("🚀 To move to production, run: python scripts/move_to_production.py")
            return True
        else:
            print("❌ No staging data found")
            return False
            
    except Exception as e:
        print(f"❌ Error checking staging data: {e}")
        return False
    finally:
        db_manager.disconnect()

if __name__ == "__main__":
    success = check_staging_data()
    if not success:
        sys.exit(1)
