#!/usr/bin/env python3
"""
Add Date Tracking Columns

This script adds first_added_date and last_updated_date columns to both
staging_candidates and filings tables to track when candidates were first
discovered and last updated.
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.config.database import get_db_connection
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_date_tracking_columns():
    """Add first_added_date and last_updated_date columns to both tables."""
    
    print("🔧 ADDING DATE TRACKING COLUMNS")
    print("=" * 60)
    
    db = get_db_connection()
    
    try:
        # Ensure database connection is established
        print("🔌 Establishing database connection...")
        if not db.connect():
            print("❌ Failed to connect to database")
            return False
        
        print("✅ Database connection established")
        
        # Check if tables exist
        staging_exists = db.table_exists('staging_candidates')
        filings_exists = db.table_exists('filings')
        
        print(f"📋 Table Status:")
        print(f"  staging_candidates: {'✅ Exists' if staging_exists else '❌ Missing'}")
        print(f"  filings: {'✅ Exists' if filings_exists else '❌ Missing'}")
        
        if not staging_exists and not filings_exists:
            print("❌ No tables found. Please run the pipeline first to create tables.")
            return False
        
        # Add columns to staging_candidates table
        if staging_exists:
            print(f"\n🔧 Adding columns to staging_candidates table...")
            
            # Check if columns already exist
            staging_info = db.get_table_info('staging_candidates')
            staging_columns = staging_info['column_name'].tolist()
            
            if 'first_added_date' not in staging_columns:
                db.execute_query("""
                    ALTER TABLE staging_candidates 
                    ADD COLUMN first_added_date TIMESTAMP
                """)
                print("  ✅ Added first_added_date column")
            else:
                print("  ℹ️  first_added_date column already exists")
            
            if 'last_updated_date' not in staging_columns:
                db.execute_query("""
                    ALTER TABLE staging_candidates 
                    ADD COLUMN last_updated_date TIMESTAMP
                """)
                print("  ✅ Added last_updated_date column")
            else:
                print("  ℹ️  last_updated_date column already exists")
        
        # Add columns to filings table
        if filings_exists:
            print(f"\n🔧 Adding columns to filings table...")
            
            # Check if columns already exist
            filings_info = db.get_table_info('filings')
            filings_columns = filings_info['column_name'].tolist()
            
            if 'first_added_date' not in filings_columns:
                db.execute_query("""
                    ALTER TABLE filings 
                    ADD COLUMN first_added_date TIMESTAMP
                """)
                print("  ✅ Added first_added_date column")
            else:
                print("  ℹ️  first_added_date column already exists")
            
            if 'last_updated_date' not in filings_columns:
                db.execute_query("""
                    ALTER TABLE filings 
                    ADD COLUMN last_updated_date TIMESTAMP
                """)
                print("  ✅ Added last_updated_date column")
            else:
                print("  ℹ️  last_updated_date column already exists")
        
        # Update existing records to set initial dates
        print(f"\n📅 Setting initial dates for existing records...")
        
        if staging_exists:
            # Set first_added_date to created_at for existing records
            # Use a different approach to avoid column reference issues
            try:
                db.execute_query("""
                    UPDATE staging_candidates 
                    SET first_added_date = created_at,
                        last_updated_date = created_at
                    WHERE first_added_date IS NULL OR last_updated_date IS NULL
                """)
                print("  ✅ Updated staging_candidates with initial dates")
            except Exception as e:
                print(f"  ⚠️  Warning updating staging_candidates dates: {e}")
        
        if filings_exists:
            # Set first_added_date to created_at for existing records
            try:
                db.execute_query("""
                    UPDATE filings 
                    SET first_added_date = created_at,
                        last_updated_date = COALESCE(updated_at, created_at)
                    WHERE first_added_date IS NULL OR last_updated_date IS NULL
                """)
                print("  ✅ Updated filings with initial dates")
            except Exception as e:
                print(f"  ⚠️  Warning updating filings dates: {e}")
        
        print(f"\n✅ Date tracking columns added successfully!")
        print(f"\n📋 New Schema:")
        
        try:
            if staging_exists:
                staging_info = db.get_table_info('staging_candidates')
                date_columns = staging_info[staging_info['column_name'].str.contains('date', case=False, na=False)]
                print(f"  staging_candidates date columns:")
                for _, col in date_columns.iterrows():
                    print(f"    - {col['column_name']}: {col['data_type']}")
            
            if filings_exists:
                filings_info = db.get_table_info('filings')
                date_columns = filings_info[filings_info['column_name'].str.contains('date', case=False, na=False)]
                print(f"  filings date columns:")
                for _, col in date_columns.iterrows():
                    print(f"    - {col['column_name']}: {col['data_type']}")
        except Exception as e:
            print(f"  ⚠️  Warning displaying schema: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to add date tracking columns: {e}")
        logger.error(f"Failed to add date tracking columns: {e}")
        return False

if __name__ == "__main__":
    success = add_date_tracking_columns()
    if success:
        print("\n🎉 Migration completed successfully!")
        print("The pipeline will now track first_added_date and last_updated_date for all candidates.")
    else:
        print("\n💥 Migration failed. Check the logs for details.")
        sys.exit(1)
