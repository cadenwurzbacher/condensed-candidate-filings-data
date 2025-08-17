#!/usr/bin/env python3
"""
Move Staging Data to Production

This script moves reviewed data from the staging_candidates table to the 
production filings table. Run this after reviewing the staging data.
"""

import sys
import os
sys.path.append('src/config')
sys.path.append('src/pipeline')

from database import DatabaseManager
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProductionUploader:
    """Handles moving data from staging to production."""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        
    def move_staging_to_production(self) -> bool:
        """Move all data from staging to production table."""
        logger.info("🚀 Starting production upload process...")
        
        try:
            # Connect to database
            if not self.db_manager.connect():
                logger.error("Failed to connect to database")
                return False
            
            # Check staging data
            staging_count = self.db_manager.get_staging_record_count()
            if staging_count == 0:
                logger.error("No data found in staging table")
                return False
            
            logger.info(f"Found {staging_count} records in staging table")
            
            # Get staging data
            staging_data = self.db_manager.execute_query("SELECT * FROM staging_candidates")
            logger.info(f"Loaded {len(staging_data)} records from staging")
            
            # Ensure production table exists
            if not self.db_manager.table_exists('filings'):
                logger.info("Creating production filings table...")
                self._create_production_table()
            
            # Clear existing production data (replace all)
            logger.info("Clearing existing production data...")
            clear_success = self.db_manager.execute_query("DELETE FROM filings")
            if clear_success is None:
                logger.error("Failed to clear production table")
                return False
            
            # Upload staging data to production
            logger.info("Uploading staging data to production...")
            upload_success = self.db_manager.upload_dataframe(
                staging_data, 'filings', if_exists='append', index=False
            )
            
            if not upload_success:
                logger.error("Failed to upload to production table")
                return False
            
            # Verify production upload
            production_count = self.db_manager.execute_query("SELECT COUNT(*) FROM filings")
            if not production_count.empty:
                final_count = int(production_count.iloc[0, 0])
                logger.info(f"✅ Production upload completed successfully: {final_count} records")
                
                # Clear staging table after successful production upload
                logger.info("Clearing staging table...")
                self.db_manager.clear_staging_table()
                
                return True
            else:
                logger.error("Failed to verify production upload")
                return False
                
        except Exception as e:
            logger.error(f"❌ Production upload failed: {e}")
            return False
        finally:
            self.db_manager.disconnect()
    
    def _create_production_table(self) -> bool:
        """Create the production filings table with the correct schema."""
        try:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS filings (
                id SERIAL PRIMARY KEY,
                election_year INTEGER,
                election_type VARCHAR(50),
                office VARCHAR(255),
                district VARCHAR(100),
                candidate_name VARCHAR(255),
                first_name VARCHAR(100),
                middle_name VARCHAR(100),
                last_name VARCHAR(100),
                prefix VARCHAR(50),
                suffix VARCHAR(50),
                nickname VARCHAR(100),
                full_name_display VARCHAR(255),
                party VARCHAR(100),
                phone VARCHAR(50),
                email VARCHAR(255),
                address TEXT,
                website VARCHAR(255),
                state VARCHAR(50),
                original_name VARCHAR(255),
                original_state VARCHAR(50),
                original_election_year INTEGER,
                original_office VARCHAR(255),
                original_filing_date DATE,
                stable_id VARCHAR(100),
                county VARCHAR(100),
                city VARCHAR(100),
                zip_code VARCHAR(20),
                filing_date DATE,
                election_date DATE,
                facebook VARCHAR(255),
                twitter VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            with self.db_manager.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
            
            logger.info("Production filings table created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create production table: {e}")
            return False
    
    def preview_staging_data(self) -> bool:
        """Show a preview of staging data before moving to production."""
        logger.info("📋 Previewing staging data...")
        
        try:
            if not self.db_manager.connect():
                logger.error("Failed to connect to database")
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
            
            summary = self.db_manager.execute_query(summary_query)
            if not summary.empty:
                logger.info("📊 Staging Data Summary:")
                logger.info("=" * 60)
                for _, row in summary.iterrows():
                    logger.info(f"  {row['state']}: {row['record_count']} records, "
                              f"Years: {row['min_year']}-{row['max_year']}, "
                              f"Offices: {row['unique_offices']}, Parties: {row['unique_parties']}")
                
                total_records = self.db_manager.get_staging_record_count()
                logger.info("=" * 60)
                logger.info(f"  Total Records: {total_records}")
                logger.info("=" * 60)
                return True
            else:
                logger.warning("No staging data found")
                return False
                
        except Exception as e:
            logger.error(f"Failed to preview staging data: {e}")
            return False
        finally:
            self.db_manager.disconnect()

def main():
    """Main function to run the production upload."""
    uploader = ProductionUploader()
    
    print("🚀 CandidateFilings Production Upload")
    print("=" * 50)
    
    # First, preview staging data
    print("\n📋 Previewing staging data...")
    if not uploader.preview_staging_data():
        print("❌ Failed to preview staging data")
        sys.exit(1)
    
    # Ask for confirmation
    print("\n⚠️  WARNING: This will replace ALL existing production data!")
    response = input("Are you sure you want to proceed? (yes/no): ").lower().strip()
    
    if response != 'yes':
        print("❌ Production upload cancelled")
        sys.exit(0)
    
    # Proceed with production upload
    print("\n🚀 Starting production upload...")
    success = uploader.move_staging_to_production()
    
    if success:
        print("\n✅ Production upload completed successfully!")
        print("🎉 Your data is now live in production!")
    else:
        print("\n❌ Production upload failed!")
        print("Check the logs for error details.")
        sys.exit(1)

if __name__ == "__main__":
    main()
