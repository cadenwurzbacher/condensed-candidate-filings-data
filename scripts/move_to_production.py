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
            
            # Prepare data for production with consolidated office field
            logger.info("Preparing data for production with consolidated office field...")
            production_data = self._prepare_production_data(staging_data)
            
            # Clear existing production data (replace all)
            logger.info("Clearing existing production data...")
            clear_success = self.db_manager.execute_query("DELETE FROM filings")
            if clear_success is None:
                logger.error("Failed to clear production table")
                return False
            
            # Upload production data
            logger.info("Uploading production data...")
            upload_success = self.db_manager.upload_dataframe(
                production_data, 'filings', if_exists='append', index=False
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
        """Create the production filings table with the enhanced schema."""
        try:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS filings (
                -- Core Identifiers
                id SERIAL PRIMARY KEY,
                staging_id BIGINT,
                stable_id VARCHAR(100),
                external_id VARCHAR(100),
                
                -- Basic Candidate Info
                first_name VARCHAR(100),
                middle_name VARCHAR(100),
                last_name VARCHAR(100),
                nickname VARCHAR(100),
                prefix VARCHAR(50),
                suffix VARCHAR(50),
                full_name_display VARCHAR(255),
                
                -- Election Details
                election_year INTEGER,
                election_type VARCHAR(50),
                office VARCHAR(255),                    -- Clean, standardized office names
                office_confidence DECIMAL(3,2),         -- Standardization confidence (0.00-1.00)
                office_category VARCHAR(100),           -- Federal/State/Local grouping
                district VARCHAR(100),
                
                -- Party & Contact
                party VARCHAR(100),
                party_standardized VARCHAR(100),        -- Cleaned party names
                phone VARCHAR(50),
                email VARCHAR(255),
                website VARCHAR(255),
                
                -- Location
                state VARCHAR(50),
                county VARCHAR(100),
                city VARCHAR(100),
                address TEXT,
                address_parsed BOOLEAN,                -- Address parsing success
                address_clean TEXT,                    -- Standardized address
                zip_code VARCHAR(20),
                has_zip BOOLEAN,                       -- Data quality flag
                has_state BOOLEAN,                     -- Data quality flag
                
                -- Dates
                filing_date DATE,
                election_date DATE,
                
                -- Social Media
                facebook VARCHAR(255),
                twitter VARCHAR(255),
                
                -- Original Data (for audit)
                original_name VARCHAR(255),
                original_state VARCHAR(50),
                original_election_year INTEGER,
                original_office VARCHAR(255),           -- Original messy office names
                original_filing_date DATE,
                source_state VARCHAR(50),               -- Track origin state
                
                -- Processing Metadata
                processing_timestamp TIMESTAMP,         -- When processed
                pipeline_version BIGINT,               -- Pipeline version
                data_source VARCHAR(255),              -- Source file/state
                data_version VARCHAR(50),              -- Already exists
                batch_id VARCHAR(100),                 -- Already exists
                
                -- System Fields
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
    
    def _prepare_production_data(self, staging_data: pd.DataFrame) -> pd.DataFrame:
        """Prepare staging data for production with consolidated office field."""
        logger.info("Preparing production data with consolidated office field...")
        
        try:
            # Create a copy to avoid modifying the original
            production_data = staging_data.copy()
            
            # Consolidate office fields: use standardized office as main office field
            logger.info("Consolidating office fields...")
            
            # If office_standardized exists and has data, use it as the main office field
            if 'office_standardized' in production_data.columns:
                # Count how many records will use standardized office
                standardized_count = production_data['office_standardized'].notna().sum()
                total_count = len(production_data)
                logger.info(f"Using standardized office for {standardized_count:,} out of {total_count:,} records")
                
                # Replace office with standardized version where available
                production_data['office'] = production_data['office_standardized'].fillna(production_data['office'])
                
                # Log some examples of the consolidation
                sample_consolidated = production_data[
                    (production_data['office_standardized'].notna()) & 
                    (production_data['office_standardized'] != production_data['original_office'])
                ].head(3)
                
                if not sample_consolidated.empty:
                    logger.info("Sample office consolidations:")
                    for _, row in sample_consolidated.iterrows():
                        logger.info(f"  '{row['original_office']}' → '{row['office_standardized']}'")
            
            # Ensure we have all required columns for production
            required_columns = [
                'staging_id', 'stable_id', 'external_id',
                'first_name', 'middle_name', 'last_name', 'nickname', 'prefix', 'suffix', 'full_name_display',
                'election_year', 'election_type', 'office', 'office_confidence', 'office_category', 'district',
                'party', 'party_standardized', 'phone', 'email', 'website',
                'state', 'county', 'city', 'address', 'address_parsed', 'address_clean', 'zip_code', 'has_zip', 'has_state',
                'filing_date', 'election_date', 'facebook', 'twitter',
                'original_name', 'original_state', 'original_election_year', 'original_office', 'original_filing_date', 'source_state',
                'processing_timestamp', 'pipeline_version', 'data_source', 'data_version', 'batch_id',
                'created_at', 'updated_at'
            ]
            
            # Add missing columns with default values
            for col in required_columns:
                if col not in production_data.columns:
                    if col in ['staging_id', 'office_confidence', 'pipeline_version']:
                        production_data[col] = None  # Numeric columns
                    elif col in ['address_parsed', 'has_zip', 'has_state']:
                        production_data[col] = False  # Boolean columns
                    elif col in ['created_at', 'updated_at', 'processing_timestamp']:
                        production_data[col] = pd.Timestamp.now()  # Timestamp columns
                    else:
                        production_data[col] = ''  # String columns
                    logger.info(f"Added missing column: {col}")
            
            # Set staging_id to the original staging table id
            if 'id' in production_data.columns:
                production_data['staging_id'] = production_data['id']
                logger.info("Set staging_id from original staging table id")
            
            # Ensure data types are correct
            logger.info("Ensuring correct data types...")
            
            # Convert date columns
            date_columns = ['filing_date', 'election_date', 'original_filing_date']
            for col in date_columns:
                if col in production_data.columns:
                    try:
                        production_data[col] = pd.to_datetime(production_data[col], errors='coerce')
                        logger.info(f"Converted {col} to datetime")
                    except Exception as e:
                        logger.warning(f"Could not convert {col} to datetime: {e}")
            
            # Convert numeric columns
            numeric_columns = ['election_year', 'original_election_year', 'office_confidence', 'pipeline_version']
            for col in numeric_columns:
                if col in production_data.columns:
                    try:
                        production_data[col] = pd.to_numeric(production_data[col], errors='coerce')
                        logger.info(f"Converted {col} to numeric")
                    except Exception as e:
                        logger.warning(f"Could not convert {col} to numeric: {e}")
            
            # Convert boolean columns
            boolean_columns = ['address_parsed', 'has_zip', 'has_state']
            for col in boolean_columns:
                if col in production_data.columns:
                    try:
                        production_data[col] = production_data[col].astype(bool)
                        logger.info(f"Converted {col} to boolean")
                    except Exception as e:
                        logger.warning(f"Could not convert {col} to boolean: {e}")
            
            logger.info(f"Production data preparation completed. Shape: {production_data.shape}")
            return production_data
            
        except Exception as e:
            logger.error(f"Failed to prepare production data: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            # Return original data if preparation fails
            return staging_data
    
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
