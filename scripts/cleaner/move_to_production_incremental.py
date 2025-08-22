#!/usr/bin/env python3
"""
Move Staging Data to Production with Incremental Updates

This script moves reviewed data from the staging_candidates table to the 
production filings table using incremental updates based on stable_id.
It handles new records, updates, and conflicts intelligently.
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
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IncrementalProductionUploader:
    """Handles moving data from staging to production with incremental updates."""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        
    def move_staging_to_production_incremental(self) -> bool:
        """Move staging data to production using incremental updates."""
        logger.info("🚀 Starting incremental production upload process...")
        
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
            
            # Get staging data with change tracking
            staging_data = self.db_manager.execute_query("""
                SELECT *, 
                       COALESCE(change_type, 'new') as change_type,
                       COALESCE(change_timestamp, CURRENT_TIMESTAMP) as change_timestamp
                FROM staging_candidates
            """)
            logger.info(f"Loaded {len(staging_data)} records from staging")
            
            # Ensure production table exists
            if not self.db_manager.table_exists('filings'):
                logger.info("Creating production filings table...")
                self._create_production_table()
            
            # Prepare data for production
            logger.info("Preparing data for production...")
            production_data = self._prepare_production_data(staging_data)
            
            # Perform incremental update
            logger.info("Performing incremental update...")
            update_stats = self._perform_incremental_update(production_data)
            
            if update_stats['success']:
                logger.info("✅ Incremental production update completed successfully!")
                logger.info(f"📊 Update Statistics:")
                logger.info(f"  • New records: {update_stats['new_records']}")
                logger.info(f"  • Updated records: {update_stats['updated_records']}")
                logger.info(f"  • Unchanged records: {update_stats['unchanged_records']}")
                logger.info(f"  • Conflicts: {update_stats['conflicts']}")
                
                # Clear staging table after successful production upload
                logger.info("Clearing staging table...")
                self.db_manager.clear_staging_table()
                
                return True
            else:
                logger.error("❌ Incremental production update failed")
                return False
                
        except Exception as e:
            logger.error(f"❌ Incremental production upload failed: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
        finally:
            self.db_manager.disconnect()
    
    def _perform_incremental_update(self, production_data: pd.DataFrame) -> dict:
        """Perform incremental update based on stable_id."""
        try:
            stats = {
                'new_records': 0,
                'updated_records': 0,
                'unchanged_records': 0,
                'conflicts': 0,
                'success': False
            }
            
            # Get existing production records for comparison
            existing_records = self.db_manager.execute_query("""
                SELECT stable_id, id, office, party, full_name_display, 
                       office_confidence, office_category, party_standardized
                FROM filings 
                WHERE stable_id IS NOT NULL
            """)
            
            existing_dict = {}
            if not existing_records.empty:
                existing_dict = existing_records.set_index('stable_id').to_dict('index')
            
            # Process each staging record
            for idx, row in production_data.iterrows():
                stable_id = row.get('stable_id')
                change_type = row.get('change_type', 'new')
                
                if not stable_id:
                    # No stable_id, treat as new record
                    stats['new_records'] += 1
                    self._insert_new_record(row)
                    continue
                
                if stable_id in existing_dict:
                    # Record exists, check if it needs updating
                    existing = existing_dict[stable_id]
                    if change_type == 'updated' or self._has_changes(row, existing):
                        stats['updated_records'] += 1
                        self._update_existing_record(row, existing['id'])
                    else:
                        stats['unchanged_records'] += 1
                else:
                    # New record
                    stats['new_records'] += 1
                    self._insert_new_record(row)
            
            stats['success'] = True
            return stats
            
        except Exception as e:
            logger.error(f"Failed to perform incremental update: {e}")
            return stats
    
    def _has_changes(self, staging_row: pd.Series, existing_record: dict) -> bool:
        """Check if staging record has meaningful changes compared to existing record."""
        key_fields = ['office', 'party', 'full_name_display', 'office_confidence', 
                     'office_category', 'party_standardized']
        
        for field in key_fields:
            staging_val = staging_row.get(field)
            existing_val = existing_record.get(field)
            
            if pd.isna(staging_val) and pd.isna(existing_val):
                continue
            if pd.isna(staging_val) or pd.isna(existing_val):
                return True
            if str(staging_val) != str(existing_val):
                return True
        
        return False
    
    def _insert_new_record(self, row: pd.Series) -> bool:
        """Insert a new record into the filings table."""
        try:
            # Prepare insert data
            insert_data = self._prepare_row_for_insert(row)
            
            # Build INSERT query
            columns = list(insert_data.keys())
            values = list(insert_data.values())
            placeholders = ['%s'] * len(values)
            
            query = f"""
            INSERT INTO filings ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            """
            
            with self.db_manager.engine.connect() as conn:
                conn.execute(text(query), values)
                conn.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert new record: {e}")
            return False
    
    def _update_existing_record(self, row: pd.Series, existing_id: int) -> bool:
        """Update an existing record in the filings table."""
        try:
            # Prepare update data
            update_data = self._prepare_row_for_update(row)
            
            # Build UPDATE query
            set_clauses = [f"{col} = %s" for col in update_data.keys()]
            query = f"""
            UPDATE filings 
            SET {', '.join(set_clauses)}, updated_at = CURRENT_TIMESTAMP
            WHERE id = {existing_id}
            """
            
            with self.db_manager.engine.connect() as conn:
                conn.execute(text(query), list(update_data.values()))
                conn.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update existing record: {e}")
            return False
    
    def _prepare_row_for_insert(self, row: pd.Series) -> dict:
        """Prepare a row for insertion into filings table."""
        # Map staging columns to filings columns
        column_mapping = {
            'stable_id': 'stable_id',
            'external_id': 'external_id',
            'first_name': 'first_name',
            'middle_name': 'middle_name',
            'last_name': 'last_name',
            'nickname': 'nickname',
            'prefix': 'prefix',
            'suffix': 'suffix',
            'full_name_display': 'full_name_display',
            'election_year': 'election_year',
            'election_type': 'election_type',
            'office': 'office',
            'office_confidence': 'office_confidence',
            'office_category': 'office_category',
            'district': 'district',
            'party': 'party',
            'party_standardized': 'party_standardized',
            'phone': 'phone',
            'email': 'email',
            'website': 'website',
            'state': 'state',
            'county': 'county',
            'city': 'city',
            'address': 'address',
            'address_parsed': 'address_parsed',
            'address_clean': 'address_clean',
            'zip_code': 'zip_code',
            'has_zip': 'has_zip',
            'has_state': 'has_state',
            'filing_date': 'filing_date',
            'election_date': 'election_date',
            'facebook': 'facebook',
            'twitter': 'twitter',
            'original_name': 'original_name',
            'original_state': 'original_state',
            'original_election_year': 'original_election_year',
            'original_office': 'original_office',
            'original_filing_date': 'original_filing_date',
            'source_state': 'source_state',
            'processing_timestamp': 'processing_timestamp',
            'pipeline_version': 'pipeline_version',
            'data_source': 'data_source',
            'data_version': 'data_version',
            'batch_id': 'batch_id'
        }
        
        insert_data = {}
        for staging_col, filings_col in column_mapping.items():
            if staging_col in row and pd.notna(row[staging_col]):
                insert_data[filings_col] = row[staging_col]
        
        # Add staging_id for audit trail
        if 'id' in row:
            insert_data['staging_id'] = row['id']
        
        return insert_data
    
    def _prepare_row_for_update(self, row: pd.Series) -> dict:
        """Prepare a row for updating existing filings record."""
        # Only update fields that have meaningful changes
        update_fields = [
            'office', 'office_confidence', 'office_category', 'party', 'party_standardized',
            'full_name_display', 'address_clean', 'processing_timestamp', 'pipeline_version'
        ]
        
        update_data = {}
        for field in update_fields:
            if field in row and pd.notna(row[field]):
                update_data[field] = row[field]
        
        return update_data
    
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
                office VARCHAR(255),
                office_confidence DECIMAL(3,2),
                office_category VARCHAR(100),
                district VARCHAR(100),
                
                -- Party & Contact
                party VARCHAR(100),
                party_standardized VARCHAR(100),
                phone VARCHAR(50),
                email VARCHAR(255),
                website VARCHAR(255),
                
                -- Location
                state VARCHAR(50),
                county VARCHAR(100),
                city VARCHAR(100),
                address TEXT,
                address_parsed BOOLEAN,
                address_clean TEXT,
                zip_code VARCHAR(20),
                has_zip BOOLEAN,
                has_state BOOLEAN,
                
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
                original_office VARCHAR(255),
                original_filing_date DATE,
                source_state VARCHAR(50),
                
                -- Processing Metadata
                processing_timestamp TIMESTAMP,
                pipeline_version BIGINT,
                data_source VARCHAR(255),
                data_version VARCHAR(50),
                batch_id VARCHAR(100),
                
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
        logger.info("Preparing production data...")
        
        try:
            # Create a copy to avoid modifying the original
            production_data = staging_data.copy()
            
            # Ensure we have all required columns
            required_columns = [
                'staging_id', 'stable_id', 'external_id',
                'first_name', 'middle_name', 'last_name', 'nickname', 'prefix', 'suffix', 'full_name_display',
                'election_year', 'election_type', 'office', 'office_confidence', 'office_category', 'district',
                'party', 'party_standardized', 'phone', 'email', 'website',
                'state', 'county', 'city', 'address', 'address_parsed', 'address_clean', 'zip_code', 'has_zip', 'has_state',
                'filing_date', 'election_date', 'facebook', 'twitter',
                'original_name', 'original_state', 'original_election_year', 'original_office', 'original_filing_date', 'source_state',
                'processing_timestamp', 'pipeline_version', 'data_source', 'data_version', 'batch_id',
                'change_type', 'change_timestamp'
            ]
            
            # Add missing columns with default values
            for col in required_columns:
                if col not in production_data.columns:
                    if col in ['staging_id', 'office_confidence', 'pipeline_version']:
                        production_data[col] = None
                    elif col in ['address_parsed', 'has_zip', 'has_state']:
                        production_data[col] = False
                    elif col in ['change_timestamp']:
                        production_data[col] = pd.Timestamp.now()
                    else:
                        production_data[col] = ''
                    logger.info(f"Added missing column: {col}")
            
            # Set staging_id to the original staging table id
            if 'id' in production_data.columns:
                production_data['staging_id'] = production_data['id']
                logger.info("Set staging_id from original staging table id")
            
            logger.info(f"Production data preparation completed. Shape: {production_data.shape}")
            return production_data
            
        except Exception as e:
            logger.error(f"Failed to prepare production data: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return staging_data

def main():
    """Main function to run the incremental production upload."""
    uploader = IncrementalProductionUploader()
    
    print("🚀 CandidateFilings Incremental Production Upload")
    print("=" * 60)
    print("This script performs incremental updates instead of complete overwrites.")
    print("It compares staging data with existing production data using stable_id.")
    print()
    
    # Run the incremental upload
    success = uploader.move_staging_to_production_incremental()
    
    if success:
        print("\n🎉 Incremental production upload completed successfully!")
        print("Your production data has been updated with new and changed records.")
        print("Existing unchanged records were preserved.")
    else:
        print("\n❌ Incremental production upload failed.")
        print("Check the logs for details.")

if __name__ == "__main__":
    main()
