#!/usr/bin/env python3
"""
Database Manager for CandidateFilings.com Pipeline

This module handles all database operations for the pipeline,
including connection management, data uploads, and staging operations.
"""

import pandas as pd
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Handles all database operations for the pipeline.
    
    This class manages:
    - Database connection and configuration
    - Data uploads to staging and production tables
    - Smart staging operations
    - Connection pooling and error handling
    """
    
    def __init__(self, config):
        """
        Initialize the database manager.
        
        Args:
            config: PipelineConfig instance
        """
        self.config = config
        self.connection = None
        self.db_manager = None
        
        # Initialize database connection if enabled
        if self.config.enable_database_connection:
            self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database connection and manager."""
        try:
            from config.database import get_db_connection
            self.db_manager = get_db_connection()
            
            # Test database connection
            if self.db_manager.connect():
                logger.info("✅ Database connection established successfully")
                # Test if we can query the database
                try:
                    test_result = self.db_manager.execute_query("SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('filings', 'staging_candidates')")
                    logger.info(f"Database test query successful: {test_result}")
                except Exception as e:
                    logger.warning(f"Database test query failed: {e}")
                    logger.warning("Pipeline will run without database connectivity")
            else:
                logger.warning("❌ Database connection failed - pipeline will run without database connectivity")
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            self.db_manager = None
    
    def is_connected(self) -> bool:
        """Check if database connection is available."""
        return self.db_manager is not None and self.config.enable_database_connection
    
    def test_connection(self) -> bool:
        """Test database connection and basic functionality."""
        if not self.is_connected():
            return False
        
        try:
            if not self.db_manager.test_connection():
                logger.error("Database connection test failed")
                return False
            
            # Test if we can query the tables
            try:
                # Check if tables exist
                tables_query = "SELECT table_name FROM information_schema.tables WHERE table_name IN ('filings', 'staging_candidates')"
                tables_result = self.db_manager.execute_query(tables_query)
                logger.info(f"Available tables: {tables_result['table_name'].tolist() if not tables_result.empty else 'None'}")
                
                # Try to get a sample of data from each table
                if not tables_result.empty:
                    for table_name in tables_result['table_name']:
                        try:
                            sample_query = f"SELECT COUNT(*) as count FROM {table_name} LIMIT 1"
                            sample_result = self.db_manager.execute_query(sample_query)
                            logger.info(f"Table {table_name}: {sample_result.iloc[0]['count'] if not sample_result.empty else 'No data'}")
                        except Exception as e:
                            logger.warning(f"Could not query table {table_name}: {e}")
                
                return True
                
            except Exception as e:
                logger.error(f"Database query test failed: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def upload_to_staging(self, data: pd.DataFrame) -> bool:
        """
        Upload data to staging table (if enabled).
        
        Args:
            data: DataFrame to upload
            
        Returns:
            True if successful, False otherwise
        """
        if not self.config.enable_staging_table:
            logger.debug("Skipping staging upload (disabled)")
            return True
        
        if not self.is_connected():
            logger.warning("Cannot upload to staging - no database connection")
            return False
        
        if data.empty:
            logger.warning("Cannot upload empty data to staging")
            return False
        
        try:
            logger.info("Starting staging database upload...")
            
            # Clear existing staging data (replace all)
            logger.info("Clearing existing staging data...")
            clear_success = self.db_manager.clear_staging_table()
            if not clear_success:
                logger.error("Failed to clear staging table")
                return False
            
            # Upload new data to staging
            logger.info("Uploading data to staging table...")
            upload_success = self.db_manager.upload_to_staging(data)
            if not upload_success:
                logger.error("Failed to upload to staging table")
                return False
            
            # Verify upload
            record_count = self.db_manager.get_staging_record_count()
            logger.info(f"✅ Staging upload completed successfully: {record_count} records in staging")
            return True
            
        except Exception as e:
            logger.error(f"❌ Staging upload failed: {e}")
            return False
    
    def upload_to_production(self, data: pd.DataFrame) -> bool:
        """
        Upload data to production table (if enabled).
        
        Args:
            data: DataFrame to upload
            
        Returns:
            True if successful, False otherwise
        """
        if not self.config.enable_production_table:
            logger.debug("Skipping production upload (disabled)")
            return True
        
        if not self.is_connected():
            logger.warning("Cannot upload to production - no database connection")
            return False
        
        if data.empty:
            logger.warning("Cannot upload empty data to production")
            return False
        
        try:
            logger.info("Starting production database upload...")
            
            # Use smart staging if enabled
            if self.config.enable_smart_staging:
                return self._smart_production_upload(data)
            else:
                return self._direct_production_upload(data)
            
        except Exception as e:
            logger.error(f"❌ Production upload failed: {e}")
            return False
    
    def _smart_production_upload(self, data: pd.DataFrame) -> bool:
        """
        Upload to production using smart staging manager.
        
        Args:
            data: DataFrame to upload
            
        Returns:
            True if successful, False otherwise
        """
        try:
            from .smart_staging_manager import SmartStagingManager
            
            # Initialize smart staging manager
            smart_staging = SmartStagingManager(self.db_manager)
            
            # Get existing production data for comparison
            production_data = pd.DataFrame()
            if self.db_manager.table_exists('filings'):
                production_data = self.db_manager.execute_query("SELECT * FROM filings")
                logger.info(f"Found {len(production_data)} existing production records")
            else:
                logger.info("No existing production data found (first run)")
            
            # Analyze staging data
            analysis = smart_staging.analyze_staging_data(data, production_data)
            
            logger.info(f"Quality Score: {analysis['quality_score']:.3f} ({analysis['quality_level'].value})")
            logger.info(f"Changes: {analysis['change_summary']}")
            logger.info(f"Recommendation: {analysis['recommendation']}")
            
            # Execute the recommended strategy
            if analysis['auto_promote']:
                logger.info("🚀 Auto-promoting data to production...")
                results = smart_staging.execute_promotion_strategy(data, analysis)
                logger.info(f"Auto-promotion results: {results}")
                return True
            else:
                logger.info("📋 Manual review required - data saved to staging only")
                return True
                
        except Exception as e:
            logger.error(f"Smart production upload failed: {e}")
            return False
    
    def _direct_production_upload(self, data: pd.DataFrame) -> bool:
        """
        Upload directly to production table.
        
        Args:
            data: DataFrame to upload
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Uploading data directly to production table...")
            
            # Clear existing production data (replace all)
            logger.info("Clearing existing production data...")
            clear_success = self.db_manager.clear_production_table()
            if not clear_success:
                logger.error("Failed to clear production table")
                return False
            
            # Upload new data to production
            logger.info("Uploading data to production table...")
            upload_success = self.db_manager.upload_to_production(data)
            if not upload_success:
                logger.error("Failed to upload to production table")
                return False
            
            # Verify upload
            record_count = self.db_manager.get_production_record_count()
            logger.info(f"✅ Production upload completed successfully: {record_count} records in production")
            return True
            
        except Exception as e:
            logger.error(f"Direct production upload failed: {e}")
            return False
    
    def get_existing_data(self) -> pd.DataFrame:
        """
        Get existing production data for comparison.
        
        Returns:
            DataFrame with existing production data
        """
        if not self.is_connected():
            return pd.DataFrame()
        
        try:
            # Get minimal columns needed for comparison
            query = """
            SELECT stable_id, first_added_date, last_updated_date, 
                   election_year, election_type, office, district, full_name_display,
                   first_name, middle_name, last_name, prefix, suffix, nickname,
                   party, phone, email, address, website, state, county, city,
                   zip_code, address_state, filing_date, election_date, facebook, twitter
            FROM filings
            """
            
            result = self.db_manager.execute_query(query)
            logger.info(f"Retrieved {len(result)} existing production records for comparison")
            return result
            
        except Exception as e:
            logger.error(f"Failed to get existing production data: {e}")
            return pd.DataFrame()
    
    def get_existing_data_with_hash(self) -> pd.DataFrame:
        """
        Get existing production data with data_hash for comparison.
        
        Returns:
            DataFrame with existing production data and data_hash
        """
        if not self.is_connected():
            return pd.DataFrame()
        
        try:
            # Check if data_hash column exists in the filings table
            check_hash_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'filings' AND column_name = 'data_hash'
            """
            hash_check = self.db_manager.execute_query(check_hash_query)
            
            if hash_check.empty:
                logger.info("data_hash column doesn't exist in filings table - creating it...")
                # Create data_hash column and populate it
                self._create_and_populate_data_hash_column()
            
            # Get minimal columns needed for comparison (including data_hash)
            query = """
            SELECT stable_id, data_hash
            FROM filings
            WHERE stable_id IS NOT NULL
            """
            
            result = self.db_manager.execute_query(query)
            logger.info(f"Retrieved {len(result)} existing production records with data_hash for comparison")
            return result
            
        except Exception as e:
            logger.error(f"Failed to get existing production data with hash: {e}")
            return pd.DataFrame()
    
    def _create_and_populate_data_hash_column(self):
        """Create data_hash column and populate it for existing records."""
        try:
            logger.info("Creating data_hash column in filings table...")
            
            # Add data_hash column
            add_column_query = """
            ALTER TABLE filings 
            ADD COLUMN IF NOT EXISTS data_hash VARCHAR(32)
            """
            self.db_manager.execute_query(add_column_query)
            
            # Update existing records with data_hash
            logger.info("Populating data_hash for existing records...")
            update_query = """
            UPDATE filings 
            SET data_hash = MD5(
                CONCAT_WS('|',
                    COALESCE(election_year::text, ''),
                    COALESCE(election_type, ''),
                    COALESCE(office, ''),
                    COALESCE(district, ''),
                    COALESCE(full_name_display, ''),
                    COALESCE(first_name, ''),
                    COALESCE(middle_name, ''),
                    COALESCE(last_name, ''),
                    COALESCE(prefix, ''),
                    COALESCE(suffix, ''),
                    COALESCE(nickname, ''),
                    COALESCE(party, ''),
                    COALESCE(phone, ''),
                    COALESCE(email, ''),
                    COALESCE(address, ''),
                    COALESCE(website, ''),
                    COALESCE(state, ''),
                    COALESCE(county, ''),
                    COALESCE(city, ''),
                    COALESCE(zip_code, ''),
                    COALESCE(address_state, ''),
                    COALESCE(filing_date::text, ''),
                    COALESCE(election_date::text, ''),
                    COALESCE(facebook, ''),
                    COALESCE(twitter, '')
                    -- EXCLUDED: first_added_date, last_updated_date (system metadata)
                )
            )::text
            WHERE data_hash IS NULL
            """
            self.db_manager.execute_query(update_query)
            
            logger.info("✅ data_hash column created and populated successfully")
            
        except Exception as e:
            logger.error(f"Failed to create and populate data_hash column: {e}")
    
    def disconnect(self):
        """Close database connection."""
        if self.db_manager:
            self.db_manager.disconnect()
            logger.info("Database connection closed")
    
    def get_database_status(self) -> Dict[str, Any]:
        """Get database configuration and connection status."""
        return {
            'connection_enabled': self.config.enable_database_connection,
            'upload_enabled': self.config.enable_database_upload,
            'staging_enabled': self.config.enable_staging_table,
            'production_enabled': self.config.enable_production_table,
            'smart_staging_enabled': self.config.enable_smart_staging,
            'is_connected': self.is_connected(),
            'connection_tested': self.test_connection() if self.is_connected() else False
        }
