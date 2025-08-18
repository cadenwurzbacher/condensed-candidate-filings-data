#!/usr/bin/env python3
"""
Database Configuration for Supabase

This module handles the connection to the Supabase PostgreSQL database
using SQLAlchemy and psycopg2.
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import logging
from typing import Optional
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections and operations for Supabase."""
    
    def __init__(self):
        self.engine: Optional[Engine] = None
        self.connection_string = self._get_connection_string()
        
    def _get_connection_string(self) -> str:
        """Get database connection string from environment variables."""
        # Supabase connection details
        host = os.getenv('SUPABASE_HOST')
        port = os.getenv('SUPABASE_PORT', '5432')
        database = os.getenv('SUPABASE_DATABASE', 'postgres')
        user = os.getenv('SUPABASE_USER')
        password = os.getenv('SUPABASE_PASSWORD')
        
        if not all([host, user, password]):
            logger.error("Missing required Supabase environment variables")
            logger.error("Please set SUPABASE_HOST, SUPABASE_USER, and SUPABASE_PASSWORD")
            raise ValueError("SUPABASE_HOST, SUPABASE_USER, and SUPABASE_PASSWORD must be set")
        else:
            logger.info(f"Using environment variables: {host}:{port}")
        
        return f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require"
    
    def connect(self) -> bool:
        """Establish database connection."""
        try:
            # Add SSL and connection parameters for Supabase
            connection_params = {
                'echo': False,  # Set to True for SQL query logging
                'pool_pre_ping': True,
                'pool_recycle': 300,
                'connect_args': {
                    'sslmode': 'require',
                    'connect_timeout': 10,
                    'application_name': 'candidatefilings_pipeline'
                }
            }
            
            self.engine = create_engine(
                self.connection_string,
                **connection_params
            )
            
            # Test connection with timeout
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                logger.info("Database connection established successfully")
                return True
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")
    
    def test_connection(self) -> bool:
        """Test if database connection is working."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.info(f"Connected to PostgreSQL: {version}")
                return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params: dict = None) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame."""
        try:
            with self.engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                
                if result.returns_rows:
                    return pd.DataFrame(result.fetchall(), columns=result.keys())
                else:
                    logger.info("Query executed successfully (no rows returned)")
                    return pd.DataFrame()
                    
        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            return pd.DataFrame()
    
    def upload_dataframe(self, df: pd.DataFrame, table_name: str, 
                        if_exists: str = 'replace', index: bool = False) -> bool:
        """Upload a DataFrame to a database table."""
        try:
            # For large datasets, use chunked upload to avoid memory issues
            if len(df) > 10000:
                logger.info(f"Large dataset detected ({len(df)} records), using chunked upload...")
                return self._upload_dataframe_chunked(df, table_name, if_exists, index)
            else:
                # For smaller datasets, use standard upload
                df.to_sql(
                    name=table_name,
                    con=self.engine,
                    if_exists=if_exists,
                    index=index,
                    method='multi',
                    chunksize=1000
                )
                logger.info(f"Successfully uploaded {len(df)} rows to table '{table_name}'")
                return True
            
        except Exception as e:
            logger.error(f"Failed to upload data to table '{table_name}': {e}")
            return False
    
    def _upload_dataframe_chunked(self, df: pd.DataFrame, table_name: str, 
                                 if_exists: str = 'replace', index: bool = False) -> bool:
        """Upload large DataFrame in chunks to avoid memory issues."""
        try:
            chunk_size = 5000  # Smaller chunks for better reliability
            total_chunks = (len(df) + chunk_size - 1) // chunk_size
            
            logger.info(f"Uploading {len(df)} records in {total_chunks} chunks of {chunk_size}")
            
            for i in range(0, len(df), chunk_size):
                chunk_num = (i // chunk_size) + 1
                chunk = df.iloc[i:i + chunk_size]
                
                logger.info(f"Uploading chunk {chunk_num}/{total_chunks} ({len(chunk)} records)")
                
                # Use replace for first chunk, append for subsequent chunks
                chunk_if_exists = 'replace' if i == 0 else 'append'
                
                chunk.to_sql(
                    name=table_name,
                    con=self.engine,
                    if_exists=chunk_if_exists,
                    index=index,
                    method='multi',
                    chunksize=1000
                )
                
                logger.info(f"✅ Chunk {chunk_num}/{total_chunks} uploaded successfully")
            
            logger.info(f"Successfully uploaded all {len(df)} rows to table '{table_name}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload chunked data to table '{table_name}': {e}")
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        try:
            query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = :table_name
            );
            """
            result = self.execute_query(query, {'table_name': table_name})
            return result.iloc[0, 0] if not result.empty else False
            
        except Exception as e:
            logger.error(f"Failed to check if table '{table_name}' exists: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """Get information about a table's structure."""
        try:
            query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = :table_name
            ORDER BY ordinal_position;
            """
            return self.execute_query(query, {'table_name': table_name})
            
        except Exception as e:
            logger.error(f"Failed to get table info for '{table_name}': {e}")
            return pd.DataFrame()

    def clear_staging_table(self) -> bool:
        """Clear all data from the staging_candidates table."""
        try:
            if not self.table_exists('staging_candidates'):
                logger.warning("staging_candidates table does not exist")
                return True  # Consider this a success if table doesn't exist
            
            query = "DELETE FROM staging_candidates"
            with self.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
            
            logger.info("Staging table cleared successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear staging table: {e}")
            return False
    
    def upload_to_staging(self, df: pd.DataFrame) -> bool:
        """Upload DataFrame to staging_candidates table."""
        try:
            # Test database connection before proceeding
            logger.info("Testing database connection before upload...")
            if not self.test_connection():
                logger.error("Database connection test failed")
                return False
            
            # Ensure the staging table exists with correct schema
            if not self.table_exists('staging_candidates'):
                logger.info("Creating staging_candidates table...")
                if not self._create_staging_table():
                    logger.error("Failed to create staging table")
                    return False
            
            # Log DataFrame info for debugging
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"DataFrame columns: {list(df.columns)}")
            logger.info(f"DataFrame dtypes: {df.dtypes.to_dict()}")
            
            # Check for any problematic data types
            for col, dtype in df.dtypes.items():
                if dtype == 'object':
                    # Check for any extremely long strings that might cause issues
                    max_length = df[col].astype(str).str.len().max()
                    if max_length > 1000:
                        logger.warning(f"Column '{col}' has very long strings (max: {max_length} chars)")
            
            # Upload data to staging
            logger.info("Starting data upload to staging table...")
            success = self.upload_dataframe(df, 'staging_candidates', if_exists='append', index=False)
            
            if success:
                logger.info(f"Successfully uploaded {len(df)} records to staging")
                return True
            else:
                logger.error("Failed to upload data to staging")
                return False
                
        except Exception as e:
            logger.error(f"Failed to upload to staging: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def get_staging_record_count(self) -> int:
        """Get the number of records in the staging_candidates table."""
        try:
            query = "SELECT COUNT(*) FROM staging_candidates"
            result = self.execute_query(query)
            if not result.empty:
                return int(result.iloc[0, 0])
            return 0
        except Exception as e:
            logger.error(f"Failed to get staging record count: {e}")
            return 0
    
    def _create_staging_table(self) -> bool:
        """Create the staging_candidates table with the enhanced schema."""
        try:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS staging_candidates (
                -- Core Identifiers
                id SERIAL PRIMARY KEY,
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
                
                -- System Fields
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
            
            logger.info("staging_candidates table created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create staging table: {e}")
            return False

# Global database manager instance
db_manager = DatabaseManager()

def get_db_connection() -> DatabaseManager:
    """Get the global database manager instance."""
    return db_manager
