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
                # Clean NaN values before upload - replace with None for proper NULL conversion
                df_clean = df.replace({pd.NA: None, pd.NaT: None})
                df_clean = df_clean.where(pd.notnull(df_clean), None)
                
                df_clean.to_sql(
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
                
                # Clean NaN values before upload - replace with None for proper NULL conversion
                chunk_clean = chunk.replace({pd.NA: None, pd.NaT: None})
                chunk_clean = chunk_clean.where(pd.notnull(chunk_clean), None)
                
                chunk_clean.to_sql(
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
                
                -- Basic Candidate Info (exact match to filings table)
                election_year INTEGER,
                election_type VARCHAR(50),
                office TEXT,
                source_office TEXT,
                district VARCHAR(100),
                source_district TEXT,
                full_name_display TEXT,
                first_name VARCHAR(100),
                middle_name VARCHAR(100),
                last_name VARCHAR(100),
                prefix VARCHAR(50),
                suffix VARCHAR(50),
                nickname VARCHAR(100),
                party VARCHAR(100),
                phone VARCHAR(50),
                email TEXT,
                address TEXT,
                website TEXT,
                state VARCHAR(50),
                county VARCHAR(100),
                city VARCHAR(100),
                zip_code VARCHAR(100),
                address_state VARCHAR(50),
                filing_date DATE,
                election_date DATE,
                facebook TEXT,
                twitter TEXT,
                processing_timestamp TIMESTAMP,
                pipeline_version VARCHAR(100),
                data_source VARCHAR(255),
                first_added_date TIMESTAMP,
                last_updated_date TIMESTAMP,
                data_hash VARCHAR(100),
                
                -- Staging Action Columns
                staging_action VARCHAR(50) DEFAULT 'pending_review',  -- auto_promoted, pending_review, error_fallback
                staging_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                staging_reason TEXT,
                quality_score DECIMAL(3,2),
                promotion_status VARCHAR(50),
                review_timestamp TIMESTAMP,
                review_reason TEXT,
                error_timestamp TIMESTAMP,
                error_message TEXT
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
    
    def recreate_staging_table(self) -> bool:
        """Drop and recreate the staging_candidates table with the new schema."""
        try:
            # Drop existing table if it exists
            drop_table_sql = "DROP TABLE IF EXISTS staging_candidates CASCADE;"
            
            with self.engine.connect() as conn:
                conn.execute(text(drop_table_sql))
                conn.commit()
            
            logger.info("Dropped existing staging_candidates table")
            
            # Create new table with updated schema
            return self._create_staging_table()
            
        except Exception as e:
            logger.error(f"Failed to recreate staging table: {e}")
            return False
    
    def add_source_office_to_filings(self) -> bool:
        """Add source_office column to the filings table if it doesn't exist."""
        try:
            if not self.engine:
                logger.error("No database connection available")
                return False
            
            # Check if source_office column already exists
            check_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'filings' AND column_name = 'source_office'
            """
            result = self.execute_query(check_query)
            
            if not result.empty:
                logger.info("source_office column already exists in filings table")
                return True
            
            # Add source_office column
            logger.info("Adding source_office column to filings table...")
            add_column_query = """
            ALTER TABLE filings 
            ADD COLUMN source_office TEXT
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(add_column_query))
                conn.commit()
            
            logger.info("✅ source_office column added to filings table successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add source_office column to filings table: {e}")
            return False
    
    def add_source_district_to_filings(self) -> bool:
        """Add source_district column to the filings table if it doesn't exist."""
        try:
            if not self.engine:
                logger.error("No database connection available")
                return False
            
            # Check if source_district column already exists
            check_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'filings' AND column_name = 'source_district'
            """
            result = self.execute_query(check_query)
            
            if not result.empty:
                logger.info("source_district column already exists in filings table")
                return True
            
            # Add source_district column
            logger.info("Adding source_district column to filings table...")
            add_column_query = """
            ALTER TABLE filings 
            ADD COLUMN source_district TEXT
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(add_column_query))
                conn.commit()
            
            logger.info("✅ source_district column added to filings table successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add source_district column to filings table: {e}")
            return False

# Global database manager instance
db_manager = DatabaseManager()

def get_db_connection() -> DatabaseManager:
    """Get the global database manager instance."""
    return db_manager
