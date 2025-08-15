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
            logger.warning("Missing Supabase environment variables. Using fallback defaults.")
            # Fallback defaults (these won't work but provide clear error)
            host = 'localhost'
            port = '5432'
            user = 'postgres'
            password = 'missing_password'
            database = 'postgres'
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

# Global database manager instance
db_manager = DatabaseManager()

def get_db_connection() -> DatabaseManager:
    """Get the global database manager instance."""
    return db_manager
