#!/usr/bin/env python3
"""
Test Database Connection

This script tests the connection to your Supabase database.
Run this to verify your database configuration is working.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.append(str(src_path))

from config.database import get_db_connection
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_database_connection():
    """Test the database connection and basic operations."""
    logger.info("Testing database connection...")
    
    # Get database manager
    db = get_db_connection()
    
    # Test connection
    if not db.connect():
        logger.error("Failed to connect to database")
        return False
    
    # Test basic operations
    if not db.test_connection():
        logger.error("Database connection test failed")
        return False
    
    # Test a simple query
    logger.info("Testing basic query...")
    result = db.execute_query("SELECT current_timestamp as current_time")
    if not result.empty:
        logger.info(f"Query successful: {result.iloc[0, 0]}")
    else:
        logger.error("Basic query failed")
        return False
    
    # Test table operations
    logger.info("Testing table operations...")
    test_table = "test_connection_table"
    
    # Check if test table exists
    if db.table_exists(test_table):
        logger.info(f"Test table '{test_table}' already exists")
    else:
        logger.info(f"Test table '{test_table}' does not exist")
    
    # Get table info (should work even if table doesn't exist)
    table_info = db.get_table_info(test_table)
    logger.info(f"Table info query completed (rows: {len(table_info)})")
    
    # Clean up
    db.disconnect()
    logger.info("Database connection test completed successfully!")
    return True

def main():
    """Main function to run the database test."""
    logger.info("Starting database connection test...")
    
    # Test the connection (will use Docker defaults if no env vars)
    success = test_database_connection()
    
    if success:
        logger.info("✅ Database connection test PASSED")
        logger.info("Your Supabase database is ready to use!")
    else:
        logger.error("❌ Database connection test FAILED")
        logger.error("Please check your configuration and try again")
    
    return success

if __name__ == "__main__":
    main()
