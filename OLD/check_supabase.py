#!/usr/bin/env python3
"""
Simple script to check Supabase connection and explore database structure.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Your Supabase credentials
SUPABASE_URL = "https://bnvpsoppbufldabquoec.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJudnBzb3BwYnVmbGRhYnF1b2VjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTc2NzYyMCwiZXhwIjoyMDY3MzQzNjIwfQ.4phfSXHEpuA35nGbpxNBuXmIpcypmviFisakLIWRDAE"

def test_connection():
    """Test the Supabase connection."""
    try:
        # Construct connection string
        connection_string = "postgresql://postgres.bnvpsoppbufldabquoec:Undsioux1!!!@aws-0-us-east-2.pooler.supabase.com:5432/postgres"
        
        # Create engine
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as connection:
            # Test basic query
            result = connection.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            logger.info(f"✅ Connected successfully!")
            logger.info(f"Database version: {version.split(',')[0]}")
            
            return engine
            
    except Exception as e:
        logger.error(f"❌ Connection failed: {e}")
        return None

def explore_database(engine):
    """Explore the database structure."""
    try:
        with engine.connect() as connection:
            # Get list of tables
            result = connection.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """))
            
            tables = [row[0] for row in result.fetchall()]
            logger.info(f"📋 Tables found: {tables}")
            
            # Explore each table
            for table in tables:
                logger.info(f"\n🔍 Exploring table: {table}")
                
                # Get table structure
                result = connection.execute(text(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = '{table}'
                    ORDER BY ordinal_position
                """))
                
                columns = result.fetchall()
                logger.info(f"Columns in {table}:")
                for col in columns:
                    logger.info(f"  - {col[0]}: {col[1]} ({'NULL' if col[2] == 'YES' else 'NOT NULL'})")
                
                # Get row count
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.fetchone()[0]
                logger.info(f"Row count: {count:,}")
                
                # Show sample data
                if count > 0:
                    result = connection.execute(text(f"SELECT * FROM {table} LIMIT 3"))
                    sample_data = result.fetchall()
                    logger.info(f"Sample data:")
                    for row in sample_data:
                        logger.info(f"  {row}")
                
    except Exception as e:
        logger.error(f"❌ Error exploring database: {e}")

def main():
    """Main function."""
    logger.info("🔌 Testing Supabase connection...")
    
    # Test connection
    engine = test_connection()
    
    if engine:
        logger.info("\n🔍 Exploring database structure...")
        explore_database(engine)
    else:
        logger.error("❌ Cannot explore database without connection")

if __name__ == "__main__":
    main() 