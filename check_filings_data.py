#!/usr/bin/env python3
"""
Script to check the current state of filings data in the database
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()

def check_filings_data():
    """Check the current state of filings data in the database."""
    
    # Get database connection details from environment
    host = os.getenv('SUPABASE_HOST')
    port = os.getenv('SUPABASE_PORT', '5432')
    database = os.getenv('SUPABASE_DATABASE', 'postgres')
    user = os.getenv('SUPABASE_USER')
    password = os.getenv('SUPABASE_PASSWORD')
    
    if not all([host, user, password]):
        print("Error: Missing required Supabase environment variables")
        return
    
    # Create connection string
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require"
    
    try:
        # Create engine
        engine = create_engine(connection_string, echo=False)
        
        # Test connection
        with engine.connect() as conn:
            print("✅ Successfully connected to Supabase database")
            
            # Check sample data for names
            print("\n🔍 Sample data for names:")
            print("-" * 50)
            result = conn.execute(text("""
                SELECT full_name_display, first_name, middle_name, last_name, 
                       prefix, suffix, nickname, election_type
                FROM filings 
                LIMIT 10
            """))
            
            for row in result.fetchall():
                print(f"Full: '{row[0]}' | First: '{row[1]}' | Middle: '{row[2]}' | Last: '{row[3]}' | Prefix: '{row[4]}' | Suffix: '{row[5]}' | Nickname: '{row[6]}' | Election Type: '{row[7]}'")
            
            # Check election type distribution
            print("\n📊 Election type distribution:")
            print("-" * 50)
            result = conn.execute(text("""
                SELECT election_type, COUNT(*) as count
                FROM filings 
                GROUP BY election_type
                ORDER BY count DESC
            """))
            
            for row in result.fetchall():
                print(f"'{row[0]}': {row[1]:,} rows")
            
            # Check for dots in names
            print("\n🔍 Checking for dots in names:")
            print("-" * 50)
            result = conn.execute(text("""
                SELECT full_name_display, first_name, last_name
                FROM filings 
                WHERE full_name_display LIKE '%.%' 
                   OR first_name LIKE '%.%' 
                   OR last_name LIKE '%.%'
                LIMIT 5
            """))
            
            dot_count = 0
            for row in result.fetchall():
                print(f"Full: '{row[0]}' | First: '{row[1]}' | Last: '{row[2]}'")
                dot_count += 1
            
            if dot_count == 0:
                print("No names with dots found in sample")
            else:
                print(f"Found {dot_count} names with dots in sample")
            
            # Check total rows
            result = conn.execute(text("SELECT COUNT(*) FROM filings"))
            total_rows = result.fetchone()[0]
            print(f"\n📊 Total rows in database: {total_rows:,}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_filings_data()
