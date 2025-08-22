#!/usr/bin/env python3
"""
Script to inspect Supabase database tables and show column information
for staging and filings tables.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from config.database import get_db_connection
import pandas as pd

def main():
    """Main function to inspect database tables."""
    print("🔍 Inspecting Supabase database tables...")
    
    # Get database connection
    db = get_db_connection()
    
    try:
        # Connect to database
        if not db.connect():
            print("❌ Failed to connect to database")
            return
        
        print("✅ Connected to database successfully")
        
        # Check if tables exist
        staging_exists = db.table_exists('staging_candidates')
        filings_exists = db.table_exists('filings')
        
        print(f"\n📊 Table Status:")
        print(f"  staging_candidates: {'✅ Exists' if staging_exists else '❌ Does not exist'}")
        print(f"  filings: {'✅ Exists' if filings_exists else '❌ Does not exist'}")
        
        # Get staging table info
        if staging_exists:
            print(f"\n📋 staging_candidates table columns:")
            staging_info = db.get_table_info('staging_candidates')
            if not staging_info.empty:
                for _, row in staging_info.iterrows():
                    nullable = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
                    default = f" DEFAULT {row['column_default']}" if row['column_default'] else ""
                    print(f"  • {row['column_name']:<25} {row['data_type']:<20} {nullable}{default}")
            else:
                print("  No column information available")
        
        # Get filings table info
        if filings_exists:
            print(f"\n📋 filings table columns:")
            filings_info = db.get_table_info('filings')
            if not filings_info.empty:
                for _, row in filings_info.iterrows():
                    nullable = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
                    default = f" DEFAULT {row['column_default']}" if row['column_default'] else ""
                    print(f"  • {row['column_name']:<25} {row['data_type']:<20} {nullable}{default}")
            else:
                print("  No column information available")
        
        # Get record counts
        if staging_exists:
            staging_count = db.get_staging_record_count()
            print(f"\n📈 staging_candidates record count: {staging_count:,}")
        
        if filings_exists:
            filings_count_query = "SELECT COUNT(*) FROM filings"
            filings_count_result = db.execute_query(filings_count_query)
            if not filings_count_result.empty:
                filings_count = int(filings_count_result.iloc[0, 0])
                print(f"📈 filings record count: {filings_count:,}")
        
        # List all tables in the database
        print(f"\n🔍 All tables in database:")
        all_tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name;
        """
        all_tables = db.execute_query(all_tables_query)
        if not all_tables.empty:
            for _, row in all_tables.iterrows():
                print(f"  • {row['table_name']}")
        else:
            print("  No tables found")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Disconnect from database
        db.disconnect()
        print("\n🔌 Disconnected from database")

if __name__ == "__main__":
    main()
