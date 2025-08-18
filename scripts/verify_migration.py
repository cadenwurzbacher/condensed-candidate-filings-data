#!/usr/bin/env python3
"""
Verify Migration Results

Simple script to check that the schema migration worked correctly.
"""

import sys
import os
sys.path.append('../src/config')

from database import get_db_connection

def main():
    """Verify the migration results."""
    print("🔍 Verifying Migration Results...")
    
    db = get_db_connection()
    
    try:
        if not db.connect():
            print("❌ Failed to connect to database")
            return
        
        print("✅ Connected to database successfully")
        
        # Check office field consolidation
        print("\n📋 Verifying Office Field Consolidation...")
        result = db.execute_query("""
            SELECT 
                COUNT(*) as total, 
                COUNT(CASE WHEN office != original_office THEN 1 END) as standardized 
            FROM staging_candidates;
        """)
        
        if not result.empty:
            total = result.iloc[0]['total']
            standardized = result.iloc[0]['standardized']
            print(f"  Total records: {total:,}")
            print(f"  Records with standardized office: {standardized:,}")
            print(f"  Percentage standardized: {standardized/total*100:.1f}%")
            
            if standardized > 0:
                print("  ✅ Office consolidation successful!")
            else:
                print("  ⚠️  No office consolidation detected")
        
        # Check some examples
        print("\n📋 Sample Office Consolidations:")
        examples = db.execute_query("""
            SELECT original_office, office 
            FROM staging_candidates 
            WHERE office != original_office 
            LIMIT 5;
        """)
        
        if not examples.empty:
            for _, row in examples.iterrows():
                print(f"  \"{row['original_office']}\" → \"{row['office']}\"")
        else:
            print("  No examples found")
        
        # Check new columns in filings table
        print("\n📋 New Columns in Filings Table:")
        new_columns = [
            'staging_id', 'external_id', 'nickname', 'office_confidence', 
            'office_category', 'party_standardized', 'address_parsed', 
            'address_clean', 'has_zip', 'has_state', 'source_state', 
            'processing_timestamp', 'pipeline_version', 'data_source'
        ]
        
        for col in new_columns:
            exists = db.execute_query(f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'filings' 
                    AND column_name = '{col}'
                );
            """)
            status = "✅" if exists.iloc[0, 0] else "❌"
            print(f"  {status} {col}")
        
        # Check backup tables
        print("\n📋 Backup Tables Created:")
        backups = db.execute_query("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE '%backup%'
            ORDER BY table_name;
        """)
        
        if not backups.empty:
            for _, row in backups.iterrows():
                print(f"  ✅ {row['table_name']}")
        else:
            print("  No backup tables found")
        
        print("\n🎉 Migration verification completed!")
        
    except Exception as e:
        print(f"❌ Error during verification: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        db.disconnect()
        print("\n🔌 Disconnected from database")

if __name__ == "__main__":
    main()
