#!/usr/bin/env python3
"""
Remove office_standardized Column

Remove the office_standardized column from both staging and filings tables.
We're using the consolidated approach where standardized names go directly in the 'office' field.
"""

import sys
import os
sys.path.append('../src/config')

from database import get_db_connection
from sqlalchemy import text

def main():
    """Remove office_standardized column from both tables."""
    print("🗑️  Removing office_standardized Column...")
    
    db = get_db_connection()
    
    try:
        if not db.connect():
            print("❌ Failed to connect to database")
            return
        
        print("✅ Connected to database successfully")
        
        # Check if office_standardized exists in both tables
        print("\n🔍 Checking for office_standardized columns...")
        
        staging_has_office_std = db.execute_query("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'staging_candidates' 
                AND column_name = 'office_standardized'
            );
        """).iloc[0, 0]
        
        filings_has_office_std = db.execute_query("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'filings' 
                AND column_name = 'office_standardized'
            );
        """).iloc[0, 0]
        
        print(f"  Staging table: {'✅ Has' if staging_has_office_std else '❌ Missing'} office_standardized")
        print(f"  Filings table: {'✅ Has' if filings_has_office_std else '❌ Missing'} office_standardized")
        
        # Remove from staging table
        if staging_has_office_std:
            print("\n🗑️  Removing office_standardized from staging_candidates...")
            try:
                with db.engine.connect() as conn:
                    conn.execute(text("ALTER TABLE staging_candidates DROP COLUMN office_standardized;"))
                    conn.commit()
                print("  ✅ Removed office_standardized from staging table")
            except Exception as e:
                print(f"  ❌ Failed to remove from staging: {e}")
        else:
            print("  ℹ️  office_standardized not found in staging table")
        
        # Remove from filings table
        if filings_has_office_std:
            print("\n🗑️  Removing office_standardized from filings...")
            try:
                with db.engine.connect() as conn:
                    conn.execute(text("ALTER TABLE filings DROP COLUMN office_standardized;"))
                    conn.commit()
                print("  ✅ Removed office_standardized from filings table")
            except Exception as e:
                print(f"  ❌ Failed to remove from filings: {e}")
        else:
            print("  ℹ️  office_standardized not found in filings table")
        
        # Verify removal
        print("\n🔍 Verifying office_standardized removal...")
        
        staging_has_office_std_after = db.execute_query("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'staging_candidates' 
                AND column_name = 'office_standardized'
            );
        """).iloc[0, 0]
        
        filings_has_office_std_after = db.execute_query("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'filings' 
                AND column_name = 'office_standardized'
            );
        """).iloc[0, 0]
        
        if not staging_has_office_std_after and not filings_has_office_std_after:
            print("  ✅ office_standardized successfully removed from both tables!")
        else:
            print("  ❌ office_standardized still exists in some tables")
            if staging_has_office_std_after:
                print("    - Still in staging_candidates")
            if filings_has_office_std_after:
                print("    - Still in filings")
        
        print("\n📋 Summary:")
        print("  • office_standardized column removed from both tables")
        print("  • Using consolidated approach: standardized names go directly in 'office' field")
        print("  • Original messy names preserved in 'original_office' field")
        print("  • This eliminates the duplicate office field confusion")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        db.disconnect()
        print("\n🔌 Disconnected from database")

if __name__ == "__main__":
    main()
