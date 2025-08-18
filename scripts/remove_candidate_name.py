#!/usr/bin/env python3
"""
Remove candidate_name Column

Remove the redundant candidate_name column from both staging and filings tables.
We'll use full_name_display instead.
"""

import sys
import os
sys.path.append('../src/config')

from database import get_db_connection
from sqlalchemy import text

def main():
    """Remove candidate_name column from both tables."""
    print("🗑️  Removing candidate_name Column...")
    
    db = get_db_connection()
    
    try:
        if not db.connect():
            print("❌ Failed to connect to database")
            return
        
        print("✅ Connected to database successfully")
        
        # Check if candidate_name exists in both tables
        print("\n🔍 Checking for candidate_name columns...")
        
        staging_has_candidate_name = db.execute_query("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'staging_candidates' 
                AND column_name = 'candidate_name'
            );
        """).iloc[0, 0]
        
        filings_has_candidate_name = db.execute_query("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'filings' 
                AND column_name = 'candidate_name'
            );
        """).iloc[0, 0]
        
        print(f"  Staging table: {'✅ Has' if staging_has_candidate_name else '❌ Missing'} candidate_name")
        print(f"  Filings table: {'✅ Has' if filings_has_candidate_name else '❌ Missing'} candidate_name")
        
        # Remove from staging table
        if staging_has_candidate_name:
            print("\n🗑️  Removing candidate_name from staging_candidates...")
            try:
                with db.engine.connect() as conn:
                    conn.execute(text("ALTER TABLE staging_candidates DROP COLUMN candidate_name;"))
                    conn.commit()
                print("  ✅ Removed candidate_name from staging table")
            except Exception as e:
                print(f"  ❌ Failed to remove from staging: {e}")
        else:
            print("  ℹ️  candidate_name not found in staging table")
        
        # Remove from filings table
        if filings_has_candidate_name:
            print("\n🗑️  Removing candidate_name from filings...")
            try:
                with db.engine.connect() as conn:
                    conn.execute(text("ALTER TABLE filings DROP COLUMN candidate_name;"))
                    conn.commit()
                print("  ✅ Removed candidate_name from filings table")
            except Exception as e:
                print(f"  ❌ Failed to remove from filings: {e}")
        else:
            print("  ℹ️  candidate_name not found in filings table")
        
        # Verify removal
        print("\n🔍 Verifying candidate_name removal...")
        
        staging_has_candidate_name_after = db.execute_query("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'staging_candidates' 
                AND column_name = 'candidate_name'
            );
        """).iloc[0, 0]
        
        filings_has_candidate_name_after = db.execute_query("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'filings' 
                AND column_name = 'candidate_name'
            );
        """).iloc[0, 0]
        
        if not staging_has_candidate_name_after and not filings_has_candidate_name_after:
            print("  ✅ candidate_name successfully removed from both tables!")
        else:
            print("  ❌ candidate_name still exists in some tables")
            if staging_has_candidate_name_after:
                print("    - Still in staging_candidates")
            if filings_has_candidate_name_after:
                print("    - Still in filings")
        
        print("\n📋 Summary:")
        print("  • candidate_name column removed from both tables")
        print("  • Use full_name_display for candidate names")
        print("  • This eliminates redundancy in your schema")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        db.disconnect()
        print("\n🔌 Disconnected from database")

if __name__ == "__main__":
    main()
