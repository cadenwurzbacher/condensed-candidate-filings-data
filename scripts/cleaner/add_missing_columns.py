#!/usr/bin/env python3
"""
Add Missing Columns to Filings Table

Add the columns that exist in staging but not in filings.
"""

import sys
import os
sys.path.append('../src/config')

from database import get_db_connection
from sqlalchemy import text

def main():
    """Add missing columns to filings table."""
    print("🔧 Adding Missing Columns to Filings Table...")
    
    db = get_db_connection()
    
    try:
        if not db.connect():
            print("❌ Failed to connect to database")
            return
        
        print("✅ Connected to database successfully")
        
        # Columns to add
        missing_columns = [
            ('prefix', 'VARCHAR(50)'),
            ('suffix', 'VARCHAR(50)')
        ]
        
        print(f"\n➕ Adding {len(missing_columns)} missing columns...")
        
        for col_name, col_type in missing_columns:
            # Check if column already exists
            exists_query = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'filings' 
                AND column_name = '{col_name}'
            );
            """
            
            exists_result = db.execute_query(exists_query)
            if exists_result.iloc[0, 0]:
                print(f"  ℹ️  {col_name} already exists, skipping")
                continue
            
            # Add the column
            add_query = f"ALTER TABLE filings ADD COLUMN {col_name} {col_type};"
            
            try:
                with db.engine.connect() as conn:
                    conn.execute(text(add_query))
                    conn.commit()
                print(f"  ✅ Added column: {col_name}")
            except Exception as e:
                print(f"  ❌ Failed to add {col_name}: {e}")
        
        print("\n🎯 Note about office_standardized:")
        print("  • office_standardized is intentionally NOT added to filings")
        print("  • We're using the consolidated approach: standardized names go in 'office' field")
        print("  • Original messy names are preserved in 'original_office' field")
        
        # Verify all columns are now present
        print("\n🔍 Verifying all columns are now present...")
        staging_columns = db.execute_query("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'staging_candidates'
            ORDER BY column_name;
        """)
        
        filings_columns = db.execute_query("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'filings'
            ORDER BY column_name;
        """)
        
        staging_cols = set(staging_columns['column_name'])
        filings_cols = set(filings_columns['column_name'])
        
        # Remove office_standardized from comparison (intentionally different)
        staging_cols.discard('office_standardized')
        
        missing = staging_cols - filings_cols
        if missing:
            print(f"  ❌ Still missing columns: {missing}")
        else:
            print("  ✅ All staging columns (except office_standardized) are now in filings!")
        
        print(f"\n📊 Final Summary:")
        print(f"  Staging columns: {len(staging_cols)}")
        print(f"  Filings columns: {len(filings_cols)}")
        print(f"  Missing columns: {len(missing)}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        db.disconnect()
        print("\n🔌 Disconnected from database")

if __name__ == "__main__":
    main()
