#!/usr/bin/env python3
"""
Add Change Tracking Columns

Add columns to track what changed in the staging table:
- change_type: new, updated, unchanged
- change_timestamp: when the change was detected
- previous_values: JSON of previous values for comparison
"""

import sys
import os
sys.path.append('src/config')

from database import get_db_connection
from sqlalchemy import text

def main():
    """Add change tracking columns to staging table."""
    print("🔧 Adding Change Tracking Columns...")
    
    db = get_db_connection()
    
    try:
        if not db.connect():
            print("❌ Failed to connect to database")
            return
        
        print("✅ Connected to database successfully")
        
        # Columns to add for change tracking
        change_tracking_columns = [
            ('change_type', 'VARCHAR(20)', 'unchanged'),  # new, updated, unchanged
            ('change_timestamp', 'TIMESTAMP', 'CURRENT_TIMESTAMP'),
            ('previous_values', 'JSONB', 'NULL'),  # Store previous values for comparison
            ('change_summary', 'TEXT', 'NULL'),  # Human-readable summary of changes
            ('is_conflict', 'BOOLEAN', 'FALSE'),  # Flag for data conflicts
            ('merge_strategy', 'VARCHAR(50)', 'staging_wins')  # How to handle conflicts
        ]
        
        print(f"\n➕ Adding {len(change_tracking_columns)} change tracking columns...")
        
        for col_name, col_type, default_value in change_tracking_columns:
            # Check if column already exists
            exists_query = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'staging_candidates' 
                AND column_name = '{col_name}'
            );
            """
            
            exists_result = db.execute_query(exists_query)
            if exists_result.iloc[0, 0]:
                print(f"  ℹ️  {col_name} already exists, skipping")
                continue
            
            # Add the column
            if default_value == 'NULL':
                add_query = f"ALTER TABLE staging_candidates ADD COLUMN {col_name} {col_type};"
            elif default_value == 'CURRENT_TIMESTAMP':
                add_query = f"ALTER TABLE staging_candidates ADD COLUMN {col_name} {col_type} DEFAULT CURRENT_TIMESTAMP;"
            else:
                add_query = f"ALTER TABLE staging_candidates ADD COLUMN {col_name} {col_type} DEFAULT '{default_value}';"
            
            try:
                with db.engine.connect() as conn:
                    conn.execute(text(add_query))
                    conn.commit()
                print(f"  ✅ Added column: {col_name}")
            except Exception as e:
                print(f"  ❌ Failed to add {col_name}: {e}")
        
        # Verify all columns were added
        print("\n🔍 Verifying change tracking columns...")
        staging_columns = db.execute_query("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'staging_candidates'
            ORDER BY column_name;
        """)
        
        staging_cols = set(staging_columns['column_name'])
        required_cols = {col[0] for col in change_tracking_columns}
        
        missing_cols = required_cols - staging_cols
        if missing_cols:
            print(f"  ❌ Missing columns: {missing_cols}")
        else:
            print("  ✅ All change tracking columns added successfully!")
        
        print(f"\n📋 Change Tracking Columns Added:")
        for col_name, col_type, default_value in change_tracking_columns:
            print(f"  • {col_name} ({col_type}) - Default: {default_value}")
        
        print(f"\n🎯 How Change Tracking Works:")
        print(f"  • change_type: 'new', 'updated', or 'unchanged'")
        print(f"  • change_timestamp: When the change was detected")
        print(f"  • previous_values: JSON of old values for comparison")
        print(f"  • change_summary: Human-readable description of changes")
        print(f"  • is_conflict: Flag for data conflicts that need manual review")
        print(f"  • merge_strategy: How to handle conflicts (staging_wins, production_wins, manual)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        db.disconnect()
        print("\n🔌 Disconnected from database")

if __name__ == "__main__":
    main()
