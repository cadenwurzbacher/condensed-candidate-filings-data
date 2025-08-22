#!/usr/bin/env python3
"""
Add New Columns to Staging Table
"""

from src.config.database import get_db_connection

def add_new_columns():
    """Add new columns to staging_candidates table."""
    db = get_db_connection()
    
    if not db.connect():
        print("❌ Failed to connect to database")
        return False
    
    print("=== ADDING NEW COLUMNS TO STAGING TABLE ===")
    
    # Columns to add
    new_columns = [
        ("office_standardized", "VARCHAR(50)"),
        ("office_confidence", "DECIMAL(3,2)"),
        ("office_category", "VARCHAR(50)"),
        ("ward", "VARCHAR(50)"),
        ("precinct", "VARCHAR(50)"),
        ("address_parsed", "BOOLEAN DEFAULT FALSE"),
        ("processing_status", "VARCHAR(20) DEFAULT 'pending'")
    ]
    
    for column_name, column_type in new_columns:
        try:
            query = f"ALTER TABLE staging_candidates ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
            db.execute_query(query)
            print(f"✅ Added {column_name}")
        except Exception as e:
            print(f"❌ Failed to add {column_name}: {e}")
    
    print("\n=== VERIFYING NEW COLUMNS ===")
    try:
        result = db.execute_query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'staging_candidates' "
            "AND column_name IN ('office_standardized', 'office_confidence', 'office_category', 'ward', 'precinct', 'address_parsed', 'processing_status')"
        )
        if not result.empty:
            print("New columns found:")
            for _, row in result.iterrows():
                print(f"  - {row['column_name']}")
        else:
            print("No new columns found")
    except Exception as e:
        print(f"Error checking columns: {e}")
    
    db.disconnect()
    return True

if __name__ == "__main__":
    add_new_columns()
