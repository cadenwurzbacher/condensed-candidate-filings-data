#!/usr/bin/env python3
"""
Fix Database Alignment

This script fixes misalignments between the actual database schema
and what the pipeline scripts expect, ensuring all columns exist
with the correct data types.
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.config.database import get_db_connection
import logging
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_database_alignment():
    """Fix all database column misalignments."""
    
    print("🔧 FIXING DATABASE ALIGNMENT")
    print("=" * 60)
    
    db = get_db_connection()
    
    try:
        # Ensure database connection is established
        print("🔌 Establishing database connection...")
        if not db.connect():
            print("❌ Failed to connect to database")
            return False
        
        print("✅ Database connection established")
        
        # Fix staging_candidates table
        print(f"\n🔧 Fixing staging_candidates table...")
        fix_staging_table(db)
        
        # Fix filings table
        print(f"\n🔧 Fixing filings table...")
        fix_filings_table(db)
        
        print(f"\n✅ Database alignment completed successfully!")
        print(f"\n📋 Final Schema Summary:")
        show_final_schema(db)
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to fix database alignment: {e}")
        logger.error(f"Failed to fix database alignment: {e}")
        return False

def fix_staging_table(db):
    """Fix staging_candidates table schema."""
    try:
        # Check current columns
        staging_info = db.get_table_info('staging_candidates')
        staging_cols = set(staging_info['column_name'].tolist())
        
        print(f"  Current columns: {len(staging_cols)}")
        
        # Add missing columns with direct SQL execution
        missing_columns = {
            'office_category': 'VARCHAR(100)',
            'external_id': 'VARCHAR(100)',
            'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        }
        
        for col_name, col_type in missing_columns.items():
            if col_name not in staging_cols:
                try:
                    # Use direct SQL execution to ensure transaction commits
                    with db.engine.connect() as conn:
                        conn.execute(text(f"""
                            ALTER TABLE staging_candidates 
                            ADD COLUMN {col_name} {col_type}
                        """))
                        conn.commit()
                    print(f"    ✅ Added {col_name} column")
                except Exception as e:
                    print(f"    ⚠️  Warning adding {col_name}: {e}")
            else:
                print(f"    ℹ️  {col_name} column already exists")
        
        # Verify date columns exist
        if 'first_added_date' not in staging_cols:
            with db.engine.connect() as conn:
                conn.execute(text("""
                    ALTER TABLE staging_candidates 
                    ADD COLUMN first_added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                """))
                conn.commit()
            print(f"    ✅ Added first_added_date column")
        else:
            print(f"    ℹ️  first_added_date column already exists")
            
        if 'last_updated_date' not in staging_cols:
            with db.engine.connect() as conn:
                conn.execute(text("""
                    ALTER TABLE staging_candidates 
                    ADD COLUMN last_updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                """))
                conn.commit()
            print(f"    ✅ Added last_updated_date column")
        else:
            print(f"    ℹ️  last_updated_date column already exists")
        
        # Fix data type issues
        print(f"  🔧 Fixing data type issues...")
        
        # Fix stable_id to VARCHAR if it's currently numeric
        stable_id_info = staging_info[staging_info['column_name'] == 'stable_id']
        if not stable_id_info.empty and 'character varying' not in stable_id_info.iloc[0]['data_type']:
            try:
                with db.engine.connect() as conn:
                    conn.execute(text("""
                        ALTER TABLE staging_candidates 
                        ALTER COLUMN stable_id TYPE VARCHAR(100)
                    """))
                    conn.commit()
                print(f"    ✅ Fixed stable_id data type to VARCHAR")
            except Exception as e:
                print(f"    ⚠️  Warning fixing stable_id type: {e}")
        
        # Fix election_year to INTEGER if it's currently numeric
        election_year_info = staging_info[staging_info['column_name'] == 'election_year']
        if not election_year_info.empty and 'integer' not in election_year_info.iloc[0]['data_type']:
            try:
                with db.engine.connect() as conn:
                    conn.execute(text("""
                        ALTER TABLE staging_candidates 
                        ALTER COLUMN election_year TYPE INTEGER USING election_year::integer
                    """))
                    conn.commit()
                print(f"    ✅ Fixed election_year data type to INTEGER")
            except Exception as e:
                print(f"    ⚠️  Warning fixing election_year type: {e}")
        
        print(f"  ✅ Staging table fixes completed")
        
    except Exception as e:
        print(f"  ❌ Failed to fix staging table: {e}")
        logger.error(f"Failed to fix staging table: {e}")

def fix_filings_table(db):
    """Fix filings table schema."""
    try:
        # Check current columns
        filings_info = db.get_table_info('filings')
        filings_cols = set(filings_info['column_name'].tolist())
        
        print(f"  Current columns: {len(filings_cols)}")
        
        # Add missing columns with direct SQL execution
        missing_columns = {
            'first_added_date': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'last_updated_date': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        }
        
        for col_name, col_type in missing_columns.items():
            if col_name not in filings_cols:
                try:
                    # Use direct SQL execution to ensure transaction commits
                    with db.engine.connect() as conn:
                        conn.execute(text(f"""
                            ALTER TABLE filings 
                            ADD COLUMN {col_name} {col_type}
                        """))
                        conn.commit()
                    print(f"    ✅ Added {col_name} column")
                except Exception as e:
                    print(f"    ⚠️  Warning adding {col_name}: {e}")
            else:
                print(f"    ℹ️  {col_name} column already exists")
        
        # Fix data type issues
        print(f"  🔧 Fixing data type issues...")
        
        # Fix stable_id to VARCHAR if it's not already
        stable_id_info = filings_info[filings_info['column_name'] == 'stable_id']
        if not stable_id_info.empty and 'character varying' not in stable_id_info.iloc[0]['data_type']:
            try:
                with db.engine.connect() as conn:
                    conn.execute(text("""
                        ALTER TABLE filings 
                        ALTER COLUMN stable_id TYPE VARCHAR(100)
                    """))
                    conn.commit()
                print(f"    ✅ Fixed stable_id data type to VARCHAR")
            except Exception as e:
                print(f"    ⚠️  Warning fixing stable_id type: {e}")
        
        # Fix election_year to INTEGER if it's not already
        election_year_info = filings_info[filings_info['column_name'] == 'election_year']
        if not election_year_info.empty and 'integer' not in election_year_info.iloc[0]['data_type']:
            try:
                with db.engine.connect() as conn:
                    conn.execute(text("""
                        ALTER TABLE filings 
                        ALTER COLUMN election_year TYPE INTEGER USING election_year::integer
                    """))
                    conn.commit()
                print(f"    ✅ Fixed election_year data type to INTEGER")
            except Exception as e:
                print(f"    ⚠️  Warning fixing election_year type: {e}")
        
        print(f"  ✅ Filings table fixes completed")
        
    except Exception as e:
        print(f"    ❌ Failed to fix filings table: {e}")
        logger.error(f"Failed to fix filings table: {e}")

def show_final_schema(db):
    """Show the final schema after fixes."""
    try:
        print(f"\n📋 STAGING_CANDIDATES TABLE:")
        staging_info = db.get_table_info('staging_candidates')
        date_columns = staging_info[staging_info['column_name'].str.contains('date|created|updated', case=False, na=False)]
        for _, col in date_columns.iterrows():
            print(f"  - {col['column_name']}: {col['data_type']}")
        
        print(f"\n📋 FILINGS TABLE:")
        filings_info = db.get_table_info('filings')
        date_columns = filings_info[filings_info['column_name'].str.contains('date|created|updated', case=False, na=False)]
        for _, col in date_columns.iterrows():
            print(f"  - {col['column_name']}: {col['data_type']}")
        
        # Show total column counts
        print(f"\n📊 FINAL COLUMN COUNTS:")
        print(f"  staging_candidates: {len(staging_info)} columns")
        print(f"  filings: {len(filings_info)} columns")
        
    except Exception as e:
        print(f"  ⚠️  Warning displaying final schema: {e}")

def main():
    """Main function to fix database alignment."""
    success = fix_database_alignment()
    
    if success:
        print("\n🎉 Database alignment completed successfully!")
        print("Your database schema is now aligned with the pipeline scripts.")
    else:
        print("\n❌ Database alignment failed. Check the logs for details.")

if __name__ == "__main__":
    main()
