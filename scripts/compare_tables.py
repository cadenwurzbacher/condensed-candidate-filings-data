#!/usr/bin/env python3
"""
Compare Table Schemas

Compare columns between staging_candidates and filings tables.
"""

import sys
import os
sys.path.append('src/config')

from database import get_db_connection

def main():
    """Compare the schemas of staging and filings tables."""
    print("🔍 Comparing Table Schemas...")
    
    db = get_db_connection()
    
    try:
        if not db.connect():
            print("❌ Failed to connect to database")
            return
        
        print("✅ Connected to database successfully")
        
        # Get staging table columns
        print("\n📋 Staging Table Columns:")
        staging_columns = db.execute_query("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = 'staging_candidates'
            ORDER BY ordinal_position;
        """)
        
        staging_cols = []
        for _, row in staging_columns.iterrows():
            nullable = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
            print(f"  • {row['column_name']:<25} {row['data_type']:<20} {nullable}")
            staging_cols.append(row['column_name'])
        
        # Get filings table columns
        print("\n📋 Filings Table Columns:")
        filings_columns = db.execute_query("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = 'filings'
            ORDER BY ordinal_position;
        """)
        
        filings_cols = []
        for _, row in filings_columns.iterrows():
            nullable = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
            print(f"  • {row['column_name']:<25} {row['data_type']:<20} {nullable}")
            filings_cols.append(row['column_name'])
        
        # Find differences
        print("\n🔍 Column Analysis:")
        
        # Columns in staging but not in filings
        staging_only = set(staging_cols) - set(filings_cols)
        if staging_only:
            print(f"\n❌ Columns in STAGING but NOT in FILINGS ({len(staging_only)}):")
            for col in sorted(staging_only):
                print(f"  • {col}")
        else:
            print("\n✅ All staging columns are present in filings")
        
        # Columns in filings but not in staging
        filings_only = set(filings_cols) - set(staging_cols)
        if filings_only:
            print(f"\n➕ Columns in FILINGS but NOT in STAGING ({len(filings_only)}):")
            for col in sorted(filings_only):
                print(f"  • {col}")
        else:
            print("\n✅ All filings columns are present in staging")
        
        # Check specific columns
        print("\n🎯 Specific Column Checks:")
        
        # Check nickname
        nickname_in_staging = 'nickname' in staging_cols
        nickname_in_filings = 'nickname' in filings_cols
        print(f"  nickname: {'✅' if nickname_in_staging else '❌'} in staging, {'✅' if nickname_in_filings else '❌'} in filings")
        
        # Check office_standardized
        office_std_in_staging = 'office_standardized' in staging_cols
        office_std_in_filings = 'office_standardized' in filings_cols
        print(f"  office_standardized: {'✅' if office_std_in_staging else '❌'} in staging, {'✅' if office_std_in_filings else '❌'} in filings")
        
        # Check original_office
        orig_office_in_staging = 'original_office' in staging_cols
        orig_office_in_filings = 'original_office' in filings_cols
        print(f"  original_office: {'✅' if orig_office_in_staging else '❌'} in staging, {'✅' if orig_office_in_filings else '❌'} in filings")
        
        # Summary
        print(f"\n📊 Summary:")
        print(f"  Staging columns: {len(staging_cols)}")
        print(f"  Filings columns: {len(filings_cols)}")
        print(f"  Columns to add to filings: {len(staging_only)}")
        print(f"  Extra columns in filings: {len(filings_only)}")
        
    except Exception as e:
        print(f"❌ Error during comparison: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        db.disconnect()
        print("\n🔌 Disconnected from database")

if __name__ == "__main__":
    main()
