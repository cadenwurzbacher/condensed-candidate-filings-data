#!/usr/bin/env python3
"""
Script to add source_office column to the filings table.
"""

import sys
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.append(str(src_path))

from config.database import get_db_connection

def main():
    print("Adding source_office and source_district columns to filings table...")
    print("=" * 60)
    
    try:
        # Get database connection
        db_manager = get_db_connection()
        
        # Test connection
        if not db_manager.connect():
            print("❌ Failed to connect to database")
            return False
        
        print("✅ Database connection established")
        
        # Add source_office column
        print("Adding source_office column...")
        success = db_manager.add_source_office_to_filings()
        
        if success:
            print("✅ source_office column added successfully to filings table")
        else:
            print("❌ Failed to add source_office column")
            return False
        
        # Add source_district column
        print("Adding source_district column...")
        success = db_manager.add_source_district_to_filings()
        
        if success:
            print("✅ source_district column added successfully to filings table")
        else:
            print("❌ Failed to add source_district column")
            return False
        
        # Verify both columns were added
        print("Verifying column additions...")
        table_info = db_manager.get_table_info('filings')
        
        if not table_info.empty:
            source_office_exists = 'source_office' in table_info['column_name'].values
            source_district_exists = 'source_district' in table_info['column_name'].values
            
            if source_office_exists and source_district_exists:
                print("✅ Both source_office and source_district columns verified in filings table")
            else:
                print("❌ Some columns not found in filings table")
                print(f"   source_office: {'✅' if source_office_exists else '❌'}")
                print(f"   source_district: {'✅' if source_district_exists else '❌'}")
        else:
            print("⚠️ Could not verify table structure")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        if 'db_manager' in locals():
            db_manager.disconnect()
            print("Database connection closed")

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 Both source_office and source_district columns successfully added to filings table!")
    else:
        print("\n💥 Failed to add source_office and source_district columns to filings table")
        sys.exit(1)
