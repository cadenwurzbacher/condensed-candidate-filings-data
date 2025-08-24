#!/usr/bin/env python3
"""
Manual Data Operations

This script handles manual data operations including:
- Manual record deletions
- Manual review of staging data
- Override staging decisions
- Data cleanup operations
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.config.database import get_db_connection
import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ManualDataOperator:
    """Handles manual data operations and overrides"""
    
    def __init__(self):
        self.db = get_db_connection()
        
    def delete_record(self, stable_id: str, reason: str = "Manual deletion") -> bool:
        """
        Manually delete a record from production
        
        Args:
            stable_id: The stable ID to delete
            reason: Reason for deletion
            
        Returns:
            Success status
        """
        print(f"🗑️  MANUAL RECORD DELETION")
        print(f"Stable ID: {stable_id}")
        print(f"Reason: {reason}")
        print("=" * 50)
        
        try:
            # First, check if record exists
            existing = self.db.execute_query(
                "SELECT * FROM filings WHERE stable_id = %s", (stable_id,)
            )
            
            if existing.empty:
                print(f"❌ Record with stable_id {stable_id} not found")
                return False
            
            # Show what will be deleted
            print(f"📋 Record to be deleted:")
            record = existing.iloc[0]
            print(f"  Name: {record.get('full_name_display', 'N/A')}")
            print(f"  Office: {record.get('office', 'N/A')}")
            print(f"  State: {record.get('state', 'N/A')}")
            print(f"  Election Year: {record.get('election_year', 'N/A')}")
            
            # Confirm deletion
            confirm = input(f"\n⚠️  Are you sure you want to delete this record? (yes/no): ").lower().strip()
            if confirm != 'yes':
                print("❌ Deletion cancelled")
                return False
            
            # Perform deletion
            delete_success = self.db.execute_query(
                "DELETE FROM filings WHERE stable_id = %s", (stable_id,)
            )
            
            if delete_success is not None:
                # Log deletion in audit table
                self._log_deletion(stable_id, reason, record)
                print(f"✅ Record deleted successfully")
                return True
            else:
                print(f"❌ Failed to delete record")
                return False
                
        except Exception as e:
            print(f"❌ Deletion failed: {e}")
            logger.error(f"Failed to delete record {stable_id}: {e}")
            return False
    
    def _log_deletion(self, stable_id: str, reason: str, record_data: pd.Series):
        """Log deletion in audit table"""
        try:
            # Create audit table if it doesn't exist
            create_audit_table = """
            CREATE TABLE IF NOT EXISTS data_audit_log (
                id SERIAL PRIMARY KEY,
                operation_type VARCHAR(50),
                stable_id VARCHAR(100),
                operation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                operation_reason TEXT,
                record_data JSONB,
                operator VARCHAR(100) DEFAULT 'manual'
            )
            """
            self.db.execute_query(create_audit_table)
            
            # Log the deletion
            record_json = record_data.to_json()
            log_query = """
            INSERT INTO data_audit_log 
            (operation_type, stable_id, operation_reason, record_data)
            VALUES (%s, %s, %s, %s)
            """
            self.db.execute_query(log_query, ('DELETE', stable_id, reason, record_json))
            
        except Exception as e:
            logger.warning(f"Failed to log deletion: {e}")
    
    def review_staging_data(self) -> bool:
        """Review staging data and make manual decisions"""
        print(f"📋 STAGING DATA REVIEW")
        print("=" * 50)
        
        try:
            # Get staging data
            staging_data = self.db.execute_query("SELECT * FROM staging_candidates")
            
            if staging_data.empty:
                print("ℹ️  No data in staging table")
                return True
            
            print(f"📊 Staging Data Overview:")
            print(f"  Total Records: {len(staging_data):,}")
            
            # Show promotion status breakdown
            if 'promotion_status' in staging_data.columns:
                status_counts = staging_data['promotion_status'].value_counts()
                print(f"  Promotion Status:")
                for status, count in status_counts.items():
                    print(f"    {status}: {count:,}")
            
            # Show quality scores
            if 'quality_score' in staging_data.columns:
                avg_quality = staging_data['quality_score'].mean()
                print(f"  Average Quality Score: {avg_quality:.3f}")
            
            # Show sample records
            print(f"\n📋 Sample Records:")
            sample_cols = ['stable_id', 'full_name_display', 'office', 'state', 'promotion_status']
            available_cols = [col for col in sample_cols if col in staging_data.columns]
            
            if available_cols:
                print(staging_data[available_cols].head(10).to_string())
            
            # Interactive review options
            print(f"\n🔧 Review Options:")
            print(f"  1. Promote all to production")
            print(f"  2. Promote specific records")
            print(f"  3. Reject all (keep in staging)")
            print(f"  4. View detailed record")
            print(f"  5. Exit")
            
            choice = input(f"\nSelect option (1-5): ").strip()
            
            if choice == '1':
                return self._promote_all_staging()
            elif choice == '2':
                return self._promote_specific_records(staging_data)
            elif choice == '3':
                return self._reject_all_staging()
            elif choice == '4':
                return self._view_detailed_record(staging_data)
            elif choice == '5':
                print("👋 Review session ended")
                return True
            else:
                print("❌ Invalid choice")
                return False
                
        except Exception as e:
            print(f"❌ Review failed: {e}")
            logger.error(f"Failed to review staging data: {e}")
            return False
    
    def _promote_all_staging(self) -> bool:
        """Promote all staging data to production"""
        print(f"🚀 Promoting all staging data to production...")
        
        try:
            # Get staging data
            staging_data = self.db.execute_query("SELECT * FROM staging_candidates")
            
            if staging_data.empty:
                print("ℹ️  No data to promote")
                return True
            
            # Confirm promotion
            confirm = input(f"⚠️  Promote {len(staging_data):,} records to production? (yes/no): ").lower().strip()
            if confirm != 'yes':
                print("❌ Promotion cancelled")
                return False
            
            # Clear existing production data and insert staging data
            self.db.execute_query("DELETE FROM filings")
            
            # Remove staging metadata columns before inserting
            staging_cols = staging_data.columns.tolist()
            metadata_cols = ['promotion_status', 'review_timestamp', 'review_reason', 
                           'quality_score', 'staging_timestamp', 'staging_status', 'pipeline_run_id']
            
            production_cols = [col for col in staging_cols if col not in metadata_cols]
            production_data = staging_data[production_cols].copy()
            
            # Set timestamps
            production_data['first_added_date'] = datetime.now()
            production_data['last_updated_date'] = datetime.now()
            
            # Upload to production
            success = self.db.upload_dataframe(
                production_data, 'filings', if_exists='append', index=False
            )
            
            if success:
                print(f"✅ Successfully promoted {len(production_data):,} records to production")
                
                # Clear staging table
                self.db.execute_query("DELETE FROM staging_candidates")
                print(f"🧹 Staging table cleared")
                
                return True
            else:
                print(f"❌ Failed to promote data to production")
                return False
                
        except Exception as e:
            print(f"❌ Promotion failed: {e}")
            logger.error(f"Failed to promote staging data: {e}")
            return False
    
    def _promote_specific_records(self, staging_data: pd.DataFrame) -> bool:
        """Promote specific records from staging"""
        print(f"🎯 Selective Record Promotion")
        print("=" * 50)
        
        try:
            # Show records with selection options
            for idx, record in staging_data.iterrows():
                print(f"{idx + 1}. {record.get('full_name_display', 'N/A')} - {record.get('office', 'N/A')} ({record.get('state', 'N/A')})")
            
            # Get user selection
            selection = input(f"\nEnter record numbers to promote (comma-separated, e.g., 1,3,5): ").strip()
            
            try:
                selected_indices = [int(x.strip()) - 1 for x in selection.split(',')]
                selected_records = staging_data.iloc[selected_indices]
                
                print(f"\n📋 Selected Records:")
                print(selected_records[['full_name_display', 'office', 'state']].to_string())
                
                # Confirm promotion
                confirm = input(f"\n⚠️  Promote {len(selected_records)} selected records? (yes/no): ").lower().strip()
                if confirm != 'yes':
                    print("❌ Promotion cancelled")
                    return False
                
                # Prepare production data
                production_cols = [col for col in staging_data.columns 
                                if col not in ['promotion_status', 'review_timestamp', 'review_reason', 
                                             'quality_score', 'staging_timestamp', 'staging_status', 'pipeline_run_id']]
                
                production_data = selected_records[production_cols].copy()
                production_data['first_added_date'] = datetime.now()
                production_data['last_updated_date'] = datetime.now()
                
                # Upload to production
                success = self.db.upload_dataframe(
                    production_data, 'filings', if_exists='append', index=False
                )
                
                if success:
                    print(f"✅ Successfully promoted {len(production_data)} selected records")
                    
                    # Remove promoted records from staging
                    remaining_staging = staging_data.drop(selected_records.index)
                    self.db.upload_dataframe(
                        remaining_staging, 'staging_candidates', if_exists='replace', index=False
                    )
                    print(f"🧹 Updated staging table ({len(remaining_staging)} records remaining)")
                    
                    return True
                else:
                    print(f"❌ Failed to promote selected records")
                    return False
                    
            except (ValueError, IndexError) as e:
                print(f"❌ Invalid selection: {e}")
                return False
                
        except Exception as e:
            print(f"❌ Selective promotion failed: {e}")
            logger.error(f"Failed to promote specific records: {e}")
            return False
    
    def _reject_all_staging(self) -> bool:
        """Reject all staging data (keep in staging for review)"""
        print(f"❌ Rejecting all staging data")
        print("=" * 50)
        
        try:
            # Update staging status
            update_query = """
            UPDATE staging_candidates 
            SET promotion_status = 'rejected',
                review_timestamp = CURRENT_TIMESTAMP
            """
            
            success = self.db.execute_query(update_query)
            
            if success is not None:
                print(f"✅ All staging data marked as rejected")
                print(f"📋 Data remains in staging for further review")
                return True
            else:
                print(f"❌ Failed to reject staging data")
                return False
                
        except Exception as e:
            print(f"❌ Rejection failed: {e}")
            logger.error(f"Failed to reject staging data: {e}")
            return False
    
    def _view_detailed_record(self, staging_data: pd.DataFrame) -> bool:
        """View detailed information about a specific record"""
        print(f"🔍 Detailed Record View")
        print("=" * 50)
        
        try:
            # Show record list
            for idx, record in staging_data.iterrows():
                print(f"{idx + 1}. {record.get('full_name_display', 'N/A')} - {record.get('office', 'N/A')}")
            
            # Get user selection
            selection = input(f"\nEnter record number to view: ").strip()
            
            try:
                record_idx = int(selection) - 1
                record = staging_data.iloc[record_idx]
                
                print(f"\n📋 Record Details:")
                print(f"  Stable ID: {record.get('stable_id', 'N/A')}")
                print(f"  Name: {record.get('full_name_display', 'N/A')}")
                print(f"  Office: {record.get('office', 'N/A')}")
                print(f"  State: {record.get('state', 'N/A')}")
                print(f"  Party: {record.get('party', 'N/A')}")
                print(f"  Election Year: {record.get('election_year', 'N/A')}")
                print(f"  Address: {record.get('address', 'N/A')}")
                print(f"  Quality Score: {record.get('quality_score', 'N/A')}")
                print(f"  Promotion Status: {record.get('promotion_status', 'N/A')}")
                
                return True
                
            except (ValueError, IndexError) as e:
                print(f"❌ Invalid selection: {e}")
                return False
                
        except Exception as e:
            print(f"❌ Detailed view failed: {e}")
            logger.error(f"Failed to view detailed record: {e}")
            return False

def main():
    """Main function for manual data operations"""
    operator = ManualDataOperator()
    
    print("🔧 MANUAL DATA OPERATIONS")
    print("=" * 50)
    print("1. Delete a record")
    print("2. Review staging data")
    print("3. Exit")
    
    choice = input(f"\nSelect option (1-3): ").strip()
    
    if choice == '1':
        stable_id = input("Enter stable_id to delete: ").strip()
        reason = input("Enter reason for deletion: ").strip()
        operator.delete_record(stable_id, reason)
    elif choice == '2':
        operator.review_staging_data()
    elif choice == '3':
        print("👋 Goodbye!")
    else:
        print("❌ Invalid choice")

if __name__ == "__main__":
    main()
