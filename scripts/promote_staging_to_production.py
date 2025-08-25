#!/usr/bin/env python3
"""
Promote Staging Data to Production

This script handles the promotion of data from staging_candidates to filings
with proper INSERT/UPDATE/DELETE logic based on action_type.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from config.database import get_db_connection
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def promote_staging_to_production():
    """Promote staging data to production based on action_type"""
    logger.info("Starting staging to production promotion...")
    
    try:
        # Get database connection
        db = get_db_connection()
        
        # Get staging data
        staging_data = db.execute_query("SELECT * FROM staging_candidates ORDER BY stable_id")
        
        if staging_data.empty:
            logger.info("No staging data found")
            return
        
        logger.info(f"Found {len(staging_data)} records in staging")
        
        # Get existing production data for comparison
        existing_data = db.execute_query("SELECT stable_id FROM filings")
        existing_ids = set(existing_data['stable_id'].tolist()) if not existing_data.empty else set()
        
        # Process each action type
        insert_count = 0
        update_count = 0
        delete_count = 0
        no_change_count = 0
        
        for idx, row in staging_data.iterrows():
            action_type = row.get('action_type', 'INSERT')
            stable_id = row.get('stable_id')
            
            if not stable_id:
                continue
                
            try:
                if action_type == 'INSERT':
                    # Insert new record
                    insert_result = insert_record(db, row)
                    if insert_result:
                        insert_count += 1
                        logger.debug(f"INSERTED: {stable_id}")
                    
                elif action_type == 'UPDATE':
                    # Update existing record
                    update_result = update_record(db, row)
                    if update_result:
                        update_count += 1
                        logger.debug(f"UPDATED: {stable_id}")
                    
                elif action_type == 'DELETE':
                    # Delete record
                    delete_result = delete_record(db, stable_id)
                    if delete_result:
                        delete_count += 1
                        logger.debug(f"DELETED: {stable_id}")
                    
                elif action_type == 'NO_CHANGE':
                    # No changes needed
                    no_change_count += 1
                    logger.debug(f"NO_CHANGE: {stable_id}")
                    
                else:
                    logger.warning(f"Unknown action_type '{action_type}' for {stable_id}")
                    
            except Exception as e:
                logger.error(f"Failed to process {stable_id} with action {action_type}: {e}")
                continue
        
        # Log summary
        logger.info("=== PROMOTION SUMMARY ===")
        logger.info(f"INSERT: {insert_count}")
        logger.info(f"UPDATE: {update_count}")
        logger.info(f"DELETE: {delete_count}")
        logger.info(f"NO_CHANGE: {no_change_count}")
        logger.info(f"TOTAL: {len(staging_data)}")
        
        # Clear staging table after successful promotion
        if insert_count + update_count + delete_count > 0:
            db.execute_query("DELETE FROM staging_candidates")
            logger.info("Cleared staging table after successful promotion")
        
        logger.info("✅ Staging to production promotion completed successfully!")
        
    except Exception as e:
        logger.error(f"Promotion failed: {e}")
        return False
    
    return True

def insert_record(db, row):
    """Insert a new record into filings table"""
    try:
        # Prepare data for insertion
        insert_data = prepare_record_data(row)
        
        # Build INSERT query
        columns = list(insert_data.keys())
        placeholders = ', '.join([f'%s'] * len(columns))
        query = f"""
        INSERT INTO filings ({', '.join(columns)})
        VALUES ({placeholders})
        """
        
        # Execute insert
        db.execute_query(query, list(insert_data.values()))
        return True
        
    except Exception as e:
        logger.error(f"Insert failed for {row.get('stable_id')}: {e}")
        return False

def update_record(db, row):
    """Update an existing record in filings table"""
    try:
        # Prepare data for update
        update_data = prepare_record_data(row)
        stable_id = row.get('stable_id')
        
        # Remove stable_id from update data (it's the WHERE clause)
        if 'stable_id' in update_data:
            del update_data['stable_id']
        
        # Build UPDATE query
        set_clause = ', '.join([f'{col} = %s' for col in update_data.keys()])
        query = f"""
        UPDATE filings 
        SET {set_clause}
        WHERE stable_id = %s
        """
        
        # Execute update
        values = list(update_data.values()) + [stable_id]
        db.execute_query(query, values)
        return True
        
    except Exception as e:
        logger.error(f"Update failed for {row.get('stable_id')}: {e}")
        return False

def delete_record(db, stable_id):
    """Delete a record from filings table"""
    try:
        query = "DELETE FROM filings WHERE stable_id = %s"
        db.execute_query(query, (stable_id,))
        return True
        
    except Exception as e:
        logger.error(f"Delete failed for {stable_id}: {e}")
        return False

def prepare_record_data(row):
    """Prepare record data for database operations"""
    # Define the columns we want to insert/update
    target_columns = [
        'stable_id', 'election_year', 'election_type', 'office', 'district',
        'full_name_display', 'first_name', 'middle_name', 'last_name', 'prefix',
        'suffix', 'nickname', 'party', 'phone', 'email', 'address', 'website',
        'state', 'county', 'city', 'zip_code', 'address_state', 'filing_date',
        'election_date', 'facebook', 'twitter', 'processing_timestamp',
        'pipeline_version', 'data_source', 'first_added_date', 'last_updated_date'
    ]
    
    # Extract data for target columns
    record_data = {}
    for col in target_columns:
        if col in row:
            value = row[col]
            
            # Handle date columns
            if col in ['filing_date', 'election_date'] and pd.notna(value):
                try:
                    if isinstance(value, str):
                        value = pd.to_datetime(value).date()
                    else:
                        value = pd.to_datetime(value).date()
                except:
                    value = None
            
            # Handle timestamp columns
            elif col in ['processing_timestamp', 'first_added_date', 'last_updated_date'] and pd.notna(value):
                try:
                    if isinstance(value, str):
                        value = pd.to_datetime(value)
                    else:
                        value = pd.to_datetime(value)
                except:
                    value = datetime.now()
            
            record_data[col] = value
    
    return record_data

def main():
    """Main function"""
    logger.info("Starting staging to production promotion script")
    
    success = promote_staging_to_production()
    
    if success:
        logger.info("🎉 Promotion completed successfully!")
        return 0
    else:
        logger.error("❌ Promotion failed!")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
