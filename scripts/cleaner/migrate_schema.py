#!/usr/bin/env python3
"""
Database Schema Migration Script

This script migrates the existing database tables to the new consolidated schema
where office_standardized becomes the main office field.
"""

import sys
import os
sys.path.append('../src/config')
sys.path.append('../src/pipeline')

from database import DatabaseManager
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SchemaMigrator:
    """Handles database schema migration to consolidated office field approach."""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        
    def migrate_staging_table(self) -> bool:
        """Migrate staging_candidates table to new schema."""
        logger.info("🔄 Starting staging table migration...")
        
        try:
            if not self.db_manager.connect():
                logger.error("Failed to connect to database")
                return False
            
            # Check if we need to migrate
            if not self._needs_staging_migration():
                logger.info("✅ Staging table already has the new schema")
                return True
            
            logger.info("📋 Staging table needs migration, proceeding...")
            
            # Create backup of current staging data
            logger.info("💾 Creating backup of current staging data...")
            backup_success = self._backup_staging_table()
            if not backup_success:
                logger.error("Failed to create staging backup")
                return False
            
            # Add new columns
            logger.info("➕ Adding new columns to staging table...")
            new_columns = [
                ('external_id', 'VARCHAR(100)'),
                ('office_confidence', 'DECIMAL(3,2)'),
                ('office_category', 'VARCHAR(100)'),
                ('party_standardized', 'VARCHAR(100)'),
                ('address_parsed', 'BOOLEAN'),
                ('address_clean', 'TEXT'),
                ('has_zip', 'BOOLEAN'),
                ('has_state', 'BOOLEAN'),
                ('source_state', 'VARCHAR(50)'),
                ('processing_timestamp', 'TIMESTAMP'),
                ('pipeline_version', 'BIGINT'),
                ('data_source', 'VARCHAR(255)')
            ]
            
            for col_name, col_type in new_columns:
                if not self._column_exists('staging_candidates', col_name):
                    self._add_column('staging_candidates', col_name, col_type)
                    logger.info(f"  ✅ Added column: {col_name}")
                else:
                    logger.info(f"  ℹ️  Column already exists: {col_name}")
            
            # Consolidate office fields
            logger.info("🔄 Consolidating office fields...")
            self._consolidate_staging_office_fields()
            
            logger.info("✅ Staging table migration completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Staging table migration failed: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
        finally:
            self.db_manager.disconnect()
    
    def migrate_filings_table(self) -> bool:
        """Migrate filings table to new schema."""
        logger.info("🔄 Starting filings table migration...")
        
        try:
            if not self.db_manager.connect():
                logger.error("Failed to connect to database")
                return False
            
            # Check if we need to migrate
            if not self._needs_filings_migration():
                logger.info("✅ Filings table already has the new schema")
                return True
            
            logger.info("📋 Filings table needs migration, proceeding...")
            
            # Create backup of current filings data
            logger.info("💾 Creating backup of current filings data...")
            backup_success = self._backup_filings_table()
            if not backup_success:
                logger.error("Failed to create filings backup")
                return False
            
            # Add new columns
            logger.info("➕ Adding new columns to filings table...")
            new_columns = [
                ('staging_id', 'BIGINT'),
                ('external_id', 'VARCHAR(100)'),
                ('nickname', 'VARCHAR(100)'),
                ('office_confidence', 'DECIMAL(3,2)'),
                ('office_category', 'VARCHAR(100)'),
                ('party_standardized', 'VARCHAR(100)'),
                ('address_parsed', 'BOOLEAN'),
                ('address_clean', 'TEXT'),
                ('has_zip', 'BOOLEAN'),
                ('has_state', 'BOOLEAN'),
                ('source_state', 'VARCHAR(50)'),
                ('processing_timestamp', 'TIMESTAMP'),
                ('pipeline_version', 'BIGINT'),
                ('data_source', 'VARCHAR(255)')
            ]
            
            for col_name, col_type in new_columns:
                if not self._column_exists('filings', col_name):
                    self._add_column('filings', col_name, col_type)
                    logger.info(f"  ✅ Added column: {col_name}")
                else:
                    logger.info(f"  ℹ️  Column already exists: {col_name}")
            
            logger.info("✅ Filings table migration completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Filings table migration failed: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
        finally:
            self.db_manager.disconnect()
    
    def _needs_staging_migration(self) -> bool:
        """Check if staging table needs migration."""
        required_columns = [
            'external_id', 'office_confidence', 'office_category', 'party_standardized',
            'address_parsed', 'address_clean', 'has_zip', 'has_state',
            'source_state', 'processing_timestamp', 'pipeline_version', 'data_source'
        ]
        
        for col in required_columns:
            if not self._column_exists('staging_candidates', col):
                return True
        return False
    
    def _needs_filings_migration(self) -> bool:
        """Check if filings table needs migration."""
        required_columns = [
            'staging_id', 'external_id', 'nickname', 'office_confidence', 'office_category',
            'party_standardized', 'address_parsed', 'address_clean', 'has_zip', 'has_state',
            'source_state', 'processing_timestamp', 'pipeline_version', 'data_source'
        ]
        
        for col in required_columns:
            if not self._column_exists('filings', col):
                return True
        return False
    
    def _column_exists(self, table_name: str, column_name: str) -> bool:
        """Check if a column exists in a table."""
        query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = :table_name 
            AND column_name = :column_name
        );
        """
        result = self.db_manager.execute_query(query, {
            'table_name': table_name,
            'column_name': column_name
        })
        return result.iloc[0, 0] if not result.empty else False
    
    def _add_column(self, table_name: str, column_name: str, column_type: str) -> bool:
        """Add a new column to a table."""
        try:
            query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};"
            with self.db_manager.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to add column {column_name}: {e}")
            return False
    
    def _backup_staging_table(self) -> bool:
        """Create a backup of the staging table."""
        try:
            backup_name = f"staging_candidates_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            query = f"CREATE TABLE {backup_name} AS SELECT * FROM staging_candidates;"
            
            with self.db_manager.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
            
            # Get backup count
            count_query = f"SELECT COUNT(*) FROM {backup_name};"
            count_result = self.db_manager.execute_query(count_query)
            backup_count = count_result.iloc[0, 0] if not count_result.empty else 0
            
            logger.info(f"✅ Created staging backup: {backup_name} with {backup_count:,} records")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create staging backup: {e}")
            return False
    
    def _backup_filings_table(self) -> bool:
        """Create a backup of the filings table."""
        try:
            backup_name = f"filings_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            query = f"CREATE TABLE {backup_name} AS SELECT * FROM filings;"
            
            with self.db_manager.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
            
            # Get backup count
            count_query = f"SELECT COUNT(*) FROM {backup_name};"
            count_result = self.db_manager.execute_query(count_query)
            backup_count = count_result.iloc[0, 0] if not count_result.empty else 0
            
            logger.info(f"✅ Created filings backup: {backup_name} with {backup_count:,} records")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create filings backup: {e}")
            return False
    
    def _consolidate_staging_office_fields(self) -> bool:
        """Consolidate office fields in staging table."""
        try:
            # Check if office_standardized column exists and has data
            check_query = """
            SELECT COUNT(*) as total, 
                   COUNT(office_standardized) as standardized_count
            FROM staging_candidates;
            """
            
            result = self.db_manager.execute_query(check_query)
            if result.empty:
                logger.warning("Could not check office field consolidation")
                return False
            
            total = result.iloc[0]['total']
            standardized = result.iloc[0]['standardized_count']
            
            logger.info(f"Office field consolidation: {standardized:,} out of {total:,} records have standardized office")
            
            # If we have standardized office data, consolidate the fields
            if standardized > 0:
                update_query = """
                UPDATE staging_candidates 
                SET office = COALESCE(office_standardized, office)
                WHERE office_standardized IS NOT NULL;
                """
                
                with self.db_manager.engine.connect() as conn:
                    conn.execute(text(update_query))
                    conn.commit()
                
                logger.info("✅ Consolidated office fields: standardized office is now the main office field")
            else:
                logger.info("ℹ️  No standardized office data to consolidate")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to consolidate office fields: {e}")
            return False

def main():
    """Main function to run the schema migration."""
    migrator = SchemaMigrator()
    
    print("🔄 CandidateFilings Database Schema Migration")
    print("=" * 50)
    print("This will migrate your tables to use consolidated office fields")
    print("and add all the new columns for enhanced functionality.")
    print()
    
    # Migrate staging table
    print("📋 Migrating staging_candidates table...")
    staging_success = migrator.migrate_staging_table()
    
    if staging_success:
        print("✅ Staging table migration completed")
    else:
        print("❌ Staging table migration failed")
        return
    
    print()
    
    # Migrate filings table
    print("📋 Migrating filings table...")
    filings_success = migrator.migrate_filings_table()
    
    if filings_success:
        print("✅ Filings table migration completed")
    else:
        print("❌ Filings table migration failed")
        return
    
    print()
    print("🎉 Schema migration completed successfully!")
    print("Your tables now have:")
    print("  • Consolidated office fields (standardized office is the main field)")
    print("  • All new columns for enhanced functionality")
    print("  • Backups of your original data")
    print()
    print("Next steps:")
    print("  1. Run your pipeline to populate the new fields")
    print("  2. Use move_to_production.py to transfer data with the new schema")

if __name__ == "__main__":
    main()
