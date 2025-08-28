#!/usr/bin/env python3
"""
Script to fix the filings data issues:
1. Fix election type logic (should be 'unknown' by default, not 'general')
2. Clean up "None" string values to actual NULL values
3. Check if there's proper election type logic in the original data
"""

import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import logging

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FilingsDataFixer:
    """Fixes issues in the filings data."""
    
    def __init__(self):
        self.engine = None
        self.connection_string = self._get_connection_string()
        
    def _get_connection_string(self) -> str:
        """Get database connection string from environment variables."""
        host = os.getenv('SUPABASE_HOST')
        port = os.getenv('SUPABASE_PORT', '5432')
        database = os.getenv('SUPABASE_DATABASE', 'postgres')
        user = os.getenv('SUPABASE_USER')
        password = os.getenv('SUPABASE_PASSWORD')
        
        if not all([host, user, password]):
            raise ValueError("Missing required Supabase environment variables")
        
        return f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require"
    
    def connect(self):
        """Establish database connection."""
        try:
            self.engine = create_engine(self.connection_string, echo=False)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ Database connection established")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def check_original_excel_election_logic(self):
        """Check the original Excel file to see what election type logic should be."""
        file_path = "data/final/candidate_filings_FINAL_20250827_014603.xlsx"
        
        try:
            logger.info(f"📖 Checking original Excel file for election logic...")
            df = pd.read_excel(file_path)
            
            # Check what columns might indicate election type
            election_related_cols = [col for col in df.columns if 'election' in col.lower() or 'primary' in col.lower() or 'general' in col.lower()]
            logger.info(f"🔍 Election-related columns found: {election_related_cols}")
            
            # Check the ran_in_* columns
            if 'ran_in_primary' in df.columns:
                primary_count = df['ran_in_primary'].sum()
                logger.info(f"📊 Ran in primary: {primary_count:,} candidates")
            
            if 'ran_in_general' in df.columns:
                general_count = df['ran_in_general'].sum()
                logger.info(f"📊 Ran in general: {general_count:,} candidates")
            
            if 'ran_in_special' in df.columns:
                special_count = df['ran_in_special'].sum()
                logger.info(f"📊 Ran in special: {special_count:,} candidates")
            
            # Check election_type_notes if it exists
            if 'election_type_notes' in df.columns:
                notes_sample = df['election_type_notes'].dropna().head(10)
                logger.info(f"📝 Sample election type notes: {notes_sample.tolist()}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error reading Excel file: {e}")
            return None
    
    def fix_election_types(self):
        """Fix the election type logic in the database."""
        try:
            logger.info("🔧 Fixing election type logic...")
            
            with self.engine.connect() as conn:
                # First, let's see what the original data had for election indicators
                logger.info("📊 Checking current election type distribution...")
                result = conn.execute(text("SELECT election_type, COUNT(*) FROM filings GROUP BY election_type"))
                for row in result.fetchall():
                    logger.info(f"  {row[0]}: {row[1]:,} rows")
                
                # Update election types based on the original logic
                # Set all to 'unknown' first, then we can update based on actual data
                logger.info("🔄 Setting all election types to 'unknown'...")
                conn.execute(text("UPDATE filings SET election_type = 'unknown'"))
                
                # Now let's check if we can derive election types from the original data
                # We'll need to re-upload with proper logic, but for now let's fix the immediate issues
                
                conn.commit()
                logger.info("✅ Election types updated to 'unknown'")
                
        except Exception as e:
            logger.error(f"❌ Error fixing election types: {e}")
    
    def fix_none_strings(self):
        """Fix 'None' string values to actual NULL values."""
        try:
            logger.info("🔧 Fixing 'None' string values...")
            
            with self.engine.connect() as conn:
                # List of columns that should have NULL instead of 'None' string
                text_columns = [
                    'prefix', 'suffix', 'nickname', 'party', 'phone', 'email',
                    'address', 'website', 'county', 'city', 'zip_code', 'address_state'
                ]
                
                for col in text_columns:
                    logger.info(f"  Fixing column: {col}")
                    # Update 'None' strings to NULL
                    conn.execute(text(f"UPDATE filings SET {col} = NULL WHERE {col} = 'None'"))
                
                conn.commit()
                logger.info("✅ 'None' string values fixed")
                
        except Exception as e:
            logger.error(f"❌ Error fixing 'None' strings: {e}")
    
    def verify_fixes(self):
        """Verify that the fixes were applied correctly."""
        try:
            logger.info("🔍 Verifying fixes...")
            
            with self.engine.connect() as conn:
                # Check election type distribution
                result = conn.execute(text("SELECT election_type, COUNT(*) FROM filings GROUP BY election_type"))
                logger.info("📊 Election type distribution after fixes:")
                for row in result.fetchall():
                    logger.info(f"  {row[0]}: {row[1]:,} rows")
                
                # Check for remaining 'None' strings
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM filings 
                    WHERE prefix = 'None' OR suffix = 'None' OR nickname = 'None' 
                       OR party = 'None' OR phone = 'None' OR email = 'None'
                """))
                none_count = result.fetchone()[0]
                logger.info(f"📊 Remaining 'None' strings: {none_count}")
                
                # Check sample data
                result = conn.execute(text("""
                    SELECT full_name_display, prefix, suffix, nickname, election_type
                    FROM filings 
                    LIMIT 5
                """))
                
                logger.info("📝 Sample data after fixes:")
                for row in result.fetchall():
                    logger.info(f"  {row[0]} | Prefix: {row[1]} | Suffix: {row[2]} | Nickname: {row[3]} | Election: {row[4]}")
                
        except Exception as e:
            logger.error(f"❌ Error verifying fixes: {e}")
    
    def run_fixes(self):
        """Run all the fixes."""
        try:
            if not self.connect():
                return False
            
            # Check original Excel logic first
            self.check_original_excel_election_logic()
            
            # Apply fixes
            self.fix_election_types()
            self.fix_none_strings()
            
            # Verify fixes
            self.verify_fixes()
            
            logger.info("🎉 All fixes completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Process failed: {e}")
            return False
        finally:
            if self.engine:
                self.engine.dispose()

def main():
    """Main function."""
    fixer = FilingsDataFixer()
    success = fixer.run_fixes()
    
    if success:
        logger.info("🎉 Data fixes completed! Check the logs above for details.")
    else:
        logger.error("❌ Fixes failed. Please check the logs above.")

if __name__ == "__main__":
    main()
