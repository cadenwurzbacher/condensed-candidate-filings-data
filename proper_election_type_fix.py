#!/usr/bin/env python3
"""
Script to properly fix election types based on the original Excel data logic
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

class ElectionTypeFixer:
    """Properly fixes election types based on original Excel logic."""
    
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
    
    def create_election_type_mapping(self):
        """Create a mapping of stable_id to proper election type based on original Excel data."""
        file_path = "data/final/candidate_filings_FINAL_20250827_014603.xlsx"
        
        try:
            logger.info(f"📖 Reading original Excel file to create election type mapping...")
            df = pd.read_excel(file_path)
            
            # Create election type mapping based on the ran_in_* columns
            election_mapping = {}
            
            for idx, row in df.iterrows():
                stable_id = row.get('stable_id')
                if pd.isna(stable_id):
                    continue
                
                # Determine election type based on ran_in_* columns
                ran_in_primary = row.get('ran_in_primary', False)
                ran_in_general = row.get('ran_in_general', False)
                ran_in_special = row.get('ran_in_special', False)
                
                # Check election_type_notes for special cases
                election_notes = row.get('election_type_notes', '')
                
                if pd.isna(election_notes):
                    election_notes = ''
                
                # Logic: if there are notes mentioning special, or ran_in_special is True, it's special
                if ran_in_special or 'special' in str(election_notes).lower():
                    election_type = 'special'
                elif ran_in_primary and ran_in_general:
                    election_type = 'both'
                elif ran_in_primary:
                    election_type = 'primary'
                elif ran_in_general:
                    election_type = 'general'
                else:
                    # Default to unknown if we can't determine
                    election_type = 'unknown'
                
                election_mapping[stable_id] = election_type
            
            logger.info(f"✅ Created election type mapping for {len(election_mapping):,} records")
            
            # Log distribution
            type_counts = {}
            for election_type in election_mapping.values():
                type_counts[election_type] = type_counts.get(election_type, 0) + 1
            
            logger.info("📊 Election type distribution in mapping:")
            for election_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  {election_type}: {count:,}")
            
            return election_mapping
            
        except Exception as e:
            logger.error(f"❌ Error creating election type mapping: {e}")
            return None
    
    def apply_election_type_mapping(self, election_mapping):
        """Apply the election type mapping to the database."""
        try:
            logger.info("🔄 Applying election type mapping to database...")
            
            with self.engine.connect() as conn:
                # Update election types based on the mapping
                updated_count = 0
                
                for stable_id, election_type in election_mapping.items():
                    try:
                        conn.execute(
                            text("UPDATE filings SET election_type = :election_type WHERE stable_id = :stable_id"),
                            {"election_type": election_type, "stable_id": stable_id}
                        )
                        updated_count += 1
                        
                        # Commit every 1000 updates to avoid long transactions
                        if updated_count % 1000 == 0:
                            conn.commit()
                            logger.info(f"  Updated {updated_count:,} records...")
                            
                    except Exception as e:
                        logger.warning(f"⚠️  Failed to update stable_id {stable_id}: {e}")
                        continue
                
                # Final commit
                conn.commit()
                logger.info(f"✅ Updated election types for {updated_count:,} records")
                
        except Exception as e:
            logger.error(f"❌ Error applying election type mapping: {e}")
    
    def verify_election_types(self):
        """Verify the election types were applied correctly."""
        try:
            logger.info("🔍 Verifying election types...")
            
            with self.engine.connect() as conn:
                # Check election type distribution
                result = conn.execute(text("SELECT election_type, COUNT(*) FROM filings GROUP BY election_type ORDER BY COUNT(*) DESC"))
                
                logger.info("📊 Final election type distribution:")
                for row in result.fetchall():
                    logger.info(f"  {row[0]}: {row[1]:,} rows")
                
                # Check sample data
                result = conn.execute(text("""
                    SELECT full_name_display, stable_id, election_type
                    FROM filings 
                    WHERE election_type != 'unknown'
                    LIMIT 10
                """))
                
                logger.info("📝 Sample records with determined election types:")
                for row in result.fetchall():
                    logger.info(f"  {row[0]} (ID: {row[1]}) -> {row[2]}")
                
        except Exception as e:
            logger.error(f"❌ Error verifying election types: {e}")
    
    def run_fix(self):
        """Run the complete election type fix."""
        try:
            if not self.connect():
                return False
            
            # Create election type mapping
            election_mapping = self.create_election_type_mapping()
            if not election_mapping:
                logger.error("❌ Failed to create election type mapping")
                return False
            
            # Apply the mapping
            self.apply_election_type_mapping(election_mapping)
            
            # Verify the fix
            self.verify_election_types()
            
            logger.info("🎉 Election type fix completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Process failed: {e}")
            return False
        finally:
            if self.engine:
                self.engine.dispose()

def main():
    """Main function."""
    fixer = ElectionTypeFixer()
    success = fixer.run_fix()
    
    if success:
        logger.info("🎉 Election types properly fixed! Check the logs above for details.")
    else:
        logger.error("❌ Election type fix failed. Please check the logs above.")

if __name__ == "__main__":
    main()
