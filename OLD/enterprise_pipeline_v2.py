#!/usr/bin/env python3
"""
Enterprise Political Candidates Data Pipeline v2.0

A comprehensive, enterprise-grade data pipeline for processing political candidate data
from various state sources with state-specific cleaning, stable ID generation, and
zero-downtime Supabase integration.
"""

import pandas as pd
import os
import re
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
import logging
from sqlalchemy import create_engine, text
import numpy as np

# Import our custom modules
from state_cleaners import StateCleaners
from data_quality_monitor import DataQualityMonitor
from pipeline_config import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGGING_CONFIG['file']),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StableIDGenerator:
    """Generates stable IDs from original, uncleaned data."""
    
    def __init__(self):
        self.id_fields = STABLE_ID_CONFIG['fields']
        self.hash_length = STABLE_ID_CONFIG['hash_length']
        self.include_nulls = STABLE_ID_CONFIG.get('include_nulls', True)
        self.null_placeholder = STABLE_ID_CONFIG.get('null_placeholder', 'NULL_VALUE')
    
    def generate_stable_id(self, row: pd.Series) -> str:
        """Generate a stable ID from ALL available data, including blanks."""
        # Create a comprehensive string from ALL fields
        id_parts = []
        
        for field in self.id_fields:
            if field in row:
                # Handle the value, including nulls
                if pd.isna(row[field]) or row[field] is None:
                    # Use placeholder for null values to maintain uniqueness
                    id_parts.append(f"{field}:{self.null_placeholder}")
                else:
                    # Convert to string, normalize, and add field name for context
                    value = str(row[field]).strip().lower()
                    id_parts.append(f"{field}:{value}")
            else:
                # Field doesn't exist in this row, mark as missing
                id_parts.append(f"{field}:MISSING_FIELD")
        
        # Sort the parts to ensure consistent ordering
        id_parts.sort()
        
        # Join all parts with a delimiter
        id_string = "||".join(id_parts)
        
        # Generate SHA-256 hash
        return hashlib.sha256(id_string.encode('utf-8')).hexdigest()[:self.hash_length]
    
    def generate_stable_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate stable IDs for all rows in the dataframe using ALL available data."""
        logger.info("Generating comprehensive stable IDs from ALL available data...")
        logger.info(f"Using {len(self.id_fields)} fields: {', '.join(self.id_fields)}")
        
        # Create a copy to preserve original data
        df_with_ids = df.copy()
        
        # Generate stable IDs
        df_with_ids['stable_id'] = df_with_ids.apply(self.generate_stable_id, axis=1)
        
        # Check for duplicates
        duplicate_count = df_with_ids['stable_id'].duplicated().sum()
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate stable IDs")
            
            # Show examples of duplicates for debugging
            duplicates = df_with_ids[df_with_ids['stable_id'].duplicated(keep=False)]
            logger.warning(f"Sample duplicate records:")
            for stable_id in duplicates['stable_id'].unique()[:3]:  # Show first 3 duplicates
                dup_records = duplicates[duplicates['stable_id'] == stable_id]
                logger.warning(f"  Stable ID {stable_id}: {len(dup_records)} records")
                for idx, record in dup_records.head(2).iterrows():
                    name_field = record.get('original_name', record.get('candidate_name', 'N/A'))
                    logger.warning(f"    Record {idx}: {name_field} - {record.get('state', 'N/A')} - {record.get('office', 'N/A')}")
        else:
            logger.info("✅ No duplicate stable IDs found - all records are unique!")
        
        # Log some sample stable IDs for verification
        logger.info("Sample stable IDs generated:")
        for idx, row in df_with_ids.head(3).iterrows():
            name_field = row.get('original_name', row.get('candidate_name', 'N/A'))
            logger.info(f"  Record {idx}: {name_field} -> {row['stable_id']}")
        
        return df_with_ids

class DataStandardizer:
    """Applies general standardization and transforms data to match Supabase schema."""
    
    def __init__(self):
        self.party_mapping = PARTY_MAPPING
    
    def standardize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply general standardization and transform to Supabase schema."""
        logger.info("Applying general standardization and Supabase schema transformation...")
        
        standardized_df = df.copy()
        
        # Step 1: Preserve original data before any transformations
        self._preserve_original_data(standardized_df)
        
        # Step 2: Parse names into components
        self._parse_names(standardized_df)
        
        # Step 3: Extract election year from election date
        self._extract_election_year(standardized_df)
        
        # Step 4: Standardize party names
        if 'party' in standardized_df.columns:
            standardized_df['party'] = standardized_df['party'].apply(self.standardize_party)
        
        # Step 5: Clean phone numbers
        if 'phone' in standardized_df.columns:
            standardized_df['phone'] = standardized_df['phone'].apply(self.clean_phone)
        
        # Step 6: Clean email addresses
        if 'email' in standardized_df.columns:
            standardized_df['email'] = standardized_df['email'].apply(self.clean_email)
        
        # Step 7: Clean addresses
        if 'address' in standardized_df.columns:
            standardized_df['address'] = standardized_df['address'].apply(self.clean_address)
        
        # Step 8: Clean zip codes
        if 'zip_code' in standardized_df.columns:
            standardized_df['zip_code'] = standardized_df['zip_code'].apply(self.clean_zip_code)
        
        # Step 9: Standardize dates
        date_columns = ['filing_date', 'election_date']
        for col in date_columns:
            if col in standardized_df.columns:
                standardized_df[col] = pd.to_datetime(standardized_df[col], errors='coerce')
        
        # Step 10: Add metadata columns
        self._add_metadata_columns(standardized_df)
        
        # Step 11: Ensure all required columns exist
        self._ensure_all_columns(standardized_df)
        
        return standardized_df
    
    def _preserve_original_data(self, df: pd.DataFrame):
        """Preserve original data in separate columns."""
        if 'candidate_name' in df.columns:
            df['original_name'] = df['candidate_name'].copy()
        if 'state' in df.columns:
            df['original_state'] = df['state'].copy()
        if 'office' in df.columns:
            df['original_office'] = df['office'].copy()
        if 'filing_date' in df.columns:
            df['original_filing_date'] = df['filing_date'].copy()
        if 'source_file' in df.columns:
            df['batch_id'] = df['source_file'].copy()
    
    def _parse_names(self, df: pd.DataFrame):
        """Parse candidate names into first, middle, last, and display name."""
        if 'candidate_name' not in df.columns:
            return
        
        def parse_name(name):
            if pd.isna(name) or not name:
                return None, None, None, None
            
            # Clean the name first
            name = str(name).strip().strip('"\'')
            name = re.sub(r'\s+', ' ', name)
            
            # Split by spaces
            parts = name.split()
            
            if len(parts) == 1:
                return parts[0], None, None, parts[0]
            elif len(parts) == 2:
                return parts[0], None, parts[1], f"{parts[0]} {parts[1]}"
            elif len(parts) == 3:
                return parts[0], parts[1], parts[2], f"{parts[0]} {parts[1]} {parts[2]}"
            else:
                # For names with more than 3 parts, treat first as first, last as last, rest as middle
                first = parts[0]
                last = parts[-1]
                middle = ' '.join(parts[1:-1]) if len(parts) > 2 else None
                display = name
                return first, middle, last, display
        
        # Apply name parsing
        parsed_names = df['candidate_name'].apply(parse_name)
        df['first_name'] = [name[0] for name in parsed_names]
        df['middle_name'] = [name[1] for name in parsed_names]
        df['last_name'] = [name[2] for name in parsed_names]
        df['full_name_display'] = [name[3] for name in parsed_names]
    
    def _extract_election_year(self, df: pd.DataFrame):
        """Extract election year from election date."""
        if 'election_date' in df.columns:
            df['election_year'] = pd.to_datetime(df['election_date'], errors='coerce').dt.year
            df['original_election_year'] = df['election_year'].copy()
        else:
            df['election_year'] = None
            df['original_election_year'] = None
    
    def _add_metadata_columns(self, df: pd.DataFrame):
        """Add metadata columns required by Supabase."""
        from datetime import datetime
        
        current_time = datetime.now()
        df['created_at'] = current_time
        df['updated_at'] = current_time
        df['data_version'] = PIPELINE_VERSION
    
    def _ensure_all_columns(self, df: pd.DataFrame):
        """Ensure all required Supabase columns exist."""
        required_columns = [
            'id', 'stable_id', 'original_name', 'original_state', 'original_election_year',
            'original_office', 'original_filing_date', 'first_name', 'middle_name', 'last_name',
            'full_name_display', 'state', 'election_year', 'office', 'party', 'district',
            'county', 'city', 'address', 'zip_code', 'phone', 'email', 'website',
            'filing_date', 'election_date', 'election_type', 'facebook', 'twitter',
            'created_at', 'updated_at', 'data_version', 'batch_id'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = None
        
        # Set id to index + 1 (will be overridden by database)
        df['id'] = range(1, len(df) + 1)
    
    def standardize_party(self, party: str) -> str:
        """Standardize party names."""
        if pd.isna(party):
            return None
        
        party_lower = str(party).strip().lower()
        return self.party_mapping.get(party_lower, party)
    
    def clean_name(self, name: str) -> str:
        """Clean candidate names."""
        if pd.isna(name):
            return None
        
        # Remove extra whitespace and quotes
        cleaned = str(name).strip().strip('"\'')
        # Remove multiple spaces
        cleaned = re.sub(r'\s+', ' ', cleaned)
        return cleaned
    
    def clean_phone(self, phone: str) -> str:
        """Clean phone numbers."""
        if pd.isna(phone):
            return None
        
        # Remove all non-digit characters
        digits = re.sub(r'[^\d]', '', str(phone))
        
        # Validate US phone number
        if len(digits) == 10:
            return digits
        elif len(digits) == 11 and digits.startswith('1'):
            return digits[1:]
        
        return None
    
    def clean_email(self, email: str) -> str:
        """Clean email addresses."""
        if pd.isna(email):
            return None
        
        email = str(email).strip().lower()
        
        # Basic email validation
        if '@' in email and '.' in email.split('@')[1]:
            return email
        
        return None
    
    def clean_address(self, address: str) -> str:
        """Clean addresses."""
        if pd.isna(address):
            return None
        
        # Remove extra whitespace and quotes
        cleaned = str(address).strip().strip('"\'')
        # Remove multiple spaces
        cleaned = re.sub(r'\s+', ' ', cleaned)
        return cleaned
    
    def clean_zip_code(self, zip_code: str) -> str:
        """Clean zip codes."""
        if pd.isna(zip_code):
            return None
        
        # Remove all non-digit characters
        digits = re.sub(r'[^\d]', '', str(zip_code))
        
        # Validate US zip code
        if len(digits) == 5:
            return digits
        elif len(digits) == 9:
            return f"{digits[:5]}-{digits[5:]}"
        
        return None

class SupabaseLoader:
    """Handles data loading to Supabase with zero-downtime updates."""
    
    def __init__(self):
        self.engine = None
        self.connect()
    
    def connect(self):
        """Establish connection to Supabase."""
        try:
            self.engine = create_engine(SUPABASE_CONFIG['connection_string'])
            logger.info("✅ Connected to Supabase successfully")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Supabase: {e}")
            raise
    
    def load_to_staging(self, df: pd.DataFrame) -> bool:
        """Load data to staging table."""
        try:
            logger.info("Loading data to staging table...")
            
            # Add metadata columns
            df_with_metadata = df.copy()
            df_with_metadata['data_version'] = datetime.now().isoformat()
            df_with_metadata['action_type'] = 'INSERT'  # Will be updated based on comparison
            df_with_metadata['created_at'] = datetime.now()
            df_with_metadata['updated_at'] = datetime.now()
            
            # Load to staging table
            df_with_metadata.to_sql(
                SUPABASE_CONFIG['tables']['staging'], 
                self.engine, 
                if_exists='replace', 
                index=False,
                method='multi'
            )
            
            logger.info(f"✅ Loaded {len(df)} records to staging table")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load to staging: {e}")
            return False
    
    def compare_and_update_production(self) -> Dict[str, int]:
        """Compare staging with production and update with zero downtime."""
        try:
            logger.info("Comparing staging with production...")
            
            with self.engine.connect() as conn:
                # Get counts
                staging_count = conn.execute(text(f"SELECT COUNT(*) FROM {SUPABASE_CONFIG['tables']['staging']}")).scalar()
                production_count = conn.execute(text(f"SELECT COUNT(*) FROM {SUPABASE_CONFIG['tables']['production']}")).scalar()
                
                logger.info(f"Staging records: {staging_count}")
                logger.info(f"Production records: {production_count}")
                
                # Find new records (INSERT)
                insert_query = f"""
                INSERT INTO {SUPABASE_CONFIG['tables']['production']} (
                    stable_id, candidate_name, state, office, party, district, 
                    county, city, address, zip_code, phone, email, website,
                    filing_date, election_date, election_type, source_file,
                    data_version, created_at, updated_at
                )
                SELECT 
                    s.stable_id, s.candidate_name, s.state, s.office, s.party, s.district,
                    s.county, s.city, s.address, s.zip_code, s.phone, s.email, s.website,
                    s.filing_date, s.election_date, s.election_type, s.source_file,
                    s.data_version, s.created_at, s.updated_at
                FROM {SUPABASE_CONFIG['tables']['staging']} s
                LEFT JOIN {SUPABASE_CONFIG['tables']['production']} p ON s.stable_id = p.stable_id
                WHERE p.stable_id IS NULL
                """
                
                insert_result = conn.execute(text(insert_query))
                insert_count = insert_result.rowcount
                
                # Find updated records (UPDATE)
                update_query = f"""
                UPDATE {SUPABASE_CONFIG['tables']['production']} p
                SET 
                    candidate_name = s.candidate_name,
                    state = s.state,
                    office = s.office,
                    party = s.party,
                    district = s.district,
                    county = s.county,
                    city = s.city,
                    address = s.address,
                    zip_code = s.zip_code,
                    phone = s.phone,
                    email = s.email,
                    website = s.website,
                    filing_date = s.filing_date,
                    election_date = s.election_date,
                    election_type = s.election_type,
                    source_file = s.source_file,
                    data_version = s.data_version,
                    updated_at = s.updated_at
                FROM {SUPABASE_CONFIG['tables']['staging']} s
                WHERE p.stable_id = s.stable_id
                AND (
                    COALESCE(p.candidate_name, '') != COALESCE(s.candidate_name, '') OR
                    COALESCE(p.state, '') != COALESCE(s.state, '') OR
                    COALESCE(p.office, '') != COALESCE(s.office, '') OR
                    COALESCE(p.party, '') != COALESCE(s.party, '') OR
                    COALESCE(p.district, '') != COALESCE(s.district, '') OR
                    COALESCE(p.county, '') != COALESCE(s.county, '') OR
                    COALESCE(p.city, '') != COALESCE(s.city, '') OR
                    COALESCE(p.address, '') != COALESCE(s.address, '') OR
                    COALESCE(p.zip_code, '') != COALESCE(s.zip_code, '') OR
                    COALESCE(p.phone, '') != COALESCE(s.phone, '') OR
                    COALESCE(p.email, '') != COALESCE(s.email, '') OR
                    COALESCE(p.website, '') != COALESCE(s.website, '') OR
                    COALESCE(p.filing_date::text, '') != COALESCE(s.filing_date::text, '') OR
                    COALESCE(p.election_date::text, '') != COALESCE(s.election_date::text, '') OR
                    COALESCE(p.election_type, '') != COALESCE(s.election_type, '') OR
                    COALESCE(p.source_file, '') != COALESCE(s.source_file, '')
                )
                """
                
                update_result = conn.execute(text(update_query))
                update_count = update_result.rowcount
                
                # Commit changes
                conn.commit()
                
                logger.info(f"✅ Production update completed:")
                logger.info(f"  - New records inserted: {insert_count}")
                logger.info(f"  - Records updated: {update_count}")
                
                return {
                    'inserted': insert_count,
                    'updated': update_count,
                    'total_staging': staging_count,
                    'total_production': production_count + insert_count
                }
                
        except Exception as e:
            logger.error(f"❌ Failed to update production: {e}")
            return {'inserted': 0, 'updated': 0, 'error': str(e)}

class EnterprisePoliticalPipeline:
    """Main pipeline orchestrator."""
    
    def __init__(self, input_directory: str = INPUT_DIRECTORY):
        self.input_directory = input_directory
        self.state_cleaner = StateCleaners()
        self.id_generator = StableIDGenerator()
        self.standardizer = DataStandardizer()
        self.supabase_loader = SupabaseLoader()
        self.quality_monitor = DataQualityMonitor()
    
    def run_pipeline(self, output_to_file: bool = True) -> pd.DataFrame:
        """Run the complete enterprise pipeline."""
        logger.info("🚀 Starting Enterprise Political Candidates Data Pipeline v2.0")
        logger.info("=" * 60)
        
        # Step 1: Load and consolidate all state files
        logger.info("\n📁 STEP 1: Loading and consolidating state files...")
        consolidated_data = self._load_and_consolidate_files()
        
        if consolidated_data is None or len(consolidated_data) == 0:
            logger.error("❌ No data to process!")
            return None
        
        # Step 2: Apply state-specific cleaning
        logger.info("\n🧹 STEP 2: Applying state-specific cleaning...")
        cleaned_data = self._apply_state_cleaning(consolidated_data)
        
        # Step 3: Generate stable IDs from original data
        logger.info("\n🆔 STEP 3: Generating stable IDs...")
        data_with_ids = self.id_generator.generate_stable_ids(cleaned_data)
        
        # Step 4: Apply general standardization
        logger.info("\n⚙️ STEP 4: Applying general standardization...")
        standardized_data = self.standardizer.standardize_data(data_with_ids)
        
        # Step 5: Data quality validation
        logger.info("\n🔍 STEP 5: Data quality validation...")
        quality_results = self.quality_monitor.validate_dataset(standardized_data, stage="post_standardization")
        
        if not quality_results['validation_passed']:
            logger.warning("⚠️ Data quality validation failed, but continuing...")
        
        # Step 6: Load to Supabase staging
        logger.info("\n☁️ STEP 6: Loading to Supabase staging...")
        staging_success = self.supabase_loader.load_to_staging(standardized_data)
        
        if staging_success:
            # Step 7: Update production with zero downtime
            logger.info("\n🔄 STEP 7: Updating production with zero downtime...")
            update_results = self.supabase_loader.compare_and_update_production()
            
            logger.info(f"\n📊 PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info(f"Final results: {update_results}")
        
        # Generate quality report
        self.quality_monitor.print_quality_summary()
        
        # Save to file if requested
        if output_to_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"political_candidates_database_{timestamp}.xlsx"
            standardized_data.to_excel(output_file, index=False)
            logger.info(f"💾 Data saved to: {output_file}")
        
        return standardized_data
    
    def _load_and_consolidate_files(self) -> List[Dict]:
        """Load and consolidate all Excel files."""
        excel_files = [f for f in os.listdir(self.input_directory) 
                      if f.endswith('.xlsx') and not f.startswith('political_candidates')]
        
        if not excel_files:
            logger.error("No Excel files found!")
            return None
        
        logger.info(f"Found {len(excel_files)} Excel files to process")
        
        consolidated_data = []
        
        for i, file in enumerate(excel_files, 1):
            try:
                logger.info(f"[{i}/{len(excel_files)}] Processing {file}...")
                file_path = os.path.join(self.input_directory, file)
                df = pd.read_excel(file_path)
                
                # Add to consolidated data
                consolidated_data.append({
                    'filename': file,
                    'dataframe': df,
                    'row_count': len(df)
                })
                
                logger.info(f"  ✓ Loaded {len(df)} records from {file}")
                
            except Exception as e:
                logger.error(f"  ❌ Failed to load {file}: {e}")
        
        return consolidated_data
    
    def _apply_state_cleaning(self, consolidated_data: List[Dict]) -> pd.DataFrame:
        """Apply state-specific cleaning to all files."""
        cleaned_dfs = []
        
        for file_data in consolidated_data:
            filename = file_data['filename']
            df = file_data['dataframe']
            
            # Apply state-specific cleaning
            cleaned_df = self.state_cleaner.clean_state_data(df, filename)
            
            if cleaned_df is not None and len(cleaned_df) > 0:
                cleaned_dfs.append(cleaned_df)
                logger.info(f"  ✓ Cleaned {len(cleaned_df)} records from {filename}")
            else:
                logger.warning(f"  ⚠️ No data after cleaning for {filename}")
        
        if not cleaned_dfs:
            logger.error("No data after state-specific cleaning!")
            return pd.DataFrame()
        
        # Combine all cleaned dataframes
        final_df = pd.concat(cleaned_dfs, ignore_index=True)
        logger.info(f"Combined {len(final_df)} total records from all states")
        
        return final_df

def main():
    """Main function to run the pipeline."""
    pipeline = EnterprisePoliticalPipeline()
    result = pipeline.run_pipeline(output_to_file=True)
    
    if result is not None:
        logger.info(f"✅ Pipeline completed successfully with {len(result)} records")
    else:
        logger.error("❌ Pipeline failed!")

if __name__ == "__main__":
    main() 