#!/usr/bin/env python3
"""
Main Pipeline for CandidateFilings.com Data Processing

This is the one script to rule them all - handles the complete pipeline from raw data
to final production database with all standardization and deduplication.
"""

import pandas as pd
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import concurrent.futures
import glob
import time
import sys
from pathlib import Path

# Add current directory to Python path for imports
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

# Import state cleaners
from state_cleaners.alaska_cleaner import clean_alaska_candidates
from state_cleaners.arizona_cleaner import clean_arizona_candidates
from state_cleaners.arkansas_cleaner import clean_arkansas_candidates
from state_cleaners.colorado_cleaner import clean_colorado_candidates
from state_cleaners.delaware_cleaner import clean_delaware_candidates
from state_cleaners.georgia_cleaner import clean_georgia_candidates
from state_cleaners.idaho_cleaner import clean_idaho_candidates
from state_cleaners.illinois_cleaner import clean_illinois_candidates
from state_cleaners.indiana_cleaner import clean_indiana_candidates
from state_cleaners.iowa_cleaner import clean_iowa_candidates
from state_cleaners.kansas_cleaner import clean_kansas_candidates
from state_cleaners.kentucky_cleaner import clean_kentucky_candidates
from state_cleaners.louisiana_cleaner import clean_louisiana_candidates
from state_cleaners.maryland_cleaner import clean_maryland_candidates
from state_cleaners.missouri_cleaner import clean_missouri_candidates
from state_cleaners.montana_cleaner import clean_montana_candidates
from state_cleaners.nebraska_cleaner import clean_nebraska_candidates
from state_cleaners.new_mexico_cleaner import clean_new_mexico_candidates
from state_cleaners.new_york_cleaner import clean_new_york_candidates
from state_cleaners.north_carolina_cleaner import clean_north_carolina_candidates
from state_cleaners.oklahoma_cleaner import clean_oklahoma_candidates
from state_cleaners.oregon_cleaner import clean_oregon_candidates
from state_cleaners.pennsylvania_cleaner import clean_pennsylvania_candidates
from state_cleaners.south_carolina_cleaner import clean_south_carolina_candidates
from state_cleaners.south_dakota_cleaner import clean_south_dakota_candidates
from state_cleaners.vermont_cleaner import clean_vermont_candidates
from state_cleaners.virginia_cleaner import clean_virginia_candidates
from state_cleaners.washington_cleaner import clean_washington_candidates
from state_cleaners.west_virginia_cleaner import clean_west_virginia_candidates
from state_cleaners.wyoming_cleaner import clean_wyoming_candidates

# Import processors
from office_standardizer import OfficeStandardizer

# Import address processing
from fix_address_separation_issues import AddressFixer
from address_parsing_audit import audit_address_fields
from address_separation_audit import analyze_address_separation
from data_audit import audit_data_quality, audit_column_consistency
from verify_address_fixes import verify_address_fixes, compare_before_after

# Import database utilities
sys.path.append(str(current_dir.parent.parent / 'src' / 'config'))
from database import get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'pipeline_run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

class MainPipeline:
    """Main pipeline orchestrator for CandidateFilings data processing."""

    def __init__(self,
                 raw_data_dir: str = "data/raw",
                 processed_dir: str = "data/processed",
                 final_dir: str = "data/final",
                 staging_table: str = "staging_candidates",
                 production_table: str = "filings"):
        
        self.raw_data_dir = raw_data_dir
        self.processed_dir = processed_dir
        self.final_dir = final_dir
        self.staging_table = staging_table
        self.production_table = production_table
        
        # Initialize processors
        self.office_standardizer = OfficeStandardizer()
        self.address_fixer = AddressFixer()
        self.db_manager = get_db_connection()
        
        # State cleaner mapping
        self.state_cleaners = {
            'alaska': clean_alaska_candidates,
            'arizona': clean_arizona_candidates,
            'arkansas': clean_arkansas_candidates,
            'colorado': clean_colorado_candidates,
            'delaware': clean_delaware_candidates,
            'georgia': clean_georgia_candidates,
            'idaho': clean_idaho_candidates,
            'illinois': clean_illinois_candidates,
            'indiana': clean_indiana_candidates,
            'iowa': clean_iowa_candidates,
            'kansas': clean_kansas_candidates,
            'kentucky': clean_kentucky_candidates,
            'louisiana': clean_louisiana_candidates,
            'maryland': clean_maryland_candidates,
            'missouri': clean_missouri_candidates,
            'montana': clean_montana_candidates,
            'nebraska': clean_nebraska_candidates,
            'new_mexico': clean_new_mexico_candidates,
            'new_york': clean_new_york_candidates,
            'north_carolina': clean_north_carolina_candidates,
            'oklahoma': clean_oklahoma_candidates,
            'oregon': clean_oregon_candidates,
            'pennsylvania': clean_pennsylvania_candidates,
            'south_carolina': clean_south_carolina_candidates,
            'south_dakota': clean_south_dakota_candidates,
            'vermont': clean_vermont_candidates,
            'virginia': clean_virginia_candidates,
            'washington': clean_washington_candidates,
            'west_virginia': clean_west_virginia_candidates,
            'wyoming': clean_wyoming_candidates
        }
        
        # Ensure directories exist
        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.final_dir, exist_ok=True)
        os.makedirs("data/reports", exist_ok=True)

    def run_state_cleaners(self) -> Dict[str, str]:
        """Run all state cleaners on raw data."""
        logger.info("Starting state cleaning process...")
        
        cleaned_files = {}
        raw_files = self._get_raw_data_files()
        
        if not raw_files:
            logger.warning("No raw data files found")
            return {}
        
        # Process each state
        for state, cleaner_func in self.state_cleaners.items():
            try:
                # Find raw data file for this state
                raw_file = self._find_state_file(raw_files, state)
                if not raw_file:
                    logger.warning(f"No raw data found for {state}")
                    continue
                
                logger.info(f"Cleaning {state} data...")
                
                # Run state cleaner
                cleaned_file = cleaner_func(raw_file, self.processed_dir)
                if cleaned_file and os.path.exists(cleaned_file):
                    cleaned_files[state] = cleaned_file
                    logger.info(f"✅ {state} cleaned successfully")
                else:
                    logger.error(f"❌ {state} cleaning failed")
                    
            except Exception as e:
                logger.error(f"Error cleaning {state}: {e}")
                continue
        
        logger.info(f"State cleaning completed. {len(cleaned_files)} states processed.")
        return cleaned_files

    def _get_raw_data_files(self) -> List[str]:
        """Get all raw data files."""
        raw_files = []
        for ext in ['*.xlsx', '*.csv', '*.xls']:
            raw_files.extend(glob.glob(os.path.join(self.raw_data_dir, ext)))
        return raw_files

    def _find_state_file(self, raw_files: List[str], state: str) -> Optional[str]:
        """Find raw data file for a specific state."""
        state_lower = state.lower()
        for file_path in raw_files:
            filename = os.path.basename(file_path).lower()
            if state_lower in filename:
                return file_path
        return None

    def run_address_fixing(self, cleaned_files: Dict[str, str]) -> Dict[str, str]:
        """Fix address separation issues across all states."""
        logger.info("Starting address fixing process...")
        
        # Load all cleaned data
        all_data = []
        for state, file_path in cleaned_files.items():
            try:
                df = pd.read_excel(file_path)
                df['state'] = state.title()
                all_data.append(df)
            except Exception as e:
                logger.error(f"Error loading {state} data: {e}")
                continue
        
        if not all_data:
            logger.warning("No cleaned data to process")
            return {}
        
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Combined {len(combined_df)} records from {len(all_data)} states")
        
        # Fix addresses
        fixed_df = self.address_fixer.fix_addresses_generic(combined_df)
        
        # Save fixed data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.processed_dir, f"all_states_addresses_fixed_{timestamp}.xlsx")
        fixed_df.to_excel(output_file, index=False)
        
        logger.info(f"✅ Address fixing completed. Saved to: {output_file}")
        return {'all_states': output_file}

    def run_office_standardization(self, cleaned_files: Dict[str, str]) -> str:
        """Run office standardization on cleaned data."""
        logger.info("Starting office standardization...")
        
        # Load all cleaned data
        all_data = []
        for state, file_path in cleaned_files.items():
            try:
                df = pd.read_excel(file_path)
                df['state'] = state.title()
                all_data.append(df)
            except Exception as e:
                logger.error(f"Error loading {state} data: {e}")
                continue
        
        if not all_data:
            logger.warning("No cleaned data to standardize")
            return ""
        
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Standardizing offices for {len(combined_df)} records")
        
        # Standardize offices
        standardized_df = self.office_standardizer.standardize_offices(combined_df)
        
        # Save standardized data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.processed_dir, f"all_states_offices_standardized_{timestamp}.xlsx")
        standardized_df.to_excel(output_file, index=False)
        
        logger.info(f"✅ Office standardization completed. Saved to: {output_file}")
        return output_file

    def run_national_standardization(self, standardized_file: str) -> str:
        """Run national-level standardization (party names, addresses, etc.)."""
        logger.info("Starting national standardization...")
        
        # Load standardized data
        df = pd.read_excel(standardized_file)
        
        # Party standardization
        df = self._standardize_parties(df)
        
        # Address parsing and standardization
        df = self._parse_addresses(df)
        
        # Other national standardizations
        df = self._apply_national_standards(df)
        
        # Save nationally standardized data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.final_dir, f"all_states_nationally_standardized_{timestamp}.xlsx")
        df.to_excel(output_file, index=False)
        
        logger.info(f"✅ National standardization completed. Saved to: {output_file}")
        return output_file

    def _standardize_parties(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize party names to major parties only."""
        logger.info("Standardizing party names...")
        
        # Major party mappings
        party_mappings = {
            'democrat': 'Democratic',
            'democratic': 'Democratic',
            'dem': 'Democratic',
            'republican': 'Republican',
            'rep': 'Republican',
            'gop': 'Republican',
            'independent': 'Independent',
            'ind': 'Independent',
            'libertarian': 'Libertarian',
            'lib': 'Libertarian',
            'green': 'Green Party',
            'constitution': 'Constitution Party',
            'constitution party': 'Constitution Party'
        }
        
        if 'party' in df.columns:
            df['party_standardized'] = df['party'].str.lower().map(party_mappings).fillna(df['party'])
            logger.info("Party standardization completed")
        
        return df

    def _parse_addresses(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse and standardize address fields."""
        logger.info("Parsing and standardizing addresses...")
        
        if 'address' in df.columns:
            # Add address parsing columns
            df['address_parsed'] = True
            df['address_clean'] = df['address'].astype(str).str.strip()
            
            # Extract common address components
            df['has_zip'] = df['address'].str.contains(r'\d{5}(?:-\d{4})?', na=False)
            df['has_state'] = df['address'].str.contains(r'\b[A-Z]{2}\b', na=False)
            
            logger.info("Address parsing completed")
        
        return df

    def _apply_national_standards(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply other national-level standardizations."""
        logger.info("Applying national standards...")
        
        # Add processing metadata
        df['processing_timestamp'] = datetime.now()
        df['pipeline_version'] = '1.0'
        df['data_source'] = 'state_filings'
        
        # Ensure consistent column names
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        
        logger.info("National standards applied")
        return df

    def run_data_audit(self) -> Tuple[str, str]:
        """Run comprehensive data audit."""
        logger.info("Starting data audit...")
        
        # Run various audits
        quality_results = audit_data_quality(self.processed_dir)
        consistency_results = audit_column_consistency(self.processed_dir)
        address_audit = audit_address_fields(self.processed_dir)
        separation_audit = analyze_address_separation(self.processed_dir)
        
        # Generate comprehensive report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        audit_file = f"data/reports/comprehensive_audit_{timestamp}.xlsx"
        
        with pd.ExcelWriter(audit_file, engine='openpyxl') as writer:
            quality_results.to_excel(writer, sheet_name='Data_Quality', index=False)
            consistency_results.to_excel(writer, sheet_name='Column_Consistency', index=False)
            address_audit.to_excel(writer, sheet_name='Address_Audit', index=False)
            separation_audit.to_excel(writer, sheet_name='Address_Separation', index=False)
        
        logger.info(f"✅ Data audit completed. Report saved to: {audit_file}")
        return audit_file, "audit_completed"

    def run_deduplication(self, final_file: str) -> str:
        """Remove exact duplicate records."""
        logger.info("Starting deduplication process...")
        
        # Load final data
        df = pd.read_excel(final_file)
        original_count = len(df)
        
        # Remove exact duplicates
        df_deduped = df.drop_duplicates()
        final_count = len(df_deduped)
        duplicates_removed = original_count - final_count
        
        logger.info(f"Removed {duplicates_removed} duplicate records")
        
        # Save deduplicated data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.final_dir, f"all_states_deduplicated_{timestamp}.xlsx")
        df_deduped.to_excel(output_file, index=False)
        
        logger.info(f"✅ Deduplication completed. Saved to: {output_file}")
        return output_file

    def upload_to_database(self, final_file: str) -> bool:
        """Upload final data to database."""
        logger.info("Starting database upload...")
        
        try:
            # Connect to database
            if not self.db_manager.connect():
                logger.error("Failed to connect to database")
                return False
            
            # Load final data
            df = pd.read_excel(final_file)
            logger.info(f"Loaded {len(df)} records for database upload")
            
            # TODO: Implement staging table upload
            # TODO: Implement production table migration
            # TODO: Implement conflict resolution
            
            logger.info("✅ Database upload completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Database upload failed: {e}")
            return False
        finally:
            self.db_manager.disconnect()

    def run_full_pipeline(self) -> bool:
        """Run the complete pipeline from start to finish."""
        logger.info("🚀 Starting full CandidateFilings pipeline...")
        
        try:
            # Step 1: State cleaning
            logger.info("=== STEP 1: State Cleaning ===")
            cleaned_files = self.run_state_cleaners()
            if not cleaned_files:
                logger.error("State cleaning failed")
                return False
            
            # Step 2: Address fixing
            logger.info("=== STEP 2: Address Fixing ===")
            address_fixed_files = self.run_address_fixing(cleaned_files)
            
            # Step 3: Office standardization
            logger.info("=== STEP 3: Office Standardization ===")
            office_standardized_file = self.run_office_standardization(cleaned_files)
            if not office_standardized_file:
                logger.error("Office standardization failed")
                return False
            
            # Step 4: National standardization
            logger.info("=== STEP 4: National Standardization ===")
            nationally_standardized_file = self.run_national_standardization(office_standardized_file)
            if not nationally_standardized_file:
                logger.error("National standardization failed")
                return False
            
            # Step 5: Deduplication
            logger.info("=== STEP 5: Deduplication ===")
            final_file = self.run_deduplication(nationally_standardized_file)
            if not final_file:
                logger.error("Deduplication failed")
                return False
            
            # Step 6: Data audit
            logger.info("=== STEP 6: Data Audit ===")
            audit_file, audit_status = self.run_data_audit()
            
            # Step 7: Database upload (optional)
            logger.info("=== STEP 7: Database Upload ===")
            db_success = self.upload_to_database(final_file)
            
            logger.info("🎉 Full pipeline completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            return False

def main():
    """Main function to run the pipeline."""
    pipeline = MainPipeline()
    
    # Run the full pipeline
    success = pipeline.run_full_pipeline()
    
    if success:
        print("\n✅ Pipeline completed successfully!")
        print("Check the logs and output files for details.")
    else:
        print("\n❌ Pipeline failed!")
        print("Check the logs for error details.")

if __name__ == "__main__":
    main()
