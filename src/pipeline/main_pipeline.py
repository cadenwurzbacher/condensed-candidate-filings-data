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
import re
from pathlib import Path
import hashlib

# Add current directory to Python path for imports
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

# Import state cleaners
from .state_cleaners.alaska_cleaner import AlaskaCleaner
from .state_cleaners.arizona_cleaner import ArizonaCleaner
from .state_cleaners.arkansas_cleaner import ArkansasCleaner
from .state_cleaners.colorado_cleaner import ColoradoCleaner
from .state_cleaners.delaware_cleaner import DelawareCleaner
from .state_cleaners.georgia_cleaner import GeorgiaCleaner
from .state_cleaners.idaho_cleaner import IdahoCleaner
from .state_cleaners.illinois_cleaner import IllinoisCleaner
from .state_cleaners.indiana_cleaner import IndianaCleaner
from .state_cleaners.iowa_cleaner import IowaCleaner
from .state_cleaners.kansas_cleaner import KansasCleaner
from .state_cleaners.kentucky_cleaner import KentuckyCleaner
from .state_cleaners.louisiana_cleaner import LouisianaCleaner
from .state_cleaners.maryland_cleaner import MarylandCleaner
from .state_cleaners.missouri_cleaner import MissouriCleaner
from .state_cleaners.montana_cleaner import MontanaCleaner
from .state_cleaners.nebraska_cleaner import NebraskaCleaner
from .state_cleaners.new_mexico_cleaner import NewMexicoCleaner
from .state_cleaners.new_york_cleaner import NewYorkCleaner
from .state_cleaners.north_carolina_cleaner import NorthCarolinaCleaner
from .state_cleaners.oklahoma_cleaner import OklahomaCleaner
from .state_cleaners.oregon_cleaner import OregonCleaner
from .state_cleaners.pennsylvania_cleaner import PennsylvaniaCleaner
from .state_cleaners.south_carolina_cleaner import SouthCarolinaCleaner
from .state_cleaners.south_dakota_cleaner import SouthDakotaCleaner
from .state_cleaners.vermont_cleaner import VermontCleaner
from .state_cleaners.virginia_cleaner import VirginiaCleaner
from .state_cleaners.washington_cleaner import WashingtonCleaner
from .state_cleaners.west_virginia_cleaner import WestVirginiaCleaner
from .state_cleaners.wyoming_cleaner import WyomingCleaner

# Import structural cleaners (new)
from .state_cleaners.alaska_structural_cleaner import AlaskaStructuralCleaner

# Import processors
from .office_standardizer import OfficeStandardizer

# Import database utilities
from ..config.database import get_db_connection

# Configure logging
# Create logs directory if it doesn't exist
logs_dir = Path("data/logs")
logs_dir.mkdir(parents=True, exist_ok=True)

# Clean up old log files (keep only last 5)
log_files = list(logs_dir.glob("pipeline_run_*.log"))
if len(log_files) > 5:
    log_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    for old_log in log_files[5:]:
        old_log.unlink()

# Create a unique log filename
log_filename = logs_dir / f'pipeline_run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

# Configure logging with both console and file handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Clear any existing handlers
logger.handlers.clear()

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# File handler
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Add both handlers
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Log the start of the pipeline
logger.info(f"Pipeline logging initialized. Log file: {log_filename}")

class MainPipeline:
    """Main pipeline orchestrator for CandidateFilings data processing."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        
        # Define directory structure
        self.raw_data_dir = os.path.join(data_dir, "raw")
        self.structured_dir = os.path.join(data_dir, "structured")
        self.cleaner_dir = os.path.join(data_dir, "cleaner")
        self.final_dir = os.path.join(data_dir, "final")
        
        # Structural cleaner mapping (new)
        self.structural_cleaners = {
            'alaska': AlaskaStructuralCleaner(),
            # Add other structural cleaners as we implement them
        }
        
        # State cleaner mapping (only the ones that exist)
        self.state_cleaners = {
            'alaska': AlaskaCleaner(),
            'arizona': None,  # Will be implemented later
            'arkansas': None,  # Will be implemented later
            'colorado': None,  # Will be implemented later
            'delaware': None,  # Will be implemented later
            'georgia': None,  # Will be implemented later
            'idaho': None,  # Will be implemented later
            'illinois': None,  # Will be implemented later
            'indiana': None,  # Will be implemented later
            'iowa': None,  # Will be implemented later
            'kansas': None,  # Will be implemented later
            'kentucky': None,  # Will be implemented later
            'louisiana': None,  # Will be implemented later
            'maryland': None,  # Will be implemented later
            'missouri': None,  # Will be implemented later
            'montana': None,  # Will be implemented later
            'nebraska': None,  # Will be implemented later
            'new_mexico': None,  # Will be implemented later
            'new_york': None,  # Will be implemented later
            'north_carolina': None,  # Will be implemented later
            'oklahoma': None,  # Will be implemented later
            'oregon': None,  # Will be implemented later
            'pennsylvania': None,  # Will be implemented later
            'south_carolina': None,  # Will be implemented later
            'south_dakota': None,  # Will be implemented later
            'vermont': None,  # Will be implemented later
            'virginia': None,  # Will be implemented later
            'washington': None,  # Will be implemented later
            'west_virginia': None,  # Will be implemented later
            'wyoming': None  # Will be implemented later
        }
        
        # Track existing IDs for first ingestion date preservation
        self.existing_ids = {}
        
        # Initialize processors
        self.office_standardizer = OfficeStandardizer()
        self.db_manager = get_db_connection()
        
        # Create directories if they don't exist
        for directory in [self.raw_data_dir, self.structured_dir, self.cleaner_dir, self.final_dir, "data/reports"]:
            Path(directory).mkdir(parents=True, exist_ok=True)
        
        # Clean up old processed files before starting
        self._cleanup_old_files()
    
    def run_pipeline(self) -> pd.DataFrame:
        """
        Run the new 5-phase pipeline:
        1. Structural parsing (extract data from messy files)
        2. ID generation (create stable IDs from structured data)
        3. State cleaning (improve data quality within each state)
        4. National standards (cross-state consistency)
        5. Output generation
        """
        logger.info("Starting new 5-phase pipeline")
        
        # Phase 1: Structural parsing
        logger.info("Phase 1: Running structural cleaners")
        structural_data = self._run_structural_cleaners()
        
        # Phase 2: ID generation
        logger.info("Phase 2: Generating stable IDs")
        all_data = self._generate_stable_ids(structural_data)
        
        # Phase 3: State data cleaning
        logger.info("Phase 3: Running state data cleaners")
        cleaned_data = self._run_state_cleaners(all_data)
        
        # Phase 4: National standards (simplified)
        logger.info("Phase 4: Applying national standards (simplified)")
        # For now, just combine the data without applying standards
        all_records = []
        for state, df in cleaned_data.items():
            df = df.copy()
            df['state'] = state.title()
            all_records.append(df)
        
        if all_records:
            final_data = pd.concat(all_records, ignore_index=True)
            logger.info(f"Combined data: {len(final_data)} records")
        else:
            final_data = pd.DataFrame()
            logger.warning("No data to combine")
        
        # Phase 5: Final processing
        logger.info("Phase 5: Final processing and output")
        final_data = self._final_processing(final_data)
        
        logger.info(f"Pipeline complete. Final dataset: {len(final_data)} records")
        return final_data
    
    def _run_structural_cleaners(self) -> Dict[str, pd.DataFrame]:
        """Phase 1: Extract structured data from messy raw files"""
        structural_data = {}
        
        for state, structural_cleaner in self.structural_cleaners.items():
            try:
                logger.info(f"Running structural cleaner for {state}")
                df = structural_cleaner.clean()
                structural_data[state] = df
                
                # Save structured output to structured directory
                if not df.empty:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_file = os.path.join(self.structured_dir, f"{state}_structured_{timestamp}.xlsx")
                    df.to_excel(output_file, index=False)
                    logger.info(f"Saved structured data for {state} to: {output_file}")
                
                logger.info(f"Structural cleaning complete for {state}: {len(df)} records")
            except Exception as e:
                logger.error(f"Structural cleaning failed for {state}: {e}")
                # Continue with other states
                continue
        
        return structural_data
    
    def _generate_stable_ids(self, structural_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Phase 2: Generate stable IDs from structured but raw data"""
        all_data = {}
        
        for state, df in structural_data.items():
            try:
                logger.info(f"Generating stable IDs for {state}")
                df_with_ids = self._add_stable_ids_to_dataframe(df, state)
                all_data[state] = df_with_ids
                logger.info(f"ID generation complete for {state}")
            except Exception as e:
                logger.error(f"ID generation failed for {state}: {e}")
                continue
        
        return all_data
    
    def _add_stable_ids_to_dataframe(self, df: pd.DataFrame, state: str) -> pd.DataFrame:
        """Add stable IDs to a single state's dataframe"""
        df = df.copy()
        
        # Generate stable IDs based on core candidate data
        stable_ids = []
        first_ingestion_dates = []
        
        for idx, row in df.iterrows():
            try:
                # Create stable ID from core fields (before any cleaning)
                stable_id, first_ingestion_date = self._generate_stable_id(row, state)
                stable_ids.append(stable_id)
                first_ingestion_dates.append(first_ingestion_date)
            except Exception as e:
                logger.warning(f"Failed to generate ID for row {idx} in {state}: {e}")
                stable_ids.append(None)
                first_ingestion_dates.append(None)
        
        # Add ID columns
        df['stable_id'] = stable_ids
        df['first_ingestion_date'] = first_ingestion_dates
        
        return df
    
    def _generate_stable_id(self, row: pd.Series, state: str) -> Tuple[str, datetime]:
        """Generate a stable ID for a candidate record"""
        # Use core fields for ID generation (before any cleaning/standardization)
        candidate_name = str(row.get('candidate_name', ''))
        office = str(row.get('office', ''))
        election_year = str(row.get('election_year', ''))
        
        # Create deterministic hash
        key = f"{candidate_name}|{state}|{office}|{election_year}"
        stable_id = hashlib.md5(key.encode()).hexdigest()[:12]
        
        # Check if we've seen this ID before
        if stable_id in self.existing_ids:
            # Use existing first_ingestion_date
            first_ingestion_date = self.existing_ids[stable_id]
        else:
            # New candidate - set current timestamp
            first_ingestion_date = datetime.now()
            self.existing_ids[stable_id] = first_ingestion_date
        
        return stable_id, first_ingestion_date
    
    def _run_state_cleaners(self, all_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Phase 3: Clean and standardize data within each state"""
        cleaned_data = {}
        
        for state, df in all_data.items():
            logger.info(f"Processing state cleaner for {state}, input type: {type(df)}")
            if state in self.state_cleaners and self.state_cleaners[state] is not None:
                try:
                    logger.info(f"Running data cleaner for {state}")
                    cleaner = self.state_cleaners[state]
                    cleaned_df = cleaner.clean(df)  # Pass the dataframe with IDs
                    logger.info(f"Cleaner returned type: {type(cleaned_df)}")
                    cleaned_data[state] = cleaned_df
                    
                    # Save cleaner output to cleaner directory
                    if not cleaned_df.empty:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        output_file = os.path.join(self.cleaner_dir, f"{state}_cleaned_{timestamp}.xlsx")
                        cleaned_df.to_excel(output_file, index=False)
                        logger.info(f"Saved cleaned data for {state} to: {output_file}")
                    
                    logger.info(f"Data cleaning complete for {state}: {len(cleaned_df)} records")
                except Exception as e:
                    logger.error(f"Data cleaning failed for {state}: {e}")
                    # Use structural data if cleaning fails
                    cleaned_data[state] = df
            else:
                # No data cleaner available, use structural data
                cleaned_data[state] = df
        
        logger.info(f"State cleaners completed. Returning {len(cleaned_data)} states")
        for state, df in cleaned_data.items():
            logger.info(f"  {state}: {type(df)}, {len(df) if hasattr(df, '__len__') else 'N/A'} records")
            if hasattr(df, 'columns'):
                logger.info(f"    Columns: {df.columns.tolist()}")
            else:
                logger.info(f"    No columns attribute - this is not a DataFrame!")
        
        return cleaned_data
    
    def _apply_national_standards(self, cleaned_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Phase 4: Apply cross-state consistency and national standards"""
        try:
            # Validate input
            if not isinstance(cleaned_data, dict):
                logger.error(f"cleaned_data is not a dict, it's {type(cleaned_data)}")
                return pd.DataFrame()
            
            logger.info(f"cleaned_data type: {type(cleaned_data)}")
            logger.info(f"cleaned_data keys: {list(cleaned_data.keys())}")
            
            # Combine all state data
            all_records = []
            for state, df in cleaned_data.items():
                logger.info(f"Processing {state} with type {type(df)} and {len(df) if hasattr(df, '__len__') else 'N/A'} records")
                if not hasattr(df, 'columns'):
                    logger.error(f"Data for {state} is not a DataFrame, it's {type(df)}")
                    continue
                logger.info(f"DataFrame columns: {df.columns.tolist()}")
                df['state'] = state.title()  # Ensure state column exists
                all_records.append(df)
            
            if not all_records:
                logger.warning("No data to process")
                return pd.DataFrame()
            
            combined_df = pd.concat(all_records, ignore_index=True)
            logger.info(f"Combined data: {len(combined_df)} records")
            
            # Apply national standards
            logger.info("Skipping party standardization for now...")
            # combined_df = self._standardize_parties(combined_df)
            
            logger.info("Skipping office standardization for now...")
            # combined_df = self._standardize_offices(combined_df)
            
            logger.info("Skipping address parsing for now...")
            # combined_df = self._parse_addresses(combined_df)
            
            # Apply deduplication logic
            logger.info("Skipping state-wide deduplication for now...")
            # combined_df = self._deduplicate_statewide_records(combined_df)
            
            logger.info("Skipping election deduplication for now...")
            # combined_df = self._deduplicate_election_records(combined_df)
            
            logger.info("National standards application completed successfully")
            return combined_df
            
        except Exception as e:
            logger.error(f"National standards application failed: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            # Return empty DataFrame instead of dict
            return pd.DataFrame()
    
    def _standardize_offices(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize office names using the office standardizer"""
        logger.info("Standardizing office names")
        
        try:
            standardized_df = self.office_standardizer.standardize_dataset(df, 'office')
            logger.info(f"Office standardization complete: {len(standardized_df)} records")
            return standardized_df
        except Exception as e:
            logger.error(f"Office standardization failed: {e}")
            return df
    
    def _deduplicate_statewide_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate records for state-wide offices appearing in multiple counties"""
        logger.info("Deduplicating state-wide records")
        
        # Identify state-wide offices (no district/county specificity)
        statewide_offices = [
            'Governor', 'Lieutenant Governor', 'Attorney General', 'Secretary of State',
            'Treasurer', 'Auditor', 'US Senate', 'US President'
        ]
        
        # Group by candidate + year + office + state
        groups = df.groupby(['stable_id', 'election_year', 'office', 'state'])
        
        deduplicated_records = []
        
        for (stable_id, year, office, state), group in groups:
            if office in statewide_offices and len(group) > 1:
                # Keep the most complete record, set county to NULL
                best_record = group.iloc[group.notna().sum(axis=1).idxmax()].copy()
                best_record['county'] = None
                deduplicated_records.append(best_record)
                logger.info(f"Deduplicated {len(group)} records for {office} in {state} {year}")
            else:
                # Not a state-wide office or no duplicates, keep all records
                deduplicated_records.extend(group.to_dict('records'))
        
        result_df = pd.DataFrame(deduplicated_records)
        logger.info(f"State-wide deduplication complete: {len(result_df)} records")
        return result_df
    
    def _deduplicate_election_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """Deduplicate records by election type (Primary, General, etc.)"""
        logger.info("Deduplicating election type records")
        
        # Group by candidate + year + office + state
        groups = df.groupby(['stable_id', 'election_year', 'office', 'state'])
        
        deduplicated_records = []
        
        for (stable_id, year, office, state), group in groups:
            if len(group) > 1:
                # Analyze election types
                election_types = group['election_type'].dropna().unique()
                
                if len(election_types) > 1:
                    # Multiple election types - merge into single record
                    best_record = group.iloc[group.notna().sum(axis=1).idxmax()].copy()
                    
                    # Create combined election type
                    if 'Primary' in election_types and 'General' in election_types:
                        best_record['election_type'] = 'Primary, General'
                    elif 'Primary' in election_types:
                        best_record['election_type'] = 'Primary'
                    elif 'General' in election_types:
                        best_record['election_type'] = 'General'
                    elif 'Special' in election_types:
                        best_record['election_type'] = 'Special'
                    else:
                        best_record['election_type'] = ', '.join(sorted(election_types))
                    
                    deduplicated_records.append(best_record)
                    logger.info(f"Merged {len(group)} election records for {office} in {state} {year}")
                else:
                    # Same election type, keep the best record
                    best_record = group.iloc[group.notna().sum(axis=1).idxmax()]
                    deduplicated_records.append(best_record)
            else:
                # Single record, keep as is
                deduplicated_records.extend(group.to_dict('records'))
        
        result_df = pd.DataFrame(deduplicated_records)
        logger.info(f"Election type deduplication complete: {len(result_df)} records")
        return result_df
    
    def _final_processing(self, df: pd.DataFrame) -> pd.DataFrame:
        """Phase 5: Final processing and output preparation"""
        logger.info("Running final processing")
        
        # Ensure required columns exist
        required_columns = ['stable_id', 'candidate_name', 'state', 'office', 'election_year']
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"Missing required column: {col}")
                df[col] = None
        
        # Sort by state, office, candidate name
        df = df.sort_values(['state', 'office', 'candidate_name']).reset_index(drop=True)
        
        # Final logging
        logger.info(f"Final dataset: {len(df)} records")
        logger.info(f"States represented: {df['state'].nunique()}")
        logger.info(f"Offices represented: {df['office'].nunique()}")
        
        return df
    
    def _cleanup_old_files(self):
        """Clean up old processed files, keeping only the latest version per state."""
        logger.info("Cleaning up old processed files...")
        
        # Remove ALL old files first
        all_old_files = list(Path(self.cleaner_dir).glob("*_cleaned_*.xlsx"))
        all_old_files.extend(list(Path(self.cleaner_dir).glob("*_cleaned_*.csv")))
        
        files_removed = 0
        for old_file in all_old_files:
            try:
                old_file.unlink()
                files_removed += 1
                logger.info(f"Removed old file: {old_file.name}")
            except Exception as e:
                logger.warning(f"Could not remove old file {old_file.name}: {e}")
        
        logger.info(f"Cleanup completed. Removed {files_removed} old files.")
        
        # Also clean up any temporary merged files
        temp_files = list(Path(self.cleaner_dir).glob("*_merged_temp*"))
        for temp_file in temp_files:
            try:
                temp_file.unlink()
                logger.info(f"Removed temporary file: {temp_file.name}")
            except Exception as e:
                logger.warning(f"Could not remove temporary file {temp_file.name}: {e}")

    def run_state_cleaners(self) -> Dict[str, str]:
        """Run all state cleaners on raw data."""
        logger.info("Starting state cleaning process...")
        
        cleaned_files = {}
        raw_files = self._get_raw_data_files()
        
        if not raw_files:
            logger.warning(f"No raw data files found in {self.raw_data_dir}")
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
                
                # Run state cleaner with proper output directory
                try:
                    cleaned_df = cleaner_func(raw_file, output_dir=self.cleaner_dir)
                except Exception as cleaner_error:
                    logger.error(f"State cleaner function failed for {state}: {cleaner_error}")
                    continue
                
                # Check if cleaning was successful
                if cleaned_df is None or cleaned_df.empty:
                    logger.error(f"State cleaner for {state} returned empty data")
                    continue
                
                # Prefer file saved by the cleaner itself if present; fallback to saving here
                base_name = os.path.splitext(os.path.basename(raw_file))[0]
                pattern = f"{base_name}_cleaned_*.xlsx"
                saved_candidates = sorted(
                    Path(self.cleaner_dir).glob(pattern),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True
                )
                if saved_candidates:
                    chosen_file = str(saved_candidates[0])
                    cleaned_files[state] = chosen_file
                    logger.info(f"✅ {state} cleaned successfully and using existing file: {chosen_file}")
                    # Remove redundant CSV outputs if a matching Excel file exists
                    try:
                        for csv_path in Path(self.cleaner_dir).glob(f"{base_name}_cleaned_*.csv"):
                            try:
                                csv_path.unlink()
                                logger.info(f"Removed redundant CSV: {csv_path.name}")
                            except Exception as e:
                                logger.warning(f"Could not remove redundant CSV {csv_path.name}: {e}")
                    except Exception as e:
                        logger.warning(f"Error while scanning redundant CSVs for {state}: {e}")
                else:
                    # Save here if the cleaner didn't write a file
                    try:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        output_file = os.path.join(self.cleaner_dir, f"{base_name}_cleaned_{timestamp}.xlsx")
                        cleaned_df.to_excel(output_file, index=False)
                        cleaned_files[state] = output_file
                        logger.info(f"✅ {state} cleaned successfully and saved to: {output_file}")
                        # Also remove any CSV that may have been created by the cleaner
                        try:
                            for csv_path in Path(self.cleaner_dir).glob(f"{base_name}_cleaned_*.csv"):
                                try:
                                    csv_path.unlink()
                                    logger.info(f"Removed redundant CSV: {csv_path.name}")
                                except Exception as e:
                                    logger.warning(f"Could not remove redundant CSV {csv_path.name}: {e}")
                        except Exception as e:
                            logger.warning(f"Error while scanning redundant CSVs for {state}: {e}")
                    except Exception as save_error:
                        logger.error(f"❌ Failed to save cleaned data for {state}: {save_error}")
                        continue
                    
            except Exception as e:
                logger.error(f"Error cleaning {state}: {e}")
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
                continue
        
        logger.info(f"State cleaning completed. {len(cleaned_files)} states processed.")
        
        # Clean up temporary merged files
        self._cleanup_temp_files()
        
        return cleaned_files
    
    def _extract_election_year_from_filename(self, filename: str) -> Optional[int]:
        """Extract election year from filename."""
        import re
        # Look for 4-digit year patterns in filename
        year_match = re.search(r'(\d{4})', filename)
        if year_match:
            year = int(year_match.group(1))
            # Validate it's a reasonable election year (1900-2100)
            if 1900 <= year <= 2100:
                return year
        return None
    
    def _cleanup_temp_files(self):
        """Clean up temporary merged files."""
        temp_files = list(Path(self.processed_dir).glob("*_merged_temp.xlsx"))
        for temp_file in temp_files:
            try:
                temp_file.unlink()
                logger.info(f"Cleaned up temporary file: {temp_file.name}")
            except Exception as e:
                logger.warning(f"Could not clean up temporary file {temp_file.name}: {e}")

    def _get_raw_data_files(self) -> List[str]:
        """Get all raw data files."""
        raw_files = []
        for ext in ['*.xlsx', '*.csv', '*.xls']:
            raw_files.extend(glob.glob(os.path.join(self.raw_data_dir, ext)))
        return raw_files

    def _find_state_file(self, raw_files: List[str], state: str) -> Optional[str]:
        """Find raw data file(s) for a specific state and merge if multiple exist."""
        state_lower = state.lower()
        matching_files = []
        
        for file_path in raw_files:
            filename = os.path.basename(file_path).lower()
            if state_lower in filename:
                matching_files.append(file_path)
        
        if not matching_files:
            return None
        
        if len(matching_files) == 1:
            return matching_files[0]
        
        # Multiple files found - merge them
        logger.info(f"Found {len(matching_files)} files for {state}, merging...")
        try:
            merged_df = self._merge_state_files(matching_files, state)
            if merged_df is None or merged_df.empty:
                logger.error(f"Failed to merge files for {state}")
                return None
            
            # Save merged file temporarily
            temp_file = os.path.join(self.processed_dir, f"{state}_merged_temp.xlsx")
            merged_df.to_excel(temp_file, index=False)
            logger.info(f"Merged {len(merged_df)} records from {len(matching_files)} files for {state}")
            
            return temp_file
        except Exception as e:
            logger.error(f"Failed to merge files for {state}: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None
    
    def _merge_state_files(self, file_paths: List[str], state: str) -> pd.DataFrame:
        """Merge multiple raw data files for a state."""
        all_data = []
        
        for file_path in file_paths:
            try:
                # Handle different file types
                if file_path.endswith('.csv'):
                    # Try multiple encodings for CSV files
                    df = None
                    encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
                    
                    for encoding in encodings_to_try:
                        try:
                            df = pd.read_csv(file_path, encoding=encoding)
                            logger.info(f"Successfully read {os.path.basename(file_path)} with {encoding} encoding")
                            break
                        except UnicodeDecodeError:
                            continue
                        except Exception as e:
                            logger.warning(f"Error reading {os.path.basename(file_path)} with {encoding} encoding: {e}")
                            continue
                    
                    if df is None:
                        logger.error(f"Failed to read {os.path.basename(file_path)} with any encoding")
                        continue
                        
                elif file_path.endswith('.xls'):
                    # Try to read as HTML first (since some .xls files are actually HTML)
                    try:
                        df = pd.read_html(file_path)[0]
                        logger.info(f"Successfully read {os.path.basename(file_path)} as HTML")
                    except Exception as html_error:
                        logger.info(f"File {os.path.basename(file_path)} is not HTML, trying as Excel...")
                        try:
                            df = pd.read_excel(file_path, engine='xlrd')
                            logger.info(f"Successfully read {os.path.basename(file_path)} as Excel")
                        except Exception as excel_error:
                            logger.error(f"Failed to read {os.path.basename(file_path)} as either HTML or Excel: {excel_error}")
                            continue
                else:
                    df = pd.read_excel(file_path)
                
                # Extract election year from filename
                filename = os.path.basename(file_path)
                election_year = self._extract_election_year_from_filename(filename)
                if election_year:
                    df['election_year'] = election_year
                    logger.info(f"Extracted election year {election_year} from {filename}")
                
                # Add source file info
                df['_source_file'] = filename
                all_data.append(df)
                logger.info(f"Loaded {len(df)} records from {filename}")
                
            except Exception as e:
                logger.warning(f"Error reading {file_path}: {e}")
                continue
        
        if not all_data:
            logger.error(f"No data could be loaded for {state}")
            return pd.DataFrame()
        
        # Merge all data
        try:
            merged_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"Successfully merged {len(merged_df)} total records for {state}")
            return merged_df
        except Exception as e:
            logger.error(f"Failed to merge data for {state}: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return pd.DataFrame()

    def run_office_standardization(self, cleaned_files: Dict[str, str]) -> str:
        """Run office standardization on cleaned data."""
        logger.info("Starting office standardization...")
        
        # Combine all cleaned data
        all_data = []
        for state, file_path in cleaned_files.items():
            try:
                df = pd.read_excel(file_path)
                if df is None or df.empty:
                    logger.warning(f"Empty data loaded from {state}")
                    continue
                df['source_state'] = state
                all_data.append(df)
                logger.info(f"Loaded {len(df)} records from {state}")
            except Exception as e:
                logger.error(f"Error loading {state} data: {e}")
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
                continue
        
        if not all_data:
            logger.error("No cleaned data to standardize")
            return None
        
        # Validate that we have data to process
        total_records = sum(len(df) for df in all_data)
        if total_records == 0:
            logger.error("All cleaned data files are empty")
            return None
        
        logger.info(f"Total records to standardize: {total_records}")
        
        # Combine all data
        try:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"Standardizing offices for {len(combined_df)} records")
        except Exception as e:
            logger.error(f"Failed to combine data from all states: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None
        
        # Standardize office names
        try:
            standardized_df = self.office_standardizer.standardize_dataset(combined_df, 'office')
            if standardized_df is None or standardized_df.empty:
                logger.error("Office standardization returned empty data")
                return None
        except Exception as e:
            logger.error(f"Office standardization failed: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None
        
        # Save standardized data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.final_dir, f"all_states_office_standardized_{timestamp}.xlsx")
        
        try:
            standardized_df.to_excel(output_file, index=False)
            logger.info(f"✅ Office standardization completed. Saved to: {output_file}")
            return output_file
        except Exception as e:
            logger.error(f"Failed to save standardized data: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None

    def run_national_standardization(self, standardized_file: str) -> str:
        """Run national-level standardization (party names, addresses, etc.)."""
        logger.info("Starting national standardization...")
        
        # Load standardized data
        try:
            df = pd.read_excel(standardized_file)
            if df is None or df.empty:
                logger.error("Standardized file is empty or could not be loaded")
                return None
        except Exception as e:
            logger.error(f"Failed to load standardized file: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None
        
        # Party standardization
        try:
            df = self._standardize_parties(df)
        except Exception as e:
            logger.error(f"Party standardization failed: {e}")
            # Continue with original data
        
        # Address parsing and standardization
        try:
            df = self._parse_addresses(df)
        except Exception as e:
            logger.error(f"Address parsing failed: {e}")
            # Continue with original data
        
        # Other national standardizations
        try:
            df = self._apply_national_standards(df)
        except Exception as e:
            logger.error(f"National standards application failed: {e}")
            # Continue with original data
        
        # Save nationally standardized data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.final_dir, f"all_states_nationally_standardized_{timestamp}.xlsx")
        
        try:
            df.to_excel(output_file, index=False)
            logger.info(f"✅ National standardization completed. Saved to: {output_file}")
            return output_file
        except Exception as e:
            logger.error(f"Failed to save nationally standardized data: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None

    def _standardize_parties(self, df: pd.DataFrame) -> pd.DataFrame:
        """Comprehensive party name standardization."""
        logger.info("Standardizing party names...")
        
        # Comprehensive party mappings
        party_mappings = {
            # Democratic variations
            'democrat': 'Democratic',
            'democratic': 'Democratic',
            'dem': 'Democratic',
            'democratic party': 'Democratic',
            'democrats': 'Democratic',
            
            # Republican variations
            'republican': 'Republican',
            'republicans': 'Republican',
            'rep': 'Republican',
            'gop': 'Republican',
            'grand old party': 'Republican',
            
            # Independent variations
            'independent': 'Independent',
            'ind': 'Independent',
            'independents': 'Independent',
            'no party': 'Independent',
            'no party preference': 'Independent',
            'unaffiliated': 'Independent',
            'una': 'Independent',
            
            # Libertarian variations
            'libertarian': 'Libertarian',
            'lib': 'Libertarian',
            'libertarians': 'Libertarian',
            'lbt': 'Libertarian',
            
            # Green Party variations
            'green': 'Green Party',
            'green party': 'Green Party',
            'gre': 'Green Party',
            'greens': 'Green Party',
            
            # Constitution Party variations
            'constitution': 'Constitution Party',
            'constitution party': 'Constitution Party',
            'cst': 'Constitution Party',
            'constitutional': 'Constitution Party',
            
            # Nonpartisan variations
            'nonpartisan': 'Nonpartisan',
            'non-partisan': 'Nonpartisan',
            'non partisan': 'Nonpartisan',
            'nonpartisan judicial': 'Nonpartisan',
            'non-partisan judicial': 'Nonpartisan',
            
            # Progressive variations
            'progressive': 'Progressive',
            'progressive party': 'Progressive',
            
            # Other parties
            'reform': 'Reform Party',
            'natural law': 'Natural Law Party',
            'socialist': 'Socialist Party',
            'communist': 'Communist Party',
            'american independent': 'American Independent Party',
            'peace and freedom': 'Peace and Freedom Party',
            'working families': 'Working Families Party',
            'women\'s equality': 'Women\'s Equality Party',
            'independence': 'Independence Party',
            'conservative': 'Conservative Party',
            'liberal': 'Liberal Party',
            'moderate': 'Moderate Party',
            'tea party': 'Tea Party',
            'write-in': 'Write-In',
            'other': 'Other'
        }
        
        if 'party' in df.columns:
            try:
                # Step 1: Infer party from office context if party is missing
                df = self._infer_party_from_office(df)
                
                # Step 2: Standardize party names using comprehensive mappings
                party_series = df['party'].astype(str)
                
                # Handle case variations first (ALL CAPS → Title Case)
                party_series = party_series.str.title()
                
                # Apply comprehensive party mappings
                df['party_standardized'] = party_series.str.lower().map(party_mappings).fillna(party_series)
                
                # Clean up any remaining issues
                df['party_standardized'] = df['party_standardized'].replace({
                    'nan': None,
                    'None': None,
                    '': None
                })
                
                logger.info("Comprehensive party standardization completed")
                
                # Log improvement statistics
                original_coverage = df['party'].notna().sum()
                improved_coverage = df['party_standardized'].notna().sum()
                logger.info(f"Party coverage improved from {original_coverage:,} to {improved_coverage:,} records")
                
            except Exception as e:
                logger.error(f"Party standardization failed: {e}")
                # Add a default column if standardization fails
                df['party_standardized'] = df['party']
        else:
            logger.warning("No 'party' column found for standardization")
            df['party_standardized'] = None
        
        return df
    
    def _infer_party_from_office(self, df: pd.DataFrame) -> pd.DataFrame:
        """Infer missing party information from office context."""
        logger.info("Inferring party from office context...")
        
        if 'office' not in df.columns:
            return df
        
        # Create a copy to avoid modifying original
        df_inferred = df.copy()
        
        # Party indicators in office names - comprehensive coverage
        party_indicators = {
            # Single letter indicators
            r'\(D\)': 'Democratic',
            r'\(R\)': 'Republican', 
            r'\(I\)': 'Independent',
            r'\(L\)': 'Libertarian',
            r'\(G\)': 'Green Party',
            r'\(C\)': 'Constitution Party',
            r'\(P\)': 'Progressive',
            r'\(W\)': 'Working Families',
            r'\(N\)': 'Natural Law',
            r'\(S\)': 'Socialist',
            r'\(A\)': 'American Independent',
            r'\(F\)': 'Peace and Freedom',
            r'\(E\)': 'Women\'s Equality',
            
            # Full party names
            r'Democratic Party': 'Democratic',
            r'Republican Party': 'Republican',
            r'Independent Party': 'Independent',
            r'Libertarian Party': 'Libertarian',
            r'Green Party': 'Green Party',
            r'Constitution Party': 'Constitution Party',
            r'Progressive Party': 'Progressive',
            r'Working Families Party': 'Working Families',
            r'Natural Law Party': 'Natural Law',
            r'Socialist Party': 'Socialist',
            r'American Independent Party': 'American Independent',
            r'Peace and Freedom Party': 'Peace and Freedom',
            r'Women\'s Equality Party': 'Women\'s Equality',
            
            # Abbreviated party names
            r'DEM': 'Democratic',
            r'REP': 'Republican',
            r'IND': 'Independent',
            r'LIB': 'Libertarian',
            r'GRE': 'Green Party',
            r'CST': 'Constitution Party',
            r'PRO': 'Progressive',
            r'WFP': 'Working Families',
            r'NLP': 'Natural Law',
            r'SOC': 'Socialist',
            r'AIP': 'American Independent',
            r'PFP': 'Peace and Freedom',
            r'WEP': 'Women\'s Equality',
            
            # Party indicators in different positions
            r'- Democratic': 'Democratic',
            r'- Republican': 'Republican',
            r'- Independent': 'Independent',
            r'- Libertarian': 'Libertarian',
            r'- Green': 'Green Party',
            r'- Constitution': 'Constitution Party',
            r'- Progressive': 'Progressive',
            r'- Working Families': 'Working Families',
            r'- Natural Law': 'Natural Law',
            r'- Socialist': 'Socialist',
            r'- American Independent': 'American Independent',
            r'- Peace and Freedom': 'Peace and Freedom',
            r'- Women\'s Equality': 'Women\'s Equality'
        }
        
        inferred_count = 0
        
        for idx, row in df_inferred.iterrows():
            # Only infer if party is missing or null
            if pd.isna(row['party']) or str(row['party']).strip() in ['', 'nan', 'None']:
                office_str = str(row['office']) if pd.notna(row['office']) else ''
                
                # Check for party indicators
                for pattern, party in party_indicators.items():
                    if re.search(pattern, office_str, re.IGNORECASE):
                        df_inferred.at[idx, 'party'] = party
                        inferred_count += 1
                        break
        
        if inferred_count > 0:
            logger.info(f"Inferred party for {inferred_count:,} records from office context")
        else:
            logger.info("No additional party information inferred from office context")
        
        return df_inferred
    
    def _parse_addresses(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse and standardize address fields."""
        logger.info("Parsing and standardizing addresses...")
        
        if 'address' in df.columns:
            try:
                # Add address parsing columns
                df['address_parsed'] = True
                # Ensure address column is string type before using .str methods
                address_series = df['address'].astype(str)
                df['address_clean'] = address_series.str.strip()
                
                # Extract common address components
                df['has_zip'] = address_series.str.contains(r'\d{5}(?:-\d{4})?', na=False)
                df['has_state'] = address_series.str.contains(r'\b[A-Z]{2}\b', na=False)
                
                logger.info("Address parsing completed")
            except Exception as e:
                logger.error(f"Address parsing failed: {e}")
                # Add default columns if parsing fails
                df['address_parsed'] = False
                df['address_clean'] = df['address']
                df['has_zip'] = False
                df['has_state'] = False
        else:
            logger.warning("No 'address' column found for parsing")
            df['address_parsed'] = False
            df['address_clean'] = None
            df['has_zip'] = False
            df['has_state'] = False
        
        # Fix Alaska county capitalization
        if 'county' in df.columns and 'state' in df.columns:
            try:
                # Fix Alaska counties from ALL CAPS to Proper Case
                alaska_mask = df['state'].str.contains('Alaska', case=False, na=False)
                if alaska_mask.any():
                    df.loc[alaska_mask, 'county'] = df.loc[alaska_mask, 'county'].str.title()
                    logger.info(f"Fixed capitalization for {alaska_mask.sum()} Alaska county records")
            except Exception as e:
                logger.warning(f"Alaska county capitalization fix failed: {e}")
        
        return df

    def _apply_national_standards(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply other national-level standardizations."""
        logger.info("Applying national standards...")
        
        try:
            # Add processing metadata
            df['processing_timestamp'] = datetime.now()
            df['pipeline_version'] = '1.0'
            df['data_source'] = 'state_filings'
            
            # Ensure consistent column names
            df.columns = df.columns.astype(str).str.lower().str.replace(' ', '_')
            
            logger.info("National standards applied")
        except Exception as e:
            logger.error(f"National standards application failed: {e}")
            # Add basic metadata even if column processing fails
            df['processing_timestamp'] = datetime.now()
            df['pipeline_version'] = '1.0'
            df['data_source'] = 'state_filings'
        
        return df

    def run_data_audit(self) -> Tuple[str, str]:
        """Run comprehensive data audit."""
        logger.info("Starting data audit...")
        
        # Run various audits
        try:
            quality_results = self._audit_data_quality()
        except Exception as e:
            logger.error(f"Data quality audit failed: {e}")
            quality_results = pd.DataFrame()
        
        try:
            consistency_results = self._audit_column_consistency()
        except Exception as e:
            logger.error(f"Column consistency audit failed: {e}")
            consistency_results = pd.DataFrame()
        
        try:
            address_audit = self._audit_address_fields()
        except Exception as e:
            logger.error(f"Address field audit failed: {e}")
            address_audit = pd.DataFrame()
        
        try:
            separation_audit = self._analyze_address_separation()
        except Exception as e:
            logger.error(f"Address separation analysis failed: {e}")
            separation_audit = pd.DataFrame()
        
        # Generate comprehensive report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        audit_file = f"data/reports/comprehensive_audit_{timestamp}.xlsx"
        
        try:
            with pd.ExcelWriter(audit_file, engine='openpyxl') as writer:
                if not quality_results.empty:
                    quality_results.to_excel(writer, sheet_name='Data_Quality', index=False)
                if not consistency_results.empty:
                    consistency_results.to_excel(writer, sheet_name='Column_Consistency', index=False)
                if not address_audit.empty:
                    address_audit.to_excel(writer, sheet_name='Address_Audit', index=False)
                if not separation_audit.empty:
                    separation_audit.to_excel(writer, sheet_name='Address_Separation', index=False)
            
            logger.info(f"✅ Data audit completed. Report saved to: {audit_file}")
            return audit_file, "audit_completed"
        except Exception as e:
            logger.error(f"Failed to generate audit report: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None, "audit_failed"

    def _audit_data_quality(self) -> pd.DataFrame:
        """Audit data quality across all processed state data."""
        logger.info("Starting data quality audit...")
        pattern = os.path.join(self.processed_dir, "*_cleaned_*.xlsx")
        files = glob.glob(pattern)
        
        if not files:
            logger.warning(f"No processed files found in {self.processed_dir}")
            return pd.DataFrame()
        
        quality_results = []
        
        for i, file_path in enumerate(files):
            try:
                logger.info(f"Auditing file {i+1}/{len(files)}: {Path(file_path).name}")
                df = pd.read_excel(file_path)
                state = Path(file_path).stem.split('_')[0].title()
                
                # Sample data for faster processing (max 1000 records)
                try:
                    if len(df) > 1000:
                        df_sample = df.sample(n=1000, random_state=42)
                        logger.info(f"Sampling 1000 records from {len(df)} total for {state}")
                    else:
                        df_sample = df
                except Exception as e:
                    logger.warning(f"Could not sample data for {state}: {e}")
                    df_sample = df
                
                # Basic quality metrics
                total_records = len(df)
                duplicates = df.duplicated().sum()
                null_values = df.isnull().sum().sum()
                empty_strings = (df == '').sum().sum()
                # Check for whitespace-only strings safely
                try:
                    # Convert to string and check for whitespace-only
                    df_str = df.astype(str)
                    whitespace_only = (df_str.str.strip() == '').sum().sum()
                except Exception as e:
                    logger.warning(f"Could not check whitespace for {state}: {e}")
                    whitespace_only = 0
                
                # Calculate quality score
                quality_score = self._calculate_quality_score(
                    total_records, duplicates, null_values, empty_strings, whitespace_only
                )
                
                quality_results.append({
                    'state': state,
                    'file': Path(file_path).name,
                    'total_records': total_records,
                    'duplicates': duplicates,
                    'null_values': null_values,
                    'empty_strings': empty_strings,
                    'whitespace_only': whitespace_only,
                    'quality_score': quality_score,
                    'columns': len(df.columns)
                })
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                continue
        
        logger.info(f"Data quality audit completed for {len(quality_results)} files")
        return pd.DataFrame(quality_results)

    def _calculate_quality_score(self, total_records, duplicates, null_values, empty_strings, whitespace_only):
        """Calculate a data quality score (0-100)."""
        if total_records == 0:
            return 0
        
        # Penalize various issues
        duplicate_penalty = (duplicates / total_records) * 30
        null_penalty = (null_values / (total_records * 10)) * 20  # Assuming ~10 columns
        empty_penalty = (empty_strings / total_records) * 25
        whitespace_penalty = (whitespace_only / total_records) * 25
        
        score = 100 - duplicate_penalty - null_penalty - empty_penalty - whitespace_penalty
        return max(0, min(100, score))

    def _audit_column_consistency(self) -> pd.DataFrame:
        """Audit column consistency across all state data."""
        logger.info("Starting column consistency audit...")
        pattern = os.path.join(self.processed_dir, "*_cleaned_*.xlsx")
        files = glob.glob(pattern)
        
        if not files:
            return pd.DataFrame()
        
        column_analysis = {}
        
        for i, file_path in enumerate(files):
            try:
                logger.info(f"Checking columns in file {i+1}/{len(files)}: {Path(file_path).name}")
                df = pd.read_excel(file_path)
                state = Path(file_path).stem.split('_')[0].title()
                
                for col in df.columns:
                    if col not in column_analysis:
                        column_analysis[col] = {
                            'column_name': col,
                            'states_with_column': [],
                            'data_types': set(),
                            'sample_values': []
                        }
                    
                    column_analysis[col]['states_with_column'].append(state)
                    column_analysis[col]['data_types'].add(str(df[col].dtype))
                    
                    # Sample only first 2 values for speed
                    try:
                        sample = df[col].dropna().head(2).tolist()
                        column_analysis[col]['sample_values'].extend(sample)
                    except Exception as e:
                        logger.warning(f"Could not sample values for column {col} in {state}: {e}")
                        column_analysis[col]['sample_values'].extend([])
                    
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
        
        # Convert to DataFrame
        consistency_results = []
        for col, info in column_analysis.items():
            consistency_results.append({
                'column_name': col,
                'states_with_column': len(info['states_with_column']),
                'states_list': str(info['states_with_column']),
                'data_types': str(list(info['data_types'])),
                'sample_values': str(info['sample_values'][:3])  # Limit to 3 samples
            })
        
        logger.info(f"Column consistency audit completed")
        return pd.DataFrame(consistency_results)

    def _audit_address_fields(self) -> pd.DataFrame:
        """Audit address fields across all state data."""
        logger.info("Starting address field audit...")
        pattern = os.path.join(self.processed_dir, "*_cleaned_*.xlsx")
        files = glob.glob(pattern)
        
        if not files:
            logger.warning(f"No processed files found in {self.processed_dir}")
            return pd.DataFrame()
        
        audit_results = []
        
        for i, file_path in enumerate(files):
            try:
                logger.info(f"Auditing addresses in file {i+1}/{len(files)}: {Path(file_path).name}")
                df = pd.read_excel(file_path)
                state = Path(file_path).stem.split('_')[0].title()
                
                # Check for address-related columns
                address_cols = [col for col in df.columns if 'address' in col.lower()]
                
                for col in address_cols:
                    # Sample only first 5 values for speed
                    try:
                        sample_values = df[col].dropna().head(5).tolist()
                    except Exception as e:
                        logger.warning(f"Could not sample values for column {col} in {state}: {e}")
                        sample_values = []
                    
                    # Check for common issues (simplified)
                    issues = []
                    if df[col].dtype == 'object' and sample_values:
                        # Quick separator check
                        separators = set()
                        for val in sample_values:
                            if isinstance(val, str):
                                if ',' in val:
                                    separators.add(',')
                                if ';' in val:
                                    separators.add(';')
                                if '|' in val:
                                    separators.add('|')
                        
                        if len(separators) > 1:
                            issues.append("Mixed separators")
                    
                    audit_results.append({
                        'state': state,
                        'file': Path(file_path).name,
                        'column': col,
                        'total_records': len(df),
                        'non_null_count': df[col].count(),
                        'null_count': df[col].isnull().sum(),
                        'sample_values': str(sample_values[:2]),  # Only 2 samples
                        'issues': '; '.join(issues) if issues else 'None'
                    })
                    
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                continue
        
        logger.info(f"Address field audit completed")
        return pd.DataFrame(audit_results)

    def _analyze_address_separation(self) -> pd.DataFrame:
        """Analyze address field separation patterns across all state data."""
        logger.info("Starting address separation analysis...")
        pattern = os.path.join(self.processed_dir, "*_cleaned_*.xlsx")
        files = glob.glob(pattern)
        
        if not files:
            return pd.DataFrame()
        
        separation_results = []
        
        for i, file_path in enumerate(files):
            try:
                logger.info(f"Analyzing addresses in file {i+1}/{len(files)}: {Path(file_path).name}")
                df = pd.read_excel(file_path)
                state = Path(file_path).stem.split('_')[0].title()
                
                # Find address columns
                address_cols = [col for col in df.columns if 'address' in col.lower()]
                
                for col in address_cols:
                    # Sample only first 100 records for speed
                    try:
                        sample_df = df[col].dropna().head(100)
                    except Exception as e:
                        logger.warning(f"Could not sample data for column {col} in {state}: {e}")
                        sample_df = pd.Series(dtype='object')
                    
                    separator_counts = {}
                    field_counts = []
                    
                    for val in sample_df:
                        if isinstance(val, str):
                            # Count separators
                            for sep in [',', ';', '|', '\t', '\n']:
                                if sep in val:
                                    separator_counts[sep] = separator_counts.get(sep, 0) + 1
                            
                            # Count fields (split by comma for now)
                            try:
                                fields = [f.strip() for f in val.split(',') if f.strip()]
                                field_counts.append(len(fields))
                            except Exception as e:
                                logger.warning(f"Could not parse fields for value in {col}: {e}")
                                field_counts.append(0)
                    
                    # Calculate statistics
                    try:
                        avg_fields = sum(field_counts) / len(field_counts) if field_counts else 0
                        max_fields = max(field_counts) if field_counts else 0
                        min_fields = min(field_counts) if field_counts else 0
                    except Exception as e:
                        logger.warning(f"Could not calculate statistics for {col} in {state}: {e}")
                        avg_fields = 0
                        max_fields = 0
                        min_fields = 0
                    
                    separation_results.append({
                        'state': state,
                        'file': Path(file_path).name,
                        'column': col,
                        'total_records': len(df),
                        'sample_size': len(sample_df),
                        'separator_counts': str(separator_counts),
                        'avg_fields': round(avg_fields, 2),
                        'max_fields': max_fields,
                        'min_fields': min_fields,
                        'field_count_distribution': str(field_counts[:10])  # Only first 10
                    })
                    
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                continue
        
        logger.info(f"Address separation analysis completed")
        return pd.DataFrame(separation_results)

    def run_deduplication(self, final_file: str) -> str:
        """Remove exact duplicate records."""
        logger.info("Starting deduplication process...")
        
        # Load final data
        try:
            df = pd.read_excel(final_file)
            if df is None or df.empty:
                logger.error("Final file is empty or could not be loaded")
                return None
            original_count = len(df)
        except Exception as e:
            logger.error(f"Failed to load final file: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None
        
        # Remove exact duplicates
        try:
            df_deduped = df.drop_duplicates()
            final_count = len(df_deduped)
            duplicates_removed = original_count - final_count
            
            logger.info(f"Removed {duplicates_removed} duplicate records")
        except Exception as e:
            logger.error(f"Deduplication failed: {e}")
            # Use original data if deduplication fails
            df_deduped = df
            final_count = len(df_deduped)
            duplicates_removed = 0
            logger.warning("Using original data due to deduplication failure")
        
        # Save deduplicated data with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.final_dir, f"candidate_filings_{timestamp}.xlsx")
        
        try:
            df_deduped.to_excel(output_file, index=False)
            logger.info(f"✅ Deduplication completed. Saved to: {output_file}")
            return output_file
        except Exception as e:
            logger.error(f"Failed to save deduplicated data: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None

    def upload_to_staging(self, final_file: str) -> bool:
        """Upload final data to staging database table."""
        logger.info("Starting staging database upload...")
        
        try:
            # Connect to database
            if not self.db_manager.connect():
                logger.error("Failed to connect to database")
                return False
            
            # Load final data
            df = pd.read_excel(final_file)
            logger.info(f"Loaded {len(df)} records for staging upload")
            
            # Clear existing staging data (replace all)
            logger.info("Clearing existing staging data...")
            clear_success = self.db_manager.clear_staging_table()
            if not clear_success:
                logger.error("Failed to clear staging table")
                return False
            
            # Upload new data to staging
            logger.info("Uploading data to staging table...")
            upload_success = self.db_manager.upload_to_staging(df)
            if not upload_success:
                logger.error("Failed to upload to staging table")
                return False
            
            # Verify upload
            record_count = self.db_manager.get_staging_record_count()
            logger.info(f"✅ Staging upload completed successfully: {record_count} records in staging")
            return True
            
        except Exception as e:
            logger.error(f"❌ Staging upload failed: {e}")
            return False
        finally:
            self.db_manager.disconnect()

    def run_full_pipeline(self) -> bool:
        """Run the complete pipeline from start to finish."""
        logger.info("🚀 Starting full CandidateFilings pipeline...")
        
        # Track pipeline progress for recovery
        pipeline_state = {
            'state_cleaning_completed': False,
            'office_standardization_completed': False,
            'national_standardization_completed': False,
            'deduplication_completed': False,
            'audit_completed': False,
            'staging_upload_completed': False
        }
        
        try:
            # Step 1: State cleaning
            logger.info("=== STEP 1: State Cleaning ===")
            cleaned_files = self.run_state_cleaners()
            if not cleaned_files:
                logger.error("State cleaning failed")
                return False
            
            pipeline_state['state_cleaning_completed'] = True
            logger.info(f"✅ State cleaning completed with {len(cleaned_files)} states")
            
            # Step 2: Office standardization
            logger.info("=== STEP 2: Office Standardization ===")
            office_standardized_file = self.run_office_standardization(cleaned_files)
            if not office_standardized_file:
                logger.error("Office standardization failed")
                return False
            
            pipeline_state['office_standardization_completed'] = True
            logger.info("✅ Office standardization completed")
            
            # Step 3: National standardization
            logger.info("=== STEP 3: National Standardization ===")
            nationally_standardized_file = self.run_national_standardization(office_standardized_file)
            if not nationally_standardized_file:
                logger.error("National standardization failed")
                return False
            
            pipeline_state['national_standardization_completed'] = True
            logger.info("✅ National standardization completed")
            
            # Step 4: Deduplication
            logger.info("=== STEP 4: Deduplication ===")
            final_file = self.run_deduplication(nationally_standardized_file)
            if not final_file:
                logger.error("Deduplication failed")
                return False
            
            pipeline_state['deduplication_completed'] = True
            logger.info("✅ Deduplication completed")
            
            # Step 5: Data audit
            logger.info("=== STEP 5: Data Audit ===")
            try:
                audit_file, audit_status = self.run_data_audit()
                if audit_status == "audit_failed":
                    logger.warning("Data audit failed, but continuing with pipeline")
                else:
                    pipeline_state['audit_completed'] = True
                    logger.info("✅ Data audit completed")
            except Exception as e:
                logger.error(f"Data audit step failed: {e}")
                logger.warning("Continuing with pipeline despite audit failure")
            
            # Step 6: Upload to staging database
            logger.info("=== STEP 6: Upload to Staging ===")
            try:
                staging_success = self.upload_to_staging(final_file)
                if not staging_success:
                    logger.error("Staging upload failed")
                    return False
                
                pipeline_state['staging_upload_completed'] = True
                logger.info("✅ Staging upload completed")
            except Exception as e:
                logger.error(f"Staging upload failed with exception: {e}")
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
                return False
            
            logger.info("✅ Data uploaded to staging successfully!")
            logger.info("📋 Review the data in staging before moving to production")
            logger.info("🚀 To move to production, run: python scripts/move_to_production.py")
            
            # Log pipeline completion summary
            logger.info("🎉 Full pipeline completed successfully!")
            logger.info("📊 Pipeline completion summary:")
            for step, completed in pipeline_state.items():
                status = "✅" if completed else "❌"
                step_name = step.replace('_', ' ').title()
                logger.info(f"  {status} {step_name}")
            
            # Final cleanup - keep only the latest cleaned file per state
            try:
                self._final_cleanup()
            except Exception as e:
                logger.warning(f"Final cleanup failed: {e}")
                # Don't fail the pipeline for cleanup issues
            
            # Remove intermediate final outputs so only one final file remains
            try:
                self._cleanup_final_outputs(file_to_keep=final_file)
            except Exception as e:
                logger.warning(f"Could not clean up intermediate final outputs: {e}")
            
            # Clean up old candidate_filings files, keep only the most recent
            try:
                self._cleanup_old_candidate_filings()
            except Exception as e:
                logger.warning(f"Could not clean up old candidate_filings files: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            return False

    def _final_cleanup(self):
        """Final cleanup to keep only the latest cleaned file per state."""
        logger.info("Performing final cleanup...")
        
        # Group files by state
        state_files = {}
        for file_path in Path(self.processed_dir).glob("*_cleaned_*.xlsx"):
            # Extract state name from filename (e.g., "alaska_candidates_2024_cleaned_20250816_111305.xlsx")
            parts = file_path.stem.split('_')
            if len(parts) >= 3 and parts[-2] == 'cleaned':
                state = parts[0]
                if state not in state_files:
                    state_files[state] = []
                state_files[state].append(file_path)
        
        # Keep only the latest file per state
        files_removed = 0
        for state, files in state_files.items():
            if len(files) > 1:
                try:
                    # Sort by modification time (newest first)
                    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                    # Remove all but the newest
                    for old_file in files[1:]:
                        try:
                            old_file.unlink()
                            files_removed += 1
                            logger.info(f"Final cleanup: Removed old file: {old_file.name}")
                        except Exception as e:
                            logger.warning(f"Could not remove old file {old_file.name}: {e}")
                except Exception as e:
                    logger.warning(f"Could not process files for state {state}: {e}")
        
        logger.info(f"Final cleanup completed. Removed {files_removed} old files.")
    
    def _cleanup_final_outputs(self, file_to_keep: Optional[str]) -> None:
        """Keep only the final candidate_filings file in `final_dir`, remove all others.
        
        Parameters
        ----------
        file_to_keep: Optional[str]
            Path to the final candidate_filings file that should be preserved.
        """
        logger.info("Cleaning up intermediate final outputs...")
        try:
            removed = 0
            for path in Path(self.final_dir).glob("*.xlsx"):
                try:
                    # Keep only candidate_filings files, remove all intermediate files
                    if "candidate_filings_" in path.name:
                        # This is a final output file, keep it
                        continue
                    else:
                        # This is an intermediate file, remove it
                        path.unlink()
                        removed += 1
                        logger.info(f"Removed intermediate final file: {path.name}")
                except Exception as e:
                    logger.warning(f"Could not remove final file {path.name}: {e}")
            logger.info(f"Final outputs cleanup completed. Removed {removed} files.")
        except Exception as e:
            logger.warning(f"Final outputs cleanup encountered an error: {e}")
    
    def _cleanup_old_candidate_filings(self) -> None:
        """Keep only the most recent candidate_filings file, remove older ones."""
        logger.info("Cleaning up old candidate_filings files...")
        try:
            candidate_files = list(Path(self.final_dir).glob("candidate_filings_*.xlsx"))
            if len(candidate_files) > 1:
                # Sort by modification time (newest first)
                candidate_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                # Remove all but the newest
                removed = 0
                for old_file in candidate_files[1:]:
                    try:
                        old_file.unlink()
                        removed += 1
                        logger.info(f"Removed old candidate_filings file: {old_file.name}")
                    except Exception as e:
                        logger.warning(f"Could not remove old file {old_file.name}: {e}")
                logger.info(f"Removed {removed} old candidate_filings files, keeping only: {candidate_files[0].name}")
            else:
                logger.info("No old candidate_filings files to clean up")
        except Exception as e:
            logger.warning(f"Could not clean up old candidate_filings files: {e}")
    
    def get_pipeline_status(self) -> dict:
        """Get current pipeline status and file counts."""
        status = {
            'raw_files': len(self._get_raw_data_files()),
            'processed_files': len(list(Path(self.processed_dir).glob("*_cleaned_*.xlsx"))),
            'final_files': len(list(Path(self.final_dir).glob("*.xlsx"))),
            'report_files': len(list(Path("data/reports").glob("*.xlsx"))),
            'log_files': len(list(Path("data/logs").glob("*.log")))
        }
        return status

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
