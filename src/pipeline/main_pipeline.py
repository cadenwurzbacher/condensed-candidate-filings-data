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
from typing import Dict, List, Optional, Tuple, Any
import glob
import sys
import re
from pathlib import Path
import hashlib
import json

# Add current directory to Python path for imports
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

# Import dynamic import system
from .dynamic_imports import dynamic_importer

# Import processors
from .office_standardizer import OfficeStandardizer
from .smart_staging_manager import SmartStagingManager
from .national_standards import NationalStandards

# Import database utilities
from src.config.database import get_db_connection

# Import new pipeline infrastructure
from .pipeline_config import PipelineConfig
from .managers import DataManager, DatabaseManager, DataProcessor

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

    def __init__(self, config: PipelineConfig = None, data_dir: str = "data"):
        """
        Initialize the main pipeline.
        
        Args:
            config: PipelineConfig instance (if None, creates default config)
            data_dir: Data directory path
        """
        # Initialize configuration
        self.config = config if config is not None else PipelineConfig()
        self.data_dir = data_dir
        
        # Log configuration status
        logger.info("Pipeline Configuration:")
        logger.info(str(self.config))
        
        # Initialize manager classes
        self.data_manager = DataManager(self.config)
        self.database_manager = DatabaseManager(self.config)
        self.data_processor = DataProcessor(self.config)
        
        # Legacy support - keep existing attributes for backward compatibility
        self._initialize_legacy_attributes()
        
        # Create directories if they don't exist
        for directory in [self.raw_data_dir, self.structured_dir, self.cleaner_dir, self.final_dir, self.processed_dir, "data/reports"]:
            Path(directory).mkdir(parents=True, exist_ok=True)
        
        # Create OLD folder within final directory
        old_final_dir = os.path.join(self.final_dir, "OLD")
        Path(old_final_dir).mkdir(parents=True, exist_ok=True)
        
        # Clean up old processed files before starting
        self._cleanup_old_files()
    
    def _initialize_legacy_attributes(self):
        """Initialize legacy attributes for backward compatibility."""
        # Define directory structure (legacy)
        self.raw_data_dir = os.path.join(self.data_dir, "raw")
        self.structured_dir = os.path.join(self.data_dir, "structured")
        self.cleaner_dir = os.path.join(self.data_dir, "cleaner")
        self.final_dir = os.path.join(self.data_dir, "final")
        self.processed_dir = os.path.join(self.data_dir, "processed")
        
        # Dynamic structural cleaner mapping (using dynamic importer)
        self.structural_cleaners = {}
        for state in dynamic_importer.get_available_structural_states():
            try:
                cleaner_class = dynamic_importer.get_structural_cleaner(state)
                self.structural_cleaners[state] = cleaner_class()
                logger.debug(f"Initialized structural cleaner for {state}: {cleaner_class.__name__}")
            except Exception as e:
                logger.error(f"Failed to initialize structural cleaner for {state}: {e}")
        
        # Dynamic state cleaner mapping (using dynamic importer)
        self.state_cleaners = {}
        for state in dynamic_importer.get_available_states():
            try:
                cleaner_class = dynamic_importer.get_state_cleaner(state)
                self.state_cleaners[state] = cleaner_class()
                logger.debug(f"Initialized state cleaner for {state}: {cleaner_class.__name__}")
            except Exception as e:
                logger.error(f"Failed to initialize state cleaner for {state}: {e}")
        
        # Log cleaner discovery summary
        logger.info(dynamic_importer.get_cleaner_summary())
        
        # Track existing IDs for first ingestion date preservation
        self.existing_ids = {}
        
        # Legacy database manager (kept for backward compatibility)
        self.db_manager = get_db_connection()
        
        # Test database connection (legacy)
        if self.config.enable_database_connection and self.db_manager.connect():
            logger.info("✅ Database connection established successfully")
            # Test if we can query the database
            try:
                test_result = self.db_manager.execute_query("SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('filings', 'staging_candidates')")
                logger.info(f"Database test query successful: {test_result}")
            except Exception as e:
                logger.warning(f"Database test query failed: {e}")
                logger.warning("Pipeline will run without database connectivity")
        else:
            logger.warning("❌ Database connection failed - pipeline will run without database connectivity")
        
        # Legacy processors (kept for backward compatibility)
        self.office_standardizer = OfficeStandardizer()
        self.smart_staging_manager = SmartStagingManager(self.db_manager)
        self.national_standards = NationalStandards()
        old_final_dir = os.path.join(self.final_dir, "OLD")
        Path(old_final_dir).mkdir(parents=True, exist_ok=True)
        
        # Clean up old processed files before starting
        self._cleanup_old_files()
    
    def test_database_connection(self) -> bool:
        """Test database connection and basic functionality."""
        try:
            if not self.db_manager:
                logger.error("No database manager available")
                return False
            
            # Test basic connection
            if not self.db_manager.test_connection():
                logger.error("Database connection test failed")
                return False
            
            # Test if we can query the tables
            try:
                # Check if tables exist
                tables_query = "SELECT table_name FROM information_schema.tables WHERE table_name IN ('filings', 'staging_candidates')"
                tables_result = self.db_manager.execute_query(tables_query)
                logger.info(f"Available tables: {tables_result['table_name'].tolist() if not tables_result.empty else 'None'}")
                
                # Try to get a sample of data from each table
                if not tables_result.empty:
                    for table_name in tables_result['table_name']:
                        try:
                            sample_query = f"SELECT COUNT(*) as count FROM {table_name} LIMIT 1"
                            sample_result = self.db_manager.execute_query(sample_query)
                            logger.info(f"Table {table_name}: {sample_result.iloc[0]['count'] if not sample_result.empty else 'No data'}")
                        except Exception as e:
                            logger.warning(f"Could not query table {table_name}: {e}")
                
                return True
                
            except Exception as e:
                logger.error(f"Database query test failed: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def run_pipeline(self) -> pd.DataFrame:
        """
        Run the toggle-based pipeline with configurable phases and operations.
        
        The pipeline follows these phases (all configurable):
        1. Structural parsing (extract data from messy files)
        2. ID generation (create stable IDs from structured data)
        3. State cleaning (improve data quality within each state)
        4. National standards (cross-state consistency)
        5. Final processing and output generation
        """
        logger.info("Starting toggle-based pipeline")
        logger.info(f"Configuration: {self.config}")
        
        # Load raw data into memory
        logger.info("Loading raw data files into memory...")
        raw_data = self.data_manager.load_raw_data()
        
        if not raw_data:
            logger.warning("No raw data found to process")
            return pd.DataFrame()
        
        logger.info(f"Loaded raw data for {len(raw_data)} states")
        
        # Phase 1: Structural parsing (if enabled)
        if self.config.enable_phase_1_structural:
            logger.info("Phase 1: Running structural cleaners")
            structural_data = self.data_processor.run_structural_cleaners(raw_data)
            
            # Save structured files (if enabled)
            if self.config.save_structured_files:
                self.data_manager.save_structured_files(structural_data)
        else:
            logger.info("Phase 1: Skipped (disabled)")
            structural_data = raw_data
        
        # Phase 2: ID generation (if enabled)
        if self.config.enable_phase_2_id_generation:
            logger.info("Phase 2: Generating stable IDs")
            structured_data = self.data_processor.generate_stable_ids(structural_data)
        else:
            logger.info("Phase 2: Skipped (disabled)")
            # Keep the same data for next phase
            structured_data = structural_data
        
        # Phase 3: State cleaning (if enabled)
        if self.config.enable_phase_3_state_cleaning:
            logger.info("Phase 3: Running state data cleaners")
            cleaned_data = self.data_processor.run_state_cleaners(structural_data)
            
            # Save cleaned files (if enabled)
            if self.config.save_cleaned_files:
                self.data_manager.save_cleaned_files(cleaned_data)
        else:
            logger.info("Phase 3: Skipped (disabled)")
            cleaned_data = structured_data
        
        # Phase 4: National standards (if enabled)
        if self.config.enable_phase_4_national_standards:
            logger.info("Phase 4: Applying national standards")
            combined_data = self.data_processor.apply_national_standards(cleaned_data)
        else:
            logger.info("Phase 4: Skipped (disabled)")
            # Combine data without applying national standards
            combined_data = self.data_processor._combine_state_data(cleaned_data)
        
        if combined_data.empty:
            logger.warning("No data to process after Phase 4")
            return pd.DataFrame()
        
        logger.info(f"Combined data after Phase 4: {len(combined_data)} records")
        
        # Phase 5: Final processing (if enabled)
        if self.config.enable_phase_5_final_processing:
            logger.info("Phase 5: Final processing and output preparation")
            final_data = self.data_processor.final_processing(combined_data)
            
            # Add processing metadata
            final_data = self.data_processor.add_processing_metadata(final_data)
        else:
            logger.info("Phase 5: Skipped (disabled)")
            final_data = combined_data
        
        # Database operations (if enabled)
        if self.config.enable_database_upload:
            logger.info("Database operations: Starting uploads")
            
            # Upload to staging (if enabled)
            if self.config.enable_staging_table:
                self.database_manager.upload_to_staging(final_data)
            
            # Upload to production (if enabled)
            if self.config.enable_production_table:
                self.database_manager.upload_to_production(final_data)
        else:
            logger.info("Database operations: Skipped (disabled)")
        
        # Save final file (if enabled)
        if self.config.save_final_file:
            self.data_manager.save_final_file(final_data)
        else:
            logger.info("Final file save: Skipped (disabled)")
        
        # Cleanup intermediate files (if enabled)
        if hasattr(self.config, 'clear_intermediate_files') and self.config.clear_intermediate_files:
            self.data_manager.cleanup_intermediate_files()
        
        # Log final status
        logger.info("Pipeline complete!")
        logger.info(f"Final dataset: {len(final_data)} records")
        logger.info(f"States represented: {final_data['state'].nunique() if not final_data.empty else 0}")
        logger.info(f"Offices represented: {final_data['office'].nunique() if not final_data.empty else 0}")
        
        return final_data
    
    def get_pipeline_status(self) -> dict:
        """Get current pipeline status and configuration."""
        return {
            'configuration': {
                'enabled_phases': self.config.get_enabled_phases(),
                'database_status': self.config.get_database_status(),
                'file_output_status': self.config.get_file_output_status()
            },
            'data_status': self.data_manager.get_pipeline_status(),
            'database_status': self.database_manager.get_database_status(),
            'data_directories': {
                'raw_data_dir': self.raw_data_dir,
                'structured_dir': self.structured_dir,
                'cleaner_dir': self.cleaner_dir,
                'final_dir': self.final_dir,
                'processed_dir': self.processed_dir
            }
        }
    
    def _save_excel_robust(self, df: pd.DataFrame, output_file: str) -> bool:
        """
        Robust Excel file saving with error handling and validation.
        
        Args:
            df: DataFrame to save
            output_file: Output file path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Starting robust Excel save for {len(df):,} records...")
            
            # Step 1: Create temporary file first
            temp_file = output_file.replace('.xlsx', '_temp.xlsx')
            
            # Step 2: Save with explicit engine and options
            with pd.ExcelWriter(temp_file, engine='openpyxl', mode='w') as writer:
                df.to_excel(writer, sheet_name='Data', index=False)
                # Context manager automatically closes the writer
            
            # Step 3: Validate the temporary file
            if not os.path.exists(temp_file):
                logger.error("Temporary file was not created")
                return False
            
            temp_size = os.path.getsize(temp_file)
            if temp_size == 0:
                logger.error("Temporary file is empty")
                os.remove(temp_file)
                return False
            
            # Step 4: Test read the temporary file
            try:
                test_df = pd.read_excel(temp_file, engine='openpyxl')
                if len(test_df) != len(df):
                    logger.error(f"Temporary file record count mismatch: {len(test_df)} vs {len(df)}")
                    os.remove(temp_file)
                    return False
                logger.info("Temporary file validation successful")
            except Exception as e:
                logger.error(f"Temporary file validation failed: {e}")
                os.remove(temp_file)
                return False
            
            # Step 5: Move temporary file to final location
            import shutil
            shutil.move(temp_file, output_file)
            
            # Step 6: Final validation
            final_size = os.path.getsize(output_file)
            if final_size != temp_size:
                logger.error(f"File size changed during move: {temp_size} -> {final_size}")
                return False
            
            logger.info(f"Excel file saved successfully: {output_file} ({final_size:,} bytes)")
            return True
            
        except Exception as e:
            logger.error(f"Robust Excel save failed: {e}")
            # Clean up any temporary files
            temp_file = output_file.replace('.xlsx', '_temp.xlsx')
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
            return False
    
    def _run_structural_cleaners(self) -> Dict[str, pd.DataFrame]:
        """Phase 1: Extract structured data from messy raw files"""
        # Clear structured directory before processing
        self._clear_structured_directory()
        
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
                    
                    # Use robust Excel writing
                    success = self._save_excel_robust(df, output_file)
                    if success:
                        logger.info(f"Saved structured data for {state} to: {output_file}")
                    else:
                        logger.warning(f"Failed to save Excel for {state}, using CSV fallback")
                        csv_output = output_file.replace('.xlsx', '.csv')
                        df.to_csv(csv_output, index=False)
                        logger.info(f"Saved structured data for {state} to: {csv_output}")
                
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
        """Add stable IDs to a single state's dataframe (dates will be set in Phase 5)"""
        df = df.copy()
        
        # Filter out records with missing critical data BEFORE stable ID generation
        logger.info(f"Filtering records with missing critical data for {state}...")
        initial_count = len(df)
        
        # Remove records with null/empty candidate_name, office, or election_year
        df = df.dropna(subset=['candidate_name', 'office', 'election_year'])
        
        # Also remove records with empty strings
        df = df[
            (df['candidate_name'].str.strip() != '') & 
            (df['office'].str.strip() != '') & 
            (df['election_year'].notna())  # election_year is numeric, just check if not null
        ]
        
        filtered_count = len(df)
        removed_count = initial_count - filtered_count
        
        if removed_count > 0:
            logger.warning(f"Removed {removed_count} records with missing critical data in {state}")
            logger.info(f"Proceeding with {filtered_count} valid records for stable ID generation")
        
        if df.empty:
            logger.warning(f"No valid records remaining for {state} after filtering")
            # Return empty DataFrame with required columns
            df['stable_id'] = []
            df['first_added_date'] = []
            df['last_updated_date'] = []
            df['action_type'] = []
            return df
        
        # Generate stable IDs based on core candidate data
        stable_ids = []
        
        for idx, row in df.iterrows():
            try:
                # Create stable ID from core fields (before any cleaning)
                stable_id, _ = self._generate_stable_id(row, state)
                stable_ids.append(stable_id)
                    
            except Exception as e:
                logger.warning(f"Failed to generate ID for row {idx} in {state}: {e}")
                stable_ids.append(None)
        
        # Add stable_id column
        df['stable_id'] = stable_ids
        
        # Add placeholder columns (will be populated in Phase 5)
        df['first_added_date'] = None
        df['last_updated_date'] = None
        df['action_type'] = 'INSERT'  # Default, will be updated in Phase 5
        
        return df
    
    def _generate_stable_id(self, row: pd.Series, state: str) -> Tuple[str, datetime]:
        """Generate a stable ID for a candidate record"""
        # Use core fields for ID generation (before any cleaning/standardization)
        # Structural cleaners output 'candidate_name' column
        candidate_name = str(row.get('candidate_name', '')).strip()
        office = str(row.get('office', '')).strip()
        election_year = str(row.get('election_year', '')).strip()
        
        # Validate that we have the minimum required data
        if not candidate_name or candidate_name.lower() in ['nan', 'none', 'null', '']:
            raise ValueError(f"Invalid candidate_name: '{candidate_name}'")
        
        if not office or office.lower() in ['nan', 'none', 'null', '']:
            raise ValueError(f"Invalid office: '{office}'")
            
        if not election_year or election_year.lower() in ['nan', 'none', 'null', '']:
            raise ValueError(f"Invalid election_year: '{election_year}'")
        
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
    
    def _detect_and_update_changes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect data changes using data_hash for fast comparison
        
        Args:
            df: DataFrame with stable_ids and existing data
            
        Returns:
            DataFrame with updated action_type and last_updated_date
        """
        logger.info("Detecting data changes using data_hash for fast comparison...")
        
        if df.empty or 'stable_id' not in df.columns:
            logger.warning("No stable_id column found, skipping change detection")
            return df
        
        # 1. Generate data_hash for current pipeline data (ALL columns automatically)
        logger.info("Generating data_hash for pipeline records...")
        df['data_hash'] = df.apply(self._generate_row_hash, axis=1)
        
        # 2. Get existing data from DB (with their data_hash)
        existing_data = self._get_existing_production_data_with_hash()
        
        if existing_data.empty:
            logger.info("No existing production data found - all records will be INSERT")
            df['action_type'] = 'INSERT'
            return df
        
        logger.info(f"Found {len(existing_data)} existing production records for comparison")
        
        # 3. Merge on stable_id to compare hashes efficiently
        logger.info("Merging pipeline data with existing data for comparison...")
        merged_df = df.merge(
            existing_data[['stable_id', 'data_hash']], 
            on='stable_id', 
            how='left', 
            suffixes=('_pipeline', '_db')
        )
        
        # 4. Set action types based on hash comparison
        logger.info("Setting action types based on hash comparison...")
        merged_df['action_type'] = 'INSERT'  # Default for new records
        
        # For existing records, compare hashes to detect changes
        existing_mask = merged_df['data_hash_db'].notna()
        merged_df.loc[existing_mask, 'action_type'] = (
            merged_df.loc[existing_mask, 'data_hash_pipeline'] != 
            merged_df.loc[existing_mask, 'data_hash_db']
        ).map({True: 'UPDATE', False: 'NO_CHANGE'})
        
        # 5. Update last_updated_date for changed records
        current_time = datetime.now()
        merged_df.loc[merged_df['action_type'] == 'INSERT', 'last_updated_date'] = current_time
        merged_df.loc[merged_df['action_type'] == 'UPDATE', 'last_updated_date'] = current_time
        
        # 6. Log action type summary
        action_counts = merged_df['action_type'].value_counts()
        logger.info(f"Action type summary: {action_counts.to_dict()}")
        
        return merged_df
    
    def _generate_row_hash(self, row):
        """Generate hash from candidate data columns (excluding system metadata)"""
        # Exclude system metadata columns that shouldn't affect data comparison
        exclude_columns = [
            'data_hash',           # The hash itself
            'first_added_date',    # System metadata - changes every run
            'last_updated_date',   # System metadata - changes every run
            'action_type',         # Pipeline processing flag
            'processing_timestamp', # Pipeline metadata
            'pipeline_version',    # Pipeline metadata
            'data_source'          # Pipeline metadata
        ]
        
        # Get only candidate data columns
        columns_to_hash = [col for col in row.index if col not in exclude_columns]
        
        # Get all values and create hash string
        all_values = [str(row[col]) for col in columns_to_hash]
        data_string = '|'.join(all_values)
        
        return hashlib.md5(data_string.encode()).hexdigest()[:16]
    
    def _get_existing_production_data(self) -> pd.DataFrame:
        """Get existing production data for comparison"""
        try:
            if not self.db_manager:
                return pd.DataFrame()
            
            # Get minimal columns needed for comparison
            query = """
            SELECT stable_id, first_added_date, last_updated_date, 
                   election_year, election_type, office, district, full_name_display,
                   first_name, middle_name, last_name, prefix, suffix, nickname,
                   party, phone, email, address, website, state, county, city,
                   zip_code, address_state, filing_date, election_date, facebook, twitter
            FROM filings
            """
            
            result = self.db_manager.execute_query(query)
            logger.info(f"Retrieved {len(result)} existing production records for comparison")
            return result
            
        except Exception as e:
            logger.error(f"Failed to get existing production data: {e}")
            return pd.DataFrame()
    
    def _get_existing_production_data_with_hash(self) -> pd.DataFrame:
        """Get existing production data with data_hash for comparison"""
        try:
            if not self.db_manager:
                return pd.DataFrame()
            
            # Check if data_hash column exists in the filings table
            check_hash_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'filings' AND column_name = 'data_hash'
            """
            hash_check = self.db_manager.execute_query(check_hash_query)
            
            if hash_check.empty:
                logger.info("data_hash column doesn't exist in filings table - creating it...")
                # Create data_hash column and populate it
                self._create_and_populate_data_hash_column()
            
            # Get minimal columns needed for comparison (including data_hash)
            query = """
            SELECT stable_id, data_hash
            FROM filings
            WHERE stable_id IS NOT NULL
            """
            
            result = self.db_manager.execute_query(query)
            logger.info(f"Retrieved {len(result)} existing production records with data_hash for comparison")
            return result
            
        except Exception as e:
            logger.error(f"Failed to get existing production data with hash: {e}")
            return pd.DataFrame()
    
    def _create_and_populate_data_hash_column(self):
        """Create data_hash column and populate it for existing records"""
        try:
            logger.info("Creating data_hash column in filings table...")
            
            # Add data_hash column
            add_column_query = """
            ALTER TABLE filings 
            ADD COLUMN IF NOT EXISTS data_hash VARCHAR(32)
            """
            self.db_manager.execute_query(add_column_query)
            
            # Update existing records with data_hash
            logger.info("Populating data_hash for existing records...")
            update_query = """
            UPDATE filings 
            SET data_hash = MD5(
                CONCAT_WS('|',
                    COALESCE(election_year::text, ''),
                    COALESCE(election_type, ''),
                    COALESCE(office, ''),
                    COALESCE(district, ''),
                    COALESCE(full_name_display, ''),
                    COALESCE(first_name, ''),
                    COALESCE(middle_name, ''),
                    COALESCE(last_name, ''),
                    COALESCE(prefix, ''),
                    COALESCE(suffix, ''),
                    COALESCE(nickname, ''),
                    COALESCE(party, ''),
                    COALESCE(phone, ''),
                    COALESCE(email, ''),
                    COALESCE(address, ''),
                    COALESCE(website, ''),
                    COALESCE(state, ''),
                    COALESCE(county, ''),
                    COALESCE(city, ''),
                    COALESCE(zip_code, ''),
                    COALESCE(address_state, ''),
                    COALESCE(filing_date::text, ''),
                    COALESCE(election_date::text, ''),
                    COALESCE(facebook, ''),
                    COALESCE(twitter, '')
                    -- EXCLUDED: first_added_date, last_updated_date (system metadata)
                )
            )::text
            WHERE data_hash IS NULL
            """
            self.db_manager.execute_query(update_query)
            
            logger.info("✅ data_hash column created and populated successfully")
            
        except Exception as e:
            logger.error(f"Failed to create and populate data_hash column: {e}")
    
    def _has_data_changed(self, new_record: pd.Series, existing_record: pd.Series) -> bool:
        """
        Compare new record with existing record to detect changes
        
        Args:
            new_record: New data from pipeline
            existing_record: Existing data from production
            
        Returns:
            True if data has changed, False otherwise
        """
        # Columns to compare (excluding metadata columns)
        compare_columns = [
            'election_year', 'election_type', 'office', 'district', 'full_name_display',
            'first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname',
            'party', 'phone', 'email', 'address', 'website', 'state', 'county', 'city',
            'zip_code', 'address_state', 'filing_date', 'election_date', 'facebook', 'twitter'
        ]
        
        for col in compare_columns:
            if col in new_record and col in existing_record:
                new_val = new_record[col]
                existing_val = existing_record[col]
                
                # Handle NaN values
                if pd.isna(new_val) and pd.isna(existing_val):
                    continue
                if pd.isna(new_val) or pd.isna(existing_val):
                    return True
                
                # Compare values
                if str(new_val).strip() != str(existing_val).strip():
                    return True
        
        return False
    
    def _run_state_cleaners(self, all_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Phase 3: Clean and standardize data within each state"""
        cleaned_data = {}
        
        for state, df in all_data.items():
            logger.info(f"Processing state cleaner for {state}, input type: {type(df)}")
            if state in self.state_cleaners and self.state_cleaners[state] is not None:
                try:
                    logger.info(f"Running data cleaner for {state}")
                    cleaner = self.state_cleaners[state]
                    # Call the clean_data method from BaseStateCleaner
                    if hasattr(cleaner, 'clean_data'):
                        cleaned_df = cleaner.clean_data(df)
                    else:
                        # Fallback to alternative methods if clean_data doesn't exist
                        method_name = f"clean_{state}_data"
                        if hasattr(cleaner, method_name):
                            cleaned_df = getattr(cleaner, method_name)(df)
                        else:
                            # Try other alternative method names
                            alt_methods = [
                                f"clean_{state}_candidates",
                                "clean",
                                "process"
                            ]
                            method_found = False
                            for alt_method in alt_methods:
                                if hasattr(cleaner, alt_method):
                                    cleaned_df = getattr(cleaner, alt_method)(df)
                                    method_found = True
                                    break
                            if not method_found:
                                logger.warning(f"No cleaning method found for {state}, using original data")
                                cleaned_df = df
                    logger.info(f"Cleaner returned type: {type(cleaned_df)}")
                    cleaned_data[state] = cleaned_df
                    
                    # Save cleaner output to processed directory
                    if not cleaned_df.empty:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        output_file = os.path.join(self.processed_dir, f"{state}_cleaned_{timestamp}.xlsx")
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
                df['state'] = self._format_state_name(state)  # Ensure state column exists
                all_records.append(df)
            
            if not all_records:
                logger.warning("No data to process")
                return pd.DataFrame()
            
            # Ensure unique columns across all frames before concatenation
            deduped_records = []
            for df in all_records:
                if df is not None and hasattr(df, 'columns'):
                    df = df.loc[:, ~df.columns.duplicated()]
                    deduped_records.append(df)
            combined_df = pd.concat(deduped_records, ignore_index=True)
            logger.info(f"Combined data: {len(combined_df)} records")
            
            # Apply national standards using the dedicated module
            logger.info("Applying national standards...")
            try:
                combined_df = self.national_standards.apply_standards(combined_df)
                logger.info("National standards applied successfully")
            except Exception as e:
                logger.error(f"National standards application failed: {e}")
                # Fall back to basic standards if the module fails
                combined_df = self._standardize_parties(combined_df)
            
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
        
        # Ensure core columns exist
        core_columns = [
            'stable_id', 'full_name_display', 'state', 'office', 'election_year',
            'address', 'city', 'zip_code', 'address_state', 'raw_data'
        ]
        for col in core_columns:
            if col not in df.columns:
                df[col] = None
        
        # Prefer full_name_display and drop candidate_name in final output
        if 'full_name_display' not in df.columns or df['full_name_display'].isna().all():
            if 'candidate_name' in df.columns:
                df['full_name_display'] = df['candidate_name']
        else:
            if 'candidate_name' in df.columns:
                # If full_name_display is null but candidate_name has value, fill forward
                df['full_name_display'] = df['full_name_display'].fillna(df['candidate_name'])

        # Normalize raw_data to JSON strings or None
        try:
            if 'raw_data' not in df.columns:
                df['raw_data'] = None
            else:
                def _to_json_safe(value):
                    if value is None or (isinstance(value, float) and pd.isna(value)):
                        return None
                    if isinstance(value, str):
                        return value
                    try:
                        return json.dumps(value, default=str)
                    except Exception:
                        return None
                df['raw_data'] = df['raw_data'].apply(_to_json_safe)
        except Exception as e:
            logger.warning(f"raw_data normalization failed: {e}")

        # Use new modular address parser for comprehensive address parsing
        try:
            from .address_parser import UnifiedAddressParser
            
            # Initialize the address parser
            address_parser = UnifiedAddressParser()
            
            # Parse addresses using the new module
            df = address_parser.parse_dataframe_addresses(df)
            
            # Normalize address states to USPS codes
            df = address_parser.normalize_address_states(df)
            
            # Backfill missing states from main state column
            df = address_parser.backfill_missing_states(df)
            
            logger.info("Address parsing completed using new modular parser")
            
        except Exception as e:
            logger.warning(f"New address parser failed: {e}")
            # Fallback to basic ZIP extraction
            try:
                import re
                zip_pattern = re.compile(r'\b\d{5}(?:-\d{4})?\b')
                def _extract_zip_fallback(addr, existing_zip):
                    if pd.notna(existing_zip) and str(existing_zip).strip() != "":
                        return existing_zip, addr
                    if isinstance(addr, str):
                        m = zip_pattern.search(addr)
                        if m:
                            zipc = m.group(0)
                            street = zip_pattern.sub("", addr).strip().rstrip(',')
                            return zipc, street
                    return existing_zip, addr
                zips_streets = df.apply(lambda r: _extract_zip_fallback(r.get('address'), r.get('zip_code')), axis=1, result_type='expand')
                df['zip_code'] = zips_streets[0]
                df['address'] = zips_streets[1]
                logger.info("Fallback ZIP extraction completed")
            except Exception as e2:
                logger.warning(f"Fallback ZIP extraction also failed: {e2}")

        # State normalization and backfill now handled by AddressParser module

        # Initialize nickname column if it doesn't exist (state cleaners don't create this)
        try:
            if 'nickname' not in df.columns:
                df['nickname'] = None
                logger.info("Initialized missing nickname column")
        except Exception as e:
            logger.warning(f"Error initializing nickname column: {e}")
        
        # Convert name columns from ALL CAPS to proper case
        try:
            logger.info("Converting name columns to proper case...")
            
            name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix']
            for col in name_columns:
                if col in df.columns:
                    # Convert to proper case (first letter capitalized, rest lowercase)
                    # Handle None values properly to avoid converting to "None" string
                    df[col] = df[col].apply(lambda x: x.title() if pd.notna(x) and isinstance(x, str) else x)
                    logger.info(f"Converted {col} to proper case")
            
            logger.info("Name case conversion completed successfully")
            
        except Exception as e:
            logger.warning(f"Error during name case conversion: {e}")
        
        # Convert email addresses to lowercase
        try:
            logger.info("Converting email addresses to lowercase...")
            
            if 'email' in df.columns:
                # Convert emails to lowercase and handle None values properly
                df['email'] = df['email'].apply(lambda x: x.lower() if pd.notna(x) and isinstance(x, str) else x)
                logger.info("Converted email addresses to lowercase")
            
            logger.info("Email case conversion completed successfully")
            
        except Exception as e:
            logger.warning(f"Error during email case conversion: {e}")
        
        # Clean address formatting (fix float street numbers, etc.)
        try:
            logger.info("Cleaning address formatting...")
            
            if 'address' in df.columns:
                # Fix float street numbers (e.g., "123.0 Main St" -> "123 Main St")
                # Handle None values properly to avoid converting to "None" string
                df['address'] = df['address'].apply(lambda x: 
                    re.sub(r'^(\d+)\.0\b', r'\1', str(x)) if pd.notna(x) else x
                )
                
                # Fix other common address formatting issues
                df['address'] = df['address'].apply(lambda x: 
                    re.sub(r'\s+', ' ', str(x).strip()) if pd.notna(x) else x
                )
                
                logger.info("Cleaned address formatting")
            
            logger.info("Address cleaning completed successfully")
            
        except Exception as e:
            logger.warning(f"Error during address cleaning: {e}")

        # Backfill missing stable_id deterministically from core fields
        try:
            def _gen_stable(row):
                if isinstance(row.get('stable_id'), str) and row.get('stable_id').strip():
                    return row.get('stable_id')
                key_parts = [
                    str(row.get('full_name_display') or ''),
                    str(row.get('state') or ''),
                    str(row.get('election_year') or ''),
                    str(row.get('office') or '')
                ]
                key = '|'.join(key_parts)
                return hashlib.md5(key.encode()).hexdigest()[:12]
            df['stable_id'] = df.apply(_gen_stable, axis=1)
        except Exception as e:
            logger.warning(f"stable_id backfill failed: {e}")

        # Sort by state, office, full_name_display
        df = df.sort_values(['state', 'office', 'full_name_display']).reset_index(drop=True)

        # Drop candidate_name from final output if present
        if 'candidate_name' in df.columns:
            try:
                df = df.drop(columns=['candidate_name'])
            except Exception:
                pass
        

        
        # Final logging
        logger.info(f"Final dataset: {len(df)} records")
        logger.info(f"States represented: {df['state'].nunique()}")
        logger.info(f"Offices represented: {df['office'].nunique()}")
        
        return df
    

    
    def _cleanup_old_files(self):
        """Clean up old processed files, keeping only the latest version per state."""
        logger.info("Cleaning up old processed files...")
        
        # Remove ALL old files first
        all_old_files = list(Path(self.processed_dir).glob("*_cleaned_*.xlsx"))
        all_old_files.extend(list(Path(self.processed_dir).glob("*_cleaned_*.csv")))
        
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
    
    def _clear_structured_directory(self):
        """Clear all files from the structured directory before processing."""
        logger.info("Clearing structured directory before processing...")
        
        if not os.path.exists(self.structured_dir):
            logger.info("Structured directory does not exist, creating it...")
            Path(self.structured_dir).mkdir(parents=True, exist_ok=True)
            return
        
        # Remove all files from structured directory
        files_removed = 0
        for file_path in Path(self.structured_dir).glob("*"):
            if file_path.is_file():
                try:
                    file_path.unlink()
                    files_removed += 1
                    logger.info(f"Removed old structured file: {file_path.name}")
                except Exception as e:
                    logger.warning(f"Could not remove old structured file {file_path.name}: {e}")
        
        logger.info(f"Structured directory cleared. Removed {files_removed} old files.")
    
    def _format_state_name(self, state: str) -> str:
        """
        Format state name from underscore format to proper display format.
        
        Args:
            state: State name in underscore format (e.g., 'south_dakota')
            
        Returns:
            str: Properly formatted state name (e.g., 'South Dakota')
        """
        # State name mapping for proper formatting
        # Complete mapping for all 50 US states
        state_mapping = {
            # Multi-word states
            'new_york': 'New York',
            'north_carolina': 'North Carolina', 
            'south_carolina': 'South Carolina',
            'south_dakota': 'South Dakota',
            'north_dakota': 'North Dakota',
            'west_virginia': 'West Virginia',
            'new_mexico': 'New Mexico',
            'new_jersey': 'New Jersey',
            'new_hampshire': 'New Hampshire',
            'rhode_island': 'Rhode Island',
            'south_dakota': 'South Dakota',
            # Single-word states (for consistency and future-proofing)
            'alabama': 'Alabama',
            'alaska': 'Alaska',
            'arizona': 'Arizona',
            'arkansas': 'Arkansas',
            'california': 'California',
            'colorado': 'Colorado',
            'connecticut': 'Connecticut',
            'delaware': 'Delaware',
            'florida': 'Florida',
            'georgia': 'Georgia',
            'hawaii': 'Hawaii',
            'idaho': 'Idaho',
            'illinois': 'Illinois',
            'indiana': 'Indiana',
            'iowa': 'Iowa',
            'kansas': 'Kansas',
            'kentucky': 'Kentucky',
            'louisiana': 'Louisiana',
            'maine': 'Maine',
            'maryland': 'Maryland',
            'massachusetts': 'Massachusetts',
            'michigan': 'Michigan',
            'minnesota': 'Minnesota',
            'mississippi': 'Mississippi',
            'missouri': 'Missouri',
            'montana': 'Montana',
            'nebraska': 'Nebraska',
            'nevada': 'Nevada',
            'ohio': 'Ohio',
            'oklahoma': 'Oklahoma',
            'oregon': 'Oregon',
            'pennsylvania': 'Pennsylvania',
            'tennessee': 'Tennessee',
            'texas': 'Texas',
            'utah': 'Utah',
            'vermont': 'Vermont',
            'virginia': 'Virginia',
            'washington': 'Washington',
            'wisconsin': 'Wisconsin',
            'wyoming': 'Wyoming'
        }
        
        # Check if we have a specific mapping
        if state in state_mapping:
            return state_mapping[state]
        
        # For single-word states, just use title case
        if '_' not in state:
            return state.title()
        
        # For other multi-word states, replace underscores with spaces and title case
        return state.replace('_', ' ').title()
    
    def _archive_old_final_files(self):
        """Move old final files to OLD folder instead of deleting them."""
        logger.info("Archiving old final files to OLD folder...")
        
        old_final_dir = os.path.join(self.final_dir, "OLD")
        files_moved = 0
        
        # Find all Excel and CSV files in final directory (excluding OLD folder)
        for file_path in Path(self.final_dir).glob("*.xlsx"):
            if "OLD" not in str(file_path):
                try:
                    # Create timestamped filename for OLD folder
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    base_name = file_path.stem
                    new_name = f"{base_name}_archived_{timestamp}.xlsx"
                    new_path = os.path.join(old_final_dir, new_name)
                    
                    # Move file to OLD folder
                    file_path.rename(new_path)
                    files_moved += 1
                    logger.info(f"Moved old final file to OLD folder: {new_name}")
                except Exception as e:
                    logger.warning(f"Could not move old final file {file_path.name}: {e}")
        
        # Also archive CSV files
        for file_path in Path(self.final_dir).glob("*.csv"):
            if "OLD" not in str(file_path):
                try:
                    # Create timestamped filename for OLD folder
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    base_name = file_path.stem
                    new_name = f"{base_name}_archived_{timestamp}.csv"
                    new_path = os.path.join(old_final_dir, new_name)
                    
                    # Move file to OLD folder
                    file_path.rename(new_path)
                    files_moved += 1
                    logger.info(f"Moved old final file to OLD folder: {new_name}")
                except Exception as e:
                    logger.warning(f"Could not move old final file {file_path.name}: {e}")
        
        if files_moved > 0:
            logger.info(f"Archive completed. Moved {files_moved} old final files to OLD folder")
        else:
            logger.info("No old final files to archive")

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
                    # Load the raw data first
                    if raw_file.endswith('.xlsx'):
                        raw_df = pd.read_excel(raw_file)
                    elif raw_file.endswith('.csv'):
                        # Try different encodings for CSV files
                        try:
                            raw_df = pd.read_csv(raw_file, encoding='utf-8')
                        except UnicodeDecodeError:
                            try:
                                raw_df = pd.read_csv(raw_file, encoding='latin-1')
                            except UnicodeDecodeError:
                                raw_df = pd.read_csv(raw_file, encoding='cp1252')
                    elif raw_file.endswith('.xls'):
                        # Handle old Excel format
                        raw_df = pd.read_excel(raw_file, engine='xlrd')
                    else:
                        logger.warning(f"Unsupported file format for {raw_file}")
                        continue
                    
                    # Call the appropriate method for each state
                    method_name = f"clean_{state}_data"
                    if hasattr(cleaner_func, method_name):
                        cleaned_df = getattr(cleaner_func, method_name)(raw_df)
                    else:
                        # Try alternative method names
                        alt_methods = [
                            f"clean_{state}_candidates",
                            "clean",
                            "process"
                        ]
                        method_found = False
                        for alt_method in alt_methods:
                            if hasattr(cleaner_func, alt_method):
                                cleaned_df = getattr(cleaner_func, alt_method)(raw_df)
                                method_found = True
                                break
                        if not method_found:
                            logger.warning(f"No cleaning method found for {state}, using original data")
                            cleaned_df = raw_df
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
                        for csv_path in Path(self.processed_dir).glob(f"{base_name}_cleaned_*.csv"):
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
                        output_file = os.path.join(self.processed_dir, f"{base_name}_cleaned_{timestamp}.xlsx")
                        cleaned_df.to_excel(output_file, index=False)
                        cleaned_files[state] = output_file
                        logger.info(f"✅ {state} cleaned successfully and saved to: {output_file}")
                        # Also remove any CSV that may have been created by the cleaner
                        try:
                            for csv_path in Path(self.processed_dir).glob(f"{base_name}_cleaned_*.csv"):
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
        
        # Party standardization using new modular party standardizer
        try:
            from .party_standardizer import PartyStandardizer
            party_standardizer = PartyStandardizer()
            df = party_standardizer.standardize_parties(df)
            logger.info("Party standardization completed using new modular party standardizer")
        except Exception as e:
            logger.error(f"New party standardizer failed: {e}")
            # Fallback to old method if new one fails
            try:
                df = self._standardize_parties(df)
                logger.info("Fallback to old party standardization method")
            except Exception as e2:
                logger.error(f"Fallback party standardization also failed: {e2}")
                # Continue with original data
        
        # Address parsing and standardization
        try:
            df = self._parse_addresses(df)
        except Exception as e:
            logger.error(f"Address parsing failed: {e}")
            # Continue with original data
        
        # Other national standardizations - this method is for a different phase
        # The _apply_national_standards method expects a dict of state DataFrames
        # but here we have a single combined DataFrame, so we skip it
        logger.info("Skipping _apply_national_standards (designed for different phase)")
        
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
        
        # Log initial party coverage
        initial_coverage = df['party'].notna().sum() if 'party' in df.columns else 0
        logger.info(f"Initial party coverage: {initial_coverage:,} records")
        
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
                
                # Replace the original party column with the standardized version
                df['party'] = df['party_standardized']
                
                # Log improvement statistics
                original_coverage = df['party'].notna().sum()
                improved_coverage = df['party_standardized'].notna().sum()
                logger.info(f"Party coverage improved from {original_coverage:,} to {improved_coverage:,} records")
                logger.info(f"Replaced original party column with standardized version")
                
                # Log final party coverage
                final_coverage = df['party'].notna().sum()
                logger.info(f"Final party coverage after standardization: {final_coverage:,} records")
                
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

    def _add_processing_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add processing metadata to the dataframe."""
        logger.info("Adding processing metadata...")
        
        try:
            # Add processing metadata
            df['processing_timestamp'] = datetime.now()
            df['pipeline_version'] = '1.0'
            df['data_source'] = 'state_filings'
            
            logger.info("Processing metadata added")
        except Exception as e:
            logger.error(f"Processing metadata addition failed: {e}")
            # Add basic metadata even if processing fails
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
            
            # Step 4: Deduplication (load data and dedupe in memory)
            logger.info("=== STEP 4: Deduplication ===")
            try:
                # Load the nationally standardized data
                df = pd.read_excel(nationally_standardized_file)
                if df is None or df.empty:
                    logger.error("Nationally standardized file is empty or could not be loaded")
                    return False
                
                # Remove exact duplicates in memory
                original_count = len(df)
                df_deduped = df.drop_duplicates()
                final_count = len(df_deduped)
                duplicates_removed = original_count - final_count
                
                logger.info(f"Removed {duplicates_removed} duplicate records")
                logger.info("✅ Deduplication completed")
                
                # Save deduplicated data
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                final_file = os.path.join(self.final_dir, f"candidate_filings_{timestamp}.xlsx")
                df_deduped.to_excel(final_file, index=False)
                logger.info(f"✅ Deduplicated data saved to: {final_file}")
                
                pipeline_state['deduplication_completed'] = True
                
            except Exception as e:
                logger.error(f"Deduplication failed: {e}")
                return False
            
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
            
            # Step 6: Upload to staging database (SKIPPED FOR TESTING)
            logger.info("=== STEP 6: Upload to Staging (SKIPPED) ===")
            logger.info("🔄 Database upload step skipped for testing purposes")
            logger.info("📁 Data saved to local files only")
            
            # Mark as completed but skipped
            pipeline_state['staging_upload_completed'] = True
            logger.info("✅ Staging upload step completed (skipped)")
            
            logger.info("📋 Data processing completed without database upload")
            logger.info("🚀 To run with database upload, modify the pipeline configuration")
            
            # Log pipeline completion summary
            logger.info("🎉 Full pipeline completed successfully!")
            logger.info("📊 Pipeline completion summary:")
            for step, completed in pipeline_state.items():
                status = "✅" if completed else "❌"
                step_name = step.replace('_', ' ').title()
                logger.info(f"  {status} {step_name}")
            
            # Archive old final files to OLD folder before cleanup
            try:
                self._archive_old_final_files()
            except Exception as e:
                logger.warning(f"Could not archive old final files: {e}")
                # Don't fail the pipeline for archive issues
            
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

    def _check_existing_candidate(self, stable_id: str) -> Optional[Dict]:
        """
        Check if a candidate already exists in the database
        
        Args:
            stable_id: The stable ID to check
            
        Returns:
            Dict with candidate info if exists, None if not found
        """
        if not stable_id or not self.db_manager:
            return None
            
        try:
            # Check only the filings table (production data)
            query = """
            SELECT stable_id, first_added_date, last_updated_date, created_at
            FROM filings 
            WHERE stable_id = %s 
            LIMIT 1
            """
            
            result = self.db_manager.execute_query(query, {'stable_id': stable_id})
            
            if not result.empty:
                row = result.iloc[0]
                return {
                    'stable_id': row['stable_id'],
                    'first_added_date': row.get('first_added_date', row.get('created_at')),
                    'last_updated_date': row.get('last_updated_date', row.get('updated_at')),
                    'created_at': row.get('created_at')
                }
                
            return None
            
        except Exception as e:
            logger.warning(f"Failed to check existing candidate {stable_id}: {e}")
            return None

    def _make_smart_staging_decision(self, final_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Make intelligent staging decisions based on data quality and changes
        
        Args:
            final_data: Processed data ready for staging
            
        Returns:
            Staging decision and results
        """
        logger.info("Making smart staging decision...")
        
        if final_data.empty:
            logger.warning("No data to stage")
            return {'recommendation': 'no_data'}
        
        try:
            # Get existing production data for comparison
            production_data = pd.DataFrame()
            if self.db_manager and self.db_manager.table_exists('filings'):
                production_data = self.db_manager.execute_query("SELECT * FROM filings")
                logger.info(f"Found {len(production_data)} existing production records")
            else:
                logger.info("No existing production data found (first run)")
            
            # Analyze staging data
            analysis = self.smart_staging_manager.analyze_staging_data(final_data, production_data)
            
            logger.info(f"Quality Score: {analysis['quality_score']:.3f} ({analysis['quality_level'].value})")
            logger.info(f"Changes: {analysis['change_summary']}")
            logger.info(f"Recommendation: {analysis['recommendation']}")
            
            # Execute the recommended strategy
            if analysis['auto_promote']:
                logger.info("🚀 Auto-promoting data to production...")
                results = self.smart_staging_manager.execute_promotion_strategy(final_data, analysis)
                logger.info(f"Auto-promotion results: {results}")
                
                # Update staging table with results metadata
                final_data['promotion_status'] = 'auto_promoted'
                final_data['promotion_timestamp'] = datetime.now()
                final_data['promotion_strategy'] = analysis['recommendation']
                
                # Save to staging for audit trail
                self._save_to_staging(final_data, 'auto_promoted')
                
                return {
                    'recommendation': analysis['recommendation'],
                    'auto_promoted': True,
                    'results': results,
                    'quality_score': analysis['quality_score'],
                    'changes': analysis['change_summary']
                }
            else:
                logger.info("📋 Manual review required - saving to staging...")
                
                # Prepare for manual review
                results = self.smart_staging_manager.execute_promotion_strategy(final_data, analysis)
                
                # Save to staging with review metadata
                final_data['promotion_status'] = 'pending_review'
                final_data['review_timestamp'] = datetime.now()
                final_data['review_reason'] = analysis['reason']
                final_data['quality_score'] = analysis['quality_score']
                
                self._save_to_staging(final_data, 'pending_review')
                
                return {
                    'recommendation': analysis['recommendation'],
                    'auto_promoted': False,
                    'manual_review_needed': True,
                    'reason': analysis['reason'],
                    'quality_score': analysis['quality_score'],
                    'changes': analysis['change_summary']
                }
                
        except Exception as e:
            logger.error(f"Failed to make smart staging decision: {e}")
            # Fallback: save to staging for manual review
            final_data['promotion_status'] = 'error_fallback'
            final_data['error_timestamp'] = datetime.now()
            final_data['error_message'] = str(e)
            
            self._save_to_staging(final_data, 'error_fallback')
            
            return {
                'recommendation': 'manual_review_error',
                'auto_promoted': False,
                'manual_review_needed': True,
                'error': str(e)
            }
    
    def recreate_staging_table(self) -> bool:
        """Recreate the staging table with the updated schema."""
        try:
            if not self.db_manager:
                logger.warning("No database connection for staging")
                return False
            
            logger.info("🔄 Recreating staging table with updated schema...")
            success = self.db_manager.recreate_staging_table()
            
            if success:
                logger.info("✅ Staging table recreated successfully")
            else:
                logger.error("❌ Failed to recreate staging table")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to recreate staging table: {e}")
            return False
    
    def _save_to_staging(self, data: pd.DataFrame, status: str):
        """Save data to staging table with metadata"""
        try:
            if not self.db_manager:
                logger.warning("No database connection for staging")
                return
            
            # Define staging table columns (exact match to filings table + staging action columns)
            staging_columns = [
                'stable_id', 'election_year', 'election_type', 'office', 'district', 
                'full_name_display', 'first_name', 'middle_name', 'last_name', 'prefix', 
                'suffix', 'nickname', 'party', 'phone', 'email', 'address', 'website', 
                'state', 'county', 'city', 'zip_code', 'address_state', 'filing_date', 
                'election_date', 'facebook', 'twitter', 'processing_timestamp', 
                'pipeline_version', 'data_source', 'first_added_date', 'last_updated_date', 
                'data_hash', 'staging_action', 'staging_timestamp', 'staging_reason', 
                'quality_score', 'promotion_status', 'review_timestamp', 'review_reason', 
                'error_timestamp', 'error_message'
            ]
            
            # Filter data to only include staging table columns
            staging_data = data.copy()
            
            # Add missing columns with None values
            for col in staging_columns:
                if col not in staging_data.columns:
                    staging_data[col] = None
            
            # Keep only staging table columns
            staging_data = staging_data[staging_columns]
            
            # Verify staging table schema before upload
            if not self.db_manager.table_exists('staging_candidates'):
                logger.warning("Staging table doesn't exist - creating it...")
                if not self.db_manager._create_staging_table():
                    logger.error("Failed to create staging table")
                    return
            
            # Set staging action metadata
            staging_data['staging_action'] = status
            staging_data['staging_timestamp'] = datetime.now()
            
            # Set staging reason based on status
            if status == 'auto_promoted':
                staging_data['staging_reason'] = 'Data quality passed thresholds for auto-promotion'
            elif status == 'pending_review':
                staging_data['staging_reason'] = 'Data quality requires manual review'
            elif status == 'error_fallback':
                staging_data['staging_reason'] = 'Pipeline error occurred, manual review required'
            
            # Ensure all staging metadata columns exist with proper values
            if 'quality_score' not in staging_data.columns:
                staging_data['quality_score'] = None
            if 'promotion_status' not in staging_data.columns:
                staging_data['promotion_status'] = status
            if 'review_timestamp' not in staging_data.columns:
                staging_data['review_timestamp'] = None
            if 'review_reason' not in staging_data.columns:
                staging_data['review_reason'] = None
            if 'error_timestamp' not in staging_data.columns:
                staging_data['error_timestamp'] = None
            if 'error_message' not in staging_data.columns:
                staging_data['error_message'] = None
            
            # Save to staging table
            success = self.db_manager.upload_dataframe(
                staging_data, 'staging_candidates', if_exists='append', index=False
            )
            
            if success:
                logger.info(f"✅ Data saved to staging with status: {status}")
            else:
                logger.error("❌ Failed to save data to staging")
                
        except Exception as e:
            logger.error(f"Failed to save to staging: {e}")

def main():
    """Main function to run the pipeline with default configuration."""
    from .pipeline_config import PipelineConfig
    
    # Create default configuration (full pipeline enabled)
    config = PipelineConfig()
    
    # Initialize pipeline with configuration
    pipeline = MainPipeline(config=config)
    
    # Log pipeline status
    print("🚀 Starting CandidateFilings Pipeline...")
    print("Configuration:")
    print(str(config))
    
    # Run the pipeline
    try:
        final_data = pipeline.run_pipeline()
        
        if final_data is not None and not final_data.empty:
            print(f"\n✅ Pipeline completed successfully!")
            print(f"📊 Final dataset: {len(final_data)} records")
            print(f"🌍 States represented: {final_data['state'].nunique()}")
            print(f"🏛️ Offices represented: {final_data['office'].nunique()}")
            print("Check the logs and output files for details.")
        else:
            print("\n⚠️ Pipeline completed but no data was processed!")
            print("Check the logs for details.")
            
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        print("Check the logs for error details.")
        raise

if __name__ == "__main__":
    main()
