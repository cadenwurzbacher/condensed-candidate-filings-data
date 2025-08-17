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

# Add current directory to Python path for imports
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

# Import state cleaners (only the ones that exist)
from state_cleaners.alaska_cleaner import clean_alaska_candidates
from state_cleaners.arizona_cleaner import clean_arizona_candidates
from state_cleaners.arkansas_cleaner import clean_arkansas_candidates
from state_cleaners.colorado_cleaner import clean_colorado_candidates
from state_cleaners.delaware_cleaner import clean_delaware_candidates
from state_cleaners.georgia_cleaner import clean_georgia_candidates
from state_cleaners.idaho_cleaner import clean_idaho_candidates
from state_cleaners.illinois_cleaner import clean_illinois_candidates
from state_cleaners.indiana_cleaner import clean_indiana_candidates
from state_cleaners.kansas_cleaner import clean_kansas_candidates
from state_cleaners.kentucky_cleaner import clean_kentucky_candidates
from state_cleaners.louisiana_cleaner import clean_louisiana_candidates
from state_cleaners.missouri_cleaner import clean_missouri_candidates
from state_cleaners.montana_cleaner import clean_montana_candidates
from state_cleaners.nebraska_cleaner import clean_nebraska_candidates
from state_cleaners.new_mexico_cleaner import clean_new_mexico_candidates
from state_cleaners.oklahoma_cleaner import clean_oklahoma_candidates
from state_cleaners.oregon_cleaner import clean_oregon_candidates
from state_cleaners.south_carolina_cleaner import clean_south_carolina_candidates
from state_cleaners.south_dakota_cleaner import clean_south_dakota_candidates
from state_cleaners.vermont_cleaner import clean_vermont_candidates
from state_cleaners.washington_cleaner import clean_washington_candidates
from state_cleaners.west_virginia_cleaner import clean_west_virginia_candidates
from state_cleaners.wyoming_cleaner import clean_wyoming_candidates

# Import processors
from office_standardizer import OfficeStandardizer

# Import database utilities
sys.path.append(str(current_dir.parent.parent / 'src' / 'config'))
from database import get_db_connection

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
        self.db_manager = get_db_connection()
        
        # State cleaner mapping (only the ones that exist)
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
            'kansas': clean_kansas_candidates,
            'kentucky': clean_kentucky_candidates,
            'louisiana': clean_louisiana_candidates,
            'missouri': clean_missouri_candidates,
            'montana': clean_montana_candidates,
            'nebraska': clean_nebraska_candidates,
            'new_mexico': clean_new_mexico_candidates,
            'oklahoma': clean_oklahoma_candidates,
            'oregon': clean_oregon_candidates,
            'south_carolina': clean_south_carolina_candidates,
            'south_dakota': clean_south_dakota_candidates,
            'vermont': clean_vermont_candidates,
            'washington': clean_washington_candidates,
            'west_virginia': clean_west_virginia_candidates,
            'wyoming': clean_wyoming_candidates
        }
        
        # Create directories if they don't exist
        for directory in [self.raw_data_dir, self.processed_dir, self.final_dir, "data/reports"]:
            Path(directory).mkdir(parents=True, exist_ok=True)
        
        # Clean up old processed files before starting
        self._cleanup_old_files()
    
    def _cleanup_old_files(self):
        """Clean up old processed files, keeping only the latest version per state."""
        logger.info("Cleaning up old processed files...")
        
        # Remove ALL old files first
        all_old_files = list(Path(self.processed_dir).glob("*_cleaned_*.xlsx"))
        all_old_files.extend(list(Path(self.processed_dir).glob("*_cleaned_*.csv")))
        
        files_removed = 0
        for old_file in all_old_files:
            old_file.unlink()
            files_removed += 1
            logger.info(f"Removed old file: {old_file.name}")
        
        logger.info(f"Cleanup completed. Removed {files_removed} old files.")
        
        # Also clean up any temporary merged files
        temp_files = list(Path(self.processed_dir).glob("*_merged_temp*"))
        for temp_file in temp_files:
            temp_file.unlink()
            logger.info(f"Removed temporary file: {temp_file.name}")

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
                cleaned_df = cleaner_func(raw_file, output_dir=self.processed_dir)
                
                # Generate proper output filename
                base_name = os.path.splitext(os.path.basename(raw_file))[0]
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = os.path.join(self.processed_dir, f"{base_name}_cleaned_{timestamp}.xlsx")
                
                # Save the cleaned data
                cleaned_df.to_excel(output_file, index=False)
                
                if os.path.exists(output_file):
                    cleaned_files[state] = output_file
                    logger.info(f"✅ {state} cleaned successfully and saved to: {output_file}")
                else:
                    logger.error(f"❌ {state} cleaning failed - file not saved")
                    
            except Exception as e:
                logger.error(f"Error cleaning {state}: {e}")
                continue
        
        logger.info(f"State cleaning completed. {len(cleaned_files)} states processed.")
        
        # Clean up temporary merged files
        self._cleanup_temp_files()
        
        return cleaned_files
    
    def _cleanup_temp_files(self):
        """Clean up temporary merged files."""
        temp_files = list(Path(self.processed_dir).glob("*_merged_temp.xlsx"))
        for temp_file in temp_files:
            temp_file.unlink()
            logger.info(f"Cleaned up temporary file: {temp_file.name}")

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
        merged_df = self._merge_state_files(matching_files, state)
        
        # Save merged file temporarily
        temp_file = os.path.join(self.processed_dir, f"{state}_merged_temp.xlsx")
        merged_df.to_excel(temp_file, index=False)
        logger.info(f"Merged {len(merged_df)} records from {len(matching_files)} files for {state}")
        
        return temp_file
    
    def _merge_state_files(self, file_paths: List[str], state: str) -> pd.DataFrame:
        """Merge multiple raw data files for a state."""
        all_data = []
        
        for file_path in file_paths:
            try:
                # Handle different file types
                if file_path.endswith('.csv'):
                    df = pd.read_csv(file_path)
                elif file_path.endswith('.xls'):
                    df = pd.read_excel(file_path, engine='xlrd')
                else:
                    df = pd.read_excel(file_path)
                
                # Add source file info
                df['_source_file'] = os.path.basename(file_path)
                all_data.append(df)
                logger.info(f"Loaded {len(df)} records from {os.path.basename(file_path)}")
                
            except Exception as e:
                logger.warning(f"Error reading {file_path}: {e}")
                continue
        
        if not all_data:
            logger.error(f"No data could be loaded for {state}")
            return pd.DataFrame()
        
        # Merge all data
        merged_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Successfully merged {len(merged_df)} total records for {state}")
        
        return merged_df

    def run_office_standardization(self, cleaned_files: Dict[str, str]) -> str:
        """Run office standardization on cleaned data."""
        logger.info("Starting office standardization...")
        
        # Combine all cleaned data
        all_data = []
        for state, file_path in cleaned_files.items():
            try:
                df = pd.read_excel(file_path)
                df['source_state'] = state
                all_data.append(df)
                logger.info(f"Loaded {len(df)} records from {state}")
            except Exception as e:
                logger.error(f"Error loading {state} data: {e}")
                continue
        
        if not all_data:
            logger.error("No cleaned data to standardize")
            return None
        
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Standardizing offices for {len(combined_df)} records")
        
        # Standardize office names
        standardized_df = self.office_standardizer.standardize_dataset(combined_df, 'office')
        
        # Save standardized data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.final_dir, f"all_states_office_standardized_{timestamp}.xlsx")
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
            # Ensure party column is string type before using .str methods
            party_series = df['party'].astype(str)
            df['party_standardized'] = party_series.str.lower().map(party_mappings).fillna(df['party'])
            logger.info("Party standardization completed")
        
        return df

    def _parse_addresses(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse and standardize address fields."""
        logger.info("Parsing and standardizing addresses...")
        
        if 'address' in df.columns:
            # Add address parsing columns
            df['address_parsed'] = True
            # Ensure address column is string type before using .str methods
            address_series = df['address'].astype(str)
            df['address_clean'] = address_series.str.strip()
            
            # Extract common address components
            df['has_zip'] = address_series.str.contains(r'\d{5}(?:-\d{4})?', na=False)
            df['has_state'] = address_series.str.contains(r'\b[A-Z]{2}\b', na=False)
            
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
        df.columns = df.columns.astype(str).str.lower().str.replace(' ', '_')
        
        logger.info("National standards applied")
        return df

    def run_data_audit(self) -> Tuple[str, str]:
        """Run comprehensive data audit."""
        logger.info("Starting data audit...")
        
        # Run various audits
        quality_results = self._audit_data_quality()
        consistency_results = self._audit_column_consistency()
        address_audit = self._audit_address_fields()
        separation_audit = self._analyze_address_separation()
        
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
                if len(df) > 1000:
                    df_sample = df.sample(n=1000, random_state=42)
                    logger.info(f"Sampling 1000 records from {len(df)} total for {state}")
                else:
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
                    sample = df[col].dropna().head(2).tolist()
                    column_analysis[col]['sample_values'].extend(sample)
                    
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
                    sample_values = df[col].dropna().head(5).tolist()
                    
                    # Check for common issues (simplified)
                    issues = []
                    if df[col].dtype == 'object':
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
                    sample_df = df[col].dropna().head(100)
                    
                    separator_counts = {}
                    field_counts = []
                    
                    for val in sample_df:
                        if isinstance(val, str):
                            # Count separators
                            for sep in [',', ';', '|', '\t', '\n']:
                                if sep in val:
                                    separator_counts[sep] = separator_counts.get(sep, 0) + 1
                            
                            # Count fields (split by comma for now)
                            fields = [f.strip() for f in val.split(',') if f.strip()]
                            field_counts.append(len(fields))
                    
                    # Calculate statistics
                    avg_fields = sum(field_counts) / len(field_counts) if field_counts else 0
                    max_fields = max(field_counts) if field_counts else 0
                    min_fields = min(field_counts) if field_counts else 0
                    
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
        
        try:
            # Step 1: State cleaning
            logger.info("=== STEP 1: State Cleaning ===")
            cleaned_files = self.run_state_cleaners()
            if not cleaned_files:
                logger.error("State cleaning failed")
                return False
            
            # Step 2: Office standardization
            logger.info("=== STEP 2: Office Standardization ===")
            office_standardized_file = self.run_office_standardization(cleaned_files)
            if not office_standardized_file:
                logger.error("Office standardization failed")
                return False
            
            # Step 3: National standardization
            logger.info("=== STEP 3: National Standardization ===")
            nationally_standardized_file = self.run_national_standardization(office_standardized_file)
            if not nationally_standardized_file:
                logger.error("National standardization failed")
                return False
            
            # Step 4: Deduplication
            logger.info("=== STEP 4: Deduplication ===")
            final_file = self.run_deduplication(nationally_standardized_file)
            if not final_file:
                logger.error("Deduplication failed")
                return False
            
            # Step 5: Data audit
            logger.info("=== STEP 5: Data Audit ===")
            audit_file, audit_status = self.run_data_audit()
            
            # Step 6: Upload to staging database
            logger.info("=== STEP 6: Upload to Staging ===")
            staging_success = self.upload_to_staging(final_file)
            if not staging_success:
                logger.error("Staging upload failed")
                return False
            
            logger.info("✅ Data uploaded to staging successfully!")
            logger.info("📋 Review the data in staging before moving to production")
            logger.info("🚀 To move to production, run: python scripts/move_to_production.py")
            
            logger.info("🎉 Full pipeline completed successfully!")
            
            # Final cleanup - keep only the latest cleaned file per state
            self._final_cleanup()
            
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
                # Sort by modification time (newest first)
                files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                # Remove all but the newest
                for old_file in files[1:]:
                    old_file.unlink()
                    files_removed += 1
                    logger.info(f"Final cleanup: Removed old file: {old_file.name}")
        
        logger.info(f"Final cleanup completed. Removed {files_removed} old files.")

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
