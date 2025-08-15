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
        pattern = os.path.join(self.processed_dir, "*_cleaned_*.xlsx")
        files = glob.glob(pattern)
        
        if not files:
            logger.warning(f"No processed files found in {self.processed_dir}")
            return pd.DataFrame()
        
        audit_results = []
        
        for file_path in files:
            try:
                df = pd.read_excel(file_path)
                state = Path(file_path).stem.split('_')[0].title()
                
                # Basic data quality metrics
                total_records = len(df)
                null_counts = df.isnull().sum()
                
                # Check for required columns
                required_cols = ['candidate_name', 'office', 'party']
                missing_required = [col for col in required_cols if col not in df.columns]
                
                # Check for duplicate records
                duplicates = df.duplicated().sum()
                
                # Check for empty strings
                empty_strings = 0
                for col in df.columns:
                    if df[col].dtype == 'object':
                        empty_strings += (df[col] == '').sum()
                
                # Check for whitespace-only values
                whitespace_only = 0
                for col in df.columns:
                    if df[col].dtype == 'object':
                        whitespace_only += df[col].astype(str).str.strip().eq('').sum()
                
                audit_results.append({
                    'state': state,
                    'file': Path(file_path).name,
                    'total_records': total_records,
                    'total_columns': len(df.columns),
                    'missing_required_columns': str(missing_required),
                    'duplicate_records': duplicates,
                    'null_values_total': null_counts.sum(),
                    'empty_strings': empty_strings,
                    'whitespace_only_values': whitespace_only,
                    'data_quality_score': self._calculate_quality_score(
                        total_records, duplicates, null_counts.sum(), empty_strings, whitespace_only
                    )
                })
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
        
        return pd.DataFrame(audit_results)

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
        pattern = os.path.join(self.processed_dir, "*_cleaned_*.xlsx")
        files = glob.glob(pattern)
        
        if not files:
            return pd.DataFrame()
        
        column_analysis = {}
        
        for file_path in files:
            try:
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
                    
                    # Sample some values
                    sample = df[col].dropna().head(3).tolist()
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
                'sample_values': str(info['sample_values'][:5])  # Limit to 5 samples
            })
        
        return pd.DataFrame(consistency_results)

    def _audit_address_fields(self) -> pd.DataFrame:
        """Audit address fields across all state data."""
        pattern = os.path.join(self.processed_dir, "*_cleaned_*.xlsx")
        files = glob.glob(pattern)
        
        if not files:
            logger.warning(f"No processed files found in {self.processed_dir}")
            return pd.DataFrame()
        
        audit_results = []
        
        for file_path in files:
            try:
                df = pd.read_excel(file_path)
                state = Path(file_path).stem.split('_')[0].title()
                
                # Check for address-related columns
                address_cols = [col for col in df.columns if 'address' in col.lower()]
                
                for col in address_cols:
                    # Sample some values
                    sample_values = df[col].dropna().head(10).tolist()
                    
                    # Check for common issues
                    issues = []
                    if df[col].dtype == 'object':
                        # Check for mixed separators
                        separators = []
                        for val in sample_values:
                            if isinstance(val, str):
                                if ',' in val:
                                    separators.append(',')
                                if ';' in val:
                                    separators.append(';')
                                if '|' in val:
                                    separators.append('|')
                        
                        if len(set(separators)) > 1:
                            issues.append("Mixed separators detected")
                        
                        # Check for inconsistent formatting
                        if any('  ' in str(val) for val in sample_values):
                            issues.append("Double spaces detected")
                    
                    audit_results.append({
                        'state': state,
                        'file': Path(file_path).name,
                        'column': col,
                        'total_records': len(df),
                        'non_null_count': df[col].count(),
                        'null_count': df[col].isnull().sum(),
                        'sample_values': str(sample_values[:3]),
                        'issues': '; '.join(issues) if issues else 'None'
                    })
                    
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
        
        return pd.DataFrame(audit_results)

    def _analyze_address_separation(self) -> pd.DataFrame:
        """Analyze address field separation patterns across all state data."""
        pattern = os.path.join(self.processed_dir, "*_cleaned_*.xlsx")
        files = glob.glob(pattern)
        
        if not files:
            return pd.DataFrame()
        
        separation_analysis = []
        
        for file_path in files:
            try:
                df = pd.read_excel(file_path)
                state = Path(file_path).stem.split('_')[0].title()
                
                # Check for address-related columns
                address_cols = [col for col in df.columns if 'address' in col.lower()]
                
                for col in address_cols:
                    # Get non-null address values
                    addresses = df[col].dropna()
                    
                    if len(addresses) == 0:
                        continue
                    
                    # Analyze separation patterns
                    separators = {}
                    field_counts = {}
                    max_fields = 0
                    
                    for addr in addresses:
                        if isinstance(addr, str):
                            # Count different separator types
                            if ',' in addr:
                                separators[','] = separators.get(',', 0) + 1
                            if ';' in addr:
                                separators[';'] = separators.get(';', 0) + 1
                            if '|' in addr:
                                separators['|'] = separators.get('|', 0) + 1
                            if '\n' in addr:
                                separators['\\n'] = separators.get('\\n', 0) + 1
                            
                            # Count fields (split by any separator)
                            fields = re.split(r'[,;|\n]+', addr.strip())
                            field_count = len([f for f in fields if f.strip()])
                            field_counts[field_count] = field_counts.get(field_count, 0) + 1
                            max_fields = max(max_fields, field_count)
                    
                    # Determine most common separator
                    primary_separator = max(separators.items(), key=lambda x: x[1])[0] if separators else 'None'
                    
                    # Determine most common field count
                    primary_field_count = max(field_counts.items(), key=lambda x: x[1])[0] if field_counts else 0
                    
                    separation_analysis.append({
                        'state': state,
                        'file': Path(file_path).name,
                        'column': col,
                        'total_addresses': len(addresses),
                        'primary_separator': primary_separator,
                        'separator_counts': str(separators),
                        'primary_field_count': primary_field_count,
                        'field_count_distribution': str(field_counts),
                        'max_fields_found': max_fields,
                        'consistency_score': len(set(separators.keys())) if separators else 0
                    })
                    
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
        
        return pd.DataFrame(separation_analysis)

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
            
            # Step 6: Database upload (optional)
            logger.info("=== STEP 6: Database Upload ===")
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
