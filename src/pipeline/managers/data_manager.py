#!/usr/bin/env python3
"""
Data Manager for CandidateFilings.com Pipeline

This module handles all file operations and data persistence for the pipeline,
including loading raw data, saving intermediate files, and managing output directories.
"""

import pandas as pd
import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import glob

logger = logging.getLogger(__name__)

class DataManager:
    """
    Handles all file operations and data persistence for the pipeline.
    
    This class manages:
    - Loading raw data files into memory
    - Saving intermediate files (structured, cleaned)
    - Saving final output files
    - Managing directory structure
    - File cleanup operations
    """
    
    def __init__(self, config):
        """
        Initialize the data manager.
        
        Args:
            config: PipelineConfig instance
        """
        self.config = config
        self.raw_dir = config.raw_data_dir
        self.structured_dir = config.structured_dir
        self.cleaner_dir = config.cleaner_dir
        self.final_dir = config.final_dir
        self.processed_dir = config.processed_dir
        self.reports_dir = config.reports_dir
        self.logs_dir = config.logs_dir
        
        # Create directories if they don't exist
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create all required directories if they don't exist."""
        directories = [
            self.raw_dir,
            self.structured_dir,
            self.cleaner_dir,
            self.final_dir,
            self.processed_dir,
            self.reports_dir,
            self.logs_dir
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {directory}")
    
    def load_raw_data(self) -> Dict[str, pd.DataFrame]:
        """
        Load raw data files into memory.
        
        Returns:
            Dictionary mapping state names to DataFrames
        """
        logger.info("Loading raw data files into memory...")
        
        raw_data = {}
        available_states = self._get_available_states()
        
        logger.info(f"Found {len(available_states)} states with data files")
        
        for i, state in enumerate(available_states, 1):
            try:
                logger.info(f"[{i}/{len(available_states)}] Processing {state}...")
                state_files = self._find_state_files(state)
                if state_files:
                    logger.info(f"Loading {len(state_files)} files for {state}")
                    state_data = self._load_state_files(state_files, state)
                    if not state_data.empty:
                        raw_data[state] = state_data
                        logger.info(f"✅ {state}: {len(state_data)} records loaded")
                    else:
                        logger.warning(f"⚠️ {state}: No valid data found")
                else:
                    logger.warning(f"⚠️ {state}: No files found")
            except Exception as e:
                logger.error(f"❌ {state}: Failed to load data - {e}")
                if not self.config.continue_on_state_error:
                    raise
                continue
        
        logger.info(f"✅ Loaded raw data for {len(raw_data)} states")
        return raw_data
    
    def _get_available_states(self) -> List[str]:
        """Get list of available states based on raw data files."""
        states = set()
        
        # Check each state individually using the fixed _find_state_files method
        for state in self._get_state_list():
            files = self._find_state_files(state)
            if files:
                states.add(state)
        
        return sorted(list(states))
    
    def _get_state_list(self) -> List[str]:
        """Get list of all supported states."""
        return [
            'alaska', 'arizona', 'arkansas', 'colorado', 'delaware', 'florida',
            'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa', 'kansas',
            'kentucky', 'louisiana', 'maryland', 'massachusetts', 'missouri', 'montana', 'nebraska',
            'new_mexico', 'new_york', 'north_carolina', 'north_dakota', 'oklahoma', 'oregon', 'pennsylvania', 'south_carolina',
            'south_dakota', 'vermont', 'virginia', 'washington', 'west_virginia', 'wyoming'
        ]
    
    def _get_state_abbreviation(self, state: str) -> str:
        """Get state abbreviation."""
        abbreviations = {
            'alaska': 'ak', 'arizona': 'az', 'arkansas': 'ar', 'colorado': 'co',
            'delaware': 'de', 'florida': 'fl', 'georgia': 'ga', 'hawaii': 'hi', 'idaho': 'id',
            'illinois': 'il', 'indiana': 'in', 'iowa': 'ia', 'kansas': 'ks',
            'kentucky': 'ky', 'louisiana': 'la', 'maryland': 'md', 'massachusetts': 'ma',
            'missouri': 'mo', 'montana': 'mt', 'nebraska': 'ne', 'new_mexico': 'nm',
            'new_york': 'ny', 'north_carolina': 'nc', 'north_dakota': 'nd', 'oklahoma': 'ok', 'oregon': 'or',
            'pennsylvania': 'pa', 'south_carolina': 'sc', 'south_dakota': 'sd',
            'vermont': 'vt', 'virginia': 'va', 'washington': 'wa', 'west_virginia': 'wv',
            'wyoming': 'wy'
        }
        return abbreviations.get(state.lower(), state.lower())
    
    def _find_state_files(self, state: str) -> List[str]:
        """Find all files for a specific state."""
        state_lower = state.lower()
        state_abbrev = self._get_state_abbreviation(state)
        
        matching_files = []
        
        # Look for files containing state name or abbreviation
        for ext in ['*.xlsx', '*.csv', '*.xls', '*.txt']:
            pattern = os.path.join(self.raw_dir, ext)
            for file_path in glob.glob(pattern):
                filename = os.path.basename(file_path).lower()
                
                # More precise matching to prevent conflicts like arkansas/kansas
                # Only match if the state name appears as a complete word at the beginning
                if (filename.startswith(f"{state_lower}_") or 
                    filename.startswith(f"{state_abbrev}_")):
                    matching_files.append(file_path)
        
        return sorted(matching_files)
    
    def _load_state_files(self, file_paths: List[str], state: str) -> pd.DataFrame:
        """
        Load and combine multiple files for a state.
        
        Args:
            file_paths: List of file paths to load
            state: State name for logging
            
        Returns:
            Combined DataFrame for the state
        """
        all_data = []
        
        for file_path in file_paths:
            try:
                file_ext = Path(file_path).suffix.lower()
                file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
                
                logger.info(f"Loading {os.path.basename(file_path)} ({file_size:.1f}MB)")
                
                if file_ext in ['.xlsx', '.xls']:
                    # Excel files don't support chunking, load all at once
                    import warnings
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        df = pd.read_excel(file_path)
                elif file_ext == '.csv':
                    # Try different encodings
                    for encoding in ['utf-8', 'latin-1', 'cp1252']:
                        try:
                            if file_size > 10:  # Files larger than 10MB
                                logger.info(f"Large CSV file detected, using chunked reading...")
                                chunks = []
                                for chunk in pd.read_csv(file_path, encoding=encoding, chunksize=10000):
                                    chunks.append(chunk)
                                df = pd.concat(chunks, ignore_index=True)
                            else:
                                df = pd.read_csv(file_path, encoding=encoding)
                            break
                        except UnicodeDecodeError:
                            continue
                    else:
                        logger.error(f"Failed to read {file_path} with any encoding")
                        continue
                elif file_ext == '.txt':
                    # Handle Florida .txt files
                    logger.info(f"Processing Florida .txt file: {os.path.basename(file_path)}")
                    
                    # Try different encodings for Florida files
                    for encoding in ['latin-1', 'cp1252', 'iso-8859-1', 'utf-8']:
                        try:
                            # Florida files are tab-delimited with specific columns
                            df = pd.read_csv(file_path, sep='\t', encoding=encoding)
                            
                            # Map Florida columns to standard format
                            column_mapping = {
                                'NameLast': 'last_name',
                                'NameFirst': 'first_name', 
                                'NameMiddle': 'middle_name',
                                'OfficeDesc': 'office',
                                'PartyDesc': 'party',
                                'Addr1': 'address',
                                'City': 'city',
                                'State': 'address_state',
                                'Zip': 'zip_code',
                                'County': 'county',
                                'Phone': 'phone',
                                'Email': 'email',
                                'ElectionID': 'election_year'
                            }
                            
                            # Rename columns that exist
                            existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
                            df = df.rename(columns=existing_columns)
                            
                            # Create candidate_name from components
                            if 'first_name' in df.columns and 'last_name' in df.columns:
                                df['candidate_name'] = df['first_name'].fillna('') + ' ' + df['last_name'].fillna('')
                                df['candidate_name'] = df['candidate_name'].str.strip()
                            
                            # Extract election year from ElectionID
                            if 'election_year' in df.columns:
                                df['election_year'] = df['election_year'].str[:4]
                            
                            logger.info(f"Successfully processed Florida file with {encoding} encoding: {len(df)} records")
                            break
                            
                        except UnicodeDecodeError:
                            continue
                        except Exception as e:
                            logger.error(f"Failed to read {file_path} with {encoding} encoding: {e}")
                            continue
                    else:
                        logger.error(f"Failed to read {file_path} with any encoding")
                        continue
                else:
                    logger.warning(f"Unsupported file type: {file_ext}")
                    continue
                
                # Add source file metadata
                df['_source_file'] = os.path.basename(file_path)
                df['_source_state'] = state
                
                all_data.append(df)
                logger.info(f"Loaded {len(df)} records from {os.path.basename(file_path)}")
                
            except Exception as e:
                logger.error(f"Failed to load {file_path}: {e}")
                if not self.config.continue_on_state_error:
                    raise
                continue
        
        if not all_data:
            return pd.DataFrame()
        
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Combined {len(combined_df)} total records for {state}")
        
        return combined_df
    
    def save_structured_files(self, data: Dict[str, pd.DataFrame]):
        """
        Save structured data to files (if enabled).
        
        Args:
            data: Dictionary mapping state names to DataFrames
        """
        if not self.config.save_structured_files:
            logger.debug("Skipping structured file save (disabled)")
            return
        
        logger.info("Saving structured files...")
        
        for state, df in data.items():
            if df.empty:
                logger.warning(f"Skipping empty structured data for {state}")
                continue
            
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{state}_structured_{timestamp}.xlsx"
                file_path = os.path.join(self.structured_dir, filename)
                
                df.to_excel(file_path, index=False)
                logger.info(f"Saved structured data for {state}: {len(df)} records")
                
            except Exception as e:
                logger.error(f"Failed to save structured data for {state}: {e}")
                if not self.config.continue_on_state_error:
                    raise
    
    def save_cleaned_files(self, data: Dict[str, pd.DataFrame]):
        """
        Save cleaned data to files (if enabled).
        
        Args:
            data: Dictionary mapping state names to DataFrames
        """
        if not self.config.save_cleaned_files:
            logger.debug("Skipping cleaned file save (disabled)")
            return
        
        logger.info("Saving cleaned files...")
        
        for state, df in data.items():
            if df.empty:
                logger.warning(f"Skipping empty cleaned data for {state}")
                continue
            
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{state}_cleaned_{timestamp}.xlsx"
                file_path = os.path.join(self.processed_dir, filename)
                
                df.to_excel(file_path, index=False)
                logger.info(f"Saved cleaned data for {state}: {len(df)} records")
                
            except Exception as e:
                logger.error(f"Failed to save cleaned data for {state}: {e}")
                if not self.config.continue_on_state_error:
                    raise
    
    def save_final_file(self, data: pd.DataFrame):
        """
        Save final output (if enabled).
        
        Args:
            data: Final DataFrame to save
        """
        if not self.config.save_final_file:
            logger.debug("Skipping final file save (disabled)")
            return
        
        if data.empty:
            logger.warning("Skipping empty final data")
            return
        
        try:
            # Save new final file as CSV for speed
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"candidate_filings_FINAL_{timestamp}.csv"
            file_path = os.path.join(self.final_dir, filename)
            
            # Use CSV for faster processing
            # Ensure phone and ZIP columns are properly formatted as strings
            data_to_save = data.copy()
            
            # Fix phone numbers (remove .0 suffix and ensure string type)
            if 'phone' in data_to_save.columns:
                data_to_save['phone'] = data_to_save['phone'].astype(str)
                data_to_save['phone'] = data_to_save['phone'].apply(
                    lambda x: x.replace('.0', '') if pd.notna(x) and str(x).endswith('.0') else x
                )
                data_to_save['phone'] = data_to_save['phone'].apply(
                    lambda x: '' if pd.isna(x) or str(x) == 'nan' else x
                )
            
            # Fix ZIP codes (remove .0 suffix and ensure string type)
            if 'zip_code' in data_to_save.columns:
                data_to_save['zip_code'] = data_to_save['zip_code'].astype(str)
                data_to_save['zip_code'] = data_to_save['zip_code'].apply(
                    lambda x: x.replace('.0', '') if pd.notna(x) and str(x).endswith('.0') else x
                )
                data_to_save['zip_code'] = data_to_save['zip_code'].apply(
                    lambda x: '' if pd.isna(x) or str(x) == 'nan' else x
                )
            
            data_to_save.to_csv(file_path, index=False)
            logger.info(f"✅ Final output saved as CSV: {len(data)} records")
            logger.info(f"📄 File saved: {file_path}")
                
        except Exception as e:
            logger.error(f"Failed to save final file: {e}")
            if not self.config.continue_on_phase_error:
                raise
    
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
    
    def cleanup_intermediate_files(self):
        """Clean up intermediate files if configured."""
        if not self.config.clear_intermediate_files:
            logger.debug("Skipping intermediate file cleanup (disabled)")
            return
        
        logger.info("Cleaning up intermediate files...")
        
        # Clean up old processed files
        try:
            old_files = list(Path(self.processed_dir).glob("*_cleaned_*.xlsx"))
            old_files.extend(list(Path(self.processed_dir).glob("*_cleaned_*.csv")))
            
            files_removed = 0
            for old_file in old_files:
                try:
                    old_file.unlink()
                    files_removed += 1
                    logger.debug(f"Removed old file: {old_file.name}")
                except Exception as e:
                    logger.warning(f"Could not remove old file {old_file.name}: {e}")
            
            logger.info(f"Cleanup completed. Removed {files_removed} old files.")
            
        except Exception as e:
            logger.warning(f"Cleanup encountered an error: {e}")
    
    def get_pipeline_status(self) -> dict:
        """Get current pipeline status and file counts."""
        status = {
            'raw_files': len(glob.glob(os.path.join(self.raw_dir, "*"))),
            'processed_files': len(list(Path(self.processed_dir).glob("*_cleaned_*.xlsx"))),
            'final_files': len(list(Path(self.final_dir).glob("*.xlsx"))),
            'report_files': len(list(Path(self.reports_dir).glob("*.xlsx"))),
            'log_files': len(list(Path(self.logs_dir).glob("*.log")))
        }
        return status
