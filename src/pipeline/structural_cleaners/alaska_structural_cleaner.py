import pandas as pd
import logging
import os
from pathlib import Path
from .base_structural_cleaner import BaseStructuralCleaner

logger = logging.getLogger(__name__)

class AlaskaStructuralCleaner(BaseStructuralCleaner):
    """
    Alaska Structural Cleaner - Phase 1 of new pipeline

    Focus: Extract structured data from messy raw files
    NO data transformation - just structural parsing
    Output: Clean DataFrame with consistent columns
    """
        
    def clean(self) -> pd.DataFrame:
        """
        Extract structured data from Alaska raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting Alaska structural cleaning")
        
        # Find Alaska raw files
        alaska_files = self._find_alaska_files()
        if not alaska_files:
            logger.warning("No Alaska raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in alaska_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from Alaska files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"Alaska structural cleaning complete: {len(df)} records")
        return df
    
    def _find_alaska_files(self) -> list:
        """Find all Alaska raw data files"""
        alaska_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return alaska_files
        
        # Look for Alaska files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'alaska' in filename or 'ak' in filename:
                    alaska_files.append(str(file_path))
        
        logger.info(f"Found {len(alaska_files)} Alaska files: {alaska_files}")
        return alaska_files
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single Alaska file
        
        Args:
            file_path: Path to the raw file
            
        Returns:
            list: List of dictionaries with extracted data
        """
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext in ['.xlsx', '.xls']:
            return self._extract_from_excel(file_path)
        elif file_ext == '.csv':
            return self._extract_from_csv(file_path)
        else:
            logger.warning(f"Unsupported file type: {file_ext}")
            return []
    
    def _extract_from_excel(self, file_path: str) -> list:
        """Extract data from Excel files"""
        try:
            # Read all sheets to find the one with candidate data
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            
            logger.info(f"Excel file sheets: {sheet_names}")
            
            # Try to find the main data sheet
            main_sheet = self._find_main_data_sheet(excel_file)
            
            if main_sheet is None:
                logger.warning(f"No main data sheet found in {file_path}")
                return []
            
            # Read the main sheet
            df = pd.read_excel(file_path, sheet_name=main_sheet)
            logger.info(f"Read sheet '{main_sheet}' with {len(df)} rows and {len(df.columns)} columns")
            
            # Extract structured data
            return self._extract_structured_data(df, file_path)
            
        except Exception as e:
            logger.error(f"Failed to read Excel file {file_path}: {e}")
            return []
    
    def _extract_from_csv(self, file_path: str) -> list:
        """Extract data from CSV files"""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Read CSV with {len(df)} rows and {len(df.columns)} columns")
            
            return self._extract_structured_data(df, file_path)
            
        except Exception as e:
            logger.error(f"Failed to read CSV file {file_path}: {e}")
            return []
    
    def _find_main_data_sheet(self, excel_file: pd.ExcelFile) -> str:
        """Find the sheet containing the main candidate data"""
        for sheet_name in excel_file.sheet_names:
            try:
                # Read a small sample to check if it looks like candidate data
                sample_df = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=5)
                
                # Check if this looks like candidate data
                if self._looks_like_candidate_data(sample_df):
                    return sheet_name
                    
            except Exception as e:
                logger.debug(f"Could not read sheet {sheet_name}: {e}")
                continue
        
        return None

    def _extract_structured_data(self, df: pd.DataFrame, file_path: str) -> list:
        """
        Extract structured data from a DataFrame
        
        Args:
            df: Raw DataFrame from file
            file_path: Source file path for metadata
            
        Returns:
            list: List of structured records
        """
        if df.empty:
            return []
        
        # Clean up the DataFrame
        df = self._clean_dataframe_structure(df)
        
        # Extract records
        records = []
        
        for idx, row in df.iterrows():
            try:
                record = self._extract_single_record(row, file_path)
                if record:
                    records.append(record)
            except Exception as e:
                logger.debug(f"Failed to extract record {idx}: {e}")
                continue
        
        return records

    def _extract_single_record(self, row: pd.Series, file_path: str) -> dict:
        """
        Extract a single structured record from a row
        
        Args:
            row: DataFrame row
            file_path: Source file path
            
        Returns:
            dict: Structured record or None if invalid
        """
        # Skip rows that don't look like candidate data
        if not self._is_valid_candidate_row(row):
            return None
        
        # Extract basic fields
        record = {
            'candidate_name': self._extract_candidate_name(row),
            'office': self._extract_office(row),
            'party': self._extract_party(row),
            'county': self._extract_county(row),
            'district': self._extract_district(row),
            'address': self._extract_address(row),
            'city': self._extract_city(row),
            'state': self._extract_state(row),
            'zip_code': self._extract_zip_code(row),
            'phone': self._extract_phone(row),
            'email': self._extract_email(row),
            'website': self._extract_website(row),
            'facebook': self._extract_facebook(row),
            'twitter': self._extract_twitter(row),
            'filing_date': self._extract_filing_date(row),
            'election_year': self._extract_election_year(row, file_path),
            'election_type': self._extract_election_type(row),
            'address_state': self._extract_address_state(row),
            'raw_data': row.to_dict()  # Keep original data for reference
        }
        
        # Only return records with essential data
        if record['candidate_name'] and record['office']:
            return record

        return None

    def _extract_state(self, row: pd.Series) -> str:
        """Extract state from row - Alaska specific override"""
        result = super()._extract_state(row)
        # Default to Alaska for Alaska files
        return result if result else "Alaska"

    def _extract_address_state(self, row: pd.Series) -> str:
        """Extract address state from row - Alaska specific override"""
        result = super()._extract_address_state(row)
        # Default to Alaska for Alaska files
        return result if result else "Alaska"
