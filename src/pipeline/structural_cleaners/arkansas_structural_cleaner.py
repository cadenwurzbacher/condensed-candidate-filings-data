import pandas as pd
import logging
import os
from pathlib import Path
from .base_structural_cleaner import BaseStructuralCleaner
import re

logger = logging.getLogger(__name__)

class ArkansasStructuralCleaner(BaseStructuralCleaner):
    """
    Arkansas Structural Cleaner - Phase 1 of new pipeline
    
    Focus: Extract structured data from messy raw files
    NO data transformation - just structural parsing
    Output: Clean DataFrame with consistent columns
    """
    
    def clean(self) -> pd.DataFrame:
        """
        Extract structured data from Arkansas raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting Arkansas structural cleaning")
        
        # Find Arkansas raw files
        arkansas_files = self._find_arkansas_files()
        if not arkansas_files:
            logger.warning("No Arkansas raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in arkansas_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from Arkansas files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"Arkansas structural cleaning complete: {len(df)} records")
        return df
    
    def _find_arkansas_files(self) -> list:
        """Find all Arkansas raw data files"""
        arkansas_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return arkansas_files
        
        # Look for Arkansas files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'arkansas' in filename:
                    arkansas_files.append(str(file_path))
        
        logger.info(f"Found {len(arkansas_files)} Arkansas files: {arkansas_files}")
        return arkansas_files
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single Arkansas file
        
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
        """Extract data from Excel file"""
        try:
            # Read all sheets
            excel_file = pd.ExcelFile(file_path)
            logger.info(f"Excel file sheets: {excel_file.sheet_names}")
            
            # Find the main data sheet (usually the first one with data)
            main_sheet = self._find_main_data_sheet(excel_file)
            if not main_sheet:
                logger.warning(f"No suitable data sheet found in {file_path}")
                return []
            
            # Read the main sheet
            df = pd.read_excel(file_path, sheet_name=main_sheet)
            logger.info(f"Read sheet '{main_sheet}' with {len(df)} rows and {len(df.columns)} columns")
            
            # Extract structured data
            return self._extract_structured_data(df)
            
        except Exception as e:
            logger.error(f"Failed to read Excel file {file_path}: {e}")
            return []
    
    def _extract_from_csv(self, file_path: str) -> list:
        """Extract data from CSV file"""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Read CSV file with {len(df)} rows and {len(df.columns)} columns")
            return self._extract_structured_data(df)
        except Exception as e:
            logger.error(f"Failed to read CSV file {file_path}: {e}")
            return []
    
    def _find_main_data_sheet(self, excel_file: pd.ExcelFile) -> str:
        """Find the sheet containing the main candidate data"""
        for sheet_name in excel_file.sheet_names:
            try:
                df = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=5)
                if self._looks_like_candidate_data(df):
                    return sheet_name
            except Exception:
                continue
        return None
    
    def _extract_structured_data(self, df: pd.DataFrame) -> list:
        """Extract structured records from DataFrame"""
        # Clean the DataFrame structure
        df = self._clean_dataframe_structure(df)
        
        if df.empty:
            return []
        
        # Extract records
        records = []
        for idx, row in df.iterrows():
            if self._is_valid_candidate_row(row):
                record = self._extract_single_record(row)
                if record:
                    records.append(record)
        
        return records
    
    def _is_valid_candidate_row(self, row: pd.Series) -> bool:
        """Check if a row contains valid candidate data"""
        # Check if we have at least a candidate name or office
        candidate_name = str(row.get('Candidate Name', '')).strip()
        office = str(row.get('Position/Office', '')).strip()
        
        return bool(candidate_name and candidate_name != 'nan') or bool(office and office != 'nan')
    
    def _extract_single_record(self, row: pd.Series) -> dict:
        """Extract a single candidate record from a row"""
        try:
            record = {
                'candidate_name': self._extract_candidate_name(row),
                'office': self._extract_office(row),
                'party': self._extract_party(row),
                'county': None,  # Arkansas doesn't have county info
                'district': self._extract_district_from_office(row),
                'address': None,  # Arkansas doesn't have address info
                'city': None,  # Arkansas doesn't have city info
                'state': 'Arkansas',
                'zip_code': None,  # Arkansas doesn't have zip info
                'phone': None,  # Arkansas doesn't have phone info
                'email': None,
                'website': None,  # Arkansas doesn't have email info
                'filing_date': self._extract_filing_date(row),
                'election_year': '2024',  # From filename
                'election_type': 'General',  # Default assumption
                'address_state': 'Arkansas',
                'raw_data': str(row.to_dict())  # Store original row data
            }
            
            return record
            
        except Exception as e:
            logger.warning(f"Failed to extract record from row: {e}")
            return None
    
    def _extract_candidate_name(self, row: pd.Series) -> str:
        """Extract candidate name from row"""
        name = str(row.get('Candidate Name', '')).strip()
        if name and name != 'nan':
            return name
        return None
    
    def _extract_office(self, row: pd.Series) -> str:
        """Extract office from row"""
        office = str(row.get('Position/Office', '')).strip()
        if office and office != 'nan':
            return office
        return None
    
    def _extract_party(self, row: pd.Series) -> str:
        """Extract party from row"""
        party = str(row.get('Party Affiliation', '')).strip()
        if party and party != 'nan':
            return party
        return None
    
    def _extract_district_from_office(self, row: pd.Series) -> str:
        """Extract district number from office field if present"""
        office = str(row.get('Position/Office', '')).strip()
        if office and office != 'nan':
            # Look for "District XX" pattern
            district_match = re.search(r'District\s+(\d+)', office, re.IGNORECASE)
            if district_match:
                return district_match.group(1)
        return None
    
    def _extract_filing_date(self, row: pd.Series) -> str:
        """Extract filing date from row"""
        filing_date = row.get('Filing Date')
        if pd.notna(filing_date):
            # Convert to string format
            if hasattr(filing_date, 'strftime'):
                return filing_date.strftime('%Y-%m-%d')
            else:
                return str(filing_date)
        return None
    
