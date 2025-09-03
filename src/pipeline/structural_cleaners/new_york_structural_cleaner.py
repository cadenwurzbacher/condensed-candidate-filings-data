import pandas as pd
import logging
import os
from pathlib import Path
import re

logger = logging.getLogger(__name__)

class NewYorkStructuralCleaner:
    """
    New York Structural Cleaner - Phase 1 of new pipeline
    
    Focus: Extract structured data from messy raw files
    NO data transformation - just structural parsing
    Output: Clean DataFrame with consistent columns
    """
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.raw_dir = os.path.join(data_dir, "raw")
        self.structured_dir = os.path.join(data_dir, "structured")
        
    def clean(self) -> pd.DataFrame:
        """
        Extract structured data from New York raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting New York structural cleaning")
        
        # Find New York raw files
        new_york_files = self._find_new_york_files()
        if not new_york_files:
            logger.warning("No New York raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in new_york_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from New York files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"New York structural cleaning complete: {len(df)} records")
        return df
    
    def _find_new_york_files(self) -> list:
        """Find all New York raw data files"""
        new_york_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return new_york_files
        
        # Look for New York files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'new_york' in filename or 'new_york' in filename.replace('_', ''):
                    new_york_files.append(str(file_path))
        
        logger.info(f"Found {len(new_york_files)} New York files: {new_york_files}")
        return new_york_files
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single New York file
        
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
    
    def _looks_like_candidate_data(self, df: pd.DataFrame) -> bool:
        """Check if a DataFrame sample contains candidate-like columns"""
        if df.empty or len(df.columns) < 3:
            return False
        
        # Check for candidate-like column names
        columns_lower = [col.lower() for col in df.columns]
        candidate_indicators = ['candidate', 'office', 'district', 'election', 'filer']
        
        matches = sum(1 for indicator in candidate_indicators 
                     if any(indicator in col for col in columns_lower))
        
        return matches >= 2
    
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
    
    def _clean_dataframe_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame structure (remove empty rows/columns, reset index)"""
        # Remove completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        # Reset index
        df = df.reset_index(drop=True)
        
        # Clean column names
        df.columns = [str(col).strip() for col in df.columns]
        
        return df
    
    def _is_valid_candidate_row(self, row: pd.Series) -> bool:
        """Check if a row contains valid candidate data"""
        # Check if we have at least a candidate name or office
        candidate_name = str(row.get('Candidate Name', '')).strip()
        candidate_office = str(row.get('Candidate Office', '')).strip()
        office = str(row.get('Office', '')).strip()
        
        return (bool(candidate_name and candidate_name != 'nan') or
                bool(candidate_office and candidate_office != 'nan') or
                bool(office and office != 'nan'))
    
    def _extract_single_record(self, row: pd.Series) -> dict:
        """Extract a single candidate record from a row"""
        try:
            record = {
                'candidate_name': self._extract_candidate_name(row),
                'office': self._extract_office(row),
                'party': self._extract_party(row),
                'county': self._extract_county(row),
                'district': self._extract_district(row),
                'address': self._extract_address(row),
                'city': self._extract_city(row),
                'state': 'New York',
                'zip_code': None,  # New York doesn't have zip code info
                'phone': None,  # New York doesn't have phone info
                'email': None,
                'website': None,  # New York doesn't have email info
                'filing_date': self._extract_filing_date(row),
                'election_year': self._extract_election_year(row),
                'election_type': self._extract_election_type(row),
                'address_state': 'New York',
                'raw_data': str(row.to_dict())  # Store original row data
            }
            
            return record
            
        except Exception as e:
            logger.warning(f"Failed to extract record from row: {e}")
            return None
    
    def _extract_candidate_name(self, row: pd.Series) -> str:
        """Extract candidate name from row"""
        candidate_name = str(row.get('Candidate Name', '')).strip()
        if candidate_name and candidate_name != 'nan':
            return candidate_name
        return None
    
    def _extract_office(self, row: pd.Series) -> str:
        """Extract office from row"""
        # Try candidate office first, then office
        candidate_office = str(row.get('Candidate Office', '')).strip()
        office = str(row.get('Office', '')).strip()
        
        if candidate_office and candidate_office != 'nan':
            return candidate_office
        elif office and office != 'nan':
            return office
        return None
    
    def _extract_party(self, row: pd.Series) -> str:
        """Extract party from row"""
        # New York doesn't have explicit party field, so we'll set to None
        return None
    
    def _extract_county(self, row: pd.Series) -> str:
        """Extract county from row"""
        county = str(row.get('County', '')).strip()
        if county and county != 'nan':
            return county
        return None
    
    def _extract_district(self, row: pd.Series) -> str:
        """Extract district from row"""
        # Try candidate district first, then district
        candidate_district = row.get('Candidate District')
        district = row.get('District')
        
        if pd.notna(candidate_district):
            candidate_district_str = str(candidate_district).strip()
            if candidate_district_str and candidate_district_str != 'nan':
                return candidate_district_str
        
        if pd.notna(district):
            district_str = str(district).strip()
            if district_str and district_str != 'nan':
                return district_str
        
        return None
    
    def _extract_address(self, row: pd.Series) -> str:
        """Extract address from row"""
        address = str(row.get('Address', '')).strip()
        if address and address != 'nan':
            return address
        return None
    
    def _extract_city(self, row: pd.Series) -> str:
        """Extract city from row"""
        municipality = str(row.get('Municipality', '')).strip()
        if municipality and municipality != 'nan':
            return municipality
        return None
    
    def _extract_filing_date(self, row: pd.Series) -> str:
        """Extract filing date from row"""
        # Try candidate registration date first, then registration date
        candidate_reg_date = row.get('Candidate Registration Date')
        reg_date = row.get('Registration Date')
        
        if pd.notna(candidate_reg_date):
            if hasattr(candidate_reg_date, 'strftime'):
                return candidate_reg_date.strftime('%Y-%m-%d')
            else:
                return str(candidate_reg_date)
        
        if pd.notna(reg_date):
            if hasattr(reg_date, 'strftime'):
                return reg_date.strftime('%Y-%m-%d')
            else:
                return str(reg_date)
        
        return None
    
    def _extract_election_year(self, row: pd.Series) -> str:
        """Extract election year from row"""
        election_year = row.get('Election Year')
        if pd.notna(election_year):
            election_year_str = str(election_year).strip()
            if election_year_str and election_year_str != 'nan':
                # Look for year pattern
                year_match = re.search(r'\b(19|20)\d{2}\b', election_year_str)
                if year_match:
                    return year_match.group(0)
        
        # Default to 2024 based on filename
        return '2024'
    
    def _extract_election_type(self, row: pd.Series) -> str:
        """Extract election type from row"""
        # New York typically has primary and general elections
        # Default to 'Primary' as most candidate filings are for primaries
        return 'Primary'
    
    def _ensure_consistent_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all expected columns are present and in correct order"""
        expected_columns = [
            'candidate_name', 'office', 'party', 'county', 'district',
            'address', 'city', 'state', 'zip_code', 'phone', 'email', 'website',
            'facebook', 'twitter', 'filing_date', 'election_year', 'election_type', 'address_state', 'raw_data'
        
        ]
        
        # Add missing columns with None values
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None
        
        # Reorder columns to match expected order
        df = df[expected_columns]
        
        return df
