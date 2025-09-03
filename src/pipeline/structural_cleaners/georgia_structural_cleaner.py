import pandas as pd
import logging
import os
from pathlib import Path
import re

logger = logging.getLogger(__name__)

class GeorgiaStructuralCleaner:
    """
    Georgia Structural Cleaner - Phase 1 of new pipeline
    
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
        Extract structured data from Georgia raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting Georgia structural cleaning")
        
        # Find Georgia raw files
        georgia_files = self._find_georgia_files()
        if not georgia_files:
            logger.warning("No Georgia raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in georgia_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from Georgia files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"Georgia structural cleaning complete: {len(df)} records")
        return df
    
    def _find_georgia_files(self) -> list:
        """Find all Georgia raw data files"""
        georgia_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return georgia_files
        
        # Look for Georgia files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'georgia' in filename:
                    georgia_files.append(str(file_path))
        
        logger.info(f"Found {len(georgia_files)} Georgia files: {georgia_files}")
        return georgia_files
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single Georgia file
        
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
        candidate_indicators = ['candidate', 'name', 'office', 'party', 'district', 'election', 'street', 'city']
        
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
        office = str(row.get('Office Name', '')).strip()
        
        return bool(candidate_name and candidate_name != 'nan') or bool(office and office != 'nan')
    
    def _extract_single_record(self, row: pd.Series) -> dict:
        """Extract a single candidate record from a row"""
        try:
            record = {
                'candidate_name': self._extract_candidate_name(row),
                'office': self._extract_office(row),
                'party': self._extract_party(row),
                'county': self._extract_county(row),
                'district': None,  # Georgia doesn't have a distinct district field
                'address': self._extract_address(row),
                'city': self._extract_city(row),
                'state': 'Georgia',
                'zip_code': self._extract_zip_code(row),
                'phone': None,  # Georgia doesn't have phone info
                'email': None,
                'website': None,  # Georgia doesn't have email info
                'filing_date': None,  # Georgia doesn't have filing date info
                'election_year': self._extract_election_year(row),
                'election_type': self._extract_election_type(row),
                'address_state': self._extract_address_state(row),
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
        office = str(row.get('Office Name', '')).strip()
        if office and office != 'nan':
            return office
        return None
    
    def _extract_party(self, row: pd.Series) -> str:
        """Extract party from row"""
        party = str(row.get('Candidate Party', '')).strip()
        if party and party != 'nan':
            return party
        return None
    
    def _extract_county(self, row: pd.Series) -> str:
        """Extract county from row"""
        county = str(row.get('County (If Local Contest)', '')).strip()
        if county and county != 'nan' and county.lower() != 'statewide':
            return county
        return None
    
    def _extract_address(self, row: pd.Series) -> str:
        """Extract full address from row"""
        address_parts = []
        
        # Street Number
        street_number = str(row.get('Street Number', '')).strip()
        if street_number and street_number != 'nan':
            address_parts.append(street_number)
        
        # Street Name
        street_name = str(row.get('Street Name', '')).strip()
        if street_name and street_name != 'nan':
            address_parts.append(street_name)
        
        # Unit/Apt/Suite
        unit = str(row.get('Unit/Apt/Suite', '')).strip()
        if unit and unit != 'nan':
            address_parts.append(unit)
        
        # Address Line 2
        address_line_2 = str(row.get('Address Line 2', '')).strip()
        if address_line_2 and address_line_2 != 'nan':
            address_parts.append(address_line_2)
        
        if address_parts:
            return ' '.join(address_parts)
        return None
    
    def _extract_city(self, row: pd.Series) -> str:
        """Extract city from row"""
        city = str(row.get('City', '')).strip()
        if city and city != 'nan':
            return city
        return None
    
    def _extract_zip_code(self, row: pd.Series) -> str:
        """Extract zip code from row"""
        zip_code = row.get('Zip')
        if pd.notna(zip_code):
            zip_str = str(zip_code).strip()
            if zip_str and zip_str != 'nan':
                return zip_str
        return None
    
    def _extract_address_state(self, row: pd.Series) -> str:
        """Extract address state from row"""
        state = str(row.get('State', '')).strip()
        if state and state != 'nan':
            return state
        return 'Georgia'  # Default to Georgia
    
    def _extract_election_year(self, row: pd.Series) -> str:
        """Extract election year from row"""
        election_date = str(row.get('Election Date Name', '')).strip()
        if election_date and election_date != 'nan':
            # Look for year pattern in election date (e.g., "5/21/2024 - MAY 21, 2024")
            year_match = re.search(r'\b(20\d{2})\b', election_date)
            if year_match:
                return year_match.group(1)
        
        # Default to 2024 based on filename
        return '2024'
    
    def _extract_election_type(self, row: pd.Series) -> str:
        """Extract election type from row"""
        election_date = str(row.get('Election Date Name', '')).strip()
        if election_date and election_date != 'nan':
            election_lower = election_date.lower()
            if 'primary' in election_lower:
                return 'Primary'
            elif 'general' in election_lower:
                return 'General'
            elif 'runoff' in election_lower:
                return 'Runoff'
            elif 'special' in election_lower:
                return 'Special'
            else:
                return 'Unknown'
        return None
    
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
