import pandas as pd
import logging
import os
from pathlib import Path
from .base_structural_cleaner import BaseStructuralCleaner
import re

logger = logging.getLogger(__name__)

class KansasStructuralCleaner(BaseStructuralCleaner):
    """
    Kansas Structural Cleaner - Phase 1 of new pipeline
    
    Focus: Extract structured data from messy raw files
    NO data transformation - just structural parsing
    Output: Clean DataFrame with consistent columns
    """
    
    def clean(self) -> pd.DataFrame:
        """
        Extract structured data from Kansas raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting Kansas structural cleaning")
        
        # Find Kansas raw files
        kansas_files = self._find_kansas_files()
        if not kansas_files:
            logger.warning("No Kansas raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in kansas_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from Kansas files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"Kansas structural cleaning complete: {len(df)} records")
        return df
    
    def _find_kansas_files(self) -> list:
        """Find all Kansas raw data files"""
        kansas_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return kansas_files
        
        # Look for Kansas files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'kansas' in filename:
                    kansas_files.append(str(file_path))
        
        logger.info(f"Found {len(kansas_files)} Kansas files: {kansas_files}")
        return kansas_files
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single Kansas file
        
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
        candidate_name = str(row.get('Candidate', '')).strip()
        office = str(row.get('Office', '')).strip()
        
        return bool(candidate_name and candidate_name != 'nan') or bool(office and office != 'nan')
    
    def _extract_single_record(self, row: pd.Series) -> dict:
        """Extract a single candidate record from a row"""
        try:
            record = {
                'candidate_name': self._extract_candidate_name(row),
                'office': self._extract_office(row),
                'party': self._extract_party(row),
                'county': None,  # Kansas doesn't have county info
                'district': self._extract_district(row),
                'address': self._extract_address(row),
                'city': self._extract_city(row),
                'state': 'Kansas',
                'zip_code': self._extract_zip_code(row),
                'phone': self._extract_phone(row),
                'email': self._extract_email(row),
            'website': self._extract_website(row),
                'facebook': self._extract_facebook(row),
            'twitter': self._extract_twitter(row),
            'filing_date': self._extract_filing_date(row),
                'election_year': self._extract_election_year(row),
                'election_type': self._extract_election_type(row),
                'address_state': 'Kansas',
                'raw_data': str(row.to_dict())  # Store original row data
            }
            
            return record
            
        except Exception as e:
            logger.warning(f"Failed to extract record from row: {e}")
            return None
    
    def _extract_candidate_name(self, row: pd.Series) -> str:
        """Extract candidate name from row"""
        name = str(row.get('Candidate', '')).strip()
        if name and name != 'nan':
            return name
        return None
    
    def _extract_office(self, row: pd.Series) -> str:
        """Extract office from row"""
        office = str(row.get('Office', '')).strip()
        if office and office != 'nan':
            return office
        return None
    
    def _extract_party(self, row: pd.Series) -> str:
        """Extract party from row"""
        party = str(row.get('Party', '')).strip()
        if party and party != 'nan':
            return party
        return None
    
    def _extract_district(self, row: pd.Series) -> str:
        """Extract district from row"""
        district = row.get('District')
        if pd.notna(district):
            district_str = str(district).strip()
            if district_str and district_str != 'nan':
                return district_str
        return None
    
    def _extract_address(self, row: pd.Series) -> str:
        """Extract address from row"""
        address = str(row.get('Home Address', '')).strip()
        if address and address != 'nan':
            return address
        return None
    
    def _extract_city(self, row: pd.Series) -> str:
        """Extract city from row"""
        city = str(row.get('Home City', '')).strip()
        if city and city != 'nan':
            return city
        return None
    
    def _extract_zip_code(self, row: pd.Series) -> str:
        """Extract zip code from row"""
        zip_code = row.get('Home Zip')
        if pd.notna(zip_code):
            zip_str = str(zip_code).strip()
            if zip_str and zip_str != 'nan':
                return zip_str
        return None
    
    def _extract_phone(self, row: pd.Series) -> str:
        """Extract phone from row"""
        # Try home phone first, then cell phone
        home_phone = str(row.get('Home Phone', '')).strip()
        if home_phone and home_phone != 'nan':
            return home_phone
        
        cell_phone = str(row.get('Cell Phone', '')).strip()
        if cell_phone and cell_phone != 'nan':
            return cell_phone
        
        return None
    
    def _extract_email(self, row: pd.Series) -> str:
        """Extract email from row"""
        email = str(row.get('Email', '')).strip()
        if email and email != 'nan':
            return email
        return None
    
    def _extract_website(self, row: pd.Series) -> str:
        """Extract website from row"""
        website_columns = [col for col in row.index if 'website' in str(col).lower()]
        
        for col in website_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        return None
    
    
    def _extract_facebook(self, row: pd.Series) -> str:
        """Extract Facebook from row"""
        facebook_columns = [col for col in row.index if 'facebook' in str(col).lower()]
        
        for col in facebook_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        return None
    
    def _extract_twitter(self, row: pd.Series) -> str:
        """Extract Twitter from row"""
        twitter_columns = [col for col in row.index if 'twitter' in str(col).lower()]
        
        for col in twitter_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        return None
    
    def _extract_filing_date(self, row: pd.Series) -> str:
        """Extract filing date from row"""
        filing_date = row.get('Date Filed')
        if pd.notna(filing_date):
            # Convert to string format
            if hasattr(filing_date, 'strftime'):
                return filing_date.strftime('%Y-%m-%d')
            else:
                return str(filing_date)
        return None
    
    def _extract_election_year(self, row: pd.Series) -> str:
        """Extract election year from row"""
        election = str(row.get('Election', '')).strip()
        if election and election != 'nan':
            # Look for year pattern in election field
            year_match = re.search(r'\b(19|20)\d{2}\b', election)
            if year_match:
                return year_match.group(0)
        
        # Default to 2024 based on filename
        return '2024'
    
    def _extract_election_type(self, row: pd.Series) -> str:
        """Extract election type from row"""
        election = str(row.get('Election', '')).strip()
        if election and election != 'nan':
            election_lower = election.lower()
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
    
