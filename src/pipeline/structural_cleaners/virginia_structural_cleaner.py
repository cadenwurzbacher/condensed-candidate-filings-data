import pandas as pd
import logging
import os
from pathlib import Path
import re

logger = logging.getLogger(__name__)

class VirginiaStructuralCleaner:
    """
    Virginia Structural Cleaner - Phase 1 of new pipeline
    
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
        Extract structured data from Virginia raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting Virginia structural cleaning")
        
        # Find Virginia raw files
        virginia_files = self._find_virginia_files()
        if not virginia_files:
            logger.warning("No Virginia raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in virginia_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from Virginia files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"Virginia structural cleaning complete: {len(df)} records")
        return df
    
    def _find_virginia_files(self) -> list:
        """Find all Virginia raw data files"""
        virginia_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return virginia_files
        
        # Look for Virginia files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'virginia' in filename:
                    virginia_files.append(str(file_path))
        
        logger.info(f"Found {len(virginia_files)} Virginia files: {virginia_files}")
        return virginia_files
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single Virginia file
        
        Args:
            file_path: Path to the raw file
            
        Returns:
            list: List of dictionaries with extracted data
        """
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext in ['.xlsx', '.xls']:
            return self._extract_from_excel(file_path)
        else:
            logger.warning(f"Unsupported file type: {file_ext}")
            return []
    
    def _extract_from_excel(self, file_path: str) -> list:
        """Extract data from Excel file"""
        try:
            # Read the Excel file
            df = pd.read_excel(file_path)
            logger.info(f"Read Excel file with {len(df)} rows and {len(df.columns)} columns")
            
            # Extract structured data
            return self._extract_structured_data(df)
            
        except Exception as e:
            logger.error(f"Failed to read Excel file {file_path}: {e}")
            return []
    
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
        df.columns = [str(col).strip() if pd.notna(col) else f"Unnamed_{i}" for i, col in enumerate(df.columns)]
        
        return df
    
    def _is_valid_candidate_row(self, row: pd.Series) -> bool:
        """Check if a row contains valid candidate data"""
        # Check if we have at least a candidate name or office
        candidate_name = str(row.get('Candidate Name', '')).strip()
        office_title = str(row.get('Office Title', '')).strip()
        
        return (bool(candidate_name and candidate_name != 'nan') or
                bool(office_title and office_title != 'nan'))
    
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
                'state': self._extract_state(row),
                'zip_code': self._extract_zip_code(row),
                'phone': self._extract_phone(row),
                'email': self._extract_email(row),
            'website': self._extract_website(row),
                'facebook': self._extract_facebook(row),
            'twitter': self._extract_twitter(row),
            'filing_date': None,  # Virginia doesn't have filing date
                'election_year': self._extract_election_year(row),
                'election_type': 'Primary',  # Default to Primary
                'address_state': self._extract_state(row),
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
        office_title = str(row.get('Office Title', '')).strip()
        if office_title and office_title != 'nan':
            return office_title
        return None
    
    def _extract_party(self, row: pd.Series) -> str:
        """Extract party from row"""
        political_party = str(row.get('Political Party', '')).strip()
        if political_party and political_party != 'nan':
            return political_party
        return None
    
    def _extract_county(self, row: pd.Series) -> str:
        """Extract county from row"""
        locality_name = str(row.get('Locality Name', '')).strip()
        if locality_name and locality_name != 'nan':
            # Remove "COUNTY" suffix if present
            if locality_name.upper().endswith(' COUNTY'):
                return locality_name[:-7]  # Remove " COUNTY"
            return locality_name
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
        # Try campaign address first, then fall back to regular address
        campaign_address = str(row.get('Campaign Address Line 1', '')).strip()
        if campaign_address and campaign_address != 'nan':
            return campaign_address
        
        address_1 = str(row.get('Address 1', '')).strip()
        if address_1 and address_1 != 'nan':
            return address_1
        
        return None
    
    def _extract_city(self, row: pd.Series) -> str:
        """Extract city from row"""
        city = str(row.get('City', '')).strip()
        if city and city != 'nan':
            return city
        return None
    
    def _extract_state(self, row: pd.Series) -> str:
        """Extract state from row"""
        state = str(row.get('State', '')).strip()
        if state and state != 'nan':
            return state
        return 'Virginia'  # Default to Virginia
    
    def _extract_zip_code(self, row: pd.Series) -> str:
        """Extract zip code from row"""
        zip_code = row.get('Zip')
        if pd.notna(zip_code):
            zip_str = str(zip_code).strip()
            if zip_str and zip_str != 'nan':
                return zip_str
        return None
    
    def _extract_phone(self, row: pd.Series) -> str:
        """Extract phone from row"""
        # Try campaign phone first, then fall back to regular phone
        campaign_phone = str(row.get('Campaign Phone', '')).strip()
        if campaign_phone and campaign_phone != 'nan':
            return campaign_phone
        
        phone = str(row.get('Phone', '')).strip()
        if phone and phone != 'nan':
            return phone
        
        return None
    
    def _extract_email(self, row: pd.Series) -> str:
        """Extract email from row"""
        # Try campaign email first, then fall back to regular email
        campaign_email = str(row.get('Campaign Email', '')).strip()
        if campaign_email and campaign_email != 'nan':
            return campaign_email
        
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
    
    def _extract_election_year(self, row: pd.Series) -> str:
        """Extract election year from row"""
        # Try to extract from filename or default based on current context
        # For now, default to 2024 as most Virginia files are recent
        return '2024'
    
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
