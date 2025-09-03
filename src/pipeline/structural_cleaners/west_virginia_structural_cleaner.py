import pandas as pd
import logging
import os
from pathlib import Path
import re

logger = logging.getLogger(__name__)

class WestVirginiaStructuralCleaner:
    """
    West Virginia Structural Cleaner - Phase 1 of new pipeline
    
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
        Extract structured data from West Virginia raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting West Virginia structural cleaning")
        
        # Find West Virginia raw files
        west_virginia_files = self._find_west_virginia_files()
        if not west_virginia_files:
            logger.warning("No West Virginia raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in west_virginia_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from West Virginia files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"West Virginia structural cleaning complete: {len(df)} records")
        return df
    
    def _find_west_virginia_files(self) -> list:
        """Find all West Virginia raw data files"""
        west_virginia_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return west_virginia_files
        
        # Look for West Virginia files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'west_virginia' in filename:
                    west_virginia_files.append(str(file_path))
        
        logger.info(f"Found {len(west_virginia_files)} West Virginia files: {west_virginia_files}")
        return west_virginia_files
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single West Virginia file
        
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
        
        # Clean column names (remove leading/trailing spaces)
        df.columns = [str(col).strip() if pd.notna(col) else f"Unnamed_{i}" for i, col in enumerate(df.columns)]
        
        return df
    
    def _is_valid_candidate_row(self, row: pd.Series) -> bool:
        """Check if a row contains valid candidate data"""
        # Check candidate-like fields
        race = str(row.get('Race', '')).strip()
        name_fields = [col for col in row.index if 'name' in str(col).lower()]
        has_any_name = False
        for col in name_fields:
            val = str(row.get(col, '')).strip()
            if val and val != 'nan':
                has_any_name = True
                break
        return has_any_name or bool(race and race != 'nan')
    
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
            'filing_date': self._extract_filing_date(row),
                'election_year': self._extract_election_year(row),
                'election_type': self._extract_election_type(row),
                'address_state': self._extract_state(row),
                'raw_data': str(row.to_dict())  # Store original row data
            }
            
            return record
            
        except Exception as e:
            logger.warning(f"Failed to extract record from row: {e}")
            return None
    
    def _extract_candidate_name(self, row: pd.Series) -> str:
        """Extract candidate name from row"""
        # Prefer explicit Name column
        name = str(row.get('Name', '')).strip()
        if name and name != 'nan':
            return name
        # Try other name-like columns
        name_cols = [col for col in row.index if 'name' in str(col).lower()]
        for col in name_cols:
            val = str(row.get(col, '')).strip()
            if val and val != 'nan':
                return val
        # Try combining first/last if present
        first = str(row.get('First Name', '')).strip()
        last = str(row.get('Last Name', '')).strip()
        if first or last:
            return f"{first} {last}".strip()
        return None
    
    def _extract_office(self, row: pd.Series) -> str:
        """Extract office from row"""
        race = str(row.get('Race', '')).strip()
        if race and race != 'nan':
            return race
        return None
    
    def _extract_party(self, row: pd.Series) -> str:
        """Extract party from row"""
        party = str(row.get('Party', '')).strip()
        if party and party != 'nan':
            return party
        return None
    
    def _extract_county(self, row: pd.Series) -> str:
        """Extract county from row"""
        county = str(row.get('County', '')).strip()
        if county and county != 'nan':
            return county
        return None
    
    def _extract_district(self, row: pd.Series) -> str:
        """Extract district from row"""
        district = row.get('District/Circuit')
        if pd.notna(district):
            district_str = str(district).strip()
            if district_str and district_str != 'nan':
                return district_str
        return None
    
    def _extract_address(self, row: pd.Series) -> str:
        """Extract address from row"""
        mailing_address = str(row.get('MailingAddress', '')).strip()
        if mailing_address and mailing_address != 'nan':
            return mailing_address
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
        return 'West Virginia'  # Default to West Virginia
    
    def _extract_zip_code(self, row: pd.Series) -> str:
        """Extract zip code from row"""
        # Zip code might be embedded in mailing address
        mailing_address = str(row.get('MailingAddress', '')).strip()
        if mailing_address and mailing_address != 'nan':
            # Look for zip code pattern
            zip_match = re.search(r'\b\d{5}(?:-\d{4})?\b', mailing_address)
            if zip_match:
                return zip_match.group(0)
        return None
    
    def _extract_phone(self, row: pd.Series) -> str:
        """Extract phone from row"""
        phone = str(row.get('CampaignPhoneNumber', '')).strip()
        if phone and phone != 'nan':
            return phone
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
        filing_date = row.get('Filing Date')
        if pd.notna(filing_date):
            # Convert to string format
            if hasattr(filing_date, 'strftime'):
                return filing_date.strftime('%Y-%m-%d')
            else:
                return str(filing_date)
        return None
    
    def _extract_election_year(self, row: pd.Series) -> str:
        """Extract election year from row"""
        # Try to extract from filing date
        filing_date = row.get('Filing Date')
        if pd.notna(filing_date):
            if hasattr(filing_date, 'strftime'):
                date_str = filing_date.strftime('%Y-%m-%d')
            else:
                date_str = str(filing_date)
            
            year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
            if year_match:
                return year_match.group(0)
        
        # Default to 2024 based on filename
        return '2024'
    
    def _extract_election_type(self, row: pd.Series) -> str:
        """Extract election type from row"""
        # Try to extract from race field
        race = str(row.get('Race', '')).strip()
        if race and race != 'nan':
            race_lower = race.lower()
            if 'primary' in race_lower:
                return 'Primary'
            elif 'general' in race_lower:
                return 'General'
            elif 'special' in race_lower:
                return 'Special'
        
        # Default to General as most candidate filings are for general elections
        return 'General'
    
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
