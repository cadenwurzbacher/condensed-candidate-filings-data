import pandas as pd
import logging
import os
from pathlib import Path
from .base_structural_cleaner import BaseStructuralCleaner
import re

logger = logging.getLogger(__name__)

class SouthDakotaStructuralCleaner(BaseStructuralCleaner):
    """
    South Dakota Structural Cleaner - Phase 1 of new pipeline
    
    Focus: Extract structured data from messy raw files
    NO data transformation - just structural parsing
    Output: Clean DataFrame with consistent columns
    """
    
    def clean(self) -> pd.DataFrame:
        """
        Extract structured data from South Dakota raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting South Dakota structural cleaning")
        
        # Find South Dakota raw files
        south_dakota_files = self._find_south_dakota_files()
        if not south_dakota_files:
            logger.warning("No South Dakota raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in south_dakota_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from South Dakota files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"South Dakota structural cleaning complete: {len(df)} records")
        return df
    
    def _find_south_dakota_files(self) -> list:
        """Find all South Dakota raw data files"""
        south_dakota_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return south_dakota_files
        
        # Look for South Dakota files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'south_dakota' in filename:
                    south_dakota_files.append(str(file_path))
        
        logger.info(f"Found {len(south_dakota_files)} South Dakota files: {south_dakota_files}")
        return south_dakota_files
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single South Dakota file
        
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
    
    def _is_valid_candidate_row(self, row: pd.Series) -> bool:
        """Check if a row contains valid candidate data"""
        # Check if we have at least a candidate name or contest
        name = str(row.get('Name', '')).strip()
        contest = str(row.get('Contest', '')).strip()
        
        return (bool(name and name != 'nan') or
                bool(contest and contest != 'nan'))
    
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
                'state': 'South Dakota',
                'zip_code': self._extract_zip_code(row),
                'phone': self._extract_phone(row),
                'email': self._extract_email(row),
            'website': self._extract_website(row),
                'facebook': self._extract_facebook(row),
            'twitter': self._extract_twitter(row),
            'filing_date': self._extract_filing_date(row),
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
        name = str(row.get('Name', '')).strip()
        if name and name != 'nan':
            return name
        return None
    
    def _extract_office(self, row: pd.Series) -> str:
        """Extract office from row"""
        contest = str(row.get('Contest', '')).strip()
        if contest and contest != 'nan':
            return contest
        return None
    
    def _extract_party(self, row: pd.Series) -> str:
        """Extract party from row"""
        party = str(row.get('Party', '')).strip()
        if party and party != 'nan':
            return party
        return None
    
    def _extract_county(self, row: pd.Series) -> str:
        """Extract county from row"""
        district_county = str(row.get('District/County', '')).strip()
        if district_county and district_county != 'nan':
            # Look for county patterns
            if 'County' in district_county:
                return district_county
            # If it's just a county name without "County" suffix
            elif not re.search(r'\b(District|Ward|Precinct)\b', district_county, re.IGNORECASE):
                return district_county
        return None
    
    def _extract_district(self, row: pd.Series) -> str:
        """Extract district from row"""
        district_county = str(row.get('District/County', '')).strip()
        if district_county and district_county != 'nan':
            # Look for district patterns
            district_match = re.search(r'\b(District|Ward|Precinct)\s*(\d+|[A-Z]+)\b', district_county, re.IGNORECASE)
            if district_match:
                return district_match.group(0)
        return None
    
    def _extract_address(self, row: pd.Series) -> str:
        """Extract address from row"""
        mailing_address = str(row.get('Mailing Address', '')).strip()
        if mailing_address and mailing_address != 'nan':
            return mailing_address
        return None
    
    def _extract_city(self, row: pd.Series) -> str:
        """Extract city from row"""
        # City info might be embedded in mailing address
        mailing_address = str(row.get('Mailing Address', '')).strip()
        if mailing_address and mailing_address != 'nan':
            # Look for city patterns (usually before state and zip)
            # This is a simple extraction - could be enhanced with address parsing
            address_parts = mailing_address.split(',')
            if len(address_parts) > 1:
                # City is usually the second-to-last part before state
                city_part = address_parts[-2].strip() if len(address_parts) >= 2 else ''
                if city_part and not re.search(r'\b(SD|South Dakota)\b', city_part, re.IGNORECASE):
                    return city_part
        return None
    
    def _extract_zip_code(self, row: pd.Series) -> str:
        """Extract zip code from row"""
        # Zip code might be embedded in mailing address
        mailing_address = str(row.get('Mailing Address', '')).strip()
        if mailing_address and mailing_address != 'nan':
            # Look for zip code pattern
            zip_match = re.search(r'\b\d{5}(?:-\d{4})?\b', mailing_address)
            if zip_match:
                return zip_match.group(0)
        return None
    
    def _extract_phone(self, row: pd.Series) -> str:
        """Extract phone from row"""
        # South Dakota doesn't have explicit phone information
        return None
    
    def _extract_email(self, row: pd.Series) -> str:
        """Extract email from row"""
        # South Dakota doesn't have explicit email information
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
        filing_date = row.get('Petition Filing Date')
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
        filing_date = row.get('Petition Filing Date')
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
        # Try to extract from contest field
        contest = str(row.get('Contest', '')).strip()
        if contest and contest != 'nan':
            contest_lower = contest.lower()
            if 'primary' in contest_lower:
                return 'Primary'
            elif 'general' in contest_lower:
                return 'General'
            elif 'special' in contest_lower:
                return 'Special'
        
        # Default to General as most candidate filings are for general elections
        return 'General'
    
    def _extract_address_state(self, row: pd.Series) -> str:
        """Extract state from mailing address"""
        mailing_address = str(row.get('Mailing Address', '')).strip()
        if mailing_address and mailing_address != 'nan':
            # Look for state patterns
            state_match = re.search(r'\b([A-Z]{2})\s+\d{5}(?:-\d{4})?\b', mailing_address)
            if state_match:
                return state_match.group(1)
            # Look for full state names
            state_match = re.search(r'\b(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming)\b', mailing_address, re.IGNORECASE)
            if state_match:
                return state_match.group(1)
        
        return 'South Dakota'  # Default to South Dakota
    
