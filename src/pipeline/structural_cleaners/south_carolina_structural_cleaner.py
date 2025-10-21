import pandas as pd
import logging
import os
from pathlib import Path
from .base_structural_cleaner import BaseStructuralCleaner
import re

logger = logging.getLogger(__name__)

class SouthCarolinaStructuralCleaner(BaseStructuralCleaner):
    """
    South Carolina Structural Cleaner - Phase 1 of new pipeline
    
    Focus: Extract structured data from messy raw files
    NO data transformation - just structural parsing
    Output: Clean DataFrame with consistent columns
    """
    
    def clean(self) -> pd.DataFrame:
        """
        Extract structured data from South Carolina raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting South Carolina structural cleaning")
        
        # Find South Carolina raw files
        south_carolina_files = self._find_south_carolina_files()
        if not south_carolina_files:
            logger.warning("No South Carolina raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in south_carolina_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from South Carolina files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"South Carolina structural cleaning complete: {len(df)} records")
        return df
    
    def _find_south_carolina_files(self) -> list:
        """Find all South Carolina raw data files"""
        south_carolina_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return south_carolina_files
        
        # Look for South Carolina files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'south_carolina' in filename:
                    south_carolina_files.append(str(file_path))
        
        logger.info(f"Found {len(south_carolina_files)} South Carolina files: {south_carolina_files}")
        return south_carolina_files
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single South Carolina file
        
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
        # Check if we have at least a candidate name or office
        first_name = str(row.get('Candidate FirstName', '')).strip()
        last_name = str(row.get('Candidate LastName', '')).strip()
        office = str(row.get('Office', '')).strip()
        
        return (bool(first_name and first_name != 'nan') or
                bool(last_name and last_name != 'nan') or
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
                'state': 'South Carolina',
                'zip_code': self._extract_zip_code(row),
                'phone': self._extract_phone(row),
                'email': self._extract_email(row),
            'website': self._extract_website(row),
                'facebook': self._extract_facebook(row),
            'twitter': self._extract_twitter(row),
            'filing_date': self._extract_filing_date(row),
                'election_year': self._extract_election_year(row),
                'election_type': self._extract_election_type(row),
                'address_state': 'South Carolina',
                'raw_data': str(row.to_dict())  # Store original row data
            }
            
            return record
            
        except Exception as e:
            logger.warning(f"Failed to extract record from row: {e}")
            return None
    
    def _extract_candidate_name(self, row: pd.Series) -> str:
        """Extract candidate name from row"""
        # Try ballot name first, then combine individual name parts
        ballot_first = str(row.get('Ballot Name (first - middle)', '')).strip()
        ballot_last = str(row.get('Ballot Name (last - suffix)', '')).strip()
        
        if ballot_first and ballot_first != 'nan' and ballot_last and ballot_last != 'nan':
            return f"{ballot_first} {ballot_last}"
        
        # Fall back to individual name parts
        first_name = str(row.get('Candidate FirstName', '')).strip()
        middle_name = str(row.get('Candidate MiddleName', '')).strip()
        last_name = str(row.get('Candidate LastName', '')).strip()
        suffix = str(row.get('Candidate Suffix', '')).strip()
        
        name_parts = []
        if first_name and first_name != 'nan':
            name_parts.append(first_name)
        if middle_name and middle_name != 'nan':
            name_parts.append(middle_name)
        if last_name and last_name != 'nan':
            name_parts.append(last_name)
        if suffix and suffix != 'nan':
            name_parts.append(suffix)
        
        if name_parts:
            return ' '.join(name_parts)
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
    
    def _extract_county(self, row: pd.Series) -> str:
        """Extract county from row"""
        associated_counties = str(row.get('Associated Counties', '')).strip()
        if associated_counties and associated_counties != 'nan':
            return associated_counties
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
        contact_address = str(row.get('Contact Address', '')).strip()
        if contact_address and contact_address != 'nan':
            return contact_address
        return None
    
    def _extract_city(self, row: pd.Series) -> str:
        """Extract city from row"""
        # City info might be embedded in contact address
        contact_address = str(row.get('Contact Address', '')).strip()
        if contact_address and contact_address != 'nan':
            # Look for city patterns (usually before state and zip)
            # This is a simple extraction - could be enhanced with address parsing
            address_parts = contact_address.split(',')
            if len(address_parts) > 1:
                # City is usually the second-to-last part before state
                city_part = address_parts[-2].strip() if len(address_parts) >= 2 else ''
                if city_part and not re.search(r'\b(SC|South Carolina)\b', city_part, re.IGNORECASE):
                    return city_part
        return None
    
    def _extract_zip_code(self, row: pd.Series) -> str:
        """Extract zip code from row"""
        # Zip code might be embedded in contact address
        contact_address = str(row.get('Contact Address', '')).strip()
        if contact_address and contact_address != 'nan':
            # Look for zip code pattern
            zip_match = re.search(r'\b\d{5}(?:-\d{4})?\b', contact_address)
            if zip_match:
                return zip_match.group(0)
        return None
    
    def _extract_phone(self, row: pd.Series) -> str:
        """Extract phone from row"""
        phone = str(row.get('Contact Phone Number', '')).strip()
        if phone and phone != 'nan':
            return phone
        return None
    
    def _extract_email(self, row: pd.Series) -> str:
        """Extract email from row"""
        email = str(row.get('Contact Email', '')).strip()
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
        date_filed = row.get('Date Filed')
        if pd.notna(date_filed):
            # Convert to string format
            if hasattr(date_filed, 'strftime'):
                return date_filed.strftime('%Y-%m-%d')
            else:
                return str(date_filed)
        return None
    
    def _extract_election_year(self, row: pd.Series) -> str:
        """Extract election year from row"""
        # Try to extract from election name first
        election_name = str(row.get('Election Name', '')).strip()
        if election_name and election_name != 'nan':
            # Look for year pattern in election name
            year_match = re.search(r'\b(19|20)\d{2}\b', election_name)
            if year_match:
                return year_match.group(0)
        
        # Try to extract from filing date
        date_filed = row.get('Date Filed')
        if pd.notna(date_filed):
            if hasattr(date_filed, 'strftime'):
                date_str = date_filed.strftime('%Y-%m-%d')
            else:
                date_str = str(date_filed)
            
            year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
            if year_match:
                return year_match.group(0)
        
        # Default to 2024 based on filename
        return '2024'
    
    def _extract_election_type(self, row: pd.Series) -> str:
        """Extract election type from row"""
        # Try to extract from election name
        election_name = str(row.get('Election Name', '')).strip()
        if election_name and election_name != 'nan':
            election_lower = election_name.lower()
            if 'primary' in election_lower:
                return 'Primary'
            elif 'general' in election_lower:
                return 'General'
            elif 'special' in election_lower:
                return 'Special'
        
        # Default to Primary as most candidate filings are for primaries
        return 'Primary'
    
