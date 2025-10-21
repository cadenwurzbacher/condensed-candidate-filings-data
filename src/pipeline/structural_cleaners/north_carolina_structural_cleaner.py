import pandas as pd
import logging
import os
from pathlib import Path
from .base_structural_cleaner import BaseStructuralCleaner
import re

logger = logging.getLogger(__name__)

class NorthCarolinaStructuralCleaner(BaseStructuralCleaner):
    """
    North Carolina Structural Cleaner - Phase 1 of new pipeline
    
    Focus: Extract structured data from messy raw files
    NO data transformation - just structural parsing
    Output: Clean DataFrame with consistent columns
    """
    
    def clean(self) -> pd.DataFrame:
        """
        Extract structured data from North Carolina raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting North Carolina structural cleaning")
        
        # Find North Carolina raw files
        north_carolina_files = self._find_north_carolina_files()
        if not north_carolina_files:
            logger.warning("No North Carolina raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in north_carolina_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from North Carolina files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"North Carolina structural cleaning complete: {len(df)} records")
        return df
    
    def _find_north_carolina_files(self) -> list:
        """Find all North Carolina raw data files"""
        north_carolina_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return north_carolina_files
        
        # Look for North Carolina files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'north_carolina' in filename or 'north_carolina' in filename.replace('_', ''):
                    north_carolina_files.append(str(file_path))
        
        logger.info(f"Found {len(north_carolina_files)} North Carolina files: {north_carolina_files}")
        return north_carolina_files
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single North Carolina file
        
        Args:
            file_path: Path to the raw file
            
        Returns:
            list: List of dictionaries with extracted data
        """
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext == '.csv':
            return self._extract_from_csv(file_path)
        elif file_ext in ['.xlsx', '.xls']:
            return self._extract_from_excel(file_path)
        else:
            logger.warning(f"Unsupported file type: {file_ext}")
            return []
    
    def _extract_from_csv(self, file_path: str) -> list:
        """Extract data from CSV file"""
        try:
            # Try different encodings for North Carolina CSV
            for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    logger.info(f"Read CSV file with {len(df)} rows and {len(df.columns)} columns using {encoding} encoding")
                    return self._extract_structured_data(df)
                except UnicodeDecodeError:
                    continue
            # If all encodings fail, try with error handling
            df = pd.read_csv(file_path, encoding='latin-1', errors='replace')
            logger.info(f"Read CSV file with {len(df)} rows and {len(df.columns)} columns using latin-1 with error replacement")
            return self._extract_structured_data(df)
        except Exception as e:
            logger.error(f"Failed to read CSV file {file_path}: {e}")
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
        # Check if we have at least a contest name or candidate name
        contest_name = str(row.get('contest_name', '')).strip()
        name_on_ballot = str(row.get('name_on_ballot', '')).strip()
        first_name = str(row.get('first_name', '')).strip()
        last_name = str(row.get('last_name', '')).strip()
        
        return (bool(contest_name and contest_name != 'nan') or
                bool(name_on_ballot and name_on_ballot != 'nan') or
                bool(first_name and first_name != 'nan') or
                bool(last_name and last_name != 'nan'))
    
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
        # Try name_on_ballot first, then combine first/middle/last
        name_on_ballot = str(row.get('name_on_ballot', '')).strip()
        if name_on_ballot and name_on_ballot != 'nan':
            return name_on_ballot
        
        # Fall back to combining name parts
        first_name = str(row.get('first_name', '')).strip()
        middle_name = str(row.get('middle_name', '')).strip()
        last_name = str(row.get('last_name', '')).strip()
        name_suffix = str(row.get('name_suffix_lbl', '')).strip()
        
        name_parts = []
        if first_name and first_name != 'nan':
            name_parts.append(first_name)
        if middle_name and middle_name != 'nan':
            name_parts.append(middle_name)
        if last_name and last_name != 'nan':
            name_parts.append(last_name)
        if name_suffix and name_suffix != 'nan':
            name_parts.append(name_suffix)
        
        if name_parts:
            return ' '.join(name_parts)
        return None
    
    def _extract_office(self, row: pd.Series) -> str:
        """Extract office from row"""
        contest_name = str(row.get('contest_name', '')).strip()
        if contest_name and contest_name != 'nan':
            return contest_name
        return None
    
    def _extract_party(self, row: pd.Series) -> str:
        """Extract party from row"""
        # Try party_candidate first, then party_contest
        party_candidate = str(row.get('party_candidate', '')).strip()
        party_contest = str(row.get('party_contest', '')).strip()
        
        if party_candidate and party_candidate != 'nan':
            return party_candidate
        elif party_contest and party_contest != 'nan':
            return party_contest
        return None
    
    def _extract_county(self, row: pd.Series) -> str:
        """Extract county from row"""
        county_name = str(row.get('county_name', '')).strip()
        if county_name and county_name != 'nan':
            return county_name
        return None
    
    def _extract_district(self, row: pd.Series) -> str:
        """Extract district from row"""
        # North Carolina doesn't have a dedicated district field
        # District info might be embedded in contest_name
        contest_name = str(row.get('contest_name', '')).strip()
        if contest_name and contest_name != 'nan':
            # Look for district patterns like "DISTRICT 1" or "DIST 1"
            district_match = re.search(r'(?:DISTRICT|DIST)\s*(\d+)', contest_name, re.IGNORECASE)
            if district_match:
                return district_match.group(1)
        return None
    
    def _extract_address(self, row: pd.Series) -> str:
        """Extract address from row"""
        street_address = str(row.get('street_address', '')).strip()
        if street_address and street_address != 'nan':
            return street_address
        return None
    
    def _extract_city(self, row: pd.Series) -> str:
        """Extract city from row"""
        city = str(row.get('city', '')).strip()
        if city and city != 'nan':
            return city
        return None
    
    def _extract_state(self, row: pd.Series) -> str:
        """Extract state from row"""
        state = str(row.get('state', '')).strip()
        if state and state != 'nan':
            return state
        return 'NC'  # Default to NC
    
    def _extract_zip_code(self, row: pd.Series) -> str:
        """Extract zip code from row"""
        zip_code = row.get('zip_code')
        if pd.notna(zip_code):
            zip_str = str(zip_code).strip()
            if zip_str and zip_str != 'nan':
                return zip_str
        return None
    
    def _extract_phone(self, row: pd.Series) -> str:
        """Extract phone from row"""
        # Try phone, office_phone, business_phone in order
        phone = str(row.get('phone', '')).strip()
        office_phone = str(row.get('office_phone', '')).strip()
        business_phone = str(row.get('business_phone', '')).strip()
        
        if phone and phone != 'nan':
            return phone
        elif office_phone and office_phone != 'nan':
            return office_phone
        elif business_phone and business_phone != 'nan':
            return business_phone
        return None
    
    def _extract_email(self, row: pd.Series) -> str:
        """Extract email from row"""
        email = str(row.get('email', '')).strip()
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
        candidacy_dt = row.get('candidacy_dt')
        if pd.notna(candidacy_dt):
            # Convert to string format
            if hasattr(candidacy_dt, 'strftime'):
                return candidacy_dt.strftime('%Y-%m-%d')
            else:
                return str(candidacy_dt)
        return None
    
    def _extract_election_year(self, row: pd.Series) -> str:
        """Extract election year from row"""
        # Try to extract from election date first
        election_dt = row.get('election_dt')
        if pd.notna(election_dt):
            # Look for year pattern in election date
            if hasattr(election_dt, 'strftime'):
                election_date_str = election_dt.strftime('%Y-%m-%d')
            else:
                election_date_str = str(election_dt)
            
            year_match = re.search(r'\b(19|20)\d{2}\b', election_date_str)
            if year_match:
                return year_match.group(0)
        
        # Try to extract from candidacy date
        candidacy_dt = row.get('candidacy_dt')
        if pd.notna(candidacy_dt):
            if hasattr(candidacy_dt, 'strftime'):
                candidacy_date_str = candidacy_dt.strftime('%Y-%m-%d')
            else:
                candidacy_date_str = str(candidacy_dt)
            
            year_match = re.search(r'\b(19|20)\d{2}\b', candidacy_date_str)
            if year_match:
                return year_match.group(0)
        
        # Default to 2024 based on filename
        return '2024'
    
    def _extract_election_type(self, row: pd.Series) -> str:
        """Extract election type from row"""
        # Check if it's a primary election
        has_primary = str(row.get('has_primary', '')).strip().upper()
        if has_primary == 'TRUE':
            return 'Primary'
        elif has_primary == 'FALSE':
            return 'General'
        else:
            # Default to Primary as most candidate filings are for primaries
            return 'Primary'
    
