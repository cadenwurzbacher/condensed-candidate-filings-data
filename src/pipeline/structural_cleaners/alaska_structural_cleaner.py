import pandas as pd
import logging
import os
from pathlib import Path
import re

logger = logging.getLogger(__name__)

class AlaskaStructuralCleaner:
    """
    Alaska Structural Cleaner - Phase 1 of new pipeline
    
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
    
    def _looks_like_candidate_data(self, df: pd.DataFrame) -> bool:
        """Check if a DataFrame looks like it contains candidate data"""
        if df.empty or len(df.columns) < 3:
            return False
        
        # Look for common candidate data columns
        column_names = [str(col).lower() for col in df.columns]
        
        candidate_indicators = [
            'name', 'candidate', 'office', 'party', 'county', 'district',
            'address', 'phone', 'email', 'filing', 'election'
        ]
        
        # Count how many candidate indicators we find
        matches = sum(1 for indicator in candidate_indicators 
                     if any(indicator in col for col in column_names))
        
        # If we find at least 2-3 indicators, this looks like candidate data
        return matches >= 2
    
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
    
    def _clean_dataframe_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean up DataFrame structure without transforming data"""
        # Remove completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        # Reset index
        df = df.reset_index(drop=True)
        
        # Clean column names
        df.columns = [str(col).strip() if pd.notna(col) else f"col_{i}" 
                     for i, col in enumerate(df.columns)]
        
        return df
    
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
    
    def _is_valid_candidate_row(self, row: pd.Series) -> bool:
        """Check if a row contains valid candidate data"""
        # Skip rows that are likely headers or summaries
        row_str = ' '.join(str(val) for val in row.values if pd.notna(val)).lower()
        
        # Skip if it looks like a header or summary
        skip_indicators = [
            'total', 'count', 'summary', 'header', 'name', 'office', 'party',
            'candidate', 'filing', 'election', 'date', 'address'
        ]
        
        # If the row contains mostly header-like text, skip it
        header_matches = sum(1 for indicator in skip_indicators if indicator in row_str)
        if header_matches >= 3:
            return False
        
        # Skip if all values are empty or very short
        non_empty_values = [str(val) for val in row.values if pd.notna(val) and str(val).strip()]
        if len(non_empty_values) < 2:
            return False
        
        return True
    
    def _extract_candidate_name(self, row: pd.Series) -> str:
        """Extract candidate name from row"""
        # Look for name-like columns
        name_columns = [col for col in row.index if 'name' in str(col).lower()]
        
        for col in name_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        # If no name column found, try first non-empty column
        for col in row.index:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        return None
    
    def _extract_office(self, row: pd.Series) -> str:
        """Extract office from row"""
        office_columns = [col for col in row.index if 'office' in str(col).lower()]
        
        for col in office_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        return None
    
    def _extract_party(self, row: pd.Series) -> str:
        """Extract party from row"""
        party_columns = [col for col in row.index if 'party' in str(col).lower()]
        
        for col in party_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        return None
    
    def _extract_county(self, row: pd.Series) -> str:
        """Extract county from row"""
        county_columns = [col for col in row.index if 'county' in str(col).lower()]
        
        for col in county_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        return None
    
    def _extract_district(self, row: pd.Series) -> str:
        """Extract district from row"""
        district_columns = [col for col in row.index if 'district' in str(col).lower()]
        
        for col in district_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        return None
    
    def _extract_address(self, row: pd.Series) -> str:
        """Extract address from row"""
        address_columns = [col for col in row.index if 'address' in str(col).lower()]
        
        for col in address_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        return None
    
    def _extract_city(self, row: pd.Series) -> str:
        """Extract city from row"""
        city_columns = [col for col in row.index if 'city' in str(col).lower()]
        
        for col in city_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        return None
    
    def _extract_state(self, row: pd.Series) -> str:
        """Extract state from row"""
        state_columns = [col for col in row.index if 'state' in str(col).lower()]
        
        for col in state_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        # Default to Alaska for Alaska files
        return "Alaska"
    
    def _extract_zip_code(self, row: pd.Series) -> str:
        """Extract zip code from row"""
        zip_columns = [col for col in row.index if 'zip' in str(col).lower()]
        
        for col in zip_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        return None
    
    def _extract_phone(self, row: pd.Series) -> str:
        """Extract phone from row"""
        phone_columns = [col for col in row.index if 'phone' in str(col).lower()]
        
        for col in phone_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        return None
    
    def _extract_email(self, row: pd.Series) -> str:
        """Extract email from row"""
        email_columns = [col for col in row.index if 'email' in str(col).lower()]
        
        for col in email_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
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
        date_columns = [col for col in row.index if 'date' in str(col).lower() or 'filing' in str(col).lower()]
        
        for col in date_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        return None
    
    def _extract_election_year(self, row: pd.Series, file_path: str) -> str:
        """Extract election year from row or filename"""
        # Try to get from row first
        year_columns = [col for col in row.index if 'year' in str(col).lower() or 'election' in str(col).lower()]
        
        for col in year_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                # Try to extract year from value
                year_match = re.search(r'20\d{2}', str(value))
                if year_match:
                    return year_match.group()
        
        # Try to extract from filename
        filename = Path(file_path).name
        year_match = re.search(r'20\d{2}', filename)
        if year_match:
            return year_match.group()
        
        return None
    
    def _extract_election_type(self, row: pd.Series) -> str:
        """Extract election type from row"""
        type_columns = [col for col in row.index if 'type' in str(col).lower() or 'election' in str(col).lower()]
        
        for col in type_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        return None
    
    def _extract_address_state(self, row: pd.Series) -> str:
        """Extract address state from row"""
        # Look for address state or state in address
        address_state_columns = [col for col in row.index if 'address_state' in str(col).lower()]
        
        for col in address_state_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        # Default to Alaska for Alaska files
        return "Alaska"
    
    def _ensure_consistent_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure DataFrame has consistent column structure"""
        # Define expected columns
        expected_columns = [
            'candidate_name', 'office', 'party', 'county', 'district',
            'address', 'city', 'state', 'zip_code', 'phone', 'email', 'website',
            'facebook', 'twitter', 'filing_date', 'election_year', 'election_type', 'address_state',
            'raw_data'
        ]
        
        # Add missing columns with None values
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None
        
        # Reorder columns to match expected structure
        df = df[expected_columns]
        
        return df
