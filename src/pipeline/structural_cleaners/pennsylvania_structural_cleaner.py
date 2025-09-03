import pandas as pd
import logging
import os
from pathlib import Path
import re

logger = logging.getLogger(__name__)

class PennsylvaniaStructuralCleaner:
    """
    Pennsylvania Structural Cleaner - Phase 1 of new pipeline
    
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
        Extract structured data from Pennsylvania raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting Pennsylvania structural cleaning")
        
        # Find Pennsylvania raw files
        pennsylvania_files = self._find_pennsylvania_files()
        if not pennsylvania_files:
            logger.warning("No Pennsylvania raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in pennsylvania_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from Pennsylvania files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"Pennsylvania structural cleaning complete: {len(df)} records")
        return df
    
    def _find_pennsylvania_files(self) -> list:
        """Find all Pennsylvania raw data files"""
        pennsylvania_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return pennsylvania_files
        
        # Look for Pennsylvania files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'pennsylvania' in filename:
                    pennsylvania_files.append(str(file_path))
        
        logger.info(f"Found {len(pennsylvania_files)} Pennsylvania files: {pennsylvania_files}")
        return pennsylvania_files
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single Pennsylvania file
        
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
            df = pd.read_excel(file_path, header=None)
            logger.info(f"Read Excel file with {len(df)} rows and {len(df.columns)} columns")
            
            # The second row contains the actual column headers (first row is metadata)
            if len(df) > 1:
                # Use the second row as headers
                df.columns = df.iloc[1]
                # Remove the first two rows since they're metadata and headers
                df = df.iloc[2:].reset_index(drop=True)
                logger.info(f"Applied headers from second row: {df.columns.tolist()}")
            
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
        # Check if we have at least a name or office
        name = str(row.get('Name', '')).strip()
        office = str(row.get('Office', '')).strip()
        
        return (bool(name and name != 'nan') or
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
                'address': None,  # Pennsylvania doesn't have address info
                'city': self._extract_city(row),
                'state': 'Pennsylvania',
                'zip_code': None,  # Pennsylvania doesn't have zip code info
                'phone': None,  # Pennsylvania doesn't have phone info
                'email': None,
                'website': None,  # Pennsylvania doesn't have email info
                'filing_date': None,  # Pennsylvania doesn't have filing date info
                'election_year': self._extract_election_year(row),
                'election_type': self._extract_election_type(row),
                'address_state': 'Pennsylvania',
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
        county = str(row.get('County', '')).strip()
        if county and county != 'nan':
            return county
        return None
    
    def _extract_district(self, row: pd.Series) -> str:
        """Extract district from row"""
        district_name = str(row.get('District Name', '')).strip()
        if district_name and district_name != 'nan':
            # Look for district patterns like "6th Congressional District"
            district_match = re.search(r'(\d+)(?:st|nd|rd|th)?\s+(?:Congressional\s+)?District', district_name, re.IGNORECASE)
            if district_match:
                return district_match.group(1)
            
            # Look for other district patterns
            district_match = re.search(r'District\s*(\d+)', district_name, re.IGNORECASE)
            if district_match:
                return district_match.group(1)
            
            # Return the full district name if no numeric extraction
            return district_name
        return None
    
    def _extract_city(self, row: pd.Series) -> str:
        """Extract city from row"""
        municipality = str(row.get('Municipality', '')).strip()
        if municipality and municipality != 'nan':
            return municipality
        return None
    
    def _extract_election_year(self, row: pd.Series) -> str:
        """Extract election year from row"""
        # Try to extract from filename or default based on context
        # Pennsylvania files are named with years, so we can infer from filename
        # For now, default to 2024 as most recent
        return '2024'
    
    def _extract_election_type(self, row: pd.Series) -> str:
        """Extract election type from row"""
        # Check if it's a primary or general election based on the data
        # Pennsylvania has both primary and general elections
        # Default to Primary as most candidate filings are for primaries
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
