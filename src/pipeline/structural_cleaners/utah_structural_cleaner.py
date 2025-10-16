import pandas as pd
import logging
import os
from pathlib import Path
import re

logger = logging.getLogger(__name__)

class UtahStructuralCleaner:
    """
    Utah Structural Cleaner - Phase 1 of new pipeline
    
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
        Extract structured data from Utah raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting Utah structural cleaning")
        
        # Find Utah raw files
        utah_files = self._find_utah_files()
        if not utah_files:
            logger.warning("No Utah raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in utah_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from Utah files")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(all_records)
        logger.info(f"Utah structural cleaning complete: {len(df)} records")
        
        return df
    
    def _find_utah_files(self) -> list:
        """Find Utah raw data files"""
        utah_files = []
        
        # Look for Utah Excel files
        utah_patterns = [
            "utah_candidates_*.xlsx",
            "utah_candidates_*.xls", 
            "utah_*.xlsx",
            "utah_*.xls"
        ]
        
        for pattern in utah_patterns:
            files = list(Path(self.raw_dir).glob(pattern))
            utah_files.extend(files)
        
        # Remove duplicates and sort
        utah_files = sorted(list(set(utah_files)))
        logger.info(f"Found {len(utah_files)} Utah files: {[f.name for f in utah_files]}")
        
        return utah_files
    
    def _extract_election_year_from_filename(self, file_path: Path) -> int:
        """Extract election year from filename"""
        filename = file_path.name
        
        # Look for 4-digit year in filename
        year_match = re.search(r'(\d{4})', filename)
        if year_match:
            return int(year_match.group(1))
        
        # Default to current year if not found
        logger.warning(f"Could not extract election year from {filename}, using 2024")
        return 2024
    
    def _extract_from_file(self, file_path: Path) -> list:
        """Extract records from a single Utah file"""
        records = []
        
        try:
            # Read Excel file
            df = pd.read_excel(file_path)
            logger.info(f"Loaded {len(df)} rows from {file_path.name}")
            
            # Extract election year from filename
            election_year = self._extract_election_year_from_filename(file_path)
            
            # Process each row
            for idx, row in df.iterrows():
                try:
                    record = self._extract_record_from_row(row, file_path, election_year)
                    if record:
                        records.append(record)
                except Exception as e:
                    logger.warning(f"Failed to extract record from row {idx} in {file_path.name}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to read file {file_path}: {e}")
            return []
        
        return records
    
    def _extract_record_from_row(self, row: pd.Series, file_path: Path, election_year: int) -> dict:
        """Extract a single record from a row"""
        
        # Utah data structure:
        # 'Name on Ballot', 'First Name', 'Middle Name', 'Last Name', 'Suffix', 
        # 'Office', 'District', 'Party', 'Email', 'Website', 'Status'
        
        record = {
            # Basic info
            'state': 'Utah',
            'raw_file': str(file_path),
            'raw_data': str(row.to_dict()),
            'election_year': election_year,
            
            # Name fields
            'name_on_ballot': self._safe_get(row, 'Name on Ballot'),
            'candidate_name': self._safe_get(row, 'Name on Ballot'),  # Add candidate_name for base cleaner
            'first_name': self._safe_get(row, 'First Name'),
            'middle_name': self._safe_get(row, 'Middle Name'),
            'last_name': self._safe_get(row, 'Last Name'),
            'suffix': self._safe_get(row, 'Suffix'),
            
            # Office and district
            'office': self._safe_get(row, 'Office'),
            'district': self._safe_get(row, 'District'),
            
            # Party
            'party': self._safe_get(row, 'Party'),
            
            # Contact info
            'email': self._safe_get(row, 'Email'),
            'website': self._safe_get(row, 'Website'),
            
            # Status
            'status': self._safe_get(row, 'Status'),
            
            # Additional fields (set to None for now)
            'address': None,
            'city': None,
            'zip_code': None,
            'county': None,
            'phone': None,
            'filing_date': None,
            'election_date': None,
            'election_type': None,
        }
        
        # Clean up NaN values
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
        
        return record
    
    def _safe_get(self, row: pd.Series, column: str) -> any:
        """Safely get value from row, handling missing columns"""
        try:
            return row[column] if column in row else None
        except:
            return None
