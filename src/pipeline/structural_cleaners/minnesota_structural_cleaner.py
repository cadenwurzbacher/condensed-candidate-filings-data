import pandas as pd
import logging
import os
from pathlib import Path
from .base_structural_cleaner import BaseStructuralCleaner
import re

logger = logging.getLogger(__name__)

class MinnesotaStructuralCleaner(BaseStructuralCleaner):
    """
    Minnesota Structural Cleaner - Phase 1 of new pipeline
    
    Focus: Extract structured data from Minnesota semicolon-delimited files
    NO data transformation - just structural parsing
    Output: Clean DataFrame with consistent columns
    """
    
    def clean(self) -> pd.DataFrame:
        """
        Extract structured data from Minnesota raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting Minnesota structural cleaning")
        
        # Find Minnesota raw files
        minnesota_files = self._find_minnesota_files()
        if not minnesota_files:
            logger.warning("No Minnesota raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in minnesota_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from Minnesota files")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(all_records)
        logger.info(f"Minnesota structural cleaning complete: {len(df)} records")
        
        return df
    
    def _find_minnesota_files(self) -> list:
        """Find Minnesota raw data files"""
        minnesota_files = []
        
        # Look for Minnesota files
        minnesota_patterns = [
            "minnesota_candidates_*.txt",
            "minnesota_*.txt",
            "mn_candidates_*.txt",
            "mn_*.txt"
        ]
        
        for pattern in minnesota_patterns:
            files = list(Path(self.raw_dir).glob(pattern))
            minnesota_files.extend(files)
        
        # Remove duplicates and sort
        minnesota_files = sorted(list(set(minnesota_files)))
        logger.info(f"Found {len(minnesota_files)} Minnesota files: {[f.name for f in minnesota_files]}")
        
        return minnesota_files
    
    def _extract_election_year_from_filename(self, file_path: Path) -> int:
        """Extract election year from filename"""
        filename = file_path.name
        
        # Look for 4-digit year in filename
        year_match = re.search(r'(\d{4})', filename)
        if year_match:
            return int(year_match.group(1))
        
        # Default to current year if not found
        logger.warning(f"Could not extract election year from {filename}, using 2025")
        return 2025
    
    def _extract_from_file(self, file_path: Path) -> list:
        """Extract records from a single Minnesota file"""
        records = []
        
        try:
            # Read semicolon-delimited file
            df = pd.read_csv(file_path, sep=';', header=None)
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
        
        # Determine file type based on number of columns
        is_local_file = len(row) == 18
        
        if is_local_file:
            # Municipal and School District Candidates (18 columns)
            record = {
                # Basic info
                'state': 'Minnesota',
                'raw_file': str(file_path),
                'raw_data': str(row.to_dict()),
                'election_year': election_year,
                
                # Name fields (column 1: Candidate Name)
                'candidate_name': self._safe_get(row, 1),
                'name_on_ballot': self._safe_get(row, 1),
                
                # Office and district (columns 3 and 2)
                'office': self._safe_get(row, 3),  # Office Title
                'district': self._safe_get(row, 2),  # Office ID
                
                # Party (column 7: Party Abbreviation, always "NP" for local)
                'party': self._safe_get(row, 7),
                
                # Address info (columns 8-11: Residence address)
                'address': self._safe_get(row, 8),  # Residence Street Address
                'city': self._safe_get(row, 9),     # Residence City
                'zip_code': self._safe_get(row, 11), # Residence Zip
                
                # Contact info (columns 15-17: Campaign contact)
                'phone': self._safe_get(row, 15),   # Campaign Phone
                'website': self._safe_get(row, 16),  # Campaign Website
                'email': self._safe_get(row, 17),   # Campaign Email
                
                # Additional fields
                'first_name': None,
                'middle_name': None,
                'last_name': None,
                'suffix': None,
                'county': None,
                'filing_date': None,
                'election_date': None,
                'election_type': None,
                'status': None,
            }
        else:
            # Federal, State, and County Candidates (20 columns)
            record = {
                # Basic info
                'state': 'Minnesota',
                'raw_file': str(file_path),
                'raw_data': str(row.to_dict()),
                'election_year': election_year,
                
                # Name fields (column 1: Candidate Name)
                'candidate_name': self._safe_get(row, 1),
                'name_on_ballot': self._safe_get(row, 1),
                
                # Office and district (columns 3 and 2)
                'office': self._safe_get(row, 3),  # Office Title
                'district': self._safe_get(row, 2),  # Office ID
                
                # Party (column 5: Party Abbreviation)
                'party': self._safe_get(row, 5),
                
                # Address info (columns 6-9: Residence address)
                'address': self._safe_get(row, 6),  # Residence Street Address
                'city': self._safe_get(row, 7),     # Residence City
                'zip_code': self._safe_get(row, 9), # Residence Zip
                
                # Contact info (columns 14-16: Campaign contact)
                'phone': self._safe_get(row, 14),   # Campaign Phone
                'website': self._safe_get(row, 15),  # Campaign Website
                'email': self._safe_get(row, 16),   # Campaign Email
                
                # Additional fields
                'first_name': None,
                'middle_name': None,
                'last_name': None,
                'suffix': None,
                'county': None,
                'filing_date': None,
                'election_date': None,
                'election_type': None,
                'status': None,
            }
        
        # Clean up NaN values
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
        
        return record
    
    def _safe_get(self, row: pd.Series, column: int) -> any:
        """Safely get value from row, handling missing columns"""
        try:
            if column < len(row):
                return row.iloc[column]
            return None
        except:
            return None
