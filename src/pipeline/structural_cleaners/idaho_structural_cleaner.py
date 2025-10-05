import pandas as pd
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

class IdahoStructuralCleaner:
    """
    Idaho Structural Cleaner - Phase 1 of new pipeline
    
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
        Extract structured data from Idaho raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting Idaho structural cleaning")
        
        # Find Idaho raw files
        idaho_files = self._find_idaho_files()
        if not idaho_files:
            logger.warning("No Idaho raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in idaho_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from Idaho files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"Idaho structural cleaning complete: {len(df)} records")
        return df
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract structured data from a DataFrame (for new pipeline)
        
        Args:
            df: Raw DataFrame to process
            
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info(f"Starting Idaho structural cleaning for DataFrame with {len(df)} records")
        
        # Extract structured data
        records = self._extract_structured_data(df)
        
        if not records:
            logger.warning("No records extracted from DataFrame")
            return pd.DataFrame()
        
        # Create structured DataFrame
        structured_df = pd.DataFrame(records)
        
        # Ensure consistent column structure
        structured_df = self._ensure_consistent_columns(structured_df)
        
        logger.info(f"Idaho structural cleaning complete: {len(structured_df)} records")
        return structured_df
    
    def _find_idaho_files(self) -> list:
        """Find all Idaho raw data files"""
        idaho_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return idaho_files
        
        # Look for Idaho files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'idaho' in filename:
                    idaho_files.append(str(file_path))
        
        logger.info(f"Found {len(idaho_files)} Idaho files: {idaho_files}")
        return idaho_files
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single Idaho file
        
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
            # Try different encodings for Idaho CSV
            for encoding in ['latin-1', 'cp1252', 'iso-8859-1']:
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
    
    def _looks_like_candidate_data(self, df: pd.DataFrame) -> bool:
        """Check if a DataFrame sample contains candidate-like columns"""
        if df.empty or len(df.columns) < 3:
            return False
        
        # Check for candidate-like column names
        columns_lower = [col.lower() for col in df.columns]
        candidate_indicators = ['candidate', 'name', 'office', 'party', 'district', 'position', 'seat']
        
        matches = sum(1 for indicator in candidate_indicators 
                     if any(indicator in col for col in columns_lower))
        
        return matches >= 2
    
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
        df.columns = [str(col).strip() for col in df.columns]
        
        return df
    
    def _is_valid_candidate_row(self, row: pd.Series) -> bool:
        """Check if a row contains valid candidate data"""
        # Check if we have at least a candidate name or position
        candidate_name = str(row.get('Name', '')).strip()
        position = str(row.get('Position', '')).strip()
        
        # Filter out NaN, empty, and invalid values
        if candidate_name.lower() in ['nan', 'none', 'null', '']:
            candidate_name = ''
        if position.lower() in ['nan', 'none', 'null', '']:
            position = ''
        
        return bool(candidate_name) or bool(position)
    
    def _extract_single_record(self, row: pd.Series) -> dict:
        """Extract a single candidate record from a row"""
        try:
            record = {
                'candidate_name': self._extract_candidate_name(row),
                'office': self._extract_office(row),
                'party': self._extract_party(row),
                'county': None,  # Idaho doesn't have county info
                'district': self._extract_district(row),
                'address': None,  # Idaho doesn't have address info
                'city': None,  # Idaho doesn't have city info
                'state': 'Idaho',
                'zip_code': None,  # Idaho doesn't have zip info
                'phone': None,  # Idaho doesn't have phone info
                'email': None,
                'website': None,  # Idaho doesn't have email info
                'filing_date': None,  # Idaho doesn't have filing date info
                'election_year': '2024',  # From filename
                'election_type': 'General',  # Default assumption
                'address_state': 'Idaho',
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
        position = str(row.get('Position', '')).strip()
        if position and position != 'nan':
            return position
        return None
    
    def _extract_party(self, row: pd.Series) -> str:
        """Extract party from row"""
        party = str(row.get('Party', '')).strip()
        if party and party != 'nan':
            return party
        return None
    
    def _extract_district(self, row: pd.Series) -> str:
        """Extract district from row"""
        district = row.get('Dist.')
        if pd.notna(district):
            district_str = str(district).strip()
            if district_str and district_str != 'nan':
                return district_str
        
        # Also check Seat field for additional district info
        seat = row.get('Seat')
        if pd.notna(seat):
            seat_str = str(seat).strip()
            if seat_str and seat_str != 'nan':
                return seat_str
        
        return None
    
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
