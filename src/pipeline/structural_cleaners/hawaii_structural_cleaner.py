import pandas as pd
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

class HawaiiStructuralCleaner:
    """
    Hawaii Structural Cleaner - Phase 1 of new pipeline
    
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
        Extract structured data from Hawaii raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting Hawaii structural cleaning")
        
        # Find Hawaii raw files
        hawaii_files = self._find_hawaii_files()
        if not hawaii_files:
            logger.warning("No Hawaii raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in hawaii_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from Hawaii files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"Hawaii structural cleaning complete: {len(df)} records")
        return df
    
    def _find_hawaii_files(self) -> list:
        """Find all Hawaii raw data files"""
        hawaii_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return hawaii_files
        
        # Look for Hawaii files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'hawaii' in filename or 'hi' in filename:
                    hawaii_files.append(str(file_path))
        
        logger.info(f"Found {len(hawaii_files)} Hawaii files: {hawaii_files}")
        return hawaii_files
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single Hawaii file
        
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
            df = pd.read_excel(file_path)
            logger.info(f"Loaded Excel file with {len(df)} rows and {len(df.columns)} columns")
            return self._process_dataframe(df, file_path)
        except Exception as e:
            logger.error(f"Failed to read Excel file {file_path}: {e}")
            return []
    
    def _extract_from_csv(self, file_path: str) -> list:
        """Extract data from CSV file"""
        try:
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    logger.info(f"Loaded CSV file with {len(df)} rows and {len(df.columns)} columns using {encoding}")
                    return self._process_dataframe(df, file_path)
                except UnicodeDecodeError:
                    continue
            logger.error(f"Failed to read CSV file {file_path} with any encoding")
            return []
        except Exception as e:
            logger.error(f"Failed to read CSV file {file_path}: {e}")
            return []
    
    def _process_dataframe(self, df: pd.DataFrame, file_path: str) -> list:
        """
        Process DataFrame and extract structured records
        
        Args:
            df: DataFrame to process
            file_path: Source file path
            
        Returns:
            list: List of structured records
        """
        records = []
        
        for idx, row in df.iterrows():
            try:
                record = self._extract_record_from_row(row, file_path, idx)
                if record:
                    records.append(record)
            except Exception as e:
                logger.warning(f"Failed to extract record from row {idx}: {e}")
                continue
        
        return records
    
    def _extract_record_from_row(self, row: pd.Series, file_path: str, row_idx: int) -> dict:
        """
        Extract a structured record from a DataFrame row
        
        Args:
            row: DataFrame row
            file_path: Source file path
            row_idx: Row index for logging
            
        Returns:
            dict: Structured record
        """
        # Initialize record with raw data
        record = {
            'raw_data': row.to_dict(),
            'state': 'Hawaii',
            'file_source': file_path,
            'row_index': row_idx
        }
        
        # Map columns based on Hawaii's actual data structure
        column_mapping = {
            'candidate_name': ['ballot_name'],  # Use ballot name as primary
            'legal_name': ['legal_name'],
            'party': ['party'],
            'office': ['office'],
            'filing_date': ['filing_date']
        }
        
        # Extract data using column mapping
        for target_col, possible_source_cols in column_mapping.items():
            for source_col in possible_source_cols:
                if source_col in row.index and pd.notna(row[source_col]):
                    record[target_col] = str(row[source_col]).strip()
                    break
        
        # Ensure required fields exist
        required_fields = ['candidate_name', 'party', 'office', 'election_year']
        for field in required_fields:
            if field not in record:
                if field == 'election_year':
                    # Extract year from filename or use default
                    record[field] = '2024'  # Default for Hawaii
                else:
                    record[field] = None
        
        return record
    
    def _ensure_consistent_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure DataFrame has consistent column structure
        
        Args:
            df: DataFrame to standardize
            
        Returns:
            pd.DataFrame: DataFrame with consistent columns
        """
        # Define expected columns
        expected_columns = [
            'raw_data', 'state', 'file_source', 'row_index', 'election_year',
            'candidate_name', 'first_name', 'last_name', 'middle_name', 
            'prefix', 'suffix', 'party', 'office', 'district', 'county',
            'address', 'city', 'state', 'zip_code', 'email', 'website', 'phone',
            'filing_date', 'election_date'
        
        ]
        
        # Add missing columns
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None
        
        # Reorder columns
        df = df[expected_columns]
        
        logger.info(f"Ensured consistent column structure: {list(df.columns)}")
        return df
