import pandas as pd
import logging
import os
from pathlib import Path
import re

logger = logging.getLogger(__name__)

class FloridaStructuralCleaner:
    """
    Florida Structural Cleaner - Phase 1 of new pipeline
    
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
        Extract structured data from Florida raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting Florida structural cleaning")
        
        # Find Florida raw files
        florida_files = self._find_florida_files()
        if not florida_files:
            logger.warning("No Florida raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in florida_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from Florida files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"Florida structural cleaning complete: {len(df)} records")
        return df
    
    def _find_florida_files(self) -> list:
        """Find all Florida raw data files"""
        florida_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return florida_files
        
        # Look for Florida files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'florida' in filename or 'fl' in filename:
                    florida_files.append(str(file_path))
        
        logger.info(f"Found {len(florida_files)} Florida files: {florida_files}")
        return florida_files
    
    def _extract_election_year_from_filename(self, file_path: str) -> str:
        """
        Extract election year from Florida file name
        
        Args:
            file_path: Path to the file
            
        Returns:
            str: Election year (e.g., '2024', '2020', etc.)
        """
        filename = os.path.basename(file_path).lower()
        
        # Look for 4-digit year patterns in filename
        year_patterns = [
            r'(\d{4})',  # Any 4-digit number
            r'(\d{4})_',  # 4-digit number followed by underscore
            r'_(\d{4})',  # Underscore followed by 4-digit number
            r'(\d{4})-',  # 4-digit number followed by dash
            r'-(\d{4})',  # Dash followed by 4-digit number
        ]
        
        for pattern in year_patterns:
            match = re.search(pattern, filename)
            if match:
                year = match.group(1)
                # Validate it's a reasonable election year (2000-2030)
                if 2000 <= int(year) <= 2030:
                    return year
        
        # If no year found, try to extract from ElectionID field
        # This is a fallback for when the filename doesn't contain the year
        return None
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single Florida file
        
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
        elif file_ext == '.txt':
            return self._extract_from_txt(file_path)
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
            # Try different encodings
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
    
    def _extract_from_txt(self, file_path: str) -> list:
        """Extract data from tab-separated text file"""
        try:
            # Try different encodings for text files
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, sep='\t')
                    logger.info(f"Loaded text file with {len(df)} rows and {len(df.columns)} columns using {encoding}")
                    return self._process_dataframe(df, file_path)
                except UnicodeDecodeError:
                    continue
            logger.error(f"Failed to read text file {file_path} with any encoding")
            return []
        except Exception as e:
            logger.error(f"Failed to read text file {file_path}: {e}")
            return []
    
    def _process_dataframe(self, df: pd.DataFrame, file_path: str) -> list:
        """
        Process DataFrame and extract structured records
        
        Args:
            df: Raw DataFrame from file
            file_path: Source file path for logging
            
        Returns:
            list: List of structured records
        """
        records = []
        
        # Log column information
        logger.info(f"Columns in {file_path}: {list(df.columns)}")
        
        # Process each row
        for idx, row in df.iterrows():
            try:
                record = self._extract_record_from_row(row, file_path, idx)
                if record:
                    records.append(record)
            except Exception as e:
                logger.warning(f"Failed to process row {idx} in {file_path}: {e}")
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
        # Extract election year from file name
        election_year = self._extract_election_year_from_filename(file_path)
        
        # Initialize record with raw data
        record = {
            'raw_data': row.to_dict(),
            'state': 'Florida',
            'file_source': file_path,
            'row_index': row_idx,
            'election_year': election_year
        }
        
        # Map columns based on Florida's actual data structure
        column_mapping = {
            'candidate_name': ['NameFirst', 'NameMiddle', 'NameLast'],  # Special handling
            'first_name': ['NameFirst'],
            'last_name': ['NameLast'],
            'middle_name': ['NameMiddle'],
            'party': ['PartyCode', 'PartyDesc'],
            'office': ['OfficeCode', 'OfficeDesc'],
            'district': ['Juris1num', 'Juris2num'],
            'county': ['County'],
            'address': ['Addr1', 'Addr2'],
            'city': ['City'],
            'state': ['State'],
            'zip_code': ['Zip'],
            'phone': ['Phone'],
            'email': ['Email'],
            'election_date': ['ElectionID'],
            'status': ['StatusCode', 'StatusDesc'],
            'voter_id': ['VoterID'],
            'account_num': ['AcctNum']
        }
        
        # Extract data using column mapping
        for target_col, possible_source_cols in column_mapping.items():
            if target_col == 'candidate_name':
                # Special handling for candidate name - combine first, middle, last
                name_parts = []
                for source_col in ['NameFirst', 'NameMiddle', 'NameLast']:
                    if source_col in row.index and pd.notna(row[source_col]):
                        name_part = str(row[source_col]).strip()
                        # Clean corrupted name parts (remove numbers, addresses, etc.)
                        if source_col == 'NameLast':
                            # Remove phone numbers and addresses from last name
                            name_part = re.sub(r'\\s+\\d{7,}', '', name_part)  # Remove phone numbers
                            name_part = re.sub(r'\\s+\\d{3,4}\\s+\\w+\\s+\\w+', '', name_part)  # Remove addresses
                            # Remove specific corrupted patterns
                            name_part = re.sub(r'\\s+\\d{7}$', '', name_part)  # Remove trailing 7-digit numbers
                            name_part = re.sub(r'\\s+\\d{4}$', '', name_part)  # Remove trailing 4-digit numbers
                        elif source_col == 'NameMiddle':
                            # Remove corrupted data from middle name
                            name_part = re.sub(r'\\s+[A-Z]\\s+\\d+', '', name_part)  # Remove "N 123" patterns
                            name_part = re.sub(r'\\s+\\d+\\s+\\w+\\s+\\w+', '', name_part)  # Remove address patterns
                            # Remove specific corrupted patterns
                            name_part = re.sub(r'\\s+\\d{7}$', '', name_part)  # Remove trailing 7-digit numbers
                            name_part = re.sub(r'\\s+\\d{4}$', '', name_part)  # Remove trailing 4-digit numbers
                        
                        # Only add if it's not empty and looks like a name
                        if name_part and not re.match(r'^[0-9\\s]+$', name_part):  # Not just numbers
                            # Additional check: if it contains too many numbers, skip it
                            digit_count = len(re.findall(r'\\d', name_part))
                            if digit_count <= 2:  # Allow up to 2 digits (for names like "R19")
                                name_parts.append(name_part)
                
                if name_parts:
                    record[target_col] = ' '.join(name_parts)
            elif target_col == 'party':
                # Use PartyDesc if available, otherwise PartyCode
                party_value = None
                for source_col in possible_source_cols:
                    if source_col in row.index and pd.notna(row[source_col]):
                        party_value = str(row[source_col]).strip()
                        # Prefer PartyDesc over PartyCode
                        if source_col == 'PartyDesc':
                            break
                record[target_col] = party_value
            elif target_col == 'office':
                # Use OfficeDesc if available, otherwise OfficeCode
                office_value = None
                for source_col in possible_source_cols:
                    if source_col in row.index and pd.notna(row[source_col]):
                        office_value = str(row[source_col]).strip()
                        # Prefer OfficeDesc over OfficeCode
                        if source_col == 'OfficeDesc':
                            break
                record[target_col] = office_value
            elif target_col == 'address':
                # Combine Addr1 and Addr2
                addr_parts = []
                for source_col in possible_source_cols:
                    if source_col in row.index and pd.notna(row[source_col]):
                        addr_parts.append(str(row[source_col]).strip())
                if addr_parts:
                    record[target_col] = ' '.join(addr_parts)
            elif target_col == 'district':
                # Only assign districts to legislative offices
                office_code = row.get('OfficeCode', '')
                office_desc = row.get('OfficeDesc', '')
                
                # Check if this is a legislative office
                is_legislative = any(code in str(office_code) for code in ['STR', 'STS', 'USR', 'USS']) or \
                               any(term in str(office_desc).lower() for term in ['representative', 'senator'])
                
                if is_legislative:
                    # Combine Juris1num and Juris2num if both exist
                    district_parts = []
                    for source_col in possible_source_cols:
                        if source_col in row.index and pd.notna(row[source_col]):
                            district_val = str(row[source_col]).strip()
                            # Clean up .0 suffix
                            if district_val.endswith('.0'):
                                district_val = district_val[:-2]
                            district_parts.append(district_val)
                    
                    if district_parts:
                        # For US Senate, mark as statewide
                        if 'USS' in str(office_code) or 'senator' in str(office_desc).lower():
                            record[target_col] = 'Statewide'
                        else:
                            record[target_col] = ' - '.join(district_parts) if len(district_parts) > 1 else district_parts[0]
                else:
                    # Non-legislative offices should not have districts
                    record[target_col] = None
            elif target_col == 'email':
                # Extract email directly from Email field
                if 'Email' in row.index and pd.notna(row['Email']):
                    email_value = str(row['Email']).strip()
                    # Only use if it's not empty and looks like an email
                    if email_value and '@' in email_value:
                        record[target_col] = email_value
                    else:
                        record[target_col] = None
                else:
                    record[target_col] = None
            else:
                # Standard extraction for other fields
                for source_col in possible_source_cols:
                    if source_col in row.index and pd.notna(row[source_col]):
                        record[target_col] = str(row[source_col]).strip()
                        break
        
        # Ensure required fields exist
        required_fields = ['candidate_name', 'party', 'office']
        for field in required_fields:
            if field not in record:
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
