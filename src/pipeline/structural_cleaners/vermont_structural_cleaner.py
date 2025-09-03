import pandas as pd
import logging
import os
from pathlib import Path
import re

logger = logging.getLogger(__name__)

class VermontStructuralCleaner:
    """
    Vermont Structural Cleaner - Phase 1 of new pipeline
    
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
        Extract structured data from Vermont raw files
        
        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info("Starting Vermont structural cleaning")
        
        # Find Vermont raw files
        vermont_files = self._find_vermont_files()
        if not vermont_files:
            logger.warning("No Vermont raw files found")
            return pd.DataFrame()
        
        # Process each file and combine
        all_records = []
        
        for file_path in vermont_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        if not all_records:
            logger.warning("No records extracted from Vermont files")
            return pd.DataFrame()
        
        # Create structured DataFrame
        df = pd.DataFrame(all_records)
        
        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)
        
        logger.info(f"Vermont structural cleaning complete: {len(df)} records")
        return df
    
    def _find_vermont_files(self) -> list:
        """Find all Vermont raw data files"""
        vermont_files = []
        
        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return vermont_files
        
        # Look for Vermont files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if 'vermont' in filename:
                    vermont_files.append(str(file_path))
        
        logger.info(f"Found {len(vermont_files)} Vermont files: {vermont_files}")
        return vermont_files
    
    def _extract_from_file(self, file_path: str) -> list:
        """
        Extract structured data from a single Vermont file
        
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
            # Read the Excel file without headers
            df = pd.read_excel(file_path, header=None)
            logger.info(f"Read Excel file with {len(df)} rows and {len(df.columns)} columns")
            
            # Extract structured data
            return self._extract_structured_data(df)
            
        except Exception as e:
            logger.error(f"Failed to read Excel file {file_path}: {e}")
            return []
    
    def _extract_structured_data(self, df: pd.DataFrame) -> list:
        """Extract structured records from DataFrame"""
        # Build a normalized DataFrame by detecting header rows (contain 'Contest')
        normalized_df = self._normalize_multisection_sheet(df)
        if normalized_df.empty:
            return []
        
        # Map normalized DataFrame columns to expected fields and build records
        records: list[dict] = []
        for _, row in normalized_df.iterrows():
            try:
                record = {
                    'candidate_name': self._safe_str(row.get('Name On Ballot')),
                    'office': self._safe_str(row.get('Contest')),
                    'party': self._safe_str(row.get('Party')),
                    'county': self._safe_str(row.get('District Name')),
                    'district': None,  # VT statewide primary sheet generally not district-numbered; keep None
                    'address': self._compose_address(row),
                    'city': self._safe_str(row.get('City')),
                    'state': self._safe_str(row.get('State')) or 'VT',
                    'zip_code': self._safe_str(row.get('Zip')),
                    'phone': self._choose_phone(row.get('Day Time Phone'), row.get('Evening Phone')),
                    'email': self._safe_str(row.get('Email')),
                    'facebook': None,
                    'twitter': None,
                    'filing_date': None,
                    'election_year': self._infer_year_from_sheet(df) or '2024',
                    'election_type': self._infer_election_type_from_context(df, row),
                    'address_state': 'Vermont',
                    'raw_data': self._safe_str(dict(row))
                }
                # Only append rows that clearly have a candidate and office
                if record['candidate_name'] and record['office'] and record['office'] != 'Contest':
                    records.append(record)
            except Exception as e:
                logger.warning(f"Failed to extract record from normalized row: {e}")
                continue
        return records
    
    def _clean_dataframe_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame structure (remove empty rows/columns, reset index)"""
        # Remove completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        # Reset index
        df = df.reset_index(drop=True)
        
        return df

    def _normalize_multisection_sheet(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Detect repeated header rows (contain 'Contest') and stack sections into one DataFrame with proper headers.
        The VT sheet has introductory rows, then a header row with columns like
        ['Contest','District Name','Name On Ballot',...]. Later sections may repeat headers.
        """
        df = self._clean_dataframe_structure(raw_df)
        if df.empty:
            return pd.DataFrame()
        
        # Identify header rows by presence of the word 'Contest' in any cell
        header_row_idxs = df.index[
            df.apply(lambda r: r.astype(str).str.fullmatch(r"\s*Contest\s*", case=False).any(), axis=1)
        ].tolist()
        # Fallback: rows that contain 'Contest' anywhere
        if not header_row_idxs:
            header_row_idxs = df.index[
                df.apply(lambda r: r.astype(str).str.contains('Contest', case=False, na=False).any(), axis=1)
            ].tolist()
        
        if not header_row_idxs:
            logger.warning("Vermont: No header rows found (no 'Contest' markers).")
            return pd.DataFrame()
        
        # Create sections between header rows
        sections: list[pd.DataFrame] = []
        for i, h in enumerate(header_row_idxs):
            next_h = header_row_idxs[i + 1] if i + 1 < len(header_row_idxs) else len(df)
            header = df.iloc[h].copy()
            # Ensure headers are strings and stripped
            header = header.apply(lambda x: str(x).strip() if pd.notna(x) else x)
            # Slice data rows after header until next header
            data = df.iloc[h + 1:next_h].copy()
            if data.empty:
                continue
            # Assign columns from header; pad/trim length to match
            cols = list(header)
            # Some trailing columns may be NaN; drop them to keep alignment
            while cols and (cols[-1] is None or (isinstance(cols[-1], float) and pd.isna(cols[-1])) or str(cols[-1]).strip() == ''):
                cols.pop()
            data = data.iloc[:, :len(cols)].copy()
            data.columns = cols
            # Drop rows that are entirely NaN after reheader
            data = data.dropna(how='all')
            # Filter out rows that look like header repeats or separators
            if 'Contest' in data.columns:
                data = data[data['Contest'].astype(str).str.strip().ne('Contest')]
            sections.append(data)
        
        if not sections:
            return pd.DataFrame()
        stacked = pd.concat(sections, ignore_index=True)
        # Final cleanup: remove entirely empty columns and trim whitespace
        stacked = stacked.dropna(how='all', axis=1)
        for col in stacked.columns:
            stacked[col] = stacked[col].apply(lambda v: v.strip() if isinstance(v, str) else v)
        return stacked
    
    def _is_valid_candidate_row(self, row: pd.Series) -> bool:
        """Check if a row contains valid candidate data"""
        # Check if we have at least an office and candidate info
        office = str(row.get(0, '')).strip()
        candidate_info = str(row.get(13, '')).strip()
        
        return (bool(office and office != 'nan' and office != 'Contest' and 'Last Updated' not in office) and
                bool(candidate_info and candidate_info != 'nan'))
    
    def _extract_single_record(self, row: pd.Series) -> dict:
        """Extract a single candidate record from a row"""
        try:
            record = {
                'candidate_name': self._extract_candidate_name(row),
                'office': self._extract_office(row),
                'party': self._extract_party(row),
                'county': self._extract_county(row),
                'district': self._extract_district(row),
                'address': None,  # Vermont doesn't have address info
                'city': None,  # Vermont doesn't have city info
                'state': 'Vermont',
                'zip_code': None,  # Vermont doesn't have zip code info
                'phone': None,  # Vermont doesn't have phone info
                'email': None,
                'website': None,  # Vermont doesn't have email info
                'filing_date': None,  # Vermont doesn't have filing date info
                'election_year': '2024',  # Default to 2024 based on filename
                'election_type': self._extract_election_type(row),
                'address_state': 'Vermont',
                'raw_data': str(row.to_dict())  # Store original row data
            }
            
            return record
            
        except Exception as e:
            logger.warning(f"Failed to extract record from row: {e}")
            return None

    def _safe_str(self, value) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        s = str(value).strip()
        return s if s and s.lower() != 'nan' else None

    def _choose_phone(self, day: object, eve: object) -> str:
        day_s = self._safe_str(day)
        eve_s = self._safe_str(eve)
        return day_s or eve_s

    def _compose_address(self, row: pd.Series) -> str:
        # Vermont has address components in separate columns
        address_parts = []
        
        # Check for street address in various possible column names
        street_address = self._safe_str(row.get('Address')) or self._safe_str(row.get('Street Address')) or self._safe_str(row.get('Mailing Address'))
        if street_address:
            address_parts.append(street_address)
        
        # Add city if available
        city = self._safe_str(row.get('City'))
        if city:
            address_parts.append(city)
        
        # Add state if available
        state = self._safe_str(row.get('State'))
        if state:
            address_parts.append(state)
        
        # Add zip if available
        zip_code = self._safe_str(row.get('Zip'))
        if zip_code:
            address_parts.append(zip_code)
        
        addr = ', '.join([p for p in address_parts if p]) if address_parts else None
        return addr or None

    def _infer_year_from_sheet(self, raw_df: pd.DataFrame) -> str:
        # Try to detect a 4-digit year in the first column header banner
        try:
            first_rows = raw_df.iloc[:5, :].astype(str).values.flatten().tolist()
            for cell in first_rows:
                m = re.search(r'\b(19|20)\d{2}\b', cell)
                if m:
                    return m.group(0)
        except Exception:
            pass
        return None

    def _infer_election_type_from_context(self, raw_df: pd.DataFrame, row: pd.Series) -> str:
        # Prefer detecting from banners in the sheet; fall back to candidate info
        try:
            first_col = raw_df.iloc[:, 0].astype(str).str.lower()
            if first_col.str.contains('primary', na=False).any():
                return 'Primary'
            if first_col.str.contains('general', na=False).any():
                return 'General'
        except Exception:
            pass
        # Fallback
        return 'Primary'
    
    def _extract_candidate_name(self, row: pd.Series) -> str:
        """Extract candidate name from row"""
        # Candidate info is in column 13 in format like "BLAIS_MARIELLE_GOV_2024AUGPRIMARY_..."
        candidate_info = str(row.get(13, '')).strip()
        if candidate_info and candidate_info != 'nan':
            # Extract name from the filename format
            # Format: LASTNAME_FIRSTNAME_OFFICE_ELECTION_...
            parts = candidate_info.split('_')
            if len(parts) >= 2:
                last_name = parts[0]
                first_name = parts[1]
                return f"{first_name} {last_name}"
            elif len(parts) == 1:
                return parts[0]
        return None
    
    def _extract_office(self, row: pd.Series) -> str:
        """Extract office from row"""
        office = str(row.get(0, '')).strip()
        if office and office != 'nan' and office != 'Contest' and 'Last Updated' not in office:
            return office
        return None
    
    def _extract_party(self, row: pd.Series) -> str:
        """Extract party from row"""
        # Vermont doesn't have explicit party information
        # Could be inferred from candidate info or other sources
        return None
    
    def _extract_county(self, row: pd.Series) -> str:
        """Extract county from row"""
        county = row.get(1)
        if pd.notna(county):
            county_str = str(county).strip()
            if county_str and county_str != 'nan':
                return county_str
        return None
    
    def _extract_district(self, row: pd.Series) -> str:
        """Extract district from row"""
        # District info might be in column 1 along with county
        district_info = row.get(1)
        if pd.notna(district_info):
            district_str = str(district_info).strip()
            if district_str and district_str != 'nan':
                # Look for district patterns like "ADD 1", "CHI 1", etc.
                district_match = re.search(r'([A-Z]+)\s*(\d+)', district_str)
                if district_match:
                    return f"{district_match.group(1)} {district_match.group(2)}"
                # If it's just a number, return it
                elif re.match(r'^\d+$', district_str):
                    return district_str
        return None
    
    def _extract_election_type(self, row: pd.Series) -> str:
        """Extract election type from row"""
        # Try to extract from candidate info
        candidate_info = str(row.get(13, '')).strip()
        if candidate_info and candidate_info != 'nan':
            candidate_lower = candidate_info.lower()
            if 'primary' in candidate_lower:
                return 'Primary'
            elif 'general' in candidate_lower:
                return 'General'
            elif 'special' in candidate_lower:
                return 'Special'
        
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
