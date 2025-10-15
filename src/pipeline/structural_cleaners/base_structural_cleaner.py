#!/usr/bin/env python3
"""
Base Structural Cleaner for CandidateFilings.com Data Processing

This abstract base class eliminates massive code duplication across all state
structural cleaners by providing common functionality for file discovery,
data extraction, and DataFrame processing.
"""

import pandas as pd
import logging
import os
import re
from pathlib import Path
from abc import ABC, abstractmethod
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class BaseStructuralCleaner(ABC):
    """
    Abstract base class for state-specific structural cleaners.

    This class provides common functionality for:
    - Finding state-specific raw files
    - Reading Excel, CSV, and text files
    - Extracting structured data from DataFrames
    - Ensuring consistent column structure

    Subclasses only need to implement state-specific column mapping logic.
    """

    # Standard expected columns for all structural cleaners
    STANDARD_COLUMNS = [
        'raw_data', 'state', 'file_source', 'row_index', 'election_year',
        'candidate_name', 'first_name', 'last_name', 'middle_name',
        'prefix', 'suffix', 'party', 'office', 'district', 'county',
        'address', 'city', 'zip_code', 'email', 'website', 'phone',
        'facebook', 'twitter', 'filing_date', 'election_date',
        'election_type', 'address_state'
    ]

    def __init__(self, state_name: str, state_identifiers: List[str], data_dir: str = "data"):
        """
        Initialize the base structural cleaner.

        Args:
            state_name: Full name of the state (e.g., "Alaska")
            state_identifiers: List of strings to match in filenames (e.g., ['alaska', 'ak'])
            data_dir: Root data directory
        """
        self.state_name = state_name
        self.state_identifiers = [s.lower() for s in state_identifiers]
        self.data_dir = data_dir
        self.raw_dir = os.path.join(data_dir, "raw")
        self.structured_dir = os.path.join(data_dir, "structured")

    def clean(self) -> pd.DataFrame:
        """
        Extract structured data from state raw files.

        Returns:
            pd.DataFrame: Structured data with consistent columns
        """
        logger.info(f"Starting {self.state_name} structural cleaning")

        # Find state raw files
        state_files = self._find_state_files()
        if not state_files:
            logger.warning(f"No {self.state_name} raw files found")
            return pd.DataFrame()

        # Process each file and combine
        all_records = []

        for file_path in state_files:
            try:
                logger.info(f"Processing structural file: {file_path}")
                file_records = self._extract_from_file(file_path)
                all_records.extend(file_records)
                logger.info(f"Extracted {len(file_records)} records from {file_path}")
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue

        if not all_records:
            logger.warning(f"No records extracted from {self.state_name} files")
            return pd.DataFrame()

        # Create structured DataFrame
        df = pd.DataFrame(all_records)

        # Ensure consistent column structure
        df = self._ensure_consistent_columns(df)

        logger.info(f"{self.state_name} structural cleaning complete: {len(df)} records")
        return df

    def _find_state_files(self) -> List[str]:
        """
        Find all state raw data files.

        Returns:
            List of file paths matching state identifiers
        """
        state_files = []

        if not os.path.exists(self.raw_dir):
            logger.warning(f"Raw data directory not found: {self.raw_dir}")
            return state_files

        # Look for state files (case insensitive)
        for file_path in Path(self.raw_dir).rglob("*"):
            if file_path.is_file():
                filename = file_path.name.lower()
                if any(identifier in filename for identifier in self.state_identifiers):
                    state_files.append(str(file_path))

        logger.info(f"Found {len(state_files)} {self.state_name} files: {state_files}")
        return state_files

    def _extract_from_file(self, file_path: str) -> List[Dict]:
        """
        Extract structured data from a single file.

        Args:
            file_path: Path to the raw file

        Returns:
            List of dictionaries with extracted data
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

    def _extract_from_excel(self, file_path: str) -> List[Dict]:
        """Extract data from Excel file."""
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

            # Process the DataFrame
            return self._process_dataframe(df, file_path)

        except Exception as e:
            logger.error(f"Failed to read Excel file {file_path}: {e}")
            return []

    def _extract_from_csv(self, file_path: str) -> List[Dict]:
        """Extract data from CSV file."""
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

    def _extract_from_txt(self, file_path: str) -> List[Dict]:
        """Extract data from tab-separated text file."""
        try:
            # Try different encodings
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

    def _find_main_data_sheet(self, excel_file: pd.ExcelFile) -> Optional[str]:
        """Find the sheet containing the main candidate data."""
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
        """Check if a DataFrame looks like it contains candidate data."""
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

        # If we find at least 2 indicators, this looks like candidate data
        return matches >= 2

    def _process_dataframe(self, df: pd.DataFrame, file_path: str) -> List[Dict]:
        """
        Process DataFrame and extract structured records.

        Args:
            df: Raw DataFrame from file
            file_path: Source file path for logging

        Returns:
            List of structured records
        """
        if df.empty:
            return []

        # Clean up the DataFrame structure
        df = self._clean_dataframe_structure(df)

        # Log column information
        logger.info(f"Columns in {file_path}: {list(df.columns)}")

        # Extract records
        records = []

        for idx, row in df.iterrows():
            try:
                record = self._extract_record_from_row(row, file_path, idx)
                if record:
                    records.append(record)
            except Exception as e:
                logger.debug(f"Failed to extract record {idx}: {e}")
                continue

        return records

    def _clean_dataframe_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean up DataFrame structure without transforming data."""
        # Remove completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')

        # Reset index
        df = df.reset_index(drop=True)

        # Clean column names
        df.columns = [str(col).strip() if pd.notna(col) else f"col_{i}"
                     for i, col in enumerate(df.columns)]

        return df

    def _is_valid_candidate_row(self, row: pd.Series) -> bool:
        """Check if a row contains valid candidate data."""
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

    def _extract_election_year_from_filename(self, file_path: str) -> Optional[str]:
        """
        Extract election year from filename.

        Args:
            file_path: Path to the file

        Returns:
            Election year string or None
        """
        filename = os.path.basename(file_path).lower()

        # Look for 4-digit year patterns
        year_match = re.search(r'20\d{2}', filename)
        if year_match:
            year = year_match.group()
            # Validate it's a reasonable election year (2000-2030)
            if 2000 <= int(year) <= 2030:
                return year

        return None

    @abstractmethod
    def _extract_record_from_row(self, row: pd.Series, file_path: str, row_idx: int) -> Optional[Dict]:
        """
        Extract a structured record from a DataFrame row.

        This is the only method that subclasses MUST implement, as it contains
        state-specific column mapping logic.

        Args:
            row: DataFrame row
            file_path: Source file path
            row_idx: Row index for logging

        Returns:
            Dictionary with structured record or None if invalid
        """
        pass

    def _ensure_consistent_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure DataFrame has consistent column structure.

        Args:
            df: DataFrame to standardize

        Returns:
            DataFrame with consistent columns
        """
        # Add missing columns
        for col in self.STANDARD_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # Reorder columns to match standard structure
        df = df[self.STANDARD_COLUMNS]

        logger.info(f"Ensured consistent column structure: {list(df.columns)}")
        return df
