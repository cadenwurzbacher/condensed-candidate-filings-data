import pandas as pd
import logging
import os
from pathlib import Path
import re
from typing import Optional

logger = logging.getLogger(__name__)


class BaseStructuralCleaner:
    """
    Base class for structural cleaners - Phase 1 of pipeline

    Provides common functionality for extracting structured data from raw files.
    State-specific cleaners should inherit from this class.
    """

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.raw_dir = os.path.join(data_dir, "raw")
        self.structured_dir = os.path.join(data_dir, "structured")

    def _extract_field_by_column_name(self, row: pd.Series, keywords: list[str]) -> Optional[str]:
        """
        Generic method to extract a field by searching for column names containing keywords.

        Args:
            row: DataFrame row
            keywords: List of keywords to search for in column names

        Returns:
            Extracted value or None
        """
        matching_columns = [
            col for col in row.index
            if any(keyword in str(col).lower() for keyword in keywords)
        ]

        for col in matching_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()

        return None

    def _extract_candidate_name(self, row: pd.Series) -> Optional[str]:
        """Extract candidate name from row"""
        result = self._extract_field_by_column_name(row, ['name'])

        if result:
            return result

        # If no name column found, try first non-empty column
        for col in row.index:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()

        return None

    def _extract_office(self, row: pd.Series) -> Optional[str]:
        """Extract office from row"""
        return self._extract_field_by_column_name(row, ['office'])

    def _extract_party(self, row: pd.Series) -> Optional[str]:
        """Extract party from row"""
        return self._extract_field_by_column_name(row, ['party'])

    def _extract_county(self, row: pd.Series) -> Optional[str]:
        """Extract county from row"""
        return self._extract_field_by_column_name(row, ['county'])

    def _extract_district(self, row: pd.Series) -> Optional[str]:
        """Extract district from row"""
        return self._extract_field_by_column_name(row, ['district'])

    def _extract_address(self, row: pd.Series) -> Optional[str]:
        """Extract address from row"""
        return self._extract_field_by_column_name(row, ['address'])

    def _extract_city(self, row: pd.Series) -> Optional[str]:
        """Extract city from row"""
        return self._extract_field_by_column_name(row, ['city'])

    def _extract_state(self, row: pd.Series) -> Optional[str]:
        """Extract state from row"""
        return self._extract_field_by_column_name(row, ['state'])

    def _extract_zip_code(self, row: pd.Series) -> Optional[str]:
        """Extract zip code from row"""
        return self._extract_field_by_column_name(row, ['zip'])

    def _extract_phone(self, row: pd.Series) -> Optional[str]:
        """Extract phone from row"""
        return self._extract_field_by_column_name(row, ['phone'])

    def _extract_email(self, row: pd.Series) -> Optional[str]:
        """Extract email from row"""
        return self._extract_field_by_column_name(row, ['email'])

    def _extract_website(self, row: pd.Series) -> Optional[str]:
        """Extract website from row"""
        return self._extract_field_by_column_name(row, ['website', 'web'])

    def _extract_facebook(self, row: pd.Series) -> Optional[str]:
        """Extract Facebook from row"""
        return self._extract_field_by_column_name(row, ['facebook'])

    def _extract_twitter(self, row: pd.Series) -> Optional[str]:
        """Extract Twitter from row"""
        return self._extract_field_by_column_name(row, ['twitter'])

    def _extract_filing_date(self, row: pd.Series) -> Optional[str]:
        """Extract filing date from row"""
        return self._extract_field_by_column_name(row, ['date', 'filing'])

    def _extract_election_year(self, row: pd.Series, file_path: str) -> Optional[str]:
        """Extract election year from row or filename"""
        # Try to get from row first
        result = self._extract_field_by_column_name(row, ['year', 'election'])

        if result:
            # Try to extract year from value
            year_match = re.search(r'20\d{2}', str(result))
            if year_match:
                return year_match.group()

        # Try to extract from filename
        filename = Path(file_path).name
        year_match = re.search(r'20\d{2}', filename)
        if year_match:
            return year_match.group()

        return None

    def _extract_election_type(self, row: pd.Series) -> Optional[str]:
        """Extract election type from row"""
        return self._extract_field_by_column_name(row, ['type', 'election'])

    def _extract_address_state(self, row: pd.Series) -> Optional[str]:
        """Extract address state from row"""
        return self._extract_field_by_column_name(row, ['address_state'])

    def _ensure_consistent_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure DataFrame has consistent column structure"""
        expected_columns = [
            'candidate_name', 'office', 'party', 'county', 'district',
            'address', 'city', 'state', 'zip_code', 'phone', 'email', 'website',
            'facebook', 'twitter', 'filing_date', 'election_year', 'election_type',
            'address_state', 'raw_data'
        ]

        # Add missing columns with None values
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        # Reorder columns to match expected structure
        df = df[expected_columns]

        return df

    def _clean_dataframe_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean up DataFrame structure without transforming data"""
        # Remove completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')

        # Reset index
        df = df.reset_index(drop=True)

        # Clean column names
        df.columns = [
            str(col).strip() if pd.notna(col) else f"col_{i}"
            for i, col in enumerate(df.columns)
        ]

        return df

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
        matches = sum(
            1 for indicator in candidate_indicators
            if any(indicator in col for col in column_names)
        )

        # If we find at least 2-3 indicators, this looks like candidate data
        return matches >= 2
