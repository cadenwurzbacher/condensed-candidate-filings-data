#!/usr/bin/env python3
"""
Field Extractor Utility for Structural Cleaners

Provides reusable methods for extracting fields from DataFrame rows based on
column name matching. Eliminates duplication of extraction logic across all
state structural cleaners.
"""

import pandas as pd
import re
from typing import Optional, List


class FieldExtractor:
    """
    Utility class for extracting fields from DataFrame rows.

    This class provides static methods for common field extraction patterns,
    eliminating the need for duplicate _extract_* methods in every state cleaner.
    """

    @staticmethod
    def extract_by_keywords(row: pd.Series, keywords: List[str]) -> Optional[str]:
        """
        Extract a field value by matching column names against keywords.

        Args:
            row: DataFrame row
            keywords: List of keywords to match in column names (case-insensitive)

        Returns:
            First non-empty value found, or None
        """
        matching_columns = [col for col in row.index
                          if any(keyword.lower() in str(col).lower() for keyword in keywords)]

        for col in matching_columns:
            value = row[col]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()

        return None

    @staticmethod
    def extract_candidate_name(row: pd.Series) -> Optional[str]:
        """Extract candidate name from row."""
        return FieldExtractor.extract_by_keywords(row, ['name', 'candidate'])

    @staticmethod
    def extract_office(row: pd.Series) -> Optional[str]:
        """Extract office from row."""
        return FieldExtractor.extract_by_keywords(row, ['office'])

    @staticmethod
    def extract_party(row: pd.Series) -> Optional[str]:
        """Extract party from row."""
        return FieldExtractor.extract_by_keywords(row, ['party'])

    @staticmethod
    def extract_county(row: pd.Series) -> Optional[str]:
        """Extract county from row."""
        return FieldExtractor.extract_by_keywords(row, ['county'])

    @staticmethod
    def extract_district(row: pd.Series) -> Optional[str]:
        """Extract district from row."""
        return FieldExtractor.extract_by_keywords(row, ['district'])

    @staticmethod
    def extract_address(row: pd.Series) -> Optional[str]:
        """Extract address from row."""
        return FieldExtractor.extract_by_keywords(row, ['address', 'addr'])

    @staticmethod
    def extract_city(row: pd.Series) -> Optional[str]:
        """Extract city from row."""
        return FieldExtractor.extract_by_keywords(row, ['city'])

    @staticmethod
    def extract_state(row: pd.Series, default_state: str = None) -> Optional[str]:
        """
        Extract state from row.

        Args:
            row: DataFrame row
            default_state: Default state to return if not found

        Returns:
            State string or default_state
        """
        state = FieldExtractor.extract_by_keywords(row, ['state'])
        return state if state else default_state

    @staticmethod
    def extract_zip_code(row: pd.Series) -> Optional[str]:
        """Extract zip code from row."""
        return FieldExtractor.extract_by_keywords(row, ['zip', 'postal'])

    @staticmethod
    def extract_phone(row: pd.Series) -> Optional[str]:
        """Extract phone from row."""
        return FieldExtractor.extract_by_keywords(row, ['phone', 'tel'])

    @staticmethod
    def extract_email(row: pd.Series) -> Optional[str]:
        """Extract email from row."""
        email = FieldExtractor.extract_by_keywords(row, ['email', 'e-mail'])
        # Validate it looks like an email
        if email and '@' in email:
            return email
        return None

    @staticmethod
    def extract_website(row: pd.Series) -> Optional[str]:
        """Extract website from row."""
        return FieldExtractor.extract_by_keywords(row, ['website', 'web', 'url'])

    @staticmethod
    def extract_facebook(row: pd.Series) -> Optional[str]:
        """Extract Facebook from row."""
        return FieldExtractor.extract_by_keywords(row, ['facebook', 'fb'])

    @staticmethod
    def extract_twitter(row: pd.Series) -> Optional[str]:
        """Extract Twitter/X from row."""
        return FieldExtractor.extract_by_keywords(row, ['twitter', 'x'])

    @staticmethod
    def extract_filing_date(row: pd.Series) -> Optional[str]:
        """Extract filing date from row."""
        return FieldExtractor.extract_by_keywords(row, ['filing', 'date', 'filed'])

    @staticmethod
    def extract_election_type(row: pd.Series) -> Optional[str]:
        """Extract election type from row."""
        return FieldExtractor.extract_by_keywords(row, ['type', 'election_type'])

    @staticmethod
    def extract_election_year(row: pd.Series, file_path: str = None) -> Optional[str]:
        """
        Extract election year from row or filename.

        Args:
            row: DataFrame row
            file_path: Optional file path to extract year from filename

        Returns:
            Election year string or None
        """
        # Try to get from row first
        year_value = FieldExtractor.extract_by_keywords(row, ['year', 'election'])

        if year_value:
            # Try to extract year from value
            year_match = re.search(r'20\d{2}', str(year_value))
            if year_match:
                return year_match.group()

        # Try to extract from filename if provided
        if file_path:
            year_match = re.search(r'20\d{2}', file_path)
            if year_match:
                return year_match.group()

        return None

    @staticmethod
    def combine_fields(row: pd.Series, field_columns: List[str], separator: str = ' ') -> Optional[str]:
        """
        Combine multiple fields into one.

        Args:
            row: DataFrame row
            field_columns: List of exact column names to combine
            separator: String to join fields with

        Returns:
            Combined string or None
        """
        parts = []
        for col in field_columns:
            if col in row.index and pd.notna(row[col]):
                value = str(row[col]).strip()
                if value:
                    parts.append(value)

        return separator.join(parts) if parts else None
