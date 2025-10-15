#!/usr/bin/env python3
"""
Name Parser Utility for State Cleaners

Provides standardized name parsing functionality to eliminate duplicate
name parsing logic across all state cleaners. Handles common patterns for:
- Prefix extraction (Dr., Mr., Mrs., etc.)
- Suffix extraction (Jr., Sr., II, III, etc.)
- Nickname extraction (from quotes)
- First/Middle/Last name splitting
"""

import pandas as pd
import re
from typing import Tuple, Optional


class NameParser:
    """
    Utility class for parsing candidate names into components.

    This eliminates hundreds of lines of duplicate name parsing logic
    across all state cleaners.
    """

    # Common prefixes (case-insensitive)
    PREFIXES = [
        'Dr', 'Mr', 'Mrs', 'Ms', 'Miss', 'Prof', 'Rev', 'Hon',
        'Sen', 'Rep', 'Gov', 'Lt', 'Col', 'Gen', 'Adm', 'Capt',
        'Maj', 'Sgt', 'Cpl', 'Pvt'
    ]

    # Common suffixes (case-insensitive)
    SUFFIXES = [
        'Jr', 'Sr', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X'
    ]

    @staticmethod
    def parse_name(name: str) -> Tuple[Optional[str], Optional[str], Optional[str],
                                       Optional[str], Optional[str], Optional[str], Optional[str]]:
        """
        Parse a candidate name into components.

        Args:
            name: Full candidate name string

        Returns:
            Tuple of (first_name, middle_name, last_name, prefix, suffix, nickname, display_name)
        """
        if pd.isna(name) or not name:
            return None, None, None, None, None, None, None

        # Clean the name
        name = str(name).strip().strip('"\'')
        name = re.sub(r'\s+', ' ', name)

        # Initialize components
        first_name = None
        middle_name = None
        last_name = None
        prefix = None
        suffix = None
        nickname = None

        # Extract nickname (in quotes)
        nickname_match = re.search(r'["""\'\u201c\u201d\u2018\u2019]([^""""\'\u201c\u201d\u2018\u2019]+)["""\'\u201c\u201d\u2018\u2019]', name)
        if nickname_match:
            nickname = nickname_match.group(1)
            # Remove nickname from name for further processing
            name = re.sub(r'["""\'\u201c\u201d\u2018\u2019][^""""\'\u201c\u201d\u2018\u2019]+["""\'\u201c\u201d\u2018\u2019]', '', name).strip()

        # Extract prefix from beginning
        prefix_pattern = r'^(' + '|'.join(NameParser.PREFIXES) + r')\.?\s+'
        prefix_match = re.match(prefix_pattern, name, re.IGNORECASE)
        if prefix_match:
            prefix = prefix_match.group(1)
            name = re.sub(prefix_pattern, '', name, flags=re.IGNORECASE).strip()

        # Extract suffix from end
        suffix_pattern = r'\s*\.?\s*\b(' + '|'.join(NameParser.SUFFIXES) + r')\b\.?'
        suffix_match = re.search(suffix_pattern, name, re.IGNORECASE)
        if suffix_match:
            suffix = suffix_match.group(1)
            name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE).strip()

        # Parse remaining name
        if ',' in name:
            # Handle "Last, First Middle" format
            parts = [p.strip() for p in name.split(',')]
            if len(parts) >= 2:
                last_name = parts[0]
                first_middle = parts[1].split()

                if len(first_middle) == 1:
                    first_name = first_middle[0]
                elif len(first_middle) >= 2:
                    first_name = first_middle[0]
                    middle_name = ' '.join(first_middle[1:])
            else:
                last_name = parts[0]
        else:
            # Handle "First Middle Last" format
            parts = name.split()
            if len(parts) == 1:
                last_name = parts[0]
            elif len(parts) == 2:
                first_name = parts[0]
                last_name = parts[1]
            elif len(parts) >= 3:
                first_name = parts[0]
                last_name = parts[-1]
                middle_name = ' '.join(parts[1:-1])

        # Build display name
        display_parts = []
        if prefix:
            display_parts.append(prefix)
        if first_name:
            display_parts.append(first_name)
        if middle_name:
            display_parts.append(middle_name)
        if last_name:
            display_parts.append(last_name)
        if suffix:
            display_parts.append(suffix)
        if nickname:
            display_parts.append(f'"{nickname}"')

        display_name = ' '.join(display_parts).strip()

        return first_name, middle_name, last_name, prefix, suffix, nickname, display_name

    @staticmethod
    def is_initial(part: str) -> bool:
        """Check if a name part is an initial."""
        if not part:
            return False
        return len(part) == 1 or (len(part) == 2 and part.endswith('.'))

    @staticmethod
    def clean_name(name: str) -> Optional[str]:
        """
        Clean a name string by removing extra whitespace and quotes.

        Args:
            name: Raw name string

        Returns:
            Cleaned name string or None
        """
        if pd.isna(name) or not name:
            return None

        name = str(name).strip().strip('"\'')
        name = re.sub(r'\s+', ' ', name)

        return name if name else None

    @staticmethod
    def parse_dataframe_names(df: pd.DataFrame, name_column: str = 'candidate_name') -> pd.DataFrame:
        """
        Parse all names in a DataFrame.

        Args:
            df: DataFrame with candidate names
            name_column: Column containing the names to parse

        Returns:
            DataFrame with parsed name components added
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None

        # Use name_column as full_name_display if available
        if name_column in df.columns:
            df['full_name_display'] = df[name_column]

        # Parse each name
        for idx, row in df.iterrows():
            name = row.get(name_column)
            if pd.notna(name) and str(name).strip():
                parsed = NameParser.parse_name(name)

                df.at[idx, 'first_name'] = parsed[0]
                df.at[idx, 'middle_name'] = parsed[1]
                df.at[idx, 'last_name'] = parsed[2]
                df.at[idx, 'prefix'] = parsed[3]
                df.at[idx, 'suffix'] = parsed[4]
                df.at[idx, 'nickname'] = parsed[5]
                df.at[idx, 'full_name_display'] = parsed[6]

        return df
