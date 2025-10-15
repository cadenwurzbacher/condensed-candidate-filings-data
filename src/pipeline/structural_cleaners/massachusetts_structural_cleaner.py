#!/usr/bin/env python3
"""
Massachusetts Structural Cleaner - Refactored

Reduced from 300-500+ lines to ~80 lines by using BaseStructuralCleaner!
Only Massachusetts-specific column mapping logic remains.
"""

import pandas as pd
from typing import Dict, Optional
from .base_structural_cleaner import BaseStructuralCleaner
from .field_extractor import FieldExtractor


class MassachusettsStructuralCleaner(BaseStructuralCleaner):
    """
    Massachusetts Structural Cleaner - Phase 1 of new pipeline

    Refactored to use BaseStructuralCleaner, eliminating massive code duplication!
    Only state-specific column mapping logic remains.
    """

    def __init__(self, data_dir: str = "data"):
        super().__init__(
            state_name="Massachusetts",
            state_identifiers=['massachusetts', 'ma'],
            data_dir=data_dir
        )

    def _extract_record_from_row(self, row: pd.Series, file_path: str, row_idx: int) -> Optional[Dict]:
        """
        Extract a structured record from a DataFrame row (Massachusetts-specific).

        Args:
            row: DataFrame row
            file_path: Source file path
            row_idx: Row index for logging

        Returns:
            Dictionary with structured record or None if invalid
        """
        # Skip rows that don't look like candidate data
        if not self._is_valid_candidate_row(row):
            return None

        # Extract election year from filename
        election_year = self._extract_election_year_from_filename(file_path)

        # Build record using FieldExtractor utility
        record = {
            'candidate_name': FieldExtractor.extract_candidate_name(row),
            'office': FieldExtractor.extract_office(row),
            'party': FieldExtractor.extract_party(row),
            'county': FieldExtractor.extract_county(row),
            'district': FieldExtractor.extract_district(row),
            'address': FieldExtractor.extract_address(row),
            'city': FieldExtractor.extract_city(row),
            'state': FieldExtractor.extract_state(row, default_state="Massachusetts"),
            'zip_code': FieldExtractor.extract_zip_code(row),
            'phone': FieldExtractor.extract_phone(row),
            'email': FieldExtractor.extract_email(row),
            'website': FieldExtractor.extract_website(row),
            'facebook': FieldExtractor.extract_facebook(row),
            'twitter': FieldExtractor.extract_twitter(row),
            'filing_date': FieldExtractor.extract_filing_date(row),
            'election_year': election_year,
            'election_type': FieldExtractor.extract_election_type(row),
            'address_state': FieldExtractor.extract_state(row, default_state="Massachusetts"),
            'raw_data': row.to_dict(),
            'file_source': file_path,
            'row_index': row_idx
        }

        # Only return records with essential data
        if record['candidate_name'] and record['office']:
            return record

        return None
