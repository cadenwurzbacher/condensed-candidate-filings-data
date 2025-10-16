#!/usr/bin/env python3
"""
New York State Cleaner

Handles 
New York-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class NewYorkCleaner(BaseStateCleaner):
    """
    New York-specific data cleaner.
    
    This cleaner handles New York-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("New York")
        
        # County mappings removed - not needed
        
        # New York-specific office mappings
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean New York-specific data structure.
        
        This handles:
        - Name parsing (New York has specific name formats)
        - Address parsing (New York address structure)
        - District parsing (New York district numbering)
        - Borough-specific logic (NYC has unique structure)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with New York-specific structure cleaned
        """
        self.logger.info("Cleaning New York-specific data structure")
        
        # Clean candidate names (New York-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_new_york_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (New York-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_new_york_district)
        
        # Handle New York-specific borough logic
        df = self._handle_new_york_borough_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean New York-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - New York-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with New York-specific content cleaned
        """
        self.logger.info("Cleaning New York-specific content")
        
        # County standardization removed - not needed
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse candidate names into components using base class standard parsing."""
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None

        for idx, row in df.iterrows():
            candidate_name = row.get('candidate_name')
            if pd.notna(candidate_name) and str(candidate_name).strip():
                first, middle, last, prefix, suffix, nickname = self._parse_name_parts(candidate_name)
                df.at[idx, 'first_name'] = first
                df.at[idx, 'middle_name'] = middle
                df.at[idx, 'last_name'] = last
                df.at[idx, 'prefix'] = prefix
                df.at[idx, 'suffix'] = suffix
                df.at[idx, 'nickname'] = nickname
                df.at[idx, 'full_name_display'] = self._build_display_name(first, middle, last, prefix, suffix, nickname)

        return df
    
    def _clean_new_york_name(self, name: str) -> str:
        """Clean New York name formats using standard base class logic."""
        return self._standard_name_cleaning(name)

    def _clean_new_york_district(self, district: str) -> str:
        """Clean New York district - just basic cleanup."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()

    def _handle_new_york_borough_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle New York-specific borough logic (currently a pass-through)."""
        return df
