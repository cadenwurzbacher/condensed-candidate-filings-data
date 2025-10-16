#!/usr/bin/env python3
"""
Colorado State Cleaner

Handles 
Colorado-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class ColoradoCleaner(BaseStateCleaner):
    """
    Colorado-specific data cleaner.
    
    This cleaner handles Colorado-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Colorado")
        
        # County mappings removed - not needed
        
        # Colorado-specific office mappings
        
        # Colorado-specific office mappings
        # Office mappings removed - handled by national standards
    
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Colorado-specific data structure.
        
        This handles:
        - Name parsing (Colorado has specific name formats)
        - Address parsing (Colorado address structure)
        - District parsing (Colorado district numbering)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Colorado-specific structure cleaned
        """
        self.logger.info("Cleaning Colorado-specific data structure")
        
        # Clean candidate names (Colorado-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_colorado_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Colorado-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_colorado_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Colorado-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Colorado-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Colorado-specific content cleaned
        """
        self.logger.info("Cleaning Colorado-specific content")
        
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
    
    def _clean_colorado_name(self, name: str) -> str:
        """Clean Colorado name formats using standard base class logic."""
        return self._standard_name_cleaning(name)

    def _clean_colorado_district(self, district: str) -> str:
        """Clean Colorado district - just basic cleanup."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
