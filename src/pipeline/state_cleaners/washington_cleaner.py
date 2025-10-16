#!/usr/bin/env python3
"""
Washington State Cleaner

Handles 
Washington-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class WashingtonCleaner(BaseStateCleaner):
    """
    Washington-specific data cleaner.
    
    This cleaner handles Washington-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Washington")
        
        # County mappings removed - not needed
        
        # Washington-specific office mappings
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Washington-specific data structure.
        
        This handles:
        - Name parsing (Washington has specific name formats)
        - Address parsing (Washington address structure)
        - District parsing (Washington district numbering)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Washington-specific structure cleaned
        """
        self.logger.info("Cleaning Washington-specific data structure")
        
        # Clean candidate names (Washington-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_washington_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Washington-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_washington_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Washington-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Washington-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Washington-specific content cleaned
        """
        self.logger.info("Cleaning Washington-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        

        
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
    
    def _clean_washington_name(self, name: str) -> str:
        """Clean Washington name formats using standard base class logic."""
        return self._standard_name_cleaning(name)
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_washington_address method removed - now handled centrally
    
    def _clean_washington_district(self, district: str) -> str:
        """Clean Washington district - just basic cleanup."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()

