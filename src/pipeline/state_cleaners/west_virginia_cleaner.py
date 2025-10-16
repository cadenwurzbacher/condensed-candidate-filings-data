#!/usr/bin/env python3
"""
West Virginia State Cleaner

Handles 
West Virginia-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class WestVirginiaCleaner(BaseStateCleaner):
    """
    West Virginia-specific data cleaner.
    
    This cleaner handles West Virginia-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("West Virginia")
        
        # County mappings removed - not needed
        
        # West Virginia-specific office mappings
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean West Virginia-specific data structure.
        
        This handles:
        - Name parsing (West Virginia has specific name formats)
        - Address parsing (West Virginia address structure)
        - District parsing (West Virginia district numbering)
        - Mountain State-specific logic (West Virginia has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with West Virginia-specific structure cleaned
        """
        self.logger.info("Cleaning West Virginia-specific data structure")
        
        # Clean candidate names (West Virginia-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_west_virginia_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (West Virginia-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_west_virginia_district)
        
        # Handle West Virginia-specific Mountain State logic
        df = self._handle_west_virginia_mountain_state_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean West Virginia-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - West Virginia-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with West Virginia-specific content cleaned
        """
        self.logger.info("Cleaning West Virginia-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # West Virginia-specific formatting
        df = self._apply_west_virginia_formatting(df)
        
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
    
    def _clean_west_virginia_name(self, name: str) -> str:
        """
        Clean West Virginia-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # West Virginia-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common West Virginia name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_west_virginia_address method removed - now handled centrally
    
    def _clean_west_virginia_district(self, district: str) -> str:
        """
        Clean West Virginia-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # West Virginia-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common West Virginia district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
    def _handle_west_virginia_mountain_state_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle West Virginia-specific Mountain State logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # West Virginia-specific Mountain State handling logic
        # (e.g., Mountain State-specific processing, special district handling)
        
        return df
    
    def _apply_west_virginia_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply West Virginia-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # West Virginia-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
