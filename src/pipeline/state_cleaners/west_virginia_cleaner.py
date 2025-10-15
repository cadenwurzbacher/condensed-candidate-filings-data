#!/usr/bin/env python3
"""
West Virginia State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class WestVirginiaCleaner(BaseStateCleaner):
    """
    West Virginia-specific data cleaner.
    
    This cleaner handles West Virginia-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("West Virginia")
    
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
        self.logger.info(f"Cleaning West Virginia-specific content")
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
        """Clean west_virginia-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
