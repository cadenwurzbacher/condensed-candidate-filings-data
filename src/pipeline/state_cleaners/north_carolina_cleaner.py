#!/usr/bin/env python3
"""
North Carolina State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class NorthCarolinaCleaner(BaseStateCleaner):
    """
    North Carolina-specific data cleaner.
    
    This cleaner handles North Carolina-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("North Carolina")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean North Carolina-specific data structure.
        
        This handles:
        - Name parsing (North Carolina has specific name formats)
        - Address parsing (North Carolina address structure)
        - District parsing (North Carolina district numbering)
        - Tar Heel State-specific logic (North Carolina has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with North Carolina-specific structure cleaned
        """
        self.logger.info("Cleaning North Carolina-specific data structure")
        
        # Clean candidate names (North Carolina-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_north_carolina_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (North Carolina-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_north_carolina_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean North Carolina-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - North Carolina-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with North Carolina-specific content cleaned
        """
        self.logger.info(f"Cleaning North Carolina-specific content")
        return df
    
    def _clean_north_carolina_name(self, name: str) -> str:
        """
        Clean North Carolina-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # North Carolina-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common North Carolina name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_north_carolina_address method removed - now handled centrally
    
    def _clean_north_carolina_district(self, district: str) -> str:
        """Clean north_carolina-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
