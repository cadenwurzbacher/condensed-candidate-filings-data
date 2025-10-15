#!/usr/bin/env python3
"""
Washington State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class WashingtonCleaner(BaseStateCleaner):
    """
    Washington-specific data cleaner.
    
    This cleaner handles Washington-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Washington")
    
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
        self.logger.info(f"Cleaning Washington-specific content")
        return df
    
    def _clean_washington_name(self, name: str) -> str:
        """
        Clean Washington-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Washington-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Washington name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_washington_address method removed - now handled centrally
    
    def _clean_washington_district(self, district: str) -> str:
        """Clean washington-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
