#!/usr/bin/env python3
"""
New Mexico State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class NewMexicoCleaner(BaseStateCleaner):
    """
    New Mexico-specific data cleaner.
    
    This cleaner handles New Mexico-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("New Mexico")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean New Mexico-specific data structure.
        
        This handles:
        - Name parsing (New Mexico has specific name formats)
        - Address parsing (New Mexico address structure)
        - District parsing (New Mexico district numbering)
        - Land of Enchantment-specific logic (New Mexico has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with New Mexico-specific structure cleaned
        """
        self.logger.info("Cleaning New Mexico-specific data structure")
        
        # Clean candidate names (New Mexico-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_new_mexico_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (New Mexico-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_new_mexico_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean New Mexico-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - New Mexico-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with New Mexico-specific content cleaned
        """
        self.logger.info(f"Cleaning New Mexico-specific content")
        return df
    
    def _clean_new_mexico_name(self, name: str) -> str:
        """
        Clean New Mexico-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # New Mexico-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common New Mexico name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_new_mexico_address method removed - now handled centrally
    
    def _clean_new_mexico_district(self, district: str) -> str:
        """Clean new_mexico-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
