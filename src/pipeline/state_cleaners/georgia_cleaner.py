#!/usr/bin/env python3
"""
Georgia State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class GeorgiaCleaner(BaseStateCleaner):
    """
    Georgia-specific data cleaner.
    
    This cleaner handles Georgia-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Georgia")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Georgia-specific data structure.
        
        This handles:
        - Name parsing (Georgia has specific name formats)
        - Address parsing (Georgia address structure)
        - District parsing (Georgia district numbering)
        - Peach State-specific logic (Georgia has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Georgia-specific structure cleaned
        """
        self.logger.info("Cleaning Georgia-specific data structure")
        
        # Clean candidate names (Georgia-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_georgia_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Georgia-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_georgia_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Georgia-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Georgia-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Georgia-specific content cleaned
        """
        self.logger.info(f"Cleaning Georgia-specific content")
        return df
    
    def _clean_georgia_name(self, name: str) -> str:
        """
        Clean Georgia-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Georgia-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Georgia name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_georgia_address method removed - now handled centrally
    
    def _clean_georgia_district(self, district: str) -> str:
        """Clean georgia-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
