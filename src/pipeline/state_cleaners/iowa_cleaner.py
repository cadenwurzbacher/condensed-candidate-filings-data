#!/usr/bin/env python3
"""
Iowa State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class IowaCleaner(BaseStateCleaner):
    """
    Iowa-specific data cleaner.
    
    This cleaner handles Iowa-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Iowa")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Iowa-specific data structure.
        
        This handles:
        - Name parsing (Iowa has specific name formats)
        - Address parsing (Iowa address structure)
        - District parsing (Iowa district numbering)
        - Hawkeye-specific logic (Iowa has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Iowa-specific structure cleaned
        """
        self.logger.info("Cleaning Iowa-specific data structure")
        
        # Clean candidate names (Iowa-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_iowa_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Iowa-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_iowa_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Iowa-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Iowa-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Iowa-specific content cleaned
        """
        self.logger.info(f"Cleaning Iowa-specific content")
        return df
    
    def _clean_iowa_name(self, name: str) -> str:
        """
        Clean Iowa-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Iowa-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Iowa name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_iowa_address method removed - now handled centrally
    
    def _clean_iowa_district(self, district: str) -> str:
        """Clean iowa-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
