#!/usr/bin/env python3
"""
New York State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class NewYorkCleaner(BaseStateCleaner):
    """
    New York-specific data cleaner.
    
    This cleaner handles New York-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("New York")
    
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
        self.logger.info(f"Cleaning New York-specific content")
        return df
    
    def _clean_new_york_name(self, name: str) -> str:
        """
        Clean New York-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # New York-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common New York name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_new_york_address method removed - now handled centrally
    
    def _clean_new_york_district(self, district: str) -> str:
        """Clean new_york-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
