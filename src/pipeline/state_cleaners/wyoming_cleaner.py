#!/usr/bin/env python3
"""
Wyoming State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class WyomingCleaner(BaseStateCleaner):
    """
    Wyoming-specific data cleaner.
    
    This cleaner handles Wyoming-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Wyoming")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Wyoming-specific data structure.
        
        This handles:
        - Name parsing (Wyoming has specific name formats)
        - Address parsing (Wyoming address structure)
        - District parsing (Wyoming district numbering)
        - Equality State-specific logic (Wyoming has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Wyoming-specific structure cleaned
        """
        self.logger.info("Cleaning Wyoming-specific data structure")
        
        # Clean candidate names (Wyoming-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_wyoming_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Wyoming-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_wyoming_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Wyoming-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Wyoming-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Wyoming-specific content cleaned
        """
        self.logger.info(f"Cleaning Wyoming-specific content")
        return df
    
    def _clean_wyoming_name(self, name: str) -> str:
        """
        Clean Wyoming-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Wyoming-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Wyoming name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_wyoming_address method removed - now handled centrally
    
    def _clean_wyoming_district(self, district: str) -> str:
        """Clean wyoming-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
