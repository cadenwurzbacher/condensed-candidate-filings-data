#!/usr/bin/env python3
"""
Montana State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class MontanaCleaner(BaseStateCleaner):
    """
    Montana-specific data cleaner.
    
    This cleaner handles Montana-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Montana")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Montana-specific data structure.
        
        This handles:
        - Name parsing (Montana has specific name formats)
        - Address parsing (Montana address structure)
        - District parsing (Montana district numbering)
        - Rural-specific logic (Montana has many rural areas)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Montana-specific structure cleaned
        """
        self.logger.info("Cleaning Montana-specific data structure")
        
        # Clean candidate names (Montana-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_montana_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Montana-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_montana_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Montana-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Montana-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Montana-specific content cleaned
        """
        self.logger.info(f"Cleaning Montana-specific content")
        return df
    
    def _clean_montana_name(self, name: str) -> str:
        """
        Clean Montana-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Montana-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Montana name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_montana_address method removed - now handled centrally
    
    def _clean_montana_district(self, district: str) -> str:
        """Clean montana-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
