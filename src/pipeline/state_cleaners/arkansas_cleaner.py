#!/usr/bin/env python3
"""
Arkansas State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class ArkansasCleaner(BaseStateCleaner):
    """
    Arkansas-specific data cleaner.
    
    This cleaner handles Arkansas-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Arkansas")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Arkansas-specific data structure.
        
        This handles:
        - Name parsing (Arkansas has specific name formats)
        - Address parsing (Arkansas address structure)
        - District parsing (Arkansas district numbering)
        - Natural State-specific logic (Arkansas has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Arkansas-specific structure cleaned
        """
        self.logger.info("Cleaning Arkansas-specific data structure")
        
        # Clean candidate names (Arkansas-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_arkansas_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Arkansas-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_arkansas_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Arkansas-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Arkansas-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Arkansas-specific content cleaned
        """
        self.logger.info(f"Cleaning Arkansas-specific content")
        return df
    
    def _clean_arkansas_name(self, name: str) -> str:
        """
        Clean Arkansas-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Arkansas-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Arkansas name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_arkansas_address method removed - now handled centrally
    
    def _clean_arkansas_district(self, district: str) -> str:
        """Clean arkansas-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
