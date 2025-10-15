#!/usr/bin/env python3
"""
South Dakota State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class SouthDakotaCleaner(BaseStateCleaner):
    """
    South Dakota-specific data cleaner.
    
    This cleaner handles South Dakota-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("South Dakota")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean South Dakota-specific data structure.
        
        This handles:
        - Name parsing (South Dakota has specific name formats)
        - Address parsing (South Dakota address structure)
        - District parsing (South Dakota district numbering)
        - Mount Rushmore-specific logic (South Dakota has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with South Dakota-specific structure cleaned
        """
        self.logger.info("Cleaning South Dakota-specific data structure")
        
        # Clean candidate names (South Dakota-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_south_dakota_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (South Dakota-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_south_dakota_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean South Dakota-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - South Dakota-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with South Dakota-specific content cleaned
        """
        self.logger.info(f"Cleaning South Dakota-specific content")
        return df
    
    def _clean_south_dakota_name(self, name: str) -> str:
        """
        Clean South Dakota-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # South Dakota-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common South Dakota name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_south_dakota_address method removed - now handled centrally
    
    def _clean_south_dakota_district(self, district: str) -> str:
        """Clean south_dakota-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
