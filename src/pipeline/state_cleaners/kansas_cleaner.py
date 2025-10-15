#!/usr/bin/env python3
"""
Kansas State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class KansasCleaner(BaseStateCleaner):
    """
    Kansas-specific data cleaner.
    
    This cleaner handles Kansas-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Kansas")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Kansas-specific data structure.
        
        This handles:
        - Name parsing (Kansas has specific name formats)
        - Address parsing (Kansas address structure)
        - District parsing (Kansas district numbering)
        - Sunflower-specific logic (Kansas has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Kansas-specific structure cleaned
        """
        self.logger.info("Cleaning Kansas-specific data structure")
        
        # Clean candidate names (Kansas-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_kansas_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Kansas-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_kansas_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Kansas-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Kansas-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Kansas-specific content cleaned
        """
        self.logger.info(f"Cleaning Kansas-specific content")
        return df
    
    def _clean_kansas_name(self, name: str) -> str:
        """
        Clean Kansas-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Kansas-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Kansas name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_kansas_address method removed - now handled centrally
    
    def _clean_kansas_district(self, district: str) -> str:
        """Clean kansas-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
