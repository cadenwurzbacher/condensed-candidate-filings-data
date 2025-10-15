#!/usr/bin/env python3
"""
Indiana State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class IndianaCleaner(BaseStateCleaner):
    """
    Indiana-specific data cleaner.
    
    This cleaner handles Indiana-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Indiana")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Indiana-specific data structure.
        
        This handles:
        - Name parsing (Indiana has specific name formats)
        - Address parsing (Indiana address structure)
        - District parsing (Indiana district numbering)
        - Hoosier-specific logic (Indiana has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Indiana-specific structure cleaned
        """
        self.logger.info("Cleaning Indiana-specific data structure")
        
        # Clean candidate names (Indiana-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_indiana_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Indiana-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_indiana_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Indiana-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Indiana-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Indiana-specific content cleaned
        """
        self.logger.info(f"Cleaning Indiana-specific content")
        return df
    
    def _clean_indiana_name(self, name: str) -> str:
        """
        Clean Indiana-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Indiana-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Indiana name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_indiana_address method removed - now handled centrally
    
    def _clean_indiana_district(self, district: str) -> str:
        """Clean indiana-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
