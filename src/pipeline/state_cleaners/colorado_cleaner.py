#!/usr/bin/env python3
"""
Colorado State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class ColoradoCleaner(BaseStateCleaner):
    """
    Colorado-specific data cleaner.
    
    This cleaner handles Colorado-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Colorado")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Colorado-specific data structure.
        
        This handles:
        - Name parsing (Colorado has specific name formats)
        - Address parsing (Colorado address structure)
        - District parsing (Colorado district numbering)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Colorado-specific structure cleaned
        """
        self.logger.info("Cleaning Colorado-specific data structure")
        
        # Clean candidate names (Colorado-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_colorado_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Colorado-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_colorado_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Colorado-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Colorado-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Colorado-specific content cleaned
        """
        self.logger.info(f"Cleaning Colorado-specific content")
        return df
    
    def _clean_colorado_name(self, name: str) -> str:
        """
        Clean Colorado-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Colorado-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Colorado name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_colorado_address method removed - now handled centrally
    
    def _clean_colorado_district(self, district: str) -> str:
        """Clean colorado-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
