#!/usr/bin/env python3
"""
Louisiana State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class LouisianaCleaner(BaseStateCleaner):
    """
    Louisiana-specific data cleaner.
    
    This cleaner handles Louisiana-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Louisiana")
        
        # Parish mappings removed - not needed
        
        # Louisiana-specific office mappings
        
        # Louisiana-specific office mappings
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Louisiana-specific data structure.
        
        This handles:
        - Name parsing (Louisiana has specific name formats)
        - Address parsing (Louisiana address structure)
        - District parsing (Louisiana district numbering)
        - Parish-specific logic (Louisiana uses parishes instead of counties)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Louisiana-specific structure cleaned
        """
        self.logger.info("Cleaning Louisiana-specific data structure")
        
        # Clean candidate names (Louisiana-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_louisiana_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Louisiana-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_louisiana_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Louisiana-specific content.
        
        This handles:
        - Parish standardization (Louisiana uses parishes instead of counties)
        - Office standardization
        - Louisiana-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Louisiana-specific content cleaned
        """
        self.logger.info(f"Cleaning Louisiana-specific content")
        return df
    
    def _clean_louisiana_name(self, name: str) -> str:
        """
        Clean Louisiana-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Louisiana-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Louisiana name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_louisiana_address method removed - now handled centrally
    
    def _clean_louisiana_district(self, district: str) -> str:
        """Clean louisiana-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
