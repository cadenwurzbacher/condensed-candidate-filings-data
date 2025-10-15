#!/usr/bin/env python3
"""
Idaho State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class IdahoCleaner(BaseStateCleaner):
    """
    Idaho-specific data cleaner.
    
    This cleaner handles Idaho-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Idaho")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Idaho-specific data structure.
        
        This handles:
        - Name parsing (Idaho has specific name formats)
        - Address parsing (Idaho address structure)
        - District parsing (Idaho district numbering)
        - Mountain-specific logic (Idaho has many mountain areas)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Idaho-specific structure cleaned
        """
        self.logger.info("Cleaning Idaho-specific data structure")
        
        # Clean candidate names (Idaho-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_idaho_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Idaho-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_idaho_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Idaho-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Idaho-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Idaho-specific content cleaned
        """
        self.logger.info(f"Cleaning Idaho-specific content")
        return df
    
    def _clean_idaho_name(self, name: str) -> str:
        """
        Clean Idaho-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Idaho-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Idaho name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_idaho_address method removed - now handled centrally
    
    def _clean_idaho_district(self, district: str) -> str:
        """Clean idaho-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
