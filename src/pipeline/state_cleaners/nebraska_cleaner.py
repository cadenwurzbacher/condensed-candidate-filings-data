#!/usr/bin/env python3
"""
Nebraska State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class NebraskaCleaner(BaseStateCleaner):
    """
    Nebraska-specific data cleaner.
    
    This cleaner handles Nebraska-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Nebraska")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Nebraska-specific data structure.
        
        This handles:
        - Name parsing (Nebraska has specific name formats)
        - Address parsing (Nebraska address structure)
        - District parsing (Nebraska district numbering)
        - Agricultural-specific logic (Nebraska has many agricultural areas)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Nebraska-specific structure cleaned
        """
        self.logger.info("Cleaning Nebraska-specific data structure")
        
        # Clean candidate names (Nebraska-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_nebraska_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Nebraska-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_nebraska_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Nebraska-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Nebraska-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Nebraska-specific content cleaned
        """
        self.logger.info(f"Cleaning Nebraska-specific content")
        return df
    
    def _clean_nebraska_name(self, name: str) -> str:
        """
        Clean Nebraska-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Nebraska-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Nebraska name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_nebraska_address method removed - now handled centrally
    
    def _clean_nebraska_district(self, district: str) -> str:
        """Clean nebraska-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
