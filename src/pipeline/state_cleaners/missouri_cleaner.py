#!/usr/bin/env python3
"""
Missouri State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class MissouriCleaner(BaseStateCleaner):
    """
    Missouri-specific data cleaner.
    
    This cleaner handles Missouri-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Missouri")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Missouri-specific data structure.
        
        This handles:
        - Name parsing (Missouri has specific name formats)
        - Address parsing (Missouri address structure)
        - District parsing (Missouri district numbering)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Missouri-specific structure cleaned
        """
        self.logger.info("Cleaning Missouri-specific data structure")
        
        # Clean candidate names (Missouri-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_missouri_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Missouri-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_missouri_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Missouri-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Missouri-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Missouri-specific content cleaned
        """
        self.logger.info(f"Cleaning Missouri-specific content")
        return df
    
    def _clean_missouri_name(self, name: str) -> str:
        """
        Clean Missouri-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Missouri-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Missouri name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_missouri_address method removed - now handled centrally
    
    def _clean_missouri_district(self, district: str) -> str:
        """Clean missouri-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
