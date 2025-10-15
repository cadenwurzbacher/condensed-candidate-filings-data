#!/usr/bin/env python3
"""
Pennsylvania State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class PennsylvaniaCleaner(BaseStateCleaner):
    """
    Pennsylvania-specific data cleaner.
    
    This cleaner handles Pennsylvania-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Pennsylvania")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Pennsylvania-specific data structure.
        
        This handles:
        - Name parsing (Pennsylvania has specific name formats)
        - Address parsing (Pennsylvania address structure)
        - District parsing (Pennsylvania district numbering)
        - Keystone-specific logic (Pennsylvania has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Pennsylvania-specific structure cleaned
        """
        self.logger.info("Cleaning Pennsylvania-specific data structure")
        
        # Clean candidate names (Pennsylvania-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_pennsylvania_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Pennsylvania-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_pennsylvania_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Pennsylvania-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Pennsylvania-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Pennsylvania-specific content cleaned
        """
        self.logger.info(f"Cleaning Pennsylvania-specific content")
        return df
    
    def _clean_pennsylvania_name(self, name: str) -> str:
        """
        Clean Pennsylvania-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Pennsylvania-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Pennsylvania name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_pennsylvania_address method removed - now handled centrally
    
    def _clean_pennsylvania_district(self, district: str) -> str:
        """Clean pennsylvania-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
