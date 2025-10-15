#!/usr/bin/env python3
"""
Kentucky State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class KentuckyCleaner(BaseStateCleaner):
    """
    Kentucky-specific data cleaner.
    
    This cleaner handles Kentucky-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Kentucky")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Kentucky-specific data structure.
        
        This handles:
        - Name parsing (Kentucky has specific name formats)
        - Address parsing (Kentucky address structure)
        - District parsing (Kentucky district numbering)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Kentucky-specific structure cleaned
        """
        self.logger.info("Cleaning Kentucky-specific data structure")
        
        # Clean candidate names (Kentucky-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_kentucky_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Kentucky-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_kentucky_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Kentucky-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Kentucky-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Kentucky-specific content cleaned
        """
        self.logger.info(f"Cleaning Kentucky-specific content")
        return df
    
    def _clean_kentucky_name(self, name: str) -> str:
        """
        Clean Kentucky-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Kentucky-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Kentucky name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_kentucky_address method removed - now handled centrally
    
    def _clean_kentucky_district(self, district: str) -> str:
        """Clean kentucky-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
