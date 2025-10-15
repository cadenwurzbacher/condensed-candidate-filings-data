#!/usr/bin/env python3
"""
Oregon State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class OregonCleaner(BaseStateCleaner):
    """
    Oregon-specific data cleaner.
    
    This cleaner handles Oregon-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Oregon")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Oregon-specific data structure.
        
        This handles:
        - Name parsing (Oregon has specific name formats)
        - Address parsing (Oregon address structure)
        - District parsing (Oregon district numbering)
        - Metro-specific logic (Portland metro area)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Oregon-specific structure cleaned
        """
        self.logger.info("Cleaning Oregon-specific data structure")
        
        # Clean candidate names (Oregon-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_oregon_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Oregon-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_oregon_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Oregon-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Oregon-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Oregon-specific content cleaned
        """
        self.logger.info(f"Cleaning Oregon-specific content")
        return df
    
    def _clean_oregon_name(self, name: str) -> str:
        """
        Clean Oregon-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Oregon-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Oregon name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_oregon_address method removed - now handled centrally
    
    def _clean_oregon_district(self, district: str) -> str:
        """Clean oregon-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
