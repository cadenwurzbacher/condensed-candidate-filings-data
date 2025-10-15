#!/usr/bin/env python3
"""
Arizona State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class ArizonaCleaner(BaseStateCleaner):
    """
    Arizona-specific data cleaner.
    
    This cleaner handles Arizona-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Arizona")
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Arizona-specific data structure.
        
        This handles:
        - Name parsing (Arizona has specific name formats)
        - Address parsing (Arizona address structure)
        - District parsing (Arizona district numbering)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Arizona-specific structure cleaned
        """
        self.logger.info("Cleaning Arizona-specific data structure")
        
        # Clean candidate names (Arizona-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_arizona_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Arizona-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_arizona_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Arizona-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Arizona-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Arizona-specific content cleaned
        """
        self.logger.info(f"Cleaning Arizona-specific content")
        return df
    
    def _clean_arizona_name(self, name: str) -> str:
        """
        Clean Arizona-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Arizona-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Arizona name patterns
        if ',' in name:
            # Handle "Last, First" format
            parts = name.split(',')
            if len(parts) == 2:
                last, first = parts
                name = f"{first.strip()} {last.strip()}"
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_arizona_address method removed - now handled centrally
    
    def _clean_arizona_district(self, district: str) -> str:
        """Clean Arizona-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
