#!/usr/bin/env python3
"""
Colorado State Cleaner

Handles 
Colorado-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class ColoradoCleaner(BaseStateCleaner):
    """
    Colorado-specific data cleaner.
    
    This cleaner handles Colorado-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Colorado")
        
        # County mappings removed - not needed
        
        # Colorado-specific office mappings
        
        # Colorado-specific office mappings
        # Office mappings removed - handled by national standards
    
    
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
            df['candidate_name'] = df['candidate_name'].apply(self._standard_name_cleaning)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Colorado-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._standard_district_cleaning)
        
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
        self.logger.info("Cleaning Colorado-specific content")
        
        # County standardization removed - not needed
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        return df
    


