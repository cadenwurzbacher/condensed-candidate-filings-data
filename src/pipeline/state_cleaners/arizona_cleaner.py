#!/usr/bin/env python3
"""
Arizona State Cleaner

Handles 
Arizona-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class ArizonaCleaner(BaseStateCleaner):
    """
    Arizona-specific data cleaner.
    
    This cleaner handles Arizona-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Arizona")
        
        # County mappings removed - not needed
        
        # Arizona-specific office mappings (kept state-specific as intended)
        # Office mappings removed - handled by national standards
    
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
        
        # Clean candidate names (using standard base class logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._standard_name_cleaning)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (using standard base class logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._standard_district_cleaning)
        
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
        self.logger.info("Cleaning Arizona-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # # if 'office' in df.columns:
        #     df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])

        return df

    # _parse_names() now uses default implementation from BaseStateCleaner
    # _clean_arizona_name() removed - using _standard_name_cleaning() directly
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_arizona_district() removed - using _standard_district_cleaning() directly
