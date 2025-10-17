#!/usr/bin/env python3
"""
Washington State Cleaner

Handles 
Washington-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class WashingtonCleaner(BaseStateCleaner):
    """
    Washington-specific data cleaner.
    
    This cleaner handles Washington-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Washington")
        
        # County mappings removed - not needed
        
        # Washington-specific office mappings
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Washington-specific data structure.
        
        This handles:
        - Name parsing (Washington has specific name formats)
        - Address parsing (Washington address structure)
        - District parsing (Washington district numbering)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Washington-specific structure cleaned
        """
        self.logger.info("Cleaning Washington-specific data structure")
        
        # Clean candidate names (Washington-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._standard_name_cleaning)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Washington-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._standard_district_cleaning)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Washington-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Washington-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Washington-specific content cleaned
        """
        self.logger.info("Cleaning Washington-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        

        
        return df
    


    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_washington_address method removed - now handled centrally
    
