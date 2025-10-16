#!/usr/bin/env python3
"""
Kentucky State Cleaner

Handles 
Kentucky-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class KentuckyCleaner(BaseStateCleaner):
    """
    Kentucky-specific data cleaner.
    
    This cleaner handles Kentucky-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Kentucky")
        
        # County mappings removed - not needed
        
        # Kentucky-specific office mappings
        # Office mappings removed - handled by national standards
    
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
            df['candidate_name'] = df['candidate_name'].apply(self._standard_name_cleaning)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Kentucky-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._standard_district_cleaning)
        
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
        self.logger.info("Cleaning Kentucky-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        return df
    


    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_kentucky_address method removed - now handled centrally
    
