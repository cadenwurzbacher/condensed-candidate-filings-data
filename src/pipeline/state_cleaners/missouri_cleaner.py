#!/usr/bin/env python3
"""
Missouri State Cleaner

Handles 
Missouri-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class MissouriCleaner(BaseStateCleaner):
    """
    Missouri-specific data cleaner.
    
    This cleaner handles Missouri-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Missouri")
        
        # County mappings removed - not needed
        
        # Missouri-specific office mappings
        # Office mappings removed - handled by national standards
    
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
            df['candidate_name'] = df['candidate_name'].apply(self._standard_name_cleaning)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Missouri-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._standard_district_cleaning)
        
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
        self.logger.info("Cleaning Missouri-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        return df
    


    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_missouri_address method removed - now handled centrally
    
