#!/usr/bin/env python3
"""
Iowa State Cleaner

Handles 
Iowa-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class IowaCleaner(BaseStateCleaner):
    """
    Iowa-specific data cleaner.
    
    This cleaner handles Iowa-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Iowa")
        
        # County mappings removed - not needed
        
        # Iowa-specific office mappings
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Iowa-specific data structure.
        
        This handles:
        - Name parsing (Iowa has specific name formats)
        - Address parsing (Iowa address structure)
        - District parsing (Iowa district numbering)
        - Hawkeye-specific logic (Iowa has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Iowa-specific structure cleaned
        """
        self.logger.info("Cleaning Iowa-specific data structure")
        
        # Clean candidate names (Iowa-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._standard_name_cleaning)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Iowa-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._standard_district_cleaning)
        
        # Handle Iowa-specific Hawkeye logic
        df = self._handle_iowa_hawkeye_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Iowa-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Iowa-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Iowa-specific content cleaned
        """
        self.logger.info("Cleaning Iowa-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        return df
    


    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_iowa_address method removed - now handled centrally
    

    def _handle_iowa_hawkeye_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Iowa-specific Hawkeye logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Iowa-specific Hawkeye handling logic
        # (e.g., Hawkeye-specific processing, special district handling)
        
        return df

