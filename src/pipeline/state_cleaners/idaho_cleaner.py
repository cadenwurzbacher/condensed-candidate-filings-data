#!/usr/bin/env python3
"""
Idaho State Cleaner

Handles 
Idaho-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class IdahoCleaner(BaseStateCleaner):
    """
    Idaho-specific data cleaner.
    
    This cleaner handles Idaho-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Idaho")
        
        # County mappings removed - not needed
        
        # Idaho-specific office mappings
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Idaho-specific data structure.
        
        This handles:
        - Name parsing (Idaho has specific name formats)
        - Address parsing (Idaho address structure)
        - District parsing (Idaho district numbering)
        - Mountain-specific logic (Idaho has many mountain areas)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Idaho-specific structure cleaned
        """
        self.logger.info("Cleaning Idaho-specific data structure")
        
        # Clean candidate names (Idaho-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._standard_name_cleaning)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Idaho-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._standard_district_cleaning)
        
        # Handle Idaho-specific mountain logic
        df = self._handle_idaho_mountain_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Idaho-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Idaho-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Idaho-specific content cleaned
        """
        self.logger.info("Cleaning Idaho-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        return df
    


    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_idaho_address method removed - now handled centrally
    

    def _handle_idaho_mountain_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Idaho-specific mountain logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Idaho-specific mountain handling logic
        # (e.g., mountain area processing, special district handling)
        
        return df

