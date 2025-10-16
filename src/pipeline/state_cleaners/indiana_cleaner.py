#!/usr/bin/env python3
"""
Indiana State Cleaner

Handles 
Indiana-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class IndianaCleaner(BaseStateCleaner):
    """
    Indiana-specific data cleaner.
    
    This cleaner handles Indiana-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Indiana")
        
        # County mappings removed - not needed
        
        # Indiana-specific office mappings
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Indiana-specific data structure.
        
        This handles:
        - Name parsing (Indiana has specific name formats)
        - Address parsing (Indiana address structure)
        - District parsing (Indiana district numbering)
        - Hoosier-specific logic (Indiana has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Indiana-specific structure cleaned
        """
        self.logger.info("Cleaning Indiana-specific data structure")
        
        # Clean candidate names (Indiana-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._standard_name_cleaning)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Indiana-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._standard_district_cleaning)
        
        # Handle Indiana-specific Hoosier logic
        df = self._handle_indiana_hoosier_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Indiana-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Indiana-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Indiana-specific content cleaned
        """
        self.logger.info("Cleaning Indiana-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        return df
    


    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_indiana_address method removed - now handled centrally
    

    def _handle_indiana_hoosier_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Indiana-specific Hoosier logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Indiana-specific Hoosier handling logic
        # (e.g., Hoosier-specific processing, special district handling)
        
        return df

