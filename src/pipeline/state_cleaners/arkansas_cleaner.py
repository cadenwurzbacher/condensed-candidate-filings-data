#!/usr/bin/env python3
"""
Arkansas State Cleaner

Handles 
Arkansas-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class ArkansasCleaner(BaseStateCleaner):
    """
    Arkansas-specific data cleaner.
    
    This cleaner handles Arkansas-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Arkansas")
        
        # County mappings removed - not needed
        
        # Arkansas-specific office mappings
        
        # Arkansas-specific office mappings
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Arkansas-specific data structure.
        
        This handles:
        - Name parsing (Arkansas has specific name formats)
        - Address parsing (Arkansas address structure)
        - District parsing (Arkansas district numbering)
        - Natural State-specific logic (Arkansas has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Arkansas-specific structure cleaned
        """
        self.logger.info("Cleaning Arkansas-specific data structure")
        
        # Clean candidate names (Arkansas-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._standard_name_cleaning)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Arkansas-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._standard_district_cleaning)
        
        # Handle Arkansas-specific Natural State logic
        df = self._handle_arkansas_natural_state_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Arkansas-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Arkansas-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Arkansas-specific content cleaned
        """
        self.logger.info("Cleaning Arkansas-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        return df
    


    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_arkansas_address method removed - now handled centrally
    

    def _handle_arkansas_natural_state_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Arkansas-specific Natural State logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Arkansas-specific Natural State handling logic
        # (e.g., Natural State-specific processing, special district handling)
        
        return df

