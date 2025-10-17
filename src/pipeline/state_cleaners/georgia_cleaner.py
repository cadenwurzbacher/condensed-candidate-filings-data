#!/usr/bin/env python3
"""
Georgia State Cleaner

Handles 
Georgia-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class GeorgiaCleaner(BaseStateCleaner):
    """
    Georgia-specific data cleaner.
    
    This cleaner handles Georgia-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Georgia")
        
        # County mappings removed - not needed
        
        # Georgia-specific office mappings
        
        # Georgia-specific office mappings
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Georgia-specific data structure.
        
        This handles:
        - Name parsing (Georgia has specific name formats)
        - Address parsing (Georgia address structure)
        - District parsing (Georgia district numbering)
        - Peach State-specific logic (Georgia has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Georgia-specific structure cleaned
        """
        self.logger.info("Cleaning Georgia-specific data structure")
        
        # Clean candidate names (Georgia-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._standard_name_cleaning)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Georgia-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._standard_district_cleaning)
        
        # Handle Georgia-specific Peach State logic
        df = self._handle_georgia_peach_state_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Georgia-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Georgia-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Georgia-specific content cleaned
        """
        self.logger.info("Cleaning Georgia-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        return df
    


    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_georgia_address method removed - now handled centrally
    

    def _handle_georgia_peach_state_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Georgia-specific Peach State logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Georgia-specific Peach State handling logic
        # (e.g., Peach State-specific processing, special district handling)
        
        return df

