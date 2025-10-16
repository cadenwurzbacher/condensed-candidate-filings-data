#!/usr/bin/env python3
"""
South Dakota State Cleaner

Handles 
South Dakota-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class SouthDakotaCleaner(BaseStateCleaner):
    """
    South Dakota-specific data cleaner.
    
    This cleaner handles South Dakota-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("South Dakota")
        
        # County mappings removed - not needed
        
        # South Dakota-specific office mappings
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean South Dakota-specific data structure.
        
        This handles:
        - Name parsing (South Dakota has specific name formats)
        - Address parsing (South Dakota address structure)
        - District parsing (South Dakota district numbering)
        - Mount Rushmore-specific logic (South Dakota has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with South Dakota-specific structure cleaned
        """
        self.logger.info("Cleaning South Dakota-specific data structure")
        
        # Clean candidate names (South Dakota-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._standard_name_cleaning)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (South Dakota-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._standard_district_cleaning)
        
        # Handle South Dakota-specific Mount Rushmore logic
        df = self._handle_south_dakota_mount_rushmore_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean South Dakota-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - South Dakota-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with South Dakota-specific content cleaned
        """
        self.logger.info("Cleaning South Dakota-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # South Dakota-specific formatting
        df = self._apply_south_dakota_formatting(df)
        
        return df
    


    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_south_dakota_address method removed - now handled centrally
    

    def _handle_south_dakota_mount_rushmore_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle South Dakota-specific Mount Rushmore logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # South Dakota-specific Mount Rushmore handling logic
        # (e.g., Mount Rushmore-specific processing, special district handling)
        
        return df
    
    def _apply_south_dakota_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply South Dakota-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # South Dakota-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
