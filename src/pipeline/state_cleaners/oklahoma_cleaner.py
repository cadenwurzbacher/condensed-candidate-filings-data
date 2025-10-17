#!/usr/bin/env python3
"""
Oklahoma State Cleaner

Handles 
Oklahoma-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class OklahomaCleaner(BaseStateCleaner):
    """
    Oklahoma-specific data cleaner.
    
    This cleaner handles Oklahoma-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Oklahoma")
        
        # County mappings removed - not needed
        
        # Oklahoma-specific office mappings
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Oklahoma-specific data structure.
        
        This handles:
        - Name parsing (Oklahoma has specific name formats)
        - Address parsing (Oklahoma address structure)
        - District parsing (Oklahoma district numbering)
        - Sooner-specific logic (Oklahoma has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Oklahoma-specific structure cleaned
        """
        self.logger.info("Cleaning Oklahoma-specific data structure")
        
        # Clean candidate names (Oklahoma-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._standard_name_cleaning)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Oklahoma-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._standard_district_cleaning)
        
        # Handle Oklahoma-specific Sooner logic
        df = self._handle_oklahoma_sooner_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Oklahoma-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Oklahoma-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Oklahoma-specific content cleaned
        """
        self.logger.info("Cleaning Oklahoma-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        return df
    


    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_oklahoma_address method removed - now handled centrally
    

    def _handle_oklahoma_sooner_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Oklahoma-specific Sooner logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Oklahoma-specific Sooner handling logic
        # (e.g., Sooner-specific processing, special district handling)
        
        return df

