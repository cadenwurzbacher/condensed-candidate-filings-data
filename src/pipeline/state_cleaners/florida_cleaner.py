#!/usr/bin/env python3
"""
Florida State Cleaner

Handles 
Florida-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class FloridaCleaner(BaseStateCleaner):
    """
    Florida-specific data cleaner.
    
    This cleaner handles Florida-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Florida")
        
        # County mappings removed - not needed
        
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Florida-specific data structure.
        
        This handles:
        - Name parsing (Florida has specific name formats)
        - Address parsing (Florida address structure)
        - District parsing (Florida district numbering)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Florida-specific structure cleaned
        """
        self.logger.info("Cleaning Florida-specific data structure")
        
        # Clean candidate names (Florida-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._standard_name_cleaning)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Florida-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._standard_district_cleaning)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Florida-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Florida-specific formatting
        
        Args:
            df: DataFrame with structure cleaned
            
        Returns:
            DataFrame with Florida-specific content cleaned
        """
        self.logger.info("Cleaning Florida-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
        #     df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Florida-specific party cleaning - MOVED TO NATIONAL STANDARDS PHASE
        # if 'party' in df.columns:
        #     df['party'] = df['party'].apply(self._clean_florida_party)
        
        return df
    

    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_florida_address method removed - now handled centrally
    

    def _clean_florida_party(self, party: str) -> str:
        """
        Clean Florida-specific party formats - MOVED TO NATIONAL STANDARDS.
        
        Args:
            party: Raw party string
            
        Returns:
            str: Original party (standardization moved to national standards)
        """
        # Party standardization moved to national standards phase
        return party
    
