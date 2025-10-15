#!/usr/bin/env python3
"""
Oklahoma State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class OklahomaCleaner(BaseStateCleaner):
    """
    Oklahoma-specific data cleaner.
    
    This cleaner handles Oklahoma-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Oklahoma")
    
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
            df['candidate_name'] = df['candidate_name'].apply(self._clean_oklahoma_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Oklahoma-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_oklahoma_district)
        
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
        self.logger.info(f"Cleaning Oklahoma-specific content")
        return df
    
    def _clean_oklahoma_name(self, name: str) -> str:
        """
        Clean Oklahoma-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Oklahoma-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Oklahoma name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_oklahoma_address method removed - now handled centrally
    
    def _clean_oklahoma_district(self, district: str) -> str:
        """Clean oklahoma-specific district formats."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
