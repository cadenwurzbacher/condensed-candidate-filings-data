#!/usr/bin/env python3
"""
Florida State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
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
            df['candidate_name'] = df['candidate_name'].apply(self._clean_florida_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Florida-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_florida_district)
        
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
    
    def _clean_florida_name(self, name: str) -> str:
        """
        Clean Florida-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            str: Cleaned name
        """
        if pd.isna(name) or not isinstance(name, str):
            return name
        
        # Remove extra whitespace
        name = re.sub(r'\s+', ' ', name.strip())
        
        # Handle common Florida name patterns
        # Remove titles that might be in the name field
        title_patterns = [
            r'\b(?:Mr\.|Mrs\.|Ms\.|Dr\.|Hon\.|Sen\.|Rep\.|Gov\.|Lt\. Gov\.|Sec\.|Atty\.|Judge)\b\.?\s*',
            r'\b(?:MR|MRS|MS|DR|HON|SEN|REP|GOV|LT GOV|SEC|ATTY|JUDGE)\b\s*'
        ]
        
        for pattern in title_patterns:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
        
        return name.strip()
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_florida_address method removed - now handled centrally
    
    def _clean_florida_district(self, district: str) -> str:
        """
        Clean Florida-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            str: Cleaned district
        """
        if pd.isna(district) or not isinstance(district, str):
            return district
        
        district = str(district).strip()
        
        # Extract district number from common Florida formats
        # Examples: "District 1", "Dist. 1", "1st District", etc.
        patterns = [
            r'(\d+)(?:st|nd|rd|th)?\s*(?:district|dist\.?)',
            r'(?:district|dist\.?)\s*(\d+)',
            r'(\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, district, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return district
    
    # _parse_names removed - using default NameParser from base class
