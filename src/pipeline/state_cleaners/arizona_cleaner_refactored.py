#!/usr/bin/env python3
"""
Arizona State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class ArizonaCleaner(BaseStateCleaner):
    """
    Arizona-specific data cleaner.
    
    This cleaner handles Arizona-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Arizona")
        
        # County mappings removed - not needed
        
        # Arizona-specific office mappings (kept state-specific as intended)
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Arizona-specific data structure.
        
        This handles:
        - Name parsing (Arizona has specific name formats)
        - Address parsing (Arizona address structure)
        - District parsing (Arizona district numbering)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Arizona-specific structure cleaned
        """
        self.logger.info("Cleaning Arizona-specific data structure")
        
        # Clean candidate names (Arizona-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_arizona_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Arizona-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_arizona_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Arizona-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Arizona-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Arizona-specific content cleaned
        """
        self.logger.info("Cleaning Arizona-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # # if 'office' in df.columns:
        #     df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Arizona-specific formatting
        df = self._apply_arizona_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Arizona-specific name parsing logic.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # Basic name parsing for Arizona
        for idx, row in df.iterrows():
            candidate_name = row.get('candidate_name')
            if pd.notna(candidate_name) and str(candidate_name).strip():
                name_str = str(candidate_name).strip()
                
                # Extract suffix from the end
                suffix_pattern = r'\b(Jr|Sr|II|III|IV|V|VI|VII|VIII|IX|X)\b'
                suffix_match = re.search(suffix_pattern, name_str, re.IGNORECASE)
                if suffix_match:
                    suffix = suffix_match.group(1)
                    df.at[idx, 'suffix'] = suffix
                    # Remove suffix from name for further processing
                    name_str = re.sub(suffix_pattern, '', name_str, flags=re.IGNORECASE).strip()
                else:
                    df.at[idx, 'suffix'] = None
                
                # Extract prefix from the beginning
                prefix_pattern = r'^(Dr|Mr|Mrs|Ms|Miss|Prof|Rev|Hon|Sen|Rep|Gov|Lt|Col|Gen|Adm|Capt|Maj|Sgt|Cpl|Pvt)\.?\s+'
                prefix_match = re.match(prefix_pattern, name_str, re.IGNORECASE)
                if prefix_match:
                    prefix = prefix_match.group(1)
                    df.at[idx, 'prefix'] = prefix
                    # Remove prefix from name for further processing
                    name_str = re.sub(prefix_pattern, '', name_str, flags=re.IGNORECASE).strip()
                else:
                    df.at[idx, 'prefix'] = None
                
                # Split remaining name into parts
                parts = [p.strip() for p in name_str.split() if p.strip()]
                
                if len(parts) >= 1:
                    df.at[idx, 'first_name'] = parts[0]
                if len(parts) >= 2:
                    df.at[idx, 'last_name'] = parts[-1]
                if len(parts) > 2:
                    # Middle names are everything between first and last
                    df.at[idx, 'middle_name'] = ' '.join(parts[1:-1])
        
        return df
    
    def _clean_arizona_name(self, name: str) -> str:
        """
        Clean Arizona-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Arizona-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Arizona name patterns
        if ',' in name:
            # Handle "Last, First" format
            parts = name.split(',')
            if len(parts) == 2:
                last, first = parts
                name = f"{first.strip()} {last.strip()}"
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_arizona_address method removed - now handled centrally
    
    def _clean_arizona_district(self, district: str) -> str:
        """
        Clean Arizona-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Arizona-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Arizona district patterns
        # (e.g., "District 1", "LD 1", etc.)
        
        return district
    
    def _apply_arizona_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Arizona-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Arizona-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
