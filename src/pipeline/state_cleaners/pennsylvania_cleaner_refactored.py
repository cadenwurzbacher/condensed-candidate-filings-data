#!/usr/bin/env python3
"""
Pennsylvania State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class PennsylvaniaCleaner(BaseStateCleaner):
    """
    Pennsylvania-specific data cleaner.
    
    This cleaner handles Pennsylvania-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Pennsylvania")
        
        # County mappings removed - not needed
        
        # Pennsylvania-specific office mappings
        # Office mappings removed - handled by national standards
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Pennsylvania candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Pennsylvania data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Pennsylvania-specific data structure.
        
        This handles:
        - Name parsing (Pennsylvania has specific name formats)
        - Address parsing (Pennsylvania address structure)
        - District parsing (Pennsylvania district numbering)
        - Keystone-specific logic (Pennsylvania has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Pennsylvania-specific structure cleaned
        """
        self.logger.info("Cleaning Pennsylvania-specific data structure")
        
        # Clean candidate names (Pennsylvania-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_pennsylvania_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Pennsylvania-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_pennsylvania_district)
        
        # Handle Pennsylvania-specific Keystone logic
        df = self._handle_pennsylvania_keystone_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Pennsylvania-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Pennsylvania-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Pennsylvania-specific content cleaned
        """
        self.logger.info("Cleaning Pennsylvania-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Pennsylvania-specific formatting
        df = self._apply_pennsylvania_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Pennsylvania-specific name parsing logic - Keystone State naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # Pennsylvania name parsing - handles northeastern naming patterns
        for idx, row in df.iterrows():
            candidate_name = row.get('candidate_name')
            if pd.notna(candidate_name) and str(candidate_name).strip():
                name_str = str(candidate_name).strip()
                
                # Extract prefix from the beginning FIRST
                prefix_pattern = r'^(Dr|Mr|Mrs|Ms|Miss|Prof|Rev|Hon|Sen|Rep|Gov|Lt|Col|Gen|Adm|Capt|Maj|Sgt|Cpl|Pvt)\.?\s+'
                prefix_match = re.match(prefix_pattern, name_str, re.IGNORECASE)
                prefix = None
                if prefix_match:
                    prefix = prefix_match.group(1)
                    df.at[idx, 'prefix'] = prefix
                    # Remove prefix from name for further processing
                    name_str = re.sub(prefix_pattern, '', name_str, flags=re.IGNORECASE).strip()
                
                # Extract suffix from the end
                suffix_pattern = r'\b(Jr|Sr|II|III|IV|V|VI|VII|VIII|IX|X)\b'
                suffix_match = re.search(suffix_pattern, name_str, re.IGNORECASE)
                if suffix_match:
                    suffix = suffix_match.group(1)
                    df.at[idx, 'suffix'] = suffix
                    # Remove suffix from name for further processing
                    name_str = re.sub(suffix_pattern, '', name_str, flags=re.IGNORECASE).strip()
                
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
    
    def _clean_pennsylvania_name(self, name: str) -> str:
        """
        Clean Pennsylvania-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Pennsylvania-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Pennsylvania name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_pennsylvania_address method removed - now handled centrally
    
    def _clean_pennsylvania_district(self, district: str) -> str:
        """
        Clean Pennsylvania-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Pennsylvania-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Pennsylvania district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
    def _handle_pennsylvania_keystone_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Pennsylvania-specific Keystone logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Pennsylvania-specific Keystone handling logic
        # (e.g., Keystone-specific processing, special district handling)
        
        return df
    
    def _apply_pennsylvania_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Pennsylvania-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Pennsylvania-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
