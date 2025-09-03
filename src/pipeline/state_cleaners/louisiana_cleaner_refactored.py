#!/usr/bin/env python3
"""
Louisiana State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class LouisianaCleaner(BaseStateCleaner):
    """
    Louisiana-specific data cleaner.
    
    This cleaner handles Louisiana-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Louisiana")
        
        # Parish mappings removed - not needed
        
        # Louisiana-specific office mappings
        
        # Louisiana-specific office mappings
        # Office mappings removed - handled by national standards
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Louisiana candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Louisiana data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Louisiana-specific data structure.
        
        This handles:
        - Name parsing (Louisiana has specific name formats)
        - Address parsing (Louisiana address structure)
        - District parsing (Louisiana district numbering)
        - Parish-specific logic (Louisiana uses parishes instead of counties)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Louisiana-specific structure cleaned
        """
        self.logger.info("Cleaning Louisiana-specific data structure")
        
        # Clean candidate names (Louisiana-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_louisiana_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Louisiana-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_louisiana_district)
        
        # Handle Louisiana-specific parish logic
        df = self._handle_louisiana_parish_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Louisiana-specific content.
        
        This handles:
        - Parish standardization (Louisiana uses parishes instead of counties)
        - Office standardization
        - Louisiana-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Louisiana-specific content cleaned
        """
        self.logger.info("Cleaning Louisiana-specific content")
        
        # Parish standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Louisiana-specific formatting
        df = self._apply_louisiana_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Louisiana-specific name parsing logic - simple extraction from existing columns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Louisiana approach: simple extraction from candidate_name
        for idx, row in df.iterrows():
            candidate_name = row.get('candidate_name')
            
            if pd.notna(candidate_name) and str(candidate_name).strip():
                # Use candidate_name as full_name_display
                df.at[idx, 'full_name_display'] = str(candidate_name).strip()
                
                # Simple name parsing for Louisiana
                name_str = str(candidate_name).strip().strip("'\"")
                
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
    
    def _clean_louisiana_name(self, name: str) -> str:
        """
        Clean Louisiana-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Louisiana-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Louisiana name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_louisiana_address method removed - now handled centrally
    
    def _clean_louisiana_district(self, district: str) -> str:
        """
        Clean Louisiana-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Louisiana-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Louisiana district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
    def _handle_louisiana_parish_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Louisiana-specific parish logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Louisiana-specific parish handling logic
        # (e.g., parish-specific processing, special district handling)
        
        return df
    
    def _apply_louisiana_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Louisiana-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Louisiana-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
