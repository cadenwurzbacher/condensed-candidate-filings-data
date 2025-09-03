#!/usr/bin/env python3
"""
Kentucky State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class KentuckyCleaner(BaseStateCleaner):
    """
    Kentucky-specific data cleaner.
    
    This cleaner handles Kentucky-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Kentucky")
        
        # County mappings removed - not needed
        
        # Kentucky-specific office mappings
        # Office mappings removed - handled by national standards
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Kentucky candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Kentucky data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Kentucky-specific data structure.
        
        This handles:
        - Name parsing (Kentucky has specific name formats)
        - Address parsing (Kentucky address structure)
        - District parsing (Kentucky district numbering)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Kentucky-specific structure cleaned
        """
        self.logger.info("Cleaning Kentucky-specific data structure")
        
        # Clean candidate names (Kentucky-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_kentucky_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Kentucky-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_kentucky_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Kentucky-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Kentucky-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Kentucky-specific content cleaned
        """
        self.logger.info("Cleaning Kentucky-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Kentucky-specific formatting
        df = self._apply_kentucky_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Kentucky-specific name parsing logic - column-based approach.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Kentucky approach: look for separate first_name and last_name columns
        first_name_col = None
        last_name_col = None
        
        # Find the first name column
        for col in ['first_name', 'candidate_first_name', 'first']:
            if col in df.columns:
                first_name_col = col
                break
        
        # Find the last name column  
        for col in ['last_name', 'candidate_last_name', 'last']:
            if col in df.columns:
                last_name_col = col
                break
        
        # Check if the found columns actually have data
        has_data = False
        if first_name_col and last_name_col:
            # Check if these columns have actual data (not just None/empty)
            first_data = df[first_name_col].notna().sum()
            last_data = df[last_name_col].notna().sum()
            has_data = first_data > 0 and last_data > 0
        
        # If we don't have separate columns OR if the columns are empty, try to parse from candidate_name
        if not first_name_col or not last_name_col or not has_data:
            if 'candidate_name' in df.columns:
                # Fallback to basic parsing with Kentucky's prefix detection
                for idx, row in df.iterrows():
                    candidate_name = row.get('candidate_name')
                    if pd.notna(candidate_name) and str(candidate_name).strip():
                        name_str = str(candidate_name).strip()
                        
                        # Extract prefix first (from beginning of name)
                        prefix_pattern = r'^(Dr|Mr|Mrs|Ms|Miss|Prof|Rev|Hon|Sen|Rep|Gov|Lt|Col|Gen|Adm|Capt|Maj|Sgt|Cpl|Pvt)\.?\s+'
                        prefix_match = re.match(prefix_pattern, name_str, re.IGNORECASE)
                        prefix = None
                        if prefix_match:
                            prefix = prefix_match.group(1)
                            df.at[idx, 'prefix'] = prefix
                            # Remove prefix from name for further processing
                            name_str = re.sub(prefix_pattern, '', name_str, flags=re.IGNORECASE).strip()
                        
                        # Extract suffix
                        suffix_pattern = r'\b(Jr|Sr|II|III|IV|V|VI|VII|VIII|IX|X)\b'
                        suffix_match = re.search(suffix_pattern, name_str, re.IGNORECASE)
                        suffix = None
                        if suffix_match:
                            suffix = suffix_match.group(1)
                            df.at[idx, 'suffix'] = suffix
                            name_str = re.sub(suffix_pattern, '', name_str, flags=re.IGNORECASE).strip()
                        
                        # Split remaining name
                        parts = [p.strip() for p in name_str.split() if p.strip()]
                        if len(parts) >= 1:
                            df.at[idx, 'first_name'] = parts[0]
                        if len(parts) >= 2:
                            df.at[idx, 'last_name'] = parts[-1]
                        if len(parts) > 2:
                            df.at[idx, 'middle_name'] = ' '.join(parts[1:-1])
                        
                        df.at[idx, 'full_name_display'] = str(candidate_name).strip()
            else:
                # No name data available
                return df
        else:
            # We have separate columns - use Kentucky's column-based approach
            for idx in range(len(df)):
                first_name = str(df.iloc[idx][first_name_col]).strip() if pd.notna(df.iloc[idx][first_name_col]) else ""
                last_name = str(df.iloc[idx][last_name_col]).strip() if pd.notna(df.iloc[idx][last_name_col]) else ""
                
                # Check for suffix in last name
                suffix = None
                if last_name:
                    suffix_pattern = r'\b(Jr|Sr|II|III|IV|V|VI|VII|VIII|IX|X)\b'
                    suffix_match = re.search(suffix_pattern, last_name, re.IGNORECASE)
                    if suffix_match:
                        suffix = suffix_match.group(1)
                        # Remove suffix from last name
                        last_name = re.sub(suffix_pattern, '', last_name, flags=re.IGNORECASE).strip()
                
                # Check for prefix in first name
                prefix = None
                if first_name:
                    prefix_pattern = r'\b(Dr|Mr|Mrs|Ms|Miss|Prof|Rev|Hon|Sen|Rep|Gov|Lt|Col|Gen|Adm|Capt|Maj|Sgt|Cpl|Pvt)\b'
                    prefix_match = re.search(prefix_pattern, first_name, re.IGNORECASE)
                    if prefix_match:
                        prefix = prefix_match.group(1)
                        # Remove prefix from first name
                        first_name = re.sub(prefix_pattern, '', first_name, flags=re.IGNORECASE).strip()
                
                # Build display name
                display_parts = []
                if prefix:
                    display_parts.append(prefix)
                if first_name:
                    display_parts.append(first_name)
                if last_name:
                    display_parts.append(last_name)
                if suffix:
                    display_parts.append(suffix)
                
                display_name = ' '.join(display_parts).strip()
                
                # Assign parsed components
                df.at[idx, 'first_name'] = first_name
                df.at[idx, 'last_name'] = last_name
                df.at[idx, 'prefix'] = prefix
                df.at[idx, 'suffix'] = suffix
                df.at[idx, 'full_name_display'] = display_name
        
        return df
    
    def _clean_kentucky_name(self, name: str) -> str:
        """
        Clean Kentucky-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Kentucky-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Kentucky name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_kentucky_address method removed - now handled centrally
    
    def _clean_kentucky_district(self, district: str) -> str:
        """
        Clean Kentucky-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Kentucky-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Kentucky district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
    def _apply_kentucky_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Kentucky-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Kentucky-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
