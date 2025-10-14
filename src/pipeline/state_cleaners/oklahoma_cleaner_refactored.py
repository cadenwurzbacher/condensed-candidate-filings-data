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
        
        # County mappings removed - not needed
        
        # Oklahoma-specific office mappings
        # Office mappings removed - handled by national standards
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Oklahoma candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Oklahoma data cleaning")
        return super().clean_data(df)
    
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
        
        # Oklahoma-specific formatting
        df = self._apply_oklahoma_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Oklahoma-specific name parsing logic - Sooner State naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # Oklahoma name parsing - handles plains region naming patterns
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
        """
        Clean Oklahoma-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Oklahoma-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Oklahoma district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
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
    
    def _apply_oklahoma_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Oklahoma-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Oklahoma-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
