#!/usr/bin/env python3
"""
Delaware State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class DelawareCleaner(BaseStateCleaner):
    """
    Delaware-specific data cleaner.
    
    This cleaner handles Delaware-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Delaware")
        
        # County mappings removed - not needed
        
        # Delaware-specific office mappings
        
        # Delaware-specific office mappings
        # Office mappings removed - handled by national standards
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Delaware candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Delaware data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Delaware-specific data structure.
        
        This handles:
        - Name parsing (Delaware has specific name formats)
        - Address parsing (Delaware address structure)
        - District parsing (Delaware district numbering)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Delaware-specific structure cleaned
        """
        self.logger.info("Cleaning Delaware-specific data structure")
        
        # Clean candidate names (Delaware-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_delaware_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Delaware-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_delaware_district)
        
        # Clean phone numbers (Delaware-specific logic)
        if 'phone' in df.columns:
            df['phone'] = df['phone'].apply(self._clean_delaware_phone)
        
        # Clean filing dates (Delaware-specific logic)
        if 'filing_date' in df.columns:
            df['filing_date'] = df['filing_date'].apply(self._clean_delaware_filing_date)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Delaware-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Delaware-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Delaware-specific content cleaned
        """
        self.logger.info("Cleaning Delaware-specific content")
        
        # County standardization removed - not needed
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        

        
        # Delaware-specific formatting
        df = self._apply_delaware_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Delaware-specific name parsing logic.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # Delaware name parsing - handles "Last, First Middle" format common in official records
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
                
                if len(parts) >= 2:
                    # ALL Delaware names follow "Last, First Middle" format
                    # First part is LAST NAME, second part is FIRST NAME
                    df.at[idx, 'last_name'] = parts[0]
                    df.at[idx, 'first_name'] = parts[1]
                    
                    if len(parts) > 2:
                        # Everything after the first two parts is middle name(s)
                        df.at[idx, 'middle_name'] = ' '.join(parts[2:])
                elif len(parts) == 1:
                    # Single name - treat as first name
                    df.at[idx, 'first_name'] = parts[0]
        
        return df
    
    def _clean_delaware_name(self, name: str) -> str:
        """
        Clean Delaware-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Delaware-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Delaware name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_delaware_address method removed - now handled centrally
    
    def _clean_delaware_district(self, district: str) -> str:
        """
        Clean Delaware-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Delaware-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Delaware district patterns
        # (e.g., "District 1", "LD 1", etc.)
        
        return district
    
    def _clean_delaware_phone(self, phone: str) -> str:
        """
        Clean Delaware-specific phone number formats.
        
        Args:
            phone: Raw phone string
            
        Returns:
            Cleaned phone string
        """
        if pd.isna(phone) or not phone:
            return None
        
        # Delaware-specific phone cleaning logic
        phone = str(phone).strip()
        
        # Handle common Delaware phone patterns
        # (e.g., area code formats, extension formats)
        
        return phone
    
    def _clean_delaware_filing_date(self, filing_date: str) -> str:
        """
        Clean Delaware-specific filing date formats.
        
        Args:
            filing_date: Raw filing date string
            
        Returns:
            Cleaned filing date string
        """
        if pd.isna(filing_date) or not filing_date:
            return None
        
        # Delaware-specific filing date cleaning logic
        filing_date = str(filing_date).strip()
        
        # Handle common Delaware date patterns
        # (e.g., MM/DD/YYYY, MM-DD-YYYY, etc.)
        
        return filing_date
    
    def _apply_delaware_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Delaware-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Delaware-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
