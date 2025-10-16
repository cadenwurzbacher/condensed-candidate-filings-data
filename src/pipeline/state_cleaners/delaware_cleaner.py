#!/usr/bin/env python3
"""
Delaware State Cleaner

Handles 
Delaware-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
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
        

        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse candidate names into components using base class standard parsing."""
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None

        for idx, row in df.iterrows():
            candidate_name = row.get('candidate_name')
            if pd.notna(candidate_name) and str(candidate_name).strip():
                first, middle, last, prefix, suffix, nickname = self._parse_name_parts(candidate_name)
                df.at[idx, 'first_name'] = first
                df.at[idx, 'middle_name'] = middle
                df.at[idx, 'last_name'] = last
                df.at[idx, 'prefix'] = prefix
                df.at[idx, 'suffix'] = suffix
                df.at[idx, 'nickname'] = nickname
                df.at[idx, 'full_name_display'] = self._build_display_name(first, middle, last, prefix, suffix, nickname)

        return df
    
    def _clean_delaware_name(self, name: str) -> str:
        """Clean Delaware name formats using standard base class logic."""
        return self._standard_name_cleaning(name)
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_delaware_address method removed - now handled centrally
    
    def _clean_delaware_district(self, district: str) -> str:
        """Clean Delaware district - just basic cleanup."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
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

