#!/usr/bin/env python3
"""
Illinois State Cleaner

Handles 
Illinois-specific data cleaning while inheriting common functionality
from BaseStateCleaner.
"""

import pandas as pd
from .base_cleaner import BaseStateCleaner

class IllinoisCleaner(BaseStateCleaner):
    """
    Illinois-specific data cleaner.
    
    This cleaner handles Illinois-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Illinois")
        
        # County mappings removed - not needed
        
        # Illinois-specific office mappings
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Illinois-specific data structure.
        
        This handles:
        - Name parsing (Illinois has specific name formats)
        - Address parsing (Illinois address structure)
        - District parsing (Illinois district numbering)
        - Complex party handling
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Illinois-specific structure cleaned
        """
        self.logger.info("Cleaning Illinois-specific data structure")
        
        # Clean candidate names (Illinois-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_illinois_name)
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Clean districts (Illinois-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_illinois_district)
        
        # Clean phone numbers (Illinois-specific logic)
        if 'phone' in df.columns:
            df['phone'] = df['phone'].apply(self._clean_illinois_phone)
        
        # Clean filing dates (Illinois-specific logic)
        if 'filing_date' in df.columns:
            df['filing_date'] = df['filing_date'].apply(self._clean_illinois_filing_date)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Illinois-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Illinois-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Illinois-specific content cleaned
        """
        self.logger.info("Cleaning Illinois-specific content")
        
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
    
    def _clean_illinois_name(self, name: str) -> str:
        """Clean Illinois name formats using standard base class logic."""
        return self._standard_name_cleaning(name)
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_illinois_address method removed - now handled centrally
    
    def _clean_illinois_district(self, district: str) -> str:
        """Clean Illinois district - just basic cleanup."""
        if pd.isna(district) or not district:
            return None
        return str(district).strip()
    
    def _clean_illinois_phone(self, phone: str) -> str:
        """
        Clean Illinois-specific phone number formats.
        
        Args:
            phone: Raw phone string
            
        Returns:
            Cleaned phone string
        """
        if pd.isna(phone) or not phone:
            return None
        
        # Illinois-specific phone cleaning logic
        phone = str(phone).strip()
        
        # Handle common Illinois phone patterns
        # (e.g., area code formats, extension formats)
        
        return phone
    
    def _clean_illinois_filing_date(self, filing_date: str) -> str:
        """
        Clean Illinois-specific filing date formats.
        
        Args:
            filing_date: Raw filing date string
            
        Returns:
            Cleaned filing date string
        """
        if pd.isna(filing_date) or not filing_date:
            return None
        
        # Illinois-specific filing date cleaning logic
        filing_date = str(filing_date).strip()
        
        # Handle common Illinois date patterns
        # (e.g., MM/DD/YYYY, MM-DD-YYYY, etc.)
        
        return filing_date

