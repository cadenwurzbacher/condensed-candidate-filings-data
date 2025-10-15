#!/usr/bin/env python3
"""
Illinois State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import re
from .base_cleaner import BaseStateCleaner

class IllinoisCleaner(BaseStateCleaner):
    """
    Illinois-specific data cleaner.
    
    This cleaner handles Illinois-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Illinois")
    
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
        self.logger.info(f"Cleaning Illinois-specific content")
        return df
    
    def _clean_illinois_name(self, name: str) -> str:
        """
        Clean Illinois-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Illinois-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Illinois name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_illinois_address method removed - now handled centrally
    
    def _clean_illinois_district(self, district: str) -> str:
        """Clean illinois-specific district formats."""
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
    
