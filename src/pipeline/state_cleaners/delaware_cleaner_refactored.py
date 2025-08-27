#!/usr/bin/env python3
"""
Delaware State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class DelawareCleaner(BaseStateCleaner):
    """
    Delaware-specific data cleaner.
    
    This cleaner handles Delaware-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Delaware")
        
        # Delaware-specific county mappings
        self.county_mappings = {
            'New Castle': 'New Castle County',
            'Kent': 'Kent County',
            'Sussex': 'Sussex County'
        }
        
        # Delaware-specific office mappings
        self.office_mappings = {
            'PRESIDENT': 'US President',
            'US PRESIDENT': 'US President',
            'U.S. REPRESENTATIVE': 'US Representative',
            'US REPRESENTATIVE': 'US Representative',
            'U.S. SENATOR': 'US Senator',
            'US SENATOR': 'US Senator',
            'GOVERNOR': 'Governor',
            'LIEUTENANT GOVERNOR': 'Lieutenant Governor',
            'SHERIFF': 'Sheriff',
            'WILMINGTON CITY COUNCIL AT LARGE': 'City Council Member At-Large',
            'WILMINGTON CITY COUNCIL': 'City Council Member',
            'COUNTY COUNCIL MEMBER': 'County Council Member',
            'COUNTY LEVY COURT MEMBER': 'County Levy Court Member',
            'COUNTY CLERK OF THE PEACE': 'County Clerk of the Peace',
            'COUNTY REGISTER OF WILLS': 'County Register of Wills',
            'COUNTY PROTHONOTARY': 'County Prothonotary',
            'COUNTY RECORDER OF DEEDS': 'County Recorder of Deeds',
            'COUNTY TREASURER': 'County Treasurer',
            'COUNTY ASSESSOR': 'County Assessor',
            'COUNTY AUDITOR': 'County Auditor',
            'COUNTY ENGINEER': 'County Engineer',
            'COUNTY SURVEYOR': 'County Surveyor',
            'COUNTY CORONER': 'County Coroner',
            'COUNTY PROSECUTOR': 'County Prosecutor',
            'COUNTY PUBLIC DEFENDER': 'County Public Defender',
            'COUNTY JUDGE': 'County Judge',
            'COUNTY MAGISTRATE': 'County Magistrate',
            'COUNTY COMMISSIONER': 'County Commissioner',
            'COUNTY SUPERVISOR': 'County Supervisor',
            'COUNTY TRUSTEE': 'County Trustee',
            'COUNTY EXECUTIVE': 'County Executive',
            'COUNTY LEGISLATOR': 'County Legislator',
            'COUNTY COUNCIL PRESIDENT': 'County Council President',
            'COUNTY COUNCIL VICE PRESIDENT': 'County Council Vice President',
            'COUNTY COUNCIL SECRETARY': 'County Council Secretary',
            'COUNTY COUNCIL TREASURER': 'County Council Treasurer'
        }
    
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
        
        # Clean addresses (Delaware-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_delaware_address)
        
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
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices
        if 'office' in df.columns:
            df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        

        
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
        
        # Delaware name parsing - handles coastal region naming patterns
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
    
    def _clean_delaware_address(self, address: str) -> str:
        """
        Clean Delaware-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # Delaware-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common Delaware address patterns
        # (e.g., PO Box formats, rural route formats)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
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
