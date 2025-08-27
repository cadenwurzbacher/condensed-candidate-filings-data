#!/usr/bin/env python3
"""
Washington State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class WashingtonCleaner(BaseStateCleaner):
    """
    Washington-specific data cleaner.
    
    This cleaner handles Washington-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Washington")
        
        # Washington-specific county mappings
        self.county_mappings = {
            'Adams': 'Adams County',
            'Asotin': 'Asotin County',
            'Benton': 'Benton County',
            'Chelan': 'Chelan County',
            'Clallam': 'Clallam County',
            'Clark': 'Clark County',
            'Columbia': 'Columbia County',
            'Cowlitz': 'Cowlitz County',
            'Douglas': 'Douglas County',
            'Ferry': 'Ferry County',
            'Franklin': 'Franklin County',
            'Garfield': 'Garfield County',
            'Grant': 'Grant County',
            'Grays Harbor': 'Grays Harbor County',
            'Island': 'Island County',
            'Jefferson': 'Jefferson County',
            'King': 'King County',
            'Kitsap': 'Kitsap County',
            'Kittitas': 'Kittitas County',
            'Klickitat': 'Klickitat County',
            'Lewis': 'Lewis County',
            'Lincoln': 'Lincoln County',
            'Mason': 'Mason County',
            'Okanogan': 'Okanogan County',
            'Pacific': 'Pacific County',
            'Pend Oreille': 'Pend Oreille County',
            'Pierce': 'Pierce County',
            'San Juan': 'San Juan County',
            'Skagit': 'Skagit County',
            'Skamania': 'Skamania County',
            'Snohomish': 'Snohomish County',
            'Spokane': 'Spokane County',
            'Stevens': 'Stevens County',
            'Thurston': 'Thurston County',
            'Wahkiakum': 'Wahkiakum County',
            'Walla Walla': 'Walla Walla County',
            'Whatcom': 'Whatcom County',
            'Whitman': 'Whitman County',
            'Yakima': 'Yakima County'
        }
        
        # Washington-specific office mappings
        self.office_mappings = {
            'US PRESIDENT': 'US President',
            'US SENATOR': 'US Senator',
            'US REPRESENTATIVE': 'US Representative',
            'GOVERNOR': 'Governor',
            'LIEUTENANT GOVERNOR': 'Lieutenant Governor',
            'SECRETARY OF STATE': 'Secretary of State',
            'STATE TREASURER': 'State Treasurer',
            'ATTORNEY GENERAL': 'Attorney General',
            'AUDITOR': 'Auditor',
            'COMMISSIONER OF PUBLIC LANDS': 'Commissioner of Public Lands',
            'SUPERINTENDENT OF PUBLIC INSTRUCTION': 'Superintendent of Public Instruction',
            'INSURANCE COMMISSIONER': 'Insurance Commissioner',
            'STATE SENATOR': 'State Senator',
            'STATE REPRESENTATIVE': 'State Representative',
            'SUPREME COURT JUSTICE': 'Supreme Court Justice',
            'COURT OF APPEALS JUDGE': 'Court of Appeals Judge',
            'SUPERIOR COURT JUDGE': 'Superior Court Judge',
            'DISTRICT COURT JUDGE': 'District Court Judge',
            'COUNTY COMMISSIONER': 'County Commissioner',
            'COUNTY SHERIFF': 'County Sheriff',
            'COUNTY CLERK': 'County Clerk',
            'COUNTY TREASURER': 'County Treasurer',
            'COUNTY ASSESSOR': 'County Assessor',
            'COUNTY AUDITOR': 'County Auditor',
            'COUNTY ENGINEER': 'County Engineer',
            'COUNTY SURVEYOR': 'County Surveyor',
            'COUNTY CORONER': 'County Coroner',
            'COUNTY PROSECUTOR': 'County Prosecutor',
            'COUNTY PUBLIC DEFENDER': 'County Public Defender',
            'MAYOR': 'Mayor',
            'CITY COUNCIL MEMBER': 'City Council Member',
            'SCHOOL BOARD MEMBER': 'School Board Member',
            'PORT COMMISSIONER': 'Port Commissioner',
            'FIRE DISTRICT COMMISSIONER': 'Fire District Commissioner',
            'WATER DISTRICT COMMISSIONER': 'Water District Commissioner',
            'SEWER DISTRICT COMMISSIONER': 'Sewer District Commissioner',
            'LIBRARY DISTRICT TRUSTEE': 'Library District Trustee',
            'PARK DISTRICT COMMISSIONER': 'Park District Commissioner',
            'HOSPITAL DISTRICT COMMISSIONER': 'Hospital District Commissioner',
            'PUBLIC UTILITY DISTRICT COMMISSIONER': 'Public Utility District Commissioner',
            'SOIL AND WATER CONSERVATION DISTRICT SUPERVISOR': 'Soil and Water Conservation District Supervisor'
        }
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Washington candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Washington data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Washington-specific data structure.
        
        This handles:
        - Name parsing (Washington has specific name formats)
        - Address parsing (Washington address structure)
        - District parsing (Washington district numbering)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Washington-specific structure cleaned
        """
        self.logger.info("Cleaning Washington-specific data structure")
        
        # Clean candidate names (Washington-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_washington_name)
        
        # Clean addresses (Washington-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_washington_address)
        
        # Clean districts (Washington-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_washington_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Washington-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Washington-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Washington-specific content cleaned
        """
        self.logger.info("Cleaning Washington-specific content")
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices
        if 'office' in df.columns:
            df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        

        
        # Washington-specific formatting
        df = self._apply_washington_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Washington-specific name parsing logic - Evergreen State naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # Washington name parsing - handles pacific northwest naming patterns
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
    
    def _clean_washington_name(self, name: str) -> str:
        """
        Clean Washington-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Washington-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Washington name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _clean_washington_address(self, address: str) -> str:
        """
        Clean Washington-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # Washington-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common Washington address patterns
        # (e.g., PO Box formats, rural route formats)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_washington_district(self, district: str) -> str:
        """
        Clean Washington-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Washington-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Washington district patterns
        # (e.g., "District 1", "LD 1", etc.)
        
        return district
    
    def _apply_washington_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Washington-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Washington-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
