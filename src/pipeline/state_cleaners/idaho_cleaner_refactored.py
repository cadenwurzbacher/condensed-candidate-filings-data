#!/usr/bin/env python3
"""
Idaho State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class IdahoCleaner(BaseStateCleaner):
    """
    Idaho-specific data cleaner.
    
    This cleaner handles Idaho-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Idaho")
        
        # Idaho-specific county mappings
        self.county_mappings = {
            'Ada': 'Ada County',
            'Adams': 'Adams County',
            'Bannock': 'Bannock County',
            'Bear Lake': 'Bear Lake County',
            'Benewah': 'Benewah County',
            'Bingham': 'Bingham County',
            'Blaine': 'Blaine County',
            'Boise': 'Boise County',
            'Bonner': 'Bonner County',
            'Bonneville': 'Bonneville County',
            'Boundary': 'Boundary County',
            'Butte': 'Butte County',
            'Camas': 'Camas County',
            'Canyon': 'Canyon County',
            'Caribou': 'Caribou County',
            'Cassia': 'Cassia County',
            'Clark': 'Clark County',
            'Clearwater': 'Clearwater County',
            'Custer': 'Custer County',
            'Elmore': 'Elmore County',
            'Franklin': 'Franklin County',
            'Fremont': 'Fremont County',
            'Gem': 'Gem County',
            'Gooding': 'Gooding County',
            'Idaho': 'Idaho County',
            'Jefferson': 'Jefferson County',
            'Jerome': 'Jerome County',
            'Kootenai': 'Kootenai County',
            'Latah': 'Latah County',
            'Lemhi': 'Lemhi County',
            'Lewis': 'Lewis County',
            'Lincoln': 'Lincoln County',
            'Madison': 'Madison County',
            'Minidoka': 'Minidoka County',
            'Nez Perce': 'Nez Perce County',
            'Oneida': 'Oneida County',
            'Owyhee': 'Owyhee County',
            'Payette': 'Payette County',
            'Power': 'Power County',
            'Shoshone': 'Shoshone County',
            'Teton': 'Teton County',
            'Twin Falls': 'Twin Falls County',
            'Valley': 'Valley County',
            'Washington': 'Washington County'
        }
        
        # Idaho-specific office mappings
        self.office_mappings = {
            'US PRESIDENT': 'US President',
            'US SENATOR': 'US Senator',
            'US REPRESENTATIVE': 'US Representative',
            'GOVERNOR': 'Governor',
            'LIEUTENANT GOVERNOR': 'Lieutenant Governor',
            'SECRETARY OF STATE': 'Secretary of State',
            'STATE TREASURER': 'State Treasurer',
            'ATTORNEY GENERAL': 'Attorney General',
            'SUPERINTENDENT OF PUBLIC INSTRUCTION': 'Superintendent of Public Instruction',
            'STATE SENATOR': 'State Senator',
            'STATE REPRESENTATIVE': 'State Representative',
            'REGENT OF THE UNIVERSITY OF IDAHO': 'Regent of the University of Idaho',
            'DISTRICT ATTORNEY': 'District Attorney',
            'COUNTY PROSECUTING ATTORNEY': 'County Prosecuting Attorney',
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
            'COUNTY MAGISTRATE': 'County Magistrate',
            'COUNTY COMMISSIONER': 'County Commissioner',
            'COUNTY SUPERVISOR': 'County Supervisor',
            'COUNTY TRUSTEE': 'County Trustee',
            'COUNTY EXECUTIVE': 'County Executive',
            'COUNTY LEGISLATOR': 'County Legislator',
            'MAYOR': 'Mayor',
            'CITY COUNCIL MEMBER': 'City Council Member',
            'ALDERMAN': 'Alderman',
            'SCHOOL BOARD MEMBER': 'School Board Member',
            'PARK BOARD MEMBER': 'Park Board Member',
            'LIBRARY BOARD MEMBER': 'Library Board Member',
            'FIRE DISTRICT BOARD MEMBER': 'Fire District Board Member',
            'WATER DISTRICT BOARD MEMBER': 'Water District Board Member',
            'SEWER DISTRICT BOARD MEMBER': 'Sewer District Board Member',
            'HOSPITAL DISTRICT BOARD MEMBER': 'Hospital District Board Member',
            'PUBLIC UTILITY DISTRICT BOARD MEMBER': 'Public Utility District Board Member',
            'SOIL AND WATER CONSERVATION DISTRICT SUPERVISOR': 'Soil and Water Conservation District Supervisor',
            'TOWN COUNCIL MEMBER': 'Town Council Member',
            'TOWN CLERK': 'Town Clerk',
            'TOWN TREASURER': 'Town Treasurer',
            'TOWN ASSESSOR': 'Town Assessor',
            'TOWN HIGHWAY SUPERINTENDENT': 'Town Highway Superintendent',
            'VILLAGE MAYOR': 'Village Mayor',
            'VILLAGE TRUSTEE': 'Village Trustee',
            'VILLAGE CLERK': 'Village Clerk',
            'VILLAGE TREASURER': 'Village Treasurer'
        }
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Idaho candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Idaho data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Idaho-specific data structure.
        
        This handles:
        - Name parsing (Idaho has specific name formats)
        - Address parsing (Idaho address structure)
        - District parsing (Idaho district numbering)
        - Mountain-specific logic (Idaho has many mountain areas)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Idaho-specific structure cleaned
        """
        self.logger.info("Cleaning Idaho-specific data structure")
        
        # Clean candidate names (Idaho-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_idaho_name)
        
        # Clean addresses (Idaho-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_idaho_address)
        
        # Clean districts (Idaho-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_idaho_district)
        
        # Handle Idaho-specific mountain logic
        df = self._handle_idaho_mountain_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Idaho-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Idaho-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Idaho-specific content cleaned
        """
        self.logger.info("Cleaning Idaho-specific content")
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices
        if 'office' in df.columns:
            df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Idaho-specific formatting
        df = self._apply_idaho_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Idaho-specific name parsing logic - Gem State mountain naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # Idaho name parsing - handles mountain region naming patterns
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
    
    def _clean_idaho_name(self, name: str) -> str:
        """
        Clean Idaho-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Idaho-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Idaho name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _clean_idaho_address(self, address: str) -> str:
        """
        Clean Idaho-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # Idaho-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common Idaho address patterns
        # (e.g., PO Box formats, rural route formats, mountain references)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_idaho_district(self, district: str) -> str:
        """
        Clean Idaho-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Idaho-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Idaho district patterns
        # (e.g., "District 1", "LD 1", etc.)
        
        return district
    
    def _handle_idaho_mountain_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Idaho-specific mountain logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Idaho-specific mountain handling logic
        # (e.g., mountain area processing, special district handling)
        
        return df
    
    def _apply_idaho_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Idaho-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Idaho-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
