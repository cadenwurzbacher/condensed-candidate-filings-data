#!/usr/bin/env python3
"""
West Virginia State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class WestVirginiaCleaner(BaseStateCleaner):
    """
    West Virginia-specific data cleaner.
    
    This cleaner handles West Virginia-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("West Virginia")
        
        # West Virginia-specific county mappings
        self.county_mappings = {
            'Barbour': 'Barbour County',
            'Berkeley': 'Berkeley County',
            'Boone': 'Boone County',
            'Braxton': 'Braxton County',
            'Brooke': 'Brooke County',
            'Cabell': 'Cabell County',
            'Calhoun': 'Calhoun County',
            'Clay': 'Clay County',
            'Doddridge': 'Doddridge County',
            'Fayette': 'Fayette County',
            'Gilmer': 'Gilmer County',
            'Grant': 'Grant County',
            'Greenbrier': 'Greenbrier County',
            'Hampshire': 'Hampshire County',
            'Hancock': 'Hancock County',
            'Hardy': 'Hardy County',
            'Harrison': 'Harrison County',
            'Jackson': 'Jackson County',
            'Jefferson': 'Jefferson County',
            'Kanawha': 'Kanawha County',
            'Lewis': 'Lewis County',
            'Lincoln': 'Lincoln County',
            'Logan': 'Logan County',
            'Marion': 'Marion County',
            'Marshall': 'Marshall County',
            'Mason': 'Mason County',
            'McDowell': 'McDowell County',
            'Mercer': 'Mercer County',
            'Mineral': 'Mineral County',
            'Mingo': 'Mingo County',
            'Monongalia': 'Monongalia County',
            'Monroe': 'Monroe County',
            'Morgan': 'Morgan County',
            'Nicholas': 'Nicholas County',
            'Ohio': 'Ohio County',
            'Pendleton': 'Pendleton County',
            'Pleasants': 'Pleasants County',
            'Pocahontas': 'Pocahontas County',
            'Preston': 'Preston County',
            'Putnam': 'Putnam County',
            'Raleigh': 'Raleigh County',
            'Randolph': 'Randolph County',
            'Ritchie': 'Ritchie County',
            'Roane': 'Roane County',
            'Summers': 'Summers County',
            'Taylor': 'Taylor County',
            'Tucker': 'Tucker County',
            'Tyler': 'Tyler County',
            'Upshur': 'Upshur County',
            'Wayne': 'Wayne County',
            'Webster': 'Webster County',
            'Wetzel': 'Wetzel County',
            'Wirt': 'Wirt County',
            'Wood': 'Wood County',
            'Wyoming': 'Wyoming County'
        }
        
        # West Virginia-specific office mappings
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
            'SUPERINTENDENT OF SCHOOLS': 'Superintendent of Schools',
            'COMMISSIONER OF AGRICULTURE': 'Commissioner of Agriculture',
            'STATE SENATOR': 'State Senator',
            'STATE DELEGATE': 'State Delegate',
            'REGENT OF THE UNIVERSITY OF WEST VIRGINIA': 'Regent of the University of West Virginia',
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
            'COUNTY JUDGE': 'County Judge',
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
        Clean West Virginia candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting West Virginia data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean West Virginia-specific data structure.
        
        This handles:
        - Name parsing (West Virginia has specific name formats)
        - Address parsing (West Virginia address structure)
        - District parsing (West Virginia district numbering)
        - Mountain State-specific logic (West Virginia has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with West Virginia-specific structure cleaned
        """
        self.logger.info("Cleaning West Virginia-specific data structure")
        
        # Clean candidate names (West Virginia-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_west_virginia_name)
        
        # Clean addresses (West Virginia-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_west_virginia_address)
        
        # Clean districts (West Virginia-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_west_virginia_district)
        
        # Handle West Virginia-specific Mountain State logic
        df = self._handle_west_virginia_mountain_state_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean West Virginia-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - West Virginia-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with West Virginia-specific content cleaned
        """
        self.logger.info("Cleaning West Virginia-specific content")
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # West Virginia-specific formatting
        df = self._apply_west_virginia_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        West Virginia-specific name parsing logic - Mountain State naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # West Virginia name parsing - handles mountain region naming patterns
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
    
    def _clean_west_virginia_name(self, name: str) -> str:
        """
        Clean West Virginia-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # West Virginia-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common West Virginia name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _clean_west_virginia_address(self, address: str) -> str:
        """
        Clean West Virginia-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # West Virginia-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common West Virginia address patterns
        # (e.g., PO Box formats, rural route formats, etc.)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_west_virginia_district(self, district: str) -> str:
        """
        Clean West Virginia-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # West Virginia-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common West Virginia district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
    def _handle_west_virginia_mountain_state_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle West Virginia-specific Mountain State logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # West Virginia-specific Mountain State handling logic
        # (e.g., Mountain State-specific processing, special district handling)
        
        return df
    
    def _apply_west_virginia_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply West Virginia-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # West Virginia-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
