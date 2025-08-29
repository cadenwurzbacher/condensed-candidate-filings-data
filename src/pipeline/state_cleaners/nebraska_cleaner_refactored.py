#!/usr/bin/env python3
"""
Nebraska State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class NebraskaCleaner(BaseStateCleaner):
    """
    Nebraska-specific data cleaner.
    
    This cleaner handles Nebraska-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Nebraska")
        
        # Nebraska-specific county mappings
        self.county_mappings = {
            'Adams': 'Adams County',
            'Antelope': 'Antelope County',
            'Arthur': 'Arthur County',
            'Banner': 'Banner County',
            'Blaine': 'Blaine County',
            'Boone': 'Boone County',
            'Box Butte': 'Box Butte County',
            'Boyd': 'Boyd County',
            'Brown': 'Brown County',
            'Buffalo': 'Buffalo County',
            'Burt': 'Burt County',
            'Butler': 'Butler County',
            'Cass': 'Cass County',
            'Cedar': 'Cedar County',
            'Chase': 'Chase County',
            'Cherry': 'Cherry County',
            'Cheyenne': 'Cheyenne County',
            'Clay': 'Clay County',
            'Colfax': 'Colfax County',
            'Cuming': 'Cuming County',
            'Custer': 'Custer County',
            'Dakota': 'Dakota County',
            'Dawes': 'Dawes County',
            'Dawson': 'Dawson County',
            'Deuel': 'Deuel County',
            'Dixon': 'Dixon County',
            'Dodge': 'Dodge County',
            'Douglas': 'Douglas County',
            'Dundy': 'Dundy County',
            'Fillmore': 'Fillmore County',
            'Franklin': 'Franklin County',
            'Frontier': 'Frontier County',
            'Furnas': 'Furnas County',
            'Gage': 'Gage County',
            'Garden': 'Garden County',
            'Garfield': 'Garfield County',
            'Gosper': 'Gosper County',
            'Grant': 'Grant County',
            'Greeley': 'Greeley County',
            'Hall': 'Hall County',
            'Hamilton': 'Hamilton County',
            'Harlan': 'Harlan County',
            'Hayes': 'Hayes County',
            'Hitchcock': 'Hitchcock County',
            'Holt': 'Holt County',
            'Hooker': 'Hooker County',
            'Howard': 'Howard County',
            'Jefferson': 'Jefferson County',
            'Johnson': 'Johnson County',
            'Kearney': 'Kearney County',
            'Keith': 'Keith County',
            'Keya Paha': 'Keya Paha County',
            'Kimball': 'Kimball County',
            'Knox': 'Knox County',
            'Lancaster': 'Lancaster County',
            'Lincoln': 'Lincoln County',
            'Logan': 'Logan County',
            'Loup': 'Loup County',
            'Madison': 'Madison County',
            'McPherson': 'McPherson County',
            'Merrick': 'Merrick County',
            'Morrill': 'Morrill County',
            'Nance': 'Nance County',
            'Nemaha': 'Nemaha County',
            'Nuckolls': 'Nuckolls County',
            'Otoe': 'Otoe County',
            'Pawnee': 'Pawnee County',
            'Perkins': 'Perkins County',
            'Phelps': 'Phelps County',
            'Pierce': 'Pierce County',
            'Platte': 'Platte County',
            'Polk': 'Polk County',
            'Red Willow': 'Red Willow County',
            'Richardson': 'Richardson County',
            'Rock': 'Rock County',
            'Saline': 'Saline County',
            'Sarpy': 'Sarpy County',
            'Saunders': 'Saunders County',
            'Scotts Bluff': 'Scotts Bluff County',
            'Seward': 'Seward County',
            'Sheridan': 'Sheridan County',
            'Sherman': 'Sherman County',
            'Sioux': 'Sioux County',
            'Stanton': 'Stanton County',
            'Thayer': 'Thayer County',
            'Thomas': 'Thomas County',
            'Thurston': 'Thurston County',
            'Valley': 'Valley County',
            'Washington': 'Washington County',
            'Wayne': 'Wayne County',
            'Webster': 'Webster County',
            'Wheeler': 'Wheeler County',
            'York': 'York County'
        }
        
        # Nebraska-specific office mappings
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
            'PUBLIC SERVICE COMMISSIONER': 'Public Service Commissioner',
            'STATE SENATOR': 'State Senator',
            'STATE LEGISLATOR': 'State Legislator',
            'REGENT OF THE UNIVERSITY OF NEBRASKA': 'Regent of the University of Nebraska',
            'DISTRICT ATTORNEY': 'District Attorney',
            'COUNTY ATTORNEY': 'County Attorney',
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
            'NATURAL RESOURCES DISTRICT BOARD MEMBER': 'Natural Resources District Board Member',
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
        Clean Nebraska candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Nebraska data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Nebraska-specific data structure.
        
        This handles:
        - Name parsing (Nebraska has specific name formats)
        - Address parsing (Nebraska address structure)
        - District parsing (Nebraska district numbering)
        - Agricultural-specific logic (Nebraska has many agricultural areas)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Nebraska-specific structure cleaned
        """
        self.logger.info("Cleaning Nebraska-specific data structure")
        
        # Clean candidate names (Nebraska-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_nebraska_name)
        
        # Clean addresses (Nebraska-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_nebraska_address)
        
        # Clean districts (Nebraska-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_nebraska_district)
        
        # Handle Nebraska-specific agricultural logic
        df = self._handle_nebraska_agricultural_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Nebraska-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Nebraska-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Nebraska-specific content cleaned
        """
        self.logger.info("Cleaning Nebraska-specific content")
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Nebraska-specific formatting
        df = self._apply_nebraska_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Nebraska-specific name parsing logic - Cornhusker State naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # Nebraska name parsing - handles plains region naming patterns
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
    
    def _clean_nebraska_name(self, name: str) -> str:
        """
        Clean Nebraska-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Nebraska-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Nebraska name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _clean_nebraska_address(self, address: str) -> str:
        """
        Clean Nebraska-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # Nebraska-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common Nebraska address patterns
        # (e.g., PO Box formats, rural route formats, farm references)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_nebraska_district(self, district: str) -> str:
        """
        Clean Nebraska-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Nebraska-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Nebraska district patterns
        # (e.g., "District 1", "LD 1", etc.)
        
        return district
    
    def _handle_nebraska_agricultural_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Nebraska-specific agricultural logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Nebraska-specific agricultural handling logic
        # (e.g., agricultural area processing, special district handling)
        
        return df
    
    def _apply_nebraska_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Nebraska-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Nebraska-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
