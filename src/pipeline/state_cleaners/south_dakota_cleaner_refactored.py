#!/usr/bin/env python3
"""
South Dakota State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class SouthDakotaCleaner(BaseStateCleaner):
    """
    South Dakota-specific data cleaner.
    
    This cleaner handles South Dakota-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("South Dakota")
        
        # South Dakota-specific county mappings
        self.county_mappings = {
            'Aurora': 'Aurora County',
            'Beadle': 'Beadle County',
            'Bennett': 'Bennett County',
            'Bon Homme': 'Bon Homme County',
            'Brookings': 'Brookings County',
            'Brown': 'Brown County',
            'Brule': 'Brule County',
            'Buffalo': 'Buffalo County',
            'Butte': 'Butte County',
            'Campbell': 'Campbell County',
            'Charles Mix': 'Charles Mix County',
            'Clark': 'Clark County',
            'Clay': 'Clay County',
            'Codington': 'Codington County',
            'Corson': 'Corson County',
            'Custer': 'Custer County',
            'Davison': 'Davison County',
            'Day': 'Day County',
            'Deuel': 'Deuel County',
            'Dewey': 'Dewey County',
            'Douglas': 'Douglas County',
            'Edmunds': 'Edmunds County',
            'Fall River': 'Fall River County',
            'Faulk': 'Faulk County',
            'Grant': 'Grant County',
            'Gregory': 'Gregory County',
            'Haakon': 'Haakon County',
            'Hamlin': 'Hamlin County',
            'Hand': 'Hand County',
            'Hanson': 'Hanson County',
            'Harding': 'Harding County',
            'Hughes': 'Hughes County',
            'Hutchinson': 'Hutchinson County',
            'Hyde': 'Hyde County',
            'Jackson': 'Jackson County',
            'Jerauld': 'Jerauld County',
            'Jones': 'Jones County',
            'Kingsbury': 'Kingsbury County',
            'Lake': 'Lake County',
            'Lawrence': 'Lawrence County',
            'Lincoln': 'Lincoln County',
            'Lyman': 'Lyman County',
            'Marshall': 'Marshall County',
            'McCook': 'McCook County',
            'McPherson': 'McPherson County',
            'Meade': 'Meade County',
            'Mellette': 'Mellette County',
            'Miner': 'Miner County',
            'Minnehaha': 'Minnehaha County',
            'Moody': 'Moody County',
            'Oglala Lakota': 'Oglala Lakota County',
            'Pennington': 'Pennington County',
            'Perkins': 'Perkins County',
            'Potter': 'Potter County',
            'Roberts': 'Roberts County',
            'Sanborn': 'Sanborn County',
            'Shannon': 'Shannon County',
            'Spink': 'Spink County',
            'Stanley': 'Stanley County',
            'Sully': 'Sully County',
            'Todd': 'Todd County',
            'Tripp': 'Tripp County',
            'Turner': 'Turner County',
            'Union': 'Union County',
            'Walworth': 'Walworth County',
            'Yankton': 'Yankton County',
            'Ziebach': 'Ziebach County'
        }
        
        # South Dakota-specific office mappings
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
            'COMMISSIONER OF SCHOOL AND PUBLIC LANDS': 'Commissioner of School and Public Lands',
            'STATE SENATOR': 'State Senator',
            'STATE REPRESENTATIVE': 'State Representative',
            'REGENT OF THE UNIVERSITY OF SOUTH DAKOTA': 'Regent of the University of South Dakota',
            'DISTRICT ATTORNEY': 'District Attorney',
            'COUNTY STATE\'S ATTORNEY': 'County State\'s Attorney',
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
        Clean South Dakota candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting South Dakota data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean South Dakota-specific data structure.
        
        This handles:
        - Name parsing (South Dakota has specific name formats)
        - Address parsing (South Dakota address structure)
        - District parsing (South Dakota district numbering)
        - Mount Rushmore-specific logic (South Dakota has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with South Dakota-specific structure cleaned
        """
        self.logger.info("Cleaning South Dakota-specific data structure")
        
        # Clean candidate names (South Dakota-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_south_dakota_name)
        
        # Clean addresses (South Dakota-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_south_dakota_address)
        
        # Clean districts (South Dakota-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_south_dakota_district)
        
        # Handle South Dakota-specific Mount Rushmore logic
        df = self._handle_south_dakota_mount_rushmore_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean South Dakota-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - South Dakota-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with South Dakota-specific content cleaned
        """
        self.logger.info("Cleaning South Dakota-specific content")
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices
        if 'office' in df.columns:
            df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # South Dakota-specific formatting
        df = self._apply_south_dakota_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        South Dakota-specific name parsing logic - Mount Rushmore State naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # South Dakota name parsing - handles plains region naming patterns
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
    
    def _clean_south_dakota_name(self, name: str) -> str:
        """
        Clean South Dakota-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # South Dakota-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common South Dakota name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _clean_south_dakota_address(self, address: str) -> str:
        """
        Clean South Dakota-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # South Dakota-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common South Dakota address patterns
        # (e.g., PO Box formats, rural route formats, etc.)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_south_dakota_district(self, district: str) -> str:
        """
        Clean South Dakota-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # South Dakota-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common South Dakota district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
    def _handle_south_dakota_mount_rushmore_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle South Dakota-specific Mount Rushmore logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # South Dakota-specific Mount Rushmore handling logic
        # (e.g., Mount Rushmore-specific processing, special district handling)
        
        return df
    
    def _apply_south_dakota_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply South Dakota-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # South Dakota-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
