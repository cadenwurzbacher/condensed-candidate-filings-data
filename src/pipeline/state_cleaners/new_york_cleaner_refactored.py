#!/usr/bin/env python3
"""
New York State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class NewYorkCleaner(BaseStateCleaner):
    """
    New York-specific data cleaner.
    
    This cleaner handles New York-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("New York")
        
        # New York-specific county mappings
        self.county_mappings = {
            'Albany': 'Albany County',
            'Allegany': 'Allegany County',
            'Bronx': 'Bronx County',
            'Broome': 'Broome County',
            'Cattaraugus': 'Cattaraugus County',
            'Cayuga': 'Cayuga County',
            'Chautauqua': 'Chautauqua County',
            'Chemung': 'Chemung County',
            'Chenango': 'Chenango County',
            'Clinton': 'Clinton County',
            'Columbia': 'Columbia County',
            'Cortland': 'Cortland County',
            'Delaware': 'Delaware County',
            'Dutchess': 'Dutchess County',
            'Erie': 'Erie County',
            'Essex': 'Essex County',
            'Franklin': 'Franklin County',
            'Fulton': 'Fulton County',
            'Genesee': 'Genesee County',
            'Greene': 'Greene County',
            'Hamilton': 'Hamilton County',
            'Herkimer': 'Herkimer County',
            'Jefferson': 'Jefferson County',
            'Kings': 'Kings County',
            'Lewis': 'Lewis County',
            'Livingston': 'Livingston County',
            'Madison': 'Madison County',
            'Monroe': 'Monroe County',
            'Montgomery': 'Montgomery County',
            'Nassau': 'Nassau County',
            'New York': 'New York County',
            'Niagara': 'Niagara County',
            'Oneida': 'Oneida County',
            'Onondaga': 'Onondaga County',
            'Ontario': 'Ontario County',
            'Orange': 'Orange County',
            'Orleans': 'Orleans County',
            'Oswego': 'Oswego County',
            'Otsego': 'Otsego County',
            'Putnam': 'Putnam County',
            'Queens': 'Queens County',
            'Rensselaer': 'Rensselaer County',
            'Richmond': 'Richmond County',
            'Rockland': 'Rockland County',
            'Saratoga': 'Saratoga County',
            'Schenectady': 'Schenectady County',
            'Schoharie': 'Schoharie County',
            'Schuyler': 'Schuyler County',
            'Seneca': 'Seneca County',
            'St. Lawrence': 'St. Lawrence County',
            'Steuben': 'Steuben County',
            'Suffolk': 'Suffolk County',
            'Sullivan': 'Sullivan County',
            'Tioga': 'Tioga County',
            'Tompkins': 'Tompkins County',
            'Ulster': 'Ulster County',
            'Warren': 'Warren County',
            'Washington': 'Washington County',
            'Wayne': 'Wayne County',
            'Westchester': 'Westchester County',
            'Wyoming': 'Wyoming County',
            'Yates': 'Yates County'
        }
        
        # New York-specific office mappings
        self.office_mappings = {
            'US PRESIDENT': 'US President',
            'US SENATOR': 'US Senator',
            'US REPRESENTATIVE': 'US Representative',
            'GOVERNOR': 'Governor',
            'LIEUTENANT GOVERNOR': 'Lieutenant Governor',
            'SECRETARY OF STATE': 'Secretary of State',
            'STATE COMPTROLLER': 'State Comptroller',
            'ATTORNEY GENERAL': 'Attorney General',
            'STATE SENATOR': 'State Senator',
            'STATE ASSEMBLY MEMBER': 'State Assembly Member',
            'REGENT OF THE UNIVERSITY OF THE STATE OF NEW YORK': 'Regent of the University of the State of New York',
            'DISTRICT ATTORNEY': 'District Attorney',
            'COUNTY DISTRICT ATTORNEY': 'County District Attorney',
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
            'TOWN SUPERVISOR': 'Town Supervisor',
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
        Clean New York candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting New York data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean New York-specific data structure.
        
        This handles:
        - Name parsing (New York has specific name formats)
        - Address parsing (New York address structure)
        - District parsing (New York district numbering)
        - Borough-specific logic (NYC has unique structure)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with New York-specific structure cleaned
        """
        self.logger.info("Cleaning New York-specific data structure")
        
        # Clean candidate names (New York-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_new_york_name)
        
        # Clean addresses (New York-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_new_york_address)
        
        # Clean districts (New York-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_new_york_district)
        
        # Handle New York-specific borough logic
        df = self._handle_new_york_borough_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean New York-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - New York-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with New York-specific content cleaned
        """
        self.logger.info("Cleaning New York-specific content")
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices
        if 'office' in df.columns:
            df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # New York-specific formatting
        df = self._apply_new_york_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        New York-specific name parsing logic - Empire State naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # New York name parsing - handles northeastern naming patterns
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
    
    def _clean_new_york_name(self, name: str) -> str:
        """
        Clean New York-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # New York-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common New York name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _clean_new_york_address(self, address: str) -> str:
        """
        Clean New York-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # New York-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common New York address patterns
        # (e.g., PO Box formats, rural route formats, NYC apartment formats)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_new_york_district(self, district: str) -> str:
        """
        Clean New York-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # New York-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common New York district patterns
        # (e.g., "District 1", "AD 1", etc.)
        
        return district
    
    def _handle_new_york_borough_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle New York-specific borough logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # New York-specific borough handling logic
        # (e.g., NYC borough-specific processing, special district handling)
        
        return df
    
    def _apply_new_york_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply New York-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # New York-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
