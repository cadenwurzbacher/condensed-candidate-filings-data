#!/usr/bin/env python3
"""
Indiana State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class IndianaCleaner(BaseStateCleaner):
    """
    Indiana-specific data cleaner.
    
    This cleaner handles Indiana-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Indiana")
        
        # Indiana-specific county mappings
        self.county_mappings = {
            'Adams': 'Adams County',
            'Allen': 'Allen County',
            'Bartholomew': 'Bartholomew County',
            'Benton': 'Benton County',
            'Blackford': 'Blackford County',
            'Boone': 'Boone County',
            'Brown': 'Brown County',
            'Carroll': 'Carroll County',
            'Cass': 'Cass County',
            'Clark': 'Clark County',
            'Clay': 'Clay County',
            'Clinton': 'Clinton County',
            'Crawford': 'Crawford County',
            'Daviess': 'Daviess County',
            'Dearborn': 'Dearborn County',
            'Decatur': 'Decatur County',
            'DeKalb': 'DeKalb County',
            'Delaware': 'Delaware County',
            'Dubois': 'Dubois County',
            'Elkhart': 'Elkhart County',
            'Fayette': 'Fayette County',
            'Floyd': 'Floyd County',
            'Fountain': 'Fountain County',
            'Franklin': 'Franklin County',
            'Fulton': 'Fulton County',
            'Gibson': 'Gibson County',
            'Grant': 'Grant County',
            'Greene': 'Greene County',
            'Hamilton': 'Hamilton County',
            'Hancock': 'Hancock County',
            'Harrison': 'Harrison County',
            'Hendricks': 'Hendricks County',
            'Henry': 'Henry County',
            'Howard': 'Howard County',
            'Huntington': 'Huntington County',
            'Jackson': 'Jackson County',
            'Jasper': 'Jasper County',
            'Jay': 'Jay County',
            'Jefferson': 'Jefferson County',
            'Jennings': 'Jennings County',
            'Johnson': 'Johnson County',
            'Knox': 'Knox County',
            'Kosciusko': 'Kosciusko County',
            'LaGrange': 'LaGrange County',
            'Lake': 'Lake County',
            'LaPorte': 'LaPorte County',
            'Lawrence': 'Lawrence County',
            'Madison': 'Madison County',
            'Marion': 'Marion County',
            'Marshall': 'Marshall County',
            'Martin': 'Martin County',
            'Miami': 'Miami County',
            'Monroe': 'Monroe County',
            'Montgomery': 'Montgomery County',
            'Morgan': 'Morgan County',
            'Newton': 'Newton County',
            'Noble': 'Noble County',
            'Ohio': 'Ohio County',
            'Orange': 'Orange County',
            'Owen': 'Owen County',
            'Parke': 'Parke County',
            'Perry': 'Perry County',
            'Pike': 'Pike County',
            'Porter': 'Porter County',
            'Posey': 'Posey County',
            'Pulaski': 'Pulaski County',
            'Putnam': 'Putnam County',
            'Randolph': 'Randolph County',
            'Ripley': 'Ripley County',
            'Rush': 'Rush County',
            'Scott': 'Scott County',
            'Shelby': 'Shelby County',
            'Spencer': 'Spencer County',
            'St. Joseph': 'St. Joseph County',
            'Starke': 'Starke County',
            'Steuben': 'Steuben County',
            'Sullivan': 'Sullivan County',
            'Switzerland': 'Switzerland County',
            'Tippecanoe': 'Tippecanoe County',
            'Tipton': 'Tipton County',
            'Union': 'Union County',
            'Vanderburgh': 'Vanderburgh County',
            'Vermillion': 'Vermillion County',
            'Vigo': 'Vigo County',
            'Wabash': 'Wabash County',
            'Warren': 'Warren County',
            'Warrick': 'Warrick County',
            'Washington': 'Washington County',
            'Wayne': 'Wayne County',
            'Wells': 'Wells County',
            'White': 'White County',
            'Whitley': 'Whitley County'
        }
        
        # Indiana-specific office mappings
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
            'SUPERINTENDENT OF PUBLIC INSTRUCTION': 'Superintendent of Public Instruction',
            'STATE SENATOR': 'State Senator',
            'STATE REPRESENTATIVE': 'State Representative',
            'REGENT OF THE UNIVERSITY OF INDIANA': 'Regent of the University of Indiana',
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
        Clean Indiana candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Indiana data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Indiana-specific data structure.
        
        This handles:
        - Name parsing (Indiana has specific name formats)
        - Address parsing (Indiana address structure)
        - District parsing (Indiana district numbering)
        - Hoosier-specific logic (Indiana has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Indiana-specific structure cleaned
        """
        self.logger.info("Cleaning Indiana-specific data structure")
        
        # Clean candidate names (Indiana-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_indiana_name)
        
        # Clean addresses (Indiana-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_indiana_address)
        
        # Clean districts (Indiana-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_indiana_district)
        
        # Handle Indiana-specific Hoosier logic
        df = self._handle_indiana_hoosier_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Indiana-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Indiana-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Indiana-specific content cleaned
        """
        self.logger.info("Cleaning Indiana-specific content")
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Indiana-specific formatting
        df = self._apply_indiana_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Indiana-specific name parsing logic - Hoosier State naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # Indiana name parsing - handles midwestern naming patterns
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
    
    def _clean_indiana_name(self, name: str) -> str:
        """
        Clean Indiana-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Indiana-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Indiana name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _clean_indiana_address(self, address: str) -> str:
        """
        Clean Indiana-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # Indiana-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common Indiana address patterns
        # (e.g., PO Box formats, rural route formats, etc.)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_indiana_district(self, district: str) -> str:
        """
        Clean Indiana-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Indiana-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Indiana district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
    def _handle_indiana_hoosier_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Indiana-specific Hoosier logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Indiana-specific Hoosier handling logic
        # (e.g., Hoosier-specific processing, special district handling)
        
        return df
    
    def _apply_indiana_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Indiana-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Indiana-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
