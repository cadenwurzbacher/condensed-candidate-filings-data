#!/usr/bin/env python3
"""
Illinois State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class IllinoisCleaner(BaseStateCleaner):
    """
    Illinois-specific data cleaner.
    
    This cleaner handles Illinois-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Illinois")
        
        # Illinois-specific county mappings
        self.county_mappings = {
            'Adams': 'Adams County',
            'Alexander': 'Alexander County',
            'Bond': 'Bond County',
            'Boone': 'Boone County',
            'Brown': 'Brown County',
            'Bureau': 'Bureau County',
            'Calhoun': 'Calhoun County',
            'Carroll': 'Carroll County',
            'Cass': 'Cass County',
            'Champaign': 'Champaign County',
            'Christian': 'Christian County',
            'Clark': 'Clark County',
            'Clay': 'Clay County',
            'Clinton': 'Clinton County',
            'Coles': 'Coles County',
            'Cook': 'Cook County',
            'Crawford': 'Crawford County',
            'Cumberland': 'Cumberland County',
            'DeKalb': 'DeKalb County',
            'DeWitt': 'DeWitt County',
            'Douglas': 'Douglas County',
            'DuPage': 'DuPage County',
            'Edgar': 'Edgar County',
            'Edwards': 'Edwards County',
            'Effingham': 'Effingham County',
            'Fayette': 'Fayette County',
            'Ford': 'Ford County',
            'Franklin': 'Franklin County',
            'Fulton': 'Fulton County',
            'Gallatin': 'Gallatin County',
            'Greene': 'Greene County',
            'Grundy': 'Grundy County',
            'Hamilton': 'Hamilton County',
            'Hancock': 'Hancock County',
            'Hardin': 'Hardin County',
            'Henderson': 'Henderson County',
            'Henry': 'Henry County',
            'Iroquois': 'Iroquois County',
            'Jackson': 'Jackson County',
            'Jasper': 'Jasper County',
            'Jefferson': 'Jefferson County',
            'Jersey': 'Jersey County',
            'Jo Daviess': 'Jo Daviess County',
            'Johnson': 'Johnson County',
            'Kane': 'Kane County',
            'Kankakee': 'Kankakee County',
            'Kendall': 'Kendall County',
            'Knox': 'Knox County',
            'Lake': 'Lake County',
            'LaSalle': 'LaSalle County',
            'Lawrence': 'Lawrence County',
            'Lee': 'Lee County',
            'Livingston': 'Livingston County',
            'Logan': 'Logan County',
            'Macon': 'Macon County',
            'Macoupin': 'Macoupin County',
            'Madison': 'Madison County',
            'Marion': 'Marion County',
            'Marshall': 'Marshall County',
            'Mason': 'Mason County',
            'Massac': 'Massac County',
            'McDonough': 'McDonough County',
            'McHenry': 'McHenry County',
            'McLean': 'McLean County',
            'Menard': 'Menard County',
            'Mercer': 'Mercer County',
            'Monroe': 'Monroe County',
            'Montgomery': 'Montgomery County',
            'Morgan': 'Morgan County',
            'Moultrie': 'Moultrie County',
            'Ogle': 'Ogle County',
            'Peoria': 'Peoria County',
            'Perry': 'Perry County',
            'Piatt': 'Piatt County',
            'Pike': 'Pike County',
            'Pope': 'Pope County',
            'Pulaski': 'Pulaski County',
            'Putnam': 'Putnam County',
            'Randolph': 'Randolph County',
            'Richland': 'Richland County',
            'Rock Island': 'Rock Island County',
            'Saline': 'Saline County',
            'Sangamon': 'Sangamon County',
            'Schuyler': 'Schuyler County',
            'Scott': 'Scott County',
            'Shelby': 'Shelby County',
            'St. Clair': 'St. Clair County',
            'Stark': 'Stark County',
            'Stephenson': 'Stephenson County',
            'Tazewell': 'Tazewell County',
            'Union': 'Union County',
            'Vermilion': 'Vermilion County',
            'Wabash': 'Wabash County',
            'Warren': 'Warren County',
            'Washington': 'Washington County',
            'Wayne': 'Wayne County',
            'White': 'White County',
            'Whiteside': 'Whiteside County',
            'Will': 'Will County',
            'Williamson': 'Williamson County',
            'Winnebago': 'Winnebago County',
            'Woodford': 'Woodford County'
        }
        
        # Illinois-specific office mappings
        self.office_mappings = {
            'US PRESIDENT': 'US President',
            'US SENATOR': 'US Senator',
            'US REPRESENTATIVE': 'US Representative',
            'GOVERNOR': 'Governor',
            'LIEUTENANT GOVERNOR': 'Lieutenant Governor',
            'SECRETARY OF STATE': 'Secretary of State',
            'STATE TREASURER': 'State Treasurer',
            'ATTORNEY GENERAL': 'Attorney General',
            'COMPTROLLER': 'Comptroller',
            'STATE SENATOR': 'State Senator',
            'STATE REPRESENTATIVE': 'State Representative',
            'REGENT OF THE UNIVERSITY OF ILLINOIS': 'Regent of the University of Illinois',
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
            'COUNTY BOARD MEMBER': 'County Board Member',
            'COUNTY BOARD PRESIDENT': 'County Board President',
            'COUNTY BOARD VICE PRESIDENT': 'County Board Vice President',
            'COUNTY BOARD SECRETARY': 'County Board Secretary',
            'COUNTY BOARD TREASURER': 'County Board Treasurer',
            'MAYOR': 'Mayor',
            'CITY COUNCIL MEMBER': 'City Council Member',
            'ALDERMAN': 'Alderman',
            'SCHOOL BOARD MEMBER': 'School Board Member',
            'PARK DISTRICT COMMISSIONER': 'Park District Commissioner',
            'LIBRARY DISTRICT TRUSTEE': 'Library District Trustee',
            'FIRE DISTRICT TRUSTEE': 'Fire District Trustee',
            'WATER DISTRICT TRUSTEE': 'Water District Trustee',
            'SANITARY DISTRICT TRUSTEE': 'Sanitary District Trustee',
            'TOWNSHIP SUPERVISOR': 'Township Supervisor',
            'TOWNSHIP CLERK': 'Township Clerk',
            'TOWNSHIP TREASURER': 'Township Treasurer',
            'TOWNSHIP ASSESSOR': 'Township Assessor',
            'TOWNSHIP HIGHWAY COMMISSIONER': 'Township Highway Commissioner',
            'TOWNSHIP TRUSTEE': 'Township Trustee'
        }
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Illinois candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Illinois data cleaning")
        return super().clean_data(df)
    
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
        
        # Clean addresses (Illinois-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_illinois_address)
        
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
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        

        
        # Illinois-specific formatting
        df = self._apply_illinois_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Illinois-specific name parsing logic - Prairie State naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # Illinois name parsing - handles midwestern naming patterns
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
    
    def _clean_illinois_address(self, address: str) -> str:
        """
        Clean Illinois-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # Illinois-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common Illinois address patterns
        # (e.g., PO Box formats, rural route formats)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_illinois_district(self, district: str) -> str:
        """
        Clean Illinois-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Illinois-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Illinois district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
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
    
    def _apply_illinois_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Illinois-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Illinois-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
