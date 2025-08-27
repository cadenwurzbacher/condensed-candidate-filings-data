#!/usr/bin/env python3
"""
Missouri State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class MissouriCleaner(BaseStateCleaner):
    """
    Missouri-specific data cleaner.
    
    This cleaner handles Missouri-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Missouri")
        
        # Missouri-specific county mappings
        self.county_mappings = {
            'Adair': 'Adair County',
            'Andrew': 'Andrew County',
            'Atchison': 'Atchison County',
            'Audrain': 'Audrain County',
            'Barry': 'Barry County',
            'Barton': 'Barton County',
            'Bates': 'Bates County',
            'Benton': 'Benton County',
            'Bollinger': 'Bollinger County',
            'Boone': 'Boone County',
            'Buchanan': 'Buchanan County',
            'Butler': 'Butler County',
            'Caldwell': 'Caldwell County',
            'Callaway': 'Callaway County',
            'Camden': 'Camden County',
            'Cape Girardeau': 'Cape Girardeau County',
            'Carroll': 'Carroll County',
            'Carter': 'Carter County',
            'Cass': 'Cass County',
            'Cedar': 'Cedar County',
            'Chariton': 'Chariton County',
            'Christian': 'Christian County',
            'Clark': 'Clark County',
            'Clay': 'Clay County',
            'Clinton': 'Clinton County',
            'Cole': 'Cole County',
            'Cooper': 'Cooper County',
            'Crawford': 'Crawford County',
            'Dade': 'Dade County',
            'Dallas': 'Dallas County',
            'Daviess': 'Daviess County',
            'DeKalb': 'DeKalb County',
            'Dent': 'Dent County',
            'Douglas': 'Douglas County',
            'Dunklin': 'Dunklin County',
            'Franklin': 'Franklin County',
            'Gasconade': 'Gasconade County',
            'Gentry': 'Gentry County',
            'Greene': 'Greene County',
            'Grundy': 'Grundy County',
            'Harrison': 'Harrison County',
            'Henry': 'Henry County',
            'Hickory': 'Hickory County',
            'Holt': 'Holt County',
            'Howard': 'Howard County',
            'Howell': 'Howell County',
            'Iron': 'Iron County',
            'Jackson': 'Jackson County',
            'Jasper': 'Jasper County',
            'Jefferson': 'Jefferson County',
            'Johnson': 'Johnson County',
            'Knox': 'Knox County',
            'Laclede': 'Laclede County',
            'Lafayette': 'Lafayette County',
            'Lawrence': 'Lawrence County',
            'Lewis': 'Lewis County',
            'Lincoln': 'Lincoln County',
            'Linn': 'Linn County',
            'Livingston': 'Livingston County',
            'Macon': 'Macon County',
            'Madison': 'Madison County',
            'Maries': 'Maries County',
            'Marion': 'Marion County',
            'McDonald': 'McDonald County',
            'Mercer': 'Mercer County',
            'Miller': 'Miller County',
            'Mississippi': 'Mississippi County',
            'Moniteau': 'Moniteau County',
            'Monroe': 'Monroe County',
            'Montgomery': 'Montgomery County',
            'Morgan': 'Morgan County',
            'New Madrid': 'New Madrid County',
            'Newton': 'Newton County',
            'Nodaway': 'Nodaway County',
            'Oregon': 'Oregon County',
            'Osage': 'Osage County',
            'Ozark': 'Ozark County',
            'Pemiscot': 'Pemiscot County',
            'Perry': 'Perry County',
            'Pettis': 'Pettis County',
            'Phelps': 'Phelps County',
            'Pike': 'Pike County',
            'Platte': 'Platte County',
            'Polk': 'Polk County',
            'Pulaski': 'Pulaski County',
            'Putnam': 'Putnam County',
            'Ralls': 'Ralls County',
            'Randolph': 'Randolph County',
            'Ray': 'Ray County',
            'Reynolds': 'Reynolds County',
            'Ripley': 'Ripley County',
            'Saline': 'Saline County',
            'Schuyler': 'Schuyler County',
            'Scotland': 'Scotland County',
            'Scott': 'Scott County',
            'Shannon': 'Shannon County',
            'Shelby': 'Shelby County',
            'St. Charles': 'St. Charles County',
            'St. Clair': 'St. Clair County',
            'St. Francois': 'St. Francois County',
            'St. Louis': 'St. Louis County',
            'St. Louis City': 'St. Louis City',
            'Ste. Genevieve': 'Ste. Genevieve County',
            'Stoddard': 'Stoddard County',
            'Stone': 'Stone County',
            'Sullivan': 'Sullivan County',
            'Taney': 'Taney County',
            'Texas': 'Texas County',
            'Vernon': 'Vernon County',
            'Warren': 'Warren County',
            'Washington': 'Washington County',
            'Wayne': 'Wayne County',
            'Webster': 'Webster County',
            'Worth': 'Worth County',
            'Wright': 'Wright County'
        }
        
        # Missouri-specific office mappings
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
            'STATE SENATOR': 'State Senator',
            'STATE REPRESENTATIVE': 'State Representative',
            'REGENT OF THE UNIVERSITY OF MISSOURI': 'Regent of the University of Missouri',
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
            'COUNTY PUBLIC ADMINISTRATOR': 'County Public Administrator',
            'COUNTY COLLECTOR': 'County Collector',
            'COUNTY RECORDER OF DEEDS': 'County Recorder of Deeds',
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
            'SOIL AND WATER CONSERVATION DISTRICT SUPERVISOR': 'Soil and Water Conservation District Supervisor'
        }
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Missouri candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Missouri data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Missouri-specific data structure.
        
        This handles:
        - Name parsing (Missouri has specific name formats)
        - Address parsing (Missouri address structure)
        - District parsing (Missouri district numbering)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Missouri-specific structure cleaned
        """
        self.logger.info("Cleaning Missouri-specific data structure")
        
        # Clean candidate names (Missouri-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_missouri_name)
        
        # Clean addresses (Missouri-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_missouri_address)
        
        # Clean districts (Missouri-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_missouri_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Missouri-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Missouri-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Missouri-specific content cleaned
        """
        self.logger.info("Cleaning Missouri-specific content")
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices
        if 'office' in df.columns:
            df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Missouri-specific formatting
        df = self._apply_missouri_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Missouri-specific name parsing logic - Show-Me State naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # Missouri name parsing - handles midwestern naming patterns
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
    
    def _clean_missouri_name(self, name: str) -> str:
        """
        Clean Missouri-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Missouri-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Missouri name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _clean_missouri_address(self, address: str) -> str:
        """
        Clean Missouri-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # Missouri-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common Missouri address patterns
        # (e.g., PO Box formats, rural route formats)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_missouri_district(self, district: str) -> str:
        """
        Clean Missouri-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Missouri-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Missouri district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
    def _apply_missouri_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Missouri-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Missouri-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
