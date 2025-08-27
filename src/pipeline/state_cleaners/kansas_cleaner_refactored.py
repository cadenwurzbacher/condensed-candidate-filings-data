#!/usr/bin/env python3
"""
Kansas State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class KansasCleaner(BaseStateCleaner):
    """
    Kansas-specific data cleaner.
    
    This cleaner handles Kansas-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Kansas")
        
        # Kansas-specific county mappings
        self.county_mappings = {
            'Allen': 'Allen County',
            'Anderson': 'Anderson County',
            'Atchison': 'Atchison County',
            'Barber': 'Barber County',
            'Barton': 'Barton County',
            'Bourbon': 'Bourbon County',
            'Brown': 'Brown County',
            'Butler': 'Butler County',
            'Chase': 'Chase County',
            'Chautauqua': 'Chautauqua County',
            'Cherokee': 'Cherokee County',
            'Cheyenne': 'Cheyenne County',
            'Clark': 'Clark County',
            'Clay': 'Clay County',
            'Cloud': 'Cloud County',
            'Coffey': 'Coffey County',
            'Comanche': 'Comanche County',
            'Cowley': 'Cowley County',
            'Crawford': 'Crawford County',
            'Decatur': 'Decatur County',
            'Dickinson': 'Dickinson County',
            'Doniphan': 'Doniphan County',
            'Douglas': 'Douglas County',
            'Edwards': 'Edwards County',
            'Elk': 'Elk County',
            'Ellis': 'Ellis County',
            'Ellsworth': 'Ellsworth County',
            'Finney': 'Finney County',
            'Ford': 'Ford County',
            'Franklin': 'Franklin County',
            'Geary': 'Geary County',
            'Gove': 'Gove County',
            'Graham': 'Graham County',
            'Grant': 'Grant County',
            'Gray': 'Gray County',
            'Greeley': 'Greeley County',
            'Greenwood': 'Greenwood County',
            'Hamilton': 'Hamilton County',
            'Harper': 'Harper County',
            'Harvey': 'Harvey County',
            'Haskell': 'Haskell County',
            'Hodgeman': 'Hodgeman County',
            'Jackson': 'Jackson County',
            'Jefferson': 'Jefferson County',
            'Jewell': 'Jewell County',
            'Johnson': 'Johnson County',
            'Kearny': 'Kearny County',
            'Kingman': 'Kingman County',
            'Kiowa': 'Kiowa County',
            'Labette': 'Labette County',
            'Lane': 'Lane County',
            'Leavenworth': 'Leavenworth County',
            'Lincoln': 'Lincoln County',
            'Linn': 'Linn County',
            'Logan': 'Logan County',
            'Lyon': 'Lyon County',
            'Marion': 'Marion County',
            'Marshall': 'Marshall County',
            'McPherson': 'McPherson County',
            'Meade': 'Meade County',
            'Miami': 'Miami County',
            'Mitchell': 'Mitchell County',
            'Montgomery': 'Montgomery County',
            'Morris': 'Morris County',
            'Morton': 'Morton County',
            'Nemaha': 'Nemaha County',
            'Neosho': 'Neosho County',
            'Ness': 'Ness County',
            'Norton': 'Norton County',
            'Osage': 'Osage County',
            'Osborne': 'Osborne County',
            'Ottawa': 'Ottawa County',
            'Pawnee': 'Pawnee County',
            'Phillips': 'Phillips County',
            'Pottawatomie': 'Pottawatomie County',
            'Pratt': 'Pratt County',
            'Rawlins': 'Rawlins County',
            'Reno': 'Reno County',
            'Republic': 'Republic County',
            'Rice': 'Rice County',
            'Riley': 'Riley County',
            'Rooks': 'Rooks County',
            'Rush': 'Rush County',
            'Russell': 'Russell County',
            'Saline': 'Saline County',
            'Scott': 'Scott County',
            'Sedgwick': 'Sedgwick County',
            'Seward': 'Seward County',
            'Shawnee': 'Shawnee County',
            'Sheridan': 'Sheridan County',
            'Sherman': 'Sherman County',
            'Smith': 'Smith County',
            'Stafford': 'Stafford County',
            'Stanton': 'Stanton County',
            'Stevens': 'Stevens County',
            'Sumner': 'Sumner County',
            'Thomas': 'Thomas County',
            'Trego': 'Trego County',
            'Wabaunsee': 'Wabaunsee County',
            'Wallace': 'Wallace County',
            'Washington': 'Washington County',
            'Wichita': 'Wichita County',
            'Wilson': 'Wilson County',
            'Woodson': 'Woodson County',
            'Wyandotte': 'Wyandotte County'
        }
        
        # Kansas-specific office mappings
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
            'SECRETARY OF AGRICULTURE': 'Secretary of Agriculture',
            'STATE SENATOR': 'State Senator',
            'STATE REPRESENTATIVE': 'State Representative',
            'REGENT OF THE UNIVERSITY OF KANSAS': 'Regent of the University of Kansas',
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
        Clean Kansas candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Kansas data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Kansas-specific data structure.
        
        This handles:
        - Name parsing (Kansas has specific name formats)
        - Address parsing (Kansas address structure)
        - District parsing (Kansas district numbering)
        - Sunflower-specific logic (Kansas has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Kansas-specific structure cleaned
        """
        self.logger.info("Cleaning Kansas-specific data structure")
        
        # Clean candidate names (Kansas-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_kansas_name)
        
        # Clean addresses (Kansas-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_kansas_address)
        
        # Clean districts (Kansas-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_kansas_district)
        
        # Handle Kansas-specific Sunflower logic
        df = self._handle_kansas_sunflower_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Kansas-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Kansas-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Kansas-specific content cleaned
        """
        self.logger.info("Cleaning Kansas-specific content")
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices
        if 'office' in df.columns:
            df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Kansas-specific formatting
        df = self._apply_kansas_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Kansas-specific name parsing logic - Sunflower State naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # Kansas name parsing - handles plains region naming patterns
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
    
    def _clean_kansas_name(self, name: str) -> str:
        """
        Clean Kansas-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Kansas-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Kansas name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _clean_kansas_address(self, address: str) -> str:
        """
        Clean Kansas-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # Kansas-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common Kansas address patterns
        # (e.g., PO Box formats, rural route formats, etc.)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_kansas_district(self, district: str) -> str:
        """
        Clean Kansas-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Kansas-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Kansas district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
    def _handle_kansas_sunflower_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Kansas-specific Sunflower logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Kansas-specific Sunflower handling logic
        # (e.g., Sunflower-specific processing, special district handling)
        
        return df
    
    def _apply_kansas_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Kansas-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Kansas-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
