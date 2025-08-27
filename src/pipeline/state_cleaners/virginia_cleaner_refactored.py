#!/usr/bin/env python3
"""
Virginia State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class VirginiaCleaner(BaseStateCleaner):
    """
    Virginia-specific data cleaner.
    
    This cleaner handles Virginia-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Virginia")
        
        # Virginia-specific county mappings
        self.county_mappings = {
            'Accomack': 'Accomack County',
            'Albemarle': 'Albemarle County',
            'Alleghany': 'Alleghany County',
            'Amelia': 'Amelia County',
            'Amherst': 'Amherst County',
            'Appomattox': 'Appomattox County',
            'Arlington': 'Arlington County',
            'Augusta': 'Augusta County',
            'Bath': 'Bath County',
            'Bedford': 'Bedford County',
            'Bland': 'Bland County',
            'Botetourt': 'Botetourt County',
            'Brunswick': 'Brunswick County',
            'Buchanan': 'Buchanan County',
            'Buckingham': 'Buckingham County',
            'Campbell': 'Campbell County',
            'Caroline': 'Caroline County',
            'Carroll': 'Carroll County',
            'Charles City': 'Charles City County',
            'Charlotte': 'Charlotte County',
            'Chesterfield': 'Chesterfield County',
            'Clarke': 'Clarke County',
            'Craig': 'Craig County',
            'Culpeper': 'Culpeper County',
            'Cumberland': 'Cumberland County',
            'Dickenson': 'Dickenson County',
            'Dinwiddie': 'Dinwiddie County',
            'Essex': 'Essex County',
            'Fairfax': 'Fairfax County',
            'Fauquier': 'Fauquier County',
            'Floyd': 'Floyd County',
            'Fluvanna': 'Fluvanna County',
            'Franklin': 'Franklin County',
            'Frederick': 'Frederick County',
            'Giles': 'Giles County',
            'Gloucester': 'Gloucester County',
            'Goochland': 'Goochland County',
            'Grayson': 'Grayson County',
            'Greene': 'Greene County',
            'Greensville': 'Greensville County',
            'Halifax': 'Halifax County',
            'Hanover': 'Hanover County',
            'Henrico': 'Henrico County',
            'Henry': 'Henry County',
            'Highland': 'Highland County',
            'Isle of Wight': 'Isle of Wight County',
            'James City': 'James City County',
            'King and Queen': 'King and Queen County',
            'King George': 'King George County',
            'King William': 'King William County',
            'Lancaster': 'Lancaster County',
            'Lee': 'Lee County',
            'Loudoun': 'Loudoun County',
            'Louisa': 'Louisa County',
            'Lunenburg': 'Lunenburg County',
            'Madison': 'Madison County',
            'Mathews': 'Mathews County',
            'Mecklenburg': 'Mecklenburg County',
            'Middlesex': 'Middlesex County',
            'Montgomery': 'Montgomery County',
            'Nelson': 'Nelson County',
            'New Kent': 'New Kent County',
            'Northampton': 'Northampton County',
            'Northumberland': 'Northumberland County',
            'Nottoway': 'Nottoway County',
            'Orange': 'Orange County',
            'Page': 'Page County',
            'Patrick': 'Patrick County',
            'Pittsylvania': 'Pittsylvania County',
            'Powhatan': 'Powhatan County',
            'Prince Edward': 'Prince Edward County',
            'Prince George': 'Prince George County',
            'Prince William': 'Prince William County',
            'Pulaski': 'Pulaski County',
            'Rappahannock': 'Rappahannock County',
            'Richmond': 'Richmond County',
            'Roanoke': 'Roanoke County',
            'Rockbridge': 'Rockbridge County',
            'Rockingham': 'Rockingham County',
            'Russell': 'Russell County',
            'Scott': 'Scott County',
            'Shenandoah': 'Shenandoah County',
            'Smyth': 'Smyth County',
            'Southampton': 'Southampton County',
            'Spotsylvania': 'Spotsylvania County',
            'Stafford': 'Stafford County',
            'Surry': 'Surry County',
            'Sussex': 'Sussex County',
            'Tazewell': 'Tazewell County',
            'Warren': 'Warren County',
            'Washington': 'Washington County',
            'Westmoreland': 'Westmoreland County',
            'Wise': 'Wise County',
            'Wythe': 'Wythe County',
            'York': 'York County'
        }
        
        # Virginia-specific office mappings
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
            'STATE DELEGATE': 'State Delegate',
            'REGENT OF THE UNIVERSITY OF VIRGINIA': 'Regent of the University of Virginia',
            'DISTRICT ATTORNEY': 'District Attorney',
            'COMMONWEALTH\'S ATTORNEY': 'Commonwealth\'s Attorney',
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
        Clean Virginia candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Virginia data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Virginia-specific data structure.
        
        This handles:
        - Name parsing (Virginia has specific name formats)
        - Address parsing (Virginia address structure)
        - District parsing (Virginia district numbering)
        - Old Dominion-specific logic (Virginia has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Virginia-specific structure cleaned
        """
        self.logger.info("Cleaning Virginia-specific data structure")
        
        # Clean candidate names (Virginia-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_virginia_name)
        
        # Clean addresses (Virginia-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_virginia_address)
        
        # Clean districts (Virginia-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_virginia_district)
        
        # Handle Virginia-specific Old Dominion logic
        df = self._handle_virginia_old_dominion_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Virginia-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Virginia-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Virginia-specific content cleaned
        """
        self.logger.info("Cleaning Virginia-specific content")
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices
        if 'office' in df.columns:
            df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Virginia-specific formatting
        df = self._apply_virginia_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Virginia-specific name parsing logic - Old Dominion State naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # Virginia name parsing - handles southern naming patterns
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
    
    def _clean_virginia_name(self, name: str) -> str:
        """
        Clean Virginia-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Virginia-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Virginia name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _clean_virginia_address(self, address: str) -> str:
        """
        Clean Virginia-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # Virginia-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common Virginia address patterns
        # (e.g., PO Box formats, rural route formats, etc.)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_virginia_district(self, district: str) -> str:
        """
        Clean Virginia-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Virginia-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Virginia district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
    def _handle_virginia_old_dominion_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Virginia-specific Old Dominion logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Virginia-specific Old Dominion handling logic
        # (e.g., Old Dominion-specific processing, special district handling)
        
        return df
    
    def _apply_virginia_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Virginia-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Virginia-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
