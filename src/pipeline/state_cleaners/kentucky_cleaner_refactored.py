#!/usr/bin/env python3
"""
Kentucky State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class KentuckyCleaner(BaseStateCleaner):
    """
    Kentucky-specific data cleaner.
    
    This cleaner handles Kentucky-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Kentucky")
        
        # Kentucky-specific county mappings
        self.county_mappings = {
            'Adair': 'Adair County',
            'Allen': 'Allen County',
            'Anderson': 'Anderson County',
            'Ballard': 'Ballard County',
            'Barren': 'Barren County',
            'Bath': 'Bath County',
            'Bell': 'Bell County',
            'Boone': 'Boone County',
            'Bourbon': 'Bourbon County',
            'Boyd': 'Boyd County',
            'Boyle': 'Boyle County',
            'Bracken': 'Bracken County',
            'Breathitt': 'Breathitt County',
            'Breckinridge': 'Breckinridge County',
            'Bullitt': 'Bullitt County',
            'Butler': 'Butler County',
            'Caldwell': 'Caldwell County',
            'Calloway': 'Calloway County',
            'Campbell': 'Campbell County',
            'Carlisle': 'Carlisle County',
            'Carroll': 'Carroll County',
            'Carter': 'Carter County',
            'Casey': 'Casey County',
            'Christian': 'Christian County',
            'Clark': 'Clark County',
            'Clay': 'Clay County',
            'Clinton': 'Clinton County',
            'Crittenden': 'Crittenden County',
            'Cumberland': 'Cumberland County',
            'Daviess': 'Daviess County',
            'Edmonson': 'Edmonson County',
            'Elliott': 'Elliott County',
            'Estill': 'Estill County',
            'Fayette': 'Fayette County',
            'Fleming': 'Fleming County',
            'Floyd': 'Floyd County',
            'Franklin': 'Franklin County',
            'Fulton': 'Fulton County',
            'Gallatin': 'Gallatin County',
            'Garrard': 'Garrard County',
            'Grant': 'Grant County',
            'Graves': 'Graves County',
            'Grayson': 'Grayson County',
            'Green': 'Green County',
            'Greenup': 'Greenup County',
            'Hancock': 'Hancock County',
            'Hardin': 'Hardin County',
            'Harlan': 'Harlan County',
            'Harrison': 'Harrison County',
            'Hart': 'Hart County',
            'Henderson': 'Henderson County',
            'Henry': 'Henry County',
            'Hickman': 'Hickman County',
            'Hopkins': 'Hopkins County',
            'Jackson': 'Jackson County',
            'Jefferson': 'Jefferson County',
            'Jessamine': 'Jessamine County',
            'Johnson': 'Johnson County',
            'Kenton': 'Kenton County',
            'Knott': 'Knott County',
            'Knox': 'Knox County',
            'Larue': 'Larue County',
            'Laurel': 'Laurel County',
            'Lawrence': 'Lawrence County',
            'Lee': 'Lee County',
            'Leslie': 'Leslie County',
            'Letcher': 'Letcher County',
            'Lewis': 'Lewis County',
            'Lincoln': 'Lincoln County',
            'Livingston': 'Livingston County',
            'Logan': 'Logan County',
            'Lyon': 'Lyon County',
            'Madison': 'Madison County',
            'Magoffin': 'Magoffin County',
            'Marion': 'Marion County',
            'Marshall': 'Marshall County',
            'Martin': 'Martin County',
            'Mason': 'Mason County',
            'McCracken': 'McCracken County',
            'McCreary': 'McCreary County',
            'McLean': 'McLean County',
            'Meade': 'Meade County',
            'Menifee': 'Menifee County',
            'Mercer': 'Mercer County',
            'Metcalfe': 'Metcalfe County',
            'Monroe': 'Monroe County',
            'Montgomery': 'Montgomery County',
            'Morgan': 'Morgan County',
            'Muhlenberg': 'Muhlenberg County',
            'Nelson': 'Nelson County',
            'Nicholas': 'Nicholas County',
            'Ohio': 'Ohio County',
            'Oldham': 'Oldham County',
            'Owen': 'Owen County',
            'Owsley': 'Owsley County',
            'Pendleton': 'Pendleton County',
            'Perry': 'Perry County',
            'Pike': 'Pike County',
            'Powell': 'Powell County',
            'Pulaski': 'Pulaski County',
            'Robertson': 'Robertson County',
            'Rockcastle': 'Rockcastle County',
            'Rowan': 'Rowan County',
            'Russell': 'Russell County',
            'Scott': 'Scott County',
            'Shelby': 'Shelby County',
            'Simpson': 'Simpson County',
            'Spencer': 'Spencer County',
            'Taylor': 'Taylor County',
            'Todd': 'Todd County',
            'Trigg': 'Trigg County',
            'Trimble': 'Trimble County',
            'Union': 'Union County',
            'Warren': 'Warren County',
            'Washington': 'Washington County',
            'Wayne': 'Wayne County',
            'Webster': 'Webster County',
            'Whitley': 'Whitley County',
            'Wolfe': 'Wolfe County',
            'Woodford': 'Woodford County'
        }
        
        # Kentucky-specific office mappings
        self.office_mappings = {
            'US PRESIDENT': 'US President',
            'US SENATOR': 'US Senator',
            'US REPRESENTATIVE': 'US Representative',
            'GOVERNOR': 'Governor',
            'LIEUTENANT GOVERNOR': 'Lieutenant Governor',
            'SECRETARY OF STATE': 'Secretary of State',
            'STATE TREASURER': 'State Treasurer',
            'ATTORNEY GENERAL': 'Attorney General',
            'AUDITOR OF PUBLIC ACCOUNTS': 'Auditor of Public Accounts',
            'COMMISSIONER OF AGRICULTURE': 'Commissioner of Agriculture',
            'STATE SENATOR': 'State Senator',
            'STATE REPRESENTATIVE': 'State Representative',
            'REGENT OF THE UNIVERSITY OF KENTUCKY': 'Regent of the University of Kentucky',
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
            'SOIL AND WATER CONSERVATION DISTRICT SUPERVISOR': 'Soil and Water Conservation District Supervisor'
        }
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Kentucky candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Kentucky data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Kentucky-specific data structure.
        
        This handles:
        - Name parsing (Kentucky has specific name formats)
        - Address parsing (Kentucky address structure)
        - District parsing (Kentucky district numbering)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Kentucky-specific structure cleaned
        """
        self.logger.info("Cleaning Kentucky-specific data structure")
        
        # Clean candidate names (Kentucky-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_kentucky_name)
        
        # Clean addresses (Kentucky-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_kentucky_address)
        
        # Clean districts (Kentucky-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_kentucky_district)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Kentucky-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Kentucky-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Kentucky-specific content cleaned
        """
        self.logger.info("Cleaning Kentucky-specific content")
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Kentucky-specific formatting
        df = self._apply_kentucky_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Kentucky-specific name parsing logic - column-based approach.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Kentucky approach: look for separate first_name and last_name columns
        first_name_col = None
        last_name_col = None
        
        # Find the first name column
        for col in ['first_name', 'candidate_first_name', 'first']:
            if col in df.columns:
                first_name_col = col
                break
        
        # Find the last name column  
        for col in ['last_name', 'candidate_last_name', 'last']:
            if col in df.columns:
                last_name_col = col
                break
        
        # Check if the found columns actually have data
        has_data = False
        if first_name_col and last_name_col:
            # Check if these columns have actual data (not just None/empty)
            first_data = df[first_name_col].notna().sum()
            last_data = df[last_name_col].notna().sum()
            has_data = first_data > 0 and last_data > 0
        
        # If we don't have separate columns OR if the columns are empty, try to parse from candidate_name
        if not first_name_col or not last_name_col or not has_data:
            if 'candidate_name' in df.columns:
                # Fallback to basic parsing with Kentucky's prefix detection
                for idx, row in df.iterrows():
                    candidate_name = row.get('candidate_name')
                    if pd.notna(candidate_name) and str(candidate_name).strip():
                        name_str = str(candidate_name).strip()
                        
                        # Extract prefix first (from beginning of name)
                        prefix_pattern = r'^(Dr|Mr|Mrs|Ms|Miss|Prof|Rev|Hon|Sen|Rep|Gov|Lt|Col|Gen|Adm|Capt|Maj|Sgt|Cpl|Pvt)\.?\s+'
                        prefix_match = re.match(prefix_pattern, name_str, re.IGNORECASE)
                        prefix = None
                        if prefix_match:
                            prefix = prefix_match.group(1)
                            df.at[idx, 'prefix'] = prefix
                            # Remove prefix from name for further processing
                            name_str = re.sub(prefix_pattern, '', name_str, flags=re.IGNORECASE).strip()
                        
                        # Extract suffix
                        suffix_pattern = r'\b(Jr|Sr|II|III|IV|V|VI|VII|VIII|IX|X)\b'
                        suffix_match = re.search(suffix_pattern, name_str, re.IGNORECASE)
                        suffix = None
                        if suffix_match:
                            suffix = suffix_match.group(1)
                            df.at[idx, 'suffix'] = suffix
                            name_str = re.sub(suffix_pattern, '', name_str, flags=re.IGNORECASE).strip()
                        
                        # Split remaining name
                        parts = [p.strip() for p in name_str.split() if p.strip()]
                        if len(parts) >= 1:
                            df.at[idx, 'first_name'] = parts[0]
                        if len(parts) >= 2:
                            df.at[idx, 'last_name'] = parts[-1]
                        if len(parts) > 2:
                            df.at[idx, 'middle_name'] = ' '.join(parts[1:-1])
                        
                        df.at[idx, 'full_name_display'] = str(candidate_name).strip()
            else:
                # No name data available
                return df
        else:
            # We have separate columns - use Kentucky's column-based approach
            for idx in range(len(df)):
                first_name = str(df.iloc[idx][first_name_col]).strip() if pd.notna(df.iloc[idx][first_name_col]) else ""
                last_name = str(df.iloc[idx][last_name_col]).strip() if pd.notna(df.iloc[idx][last_name_col]) else ""
                
                # Check for suffix in last name
                suffix = None
                if last_name:
                    suffix_pattern = r'\b(Jr|Sr|II|III|IV|V|VI|VII|VIII|IX|X)\b'
                    suffix_match = re.search(suffix_pattern, last_name, re.IGNORECASE)
                    if suffix_match:
                        suffix = suffix_match.group(1)
                        # Remove suffix from last name
                        last_name = re.sub(suffix_pattern, '', last_name, flags=re.IGNORECASE).strip()
                
                # Check for prefix in first name
                prefix = None
                if first_name:
                    prefix_pattern = r'\b(Dr|Mr|Mrs|Ms|Miss|Prof|Rev|Hon|Sen|Rep|Gov|Lt|Col|Gen|Adm|Capt|Maj|Sgt|Cpl|Pvt)\b'
                    prefix_match = re.search(prefix_pattern, first_name, re.IGNORECASE)
                    if prefix_match:
                        prefix = prefix_match.group(1)
                        # Remove prefix from first name
                        first_name = re.sub(prefix_pattern, '', first_name, flags=re.IGNORECASE).strip()
                
                # Build display name
                display_parts = []
                if prefix:
                    display_parts.append(prefix)
                if first_name:
                    display_parts.append(first_name)
                if last_name:
                    display_parts.append(last_name)
                if suffix:
                    display_parts.append(suffix)
                
                display_name = ' '.join(display_parts).strip()
                
                # Assign parsed components
                df.at[idx, 'first_name'] = first_name
                df.at[idx, 'last_name'] = last_name
                df.at[idx, 'prefix'] = prefix
                df.at[idx, 'suffix'] = suffix
                df.at[idx, 'full_name_display'] = display_name
        
        return df
    
    def _clean_kentucky_name(self, name: str) -> str:
        """
        Clean Kentucky-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Kentucky-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Kentucky name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _clean_kentucky_address(self, address: str) -> str:
        """
        Clean Kentucky-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # Kentucky-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common Kentucky address patterns
        # (e.g., PO Box formats, rural route formats)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_kentucky_district(self, district: str) -> str:
        """
        Clean Kentucky-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Kentucky-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Kentucky district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
    def _apply_kentucky_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Kentucky-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Kentucky-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
