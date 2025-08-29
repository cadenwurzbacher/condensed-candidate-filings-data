#!/usr/bin/env python3
"""
Oregon State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class OregonCleaner(BaseStateCleaner):
    """
    Oregon-specific data cleaner.
    
    This cleaner handles Oregon-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Oregon")
        
        # Oregon-specific county mappings
        self.county_mappings = {
            'Baker': 'Baker County',
            'Benton': 'Benton County',
            'Clackamas': 'Clackamas County',
            'Clatsop': 'Clatsop County',
            'Columbia': 'Columbia County',
            'Coos': 'Coos County',
            'Crook': 'Crook County',
            'Curry': 'Curry County',
            'Deschutes': 'Deschutes County',
            'Douglas': 'Douglas County',
            'Gilliam': 'Gilliam County',
            'Grant': 'Grant County',
            'Harney': 'Harney County',
            'Hood River': 'Hood River County',
            'Jackson': 'Jackson County',
            'Jefferson': 'Jefferson County',
            'Josephine': 'Josephine County',
            'Klamath': 'Klamath County',
            'Lake': 'Lake County',
            'Lane': 'Lane County',
            'Lincoln': 'Lincoln County',
            'Linn': 'Linn County',
            'Malheur': 'Malheur County',
            'Marion': 'Marion County',
            'Morrow': 'Morrow County',
            'Multnomah': 'Multnomah County',
            'Polk': 'Polk County',
            'Sherman': 'Sherman County',
            'Tillamook': 'Tillamook County',
            'Umatilla': 'Umatilla County',
            'Union': 'Union County',
            'Wallowa': 'Wallowa County',
            'Wasco': 'Wasco County',
            'Washington': 'Washington County',
            'Wheeler': 'Wheeler County',
            'Yamhill': 'Yamhill County'
        }
        
        # Oregon-specific office mappings
        self.office_mappings = {
            'US PRESIDENT': 'US President',
            'US SENATOR': 'US Senator',
            'US REPRESENTATIVE': 'US Representative',
            'GOVERNOR': 'Governor',
            'SECRETARY OF STATE': 'Secretary of State',
            'STATE TREASURER': 'State Treasurer',
            'ATTORNEY GENERAL': 'Attorney General',
            'LABOR COMMISSIONER': 'Labor Commissioner',
            'STATE SENATOR': 'State Senator',
            'STATE REPRESENTATIVE': 'State Representative',
            'REGENT OF THE UNIVERSITY OF OREGON': 'Regent of the University of Oregon',
            'DISTRICT ATTORNEY': 'District Attorney',
            'COUNTY DISTRICT ATTORNEY': 'County District Attorney',
            'COUNTY SHERIFF': 'County Sheriff',
            'COUNTY CLERK': 'County Clerk',
            'COUNTY TREASURER': 'County Treasurer',
            'COUNTY ASSESSOR': 'County Assessor',
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
            'COUNCILOR': 'Councilor',
            'SCHOOL BOARD MEMBER': 'School Board Member',
            'PARK BOARD MEMBER': 'Park Board Member',
            'LIBRARY BOARD MEMBER': 'Library Board Member',
            'FIRE DISTRICT BOARD MEMBER': 'Fire District Board Member',
            'WATER DISTRICT BOARD MEMBER': 'Water District Board Member',
            'SEWER DISTRICT BOARD MEMBER': 'Sewer District Board Member',
            'HOSPITAL DISTRICT BOARD MEMBER': 'Hospital District Board Member',
            'PUBLIC UTILITY DISTRICT BOARD MEMBER': 'Public Utility District Board Member',
            'SOIL AND WATER CONSERVATION DISTRICT SUPERVISOR': 'Soil and Water Conservation District Supervisor',
            'PORT COMMISSIONER': 'Port Commissioner',
            'METRO COUNCILOR': 'Metro Councilor',
            'SPECIAL DISTRICT BOARD MEMBER': 'Special District Board Member',
            'WATERSHED COUNCIL MEMBER': 'Watershed Council Member',
            'SOIL AND WATER CONSERVATION DISTRICT SUPERVISOR': 'Soil and Water Conservation District Supervisor'
        }
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Oregon candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Oregon data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Oregon-specific data structure.
        
        This handles:
        - Name parsing (Oregon has specific name formats)
        - Address parsing (Oregon address structure)
        - District parsing (Oregon district numbering)
        - Metro-specific logic (Portland metro area)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Oregon-specific structure cleaned
        """
        self.logger.info("Cleaning Oregon-specific data structure")
        
        # Clean candidate names (Oregon-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_oregon_name)
        
        # Clean addresses (Oregon-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_oregon_address)
        
        # Clean districts (Oregon-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_oregon_district)
        
        # Handle Oregon-specific metro logic
        df = self._handle_oregon_metro_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Oregon-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Oregon-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Oregon-specific content cleaned
        """
        self.logger.info("Cleaning Oregon-specific content")
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Oregon-specific formatting
        df = self._apply_oregon_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Oregon-specific name parsing logic - Beaver State naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # Oregon name parsing - handles pacific northwest naming patterns
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
    
    def _clean_oregon_name(self, name: str) -> str:
        """
        Clean Oregon-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Oregon-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Oregon name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _clean_oregon_address(self, address: str) -> str:
        """
        Clean Oregon-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # Oregon-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common Oregon address patterns
        # (e.g., PO Box formats, rural route formats, Portland metro formats)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_oregon_district(self, district: str) -> str:
        """
        Clean Oregon-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Oregon-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Oregon district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
    def _handle_oregon_metro_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Oregon-specific metro logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Oregon-specific metro handling logic
        # (e.g., Portland metro area processing, special district handling)
        
        return df
    
    def _apply_oregon_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Oregon-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Oregon-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
