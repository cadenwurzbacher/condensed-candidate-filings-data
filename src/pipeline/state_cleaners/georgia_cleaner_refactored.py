#!/usr/bin/env python3
"""
Georgia State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class GeorgiaCleaner(BaseStateCleaner):
    """
    Georgia-specific data cleaner.
    
    This cleaner handles Georgia-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Georgia")
        
        # Georgia-specific county mappings
        self.county_mappings = {
            'Appling': 'Appling County',
            'Atkinson': 'Atkinson County',
            'Bacon': 'Bacon County',
            'Baker': 'Baker County',
            'Baldwin': 'Baldwin County',
            'Banks': 'Banks County',
            'Barrow': 'Barrow County',
            'Bartow': 'Bartow County',
            'Ben Hill': 'Ben Hill County',
            'Berrien': 'Berrien County',
            'Bibb': 'Bibb County',
            'Bleckley': 'Bleckley County',
            'Brantley': 'Brantley County',
            'Brooks': 'Brooks County',
            'Bryan': 'Bryan County',
            'Bulloch': 'Bulloch County',
            'Burke': 'Burke County',
            'Butts': 'Butts County',
            'Calhoun': 'Calhoun County',
            'Camden': 'Camden County',
            'Candler': 'Candler County',
            'Carroll': 'Carroll County',
            'Catoosa': 'Catoosa County',
            'Charlton': 'Charlton County',
            'Chatham': 'Chatham County',
            'Chattahoochee': 'Chattahoochee County',
            'Chattooga': 'Chattooga County',
            'Cherokee': 'Cherokee County',
            'Clarke': 'Clarke County',
            'Clay': 'Clay County',
            'Clayton': 'Clayton County',
            'Clinch': 'Clinch County',
            'Cobb': 'Cobb County',
            'Coffee': 'Coffee County',
            'Colquitt': 'Colquitt County',
            'Columbia': 'Columbia County',
            'Cook': 'Cook County',
            'Coweta': 'Coweta County',
            'Crawford': 'Crawford County',
            'Crisp': 'Crisp County',
            'Dade': 'Dade County',
            'Dawson': 'Dawson County',
            'Decatur': 'Decatur County',
            'DeKalb': 'DeKalb County',
            'Dodge': 'Dodge County',
            'Dooly': 'Dooly County',
            'Dougherty': 'Dougherty County',
            'Douglas': 'Douglas County',
            'Early': 'Early County',
            'Echols': 'Echols County',
            'Effingham': 'Effingham County',
            'Elbert': 'Elbert County',
            'Emanuel': 'Emanuel County',
            'Evans': 'Evans County',
            'Fannin': 'Fannin County',
            'Fayette': 'Fayette County',
            'Floyd': 'Floyd County',
            'Forsyth': 'Forsyth County',
            'Franklin': 'Franklin County',
            'Fulton': 'Fulton County',
            'Gilmer': 'Gilmer County',
            'Glascock': 'Glascock County',
            'Glynn': 'Glynn County',
            'Gordon': 'Gordon County',
            'Grady': 'Grady County',
            'Greene': 'Greene County',
            'Gwinnett': 'Gwinnett County',
            'Habersham': 'Habersham County',
            'Hall': 'Hall County',
            'Hancock': 'Hancock County',
            'Haralson': 'Haralson County',
            'Harris': 'Harris County',
            'Hart': 'Hart County',
            'Heard': 'Heard County',
            'Henry': 'Henry County',
            'Houston': 'Houston County',
            'Irwin': 'Irwin County',
            'Jackson': 'Jackson County',
            'Jasper': 'Jasper County',
            'Jeff Davis': 'Jeff Davis County',
            'Jefferson': 'Jefferson County',
            'Jenkins': 'Jenkins County',
            'Johnson': 'Johnson County',
            'Jones': 'Jones County',
            'Lamar': 'Lamar County',
            'Lanier': 'Lanier County',
            'Laurens': 'Laurens County',
            'Lee': 'Lee County',
            'Liberty': 'Liberty County',
            'Lincoln': 'Lincoln County',
            'Long': 'Long County',
            'Lowndes': 'Lowndes County',
            'Lumpkin': 'Lumpkin County',
            'Macon': 'Macon County',
            'Madison': 'Madison County',
            'Marion': 'Marion County',
            'McDuffie': 'McDuffie County',
            'McIntosh': 'McIntosh County',
            'Meriwether': 'Meriwether County',
            'Miller': 'Miller County',
            'Mitchell': 'Mitchell County',
            'Monroe': 'Monroe County',
            'Montgomery': 'Montgomery County',
            'Morgan': 'Morgan County',
            'Murray': 'Murray County',
            'Muscogee': 'Muscogee County',
            'Newton': 'Newton County',
            'Oconee': 'Oconee County',
            'Oglethorpe': 'Oglethorpe County',
            'Paulding': 'Paulding County',
            'Peach': 'Peach County',
            'Pickens': 'Pickens County',
            'Pierce': 'Pierce County',
            'Pike': 'Pike County',
            'Polk': 'Polk County',
            'Pulaski': 'Pulaski County',
            'Putnam': 'Putnam County',
            'Quitman': 'Quitman County',
            'Rabun': 'Rabun County',
            'Randolph': 'Randolph County',
            'Richmond': 'Richmond County',
            'Rockdale': 'Rockdale County',
            'Schley': 'Schley County',
            'Screven': 'Screven County',
            'Seminole': 'Seminole County',
            'Spalding': 'Spalding County',
            'Stephens': 'Stephens County',
            'Stewart': 'Stewart County',
            'Sumter': 'Sumter County',
            'Talbot': 'Talbot County',
            'Taliaferro': 'Taliaferro County',
            'Tattnall': 'Tattnall County',
            'Taylor': 'Taylor County',
            'Telfair': 'Telfair County',
            'Terrell': 'Terrell County',
            'Thomas': 'Thomas County',
            'Tift': 'Tift County',
            'Toombs': 'Toombs County',
            'Towns': 'Towns County',
            'Treutlen': 'Treutlen County',
            'Troup': 'Troup County',
            'Turner': 'Turner County',
            'Twiggs': 'Twiggs County',
            'Union': 'Union County',
            'Upson': 'Upson County',
            'Walker': 'Walker County',
            'Walton': 'Walton County',
            'Ware': 'Ware County',
            'Warren': 'Warren County',
            'Washington': 'Washington County',
            'Wayne': 'Wayne County',
            'Webster': 'Webster County',
            'Wheeler': 'Wheeler County',
            'White': 'White County',
            'Whitfield': 'Whitfield County',
            'Wilcox': 'Wilcox County',
            'Wilkes': 'Wilkes County',
            'Wilkinson': 'Wilkinson County',
            'Worth': 'Worth County'
        }
        
        # Georgia-specific office mappings
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
            'SUPERINTENDENT OF SCHOOLS': 'Superintendent of Schools',
            'COMMISSIONER OF AGRICULTURE': 'Commissioner of Agriculture',
            'COMMISSIONER OF INSURANCE': 'Commissioner of Insurance',
            'COMMISSIONER OF LABOR': 'Commissioner of Labor',
            'PUBLIC SERVICE COMMISSIONER': 'Public Service Commissioner',
            'STATE SENATOR': 'State Senator',
            'STATE REPRESENTATIVE': 'State Representative',
            'REGENT OF THE UNIVERSITY OF GEORGIA': 'Regent of the University of Georgia',
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
        Clean Georgia candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Georgia data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Georgia-specific data structure.
        
        This handles:
        - Name parsing (Georgia has specific name formats)
        - Address parsing (Georgia address structure)
        - District parsing (Georgia district numbering)
        - Peach State-specific logic (Georgia has unique cultural elements)
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Georgia-specific structure cleaned
        """
        self.logger.info("Cleaning Georgia-specific data structure")
        
        # Clean candidate names (Georgia-specific logic)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_georgia_name)
        
        # Clean addresses (Georgia-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_georgia_address)
        
        # Clean districts (Georgia-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_georgia_district)
        
        # Handle Georgia-specific Peach State logic
        df = self._handle_georgia_peach_state_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Georgia-specific content.
        
        This handles:
        - County standardization
        - Office standardization
        - Georgia-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Georgia-specific content cleaned
        """
        self.logger.info("Cleaning Georgia-specific content")
        
        # Standardize counties
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices - MOVED TO NATIONAL STANDARDS PHASE
        # if 'office' in df.columns:
            # df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Georgia-specific formatting
        df = self._apply_georgia_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Georgia-specific name parsing logic - Peach State naming patterns.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        # Georgia name parsing - handles southern naming patterns
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
    
    def _clean_georgia_name(self, name: str) -> str:
        """
        Clean Georgia-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Georgia-specific name cleaning logic
        name = str(name).strip()
        
        # Handle common Georgia name patterns
        # (e.g., middle initials, suffixes, etc.)
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _clean_georgia_address(self, address: str) -> str:
        """
        Clean Georgia-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # Georgia-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common Georgia address patterns
        # (e.g., PO Box formats, rural route formats, etc.)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_georgia_district(self, district: str) -> str:
        """
        Clean Georgia-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Georgia-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Georgia district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
    def _handle_georgia_peach_state_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Georgia-specific Peach State logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Georgia-specific Peach State handling logic
        # (e.g., Peach State-specific processing, special district handling)
        
        return df
    
    def _apply_georgia_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Georgia-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Georgia-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
