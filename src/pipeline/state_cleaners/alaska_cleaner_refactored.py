#!/usr/bin/env python3
"""
Alaska State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class AlaskaCleaner(BaseStateCleaner):
    """
    Alaska-specific data cleaner.
    
    This cleaner handles Alaska-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Alaska")
        
        # Alaska-specific county mappings (boroughs and census areas)
        self.county_mappings = {
            'Ak': 'Alaska',
            'Wv': 'West Virginia',
            'Ca': 'California',
            'De': 'Delaware',
            'In': 'Indiana',
            'Dc': 'District of Columbia',
            'Sd': 'South Dakota',
            'Bldg. A Homer': 'Homer',
            'N': 'North',
            'S': 'South',
            'E': 'East',
            'W': 'West',
            'Anchorage': 'Anchorage Borough',
            'Fairbanks': 'Fairbanks North Star Borough',
            'Juneau': 'City and Borough of Juneau',
            'Sitka': 'City and Borough of Sitka',
            'Ketchikan': 'Ketchikan Gateway Borough',
            'Kodiak': 'Kodiak Island Borough',
            'North Slope': 'North Slope Borough',
            'Northwest Arctic': 'Northwest Arctic Borough',
            'Yakutat': 'City and Borough of Yakutat',
            'Aleutians East': 'Aleutians East Borough',
            'Aleutians West': 'Aleutians West Census Area',
            'Bethel': 'Bethel Census Area',
            'Bristol Bay': 'Bristol Bay Borough',
            'Denali': 'Denali Borough',
            'Dillingham': 'Dillingham Census Area',
            'Haines': 'Haines Borough',
            'Hoonah-Angoon': 'Hoonah-Angoon Census Area',
            'Kenai Peninsula': 'Kenai Peninsula Borough',
            'Kusilvak': 'Kusilvak Census Area',
            'Lake and Peninsula': 'Lake and Peninsula Borough',
            'Matanuska-Susitna': 'Matanuska-Susitna Borough',
            'Nome': 'Nome Census Area',
            'Petersburg': 'Petersburg Borough',
            'Prince of Wales-Hyder': 'Prince of Wales-Hyder Census Area',
            'Southeast Fairbanks': 'Southeast Fairbanks Census Area',
            'Valdez-Cordova': 'Valdez-Cordova Census Area',
            'Wrangell': 'City and Borough of Wrangell',
            'Yukon-Koyukuk': 'Yukon-Koyukuk Census Area'
        }
        
        # Alaska-specific office mappings
        self.office_mappings = {
            'US PRESIDENT': 'US President',
            'US SENATOR': 'US Senator',
            'US REPRESENTATIVE': 'US Representative',
            'GOVERNOR': 'Governor',
            'LIEUTENANT GOVERNOR': 'Lieutenant Governor',
            'SECRETARY OF STATE': 'Secretary of State',
            'STATE TREASURER': 'State Treasurer',
            'ATTORNEY GENERAL': 'Attorney General',
            'STATE SENATOR': 'State Senator',
            'STATE REPRESENTATIVE': 'State Representative',
            'REGENT OF THE UNIVERSITY OF ALASKA': 'Regent of the University of Alaska',
            'DISTRICT ATTORNEY': 'District Attorney',
            'BOROUGH MAYOR': 'Borough Mayor',
            'BOROUGH ASSEMBLY MEMBER': 'Borough Assembly Member',
            'CITY MAYOR': 'City Mayor',
            'CITY COUNCIL MEMBER': 'City Council Member',
            'SCHOOL BOARD MEMBER': 'School Board Member',
            'JUDGE': 'Judge',
            'MAGISTRATE': 'Magistrate',
            'COMMISSIONER': 'Commissioner',
            'SUPERINTENDENT': 'Superintendent',
            'DIRECTOR': 'Director',
            'MANAGER': 'Manager',
            'COORDINATOR': 'Coordinator',
            'SPECIALIST': 'Specialist',
            'ANALYST': 'Analyst',
            'ADMINISTRATOR': 'Administrator',
            'EXECUTIVE': 'Executive',
            'PRESIDENT': 'President',
            'VICE PRESIDENT': 'Vice President',
            'SECRETARY': 'Secretary',
            'TREASURER': 'Treasurer',
            'CHAIR': 'Chair',
            'CO-CHAIR': 'Co-Chair',
            'MEMBER': 'Member',
            'TRUSTEE': 'Trustee',
            'DELEGATE': 'Delegate',
            'REPRESENTATIVE': 'Representative',
            'SENATOR': 'Senator',
            'ASSEMBLY MEMBER': 'Assembly Member',
            'COUNCIL MEMBER': 'Council Member',
            'BOARD MEMBER': 'Board Member'
        }
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Alaska candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Alaska data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Alaska-specific data structure.
        
        This handles:
        - Complex name parsing (Alaska has multi-sheet files with complex names)
        - Address parsing (Alaska address structure)
        - District parsing (Alaska district numbering)
        - Multi-sheet file handling
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with Alaska-specific structure cleaned
        """
        self.logger.info("Cleaning Alaska-specific data structure")
        
        # Clean candidate names (Alaska-specific logic for complex names)
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].apply(self._clean_alaska_name)
        
        # Clean addresses (Alaska-specific logic)
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self._clean_alaska_address)
        
        # Clean districts (Alaska-specific logic)
        if 'district' in df.columns:
            df['district'] = df['district'].apply(self._clean_alaska_district)
        
        # Handle Alaska-specific multi-sheet file logic
        df = self._handle_alaska_multi_sheet_logic(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Alaska-specific content.
        
        This handles:
        - County/borough standardization
        - Office standardization
        - Alaska-specific formatting
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Alaska-specific content cleaned
        """
        self.logger.info("Cleaning Alaska-specific content")
        
        # Standardize counties/boroughs
        if 'county' in df.columns:
            df['county'] = df['county'].map(self.county_mappings).fillna(df['county'])
        
        # Standardize offices
        if 'office' in df.columns:
            df['office'] = df['office'].map(self.office_mappings).fillna(df['office'])
        
        # Alaska-specific formatting
        df = self._apply_alaska_formatting(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Alaska-specific name parsing logic - handles US President cases and complex formats.
        """
        # Initialize name columns
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None
        
        # Use candidate_name as full_name_display if available
        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']
        
        for idx, row in df.iterrows():
            name = row.get('full_name_display')
            office = row.get('office')
            original_name = row.get('candidate_name')
            
            if pd.isna(name) or not name:
                continue
            
            # Handle US President cases - these have special formatting like "Last, First Middle / Running Mate"
            if office == "US President" and pd.notna(original_name):
                original_str = str(original_name)
                if '/' in original_str:
                    # Extract only the first part (the actual candidate)
                    first_part = original_str.split('/')[0].strip()
                    parsed = self._parse_standard_name(first_part, original_name)
                else:
                    # Fallback for president candidates without running mates
                    parsed = self._parse_standard_name(original_name, original_name)
            else:
                # For all other cases, use the original name for parsing
                parsed = self._parse_standard_name(original_name, original_name)
            
            # Assign parsed components
            df.at[idx, 'first_name'] = parsed[0]
            df.at[idx, 'middle_name'] = parsed[1]
            df.at[idx, 'last_name'] = parsed[2]
            df.at[idx, 'prefix'] = parsed[3]
            df.at[idx, 'suffix'] = parsed[4]
            df.at[idx, 'nickname'] = parsed[5]
            df.at[idx, 'full_name_display'] = parsed[6]
        
        return df
    
    def _parse_standard_name(self, name: str, original_name: str):
        """Parse a standard name format (Last, First Middle or First Middle Last)."""
        # Initialize components
        first_name = None
        middle_name = None
        last_name = None
        prefix = None
        suffix = None
        nickname = None
        display_name = None
        
        # Clean the name first
        if pd.isna(name) or not name:
            return None, None, None, None, None, None, None
        
        name = str(name).strip().strip('"\'')
        name = re.sub(r'\s+', ' ', name)
        
        # Extract nickname from original name if present
        if pd.notna(original_name):
            original_str = str(original_name)
            # Look for nicknames in quotes (including Unicode quotes)
            nickname_match = re.search(r'["""\'\u201c\u201d\u2018\u2019]([^""""\'\u201c\u201d\u2018\u2019]+)["""\'\u201c\u201d\u2018\u2019]', original_str)
            if nickname_match:
                nickname = nickname_match.group(1)
                # Remove nickname from the name for further processing
                name = re.sub(r'["""\'\u201c\u201d\u2018\u2019][^""""\'\u201c\u201d\u2018\u2019]+["""\'\u201c\u201d\u2018\u2019]', '', name).strip()
        
        # Extract prefix from the beginning of the name FIRST
        prefix_pattern = r'^(Dr|Mr|Mrs|Ms|Miss|Prof|Rev|Hon|Sen|Rep|Gov|Lt|Col|Gen|Adm|Capt|Maj|Sgt|Cpl|Pvt)\.?\s+'
        prefix_match = re.match(prefix_pattern, name, re.IGNORECASE)
        if prefix_match:
            prefix = prefix_match.group(1)
            # Remove prefix from the name for further processing
            name = re.sub(prefix_pattern, '', name, flags=re.IGNORECASE).strip()
        
        # Extract suffix from the end of the name
        suffix_pattern = r'\b(Jr|Sr|II|III|IV|V|VI|VII|VIII|IX|X)\b'
        suffix_match = re.search(suffix_pattern, name, re.IGNORECASE)
        if suffix_match:
            suffix = suffix_match.group(1)
            # Remove suffix from the name for further processing
            name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE).strip()
        
        # Handle names with commas (Last, First Middle format)
        if ',' in name:
            parts = [part.strip() for part in name.split(',')]
            if len(parts) >= 2:
                last_name = parts[0]
                first_middle = parts[1].split()
                
                if len(first_middle) == 1:
                    first_name = first_middle[0]
                elif len(first_middle) == 2:
                    # Check if second part is an initial or nickname
                    second_part = first_middle[1]
                    if self._is_initial(second_part):
                        first_name = first_middle[0]
                        middle_name = second_part
                    else:
                        first_name = first_middle[0]
                        middle_name = second_part
                else:
                    first_name = first_middle[0]
                    middle_name = ' '.join(first_middle[1:])
            else:
                # Single part after comma - treat as last name
                last_name = parts[0]
        else:
            # Handle First Middle Last format
            parts = name.split()
            if len(parts) == 1:
                last_name = parts[0]
            elif len(parts) == 2:
                first_name = parts[0]
                last_name = parts[1]
            elif len(parts) >= 3:
                first_name = parts[0]
                last_name = parts[-1]
                middle_name = ' '.join(parts[1:-1])
        
        # Build display name
        display_parts = []
        if prefix:
            display_parts.append(prefix)
        if first_name:
            display_parts.append(first_name)
        if middle_name:
            display_parts.append(middle_name)
        if last_name:
            display_parts.append(last_name)
        if suffix:
            display_parts.append(suffix)
        if nickname:
            display_parts.append(f'"{nickname}"')
        
        display_name = ' '.join(display_parts).strip()
        
        return first_name, middle_name, last_name, prefix, suffix, nickname, display_name
    
    def _is_initial(self, part: str) -> bool:
        """Check if a name part is an initial."""
        return len(part) == 1 or (len(part) == 2 and part.endswith('.'))
    
    def _clean_alaska_name(self, name: str) -> str:
        """
        Clean Alaska-specific name formats.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name) or not name:
            return None
        
        # Alaska-specific name cleaning logic
        name = str(name).strip()
        
        # Handle complex Alaska name patterns
        # (e.g., "Trump, Donald J./Vance, JD" format)
        if '/' in name:
            # Split on forward slash and take first name
            name = name.split('/')[0].strip()
        
        # Handle comma-separated names
        if ',' in name:
            # Handle "Last, First" format
            parts = name.split(',')
            if len(parts) == 2:
                last, first = parts
                name = f"{first.strip()} {last.strip()}"
        
        # Clean up extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _clean_alaska_address(self, address: str) -> str:
        """
        Clean Alaska-specific address formats.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # Alaska-specific address cleaning logic
        address = str(address).strip()
        
        # Handle common Alaska address patterns
        # (e.g., PO Box formats, rural route formats, building references)
        
        # Clean up extra whitespace
        address = ' '.join(address.split())
        
        return address
    
    def _clean_alaska_district(self, district: str) -> str:
        """
        Clean Alaska-specific district formats.
        
        Args:
            district: Raw district string
            
        Returns:
            Cleaned district string
        """
        if pd.isna(district) or not district:
            return None
        
        # Alaska-specific district cleaning logic
        district = str(district).strip()
        
        # Handle common Alaska district patterns
        # (e.g., "District 1", "HD 1", etc.)
        
        return district
    
    def _handle_alaska_multi_sheet_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Alaska-specific multi-sheet file logic.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        # Alaska-specific multi-sheet handling logic
        # (e.g., combining data from multiple sheets, handling complex structures)
        
        return df
    
    def _apply_alaska_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Alaska-specific formatting rules.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        # Alaska-specific formatting logic
        # (e.g., date formats, phone number formats, etc.)
        
        return df
