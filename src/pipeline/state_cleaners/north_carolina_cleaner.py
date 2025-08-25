#!/usr/bin/env python3
"""
North Carolina State Data Cleaner

This module contains functions to clean and standardize North Carolina political candidate data
according to the final database schema requirements.
"""

import pandas as pd
import re
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
DEFAULT_OUTPUT_DIR = "data/processed"  # Default output directory for cleaned data
DEFAULT_INPUT_DIR = "Raw State Data - Current"  # Default input directory

def list_available_input_files(input_dir: str = DEFAULT_INPUT_DIR) -> List[str]:
    """
    List all available Excel files in the input directory.
    
    Args:
        input_dir: Directory to search for input files
        
    Returns:
        List of available Excel file paths
    """
    if not os.path.exists(input_dir):
        logger.warning(f"Input directory {input_dir} does not exist")
        return []
    
    excel_files = []
    for file in os.listdir(input_dir):
        if file.endswith(('.xlsx', '.xls')) and not file.startswith('~$'):
            excel_files.append(os.path.join(input_dir, file))
    
    return sorted(excel_files)

class NorthCarolinaCleaner:
    """Handles cleaning and standardization of North Carolina political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "North Carolina"
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
        
    def ensure_column_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure columns match Alaska's exact order."""
        ALASKA_COLUMN_ORDER = [
            'election_year', 'election_type', 'office', 'district', 'full_name_display',
            'first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname',
            'party', 'phone', 'email', 'address', 'website',
            'state', 'address_state', 'original_name', 'original_state', 'original_election_year',
            'original_office', 'original_filing_date', 'id', 'stable_id', 'county',
            'city', 'zip_code', 'filing_date', 'election_date', 'facebook', 'twitter'
        ]
        
        for col in ALASKA_COLUMN_ORDER:
            if col not in df.columns:
                df[col] = None
        
        return df[ALASKA_COLUMN_ORDER]
    
    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove original columns that have been replaced by cleaned versions."""
        logger.info("Removing duplicate columns...")
        
        # Columns to remove (original versions) - handle different column names
        columns_to_remove = []
        if 'Election' in df.columns:
            columns_to_remove.append('Election')
        if 'Office' in df.columns:
            columns_to_remove.append('Office')
        if 'Name' in df.columns:
            columns_to_remove.append('Name')
        if 'Party' in df.columns:
            columns_to_remove.append('Party')
        if 'Address' in df.columns:
            columns_to_remove.append('Address')
        if 'Email' in df.columns:
            columns_to_remove.append('Email')
        if 'Website' in df.columns:
            columns_to_remove.append('Website')
        if 'Phone Number' in df.columns:
            columns_to_remove.append('Phone Number')
        
        # Only remove if they exist and we have cleaned versions
        columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove)
            logger.info(f"Removed {len(columns_to_remove)} duplicate columns: {columns_to_remove}")
        
        return df

    def clean_north_carolina_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize North Carolina candidate data according to final schema.
        
        Args:
            df: Raw North Carolina candidate data DataFrame
            
        Returns:
            Cleaned DataFrame conforming to final schema
        """
        logger.info(f"Starting North Carolina data cleaning for {len(df)} records...")
        
        # Create a copy to avoid modifying original
        cleaned_df = df.copy()
        
        # Step 1: Handle election year and type
        cleaned_df = self._process_election_data(cleaned_df)
        
        # Step 2: Clean and standardize office and district information
        cleaned_df = self._process_office_and_district(cleaned_df)
        
        # Step 3: Clean candidate names
        cleaned_df = self._process_full_name_displays(cleaned_df)
        
        # Step 4: Standardize party names
        cleaned_df = self._standardize_parties(cleaned_df)
        
        # Step 5: Clean contact information
        cleaned_df = self._clean_contact_info(cleaned_df)
        
        # Step 6: Add required columns for final schema
        cleaned_df = self._add_required_columns(cleaned_df)
        
        # Step 7: Generate stable IDs (skipped - will be done later in process)
        ## Step 8: Remove duplicate columns
        cleaned_df = self._remove_duplicate_columns(cleaned_df)
        
        # Final step: Ensure column order matches Alaska's exact structure
        cleaned_df = self.ensure_column_order(cleaned_df)
        
        logger.info(f"North Carolina data cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def _process_election_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process election year and type from election column."""
        logger.info("Processing election data...")
        
        def extract_election_info(election_str: str) -> Tuple[Optional[int], Optional[str]]:
            if pd.isna(election_str):
                return None, None
            
            election_str = str(election_str).strip()
            
            # Extract year from election string
            year_match = re.search(r'20\d{2}', election_str)
            if year_match:
                year = int(year_match.group())
            else:
            
            
                return None, None
            
            # Determine election type
            election_str_lower = election_str.lower()
            if 'primary' in election_str_lower:
                election_type = "Primary"
            elif 'general' in election_str_lower:
                election_type = "General"
            elif 'special' in election_str_lower:
                election_type = "Special"
            else:
                election_type = "General"  # Default
            
            return year, election_type
        
        # Check if election_year is already present (from merged files)
        if 'election_year' in df.columns and df['election_year'].notna().any():
            logger.info("Election year already present from merged files, skipping extraction")
            # Still need to set election_type if not present
            if 'election_type' not in df.columns:
                df['election_type'] = None
            return df
        
        # Check if Election column exists, if not set defaults
        if 'Election' not in df.columns:
            logger.warning("Election column not found in North Carolina data, setting defaults")
            df['election_year'] = None
            df['election_type'] = None
            return df
        
        # Apply election processing
        election_results = df['Election'].apply(extract_election_info)
        df['election_year'] = [result[0] for result in election_results]
        df['election_type'] = [result[1] for result in election_results]
        
        return df
    
    def _process_office_and_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize office and district information."""
        logger.info("Processing office and district information...")
        
        def process_office_district(office_str: str) -> Tuple[str, Optional[str]]:
            if pd.isna(office_str):
                return None, None
            
            office_str = str(office_str).strip()
            
            # Handle US President/Vice President
            if "US PRESIDENT" in office_str or "US VICE PRESIDENT" in office_str:
                return "US President", None
            
            # Handle US Representative
            if "UNITED STATES REPRESENTATIVE" in office_str or "US REPRESENTATIVE" in office_str:
                # Extract district number if present
                district_match = re.search(r'DISTRICT (\d+)', office_str, re.IGNORECASE)
                if district_match:
                    district = district_match.group(1)
                else:
                    district = "At Large"
                return "US Representative", district
            
            # Handle US Senate
            if "UNITED STATES SENATOR" in office_str or "US SENATOR" in office_str:
                return "US Senator", None
            
            # Handle State Senate districts
            senate_match = re.match(r'STATE SENATE DISTRICT (\d+)', office_str, re.IGNORECASE)
            if senate_match:
                district = senate_match.group(1)
                return "State Senate", district
            
            # Handle State House districts
            house_match = re.match(r'STATE HOUSE DISTRICT (\d+)', office_str, re.IGNORECASE)
            if house_match:
                district = house_match.group(1)
                return "State House", district
            
            # Handle North Carolina specific patterns
            # Look for DISTRICT XX or WARD XX patterns
            district_match = re.search(r'(?:DISTRICT|WARD)\s+(\d+)', office_str, re.IGNORECASE)
            if district_match:
                district_num = district_match.group(1)
                # Check if it's a WARD or DISTRICT
                if 'WARD' in office_str.upper():
                    district = f"Ward {district_num}"
                else:
                    district = district_num
                # Clean up the office name by removing the district part
                clean_office = re.sub(r'\s+(?:DISTRICT|WARD)\s+\d+', '', office_str, flags=re.IGNORECASE).strip()
                return clean_office, district
            
            # Handle other offices (keep as is)
            return office_str, None
        
        # Apply office and district processing - handle different column names
        office_col = 'contest_name' if 'contest_name' in df.columns else 'Office'
        office_results = df[office_col].apply(process_office_district)
        df['office'] = [result[0] for result in office_results]
        df['district'] = [result[1] for result in office_results]
        df['district'] = df['district'].astype('object')
        
        return df
    
    def _process_full_name_displays(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process candidate names."""
        logger.info("Processing candidate names...")
        
        def clean_name(name_str: str, office_str: str) -> str:
            if pd.isna(name_str):
                return None
            
            name_str = str(name_str).strip()
            office_str = str(office_str).strip() if pd.notna(office_str) else ""
            
            # Handle US President cases - keep only first name
            if "US PRESIDENT" in office_str or "US VICE PRESIDENT" in office_str:
                # Extract first name from president candidates
                if '/' in name_str:
                    # Handle cases like "Trump, Donald J./Vance, JD"
                    first_part = name_str.split('/')[0].strip()
                    if ',' in first_part:
                        last_name, first_name = first_part.split(',', 1)
                        return first_name.strip()
                    else:
            
            
                        return first_part
                else:
            
            
            
            
                    # Handle single names
                    if ',' in name_str:
                        last_name, first_name = name_str.split(',', 1)
                        return first_name.strip()
                    else:
            
            
                        return name_str
            
            # For non-president cases, clean the name
            # Remove extra whitespace and quotes
            cleaned = re.sub(r'\s+', ' ', name_str).strip().strip('"\'')
            return cleaned
        
        # Apply name cleaning with office context - handle different column names
        name_col = 'name_on_ballot' if 'name_on_ballot' in df.columns else 'Name'
        office_col = 'contest_name' if 'contest_name' in df.columns else 'Office'
        df['full_name_display'] = df.apply(lambda row: clean_name(row[name_col], row[office_col]), axis=1)
        
        # Parse names into components
        df = self._parse_names(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse candidate names into first, middle, last, prefix, suffix, nickname, and display components."""
        logger.info("Parsing candidate names...")
        
        # Initialize new columns
        df['first_name'] = pd.NA
        df['middle_name'] = pd.NA
        df['last_name'] = pd.NA
        df['prefix'] = pd.NA
        df['suffix'] = pd.NA
        df['nickname'] = pd.NA
        
        for idx, row in df.iterrows():
            name = row['full_name_display']
            office_col = 'contest_name' if 'contest_name' in df.columns else 'office'
            office = row[office_col]
            # Get original name from the appropriate column
            name_col = 'name_on_ballot' if 'name_on_ballot' in df.columns else 'Name'
            original_name = row[name_col]
            
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
                try:
                    parsed = self._parse_standard_name(original_name, original_name)
                    
                    # Safety check: ensure we got a 7-tuple
                    if parsed is None or len(parsed) != 7:
                        logger.warning(f"Name parsing failed for '{original_name}', using fallback")
                        parsed = (original_name, None, None, None, None, None, original_name)
                except Exception as e:
                    logger.error(f"Exception in _parse_standard_name for '{original_name}': {e}")
                    logger.error(f"Using fallback values")
                    parsed = (original_name, None, None, None, None, None, original_name)
            
            # Assign parsed components with detailed error handling
            try:
                df.at[idx, 'first_name'] = parsed[0]
                df.at[idx, 'middle_name'] = parsed[1]
                df.at[idx, 'last_name'] = parsed[2]
                df.at[idx, 'prefix'] = parsed[3]
                df.at[idx, 'suffix'] = parsed[4]
                df.at[idx, 'nickname'] = parsed[5]
                df.at[idx, 'full_name_display'] = parsed[6]
            except (IndexError, TypeError) as e:
                logger.error(f"Error assigning parsed components for '{original_name}': {e}")
                logger.error(f"Parsed result: {parsed}")
                logger.error(f"Parsed type: {type(parsed)}")
                logger.error(f"Parsed length: {len(parsed) if parsed else 'None'}")
                
                # Use fallback values
                df.at[idx, 'first_name'] = original_name if original_name else 'Unknown'
                df.at[idx, 'middle_name'] = None
                df.at[idx, 'last_name'] = None
                df.at[idx, 'prefix'] = None
                df.at[idx, 'suffix'] = None
                df.at[idx, 'nickname'] = None
                df.at[idx, 'full_name_display'] = original_name if original_name else 'Unknown'
        
        return df
    
    def _parse_standard_name(self, name: str, original_name: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
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
                    elif '"' in second_part or '"' in second_part or '"' in second_part or "'" in second_part or "'" in second_part or "'" in second_part or '\u201c' in second_part or '\u201d' in second_part or '\u2018' in second_part or '\u2019' in second_part:
                        # This is a nickname, not a middle name
                        first_name = first_middle[0]
                        # Nickname should already be extracted above
                        middle_name = None
                    else:
                        first_name = first_middle[0]
                        middle_name = second_part
                else:
                    # Handle multiple parts
                    first_name = first_middle[0]
                    middle_parts = []
                    for part in first_middle[1:]:
                        if self._should_treat_as_middle_name(part):
                            middle_parts.append(part)
                    
                    middle_name = ' '.join(middle_parts) if middle_parts else None
                
                display_name = self._build_display_name(first_name, middle_name, last_name, suffix, nickname)
                return first_name, middle_name, last_name, prefix, suffix, nickname, display_name
        
        # Handle regular space-separated names
        parts = name.split()
        
        if len(parts) == 1:
            return parts[0], None, None, None, suffix, nickname, parts[0]
        elif len(parts) == 2:
            # Check if second part is an initial, suffix, or nickname
            if self._is_initial_or_suffix(parts[1]):
                return parts[0], None, None, None, suffix, nickname, parts[0]
            else:
                return parts[0], None, parts[1], None, suffix, nickname, f"{parts[0]} {parts[1]}"
        elif len(parts) == 3:
            # Check if second part is an initial
            if self._is_initial(parts[1]):
                return parts[0], parts[1], parts[2], None, suffix, nickname, f"{parts[0]} {parts[1]} {parts[2]}"
            else:
                return parts[0], parts[1], parts[2], None, suffix, nickname, f"{parts[0]} {parts[1]} {parts[2]}"
        else:
            # For names with more than 3 parts, treat first as first, last as last, rest as middle
            first = parts[0]
            last = parts[-1]
            
            # Check if last part is an initial, suffix, or nickname
            if self._is_initial_or_suffix(last):
                last = parts[-2] if len(parts) > 2 else None
                middle_parts = parts[1:-2] if len(parts) > 3 else []
            else:
                middle_parts = parts[1:-1]
            
            # Filter out initials, suffixes, and nicknames from middle parts
            filtered_middle_parts = []
            for part in middle_parts:
                if self._should_treat_as_middle_name(part):
                    filtered_middle_parts.append(part)
            
            middle = ' '.join(filtered_middle_parts) if filtered_middle_parts else None
            display = self._build_display_name(first, middle, last, suffix, nickname)
            return first, middle, last, prefix, suffix, nickname, display
    
    def _standardize_parties(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize party names."""
        logger.info("Standardizing party names...")
        
        party_mapping = {
            'democrat': 'Democratic',
            'republican': 'Republican',
            'independent': 'Independent',
            'libertarian': 'Libertarian',
            'green': 'Green',
            'constitution': 'Constitution',
            'reform': 'Reform',
            'natural law': 'Natural Law',
            'socialist': 'Socialist',
            'communist': 'Communist',
            'american independent': 'American Independent',
            'peace and freedom': 'Peace and Freedom',
            'working families': 'Working Families',
            'women\'s equality': 'Women\'s Equality',
            'independence': 'Independence',
            'conservative': 'Conservative',
            'liberal': 'Liberal',
            'moderate': 'Moderate',
            'progressive': 'Progressive',
            'tea party': 'Tea Party',
            'no party preference': 'No Party Preference',
            'nonpartisan': 'Nonpartisan',
            'unaffiliated': 'Unaffiliated',
            'decline to state': 'Decline to State',
            'write-in': 'Write-in',
            'other': 'Other'
        }
        
        def standardize_party(party_str: str) -> str:
            if pd.isna(party_str):
                return None
            
            party_lower = str(party_str).strip().lower()
            return party_mapping.get(party_lower, party_str)
        
        # Handle different party column names - North Carolina uses party_candidate
        if 'party_candidate' in df.columns:
            df['party'] = df['party_candidate'].apply(standardize_party)
        elif 'Party' in df.columns:
            df['party'] = df['Party'].apply(standardize_party)
        else:
            # No party column found, set to None
            df['party'] = pd.NA
            logger.warning("No party column found in North Carolina data, setting to None")
        
        return df
    
    def _clean_contact_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean contact information (phone, email, address, website)."""
        logger.info("Cleaning contact information...")
        
        # Clean phone numbers
        def clean_phone(phone_str: str) -> str:
            if pd.isna(phone_str):
                return None
            
            # Remove all non-digit characters
            digits = re.sub(r'[^\d]', '', str(phone_str))
            
            # Validate US phone number
            if len(digits) == 10:
                return digits
            elif len(digits) == 11 and digits.startswith('1'):
                return digits[1:]
            
            return None
        
        # Clean email addresses
        def clean_email(email_str: str) -> str:
            if pd.isna(email_str):
                return None
            
            email = str(email_str).strip().lower()
            
            # Basic email validation
            if '@' in email and '.' in email.split('@')[1]:
                return email
            
            return None
        
        # Clean addresses
        def clean_address(address_str: str) -> str:
            if pd.isna(address_str):
                return None
            
            # Remove extra whitespace and quotes
            cleaned = str(address_str).strip().strip('"\'')
            # Remove multiple spaces
            cleaned = re.sub(r'\s+', ' ', cleaned)
            return cleaned
        
        # Apply cleaning - handle different column names
        if 'phone' in df.columns:
            df['phone'] = df['phone'].apply(clean_phone)
        elif 'Phone Number' in df.columns:
            df['phone'] = df['Phone Number'].apply(clean_phone)
        else:
            df['phone'] = pd.NA
            
        if 'email' in df.columns:
            df['email'] = df['email'].apply(clean_email)
        elif 'Email' in df.columns:
            df['Email'].apply(clean_email)
        else:
            df['email'] = pd.NA
            
        if 'street_address' in df.columns:
            df['address'] = df['street_address'].apply(clean_address)
        elif 'Address' in df.columns:
            df['address'] = df['Address'].apply(clean_address)
        else:
            df['address'] = pd.NA
            
        # North Carolina doesn't have website data
        df['website'] = pd.NA
        
        # Map county from county_name column
        if 'county_name' in df.columns:
            df['county'] = df['county_name'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        else:
            df['county'] = pd.NA
        
        # Map city from city column
        if 'city' in df.columns:
            df['city'] = df['city'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        else:
            df['city'] = pd.NA
        
        # Map zip_code from zip_code column
        if 'zip_code' in df.columns:
            df['zip_code'] = df['zip_code'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        else:
            df['zip_code'] = pd.NA
        
        # Map address_state from state column (North Carolina data has explicit state field)
        if 'state' in df.columns:
            df['address_state'] = df['state'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        else:
            # Fallback: derive address_state from address when possible
            def extract_state(addr: Optional[str]) -> Optional[str]:
                if addr is None or pd.isna(addr):
                    return None
                s = str(addr)
                m = re.search(r"\b([A-Z]{2})\s+\d{5}(?:-\d{4})?\b", s)
                return m.group(1) if m else None
            df['address_state'] = df['address'].apply(extract_state)
        
        return df
    
    def _add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all required columns for the final schema."""
        logger.info("Adding required columns...")
        
        # Add state column
        df['state'] = self.state_name
        
        # Add original data preservation columns - handle different column names
        name_col = 'name_on_ballot' if 'name_on_ballot' in df.columns else 'Name'
        df['original_name'] = df[name_col].copy()
        df['original_state'] = df['state'].copy()
        df['original_election_year'] = df['election_year'].copy()
        # Handle different office column names
        office_col = 'contest_name' if 'contest_name' in df.columns else 'Office'
        df['original_office'] = df[office_col].copy()
        df['original_filing_date'] = pd.NA  # Not available in North Carolina data
        
        # Add missing columns with None values
        required_columns = [
            'id', 'stable_id', 'filing_date', 
            'election_date', 'facebook', 'twitter', 'prefix', 'suffix', 'nickname'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Map filing_date from candidacy_dt
        if 'candidacy_dt' in df.columns:
            def parse_filing_date(filing_str: str) -> str:
                """Parse filing date from North Carolina format MM/DD/YYYY."""
                if pd.isna(filing_str):
                    return None
                
                filing_str = str(filing_str).strip()
                
                try:
                    # Parse and standardize date format
                    date_obj = pd.to_datetime(filing_str, format='%m/%d/%Y')
                    return date_obj.strftime('%Y-%m-%d')
                except:
                    return filing_str
            
            df['filing_date'] = df['candidacy_dt'].apply(parse_filing_date)
            df['original_filing_date'] = df['candidacy_dt'].copy()
        else:
            df['filing_date'] = pd.NA
            df['original_filing_date'] = pd.NA
        
        # Set id to empty string (will be generated later in process)
        df['id'] = ""
        
        # Preserve source file info if present (from merged files)
        if '_source_file' in df.columns:
            logger.info(f"Preserving source file information for {len(df)} records")
        
        return df
    
    

    def _is_initial_or_suffix(self, part: str) -> bool:
        """Check if a name part is an initial, suffix, or nickname."""
        if not part:
            return False
        
        part = part.strip()
        
        # Check for initials (single letter with or without period)
        if len(part) <= 2 and (part.endswith('.') or len(part) == 1):
            return True
        
        # Check for common suffixes
        suffixes = ['jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv', 'v', 'vi', 'vii', 'viii', 'ix', 'x']
        if part.lower() in suffixes:
            return True
        
        # Check for nicknames (enclosed in quotes)
        if (part.startswith('"') and part.endswith('"')) or (part.startswith('"') and part.endswith('"')):
            return True
        
        # Check for single letters
        if len(part) == 1:
            return True
        
        return False
    
    def _is_initial(self, part: str) -> bool:
        """Check if a name part is an initial."""
        if not part:
            return False
        
        part = part.strip()
        
        # Check for initials (single letter with or without period)
        if len(part) <= 2 and (part.endswith('.') or len(part) == 1):
            return True
        
        # Check for single letters
        if len(part) == 1:
            return True
        
        return False
    
    def _clean_nickname(self, part: str) -> str:
        """Clean nickname by removing quotes."""
        if not part:
            return part
        
        part = part.strip()
        # Remove quotes from nicknames
        if (part.startswith('"') and part.endswith('"')) or (part.startswith('"') and part.endswith('"')):
            return part[1:-1]
        
        return part
    
    def _should_treat_as_middle_name(self, part: str) -> bool:
        """Determine if a part should be treated as a middle name."""
        if not part:
            return False
        
        part = part.strip()
        
        # Don't treat initials, suffixes, or nicknames as middle names
        if self._is_initial_or_suffix(part):
            return False
        
        # Don't treat nicknames as middle names (check for quotes)
        if '"' in part or '"' in part or '"' in part or "'" in part or "'" in part or "'" in part or '\u201c' in part or '\u201d' in part or '\u2018' in part or '\u2019' in part:
            return False
        
        return True
    
    def _build_display_name(self, first_name: str, middle_name: str, last_name: str, suffix: str, nickname: str) -> str:
        """Build a display name from components."""
        if not first_name:
            return ""
        
        parts = [first_name]
        
        # Add nickname if present
        if nickname:
            parts.append(f'"{nickname}"')
        
        # Add middle name if present
        if middle_name:
            parts.append(middle_name)
        
        # Add last name if present
        if last_name:
            parts.append(last_name)
        
        # Add suffix if present
        if suffix:
            parts.append(suffix)
        
        return ' '.join(parts).strip()

def clean_north_carolina_candidates(input_file: str, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean North Carolina candidate data.
    
    Args:
        input_file: Path to the input Excel file
        output_file: Optional path to save the cleaned data (if None, will use default naming in output_dir)
        output_dir: Directory to save cleaned data (default: "cleaned_data")
        
    Returns:
        Cleaned DataFrame
    """
    # Load the data - handle both CSV and Excel files
    logger.info(f"Loading North Carolina data from {input_file}...")
    if input_file.endswith('.csv'):
        df = pd.read_csv(input_file, encoding='latin-1')
    else:
        df = pd.read_excel(input_file)
    
    # Initialize cleaner with output directory
    cleaner = NorthCarolinaCleaner(output_dir=output_dir)
    
    # Clean the data
    cleaned_df = cleaner.clean_north_carolina_data(df)
    
    # Generate output filename
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"{base_name}_cleaned_{timestamp}.xlsx")
    
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Save the cleaned data
    logger.info(f"Saving cleaned data to {output_file}...")
    cleaned_df.to_excel(output_file, index=False)
    logger.info(f"Data saved successfully!")
    
    return cleaned_df

if __name__ == "__main__":
    # Show available input files
    print("Available input files:")
    available_files = list_available_input_files()
    if available_files:
        for i, file_path in enumerate(available_files, 1):
            print(f"  {i}. {os.path.basename(file_path)}")
    else:
        print("  No Excel files found in input directory")
        exit(1)
    
    # Example usage - use the first available file
    if available_files:
        input_file = available_files[0]  # Use the first available file
        output_dir = DEFAULT_OUTPUT_DIR
        
        print(f"\nProcessing: {os.path.basename(input_file)}")
        print(f"Output directory: {output_dir}")
        
        # Clean the data and save to the new output directory
        cleaned_data = clean_north_carolina_candidates(input_file, output_dir=output_dir)
        print(f"\nCleaned {len(cleaned_data)} records")
        print(f"Columns: {cleaned_data.columns.tolist()}")
        print(f"Data saved to: {output_dir}/")
    else:
        print("No input files available for processing")
