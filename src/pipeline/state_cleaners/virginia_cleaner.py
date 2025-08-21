#!/usr/bin/env python3
"""
Virginia State Data Cleaner

This module contains functions to clean and standardize Virginia political candidate data
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

class VirginiaCleaner:
    """Handles cleaning and standardization of Virginia political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "Virginia"
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
        
    def ensure_column_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure columns match Alaska's exact order."""
        ALASKA_COLUMN_ORDER = [
            'election_year', 'election_type', 'office', 'district',
            'full_name_display', 'first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname',
            'party', 'phone', 'email', 'address', 'website',
            'state', 'address_state', 'original_name', 'original_state', 'original_election_year',
            'original_office', 'original_filing_date', 'id', 'stable_id', 'county',
            'city', 'zip_code', 'filing_date', 'election_date', 'facebook', 'twitter'
        ]
        
        for col in ALASKA_COLUMN_ORDER:
            if col not in df.columns:
                df[col] = None
        
        return df[ALASKA_COLUMN_ORDER]
    
    def clean_virginia_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize Virginia candidate data according to final schema.
        
        Args:
            df: Raw Virginia candidate data DataFrame
            
        Returns:
            Cleaned DataFrame conforming to final schema
        """
        logger.info(f"Starting Virginia data cleaning for {len(df)} records...")
        
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
        
        # Step 7: Remove duplicate columns
        cleaned_df = self._remove_duplicate_columns(cleaned_df)
        
        # Step 8: Map geographic data
        cleaned_df = self._map_geographic_data(cleaned_df)
        
        # Final step: Ensure column order matches Alaska's exact structure
        cleaned_df = self.ensure_column_order(cleaned_df)
        
        logger.info(f"Virginia data cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove original columns that have been replaced by cleaned versions."""
        logger.info("Removing duplicate columns...")
        
        columns_to_remove = [
            'Election', 'Office', 'Name', 'Party', 'Address', 'Email', 'Website', 'Phone Number'
        ]
        
        columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove)
            logger.info(f"Removed {len(columns_to_remove)} duplicate columns: {columns_to_remove}")
        
        return df

    def _process_election_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process election year and type from election column."""
        logger.info("Processing election data...")
        
        # Check if election_year already exists (from main pipeline filename extraction)
        if 'election_year' in df.columns and df['election_year'].notna().any():
            logger.info("election_year already exists from main pipeline, preserving existing data")
            # Only set election_type if it doesn't exist
            if 'election_type' not in df.columns:
                df['election_type'] = 'General'  # Default for Virginia
            return df
        
        def extract_election_info(election_str: str) -> Tuple[Optional[int], Optional[str]]:
            if pd.isna(election_str):
                return None, None
            
            election_str = str(election_str).strip()
            
            year_match = re.search(r'20\d{2}', election_str)
            if year_match:
                year = int(year_match.group())
            else:
                return None, None
            
            election_str_lower = election_str.lower()
            if 'primary' in election_str_lower:
                election_type = "Primary"
            elif 'general' in election_str_lower:
                election_type = "General"
            elif 'special' in election_str_lower:
                election_type = "Special"
            else:
                election_type = "General"
            
            return year, election_type
        
        # Check if Election column exists, if not set defaults
        if 'Election' not in df.columns:
            logger.warning("Election column not found in Virginia data, setting defaults")
            df['election_year'] = None
            df['election_type'] = 'General'  # Default for Virginia
            return df
        
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
            
            if "US PRESIDENT" in office_str or "US VICE PRESIDENT" in office_str:
                return "US President", None
            
            if "UNITED STATES REPRESENTATIVE" in office_str:
                return "US Representative", "At Large"
            
            senate_match = re.match(r'SENATE DISTRICT ([A-Z])', office_str)
            if senate_match:
                district = senate_match.group(1)
                return "State Senate", district
            
            house_match = re.match(r'HOUSE DISTRICT (\d+)', office_str)
            if house_match:
                district = house_match.group(1)
                return "State House", district
            
            return office_str, None
        
        # Apply office and district processing - handle different column names
        office_col = 'Office Title' if 'Office Title' in df.columns else 'Office'
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
            
            if "US PRESIDENT" in office_str or "US VICE PRESIDENT" in office_str:
                if '/' in name_str:
                    first_part = name_str.split('/')[0].strip()
                    if ',' in first_part:
                        last_name, first_name = first_part.split(',', 1)
                        return first_name.strip()
                    else:
            
            
                        return first_part
                else:
                    if ',' in name_str:
                        last_name, first_name = name_str.split(',', 1)
                        return first_name.strip()
                    else:
            
            
                        return name_str
            
            cleaned = re.sub(r'\s+', ' ', name_str).strip().strip('"\'')
            return cleaned
        
        # Handle different column names
        office_col = 'Office Title' if 'Office Title' in df.columns else 'Office'
        name_col = 'Candidate Name' if 'Candidate Name' in df.columns else 'Name'
        df['full_name_display'] = df.apply(lambda row: clean_name(row[name_col], row[office_col]), axis=1)
        df = self._parse_names(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse candidate names into components."""
        logger.info("Parsing candidate names...")
        
        df['first_name'] = pd.NA
        df['middle_name'] = pd.NA
        df['last_name'] = pd.NA
        df['prefix'] = pd.NA
        df['suffix'] = pd.NA
        df['nickname'] = pd.NA
        
        # Get the correct name column
        name_col = 'Candidate Name' if 'Candidate Name' in df.columns else 'Name'
        
        for idx, row in df.iterrows():
            name = row['full_name_display']
            office = row['office']
            original_name = row[name_col]
            
            if pd.isna(name) or not name:
                continue
            
            if office == "US President" and pd.notna(original_name):
                original_str = str(original_name)
                if '/' in original_str:
                    first_part = original_str.split('/')[0].strip()
                    parsed = self._parse_standard_name(first_part, original_name)
                else:
                    parsed = self._parse_standard_name(original_name, original_name)
            else:
                parsed = self._parse_standard_name(original_name, original_name)
            
            df.at[idx, 'first_name'] = parsed[0]
            df.at[idx, 'middle_name'] = parsed[1]
            df.at[idx, 'last_name'] = parsed[2]
            df.at[idx, 'prefix'] = parsed[3]
            df.at[idx, 'suffix'] = parsed[4]
            df.at[idx, 'nickname'] = parsed[5]
            df.at[idx, 'full_name_display'] = parsed[6]
        
        return df
    
    def _parse_standard_name(self, name: str, original_name: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Parse a standard name format."""
        first_name = None
        middle_name = None
        last_name = None
        prefix = None
        suffix = None
        nickname = None
        display_name = None
        
        if pd.isna(name) or not name:
            return None, None, None, None, None, None, None
        
        name = str(name).strip().strip('"\'')
        name = re.sub(r'\s+', ' ', name)
        
        if pd.notna(original_name):
            original_str = str(original_name)
            nickname_match = re.search(r'["""\'\u201c\u201d\u2018\u2019]([^""""\'\u201c\u201d\u2018\u2019]+)["""\'\u201c\u201d\u2018\u2019]', original_str)
            if nickname_match:
                nickname = nickname_match.group(1)
                name = re.sub(r'["""\'\u201c\u201d\u2018\u2019][^""""\'\u201c\u201d\u2018\u2019]+["""\'\u201c\u201d\u2018\u2019]', '', name).strip()
        
        suffix_pattern = r'\b(Jr|Sr|II|III|IV|V|VI|VII|VIII|IX|X)\b'
        suffix_match = re.search(suffix_pattern, name, re.IGNORECASE)
        if suffix_match:
            suffix = suffix_match.group(1)
            name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE).strip()
        
        if ',' in name:
            parts = [part.strip() for part in name.split(',')]
            if len(parts) >= 2:
                last_name = parts[0]
                first_middle = parts[1].split()
                
                if len(first_middle) == 0:
                    # Handle empty first_middle list (empty names)
                    first_name = None
                    middle_name = None
                elif len(first_middle) == 1:
                    first_name = first_middle[0]
                elif len(first_middle) == 2:
                    second_part = first_middle[1]
                    if self._is_initial(second_part):
                        first_name = first_middle[0]
                        middle_name = second_part
                    else:
                        first_name = first_middle[0]
                        middle_name = second_part
                else:
                    first_name = first_middle[0]
                    middle_parts = []
                    for part in first_middle[1:]:
                        if self._should_treat_as_middle_name(part):
                            middle_parts.append(part)
                    
                    middle_name = ' '.join(middle_parts) if middle_parts else None
                
                display_name = self._build_display_name(first_name, middle_name, last_name, suffix, nickname)
                return first_name, middle_name, last_name, prefix, suffix, nickname, display_name
        
        parts = name.split()
        
        if len(parts) == 1:
            return parts[0], None, None, None, suffix, nickname, parts[0]
        elif len(parts) == 2:
            if self._is_initial_or_suffix(parts[1]):
                return parts[0], None, None, None, suffix, nickname, parts[0]
            else:
            
            
                return parts[0], None, parts[1], None, suffix, nickname, f"{parts[0]} {parts[1]}"
        elif len(parts) == 3:
            if self._is_initial(parts[1]):
                return parts[0], parts[1], parts[2], None, suffix, nickname, f"{parts[0]} {parts[1]} {parts[2]}"
            else:
            
            
                return parts[0], parts[1], parts[2], None, suffix, nickname, f"{parts[0]} {parts[1]} {parts[2]}"
        else:
            first = parts[0]
            last = parts[-1]
            
            if self._is_initial_or_suffix(last):
                last = parts[-2] if len(parts) > 2 else None
                middle_parts = parts[1:-2] if len(parts) > 3 else []
            else:
                middle_parts = parts[1:-1]
            
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
        
        # Check if Party column exists, if not try alternative names
        if 'Party' in df.columns:
            df['party'] = df['Party'].apply(standardize_party)
        elif 'Political Party' in df.columns:
            df['party'] = df['Political Party'].apply(standardize_party)
        elif 'Political Party Descr' in df.columns:
            df['party'] = df['Political Party Descr'].apply(standardize_party)
        else:
            logger.warning("No party column found in Virginia data, setting to None")
            df['party'] = None
        
        return df
    
    def _clean_contact_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean contact information."""
        logger.info("Cleaning contact information...")
        
        def clean_phone(phone_str: str) -> str:
            if pd.isna(phone_str):
                return None
            
            digits = re.sub(r'[^\d]', '', str(phone_str))
            
            if len(digits) == 10:
                return digits
            elif len(digits) == 11 and digits.startswith('1'):
                return digits[1:]
            
            return None
        
        def clean_email(email_str: str) -> str:
            if pd.isna(email_str):
                return None
            
            email = str(email_str).strip().lower()
            
            if '@' in email and '.' in email.split('@')[1]:
                return email
            
            return None
        
        def clean_address(address_str: str) -> str:
            if pd.isna(address_str):
                return None
            
            cleaned = str(address_str).strip().strip('"\'')
            cleaned = re.sub(r'\s+', ' ', cleaned)
            return cleaned
        
        # Handle phone - check multiple possible column names
        if 'Campaign Day Time Phone' in df.columns:
            df['phone'] = df['Campaign Day Time Phone'].apply(clean_phone)
        elif 'Campaign Phone' in df.columns:
            df['phone'] = df['Campaign Phone'].apply(clean_phone)
        elif 'Phone Number' in df.columns:
            df['phone'] = df['Phone Number'].apply(clean_phone)
        elif 'Phone' in df.columns:
            df['phone'] = df['Phone'].apply(clean_phone)
        else:
            logger.warning("No phone column found in Virginia data, setting to None")
            df['phone'] = None
        
        # Handle email - check multiple possible column names
        if 'Campaign Email' in df.columns:
            df['email'] = df['Campaign Email'].apply(clean_email)
        elif 'Email' in df.columns:
            df['email'] = df['Email'].apply(clean_email)
        else:
            logger.warning("No email column found in Virginia data, setting to None")
            df['email'] = None
        
        # Handle address - combine multiple address columns for better coverage
        def combine_addresses(row):
            address_parts = []
            
            # Check for Campaign Address columns first (most specific)
            if 'Campaign Address Line 1' in row and pd.notna(row['Campaign Address Line 1']):
                address_parts.append(str(row['Campaign Address Line 1']).strip())
            if 'Campaign Address Line 2' in row and pd.notna(row['Campaign Address Line 2']):
                address_parts.append(str(row['Campaign Address Line 2']).strip())
            
            # Fallback to general Address columns
            if not address_parts and 'Address 1' in row and pd.notna(row['Address 1']):
                address_parts.append(str(row['Address 1']).strip())
            if 'Address 2' in row and pd.notna(row['Address 2']):
                address_parts.append(str(row['Address 2']).strip())
            
            # Final fallback
            if not address_parts and 'Address' in row and pd.notna(row['Address']):
                address_parts.append(str(row['Address']).strip())
            
            if address_parts:
                combined = ' '.join(address_parts)
                return clean_address(combined)
            return None
        
        df['address'] = df.apply(combine_addresses, axis=1)
        
        # Handle website - check multiple possible column names
        if 'Campaign Website' in df.columns:
            df['website'] = df['Campaign Website'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        elif 'Website' in df.columns:
            df['website'] = df['Website'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        else:
            logger.warning("No website column found in Virginia data, setting to None")
            df['website'] = None
        
        # Map address_state from State column (Virginia has 100% coverage)
        if 'State' in df.columns:
            df['address_state'] = df['State'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
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
        
        df['state'] = self.state_name
        
        # Get the correct name column
        name_col = 'Candidate Name' if 'Candidate Name' in df.columns else 'Name'
        df['original_name'] = df[name_col].copy()
        
        df['original_state'] = df['state'].copy()
        df['original_election_year'] = df['election_year'].copy()
        # Handle different column names
        office_col = 'Office Title' if 'Office Title' in df.columns else 'Office'
        df['original_office'] = df[office_col].copy()
        df['original_filing_date'] = pd.NA
        
        required_columns = [
            'id', 'stable_id', 'county', 'city', 'zip_code', 'address_state', 'filing_date', 
            'election_date', 'facebook', 'twitter', 'prefix', 'suffix', 'nickname'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        df['id'] = ""
        
        return df
    
    def _generate_stable_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate stable IDs from original data."""
        logger.info("Generating stable IDs...")
        
        import hashlib
        
        def generate_stable_id(row):
            # Create a comprehensive string from key fields
            id_parts = []
            
            # Key fields for stable ID generation
            key_fields = [
                'original_name', 'original_state', 'original_election_year',
                'original_office', 'party', 'address', 'email', 'phone'
            ]
            
            for field in key_fields:
                if field in row and pd.notna(row[field]):
                    value = str(row[field]).strip().lower()
                    id_parts.append(f"{field}:{value}")
                else:
                    id_parts.append(f"{field}:NULL_VALUE")
            
            # Sort for consistency
            id_parts.sort()
            id_string = "||".join(id_parts)
            
            # Generate SHA-256 hash
            return hashlib.sha256(id_string.encode('utf-8')).hexdigest()[:16]
        
        df['stable_id'] = df.apply(generate_stable_id, axis=1)
        
        return df

    def _map_geographic_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map geographic data from Virginia raw data columns."""
        logger.info("Mapping geographic data...")
        
        # Map city data
        if 'Campaign City' in df.columns:
            df['city'] = df['Campaign City'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        elif 'City' in df.columns:
            df['city'] = df['City'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        else:
            logger.warning("No city column found in Virginia data, setting to None")
            df['city'] = None
        
        # Map zip code data
        if 'Campaign Zip' in df.columns:
            df['zip_code'] = df['Campaign Zip'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        elif 'Zip' in df.columns:
            df['zip_code'] = df['Zip'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        else:
            logger.warning("No zip code column found in Virginia data, setting to None")
            df['zip_code'] = None
        
        # Map county/locality data
        if 'Locality Name' in df.columns:
            df['county'] = df['Locality Name'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        elif 'Locality' in df.columns:
            df['county'] = df['Locality'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        else:
            logger.warning("No county/locality column found in Virginia data, setting to None")
            df['county'] = None
        
        # Map district data - preserve original District column data
        if 'District' in df.columns:
            df['district'] = df['District'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        
        return df

    def _is_initial_or_suffix(self, part: str) -> bool:
        """Check if a name part is an initial, suffix, or nickname."""
        if not part:
            return False
        
        part = part.strip()
        
        if len(part) <= 2 and (part.endswith('.') or len(part) == 1):
            return True
        
        suffixes = ['jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv', 'v', 'vi', 'vii', 'viii', 'ix', 'x']
        if part.lower() in suffixes:
            return True
        
        if (part.startswith('"') and part.endswith('"')) or (part.startswith('"') and part.endswith('"')):
            return True
        
        if len(part) == 1:
            return True
        
        return False
    
    def _is_initial(self, part: str) -> bool:
        """Check if a name part is an initial."""
        if not part:
            return False
        
        part = part.strip()
        
        if len(part) <= 2 and (part.endswith('.') or len(part) == 1):
            return True
        
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
        
        if self._is_initial_or_suffix(part):
            return False
        
        if '"' in part or '"' in part or '"' in part or "'" in part or "'" in part or "'" in part or '\u201c' in part or '\u201d' in part or '\u2018' in part or '\u2019' in part:
            return False
        
        return True
    
    def _build_display_name(self, first_name: str, middle_name: str, last_name: str, suffix: str, nickname: str) -> str:
        """Build a display name from components."""
        if not first_name:
            return ""
        
        parts = [first_name]
        
        if nickname:
            parts.append(f'"{nickname}"')
        
        if middle_name:
            parts.append(middle_name)
        
        if last_name:
            parts.append(last_name)
        
        if suffix:
            parts.append(suffix)
        
        return ' '.join(parts).strip()

def clean_virginia_candidates(input_file: str, output_file: str = None, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean Virginia candidate data.
    
    Args:
        input_file: Path to the input Excel file
        output_file: Optional path to save the cleaned data
        output_dir: Directory to save cleaned data (default: "cleaned_data")
        
    Returns:
        Cleaned DataFrame
    """
    logger.info(f"Loading Virginia data from {input_file}...")
    df = pd.read_excel(input_file)
    
    cleaner = VirginiaCleaner(output_dir=output_dir)
    cleaned_df = cleaner.clean_virginia_data(df)
    
    if output_file is None:
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"{base_name}_cleaned_{timestamp}.xlsx")
    
    if not os.path.dirname(output_file):
        output_file = os.path.join(output_dir, output_file)
    
    # Ensure output file has .xlsx extension
    if not output_file.endswith('.xlsx'):
        output_file = output_file + '.xlsx'
    
    logger.info(f"Saving cleaned data to {output_file}...")
    cleaned_df.to_excel(output_file, index=False)
    logger.info(f"Data saved successfully!")
    
    return cleaned_df

if __name__ == "__main__":
    print("Available input files:")
    available_files = list_available_input_files()
    if available_files:
        for i, file_path in enumerate(available_files, 1):
            print(f"  {i}. {os.path.basename(file_path)}")
    else:
        print("  No Excel files found in input directory")
        exit(1)
    
    if available_files:
        input_file = available_files[0]
        output_dir = DEFAULT_OUTPUT_DIR
        
        print(f"\nProcessing: {os.path.basename(input_file)}")
        print(f"Output directory: {output_dir}")
        
        cleaned_data = clean_virginia_candidates(input_file, output_dir=output_dir)
        print(f"\nCleaned {len(cleaned_data)} records")
        print(f"Columns: {cleaned_data.columns.tolist()}")
        print(f"Data saved to: {output_dir}/")
    else:
        print("No input files available for processing")
