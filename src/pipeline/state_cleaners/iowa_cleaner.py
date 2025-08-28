#!/usr/bin/env python3
"""
Iowa State Data Cleaner

This module contains functions to clean and standardize Iowa political candidate data
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
DEFAULT_OUTPUT_DIR = "data/processed"
DEFAULT_INPUT_DIR = "Raw State Data - Current"

def list_available_input_files(input_dir: str = DEFAULT_INPUT_DIR) -> List[str]:
    """List all available Excel files in the input directory."""
    if not os.path.exists(input_dir):
        logger.warning(f"Input directory {input_dir} does not exist")
        return []
    
    excel_files = []
    for file in os.listdir(input_dir):
        if file.endswith(('.xlsx', '.xls')) and not file.startswith('~$'):
            excel_files.append(os.path.join(input_dir, file))
    
    return sorted(excel_files)

class IowaCleaner:
    """Handles cleaning and standardization of Iowa political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "Iowa"
        self.output_dir = output_dir
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
    
    def _preprocess_iowa_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess Iowa data to handle the weird format with blank office cells."""
        logger.info("Preprocessing Iowa data format...")
        
        # Step 1: Fill in blank office cells by forward-filling from row above
        logger.info("Filling in blank office cells...")
        df['office'] = df['office'].ffill()
        
        # Step 2: Filter out rows where candidate name is "No Candidate"
        logger.info("Filtering out 'No Candidate' rows...")
        initial_count = len(df)
        df = df[~df['candidate_name'].str.contains('No Candidate', na=False)]
        filtered_count = len(df)
        logger.info(f"Filtered out {initial_count - filtered_count} 'No Candidate' rows")
        
        # Step 3: Convert "-" values to null across all columns
        logger.info("Converting '-' values to null...")
        for col in df.columns:
            if df[col].dtype == 'object':  # Only process string columns
                df.loc[:, col] = df[col].replace('-', pd.NA)
        
        logger.info(f"Preprocessing completed. Shape after preprocessing: {df.shape}")
        return df
        
    def ensure_column_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure columns match Alaska's exact order."""
        ALASKA_COLUMN_ORDER = [
            'election_year', 'election_type', 'office', 'district', 'full_name_display',
            'first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname',
            'party', 'phone', 'email', 'address', 'website', 'state', 'address_state', 'original_name',
            'original_state', 'original_election_year', 'original_office', 'original_filing_date',
            'id', 'stable_id', 'county', 'city', 'zip_code', 'filing_date', 'election_date',
            'facebook', 'twitter'
        ]
        
        for col in ALASKA_COLUMN_ORDER:
            if col not in df.columns:
                df[col] = None
        
        return df[ALASKA_COLUMN_ORDER]

    def clean_iowa_data(self, df: pd.DataFrame, filename: str = None) -> pd.DataFrame:
        """Clean and standardize Iowa candidate data according to final schema."""
        logger.info(f"Starting Iowa data cleaning for {len(df)} records...")
        
        cleaned_df = df.copy()
        
        # Step 1: Preprocess Iowa-specific data format issues
        cleaned_df = self._preprocess_iowa_format(cleaned_df)
        
        # Step 2: Handle election year and type
        cleaned_df = self._process_election_data(cleaned_df, filename)
        
        # Step 3: Clean and standardize office and district information
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
        
        # Final step: Ensure column order matches Alaska's exact structure
        cleaned_df = self.ensure_column_order(cleaned_df)
        
        logger.info(f"Iowa data cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def _process_election_data(self, df: pd.DataFrame, filename: str = None) -> pd.DataFrame:
        """Process election year and type from election column or filename."""
        logger.info("Processing election data...")
        
        # Check if Election column exists, if not try to extract from filename
        if 'election_type' not in df.columns:
            if filename:
                # Extract year from filename (e.g., "iowa_candidates_2024.xlsx" -> 2024)
                year_match = re.search(r'(\d{4})', filename)
                if year_match:
                    election_year = int(year_match.group(1))
                    logger.info(f"Extracted election year {election_year} from filename")
                else:
                    election_year = None
                    logger.warning("No election year found in filename")
            else:
                election_year = None
                logger.warning("No filename provided and no Election column found")
            
            df['election_year'] = election_year
            df['election_type'] = "Unknown"  # No default for Iowa
            return df
        
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
                election_type = "Unknown"
            
            return year, election_type
        
        # Apply election processing
        election_results = df['election_type'].apply(extract_election_info)
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
                return "US Representative", "At Large"
            
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
            
            # Handle other offices (keep as is)
            return office_str, None
        
        # Apply office and district processing
        office_results = df['office'].apply(process_office_district)
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
            
            # For non-president cases, clean the name
            cleaned = re.sub(r'\s+', ' ', name_str).strip().strip('"\'')
            return cleaned
        
        # Apply name cleaning with office context - handle different column names
        name_col = 'candidate_name' if 'candidate_name' in df.columns else 'candidate_name'
        df['full_name_display'] = df.apply(lambda row: clean_name(row[name_col], row['office']), axis=1)
        
        # Parse names into components
        df = self._parse_names(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse candidate names into components."""
        logger.info("Parsing candidate names...")
        
        # Initialize new columns (don't overwrite full_name_display!)
        df['first_name'] = pd.NA
        df['middle_name'] = pd.NA
        df['last_name'] = pd.NA
        df['prefix'] = pd.NA
        df['suffix'] = pd.NA
        df['nickname'] = pd.NA
        
        for idx, row in df.iterrows():
            name = row['full_name_display']
            office = row['office']
            name_col = 'candidate_name' if 'candidate_name' in df.columns else 'candidate_name'
            original_name = row[name_col]
            
            if pd.isna(name) or not name:
                continue
            
            # Handle US President cases (Iowa uses "President/Vice President")
            if (office == "US President" or office == "President/Vice President") and pd.notna(original_name):
                original_str = str(original_name)
                if '/' in original_str:
                    first_part = original_str.split('/')[0].strip()
                    parsed = self._parse_standard_name(first_part, original_name)
                else:
                    parsed = self._parse_standard_name(original_name, original_name)
            else:
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
    
    def _parse_standard_name(self, name: str, original_name: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Parse a standard name format."""
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
            nickname_match = re.search(r'["""\'\u201c\u201d\u2018\u2019]([^""""\'\u201c\u201d\u2018\u2019]+)["""\'\u201c\u201d\u2018\u2019]', original_str)
            if nickname_match:
                nickname = nickname_match.group(1)
                name = re.sub(r'["""\'\u201c\u201d\u2018\u2019][^""""\'\u201c\u201d\u2018\u2019]+["""\'\u201c\u201d\u2018\u2019]', '', name).strip()
        
        # Extract suffix from the end of the name
        suffix_pattern = r'\b(Jr|Sr|II|III|IV|V|VI|VII|VIII|IX|X)\b'
        suffix_match = re.search(suffix_pattern, name, re.IGNORECASE)
        if suffix_match:
            suffix = suffix_match.group(1)
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
        
        # Handle regular space-separated names
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
        
        df['party'] = df['party'].apply(standardize_party)
        
        return df
    
    def _clean_contact_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean contact information."""
        logger.info("Cleaning contact information...")
        
        # Clean phone numbers
        def clean_phone(phone_str: str) -> str:
            if pd.isna(phone_str):
                return None
            
            digits = re.sub(r'[^\d]', '', str(phone_str))
            
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
            
            if '@' in email and '.' in email.split('@')[1]:
                return email
            
            return None
        
        # Enhanced address parsing and cleaning for Iowa
        def clean_address(address_str: str) -> str:
            if pd.isna(address_str):
                return None
            
            cleaned = str(address_str).strip().strip('"\'')
            cleaned = re.sub(r'\s+', ' ', cleaned)
            
            # Extract and remove ZIP codes from address
            zip_pattern = re.compile(r'\b\d{5}(?:-\d{4})?\b')
            zip_match = zip_pattern.search(cleaned)
            if zip_match:
                zip_code = zip_match.group(0)
                # Don't extract PO Box numbers as ZIP codes
                if not re.search(r'\bPO\s+BOX\s*' + re.escape(zip_code), cleaned, re.IGNORECASE):
                    # Remove ZIP from address
                    cleaned = zip_pattern.sub('', cleaned).strip().rstrip(',')
            
            # Extract and remove state abbreviations
            state_patterns = [
                r',\s*([A-Z]{2})\s*,?\s*$',  # State at end with optional comma
                r',\s*([A-Z]{2})\s*$',       # State at end
                r'\s+([A-Z]{2})\s+\d{5}',   # State before ZIP
                r'\b([A-Z]{2})\b'            # Any 2-letter code (more aggressive)
            ]
            
            for pattern in state_patterns:
                state_match = re.search(pattern, cleaned)
                if state_match:
                    state_code = state_match.group(1)
                    # Filter out common non-state abbreviations
                    non_state_abbrevs = {'ST', 'RD', 'DR', 'LN', 'CT', 'BL', 'APT', 'STE', 'UNIT', 'PO', 'BOX', 'AVE', 'WAY', 'PL', 'CR', 'CRT', 'CIR', 'HWY', 'US', 'SR', 'CO', 'INC', 'LLC', 'LTD', 'CORP'}
                    if state_code not in non_state_abbrevs:
                        # Remove state from address
                        cleaned = re.sub(pattern, '', cleaned).strip().rstrip(',')
                        break
            
            # Extract city (everything between street and state/ZIP)
            if ',' in cleaned:
                parts = [p.strip() for p in cleaned.split(',') if p.strip()]
                if len(parts) >= 2:
                    if len(parts) == 2:
                        # "Street, City" format
                        cleaned = parts[0]
                    elif len(parts) >= 3:
                        # "Street, City, State" format - city is middle part
                        last_part = parts[-1].strip()
                        if re.match(r'^[A-Z]{2}$', last_part):
                            # Last part is state, second-to-last is city
                            cleaned = ','.join(parts[:-2])
                        else:
                            # Last part is not state, so city is last part
                            cleaned = ','.join(parts[:-1])
                    else:
                        # Only 2 parts, treat as "Street, City"
                        cleaned = parts[0]
            
            return cleaned.strip().rstrip(',')
        
        # Apply cleaning - handle different column names
        phone_col = 'phone' if 'phone' in df.columns else 'phone'
        df['phone'] = df[phone_col].apply(clean_phone)
        df['email'] = df['email'].apply(clean_email)
        df['address'] = df['address'].apply(clean_address)
        # Iowa doesn't have Website column - set to None
        df['website'] = None
        
        # Enhanced ZIP code and city extraction from address
        def extract_zip_from_address(addr):
            if pd.isna(addr) or not isinstance(addr, str):
                return None
            
            zip_pattern = re.compile(r'\b\d{5}(?:-\d{4})?\b')
            zip_match = zip_pattern.search(addr)
            if zip_match:
                zip_code = zip_match.group(0)
                # Don't extract PO Box numbers as ZIP codes
                if not re.search(r'\bPO\s+BOX\s*' + re.escape(zip_code), addr, re.IGNORECASE):
                    return zip_code
            return None
        
        # Extract ZIP codes from address if not already present
        if 'zip_code' not in df.columns:
            df['zip_code'] = df['address'].apply(extract_zip_from_address)
        else:
            # Update existing zip_code with extracted values if empty
            df['zip_code'] = df.apply(lambda row: extract_zip_from_address(row.get('address')) if pd.isna(row.get('zip_code')) or str(row.get('zip_code')).strip() == '' else row.get('zip_code'), axis=1)
        
        # Extract cities from address
        def extract_city_from_address(addr):
            if pd.isna(addr) or not isinstance(addr, str):
                return None
            
            # Look for city in comma-separated format
            if ',' in addr:
                parts = [p.strip() for p in addr.split(',') if p.strip()]
                if len(parts) >= 2:
                    if len(parts) == 2:
                        # "Street, City" format
                        return parts[1]
                    elif len(parts) >= 3:
                        # "Street, City, State" format - city is middle part
                        last_part = parts[-1].strip()
                        if re.match(r'^[A-Z]{2}$', last_part):
                            # Last part is state, second-to-last is city
                            return parts[-2]
                        else:
                            # Last part is not state, so city is last part
                            return parts[-1]
            return None
        
        # Extract cities from address if not already present
        if 'city' not in df.columns:
            df['city'] = df['address'].apply(extract_city_from_address)
        else:
            # Update existing city with extracted values if empty
            df['city'] = df.apply(lambda row: extract_city_from_address(row.get('address')) if pd.isna(row.get('city')) or str(row.get('city')).strip() == '' else row.get('city'), axis=1)
        
        # Enhanced address_state extraction from address
        def extract_state(addr: Optional[str]) -> Optional[str]:
            if addr is None or pd.isna(addr):
                return None
            s = str(addr)
            
            # Look for state codes in address
            state_patterns = [
                r',\s*([A-Z]{2})\s*,?\s*$',  # State at end with optional comma
                r',\s*([A-Z]{2})\s*$',       # State at end
                r'\s+([A-Z]{2})\s+\d{5}',   # State before ZIP
                r'\b([A-Z]{2})\b'            # Any 2-letter code (more aggressive)
            ]
            
            for pattern in state_patterns:
                state_match = re.search(pattern, s)
                if state_match:
                    state_code = state_match.group(1)
                    # Filter out common non-state abbreviations
                    non_state_abbrevs = {'ST', 'RD', 'DR', 'LN', 'CT', 'BL', 'APT', 'STE', 'UNIT', 'PO', 'BOX', 'AVE', 'WAY', 'PL', 'CR', 'CRT', 'CIR', 'HWY', 'US', 'SR', 'CO', 'INC', 'LLC', 'LTD', 'CORP'}
                    if state_code not in non_state_abbrevs:
                        return state_code
            
            # Fallback to original pattern
            m = re.search(r"\b([A-Z]{2})\s+\d{5}(?:-\d{4})?\b", s)
            return m.group(1) if m else None
        
        df['address_state'] = df['address'].apply(extract_state)
        
        return df
    
    def _add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all required columns for the final schema."""
        logger.info("Adding required columns...")
        
        # Add state column
        df['state'] = self.state_name
        
        # Add original data preservation columns
        # Iowa has 'candidate_name' instead of 'candidate_name'
        if 'candidate_name' in df.columns:
            df['original_name'] = df['candidate_name'].copy()
        elif 'candidate_name' in df.columns:
            df['original_name'] = df['candidate_name'].copy()
        else:
            
            
            
            
            df['original_name'] = 'Unknown'
        df['original_state'] = df['state'].copy()
        df['original_election_year'] = df['election_year'].copy()
        df['original_office'] = df['office'].copy()
        # Map Filing Date to filing_date
        if 'filing_date' in df.columns:
            df['original_filing_date'] = df['filing_date'].copy()
            df['filing_date'] = df['filing_date'].copy()
        else:
            df['original_filing_date'] = pd.NA
            df['filing_date'] = pd.NA
        
        # Add missing columns with None values
        required_columns = [
            'id', 'stable_id', 'county', 'city', 'zip_code', 
            'election_date', 'facebook', 'twitter', 'prefix', 'suffix', 'nickname'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Set id to empty string (will be generated later in process)
        df['id'] = ""
        
        return df
    
    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove original columns that have been replaced by cleaned versions."""
        logger.info("Removing duplicate columns...")
        
        
        
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

def clean_iowa_candidates(input_file: str, output_file: str = None, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """Main function to clean Iowa candidate data."""
    logger.info(f"Loading Iowa data from {input_file}...")
    df = pd.read_excel(input_file)
    
    cleaner = IowaCleaner(output_dir=output_dir)
    # Extract filename for election year extraction
    filename = os.path.basename(input_file)
    cleaned_df = cleaner.clean_iowa_data(df, filename)
    
    if output_file is None:
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"{base_name}_cleaned_{timestamp}.xlsx")
    
    if not os.path.dirname(output_file):
        output_file = os.path.join(output_dir, output_file)
    
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
        
        cleaned_data = clean_iowa_candidates(input_file, output_dir=output_dir)
        print(f"\nCleaned {len(cleaned_data)} records")
        print(f"Columns: {cleaned_data.columns.tolist()}")
        print(f"Data saved to: {output_dir}/")
    else:
        print("No input files available for processing")
