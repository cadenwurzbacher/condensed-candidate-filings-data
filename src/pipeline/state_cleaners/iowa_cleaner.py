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
DEFAULT_OUTPUT_DIR = "cleaned_data"
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
        
    def ensure_column_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure columns match Alaska's exact order."""
        ALASKA_COLUMN_ORDER = [
            'election_year', 'election_type', 'office', 'district', 'full_name_display',
            'first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname',
            'party', 'phone', 'email', 'address', 'website', 'state', 'original_name',
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
        
        # Step 1: Handle election year and type
        cleaned_df = self._process_election_data(cleaned_df, filename)
        
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
        
        # Final step: Ensure column order matches Alaska's exact structure
        cleaned_df = self.ensure_column_order(cleaned_df)
        
        logger.info(f"Iowa data cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def _process_election_data(self, df: pd.DataFrame, filename: str = None) -> pd.DataFrame:
        """Process election year and type from election column or filename."""
        logger.info("Processing election data...")
        
        # Check if Election column exists, if not try to extract from filename
        if 'Election' not in df.columns:
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
            df['election_type'] = "General"  # Default for Iowa
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
                election_type = "General"  # Default
            
            return year, election_type
        
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
        office_results = df['Office'].apply(process_office_district)
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
        name_col = 'Ballot Name(s)' if 'Ballot Name(s)' in df.columns else 'Name'
        df['full_name_display'] = df.apply(lambda row: clean_name(row[name_col], row['Office']), axis=1)
        
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
            name_col = 'Ballot Name(s)' if 'Ballot Name(s)' in df.columns else 'Name'
            original_name = row[name_col]
            
            if pd.isna(name) or not name:
                continue
            
            # Handle US President cases
            if office == "US President" and pd.notna(original_name):
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
        
        df['party'] = df['Party'].apply(standardize_party)
        
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
        
        # Clean addresses
        def clean_address(address_str: str) -> str:
            if pd.isna(address_str):
                return None
            
            cleaned = str(address_str).strip().strip('"\'')
            cleaned = re.sub(r'\s+', ' ', cleaned)
            return cleaned
        
        # Apply cleaning - handle different column names
        phone_col = 'Phone' if 'Phone' in df.columns else 'Phone Number'
        df['phone'] = df[phone_col].apply(clean_phone)
        df['email'] = df['Email'].apply(clean_email)
        df['address'] = df['Address'].apply(clean_address)
        # Iowa doesn't have Website column - set to None
        df['website'] = None
        
        return df
    
    def _add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all required columns for the final schema."""
        logger.info("Adding required columns...")
        
        # Add state column
        df['state'] = self.state_name
        
        # Add original data preservation columns
        # Iowa has 'Ballot Name(s)' instead of 'Name'
        if 'Name' in df.columns:
            df['original_name'] = df['Name'].copy()
        elif 'Ballot Name(s)' in df.columns:
            df['original_name'] = df['Ballot Name(s)'].copy()
        else:
            df['original_name'] = 'Unknown'
        df['original_state'] = df['state'].copy()
        df['original_election_year'] = df['election_year'].copy()
        df['original_office'] = df['Office'].copy()
        df['original_filing_date'] = pd.NA
        
        # Add missing columns with None values
        required_columns = [
            'id', 'stable_id', 'county', 'city', 'zip_code', 'filing_date', 
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
        
        columns_to_remove = [
            'Election', 'Office', 'Name', 'Party', 'Address', 'Email', 'Website', 'Phone'
        ]
        
        columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove)
            logger.info(f"Removed {len(columns_to_remove)} duplicate columns: {columns_to_remove}")
        
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
