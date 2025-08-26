#!/usr/bin/env python3
"""
Pennsylvania State Data Cleaner

This module contains functions to clean and standardize Pennsylvania political candidate data
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

class PennsylvaniaCleaner:
    """Handles cleaning and standardization of Pennsylvania political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "Pennsylvania"
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
        
    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove original columns that have been replaced by cleaned versions."""
        logger.info("Removing duplicate columns...")
        
        # Columns to remove (original versions)
        
        
        return df

    def clean_pennsylvania_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize Pennsylvania candidate data according to final schema.
        
        Args:
            df: Raw Pennsylvania candidate data DataFrame
            
        Returns:
            Cleaned DataFrame conforming to final schema
        """
        logger.info(f"Starting Pennsylvania data cleaning for {len(df)} records...")
        
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
        
        logger.info(f"Pennsylvania data cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
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
        
        # Check if Election column exists, if not set defaults
        if 'election_type' not in df.columns:
            logger.warning("Election column not found in Pennsylvania data, setting defaults")
            df['election_year'] = None
            df['election_type'] = None
            return df
        
        # Apply election processing
        election_results = df['election_type'].apply(extract_election_info)
        df['election_year'] = [result[0] for result in election_results]
        df['election_type'] = [result[1] for result in election_results]
        
        return df
    
    def _process_office_and_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize office and district information."""
        logger.info("Processing office and district information...")
        
        def process_office_district(office_str: str, district_str: str) -> Tuple[str, Optional[str]]:
            if pd.isna(office_str):
                return None, None
            
            office_str = str(office_str).strip()
            district_str = str(district_str).strip() if pd.notna(district_str) else ""
            
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
            
            # Handle Pennsylvania specific patterns
            # Look for district info in the District Name column
            if district_str:
                # Extract district number from patterns like "6th Congressional District", "19th Legislative District"
                district_match = re.search(r'(\d+)(?:st|nd|rd|th)\s+(.+?)\s*District', district_str, re.IGNORECASE)
                if district_match:
                    district_num = district_match.group(1)
                    district_type = district_match.group(2).strip()
                    
                    # Clean up the office name by removing district info
                    clean_office = re.sub(r'\s*\d+(?:st|nd|rd|th)\s+.+?\s*District', '', office_str, flags=re.IGNORECASE).strip()
                    
                    return clean_office, district_num
                
                # Handle other district patterns
                district_match = re.search(r'(\d+)(?:st|nd|rd|th)\s+(.+?)(?:\s*District)?$', district_str, re.IGNORECASE)
                if district_match:
                    district_num = district_match.group(1)
                    return office_str, district_num
            
            # Handle other offices (keep as is)
            return office_str, None
        
        # Apply office and district processing
        office_col = 'office' if 'office' in df.columns else None
        district_col = 'District Name' if 'District Name' in df.columns else None
        
        if office_col and district_col:
            office_district_results = df.apply(lambda row: process_office_district(row[office_col], row[district_col]), axis=1)
            df['office'] = [result[0] for result in office_district_results]
            df['district'] = [result[1] for result in office_district_results]
        elif office_col:
            # Fallback: just process office without district
            df['office'] = df[office_col].apply(lambda x: str(x).strip() if pd.notna(x) else None)
            df['district'] = pd.NA
        else:
            df['office'] = pd.NA
            df['district'] = pd.NA
        
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
        # Pennsylvania data can have different structures depending on the file
        if 'candidate_name' in df.columns:
            name_col = 'candidate_name'
        elif 'Unnamed: 1' in df.columns:
            # The merged data structure has names in Unnamed: 1
            name_col = 'Unnamed: 1'
        elif 'candidate_name' in df.columns:
            name_col = 'candidate_name'
        else:
            
            
            
            
            # Fallback - try to find any column that might contain names
            name_cols = [col for col in df.columns if 'candidate_name' in col.lower() or 'candidate' in col.lower()]
            if name_cols:
                name_col = name_cols[0]
            else:
            
            
            
            
                # Last resort - use the first non-empty column that looks like it has text
                for col in df.columns:
                    if df[col].dtype == 'object' and df[col].notna().any():
                        sample_val = df[col].dropna().iloc[0] if df[col].dropna().any() else None
                        if sample_val and isinstance(sample_val, str) and len(sample_val) > 3:
                            name_col = col
                            break
                else:
                    name_col = 'candidate_name'  # Default fallback
        
        df['full_name_display'] = df.apply(lambda row: clean_name(row[name_col], row['office']), axis=1)
        
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
            office = row['office']
            # Pennsylvania data can have different structures depending on the file
            if 'candidate_name' in df.columns:
                original_name = row['candidate_name']
            elif 'Unnamed: 1' in df.columns:
                # The merged data structure has names in Unnamed: 1
                original_name = row['Unnamed: 1']
            elif 'candidate_name' in df.columns:
                original_name = row['candidate_name']
            else:
            
            
            
            
                # Fallback - try to find any column that might contain names
                name_cols = [col for col in df.columns if 'candidate_name' in col.lower() or 'candidate' in col.lower()]
                if name_cols:
                    original_name = row[name_cols[0]]
                else:
            
            
            
            
                    # Last resort - use the first non-empty column that looks like it has text
                    for col in df.columns:
                        if df[col].dtype == 'object' and df[col].notna().any():
                            sample_val = df[col].dropna().iloc[0] if df[col].dropna().any() else None
                            if sample_val and isinstance(sample_val, str) and len(sample_val) > 3:
                                original_name = row[col]
                                break
                    else:
                        original_name = 'Unknown'
            
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
        
        df['party'] = df['party'].apply(standardize_party)
        
        return df
    
    def _clean_contact_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean contact information (phone, email, address, website)."""
        logger.info("Cleaning contact information...")
        
        # Apply cleaning - Pennsylvania has Municipality and County data
        # Map Municipality to city
        if 'Municipality' in df.columns:
            df['city'] = df['Municipality'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        else:
            df['city'] = pd.NA
        
        # Map County to county
        if 'County' in df.columns:
            df['county'] = df['County'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        else:
            df['county'] = pd.NA
        
        # Pennsylvania doesn't have phone, email, address, website data
        df['phone'] = pd.NA
        df['email'] = pd.NA
        df['address'] = pd.NA
        df['website'] = pd.NA
        
        # All Pennsylvania candidates are in PA
        df['address_state'] = "PA"
        
        # Pennsylvania doesn't have zip code data
        df['zip_code'] = pd.NA
        
        return df
    
    def _add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all required columns for the final schema."""
        logger.info("Adding required columns...")
        
        # Add state column
        df['state'] = self.state_name
        
        # Add original data preservation columns - handle different column names
        # Pennsylvania has complex structure, try to find name column
        if 'candidate_name' in df.columns:
            df['original_name'] = df['candidate_name'].copy()
        else:
            
            
            
            
            # Fallback - create a placeholder name
            df['original_name'] = 'Unknown Candidate'
        df['original_state'] = df['state'].copy()
        df['original_election_year'] = df['election_year'].copy()
        df['original_office'] = df['office'].copy()
        df['original_filing_date'] = pd.NA  # Not available in Pennsylvania data
        
        # Add missing columns with None values
        required_columns = [
            'id', 'stable_id', 'filing_date', 
            'election_date', 'facebook', 'twitter', 'prefix', 'suffix', 'nickname'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Set id to empty string (will be generated later in process)
        df['id'] = ""
        
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

def clean_pennsylvania_candidates(input_file: str, output_file: str = None, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean Pennsylvania candidate data.
    
    Args:
        input_file: Path to the input Excel file
        output_file: Optional path to save the cleaned data (if None, will use default naming in output_dir)
        output_dir: Directory to save cleaned data (default: "cleaned_data")
        
    Returns:
        Cleaned DataFrame
    """
    # Load the data - Pennsylvania files have a merged cell header in row 1, data starts in row 2
    logger.info(f"Loading Pennsylvania data from {input_file}...")
    
    # Try to read with header=0 first to see what we get
    df_test = pd.read_excel(input_file, header=0)
    logger.info(f"Pennsylvania columns with header=0: {df_test.columns.tolist()}")
    
    # Check if the first row is just a merged cell (like "Election Info")
    if 'Election Info' in df_test.columns and 'Unnamed: 1' in df_test.columns:
        # This is the merged cell scenario - skip first row, use second row as header
        logger.info("Detected merged cell header scenario - using header=1")
        df = pd.read_excel(input_file, header=1)
    else:
            
            
            
            
        # This has real column headers - use first row
        logger.info("Detected real column headers - using header=0")
        df = df_test
    
    logger.info(f"Final Pennsylvania columns: {df.columns.tolist()}")
    logger.info(f"Pennsylvania data shape: {df.shape}")
    
    # Initialize cleaner with output directory
    cleaner = PennsylvaniaCleaner(output_dir=output_dir)
    
    # Clean the data
    cleaned_df = cleaner.clean_pennsylvania_data(df)
    
    # Generate output filename if not provided
    if output_file is None:
        # Extract base name from input file
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"{base_name}_cleaned_{timestamp}.xlsx")
    
    # Ensure output file has proper extension and is in the output directory
    if not output_file.endswith('.xlsx'):
        if not output_file.endswith('.'):
            output_file += '.xlsx'
        else:
            output_file = output_file.rstrip('.') + '.xlsx'
    
    # Ensure output file is in the output directory
    if not os.path.dirname(output_file) or os.path.dirname(output_file) == '.':
        output_file = os.path.join(output_dir, os.path.basename(output_file))
    
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
        cleaned_data = clean_pennsylvania_candidates(input_file, output_dir=output_dir)
        print(f"\nCleaned {len(cleaned_data)} records")
        print(f"Columns: {cleaned_data.columns.tolist()}")
        print(f"Data saved to: {output_dir}/")
    else:
        print("No input files available for processing")
