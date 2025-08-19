#!/usr/bin/env python3
"""
South Dakota State Data Cleaner

This module contains functions to clean and standardize South Dakota political candidate data
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

class SouthDakotaCleaner:
    """Handles cleaning and standardization of South Dakota political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "South Dakota"
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
        
    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove original columns that have been replaced by cleaned versions."""
        logger.info("Removing duplicate columns...")
        
        # Columns to remove (original versions)
        columns_to_remove = [
            'Contest', 'Name', 'Party', 'Petition Filing Date', 'District/County', 
            'Ballot Order', 'Mailing Address'
        ]
        
        # Only remove if they exist and we have cleaned versions
        columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove)
            logger.info(f"Removed {len(columns_to_remove)} duplicate columns: {columns_to_remove}")
        
        return df

    def clean_south_dakota_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize South Dakota candidate data according to final schema.
        
        Args:
            df: Raw South Dakota candidate data DataFrame
            
        Returns:
            Cleaned DataFrame conforming to final schema
        """
        logger.info(f"Starting South Dakota data cleaning for {len(df)} records...")
        
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
        # cleaned_df = self._generate_stable_ids(cleaned_df)
        
        # Step 8: Remove duplicate columns
        cleaned_df = self._remove_duplicate_columns(cleaned_df)
        
        # Final step: Ensure column order matches Alaska's exact structure
        cleaned_df = self.ensure_column_order(cleaned_df)
        
        logger.info(f"South Dakota data cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def ensure_column_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure columns match Alaska's exact order."""
        ALASKA_COLUMN_ORDER = [
            'election_year',
            'election_type',
            'office',
            'district',
            'full_name_display',
            'first_name',
            'middle_name',
            'last_name',
            'prefix',
            'suffix',
            'nickname',
            'party',
            'phone',
            'email',
            'address',
            'website',
            'state',
            'original_name',
            'original_state',
            'original_election_year',
            'original_office',
            'original_filing_date',
            'id',
            'stable_id',
            'county',
            'city',
            'zip_code',
            'filing_date',
            'election_date',
            'facebook',
            'twitter'
        ]
        
        for col in ALASKA_COLUMN_ORDER:
            if col not in df.columns:
                df[col] = pd.NA
        
        return df[ALASKA_COLUMN_ORDER]
    
    def _process_election_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process election year and type from contest column."""
        logger.info("Processing election data...")
        
        # South Dakota data is from 2024
        df['election_year'] = 2024
        
        def determine_election_type(contest_str: str) -> str:
            if pd.isna(contest_str):
                return "General"
            
            contest_str = str(contest_str).strip().lower()
            
            if 'presidential' in contest_str:
                return "General"  # Presidential elections are general elections
            elif 'primary' in contest_str:
                return "Primary"
            elif 'special' in contest_str:
                return "Special"
            else:
            
            
                return "General"  # Default for most state/local offices
        
        df['election_type'] = df['Contest'].apply(determine_election_type)
        
        return df
    
    def _process_office_and_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize office and district information."""
        logger.info("Processing office and district information...")
        
        def process_office_district(contest_str: str, district_str: str) -> Tuple[str, Optional[str]]:
            if pd.isna(contest_str):
                return None, None
            
            contest_str = str(contest_str).strip()
            
            # Handle US President
            if "Presidential Candidate" in contest_str:
                return "US President", None
            
            # Handle US Representative
            if "United States Representative" in contest_str:
                return "US Representative", "At Large"
            
            # Handle State Senate
            if "State Senator" in contest_str:
                # Extract district from District/County column if available
                if pd.notna(district_str):
                    district = str(district_str).strip()
                    if district and district != 'nan':
                        return "State Senate", district
                return "State Senate", None
            
            # Handle State Representative
            if "State Representative" in contest_str:
                # Extract district from District/County column if available
                if pd.notna(district_str):
                    district = str(district_str).strip()
                    if district and district != 'nan':
                        return "State House", district
                return "State House", None
            
            # Handle county offices
            county_offices = {
                'County Treasurer': 'County Treasurer',
                'County Auditor': 'County Auditor',
                'County Finance Officer': 'County Finance Officer',
                'States Attorney': 'County Attorney',
                'Sheriff': 'County Sheriff',
                'Register of Deeds': 'County Register of Deeds',
                'Coroner': 'County Coroner',
                'County Commissioner': 'County Commissioner',
                'County Commissioner At Large': 'County Commissioner At Large'
            }
            
            for contest_key, office_name in county_offices.items():
                if contest_key in contest_str:
                    # Extract county/district from District/County column if available
                    if pd.notna(district_str):
                        district = str(district_str).strip()
                        if district and district != 'nan':
                            return office_name, district
                    return office_name, None
            
            # Handle other offices
            other_offices = {
                'Mayor': 'Mayor',
                'Alderman': 'City Council Member',
                'City Council Member': 'City Council Member',
                'Trustee': 'City Trustee',
                'City Commissioner': 'City Commissioner',
                'School Board Member': 'School Board Member',
                'School Area Board Member': 'School Board Member'
            }
            
            for contest_key, office_name in other_offices.items():
                if contest_key in contest_str:
                    # Extract district from District/County column if available
                    if pd.notna(district_str):
                        district = str(district_str).strip()
                        if district and district != 'nan':
                            return office_name, district
                    return office_name, None
            
            # Handle water development districts
            water_districts = [
                'East Dakota Water Development District Director',
                'James River Water Development District Director',
                'South Central Water Development District Director',
                'Vermillion Basin Water Development District Director',
                'West Dakota Water Development District Director',
                'West River Water Development District Director'
            ]
            
            for water_district in water_districts:
                if water_district in contest_str:
                    return "Water District Director", None
            
            # Handle other special districts
            if "Heartland Consumers Power District Director" in contest_str:
                return "Power District Director", None
            
            if "Conservation District Supervisor" in contest_str:
                return "Conservation District Supervisor", None
            
            if "Precinct Committeeman" in contest_str or "Precinct Committeewoman" in contest_str:
                return "Precinct Committee Member", None
            
            if "Delegates to State Convention" in contest_str:
                return "State Convention Delegate", None
            
            # Default case - return the contest as is
            return contest_str, None
        
        # Apply office and district processing
        office_results = df.apply(lambda row: process_office_district(row['Contest'], row['District/County']), axis=1)
        df['office'] = [result[0] for result in office_results]
        df['district'] = [result[1] for result in office_results]
        df['district'] = df['district'].astype('object')
        
        return df
    
    def _process_full_name_displays(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process candidate names."""
        logger.info("Processing candidate names...")
        
        def clean_name(name_str: str) -> str:
            if pd.isna(name_str):
                return None
            
            try:
                name_str = str(name_str).strip()
                
                # Remove withdrawal information
                if "(Withdrawn" in name_str:
                    name_str = name_str.split("(Withdrawn")[0].strip()
                
                # Remove extra whitespace and quotes
                cleaned = re.sub(r'\s+', ' ', name_str).strip().strip('"\'')
                return cleaned
            except Exception as e:
                logger.warning(f"Error cleaning name '{name_str}': {e}")
                return str(name_str) if name_str else None
        
        # Apply name cleaning
        df['full_name_display'] = df['Name'].apply(clean_name)
        
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
            try:
                name = row['full_name_display']
                original_name = row['Name']
                
                if pd.isna(name) or not name:
                    continue
                
                # Parse the name
                parsed = self._parse_standard_name(name, original_name)
                
                # Assign parsed components
                df.at[idx, 'first_name'] = parsed[0]
                df.at[idx, 'middle_name'] = parsed[1]
                df.at[idx, 'last_name'] = parsed[2]
                df.at[idx, 'prefix'] = parsed[3]
                df.at[idx, 'suffix'] = parsed[4]
                df.at[idx, 'nickname'] = parsed[5]
                df.at[idx, 'full_name_display'] = parsed[6]
                
            except Exception as e:
                logger.warning(f"Error parsing name at row {idx}: {e}")
                # Set default values for this row
                df.at[idx, 'first_name'] = str(row['full_name_display']) if pd.notna(row['full_name_display']) else None
                df.at[idx, 'last_name'] = None
                df.at[idx, 'full_name_display'] = str(row['full_name_display']) if pd.notna(row['full_name_display']) else None
        
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
                
                if len(first_middle) == 0:
                    # Handle case where there's nothing after the comma
                    display_name = self._build_display_name(None, None, last_name, suffix, nickname)
                    return None, None, last_name, prefix, suffix, nickname, display_name
                elif len(first_middle) == 1:
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
        
        if len(parts) == 0:
            return None, None, None, None, suffix, nickname, ""
        elif len(parts) == 1:
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
            'dem': 'Democratic',
            'rep': 'Republican',
            'ind': 'Independent',
            'non': 'Nonpartisan',
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
        """Clean contact information (phone, email, address, website)."""
        logger.info("Cleaning contact information...")
        
        # Clean addresses
        def clean_address(address_str: str) -> str:
            if pd.isna(address_str):
                return None
            
            # Remove extra whitespace and quotes
            cleaned = str(address_str).strip().strip('"\'')
            # Remove multiple spaces
            cleaned = re.sub(r'\s+', ' ', cleaned)
            return cleaned
        
        # Extract city, county, and zip code from address
        def extract_location_info(address_str: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
            if pd.isna(address_str):
                return None, None, None
            
            address = str(address_str).strip()
            
            # Extract zip code (5 digits)
            zip_match = re.search(r'\b(\d{5})\b', address)
            zip_code = zip_match.group(1) if zip_match else None
            
            # Extract city (look for city names before state)
            # Common pattern: "City, State ZIP" or "City State ZIP"
            city_state_zip_pattern = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*,?\s*([A-Z]{2})\s*\d{5}'
            city_state_match = re.search(city_state_zip_pattern, address)
            
            if city_state_match:
                city = city_state_match.group(1).strip()
                # Remove zip code from city if it got captured
                city = re.sub(r'\d+', '', city).strip()
            else:
                city = None
            
            # For county, we'll use the District/County column if available
            county = None
            
            return city, county, zip_code
        
        # Apply address cleaning
        df['address'] = df['Mailing Address'].apply(clean_address)
        
        # Extract location components
        location_results = df['Mailing Address'].apply(extract_location_info)
        df['city'] = [result[0] for result in location_results]
        df['county'] = [result[1] for result in location_results]
        df['zip_code'] = [result[2] for result in location_results]
        
        # Set other contact fields to NULL (not available in South Dakota data)
        df['phone'] = pd.NA
        df['email'] = pd.NA
        df['website'] = pd.NA
        
        return df
    
    def _add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all required columns for the final schema."""
        logger.info("Adding required columns...")
        
        # Add state column
        df['state'] = self.state_name
        
        # Add original data preservation columns
        df['original_name'] = df['Name'].copy()
        df['original_state'] = df['state'].copy()
        df['original_election_year'] = df['election_year'].copy()
        df['original_office'] = df['Contest'].copy()
        df['original_filing_date'] = df['Petition Filing Date'].copy()
        
        # Add missing columns with None values
        required_columns = [
            'id', 'stable_id', 'filing_date', 'election_date', 'facebook', 'twitter'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Set id to empty string (will be generated later in process)
        df['id'] = ""
        
        # Convert filing date to proper format
        def convert_filing_date(date_val):
            if pd.isna(date_val):
                return None
            
            try:
                # Convert Excel date number to datetime
                if isinstance(date_val, (int, float)):
                    return pd.to_datetime('1899-12-30') + pd.Timedelta(days=date_val)
                else:
            
            
                    return pd.to_datetime(date_val)
            except:
                return None
        
        df['filing_date'] = df['Petition Filing Date'].apply(convert_filing_date)
        
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

def clean_south_dakota_candidates(input_file: str, output_file: str = None, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean South Dakota candidate data.
    
    Args:
        input_file: Path to the input Excel file
        output_file: Optional path to save the cleaned data (if None, will use default naming in output_dir)
        output_dir: Directory to save cleaned data (default: "cleaned_data")
        
    Returns:
        Cleaned DataFrame
    """
    # Load the data
    logger.info(f"Loading South Dakota data from {input_file}...")
    
    # Handle both .xls and .xlsx files
    if input_file.endswith('.xls'):
        df = pd.read_excel(input_file, engine='xlrd')
    else:
        df = pd.read_excel(input_file)
    
    # Initialize cleaner with output directory
    cleaner = SouthDakotaCleaner(output_dir=output_dir)
    
    # Clean the data
    cleaned_df = cleaner.clean_south_dakota_data(df)
    
    # Generate output filename if not provided
    if output_file is None:
        # Extract base name from input file
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"{base_name}_cleaned_{timestamp}.xlsx")
    
    # Ensure output file is in the output directory
    if not os.path.dirname(output_file):
        output_file = os.path.join(output_dir, output_file)
    
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
    
    # Find South Dakota file specifically
    south_dakota_file = None
    for file_path in available_files:
        if 'south_dakota' in os.path.basename(file_path).lower():
            south_dakota_file = file_path
            break
    
    if south_dakota_file:
        output_dir = DEFAULT_OUTPUT_DIR
        
        print(f"\nProcessing South Dakota file: {os.path.basename(south_dakota_file)}")
        print(f"Output directory: {output_dir}")
        
        # Clean the data and save to the new output directory
        cleaned_data = clean_south_dakota_candidates(south_dakota_file, output_dir=output_dir)
        print(f"\nCleaned {len(cleaned_data)} records")
        print(f"Columns: {cleaned_data.columns.tolist()}")
        print(f"Data saved to: {output_dir}/")
    else:
        print("No South Dakota file found for processing")
        print("Available files:")
        for file_path in available_files:
            print(f"  - {os.path.basename(file_path)}")
