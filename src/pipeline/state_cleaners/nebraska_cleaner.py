#!/usr/bin/env python3
"""
Nebraska State Data Cleaner

This module contains functions to clean and standardize Nebraska political candidate data
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

class NebraskaCleaner:
    """Handles cleaning and standardization of Nebraska political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "Nebraska"
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
            'Office', 'District', 'Party', 'Name', 'Address', 'Phone'
        ]
        
        # Only remove if they exist and we have cleaned versions
        columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove)
            logger.info(f"Removed {len(columns_to_remove)} duplicate columns: {columns_to_remove}")
        
        return df

    def clean_nebraska_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize Nebraska candidate data according to final schema.
        
        Args:
            df: Raw Nebraska candidate data DataFrame
            
        Returns:
            Cleaned DataFrame conforming to final schema
        """
        logger.info(f"Starting Nebraska data cleaning for {len(df)} records...")
        
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
        
        # Step 6: Map geographic data from addresses
        cleaned_df = self._map_geographic_data(cleaned_df)
        
        # Step 7: Add required columns for final schema
        cleaned_df = self._add_required_columns(cleaned_df)
        
        # Step 8: Generate stable IDs (skipped - will be done later in process)
        # cleaned_df = self._generate_stable_ids(cleaned_df)
        
        # Step 9: Remove duplicate columns
        cleaned_df = self._remove_duplicate_columns(cleaned_df)
        
        # Final step: Ensure column order matches Alaska's exact structure
        cleaned_df = self.ensure_column_order(cleaned_df)
        
        logger.info(f"Nebraska data cleaning completed. Final shape: {cleaned_df.shape}")
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
            'address_state',
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
                df[col] = None
        
        return df[ALASKA_COLUMN_ORDER]
    
    def _process_election_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process election year and type from filename context."""
        logger.info("Processing election data...")
        
        # Nebraska data is from 2024, extract from filename context
        df['election_year'] = 2024
        
        # Determine election type based on office
        def determine_election_type(office_str: str) -> str:
            if pd.isna(office_str):
                return "General"
            
            office_str = str(office_str).strip()
            
            # US President is typically General election
            if "President" in office_str:
                return "General"
            
            # Default to General for Nebraska
            return "General"
        
        df['election_type'] = df['Office'].apply(determine_election_type)
        
        return df
    
    def _process_office_and_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize office and district information."""
        logger.info("Processing office and district information...")
        
        def process_office_district(office_str: str, district_str: str) -> Tuple[str, Optional[str]]:
            if pd.isna(office_str):
                return None, None
            
            office_str = str(office_str).strip()
            district_str = str(district_str).strip() if pd.notna(district_str) else ""
            
            # Handle US President
            if "President and Vice President of the United States" in office_str:
                return "US President", None
            
            # Handle US Representative
            if "United States Representative" in office_str:
                return "US Representative", district_str if district_str else None
            
            # Handle US Senate
            if "United States Senator" in office_str:
                return "US Senator", None
            
            # Handle State Senate
            if "State Senator" in office_str or "Legislature District" in office_str:
                return "State Senate", district_str if district_str else None
            
            # Handle State House
            if "State Representative" in office_str or "Legislature District" in office_str:
                return "State House", district_str if district_str else None
            
            # Handle other offices (keep as is)
            return office_str, district_str if district_str else None
        
        # Apply office and district processing
        # Handle case where District column might not exist
        if 'District' in df.columns:
            office_results = df.apply(lambda row: process_office_district(row['Office'], row['District']), axis=1)
        else:
            
            
            
            
            # If no District column,  None for district
            office_results = df.apply(lambda row: process_office_district(row['Office'], None), axis=1)
        
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
            
            # Handle US President cases - extract candidate names
            if "President" in office_str and " and " in name_str:
                # Handle cases like "Donald J. Trump and JD" or "Kamala D. Harris and Tim"
                candidates = name_str.split(" and ")
                if len(candidates) >= 2:
                    # Return the first candidate (presidential candidate)
                    return candidates[0].strip()
            
            # For other cases, clean the name
            cleaned = re.sub(r'\s+', ' ', name_str).strip().strip('"\'')
            return cleaned
        
        # Apply name cleaning with office context
        df['full_name_display'] = df.apply(lambda row: clean_name(row['Name'], row['Office']), axis=1)
        
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
        
        df['party'] = df['Party'].apply(standardize_party)
        
        return df
    
    def _clean_contact_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean contact information (phone, email, address, website)."""
        logger.info("Cleaning contact information...")
        
        # Clean phone numbers
        def clean_phone(phone_str: str) -> str:
            if pd.isna(phone_str):
                return pd.NA
            
            # Remove all non-digit characters
            digits = re.sub(r'[^\d]', '', str(phone_str))
            
            # Validate US phone number
            if len(digits) == 10:
                return digits
            elif len(digits) == 11 and digits.startswith('1'):
                return digits[1:]
            
            return pd.NA
        
        # Clean email addresses (not available in Nebraska data)
        def clean_email(email_str: str) -> str:
            return pd.NA  # Nebraska data doesn't have email
        
        # Clean addresses
        def clean_address(address_str: str) -> str:
            if pd.isna(address_str):
                return pd.NA
            
            # Remove extra whitespace and quotes
            cleaned = str(address_str).strip().strip('"\'')
            # Remove multiple spaces
            cleaned = re.sub(r'\s+', ' ', cleaned)
            
            # If address contains both street address and PO box, keep only the street address
            # Look for patterns like "Street Address P.O. Box XXXX" or "Street Address PO Box XXXX"
            po_box_patterns = [
                r'\s+P\.?O\.?\s*Box\s+[A-Z0-9]+.*$',  # "P.O. Box XXXX" or "PO Box XXXX" (any alphanumeric)
                r'\s+Post\s+Office\s+Box\s+[A-Z0-9]+.*$',  # "Post Office Box XXXX" (any alphanumeric)
                r'\s+Box\s+[A-Z0-9]+.*$'  # "Box XXXX" (any alphanumeric)
            ]
            
            for pattern in po_box_patterns:
                if re.search(pattern, cleaned, re.IGNORECASE):
                    cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE).strip()
                    break
            
            # If address contains multiple street addresses, keep only the first one
            # This handles cases like "8542 S. 160th Ave. 18406 Pasadena Ave."
            # Simple approach: if we have two numbers that look like addresses, keep first
            parts = cleaned.split()
            for i, part in enumerate(parts):
                if part.isdigit() and i > 0:  # Found a number that's not the first word
                    # Check if this looks like the start of a second address
                    if i + 1 < len(parts) and parts[i + 1][0].isupper():
                        # This is likely the start of a second address, keep everything before it
                        cleaned = ' '.join(parts[:i]).strip()
                        break
            
            return cleaned
        
        # Apply cleaning
        df['phone'] = df['Phone'].apply(clean_phone)
        df['email'] = df.apply(lambda x: clean_email(None), axis=1)  # No email column
        df['address'] = df['Address'].apply(clean_address)
        df['website'] = pd.NA  # Nebraska data doesn't have website
        
        # Set address_state to "NE" for all Nebraska records (addresses don't have state info)
        df['address_state'] = "NE"
        
        # Nebraska addresses are funky - set city and zip to null since we can't reliably parse them
        df['city'] = pd.NA
        df['zip_code'] = pd.NA
        
        return df
    
    def _map_geographic_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map geographic data from Nebraska address fields using smart inference."""
        logger.info("Mapping geographic data for Nebraska addresses...")
        
        def parse_nebraska_address(address_str: str) -> tuple:
            """Parse Nebraska address to extract city and zip code using smart inference."""
            if pd.isna(address_str) or not address_str:
                return None, None
            
            address = str(address_str).strip()
            
            # Handle edge case: just a number like "56"
            if address.isdigit():
                return None, None
            
            # Nebraska ZIP codes by city/region
            nebraska_zip_ranges = {
                'omaha': ['68102', '68103', '68104', '68105', '68106', '68107', '68108', '68109', '68110', '68111', '68112', '68113', '68114', '68116', '68117', '68118', '68122', '68123', '68124', '68127', '68128', '68130', '68131', '68132', '68133', '68134', '68135', '68136', '68137', '68138', '68139', '68142', '68144', '68147', '68152', '68154', '68155', '68157', '68164', '68178', '68179', '68180', '68182', '68183', '68197', '68198'],
                'lincoln': ['68502', '68503', '68504', '68505', '68506', '68507', '68508', '68509', '68510', '68512', '68514', '68516', '68517', '68520', '68521', '68522', '68523', '68524', '68526', '68527', '68528', '68529', '68531', '68532', '68542', '68583', '68588'],
                'bellevue': ['68005', '68123', '68147'],
                'grand island': ['68801', '68802', '68803'],
                'kearney': ['68845', '68847', '68848', '68849'],
                'fremont': ['68025', '68026'],
                'norfolk': ['68701', '68702'],
                'columbus': ['68601', '68602'],
                'hastings': ['68901', '68902'],
                'north platte': ['69101', '69103'],
                'scottsbluff': ['69361', '69363'],
                'south sioux city': ['68776'],
                'blair': ['68008'],
                'beatrice': ['68310'],
                'lexington': ['68850'],
                'york': ['68467'],
                'aurora': ['68818'],
                'holdrege': ['68949'],
                'mccook': ['69001'],
                'alliance': ['69301'],
                'sidney': ['69162'],
                'chadron': ['69337'],
                'wayne': ['68787'],
                'creighton': ['68729'],
                'o\'neill': ['68763'],
                'west point': ['68788'],
                'tekamah': ['68061'],
                'washington': ['68068'],
                'arlington': ['68002'],
                'fort calhoun': ['68023'],
                'valley': ['68064'],
                'elkhorn': ['68022'],
                'bennington': ['68007'],
                'gretna': ['68028'],
                'papillion': ['68046'],
                'la vista': ['68128', '68157'],
                'ralston': ['68127'],
                'millard': ['68135'],
                'westside': ['68779'],
                'schuyler': ['68661'],
                'central city': ['68826'],
                'broken bow': ['68822'],
                'gothenburg': ['69138'],
                'cozad': ['69130'],
                'imperial': ['69033'],
                'ogallala': ['69153'],
                'kimball': ['69145'],
                'gering': ['69341'],
                'mitchell': ['69357']
            }
            
            # Look for city indicators in the address
            address_lower = address.lower()
            
            # Check for common Nebraska city patterns
            for city_name, zip_codes in nebraska_zip_ranges.items():
                if city_name in address_lower:
                    # Found a city match - use the first ZIP code as default
                    city = city_name.title()  # Proper case
                    zip_code = zip_codes[0]
                    return city, zip_code
            
            # Check for street patterns that suggest Omaha or Lincoln
            if any(pattern in address_lower for pattern in ['ave.', 'ave', 'dr.', 'dr', 'st.', 'st', 'blvd.', 'blvd', 'plz.', 'plz', 'ct.', 'ct']):
                # This looks like a city street address
                # Check for Omaha indicators
                if any(pattern in address_lower for pattern in ['prairie', 'viburnum', 'ville de sante', '160th', 'pinnacle', 'jack pine', 'caniglia', '12th']):
                    # These street names are characteristic of Omaha
                    return 'Omaha', '68102'  # Default Omaha ZIP
                elif any(pattern in address_lower for pattern in ['south', 'north', 'east', 'west']):
                    # Directional streets often indicate larger cities
                    return 'Omaha', '68102'  # Default Omaha ZIP
            
            # If no specific city found, try to infer from street patterns
            if 'ave.' in address_lower or 'ave' in address_lower:
                return 'Omaha', '68102'  # Most likely Omaha
            elif 'dr.' in address_lower or 'dr' in address_lower:
                return 'Omaha', '68102'  # Most likely Omaha
            elif 'st.' in address_lower or 'st' in address_lower:
                return 'Omaha', '68102'  # Most likely Omaha
            
            return None, None
        
        # Apply enhanced address parsing to extract city and zip_code
        for idx, row in df.iterrows():
            address = row['address']
            if pd.notna(address):
                parsed_city, parsed_zip = parse_nebraska_address(address)
                if parsed_city:
                    df.at[idx, 'city'] = parsed_city
                if parsed_zip:
                    df.at[idx, 'zip_code'] = parsed_zip
        
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
        df['original_office'] = df['Office'].copy()
        df['original_filing_date'] = pd.NA  # Not available in Nebraska data
        
        # Add missing columns with None values
        required_columns = [
            'id', 'stable_id', 'county', 'filing_date', 
            'election_date', 'facebook', 'twitter', 'prefix', 'suffix', 'nickname'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Set id to empty string (will be generated later in process)
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

def clean_nebraska_candidates(input_file: str, output_file: str = None, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean Nebraska candidate data.
    
    Args:
        input_file: Path to the input Excel file
        output_file: Optional path to save the cleaned data (if None, will use default naming in output_dir)
        output_dir: Directory to save cleaned data (default: "cleaned_data")
        
    Returns:
        Cleaned DataFrame
    """
    # Load the data
    logger.info(f"Loading Nebraska data from {input_file}...")
    df = pd.read_excel(input_file)
    
    # Initialize cleaner with output directory
    cleaner = NebraskaCleaner(output_dir=output_dir)
    
    # Clean the data
    cleaned_df = cleaner.clean_nebraska_data(df)
    
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
    
    # Example usage - specifically target Nebraska data
    nebraska_files = [f for f in available_files if 'nebraska' in os.path.basename(f).lower()]
    
    if nebraska_files:
        input_file = nebraska_files[0]  # Use the Nebraska file
        output_dir = DEFAULT_OUTPUT_DIR
        
        print(f"\nProcessing: {os.path.basename(input_file)}")
        print(f"Output directory: {output_dir}")
        
        # Clean the data and save to the new output directory
        cleaned_data = clean_nebraska_candidates(input_file, output_dir=output_dir)
        print(f"\nCleaned {len(cleaned_data)} records")
        print(f"Columns: {cleaned_data.columns.tolist()}")
        print(f"Data saved to: {output_dir}/")
    else:
        print("No Nebraska data files found for processing")
