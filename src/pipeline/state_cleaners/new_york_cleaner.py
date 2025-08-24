#!/usr/bin/env python3
"""
New York State Data Cleaner

This module contains functions to clean and standardize New York political candidate data
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

class NewYorkCleaner:
    """Handles cleaning and standardization of New York political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "New York"
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
    
    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove original columns that have been replaced by cleaned versions."""
        logger.info("Removing duplicate columns...")
        
        # Columns to remove (original versions) - New York 1982-2024 (after renaming)
        columns_to_remove = [
            'Election', 'Office', 'Name', 'Party', 'Email', 'Website', 'Phone Number',
            'Filer Type', 'Compliance Type', 'Committee Type', 'Filer ID', 'District', 'County',
            'Address_Date', 'Filing_Date', 'Termination Date', 'Status', 'Candidate Name',
            'Election Year', 'Candidate Office', 'Candidate District', 'Purpose Type',
            'Candidate_Filing_Date', 'Candidate Termination Date'
        ]
        
        # Only remove if they exist and we have cleaned versions
        columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove)
            logger.info(f"Removed {len(columns_to_remove)} duplicate columns: {columns_to_remove}")
        
        return df

    def clean_new_york_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize New York candidate data according to final schema.
        
        Args:
            df: Raw New York candidate data DataFrame
            
        Returns:
            Cleaned DataFrame conforming to final schema
        """
        logger.info(f"Starting New York data cleaning for {len(df)} records...")
        
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
        
        logger.info(f"New York data cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def _process_election_data(self, df: pd.DataFrame, filename: str = None) -> pd.DataFrame:
        """Process election year and type from election column or filename."""
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
                election_type = "General"
            else:
                election_type = "General"  # Default
            
            return year, election_type
        
        # Check if Election Year column exists, if not try to extract from filename
        election_col = 'Election Year' if 'Election Year' in df.columns else 'Election'
        if election_col not in df.columns:
            if filename:
                # Extract year from filename (e.g., "new_york_candidates_2024.csv" -> 2024)
                year_match = re.search(r'(\d{4})', filename)
                if year_match:
                    election_year = int(year_match.group(1))
                    logger.info(f"Extracted election year {election_year} from filename")
                else:
                    election_year = None
                    logger.warning("No election year found in filename")
            else:
                election_year = None
                logger.warning("No filename provided and no Election Year column found")
            
            df['election_year'] = election_year
            df['election_type'] = "General"  # Default for New York
            return df
        
        # Apply election processing
        election_results = df[election_col].apply(extract_election_info)
        df['election_year'] = [result[0] for result in election_results]
        df['election_type'] = [result[1] for result in election_results]
        
        return df
    
    def _process_office_and_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize office and district information."""
        logger.info("Processing office and district information...")

        # Choose best available columns
        office_source_col = 'Office' if 'Office' in df.columns else None
        candidate_office_col = 'Candidate Office' if 'Candidate Office' in df.columns else None
        district_source_col = 'District' if 'District' in df.columns else None
        candidate_district_col = 'Candidate District' if 'Candidate District' in df.columns else None

        def _looks_numeric(value: object) -> bool:
            try:
                s = str(value).strip()
                if s == "":
                    return False
                # pure digits or float-like
                float(s)
                return True
            except Exception:
                return False

        def choose_office(row) -> Optional[str]:
            primary = row.get(office_source_col) if office_source_col else pd.NA
            secondary = row.get(candidate_office_col) if candidate_office_col else pd.NA
            # Prefer primary textual office
            if pd.notna(primary) and str(primary).strip():
                return str(primary).strip()
            # Fallback to candidate office only if it is non-numeric text
            if pd.notna(secondary) and str(secondary).strip() and not _looks_numeric(secondary):
                return str(secondary).strip()
            return None

        def choose_district(row) -> Optional[str]:
            primary = row.get(district_source_col) if district_source_col else pd.NA
            secondary = row.get(candidate_district_col) if candidate_district_col else pd.NA
            val = primary if pd.notna(primary) and str(primary).strip() else secondary
            if pd.isna(val) or val is None or str(val).strip() == "":
                return None
            # Normalize numeric-like districts (floats -> ints -> str)
            try:
                # Some CSVs read as floats like 31.0
                num = float(val)
                if num.is_integer():
                    return str(int(num))
                return str(val)
            except Exception:
                return str(val).strip()

        # Derive office/district
        df['office'] = df.apply(choose_office, axis=1)
        df['district'] = df.apply(choose_district, axis=1).astype('object')

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
            office = row['office']
            original_name = row['Name']
            
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
        
        # Handle different party column names - New York doesn't have party data
        if 'Office Political Party' in df.columns:
            df['party'] = df['Office Political Party'].apply(standardize_party)
        elif 'Party' in df.columns:
            df['party'] = df['Party'].apply(standardize_party)
        else:
            # New York doesn't have party data, set to None
            df['party'] = pd.NA
            logger.warning("No party column found in New York data, setting to None")
        
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
            cleaned = str(address_str).strip().strip("\"'")
            # Remove multiple spaces
            cleaned = re.sub(r'\s+', ' ', cleaned)
            # Only remove PO Box if it appears after a street address (not if it's the only address)
            # Look for patterns like "Street Address P.O. Box XXXX" but keep "P.O. Box XXXX"
            po_box_patterns = [
                r'^(.+?)\s+P\.?O\.?\s*Box\s+[A-Z0-9]+.*$',
                r'^(.+?)\s+Post\s+Office\s+Box\s+[A-Z0-9]+.*$',
                r'^(.+?)\s+Box\s+[A-Z0-9]+.*$'
            ]
            for pattern in po_box_patterns:
                match = re.search(pattern, cleaned, re.IGNORECASE)
                if match and match.group(1).strip():  # Only if there's content before PO Box
                    cleaned = match.group(1).strip()
                    break
            return cleaned
        
        # Apply cleaning with New York column mapping
        # New York doesn't have phone, email, or website data
        df['phone'] = pd.NA
        df['email'] = pd.NA
        df['website'] = pd.NA
        
        # Parse address to extract components
        # Now using the renamed 'Address' column (was 'Municipality')
        if 'Address' in df.columns:
            df['address'] = df['Address'].apply(clean_address)
            
            # Parse city, zip_code, and address_state from Address field
            def parse_address_components(address_str: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
                """Parse New York address to extract city, zip_code, and address_state.
                Heuristics:
                  - Prefer pattern ending with STATE ZIP
                  - City = tokens immediately before STATE that are likely city words (no digits, not street types)
                """
                if pd.isna(address_str):
                    return None, None, None

                address = str(address_str).strip()

                # Fallback: look for state and zip at end and then derive city by walking tokens backwards
                state_zip_match = re.search(r'\b([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$', address)
                if state_zip_match:
                    state = state_zip_match.group(1)
                    zip_code = state_zip_match.group(2)
                    before = address[:state_zip_match.start()].strip()
                    tokens = before.split()
                    # Normalize street-type tokens
                    street_types = {
                        'st','st.','street','ave','ave.','avenue','blvd','blvd.','road','rd','rd.','dr','dr.','pl','pl.','plz','plz.',
                        'way','hwy','highway','ln','ln.','lane','ct','ct.','circle','cir','cir.','terrace','ter','ter.','parkway','pkwy','pkwy.'
                    }
                    city_parts: List[str] = []
                    # Walk backwards to collect city tokens until a street-type or numeric token
                    for tok in reversed(tokens):
                        low = tok.lower().strip(',')
                        if any(ch.isdigit() for ch in tok) or low in street_types:
                            break
                        # Stop if token looks like unit designator
                        if low in {'apt','apt.','suite','ste','ste.','#','unit'}:
                            break
                        city_parts.insert(0, tok.strip(',').strip())
                    # If we captured too many (e.g., included part of street), trim to last 1-2 tokens
                    if len(city_parts) > 2:
                        city_parts = city_parts[-2:]
                    city = ' '.join(city_parts) if city_parts else None
                    return city, zip_code, state

                return None, None, None
            
            address_results = df['Address'].apply(parse_address_components)
            df['city'] = [result[0] for result in address_results]
            df['zip_code'] = [result[1] for result in address_results]
            df['address_state'] = [result[2] for result in address_results]
        else:
            df['address'] = pd.NA
            df['city'] = pd.NA
            df['zip_code'] = pd.NA
            df['address_state'] = pd.NA
        
        # Set address_state to "NY" for all New York records (fallback)
        df['address_state'] = df['address_state'].fillna("NY")
        
        # Map county from New York data
        if 'County' in df.columns:
            def clean_county(val: object) -> Optional[str]:
                if pd.isna(val):
                    return None
                s = str(val).strip()
                s = re.sub(r',?\s*(County|City)$', '', s, flags=re.IGNORECASE).strip()
                return s if s else None
            df['county'] = df['County'].apply(clean_county)
        else:
            df['county'] = pd.NA
        
        return df
    
    def _add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all required columns for the final schema."""
        logger.info("Adding required columns...")
        
        # Add state column
        df['state'] = self.state_name
        
        # Add original data preservation columns
        # Prefer Name, fallback to Candidate Name for original_name
        if 'Name' in df.columns and 'Candidate Name' in df.columns:
            df['original_name'] = df['Name']
            df['original_name'] = df['original_name'].fillna(df['Candidate Name'])
        elif 'Name' in df.columns:
            df['original_name'] = df['Name'].copy()
        elif 'Candidate Name' in df.columns:
            df['original_name'] = df['Candidate Name'].copy()
        else:
            df['original_name'] = pd.NA
        df['original_state'] = df['state'].copy()
        df['original_election_year'] = df['election_year'].copy()
        df['original_office'] = df['Office'].copy()
        # Map filing date from New York data
        if 'Filing_Date' in df.columns:
            def parse_filing_date(filing_str: str) -> str:
                """Parse filing date from New York format MM/DD/YYYY."""
                if pd.isna(filing_str):
                    return None
                
                filing_str = str(filing_str).strip()
                
                try:
                    # Parse and standardize date format
                    date_obj = pd.to_datetime(filing_str, format='%m/%d/%Y')
                    return date_obj.strftime('%Y-%m-%d')
                except:
                    return filing_str
            
            df['filing_date'] = df['Filing_Date'].apply(parse_filing_date)
            df['original_filing_date'] = df['Filing_Date'].copy()
        else:
            df['filing_date'] = pd.NA
            df['original_filing_date'] = pd.NA
        
        # Add candidate_name column (same as full_name_display, with fallback to original_name)
        df['candidate_name'] = df['full_name_display'].copy()
        df['candidate_name'] = df['candidate_name'].fillna(df['original_name'])
        
        # Add missing columns with None values
        required_columns = [
            'id', 'stable_id', 'election_date', 'facebook', 'twitter', 'prefix', 'suffix', 'nickname'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Set id to empty string (will be generated later in process)
        df['id'] = ""
        
        return df
    
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

def clean_new_york_candidates(input_file: str, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean New York candidate data.
    
    Args:
        input_file: Path to the input file (CSV or Excel)
        output_file: Optional path to save the cleaned data (if None, will use default naming in output_dir)
        output_dir: Directory to save cleaned data (default: "cleaned_data")
        
    Returns:
        Cleaned DataFrame
    """
    # Load the data
    logger.info(f"Loading New York data from {input_file}...")
    
    # Handle both CSV and Excel files
    if input_file.endswith('.csv'):
        df = pd.read_csv(input_file, low_memory=False)
        
        # Fix column names for New York CSV (columns are misaligned)
        # The 'Address' column actually contains dates, 'Municipality' contains addresses
        # Rename columns to be more intuitive
        column_mapping = {
            'Municipality': 'Address',  # Municipality contains the real addresses
            'Address': 'Address_Date',  # Address column contains dates (misleading name)
            'Registration Date': 'Filing_Date',  # More descriptive name
            'Candidate Registration Date': 'Candidate_Filing_Date'  # More descriptive name
        }
        
        # Rename the columns
        df = df.rename(columns=column_mapping)
        
        logger.info("Fixed New York CSV column names for clarity")
    else:
        df = pd.read_excel(input_file)
    
    # Initialize cleaner with output directory
    cleaner = NewYorkCleaner(output_dir=output_dir)
    
    # Clean the data
    cleaned_df = cleaner.clean_new_york_data(df)
    
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
        cleaned_data = clean_new_york_candidates(input_file, output_dir=output_dir)
        print(f"\nCleaned {len(cleaned_data)} records")
        print(f"Columns: {cleaned_data.columns.tolist()}")
        print(f"Data saved to: {output_dir}/")
    else:
        print("No input files available for processing")
