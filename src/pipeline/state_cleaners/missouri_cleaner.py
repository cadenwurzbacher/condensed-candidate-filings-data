#!/usr/bin/env python3
"""
Missouri State Data Cleaner

This module contains functions to clean and standardize Missouri political candidate data
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

class MissouriCleaner:
    """Handles cleaning and standardization of Missouri political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "Missouri"
        self.output_dir = output_dir
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
    
    def _fix_missouri_addresses(self, df: pd.DataFrame, address_column: str = 'address') -> pd.DataFrame:
        """
        Fix Missouri-specific address formatting issues.
        
        Common issues in Missouri:
        - Different separator patterns than Kentucky
        - Rural route addresses (RR, Rural Route)
        - PO Box variations
        - County-specific formatting
        """
        
        if address_column not in df.columns:
            logger.warning(f"Address column '{address_column}' not found in DataFrame")
            return df
        
        logger.info(f"Fixing Missouri addresses in column '{address_column}'")
        
        # Create a copy to avoid modifying original
        df_fixed = df.copy()
        
        # Track changes
        total_fixed = 0
        
        for idx, row in df_fixed.iterrows():
            address = row[address_column]
            
            if pd.isna(address) or not isinstance(address, str):
                continue
            
            original_address = address
            fixed_address = address
            
            # Fix 1: Standardize separators (Missouri uses different patterns)
            # Replace pipes and semicolons with commas
            fixed_address = re.sub(r'[;|]', ',', fixed_address)
            
            # Fix 2: Handle Missouri-specific patterns
            
            # Rural Route addresses: "RR 1 Box 123" -> "RR 1, Box 123"
            fixed_address = re.sub(r'(RR\s+\d+)\s+(Box\s+\d+)', r'\1, \2', fixed_address)
            
            # PO Box variations: "PO Box 123" -> "PO Box 123"
            fixed_address = re.sub(r'(P\.?O\.?\s+Box\s+\d+)', r'\1', fixed_address)
            
            # County addresses: "City, County, MO ZIP" -> "City, County, MO, ZIP"
            fixed_address = re.sub(r'([A-Za-z\s]+),\s*([A-Za-z\s]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', 
                                  r'\1, \2, \3, \4', fixed_address)
            
            # Fix 3: Standardize Missouri ZIP code patterns
            # Missouri ZIP codes: 63xxx, 64xxx, 65xxx
            fixed_address = re.sub(r'([A-Z]{2})\s+(6[3-5]\d{3}(?:-\d{4})?)', r'\1, \2', fixed_address)
            
            # Fix 4: Handle apartment/unit numbers
            # "Apt 123" -> ", Apt 123"
            fixed_address = re.sub(r'\s+(Apt|Unit|Suite|#)\s+(\d+)', r', \1 \2', fixed_address)
            
            # Fix 5: Clean up whitespace and normalize
            fixed_address = re.sub(r'\s+', ' ', fixed_address).strip()
            
            # Fix 6: Remove double commas and trailing commas
            fixed_address = re.sub(r',\s*,', ',', fixed_address)
            fixed_address = re.sub(r',\s*$', '', fixed_address)
            
            # Update if changed
            if fixed_address != original_address:
                df_fixed.at[idx, address_column] = fixed_address
                total_fixed += 1
        
        logger.info(f"Fixed {total_fixed} Missouri addresses")
        return df_fixed
    
    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate columns that may have been created during processing."""
        logger.info("Removing duplicate columns...")
        
        # Get unique columns while preserving order
        seen = set()
        unique_columns = []
        for col in df.columns:
            if col not in seen:
                seen.add(col)
                unique_columns.append(col)
        
        return df[unique_columns]
    
    def clean_missouri_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize Missouri candidate data according to final schema.
        
        Args:
            df: Raw Missouri candidate data DataFrame
            
        Returns:
            Cleaned DataFrame conforming to final schema
        """
        logger.info(f"Starting Missouri data cleaning for {len(df)} records...")
        
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
        
        # Step 5: Fix Missouri-specific address issues
        cleaned_df = self._fix_missouri_addresses(cleaned_df)
        
        # Step 6: Clean contact information
        cleaned_df = self._clean_contact_info(cleaned_df)
        
        # Step 7: Map geographic data from addresses
        cleaned_df = self._map_geographic_data(cleaned_df)
        
        # Step 8: Add required columns for final schema
        cleaned_df = self._add_required_columns(cleaned_df)
        
        # Step 9: Generate stable IDs (skipped - will be done later in process)
        # cleaned_df = self._generate_stable_ids(cleaned_df)
        
        # Step 10: Remove duplicate columns
        cleaned_df = self._remove_duplicate_columns(cleaned_df)
        
        # Final step: Ensure column order matches Alaska's exact structure
        cleaned_df = self.ensure_column_order(cleaned_df)
        
        logger.info(f"Missouri data cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def _process_election_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process election year and type from election column."""
        logger.info("Processing election data...")
        
        def extract_election_info(election_str: str) -> Tuple[Optional[int], Optional[str]]:
            """Extract election year and type from election string."""
            if pd.isna(election_str) or not election_str:
                return None, None
            
            election_str = str(election_str).strip()
            
            # Extract year (4 digits)
            year_match = re.search(r'\b(20\d{2})\b', election_str)
            year = int(year_match.group(1)) if year_match else 2024
            
            # Determine election type based on context
            election_type = "General"  # Default to General for Missouri
            
            # Check for primary indicators
            if any(term in election_str.lower() for term in ['primary', 'pri']):
                election_type = "Primary"
            elif any(term in election_str.lower() for term in ['special', 'spec']):
                election_type = "Special"
            
            return year, election_type
        
        # Extract election year and type
        df['election_year'] = 2024  # Missouri data is from 2024
        df['election_type'] = "General"  # Default to General
        
        return df
    
    def _process_office_and_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize office and district information."""
        logger.info("Processing office and district information...")
        
        def process_office_district(office_str: str) -> Tuple[str, Optional[str]]:
            """Process office string to extract office and district."""
            if pd.isna(office_str) or not office_str:
                return "Unknown", None
            
            office_str = str(office_str).strip()
            
            # Standardize office names
            office_mapping = {
                'attorney general': 'State Attorney General',
                'governor': 'State Governor',
                'lieutenant governor': 'State Lieutenant Governor',
                'secretary of state': 'State Secretary of State',
                'state auditor': 'State Auditor',
                'state treasurer': 'State Treasurer',
                'us representative': 'US Representative',
                'us senator': 'US Senator',
                'state senator': 'State Senate',
                'state representative': 'State House',
                'state house': 'State House',
                'state senate': 'State Senate'
            }
            
            # Find matching office
            office_lower = office_str.lower()
            for key, value in office_mapping.items():
                if key in office_lower:
                    office = value
                    break
            else:
                office = office_str.title()
            
            # Extract district if present
            district = None
            district_match = re.search(r'district\s+(\d+)', office_lower)
            if district_match:
                district = district_match.group(1)
            
            return office, district
        
        # Process office and district
        office_district_results = df['Office'].apply(process_office_district)
        df['office'] = [result[0] for result in office_district_results]
        df['district'] = [result[1] for result in office_district_results]
        df['district'] = df['district'].astype('object')
        
        return df
    
    def _process_full_name_displays(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean candidate names and extract components."""
        logger.info("Processing candidate names...")
        
        def clean_name(name_str: str, office_str: str) -> str:
            """Clean candidate name string."""
            if pd.isna(name_str) or not name_str:
                return ""
            
            name_str = str(name_str).strip()
            
            # Remove extra whitespace
            name_str = re.sub(r'\s+', ' ', name_str)
            
            # Remove common prefixes that aren't part of the name
            prefixes_to_remove = ['candidate', 'candidate for', 'running for']
            for prefix in prefixes_to_remove:
                if name_str.lower().startswith(prefix):
                    name_str = name_str[len(prefix):].strip()
            
            return name_str
        
        # Clean names
        df['full_name_display'] = df.apply(lambda row: clean_name(row['Name'], row['Office']), axis=1)
        
        # Parse names into components
        df = self._parse_names(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse candidate names into components."""
        logger.info("Parsing candidate names into components...")
        
        def parse_standard_name(name: str, original_name: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
            """Parse a standard name into components."""
            if not name or pd.isna(name):
                return None, None, None, None, None, None, None
            
            # Clean the name
            name = str(name).strip()
            original_name = str(original_name).strip()
            
            # Split into parts
            parts = re.split(r'\s+', name)
            if not parts:
                return None, None, None, None, None, None, None
            
            # Initialize components
            prefix = None
            first_name = None
            middle_name = None
            last_name = None
            suffix = None
            nickname = None
            full_name_display = None
            
            # Simple and robust parsing
            if len(parts) == 1:
                first_name = parts[0]
            elif len(parts) == 2:
                first_name = parts[0]
                last_name = parts[1]
            elif len(parts) == 3:
                first_name = parts[0]
                # Check if middle part is an initial
                if len(parts[1]) == 1 or (len(parts[1]) == 2 and parts[1].endswith('.')):
                    middle_name = parts[1]
                    last_name = parts[2]
                else:

                    # Middle part is not an initial, treat as middle name
                    middle_name = parts[1]
                    last_name = parts[2]
            else:

                # More than 3 parts
                first_name = parts[0]
                # Check if second part is an initial
                if len(parts[1]) == 1 or (len(parts[1]) == 2 and parts[1].endswith('.')):
                    middle_name = parts[1]
                    last_name = ' '.join(parts[2:])
                else:

                    # Second part is not an initial, treat as middle name
                    middle_name = parts[1]
                    last_name = ' '.join(parts[2:])
            
            # Build display name
            full_name_display = self._build_display_name(first_name, middle_name, last_name, suffix, nickname)
            
            return prefix, first_name, middle_name, last_name, suffix, nickname, full_name_display
        
        # Parse names
        name_components = df.apply(lambda row: parse_standard_name(row['full_name_display'], row['Name']), axis=1)
        
        df['prefix'] = [comp[0] for comp in name_components]
        df['first_name'] = [comp[1] for comp in name_components]
        df['middle_name'] = [comp[2] for comp in name_components]
        df['last_name'] = [comp[3] for comp in name_components]
        df['suffix'] = [comp[4] for comp in name_components]
        df['nickname'] = [comp[5] for comp in name_components]
        df['full_name_display'] = [comp[6] for comp in name_components]
        
        return df
    
    def _parse_standard_name(self, name: str, original_name: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Parse a standard name into components."""
        if not name or pd.isna(name):
            return None, None, None, None, None, None, None
        
        # Clean the name
        name = str(name).strip()
        original_name = str(original_name).strip()
        
        # Split into parts
        parts = re.split(r'\s+', name)
        if not parts:
            return None, None, None, None, None, None, None
        
        # Initialize components
        prefix = None
        first_name = None
        middle_name = None
        last_name = None
        suffix = None
        nickname = None
        full_name_display = None
        
        # Process parts
        processed_parts = []
        i = 0
        
        while i < len(parts):
            part = parts[i].strip()
            if not part:
                i += 1
                continue
            
            # Check for prefix
            if self._is_initial_or_suffix(part) and i == 0:
                prefix = part
                i += 1
                continue
            
            # Check for suffix (only at the very end)
            if self._is_initial_or_suffix(part) and i == len(parts) - 1:
                suffix = part
                i += 1
                continue
            
            # Check for nickname (quoted)
            if (part.startswith('"') and part.endswith('"')) or (part.startswith("'") and part.endswith("'")):
                nickname = part[1:-1]
                i += 1
                continue
            
            # First name
            if first_name is None:
                first_name = part
            # Middle name or last name
            elif middle_name is None:
                # Check if this is a single letter (likely an initial)
                if len(part) == 1 or (len(part) == 2 and part.endswith('.')):
                    middle_name = part
                elif self._should_treat_as_middle_name(part):
                    middle_name = part
                else:
                    last_name = part
            # Last name
            elif last_name is None:
                last_name = part
            # Additional parts go to last name
            else:
                last_name = last_name + " " + part
            
            i += 1
        
        # Fix common parsing issues
        # If we have a middle initial but no last name, move the last part to last name
        if middle_name and not last_name and len(parts) > 2:
            # Find the last non-initial part
            for j in range(len(parts) - 1, -1, -1):
                part = parts[j].strip()
                if part and not self._is_initial_or_suffix(part) and not (part.startswith('"') and part.endswith('"')) and not (part.startswith("'") and part.endswith("'")):
                    if last_name is None:
                        last_name = part
                    else:
                        last_name = part + " " + last_name
                    # Remove this part from middle name if it was there
                    if middle_name == part:
                        middle_name = None
        
        # Additional fix: If middle_name looks like a last name and last_name is an initial, swap them
        if middle_name and last_name and self._is_initial_or_suffix(last_name) and not self._is_initial_or_suffix(middle_name):
            # Swap middle_name and last_name
            temp = middle_name
            middle_name = last_name
            last_name = temp
        
        # Fix for cases like "Ryan L. Munro" where "Munro" is in middle_name and "L." is in last_name
        if middle_name and last_name and self._is_initial_or_suffix(last_name) and not self._is_initial_or_suffix(middle_name):
            # Swap middle_name and last_name
            temp = middle_name
            middle_name = last_name
            last_name = temp
        
        # Additional fix: Look for the pattern where we have a middle initial followed by a last name
        if middle_name and self._is_initial_or_suffix(middle_name) and not last_name:
            # Check if there are more parts after the middle initial
            remaining_parts = [p for p in parts if p not in [first_name, middle_name, suffix, nickname] and p]
            if remaining_parts:
                last_name = remaining_parts[-1]  # Take the last remaining part as the last name
                # Remove it from middle_name if it was incorrectly placed there
                if middle_name == last_name:
                    middle_name = None
        
        # Final fix: Handle the specific case of "Ryan L. Munro" pattern
        # If we have a middle name that's not an initial and a last name that is an initial, swap them
        if middle_name and last_name and not self._is_initial_or_suffix(middle_name) and self._is_initial_or_suffix(last_name):
            # This is the "Ryan L. Munro" case - swap them
            temp = middle_name
            middle_name = last_name
            last_name = temp
        
        # Build display name
        full_name_display = self._build_display_name(first_name, middle_name, last_name, suffix, nickname)
        
        return prefix, first_name, middle_name, last_name, suffix, nickname, full_name_display
    
    def _standardize_parties(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize party names."""
        logger.info("Standardizing party names...")
        
        def standardize_party(party_str: str) -> str:
            """Standardize party name to final schema format."""
            if pd.isna(party_str) or not party_str:
                return "Unknown"
            
            party_str = str(party_str).strip().lower()
            
            # Standardize party names
            party_mapping = {
                'democratic': 'Democratic',
                'democrat': 'Democratic',
                'dem': 'Democratic',
                'republican': 'Republican',
                'rep': 'Republican',
                'independent': 'Independent',
                'ind': 'Independent',
                'libertarian': 'Libertarian',
                'lib': 'Libertarian',
                'green': 'Green',
                'constitution': 'Constitution',
                'con': 'Constitution',
                'reform': 'Reform',
                'natural law': 'Natural Law',
                'american independent': 'American Independent',
                'peace and freedom': 'Peace and Freedom'
            }
            
            for key, value in party_mapping.items():
                if key in party_str:
                    return value
            
            return party_str.title()
        
        df['party'] = df['Party'].apply(standardize_party)
        return df
    
    def _clean_contact_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean contact information."""
        logger.info("Cleaning contact information...")
        
        def clean_phone(phone_str: str) -> str:
            """Clean phone number to 10 digits."""
            if pd.isna(phone_str) or not phone_str:
                return ""
            
            phone_str = str(phone_str)
            # Extract digits only
            digits = re.sub(r'\D', '', phone_str)
            
            # Return last 10 digits if available
            if len(digits) >= 10:
                return digits[-10:]
            elif len(digits) > 0:
                return digits
            else:

                return ""
        
        def clean_email(email_str: str) -> str:
            """Clean and validate email address."""
            if pd.isna(email_str) or not email_str:
                return ""
            
            email_str = str(email_str).strip().lower()
            
            # Basic email validation
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if re.match(email_pattern, email_str):
                return email_str
            
            return ""
        
        def clean_address(address_str: str) -> str:
            """Clean address string."""
            if pd.isna(address_str) or not address_str:
                return ""
            
            address_str = str(address_str).strip()
            
            # Remove extra whitespace and newlines
            address_str = re.sub(r'\s+', ' ', address_str)
            address_str = address_str.replace('\n', ' ')
            
            return address_str
        
        # Clean contact information
        df['phone'] = pd.NA  # Not available in Missouri data
        df['email'] = pd.NA  # Not available in Missouri data
        df['address'] = df['Mailing Address'].apply(clean_address)
        df['website'] = pd.NA  # Not available in Missouri data
        
        # Derive address_state from address when possible
        def extract_state(addr: Optional[str]) -> Optional[str]:
            if addr is None or pd.isna(addr):
                return None
            s = str(addr)
            m = re.search(r"\b([A-Z]{2})\s+\d{5}(?:-\d{4})?\b", s)
            return m.group(1) if m else None
        df['address_state'] = df['address'].apply(extract_state)
        
        return df
    
    def _map_geographic_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map geographic data from Missouri address fields."""
        logger.info("Mapping geographic data...")
        
        # Enhanced address parsing: extract city and zip from address field
        def parse_missouri_address(address_str: str) -> tuple:
            """Parse Missouri address to extract city and zip code."""
            if pd.isna(address_str) or not address_str:
                return None, None
            
            address = str(address_str).strip()
            
            # Missouri format: "Street Address CITY MO ZIP" or "PO BOX XXXX CITY MO ZIP"
            # Look for ZIP code at the end
            zip_match = re.search(r'(\d{5}(?:-\d{4})?)\s*$', address)
            if zip_match:
                zip_code = zip_match.group(1)
                # Remove ZIP from address to find city
                address_without_zip = address[:zip_match.start()].strip()
                
                # Look for state abbreviation before ZIP
                state_match = re.search(r'\s+([A-Z]{2})\s*$', address_without_zip)
                if state_match:
                    state = state_match.group(1)
                    if state == 'MO':
                        # Remove state to find city
                        address_without_state = address_without_zip[:state_match.start()].strip()
                        
                        # Extract city from remaining address
                        if address_without_state.upper().startswith(('PO BOX', 'P. O. BOX')):
                            # For PO Box addresses, city is after the PO Box number
                            parts = address_without_state.split()
                            if len(parts) >= 3:
                                # Find where the city starts by looking for the first non-numeric word after PO BOX
                                city_start_idx = 3  # Start after "PO BOX"
                                for i in range(3, len(parts)):
                                    if not parts[i].replace('.', '').isdigit():  # Skip numeric parts (including "491.")
                                        city_start_idx = i
                                        break
                                
                                city_parts = parts[city_start_idx:]
                                if city_parts:
                                    city = ' '.join(city_parts)
                                    return city, zip_code
                        else:
                            # For street addresses, use a simpler approach
                            # Look for common city patterns in the address
                            address_lower = address_without_state.lower()
                            
                            # Known Missouri cities to look for
                            missouri_cities = [
                                'st. louis', 'st louis', 'kansas city', 'springfield', 'columbia', 'independence',
                                'lees summit', 'o\'fallon', 'st. joseph', 'st joseph', 'st. charles', 'st charles',
                                'st. peters', 'st peters', 'jefferson city', 'columbia', 'lee\'s summit',
                                'richmond heights', 'university city', 'webster groves', 'kirkwood', 'maplewood',
                                'creve coeur', 'chesterfield', 'ballwin', 'manchester', 'ellisville', 'wildwood',
                                'eureka', 'fenton', 'arnold', 'imperial', 'festus', 'crystal city', 'herculaneum',
                                'hannibal', 'quincy', 'mexico', 'fulton', 'jefferson city', 'columbia', 'rolla',
                                'cape girardeau', 'poplar bluff', 'sikeston', 'kennett', 'malden', 'caruthersville',
                                'cottleville', 'wentzville', 'lake saint louis', 'dardenne prairie', 'o\'fallon',
                                'st. charles', 'st charles', 'st. peters', 'st peters', 'florissant', 'bridgeton',
                                'hazelwood', 'maryland heights', 'overland', 'st. ann', 'st ann', 'creve coeur',
                                'chesterfield', 'ballwin', 'manchester', 'ellisville', 'wildwood', 'eureka', 'fenton'
                            ]
                            
                            # Look for exact city matches
                            for city_name in missouri_cities:
                                if city_name in address_lower:
                                    # Find the original case version
                                    city_start = address_lower.find(city_name)
                                    city_end = city_start + len(city_name)
                                    city = address_without_state[city_start:city_end]
                                    return city, zip_code
                            
                            # Fallback: use last word as city
                            parts = address_without_state.split()
                            if parts:
                                city = parts[-1]
                                return city, zip_code
            
            return None, None
        
        # Apply enhanced address parsing to extract city and zip_code
        for idx, row in df.iterrows():
            address = row['address']
            if pd.notna(address):
                parsed_city, parsed_zip = parse_missouri_address(address)
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
        df['original_filing_date'] = df['Date Filed'].copy()
        
        # Add missing columns with None values
        required_columns = [
            'id', 'stable_id', 'county', 'city', 'zip_code', 'address_state', 'filing_date', 
            'election_date', 'facebook', 'twitter'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Set id to empty string (will be generated later in process)
        df['id'] = ""
        
        # Process filing date
        df['filing_date'] = df['Date Filed'].copy()
        
        # Set election date to None (not available in Missouri data)
        df['election_date'] = pd.NA
        
        return df
    
    return df
    
    def _is_initial_or_suffix(self, part: str) -> bool:
        """Check if a part is an initial or suffix."""
        if not part:
            return False
        
        # Common suffixes
        suffixes = ['jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv', 'v', 'vi', 'vii', 'viii', 'ix', 'x']
        
        # Check if it's a suffix
        if part.lower() in suffixes:
            return True
        
        # Check if it's a single letter (initial)
        if len(part) == 1 and part.isalpha():
            return True
        
        # Check if it's a single letter with period (initial)
        if len(part) == 2 and part.endswith('.') and part[0].isalpha():
            return True
        
        return False
    
    def _is_initial(self, part: str) -> bool:
        """Check if a part is an initial."""
        if not part:
            return False
        
        # Single letter
        if len(part) == 1 and part.isalpha():
            return True
        
        # Single letter with period
        if len(part) == 2 and part.endswith('.') and part[0].isalpha():
            return True
        
        return False
    
    def _clean_nickname(self, part: str) -> str:
        """Clean nickname by removing quotes."""
        if not part:
            return ""
        
        part = str(part).strip()
        
        # Remove quotes
        quotes = ['"', '"', '"', "'", "'", "'"]
        for quote in quotes:
            if part.startswith(quote) and part.endswith(quote):
                part = part[1:-1]
                break
        
        return part
    
    def _should_treat_as_middle_name(self, part: str) -> bool:
        """Determine if a part should be treated as a middle name."""
        if not part:
            return False
        
        # Don't treat initials as middle names
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
                df[col] = pd.NA
        
        return df[ALASKA_COLUMN_ORDER]

def clean_missouri_candidates(input_file: str, output_file: str = None, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean Missouri candidate data.
    
    Args:
        input_file: Path to the input Excel file
        output_file: Optional path to save the cleaned data (if None, will use default naming in output_dir)
        output_dir: Directory to save cleaned data (default: "cleaned_data")
        
    Returns:
        Cleaned DataFrame
    """
    # Load the data
    logger.info(f"Loading Missouri data from {input_file}...")
    df = pd.read_excel(input_file)
    
    # Initialize cleaner with output directory
    cleaner = MissouriCleaner(output_dir=output_dir)
    
    # Clean the data
    cleaned_df = cleaner.clean_missouri_data(df)
    
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
    
    # Example usage - use the Missouri file specifically
    missouri_files = [f for f in available_files if 'missouri' in f.lower()]
    if missouri_files:
        input_file = missouri_files[0]  # Use the Missouri file
        output_dir = DEFAULT_OUTPUT_DIR
        
        print(f"\nProcessing: {os.path.basename(input_file)}")
        print(f"Output directory: {output_dir}")
        
        # Clean the data and save to the new output directory
        cleaned_data = clean_missouri_candidates(input_file, output_dir=output_dir)
        print(f"\nCleaned {len(cleaned_data)} records")
        print(f"Columns: {cleaned_data.columns.tolist()}")
        print(f"Data saved to: {output_dir}/")
    else:
        print("No Missouri files found for processing")
        # Fallback to first available file for testing
        if available_files:
            input_file = available_files[0]
            output_dir = DEFAULT_OUTPUT_DIR
            
            print(f"\nTesting with: {os.path.basename(input_file)}")
            print(f"Output directory: {output_dir}")
            
            # Clean the data and save to the new output directory
            cleaned_data = clean_missouri_candidates(input_file, output_dir=output_dir)
            print(f"\nCleaned {len(cleaned_data)} records")
            print(f"Columns: {cleaned_data.columns.tolist()}")
            print(f"Data saved to: {output_dir}/")
