#!/usr/bin/env python3
"""
Louisiana State Data Cleaner

This module contains functions to clean and standardize Louisiana political candidate data
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

class LouisianaCleaner:
    """Handles cleaning and standardization of Louisiana political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "Louisiana"
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
            'Election', 'OfficeTitle', 'OfficeTitleDescription', 'BallotFirstName', 
            'BallotLastName', 'BallotSuffix', 'Party', 'Address', 'Phone', 
            'Email Address', 'City', 'State', 'Zip', 'Filed Date'
        ]
        
        # Only remove if they exist and we have cleaned versions
        columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove)
            logger.info(f"Removed {len(columns_to_remove)} duplicate columns: {columns_to_remove}")
        
        return df

    def clean_louisiana_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize Louisiana candidate data according to final schema.
        
        Args:
            df: Raw Louisiana candidate data DataFrame
            
        Returns:
            Cleaned DataFrame conforming to final schema
        """
        logger.info(f"Starting Louisiana data cleaning for {len(df)} records...")
        
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
        
        logger.info(f"Louisiana data cleaning completed. Final shape: {cleaned_df.shape}")
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
        """Process election year and type from election column."""
        logger.info("Processing election data...")
        
        def extract_election_info(election_str: str) -> Tuple[Optional[int], Optional[str]]:
            if pd.isna(election_str):
                return None, None
            
            election_str = str(election_str).strip()
            
            # Extract year from election string (format: MM/DD/YYYY)
            year_match = re.search(r'(\d{4})', election_str)
            if year_match:
                year = int(year_match.group(1))
            else:

                return None, None
            
            # Determine election type based on Louisiana patterns
            election_str_lower = election_str.lower()
            if 'primary' in election_str_lower:
                election_type = "Primary"
            elif 'general' in election_str_lower:
                election_type = "General"
            elif 'special' in election_str_lower or 'runoff' in election_str_lower:
                election_type = "Special"
            else:

                # Louisiana typically has General elections in odd years
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
        
        def process_office_district(office_str: str, description_str: str) -> Tuple[str, Optional[str]]:
            if pd.isna(office_str):
                return None, None
            
            office_str = str(office_str).strip()
            description_str = str(description_str).strip() if pd.notna(description_str) else ""
            
            # Handle US President/Vice President
            if "president" in office_str.lower():
                return "US President", None
            
            # Handle US Representative
            if "representative" in office_str.lower() and "united states" in description_str.lower():
                # Extract district from description
                district_match = re.search(r'(\d+)(?:st|nd|rd|th)?\s*district', description_str.lower())
                if district_match:
                    district = district_match.group(1)
                else:
                    district = "At Large"
                return "US Representative", district
            
            # Handle State Senate
            if "senate" in office_str.lower():
                # Extract district from description
                district_match = re.search(r'(\d+)(?:st|nd|rd|th)?\s*district', description_str.lower())
                if district_match:
                    district = district_match.group(1)
                else:
                    district = None
                return "State Senate", district
            
            # Handle State House/Representative
            if "representative" in office_str.lower() and "state" not in description_str.lower():
                # Extract district from description
                district_match = re.search(r'(\d+)(?:st|nd|rd|th)?\s*district', description_str.lower())
                if district_match:
                    district = district_match.group(1)
                else:
                    district = None
                return "State House", district
            
            # Handle other offices (keep as is)
            return office_str, None
        
        # Apply office and district processing
        office_results = df.apply(lambda row: process_office_district(row['OfficeTitle'], row['OfficeTitleDescription']), axis=1)
        df['office'] = [result[0] for result in office_results]
        df['district'] = [result[1] for result in office_results]
        df['district'] = df['district'].astype('object')
        
        return df
    
    def _process_full_name_displays(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process candidate names."""
        logger.info("Processing candidate names...")
        
        def clean_name(first_name: str, last_name: str, suffix: str) -> str:
            if pd.isna(first_name) and pd.isna(last_name):
                return None
            
            first = str(first_name).strip() if pd.notna(first_name) else ""
            last = str(last_name).strip() if pd.notna(last_name) else ""
            suf = str(suffix).strip() if pd.notna(suffix) else ""
            
            # Combine name parts
            name_parts = []
            if first:
                name_parts.append(first)
            if last:
                name_parts.append(last)
            if suf:
                name_parts.append(suf)
            
            return ' '.join(name_parts).strip()
        
        # Apply name cleaning
        df['full_name_display'] = df.apply(lambda row: clean_name(row['BallotFirstName'], row['BallotLastName'], row['BallotSuffix']), axis=1)
        
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
            first_name = row['BallotFirstName']
            last_name = row['BallotLastName']
            suffix = row['BallotSuffix']
            
            # Extract first name
            if pd.notna(first_name):
                first_str = str(first_name).strip().strip("'\"")
                df.at[idx, 'first_name'] = first_str
            
            # Extract last name
            if pd.notna(last_name):
                last_str = str(last_name).strip()
                df.at[idx, 'last_name'] = last_str
            
            # Extract suffix
            if pd.notna(suffix):
                suffix_str = str(suffix).strip()
                df.at[idx, 'suffix'] = suffix_str
            
            # Build display name
            display_parts = []
            if pd.notna(first_name):
                display_parts.append(str(first_name).strip().strip("'\""))
            if pd.notna(last_name):
                display_parts.append(str(last_name).strip())
            if pd.notna(suffix):
                display_parts.append(str(suffix).strip())
            
            if display_parts:
                df.at[idx, 'full_name_display'] = ' '.join(display_parts)
        
        return df
    
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
        
        # Apply cleaning
        df['phone'] = df['Phone'].apply(clean_phone)
        df['email'] = df['Email Address'].apply(clean_email)
        df['address'] = df['Address'].apply(clean_address)
        df['website'] = pd.NA  # Not available in Louisiana data
        
        # Derive address_state from explicit State column when present, else from address
        if 'State' in df.columns:
            df['address_state'] = df['State'].apply(lambda x: str(x).strip().upper() if pd.notna(x) else None)
        else:
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
        
        # Add original data preservation columns
        df['original_name'] = df.apply(lambda row: f"{row['BallotFirstName']} {row['BallotLastName']}".strip(), axis=1)
        df['original_state'] = df['state'].copy()
        df['original_election_year'] = df['election_year'].copy()
        df['original_office'] = df['OfficeTitle'].copy()
        df['original_filing_date'] = df['Filed Date'].copy()
        
        # Add missing columns with None values
        required_columns = [
            'id', 'stable_id', 'county', 'city', 'zip_code', 'address_state', 'filing_date', 
            'election_date', 'facebook', 'twitter', 'prefix', 'suffix', 'nickname',
            'website', 'middle_name'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Set id to empty string (will be generated later in process)
        df['id'] = ""
        
        # Map city and zip_code from original data
        df['city'] = df['City'].copy()
        df['zip_code'] = df['Zip'].copy()
        
        # Map filing_date from original data
        df['filing_date'] = df['Filed Date'].copy()
        
        # Set election_date to election year (approximate)
        df['election_date'] = df['election_year'].apply(lambda x: f"{x}-12-31" if pd.notna(x) else None)
        
        return df
    
    return df

def clean_louisiana_candidates(input_file: str, output_file: str = None, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean Louisiana candidate data.
    
    Args:
        input_file: Path to the input Excel file
        output_file: Optional path to save the cleaned data (if None, will use default naming in output_dir)
        output_dir: Directory to save cleaned data (default: "cleaned_data")
        
    Returns:
        Cleaned DataFrame
    """
    # Load the data
    logger.info(f"Loading Louisiana data from {input_file}...")
    df = pd.read_excel(input_file)
    
    # Initialize cleaner with output directory
    cleaner = LouisianaCleaner(output_dir=output_dir)
    
    # Clean the data
    cleaned_df = cleaner.clean_louisiana_data(df)
    
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
    
    # Example usage - specifically target Louisiana file
    louisiana_file = None
    for file_path in available_files:
        if 'louisiana' in os.path.basename(file_path).lower():
            louisiana_file = file_path
            break
    
    if louisiana_file:
        input_file = louisiana_file
        output_dir = DEFAULT_OUTPUT_DIR
        
        print(f"\nProcessing: {os.path.basename(input_file)}")
        print(f"Output directory: {output_dir}")
        
        # Clean the data and save to the new output directory
        cleaned_data = clean_louisiana_candidates(input_file, output_dir=output_dir)
        print(f"\nCleaned {len(cleaned_data)} records")
        print(f"Columns: {cleaned_data.columns.tolist()}")
        print(f"Data saved to: {output_dir}/")
    else:
        print("No Louisiana file found for processing")
