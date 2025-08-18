#!/usr/bin/env python3
"""
New Mexico State Data Cleaner

This module contains functions to clean and standardize New Mexico political candidate data
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
DEFAULT_OUTPUT_DIR = "cleaned_data"  # Default output directory for cleaned data
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

class NewMexicoCleaner:
    """Handles cleaning and standardization of New Mexico political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "New Mexico"
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
            'Contest', 'District', 'Filing County', 'First Name', 'Middle Name', 'Last Name', 
            'Party', 'Mailing Address', 'Address', 'City', 'State', 'Zip', 'Phone', 'Email', 
            'Website', 'Filing Date/Time', 'Ballot Order', 'Status'
        ]
        
        # Only remove if they exist and we have cleaned versions
        columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove)
            logger.info(f"Removed {len(columns_to_remove)} duplicate columns: {columns_to_remove}")
        
        return df

    def clean_new_mexico_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize New Mexico candidate data according to final schema.
        
        Args:
            df: Raw New Mexico candidate data DataFrame
            
        Returns:
            Cleaned DataFrame conforming to final schema
        """
        logger.info(f"Starting New Mexico data cleaning for {len(df)} records...")
        
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
        
        logger.info(f"New Mexico data cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def ensure_column_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure columns match Alaska's exact order."""
        ALASKA_COLUMN_ORDER = [
            'election_year', 'election_type', 'office', 'district', 'full_name_display',
            'first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname',
            'full_name_display', 'party', 'phone', 'email', 'address', 'website',
            'state', 'original_name', 'original_state', 'original_election_year',
            'original_office', 'original_filing_date', 'id', 'stable_id', 'county',
            'city', 'zip_code', 'filing_date', 'election_date', 'facebook', 'twitter'
        ]
        
        for col in ALASKA_COLUMN_ORDER:
            if col not in df.columns:
                df[col] = None
        
        return df[ALASKA_COLUMN_ORDER]
    
    def _process_election_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process election year and type from contest column."""
        logger.info("Processing election data...")
        
        def extract_election_info(contest_str: str) -> Tuple[Optional[int], Optional[str]]:
            if pd.isna(contest_str):
                return None, None
            
            contest_str = str(contest_str).strip()
            
            # Extract year from contest string or use current year
            year_match = re.search(r'20\d{2}', contest_str)
            if year_match:
                year = int(year_match.group())
            else:
                # Default to 2024 for New Mexico data
                year = 2024
            
            # Determine election type based on contest
            contest_lower = contest_str.lower()
            if 'primary' in contest_lower:
                election_type = "Primary"
            elif 'general' in contest_lower:
                election_type = "General"
            elif 'special' in contest_lower:
                election_type = "Special"
            else:
                # Default to General for most contests
                election_type = "General"
            
            return year, election_type
        
        # Apply election processing
        election_results = df['Contest'].apply(extract_election_info)
        df['election_year'] = [result[0] for result in election_results]
        df['election_type'] = [result[1] for result in election_results]
        
        return df
    
    def _process_office_and_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize office and district information."""
        logger.info("Processing office and district information...")
        
        def process_office_district(contest_str: str, district_str: str) -> Tuple[str, Optional[str]]:
            if pd.isna(contest_str):
                return None, None
            
            contest_str = str(contest_str).strip()
            district_str = str(district_str).strip() if pd.notna(district_str) else ""
            
            # Handle US President
            if "president of the united states" in contest_str.lower():
                return "US President", None
            
            # Handle US Representative
            if "united states representative" in contest_str.lower():
                if pd.notna(district_str) and district_str:
                    return "US Representative", str(district_str)
                else:
                    return "US Representative", "At Large"
            
            # Handle State Senate
            if "state senator" in contest_str.lower() or "senate" in contest_str.lower():
                if pd.notna(district_str) and district_str:
                    return "State Senate", str(district_str)
                else:
                    return "State Senate", None
            
            # Handle State House/Representative
            if "state representative" in contest_str.lower() or "house" in contest_str.lower():
                if pd.notna(district_str) and district_str:
                    return "State House", str(district_str)
                else:
                    return "State House", None
            
            # Handle other offices (keep as is)
            return contest_str, district_str if pd.notna(district_str) and district_str else None
        
        # Apply office and district processing
        office_results = df.apply(lambda row: process_office_district(row['Contest'], row['District']), axis=1)
        df['office'] = [result[0] for result in office_results]
        df['district'] = [result[1] for result in office_results]
        df['district'] = df['district'].astype('object')
        
        return df
    
    def _process_full_name_displays(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process candidate names."""
        logger.info("Processing candidate names...")
        
        def build_full_name(row) -> str:
            """Build full name from individual components."""
            parts = []
            
            if pd.notna(row['First Name']) and str(row['First Name']).strip():
                parts.append(str(row['First Name']).strip())
            
            if pd.notna(row['Middle Name']) and str(row['Middle Name']).strip():
                parts.append(str(row['Middle Name']).strip())
            
            if pd.notna(row['Last Name']) and str(row['Last Name']).strip():
                parts.append(str(row['Last Name']).strip())
            
            return ' '.join(parts) if parts else None
        
        # Build candidate name from components
        df['full_name_display'] = df.apply(build_full_name, axis=1)
        
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
        df['full_name_display'] = pd.NA
        
        for idx, row in df.iterrows():
            first = row['First Name']
            middle = row['Middle Name']
            last = row['Last Name']
            
            # Clean and assign components
            df.at[idx, 'first_name'] = str(first).strip() if pd.notna(first) else None
            df.at[idx, 'middle_name'] = str(middle).strip() if pd.notna(middle) else None
            df.at[idx, 'last_name'] = str(last).strip() if pd.notna(last) else None
            
            # Build display name
            display_parts = []
            if pd.notna(first):
                display_parts.append(str(first).strip())
            if pd.notna(middle):
                display_parts.append(str(middle).strip())
            if pd.notna(last):
                display_parts.append(str(last).strip())
            
            df.at[idx, 'full_name_display'] = ' '.join(display_parts) if display_parts else None
        
        return df
    
    def _standardize_parties(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize party names."""
        logger.info("Standardizing party names...")
        
        party_mapping = {
            'dem': 'Democratic',
            'democrat': 'Democratic',
            'democratic': 'Democratic',
            'rep': 'Republican',
            'republican': 'Republican',
            'ind': 'Independent',
            'independent': 'Independent',
            'lib': 'Libertarian',
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
        df['email'] = df['Email'].apply(clean_email)
        df['address'] = df['Address'].apply(clean_address)
        df['website'] = df['Website'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        
        return df
    
    def _add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all required columns for the final schema."""
        logger.info("Adding required columns...")
        
        # Add state column
        df['state'] = self.state_name
        
        # Add original data preservation columns
        df['original_name'] = df['full_name_display'].copy()
        df['original_state'] = df['state'].copy()
        df['original_election_year'] = df['election_year'].copy()
        df['original_office'] = df['Contest'].copy()
        df['original_filing_date'] = df['Filing Date/Time'].copy()
        
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
        
        # Map existing columns to required schema
        df['county'] = df['Filing County'].copy()
        df['city'] = df['City'].copy()
        df['zip_code'] = df['Zip'].copy()
        df['filing_date'] = df['Filing Date/Time'].copy()
        
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

def clean_new_mexico_candidates(input_file: str, output_file: str = None, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean New Mexico candidate data.
    
    Args:
        input_file: Path to the input Excel file
        output_file: Optional path to save the cleaned data (if None, will use default naming in output_dir)
        output_dir: Directory to save cleaned data (default: "cleaned_data")
        
    Returns:
        Cleaned DataFrame
    """
    # Load the data
    logger.info(f"Loading New Mexico data from {input_file}...")
    
    # Handle different file types
    if input_file.endswith('.xls'):
        # Try to read as HTML first (since some .xls files are actually HTML)
        try:
            df = pd.read_html(input_file)[0]
            logger.info("Successfully read .xls file as HTML")
        except:
            # Fallback to Excel
            df = pd.read_excel(input_file)
            logger.info("Successfully read .xls file as Excel")
    else:
        df = pd.read_excel(input_file)
    
    # Initialize cleaner with output directory
    cleaner = NewMexicoCleaner(output_dir=output_dir)
    
    # Clean the data
    cleaned_df = cleaner.clean_new_mexico_data(df)
    
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
    
    # Example usage - specifically target New Mexico file
    new_mexico_file = None
    for file_path in available_files:
        if 'new_mexico' in os.path.basename(file_path).lower():
            new_mexico_file = file_path
            break
    
    if new_mexico_file:
        input_file = new_mexico_file
        output_dir = DEFAULT_OUTPUT_DIR
        
        print(f"\nProcessing: {os.path.basename(input_file)}")
        print(f"Output directory: {output_dir}")
        
        # Clean the data and save to the new output directory
        cleaned_data = clean_new_mexico_candidates(input_file, output_dir=output_dir)
        print(f"\nCleaned {len(cleaned_data)} records")
        print(f"Columns: {cleaned_data.columns.tolist()}")
        print(f"Data saved to: {output_dir}/")
    else:
        print("No New Mexico file found for processing")
