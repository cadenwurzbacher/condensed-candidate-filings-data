#!/usr/bin/env python3
"""
South Carolina State Data Cleaner

This module contains functions to clean and standardize South Carolina political candidate data
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

class SouthCarolinaCleaner:
    """Handles cleaning and standardization of South Carolina political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "South Carolina"
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
            'Election Name', 'Office', 'District', 'Ballot Name (first - middle)', 
            'Ballot Name (last - suffix)', 'Party', 'Contact Address', 'Contact Email', 
            'Contact Phone Number', 'Date Filed'
        ]
        
        # Only remove if they exist and we have cleaned versions
        columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove)
            logger.info(f"Removed {len(columns_to_remove)} duplicate columns: {columns_to_remove}")
        
        return df

    def clean_south_carolina_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize South Carolina candidate data according to final schema.
        
        Args:
            df: Raw South Carolina candidate data DataFrame
            
        Returns:
            Cleaned DataFrame conforming to final schema
        """
        logger.info(f"Starting South Carolina data cleaning for {len(df)} records...")
        
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
        
        logger.info(f"South Carolina data cleaning completed. Final shape: {cleaned_df.shape}")
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
                df[col] = pd.NA
        
        return df[ALASKA_COLUMN_ORDER]
    
    def _process_election_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process election year and type from election column and filing dates."""
        logger.info("Processing election data...")
        
        def extract_election_info(election_str: str, filing_date: str, office_str: str) -> Tuple[Optional[int], Optional[str]]:
            if pd.isna(election_str):
                return None, None
            
            election_str = str(election_str).strip()
            
            # Try to extract year from filing date first
            year = None
            if pd.notna(filing_date):
                filing_date_str = str(filing_date)
                # Look for years in format MM/DD/YYYY or similar, ensuring it starts with "20"
                # Use a more specific pattern to avoid matching "1753" (which contains "20")
                # Look for dates in format like "3/20/2018" or "12/28/2023"
                year_match = re.search(r'\d{1,2}/\d{1,2}/20\d{2}', filing_date_str)
                if year_match:
                    # Extract just the year part from the matched date
                    year_part = year_match.group().split('/')[-1]
                    filing_year = int(year_part)
                    
                    # For Presidential candidates, the election year is typically the year after filing
                    # Check if this is a Presidential candidate by looking at the office
                    if 'president' in str(office_str).lower():
                        # Presidential candidates who file in 2023 are running in 2024
                        if filing_year == 2023:
                            year = 2024
                        else:
                            year = filing_year
                    else:
                        # For non-Presidential candidates, use the filing year as election year
                        year = filing_year
                    
                    # Additional validation: year must be between 2000 and current year + 2
                    current_year = datetime.now().year
                    if not (2000 <= year <= current_year + 2):
                        year = None
                    # Extra check: reject obviously corrupted dates like 1753
                    elif year < 1900:
                        year = None
            
            # If no year from filing date, try election string
            if year is None:
                year_match = re.search(r'20\d{2}', election_str)
                if year_match:
                    extracted_year = int(year_match.group())
                    # Additional validation: year must be between 2000 and current year + 2
                    current_year = datetime.now().year
                    if 2000 <= extracted_year <= current_year + 2:
                        year = extracted_year
            
            # If still no year, use a reasonable fallback based on the data context
            if year is None:
                # For South Carolina, most data is from 2018-2024, so use 2024 as fallback
                # This avoids using the current year (2025) which might be from corrupted dates
                year = 2024
            
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
        
        # Apply election processing with both election name and filing date
        election_results = df.apply(lambda row: extract_election_info(row['Election Name'], row['Date Filed'], row['Office']), axis=1)
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
            if "president" in office_str.lower():
                return "US President", None
            
            # Handle US Representative
            if "representative" in office_str.lower() and "united states" in office_str.lower():
                return "US Representative", district_str if district_str else None
            
            # Handle US Senate
            if "senator" in office_str.lower() and "united states" in office_str.lower():
                return "US Senator", None
            
            # Handle State Senate
            if "senate" in office_str.lower() and "state" in office_str.lower():
                return "State Senate", district_str if district_str else None
            
            # Handle State House
            if "house" in office_str.lower() and "state" in office_str.lower():
                return "State House", district_str if district_str else None
            
            # Handle Governor
            if "governor" in office_str.lower():
                return "Governor", None
            
            # Handle Lieutenant Governor
            if "lieutenant governor" in office_str.lower():
                return "Lieutenant Governor", None
            
            # Handle Secretary of State
            if "secretary of state" in office_str.lower():
                return "Secretary of State", None
            
            # Handle State Treasurer
            if "treasurer" in office_str.lower() and "state" in office_str.lower():
                return "State Treasurer", None
            
            # Handle Attorney General
            if "attorney general" in office_str.lower():
                return "Attorney General", None
            
            # Handle Comptroller General
            if "comptroller general" in office_str.lower():
                return "Comptroller General", None
            
            # Handle Superintendent of Education
            if "superintendent" in office_str.lower() and "education" in office_str.lower():
                return "Superintendent of Education", None
            
            # Handle Commissioner of Agriculture
            if "commissioner" in office_str.lower() and "agriculture" in office_str.lower():
                return "Commissioner of Agriculture", None
            
            # Handle other offices (keep as is)
            return office_str, district_str if district_str else None
        
        # Apply office and district processing
        office_results = df.apply(lambda row: process_office_district(row['Office'], row['District']), axis=1)
        df['office'] = [result[0] for result in office_results]
        df['district'] = [result[1] for result in office_results]
        df['district'] = df['district'].astype('object')
        
        return df
    
    def _process_full_name_displays(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process candidate names."""
        logger.info("Processing candidate names...")
        
        def clean_name(first_middle: str, last_suffix: str) -> str:
            if pd.isna(first_middle) and pd.isna(last_suffix):
                return None
            
            first_middle = str(first_middle).strip() if pd.notna(first_middle) else ""
            last_suffix = str(last_suffix).strip() if pd.notna(last_suffix) else ""
            
            # Combine first-middle and last-suffix
            if first_middle and last_suffix:
                return f"{first_middle} {last_suffix}".strip()
            elif first_middle:
                return first_middle
            elif last_suffix:
                return last_suffix
            else:
                return None
        
        # Apply name cleaning
        df['full_name_display'] = df.apply(lambda row: clean_name(row['Ballot Name (first - middle)'], row['Ballot Name (last - suffix)']), axis=1)
        
        # Parse names into components
        df = self._parse_names(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse candidate names into first, middle, last, prefix, suffix, nickname, and display components."""
        logger.info("Parsing candidate names...")
        
        # Initialize new columns
        df['first_name'] = None
        df['middle_name'] = None
        df['last_name'] = None
        df['prefix'] = None
        df['suffix'] = None
        df['nickname'] = None
        df['full_name_display'] = None
        
        for idx, row in df.iterrows():
            first_middle = row['Ballot Name (first - middle)']
            last_suffix = row['Ballot Name (last - suffix)']
            candidate_suffix = row['Candidate Suffix']
            
            # Parse first and middle names
            if pd.notna(first_middle):
                first_middle_parts = str(first_middle).strip().split()
                if len(first_middle_parts) >= 1:
                    df.at[idx, 'first_name'] = first_middle_parts[0]
                if len(first_middle_parts) >= 2:
                    df.at[idx, 'middle_name'] = ' '.join(first_middle_parts[1:])
            
            # Parse last name
            if pd.notna(last_suffix):
                last_suffix_parts = str(last_suffix).strip().split()
                if len(last_suffix_parts) >= 1:
                    df.at[idx, 'last_name'] = last_suffix_parts[0]
            
            # Parse suffix from candidate suffix column
            if pd.notna(candidate_suffix):
                df.at[idx, 'suffix'] = str(candidate_suffix).strip()
            
            # Build display name
            display_parts = []
            if df.at[idx, 'first_name']:
                display_parts.append(df.at[idx, 'first_name'])
            if df.at[idx, 'middle_name']:
                display_parts.append(df.at[idx, 'middle_name'])
            if df.at[idx, 'last_name']:
                display_parts.append(df.at[idx, 'last_name'])
            if df.at[idx, 'suffix']:
                display_parts.append(df.at[idx, 'suffix'])
            
            df.at[idx, 'full_name_display'] = ' '.join(display_parts) if display_parts else None
        
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
        df['phone'] = df['Contact Phone Number'].apply(clean_phone)
        df['email'] = df['Contact Email'].apply(clean_email)
        df['address'] = df['Contact Address'].apply(clean_address)
        df['website'] = pd.NA  # Not available in South Carolina data
        
        return df
    
    def _add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all required columns for the final schema."""
        logger.info("Adding required columns...")
        
        # Add state column
        df['state'] = self.state_name
        
        # Add original data preservation columns
        df['original_name'] = df.apply(lambda row: f"{row['Ballot Name (first - middle)']} {row['Ballot Name (last - suffix)']}".strip(), axis=1)
        df['original_state'] = df['state'].copy()
        df['original_election_year'] = df['election_year'].copy()
        df['original_office'] = df['Office'].copy()
        df['original_filing_date'] = df['Date Filed'].copy()
        
        # Add missing columns with None values
        required_columns = [
            'id', 'stable_id', 'county', 'city', 'zip_code', 'filing_date', 
            'election_date', 'facebook', 'twitter', 'prefix', 'nickname'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Set id to empty string (will be generated later in process)
        df['id'] = ""
        
        # Set filing_date from Date Filed
        df['filing_date'] = df['Date Filed']
        
        # Extract county from Associated Counties if available
        def extract_county(counties_str: str) -> str:
            if pd.isna(counties_str):
                return None
            
            counties = str(counties_str).strip()
            # Take the first county if multiple are listed
            if ',' in counties:
                return counties.split(',')[0].strip()
            return counties
        
        df['county'] = df['Associated Counties'].apply(extract_county)
        
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

def clean_south_carolina_candidates(input_file: str, output_file: str = None, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean South Carolina candidate data.
    
    Args:
        input_file: Path to the input Excel file
        output_file: Optional path to save the cleaned data (if None, will use default naming in output_dir)
        output_dir: Directory to save cleaned data (default: "cleaned_data")
        
    Returns:
        Cleaned DataFrame
    """
    # Load the data
    logger.info(f"Loading South Carolina data from {input_file}...")
    df = pd.read_excel(input_file)
    
    # Initialize cleaner with output directory
    cleaner = SouthCarolinaCleaner(output_dir=output_dir)
    
    # Clean the data
    cleaned_df = cleaner.clean_south_carolina_data(df)
    
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
    
    # Example usage - specifically look for South Carolina data
    if available_files:
        # Find South Carolina file specifically
        sc_file = None
        for file_path in available_files:
            if 'south_carolina' in os.path.basename(file_path).lower():
                sc_file = file_path
                break
        
        if sc_file:
            input_file = sc_file
            output_dir = DEFAULT_OUTPUT_DIR
            
            print(f"\nProcessing: {os.path.basename(input_file)}")
            print(f"Output directory: {output_dir}")
            
            # Clean the data and save to the new output directory
            cleaned_data = clean_south_carolina_candidates(input_file, output_dir=output_dir)
            print(f"\nCleaned {len(cleaned_data)} records")
            print(f"Columns: {cleaned_data.columns.tolist()}")
            print(f"Data saved to: {output_dir}/")
        else:
            print("No South Carolina data file found")
    else:
        print("No input files available for processing")
