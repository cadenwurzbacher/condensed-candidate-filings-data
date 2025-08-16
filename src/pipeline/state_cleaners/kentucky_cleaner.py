#!/usr/bin/env python3
"""
Kentucky State Data Cleaner

This module contains functions to clean and standardize Kentucky political candidate data
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

class KentuckyCleaner:
    """Handles cleaning and standardization of Kentucky political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "Kentucky"
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
        
    def _fix_kentucky_addresses(self, df: pd.DataFrame, address_column: str = 'address') -> pd.DataFrame:
        """
        Fix Kentucky-specific address formatting issues.
        
        Common issues in Kentucky:
        - Mixed separators (commas, semicolons, pipes)
        - Inconsistent field ordering
        - Extra whitespace
        - Missing city/state/zip separations
        """
        
        if address_column not in df.columns:
            logger.warning(f"Address column '{address_column}' not found in DataFrame")
            return df
        
        logger.info(f"Fixing Kentucky addresses in column '{address_column}'")
        
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
            
            # Fix 1: Standardize separators (replace semicolons and pipes with commas)
            fixed_address = re.sub(r'[;|]', ',', fixed_address)
            
            # Fix 2: Remove extra whitespace and normalize
            fixed_address = re.sub(r'\s+', ' ', fixed_address).strip()
            
            # Fix 3: Fix common Kentucky address patterns
            # Pattern: "City, KY ZIP" -> "City, KY, ZIP"
            fixed_address = re.sub(r'([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', r'\1, \2', fixed_address)
            
            # Pattern: "Street, City KY ZIP" -> "Street, City, KY, ZIP"
            fixed_address = re.sub(r'([A-Za-z\s]+),\s*([A-Za-z\s]+)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', 
                                  r'\1, \2, \3, \4', fixed_address)
            
            # Fix 4: Ensure proper comma separation for city, state, zip
            # Look for patterns like "City KY 12345" and add commas
            fixed_address = re.sub(r'([A-Za-z\s]+)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', 
                                  r'\1, \2, \3', fixed_address)
            
            # Fix 5: Remove double commas
            fixed_address = re.sub(r',\s*,', ',', fixed_address)
            fixed_address = re.sub(r',\s*$', '', fixed_address)  # Remove trailing comma
            
            # Update if changed
            if fixed_address != original_address:
                df_fixed.at[idx, address_column] = fixed_address
                total_fixed += 1
        
        logger.info(f"Fixed {total_fixed} Kentucky addresses")
        return df_fixed
    
    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove original columns that have been replaced by cleaned versions."""
        logger.info("Removing duplicate columns...")
        
        # Columns to remove (original versions) - but NOT the new ones we created
        columns_to_remove = [
            'office_sought', 'location', 'election_date', 'election_type', 'Source_File'
        ]
        
        # Only remove if they exist and we have cleaned versions
        columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove)
            logger.info(f"Removed {len(columns_to_remove)} duplicate columns: {columns_to_remove}")
        
        return df
    
    def clean_kentucky_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize Kentucky candidate data according to final schema.
        
        Args:
            df: Raw Kentucky candidate data DataFrame
            
        Returns:
            Cleaned DataFrame conforming to final schema
        """
        logger.info(f"Starting Kentucky data cleaning for {len(df)} records...")
        
        # Create a copy to avoid modifying original
        cleaned_df = df.copy()
        
        # Step 1: Handle election year and type
        cleaned_df = self._process_election_data(cleaned_df)
        
        # Step 2: Clean and standardize office and district information
        cleaned_df = self._process_office_and_district(cleaned_df)
        
        # Step 3: Clean candidate names
        cleaned_df = self._process_candidate_names(cleaned_df)
        
        # Step 4: Standardize party names
        cleaned_df = self._standardize_parties(cleaned_df)
        
        # Step 5: Fix Kentucky-specific address issues
        cleaned_df = self._fix_kentucky_addresses(cleaned_df)
        
        # Step 6: Remove duplicate columns
        cleaned_df = self._remove_duplicate_columns(cleaned_df)
        
        # Step 7: Final validation and cleanup
        cleaned_df = self._final_validation(cleaned_df)
        
        logger.info(f"Kentucky data cleaning completed. Final record count: {len(cleaned_df)}")
        return cleaned_df
    
    def _final_validation(self, df: pd.DataFrame) -> pd.DataFrame:
        """Perform final validation and cleanup of the cleaned data."""
        logger.info("Performing final validation...")
        
        # Ensure all required columns exist
        cleaned_df = self.ensure_column_order(df)
        
        # Add state column
        cleaned_df['state'] = 'Kentucky'
        
        # Final data type checks
        if 'election_year' in cleaned_df.columns:
            cleaned_df['election_year'] = pd.to_numeric(cleaned_df['election_year'], errors='coerce')
        
        # Remove any completely empty rows
        cleaned_df = cleaned_df.dropna(how='all')
        
        logger.info(f"Final validation completed. Shape: {cleaned_df.shape}")
        return cleaned_df
    
    def ensure_column_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure columns match Alaska's exact order."""
        ALASKA_COLUMN_ORDER = [
            'election_year', 'election_type', 'office', 'district', 'candidate_name',
            'first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname',
            'full_name_display', 'party', 'phone', 'email', 'address', 'website',
            'state', 'original_name', 'original_state', 'original_election_year',
            'original_office', 'original_filing_date', 'id', 'stable_id', 'county',
            'city', 'zip_code', 'filing_date', 'election_date', 'facebook', 'twitter'
        ]
        
        # Only add missing columns, don't overwrite existing ones
        for col in ALASKA_COLUMN_ORDER:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Create a new dataframe with columns in the correct order, preserving data
        result_df = pd.DataFrame()
        for col in ALASKA_COLUMN_ORDER:
            if col in df.columns:
                result_df[col] = df[col]
            else:
                result_df[col] = pd.NA
        
        return result_df
    
    def _process_election_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process election year and type from election_date and election_type columns."""
        logger.info("Processing election data...")
        
        # Use the actual column names
        election_date_col = 'election_date'
        election_type_col = 'election_type'
        
        def extract_election_year(election_date_str: str) -> Optional[int]:
            if pd.isna(election_date_str):
                return None
            
            election_date_str = str(election_date_str).strip()
            
            # Extract year from election date string
            year_match = re.search(r'20\d{2}', election_date_str)
            if year_match:
                return int(year_match.group())
            
            return None
        
        def standardize_election_type(election_type_str: str) -> str:
            if pd.isna(election_type_str):
                return "General"  # Default
            
            election_type_str = str(election_type_str).strip().lower()
            
            if 'primary' in election_type_str:
                return "Primary"
            elif 'general' in election_type_str:
                return "General"
            elif 'special' in election_type_str:
                return "Special"
            else:
                return "General"  # Default
        
        # Apply election processing
        df['election_year'] = df[election_date_col].apply(extract_election_year)
        df['election_type'] = df[election_type_col].apply(standardize_election_type)
        
        return df
    
    def _process_office_and_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize office and district information."""
        logger.info("Processing office and district information...")
        
        # Use the actual column names
        office_col = 'office_sought'
        
        def process_office_district(office_str: str) -> Tuple[str, Optional[str]]:
            if pd.isna(office_str):
                return None, None
            
            office_str = str(office_str).strip()
            
            # Handle US President
            if "president" in office_str.lower():
                return "US President", None
            
            # Handle US Representative
            if "representative" in office_str.lower() and "united states" in office_str.lower():
                return "US Representative", None
            
            # Handle US Senator
            if "senator" in office_str.lower() and "united states" in office_str.lower():
                return "US Senator", None
            
            # Handle State Senate
            if "senate" in office_str.lower() and "state" in office_str.lower():
                # Extract district if present
                district_match = re.search(r'district\s+(\d+)', office_str, re.IGNORECASE)
                if district_match:
                    return "State Senate", district_match.group(1)
                return "State Senate", None
            
            # Handle State House
            if "house" in office_str.lower() and "state" in office_str.lower():
                # Extract district if present
                district_match = re.search(r'district\s+(\d+)', office_str, re.IGNORECASE)
                if district_match:
                    return "State House", district_match.group(1)
                return "State House", None
            
            # Handle Governor
            if "governor" in office_str.lower():
                return "Governor", None
            
            # Handle Lieutenant Governor
            if "lieutenant governor" in office_str.lower():
                return "Lieutenant Governor", None
            
            # Handle Attorney General
            if "attorney general" in office_str.lower():
                return "Attorney General", None
            
            # Handle Secretary of State
            if "secretary of state" in office_str.lower():
                return "Secretary of State", None
            
            # Handle Auditor
            if "auditor" in office_str.lower():
                return "State Auditor", None
            
            # Handle Treasurer
            if "treasurer" in office_str.lower():
                return "State Treasurer", None
            
            # Handle Commissioner of Agriculture
            if "commissioner of agriculture" in office_str.lower():
                return "Commissioner of Agriculture", None
            
            # Handle Magistrate/Justice of the Peace
            if "magistrate" in office_str.lower() or "justice of the peace" in office_str.lower():
                return "Magistrate/Justice of the Peace", None
            
            # Handle City Council Member
            if "city council member" in office_str.lower():
                return "City Council Member", None
            
            # Handle other offices (keep as is)
            return office_str, None
        
        # Apply office and district processing
        office_results = df[office_col].apply(process_office_district)
        df['office'] = [result[0] for result in office_results]
        df['district'] = [result[1] for result in office_results]
        df['district'] = df['district'].astype('object')
        
        return df
    
    def _process_candidate_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process candidate names."""
        logger.info("Processing candidate names...")
        
        # Use the actual column names from raw data
        first_name_col = 'first_name'
        last_name_col = 'last_name'
        
        # Create candidate_name by combining first and last names
        df['candidate_name'] = df.apply(
            lambda row: f"{row[first_name_col]} {row[last_name_col]}".strip() 
            if pd.notna(row[first_name_col]) and pd.notna(row[last_name_col])
            else (row[first_name_col] if pd.notna(row[first_name_col]) else row[last_name_col]),
            axis=1
        )
        
        # Parse names into components
        df = self._parse_names(df, first_name_col, last_name_col)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame, first_name_col: str, last_name_col: str) -> pd.DataFrame:
        """Parse candidate names into first, middle, last, prefix, suffix, nickname, and display components."""
        logger.info("Parsing candidate names...")
        
        # Store original names before processing
        original_first_names = df[first_name_col].copy()
        original_last_names = df[last_name_col].copy()
        
        # Initialize new columns with clean names (no quotes)
        df['middle_name'] = pd.NA
        df['prefix'] = pd.NA
        df['suffix'] = pd.NA
        df['nickname'] = pd.NA
        df['full_name_display'] = pd.NA
        
        # Create clean first_name and last_name columns
        clean_first_names = []
        clean_last_names = []
        
        for idx in range(len(df)):
            first_name = str(original_first_names.iloc[idx]).strip() if pd.notna(original_first_names.iloc[idx]) else ""
            last_name = str(original_last_names.iloc[idx]).strip() if pd.notna(original_last_names.iloc[idx]) else ""
            
            # Check for suffix in last name
            suffix = None
            if last_name:
                suffix_pattern = r'\b(Jr|Sr|II|III|IV|V|VI|VII|VIII|IX|X)\b'
                suffix_match = re.search(suffix_pattern, last_name, re.IGNORECASE)
                if suffix_match:
                    suffix = suffix_match.group(1)
                    # Remove suffix from last name
                    last_name = re.sub(suffix_pattern, '', last_name, flags=re.IGNORECASE).strip()
            
            # Check for prefix in first name
            prefix = None
            if first_name:
                prefix_pattern = r'\b(Dr|Mr|Mrs|Ms|Miss|Prof|Rev|Hon|Sen|Rep|Gov|Lt|Col|Gen|Adm|Capt|Maj|Sgt|Cpl|Pvt)\b'
                prefix_match = re.match(prefix_pattern, first_name, re.IGNORECASE)
                if prefix_match:
                    prefix = prefix_match.group(1)
                    # Remove prefix from first name
                    first_name = re.sub(f'^{prefix_pattern}\\s*', '', first_name, flags=re.IGNORECASE).strip()
            
            # Build display name
            display_parts = []
            if prefix:
                display_parts.append(prefix)
            if first_name:
                display_parts.append(first_name)
            if last_name:
                display_parts.append(last_name)
            if suffix:
                display_parts.append(suffix)
            
            display_name = ' '.join(display_parts).strip()
            
            # Store clean names
            clean_first_names.append(first_name)
            clean_last_names.append(last_name)
            
            # Assign parsed components
            df.at[idx, 'prefix'] = prefix
            df.at[idx, 'suffix'] = suffix
            df.at[idx, 'full_name_display'] = display_name
        
        # Assign clean names to the dataframe
        df['first_name'] = clean_first_names
        df['last_name'] = clean_last_names
        
        return df
    
    def _standardize_parties(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize party names."""
        logger.info("Standardizing party names...")
        
        # Check if party data exists in the raw data
        party_mapping = {
            'democrat': 'Democratic',
            'democratic': 'Democratic',
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
        
        # Look for party column in raw data
        party_columns = ['party', 'Party', 'PARTY', 'party_affiliation', 'Party Affiliation']
        party_col = None
        for col in party_columns:
            if col in df.columns:
                party_col = col
                break
        
        if party_col and df[party_col].notna().any():
            # Process existing party data
            df['party'] = df[party_col].apply(standardize_party)
            logger.info(f"Found and processed party data from column: {party_col}")
        else:
            # No party data available
            df['party'] = pd.NA
            logger.info("No party data found in raw data")
        
        return df
    
    def _clean_contact_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean contact information (phone, email, address, website)."""
        logger.info("Cleaning contact information...")
        
        # Clean addresses from location column
        def clean_address(address_str: str) -> str:
            if pd.isna(address_str):
                return None
            
            # Remove extra whitespace and quotes
            cleaned = str(address_str).strip().strip('"\'')
            # Remove multiple spaces
            cleaned = re.sub(r'\s+', ' ', cleaned)
            return cleaned
        
        # Kentucky data doesn't have phone, email, or website, so set to NULL
        df['phone'] = pd.NA
        df['email'] = pd.NA
        df['website'] = pd.NA
        
        # Map location column to address if available
        if 'location' in df.columns:
            df['address'] = df['location'].apply(clean_address)
        else:
            df['address'] = pd.NA
        
        return df
    
    def _add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all required columns for the final schema."""
        logger.info("Adding required columns...")
        
        # Use the actual column names
        first_name_col = 'first_name'
        last_name_col = 'last_name'
        office_col = 'office_sought'
        election_year_col = 'election_year'
        
        # Add state column
        df['state'] = self.state_name
        
        # Add original data preservation columns
        df['original_name'] = df.apply(lambda row: f"{row[first_name_col]} {row[last_name_col]}".strip(), axis=1)
        df['original_state'] = df['state'].copy()
        df['original_election_year'] = df[election_year_col].copy()
        df['original_office'] = df[office_col].copy()
        df['original_filing_date'] = pd.NA  # Not available in Kentucky data
        
        # Add missing columns with None values
        required_columns = [
            'id', 'stable_id', 'county', 'city', 'zip_code', 'filing_date', 
            'election_date', 'facebook', 'twitter'
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

def clean_kentucky_candidates(input_file: str, output_file: str = None, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean Kentucky candidate data.
    
    Args:
        input_file: Path to the input Excel file
        output_file: Optional path to save the cleaned data (if None, will use default naming in output_dir)
        output_dir: Directory to save cleaned data (default: "cleaned_data")
        
    Returns:
        Cleaned DataFrame
    """
    # Load the data
    logger.info(f"Loading Kentucky data from {input_file}...")
    
    # Handle different file types
    if input_file.endswith('.csv'):
        df = pd.read_csv(input_file)
    else:
        df = pd.read_excel(input_file)
    
    # Initialize cleaner with output directory
    cleaner = KentuckyCleaner(output_dir=output_dir)
    
    # Clean the data
    cleaned_df = cleaner.clean_kentucky_data(df)
    
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
    
    # Find Kentucky files specifically
    kentucky_files = [f for f in available_files if 'kentucky' in os.path.basename(f).lower()]
    
    if not kentucky_files:
        print("No Kentucky files found in input directory")
        exit(1)
    
    # Use the first Kentucky file
    input_file = kentucky_files[0]
    output_dir = DEFAULT_OUTPUT_DIR
    
    print(f"\nProcessing Kentucky file: {os.path.basename(input_file)}")
    print(f"Output directory: {output_dir}")
    
    # Clean the data and save to the new output directory
    cleaned_data = clean_kentucky_candidates(input_file, output_dir=output_dir)
    print(f"\nCleaned {len(cleaned_data)} records")
    print(f"Columns: {cleaned_data.columns.tolist()}")
    print(f"Data saved to: {output_dir}/")
