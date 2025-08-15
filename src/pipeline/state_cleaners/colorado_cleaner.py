#!/usr/bin/env python3
"""
Colorado State Data Cleaner

This module contains functions to clean and standardize Colorado political candidate data
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
    List all available CSV files in the input directory.
    
    Args:
        input_dir: Directory to search for input files
        
    Returns:
        List of available CSV file paths
    """
    if not os.path.exists(input_dir):
        logger.warning(f"Input directory {input_dir} does not exist")
        return []
    
    csv_files = []
    for file in os.listdir(input_dir):
        if file.endswith('.csv') and 'colorado' in file.lower():
            csv_files.append(os.path.join(input_dir, file))
    
    return sorted(csv_files)

class ColoradoCleaner:
    """Handles cleaning and standardization of Colorado political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "Colorado"
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
        
    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove original columns that have been replaced by cleaned versions."""
        logger.info("Removing duplicate columns...")
        
        # Columns to remove (original versions) - only remove if we have cleaned versions
        columns_to_remove = []
        
        # Check if we have cleaned versions before removing originals
        if 'candidate_name' in df.columns and 'name' in df.columns:
            columns_to_remove.append('name')
        if 'office_cleaned' in df.columns and 'original_office' in df.columns:
            columns_to_remove.append('office_cleaned')
        if 'district_cleaned' in df.columns:
            columns_to_remove.append('district_cleaned')
        # Don't remove party column - we need to keep it
        
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove)
            logger.info(f"Removed {len(columns_to_remove)} duplicate columns: {columns_to_remove}")
        
        return df

    def clean_colorado_data(self, df: pd.DataFrame, filename: str) -> pd.DataFrame:
        """
        Clean and standardize Colorado candidate data according to final schema.
        
        Args:
            df: Raw Colorado candidate data DataFrame
            filename: Original filename for extracting election year
            
        Returns:
            Cleaned DataFrame conforming to final schema
        """
        logger.info(f"Starting Colorado data cleaning for {len(df)} records...")
        
        # Create a copy to avoid modifying original
        cleaned_df = df.copy()
        
        # Step 1: Handle election year and type from filename
        cleaned_df = self._process_election_data(cleaned_df, filename)
        
        # Step 2: Clean and standardize office and district information
        cleaned_df = self._process_office_and_district(cleaned_df)
        
        # Step 3: Clean candidate names
        cleaned_df = self._process_candidate_names(cleaned_df)
        
        # Step 4: Standardize party names
        cleaned_df = self._standardize_parties(cleaned_df)
        
        # Step 5: Clean contact information (Colorado doesn't have much contact info)
        cleaned_df = self._clean_contact_info(cleaned_df)
        
        # Step 6: Add required columns for final schema
        cleaned_df = self._add_required_columns(cleaned_df)
        
        # Step 7: Generate stable IDs (skipped - will be done later in process)
        # cleaned_df = self._generate_stable_ids(cleaned_df)
        
        # Step 8: Remove duplicate columns
        cleaned_df = self._remove_duplicate_columns(cleaned_df)
        
        # Step 9: Reorder columns to match Alaska's schema
        cleaned_df = self._reorder_columns(cleaned_df)
        
        # Final check: ensure district is string type
        if 'district' in cleaned_df.columns:
            # Convert to string first, then to object to ensure proper type
            cleaned_df['district'] = cleaned_df['district'].astype(str).astype('object')
            # Replace 'nan' strings with actual NaN
            cleaned_df['district'] = cleaned_df['district'].replace('nan', pd.NA)
            logger.info(f"Final district column type: {cleaned_df['district'].dtype}")
        
        logger.info(f"Colorado data cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def _process_election_data(self, df: pd.DataFrame, filename: str) -> pd.DataFrame:
        """Process election year and type from filename."""
        logger.info("Processing election data from filename...")
        
        # Extract year from filename (e.g., "colorado_candidates_2024.csv" -> 2024)
        year_match = re.search(r'(\d{4})', filename)
        if year_match:
            election_year = int(year_match.group(1))
        else:
            # Default to current year if no year found
            election_year = datetime.now().year
            logger.warning(f"No election year found in filename {filename}, using {election_year}")
        
        # For Colorado, assume it's a Primary election unless specified otherwise
        election_type = "Primary"
        
        # Add election columns
        df['election_year'] = election_year
        df['election_type'] = election_type
        
        return df
    
    def _process_office_and_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize office and district information."""
        logger.info("Processing office and district information...")
        
        def process_office_district(office_str: str, district_str: str) -> Tuple[str, Optional[str]]:
            if pd.isna(office_str):
                return None, None
            
            office_str = str(office_str).strip()
            district_str = str(district_str).strip() if pd.notna(district_str) else ""
            
            # Handle US Representative
            if "Representative to the" in office_str and "United States Congress" in office_str:
                # Extract district number from the office string
                district_match = re.search(r'District (\d+)', office_str)
                if district_match:
                    district = district_match.group(1)
                else:
                    district = None
                return "US Representative", district
            
            # Handle State Senator
            if "State Senator" in office_str:
                # Extract district from office string (e.g., "State Senator - District X")
                district_match = re.search(r'District\s+(\d+)', office_str, re.IGNORECASE)
                if district_match:
                    district = district_match.group(1)
                    return "State Senator", district
                else:
                    return "State Senator", district_str if district_str else None
            
            # Handle State Representative
            if "State Representative" in office_str:
                # Extract district from office string (e.g., "State Representative - District X")
                district_match = re.search(r'District\s+(\d+)', office_str, re.IGNORECASE)
                if district_match:
                    district = district_match.group(1)
                    return "State Representative", district
                else:
                    return "State Representative", district_str if district_str else None
            
            # Handle State Board of Education Member
            if "State Board of Education Member" in office_str:
                # Extract district from office string (e.g., "State Board of Education Member - Congressional District X")
                district_match = re.search(r'Congressional\s+District\s+(\d+)', office_str, re.IGNORECASE)
                if district_match:
                    district = district_match.group(1)
                    return "State Board of Education Member", district
                else:
                    return "State Board of Education Member", district_str if district_str else None
            
            # Handle Regent of the University of Colorado
            if "Regent of the University of Colorado" in office_str:
                # Extract district from office string (e.g., "Regent of the University of Colorado - Congressional District X")
                district_match = re.search(r'Congressional\s+District\s+(\d+)', office_str, re.IGNORECASE)
                if district_match:
                    district = district_match.group(1)
                    return "Regent of the University of Colorado", district
                else:
                    return "Regent of the University of Colorado", district_str if district_str else None
            
            # Handle District Attorney
            if "District Attorney" in office_str:
                # Extract district from office string (e.g., "District Attorney - 1st Judicial District")
                district_match = re.search(r'(\d+)(?:st|nd|rd|th)\s+Judicial\s+District', office_str, re.IGNORECASE)
                if district_match:
                    district = district_match.group(1)
                    return "District Attorney", district
                else:
                    return "District Attorney", district_str if district_str else None
            
            # For other offices, keep as is
            return office_str, district_str if district_str else None
        
        # Apply office and district processing
        office_results = df.apply(lambda row: process_office_district(row['office'], row['district']), axis=1)
        df['office_cleaned'] = [result[0] for result in office_results]
        df['district_cleaned'] = [result[1] for result in office_results]
        
        # Ensure district_cleaned is string type to prevent auto-conversion
        df['district_cleaned'] = df['district_cleaned'].fillna('').astype(str).replace('nan', '')
        # Clean up decimal places to prevent pandas auto-conversion to float
        df['district_cleaned'] = df['district_cleaned'].apply(lambda x: x.split('.')[0] if x and x != 'nan' else "")
        
        # Ensure we have the cleaned columns
        logger.info(f"Created office_cleaned column with {df['office_cleaned'].notna().sum()} non-null values")
        logger.info(f"Created district_cleaned column with {df['district_cleaned'].notna().sum()} non-null values")
        
        return df
    
    def _process_candidate_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process candidate names."""
        logger.info("Processing candidate names...")
        
        def clean_name(name_str: str) -> str:
            if pd.isna(name_str):
                return None
            
            name_str = str(name_str).strip()
            
            # Remove extra whitespace and quotes
            cleaned = re.sub(r'\s+', ' ', name_str).strip().strip('"\'')
            return cleaned
        
        # Apply name cleaning
        df['candidate_name'] = df['name'].apply(clean_name)
        
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
            name = row['candidate_name']
            original_name = row['name']
            
            if pd.isna(name) or not name:
                continue
            
            # Parse the name
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
        
        # Extract suffix from the end of the name (including optional period)
        suffix_pattern = r'\b(Jr\.?|Sr\.?|II|III|IV|V|VI|VII|VIII|IX|X)\b'
        suffix_match = re.search(suffix_pattern, name, re.IGNORECASE)
        if suffix_match:
            suffix = suffix_match.group(1)
            # Remove suffix from the name for further processing
            # Also remove any trailing period that might be left behind
            name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE).strip()
            # Clean up any trailing periods or extra spaces
            name = re.sub(r'\.\s*$', '', name).strip()
        
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
            'democratic party': 'Democratic',
            'republican party': 'Republican',
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
        """Clean contact information (Colorado doesn't have much contact info)."""
        logger.info("Cleaning contact information...")
        
        # Colorado data doesn't have contact information, so set to None
        df['phone'] = pd.NA
        df['email'] = pd.NA
        df['address'] = pd.NA
        df['website'] = pd.NA
        
        return df
    
    def _add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all required columns for the final schema."""
        logger.info("Adding required columns...")
        
        # Add state column
        df['state'] = self.state_name
        
        # Add original data preservation columns
        df['original_name'] = df['name'].copy()
        df['original_state'] = df['state'].copy()
        df['original_election_year'] = df['election_year'].copy()
        df['original_office'] = df['office'].copy()
        df['original_filing_date'] = pd.NA  # Not available in Colorado data
        
        # Add missing columns with None values
        required_columns = [
            'county', 'city', 'zip_code', 'filing_date', 
            'election_date', 'facebook', 'twitter', 'prefix', 'suffix', 'nickname'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Set ID columns to empty string (will be generated later)
        df['id'] = ""
        df['stable_id'] = ""
        
        # Map cleaned columns to final column names
        if 'office_cleaned' in df.columns:
            df['office'] = df['office_cleaned']
        if 'district_cleaned' in df.columns:
            # Only overwrite district if it doesn't already have the correct values
            # Check if current district values are correct (no underscore prefixes)
            current_districts = df['district'].fillna('').astype(str)
            has_underscore_prefix = current_districts.str.startswith('_').any()
            
            if has_underscore_prefix:
                # Use the cleaned district values (which should be correct)
                df['district'] = df['district_cleaned'].fillna('').astype(str).replace('nan', '')
            # If no underscore prefixes, keep the current district values
            
            # Force the column to be object (string) type
            df['district'] = df['district'].astype('object')
            
            # Ensure empty strings are properly handled
            df['district'] = df['district'].replace('', None)
        

        
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
                'original_office', 'party', 'district'
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
    
    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorder columns to match Alaska's exact schema order."""
        logger.info("Reordering columns to match Alaska schema...")
        
        # Alaska's exact column order
        alaska_column_order = [
            'election_year', 'election_type', 'office', 'district', 'candidate_name', 
            'first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 
            'full_name_display', 'party', 'phone', 'email', 'address', 'website', 
            'state', 'original_name', 'original_state', 'original_election_year', 
            'original_office', 'original_filing_date', 'id', 'stable_id', 'county', 
            'city', 'zip_code', 'filing_date', 'election_date', 'facebook', 'twitter'
        ]
        
        # Only include columns that exist in the dataframe
        existing_columns = [col for col in alaska_column_order if col in df.columns]
        
        # Add any remaining columns that weren't in the Alaska order
        remaining_columns = [col for col in df.columns if col not in existing_columns]
        final_column_order = existing_columns + remaining_columns
        
        # Reorder the dataframe
        df = df[final_column_order]
        
        logger.info(f"Reordered columns to match Alaska schema. Final order: {list(df.columns)}")
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

def clean_colorado_candidates(input_file: str, output_file: str = None, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean Colorado candidate data.
    
    Args:
        input_file: Path to the input CSV file
        output_file: Optional path to save the cleaned data (if None, will use default naming in output_dir)
        output_dir: Directory to save cleaned data (default: "cleaned_data")
        
    Returns:
        Cleaned DataFrame
    """
    # Load the data
    logger.info(f"Loading Colorado data from {input_file}...")
    df = pd.read_csv(input_file)
    
    # Convert district column to string to prevent float conversion
    if 'district' in df.columns:
        df['district'] = df['district'].fillna('').astype(str).replace('nan', '')
    
    # Initialize cleaner with output directory
    cleaner = ColoradoCleaner(output_dir=output_dir)
    
    # Clean the data
    cleaned_df = cleaner.clean_colorado_data(df, os.path.basename(input_file))
    
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
    
    # Save as CSV to preserve data types (Excel converts numeric strings to float)
    csv_output = output_file.replace('.xlsx', '.csv')
    cleaned_df.to_csv(csv_output, index=False)
    logger.info(f"Data saved as CSV to preserve data types!")
    
    # Also save as Excel for compatibility
    cleaned_df.to_excel(output_file, index=False)
    logger.info(f"Data also saved as Excel!")
    
    return cleaned_df

if __name__ == "__main__":
    # Show available input files
    print("Available Colorado input files:")
    available_files = list_available_input_files()
    if available_files:
        for i, file_path in enumerate(available_files, 1):
            print(f"  {i}. {os.path.basename(file_path)}")
    else:
        print("  No Colorado CSV files found in input directory")
        exit(1)
    
    # Example usage - use the first available file
    if available_files:
        input_file = available_files[0]  # Use the first available file
        output_dir = DEFAULT_OUTPUT_DIR
        
        print(f"\nProcessing: {os.path.basename(input_file)}")
        print(f"Output directory: {output_dir}")
        
        # Clean the data and save to the new output directory
        cleaned_data = clean_colorado_candidates(input_file, output_dir=output_dir)
        print(f"\nCleaned {len(cleaned_data)} records")
        print(f"Columns: {cleaned_data.columns.tolist()}")
        print(f"Data saved to: {output_dir}/")
    else:
        print("No input files available for processing") 