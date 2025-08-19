#!/usr/bin/env python3
"""
Delaware State Data Cleaner

This module contains functions to clean and standardize Delaware political candidate data
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
        if file.endswith(('.xlsx', '.xls')) and 'delaware' in file.lower() and not file.startswith('~$'):
            excel_files.append(os.path.join(input_dir, file))
    
    return sorted(excel_files)

class DelawareCleaner:
    """Handles cleaning and standardization of Delaware political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "Delaware"
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
            'Year', 'Election', 'Name', 'Office', 'District', 'County', 'Date Filed', 'Website', 'Phone'
        ]
        
        # Only remove if they exist and we have cleaned versions
        columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove)
            logger.info(f"Removed {len(columns_to_remove)} duplicate columns: {columns_to_remove}")
        
        return df

    def clean_delaware_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize Delaware candidate data according to final schema.
        
        Args:
            df: Raw Delaware candidate data DataFrame
            
        Returns:
            Cleaned DataFrame conforming to final schema
        """
        logger.info(f"Starting Delaware data cleaning for {len(df)} records...")
        
        # Create a copy to avoid modifying original
        cleaned_df = df.copy()
        
        # Step 1: Handle election year and type
        cleaned_df = self._process_election_data(cleaned_df)
        
        # Step 2: Clean and standardize office and district information
        cleaned_df = self._process_office_and_district(cleaned_df)
        
        # Step 3: Clean candidate names
        cleaned_df = self._process_full_name_displays(cleaned_df)
        
        # Step 4: Standardize party names (Delaware doesn't have party info, so set to None)
        cleaned_df = self._standardize_parties(cleaned_df)
        
        # Step 5: Clean contact information
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
            cleaned_df['district'] = cleaned_df['district'].astype('object')
            logger.info(f"Final district column type: {cleaned_df['district'].dtype}")
        
        logger.info(f"Delaware data cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def _process_election_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process election year and type from Year and Election columns."""
        logger.info("Processing election data...")
        
        def extract_election_info(row) -> Tuple[Optional[int], Optional[str], Optional[str]]:
            year_val = row['Year']
            election_str = row['Election']
            
            # Get year from Year column first
            if pd.notna(year_val):
                try:
                    year = int(year_val)
                except (ValueError, TypeError):
                    year = None
            else:
                year = None
            
            # If no year from Year column, try to extract from Election column
            if year is None and pd.notna(election_str):
                election_str = str(election_str).strip()
                year_match = re.search(r'20\d{2}', election_str)
                if year_match:
                    year = int(year_match.group())
            
            # Extract election date from Election column
            election_date = None
            if pd.notna(election_str):
                election_str = str(election_str).strip()
                # Look for date patterns like MM/DD/YYYY or MM-DD-YYYY
                date_patterns = [
                    r'(\d{1,2})/(\d{1,2})/(\d{4})',  # MM/DD/YYYY
                    r'(\d{1,2})-(\d{1,2})-(\d{4})',  # MM-DD-YYYY
                    r'(\d{1,2})\.(\d{1,2})\.(\d{4})',  # MM.DD.YYYY
                ]
                
                for pattern in date_patterns:
                    date_match = re.search(pattern, election_str)
                    if date_match:
                        month, day, year_str = date_match.groups()
                        # Validate date components
                        if 1 <= int(month) <= 12 and 1 <= int(day) <= 31 and 2000 <= int(year_str) <= 2030:
                            election_date = f"{int(year_str):04d}-{int(month):02d}-{int(day):02d}"
                            break
            
            # Determine election type from Election column
            election_type = "General"  # Default
            if pd.notna(election_str):
                election_str = str(election_str).strip()
                election_str_lower = election_str.lower()
                
                if 'primary' in election_str_lower:
                    election_type = "Primary"
                elif 'general' in election_str_lower:
                    election_type = "General"
                elif 'presidential' in election_str_lower:
                    election_type = "Primary"
                elif 'write-in' in election_str_lower:
                    election_type = "General"  # Write-in candidates are for general elections
                elif 'special' in election_str_lower:
                    election_type = "Special"
            
            return year, election_type, election_date
        
        # Apply election processing using both Year and Election columns
        election_results = df.apply(extract_election_info, axis=1)
        df['election_year'] = [result[0] for result in election_results]
        df['election_type'] = [result[1] for result in election_results]
        df['election_date'] = [result[2] for result in election_results]
        
        # Ensure election_year is int64 to match Alaska's format
        # Handle NaN values by filling with 0 first, then converting
        df['election_year'] = df['election_year'].fillna(0).astype('int64')
        # Convert 0 back to None for proper handling
        df['election_year'] = df['election_year'].replace(0, None)
        
        # Ensure the column maintains int64 type even with None values
        df['election_year'] = df['election_year'].astype('Int64')
        
        # Log election processing results
        year_counts = df['election_year'].value_counts()
        type_counts = df['election_type'].value_counts()
        logger.info(f"Election year distribution: {year_counts.to_dict()}")
        logger.info(f"Election type distribution: {type_counts.to_dict()}")
        
        return df
    
    def _process_office_and_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize office and district information."""
        logger.info("Processing office and district information...")
        
        def process_office_district(office_str: str, district_val) -> Tuple[str, Optional[str]]:
            if pd.isna(office_str):
                return None, None
            
            office_str = str(office_str).strip()
            
            # Handle US President/Vice President
            if "President" in office_str and "Vice President" not in office_str:
                return "US President", None
            
            # Handle US President/Vice President combined
            if "PresidentVice President" in office_str:
                return "US President", None
            
            # Handle US Representative
            if "REP IN CONGRESS" in office_str:
                return "US Representative", "At Large"
            
            # Handle US Senator
            if "US SENATOR" in office_str:
                return "US Senator", "At Large"
            
            # Handle State Representative (multiple formats)
            if any(pattern in office_str for pattern in ["State Representative District", "STATE REPRESENTATIVE DISTRICT", "STATE REP DIS"]):
                # Extract district number from office string
                district_match = re.search(r'District (\d+)', office_str, re.IGNORECASE)
                if district_match:
                    district = district_match.group(1)
                else:
            
            
            
            
                    # Try abbreviated format
                    district_match = re.search(r'DIS (\d+)', office_str, re.IGNORECASE)
                    if district_match:
                        district = district_match.group(1)
                    else:
                        district = None
                return "State Representative", district
            
            # Handle State Senator (multiple formats)
            if any(pattern in office_str for pattern in ["State Senator District", "STATE SENATOR DISTRICT", "STATE SEN DIS"]):
                # Extract district number from office string
                district_match = re.search(r'District (\d+)', office_str, re.IGNORECASE)
                if district_match:
                    district = district_match.group(1)
                else:
            
            
            
            
                    # Try abbreviated format
                    district_match = re.search(r'DIS (\d+)', office_str, re.IGNORECASE)
                    if district_match:
                        district = district_match.group(1)
                    else:
                        district = None
                return "State Senator", district
            
            # Handle Governor
            if "Governor" in office_str:
                return "Governor", None
            
            # Handle City Council positions (multiple formats)
            if any(pattern in office_str for pattern in ["City Council", "CITY CNCL", "Wilmington City Council", "WILMINGTON CITY COUNCIL"]):
                if "At-Large" in office_str or "AT LRG" in office_str:
                    return "City Council Member", "At-Large"
                else:
            
            
            
            
                    # Extract district number from office string
                    district_match = re.search(r'District (\d+)', office_str, re.IGNORECASE)
                    if district_match:
                        district = district_match.group(1)
                    else:
                        district = None
                    return "City Council Member", district
            
            # Handle City Mayor positions
            if "Mayor" in office_str or "MAYOR" in office_str:
                return "Mayor", None
            
            # Handle City Treasurer positions
            if "City Treasurer" in office_str or "CITY TREASURER" in office_str:
                return "City Treasurer", None
            
            # Handle City President of City Council positions
            if "President of City Council" in office_str or "PRES CITY CNCL" in office_str:
                return "President of City Council", None
            
            # Handle County positions (multiple formats)
            if "County" in office_str or "COUNTY" in office_str:
                if "Levy Court" in office_str or "LEVY COURT" in office_str:
                    # Extract district number from office string
                    district_match = re.search(r'District (\d+)', office_str, re.IGNORECASE)
                    if district_match:
                        district = district_match.group(1)
                    else:
                        district = None
                    return "County Levy Court Member", district
                elif "Clerk of the Peace" in office_str or "CLERK OF THE PEACE" in office_str:
                    return "County Clerk of the Peace", None
                elif "Register of Wills" in office_str or "REGISTER OF WILLS" in office_str:
                    return "County Register of Wills", None
                elif "Council District" in office_str or "COUNCIL DISTRICT" in office_str:
                    # Extract district number from office string
                    district_match = re.search(r'District (\d+)', office_str, re.IGNORECASE)
                    if district_match:
                        district = district_match.group(1)
                    else:
                        district = None
                    return "County Council Member", district
            
            # Handle County Council (abbreviated format)
            if "CNTY CNCL DIS" in office_str:
                # Extract district number from office string
                district_match = re.search(r'DIS (\d+)', office_str, re.IGNORECASE)
                if district_match:
                    district = district_match.group(1)
                else:
                    district = None
                return "County Council Member", district
            
            # Handle Levy Court positions (without County prefix)
            if "Levy Court District" in office_str or "LEVY COURT DISTRICT" in office_str:
                # Extract district number from office string
                district_match = re.search(r'District (\d+)', office_str, re.IGNORECASE)
                if district_match:
                    district = district_match.group(1)
                else:
                    district = None
                return "County Levy Court Member", district
            
            # Handle State Senate positions (without District prefix)
            if "State Senate District" in office_str or "STATE SENATE DISTRICT" in office_str:
                # Extract district number from office string
                district_match = re.search(r'District (\d+)', office_str, re.IGNORECASE)
                if district_match:
                    district = district_match.group(1)
                else:
                    district = None
                return "State Senator", district
            
            # Handle U.S. Senator
            if "U.S. Senator" in office_str:
                return "U.S. Senator", None
            
            # Handle U.S. Representative
            if "Representative in Congress" in office_str:
                return "U.S. Representative", None
            
            # Catch-all: Look for any office with "District" in the name
            if "District" in office_str:
                # Extract district number from office string
                district_match = re.search(r'District (\d+)', office_str, re.IGNORECASE)
                if district_match:
                    district = district_match.group(1)
                    # Determine the office type based on the string
                    if "State Representative" in office_str or "STATE REPRESENTATIVE" in office_str:
                        return "State Representative", district
                    elif "State Senator" in office_str or "STATE SENATOR" in office_str:
                        return "State Senator", district
                    elif "City Council" in office_str or "CITY COUNCIL" in office_str:
                        return "City Council Member", district
                    elif "County Council" in office_str or "COUNTY COUNCIL" in office_str:
                        return "County Council Member", district
                    elif "Levy Court" in office_str or "LEVY COURT" in office_str:
                        return "County Levy Court Member", district
                    else:
            
            
            
            
                        # For any other office with district, keep the office name but extract district
                        # Remove "District X" from the office name
                        office_clean = re.sub(r'\s+District\s+\d+', '', office_str, flags=re.IGNORECASE)
                        return office_clean, district
            
            # Handle other offices, keep as is
            return office_str, None
        
        # Apply office and district processing
        office_results = df.apply(lambda row: process_office_district(row['Office'], row['District']), axis=1)
        df['office'] = [result[0] for result in office_results]
        df['district'] = [result[1] for result in office_results]
        
        # Log some examples of office processing for debugging
        logger.info("Office processing examples:")
        for i, (original, processed) in enumerate(zip(df['Office'].head(10), df['office'].head(10))):
            logger.info(f"  {original} -> {processed}")
        
        # Ensure district is string type
        df['district'] = df['district'].fillna('').astype(str).replace('nan', '')
        
        # Extract city information from office names and add to city column
        def extract_city_from_office(office_str: str) -> Optional[str]:
            if pd.isna(office_str):
                return None
            
            office_str = str(office_str).strip()
            
            # Extract city names from office strings using multiple regex patterns
            
            # Pattern 1: "City of [City Name]" followed by additional text
            city_match = re.search(r'City of ([A-Za-z]+)', office_str, re.IGNORECASE)
            if city_match:
                city_name = city_match.group(1).strip()
                # Only return if it's a valid city name (not a common word)
                if city_name.lower() not in ['of', 'president', 'pres', 'city', 'council', 'member', 'district', 'at', 'large', 'mayor', 'treasurer']:
                    return city_name
            
            # Pattern 2: "[CITY NAME] CITY" (all caps format)
            city_match = re.search(r'([A-Z]+)\s+CITY', office_str)
            if city_match:
                return city_match.group(1).title()  # Convert to title case
            
            # Pattern 3: "[City Name] [Additional Text]" (when office contains city-specific positions)
            # Look for common city names in Delaware, but EXCLUDE county names
            delaware_cities = [
                'Wilmington', 'Dover', 'Newark', 'Middletown', 'Smyrna', 
                'Milford', 'Seaford', 'Georgetown', 'Elsmere', 
                'Laurel', 'Delmar', 'Harrington', 'Camden', 'Felton',
                'Bridgeville', 'Greenwood', 'Frankford', 'Dagsboro', 'Bethany Beach'
            ]
            # Note: Removed 'New Castle' from cities since it's a county name
            
            for city in delaware_cities:
                # Check for city name in various formats, but be more precise
                # Look for city name followed by space or end of string
                city_pattern = r'\b' + re.escape(city) + r'\b'
                if re.search(city_pattern, office_str, re.IGNORECASE):
                    # Additional validation: make sure we're not matching common words
                    if city.lower() not in ['of', 'president', 'pres', 'city', 'council', 'member', 'district', 'at', 'large', 'mayor', 'treasurer']:
                        # Additional check: make sure the city name is not part of a larger word
                        # and that it's not immediately followed by common words
                        city_match = re.search(city_pattern, office_str, re.IGNORECASE)
                        if city_match:
                            start_pos = city_match.start()
                            # Check if the city name is followed by common words that would indicate it's not a city
                            after_city = office_str[start_pos + len(city):].strip()
                            if after_city and not any(after_city.lower().startswith(word) for word in ['president', 'pres', 'of', 'city', 'council']):
                                return city
            
            return None
        
        # Extract county information from office names and add to county column
        def extract_county_from_office(office_str: str) -> Optional[str]:
            if pd.isna(office_str):
                return None
            
            office_str = str(office_str).strip()
            
            # Extract county names from office strings using multiple patterns
            
            # Pattern 1: "[County Name] County [Additional Text]" - highest priority
            county_match = re.search(r'([A-Za-z]+(?:\s+[A-Za-z]+)*)\s+County', office_str, re.IGNORECASE)
            if county_match:
                county_name = county_match.group(1).strip()
                # Only return if it's a valid county name (not a common word)
                if county_name.lower() not in ['of', 'president', 'pres', 'city', 'council', 'member', 'district', 'at', 'large', 'mayor', 'treasurer']:
                    return county_name.title()  # Convert to title case
            
            # Pattern 2: "[COUNTY NAME] COUNTY" (all caps format)
            county_match = re.search(r'([A-Z]+(?:\s+[A-Z]+)*)\s+COUNTY', office_str)
            if county_match:
                county_name = county_match.group(1).strip()
                return county_name.title()  # Convert to title case
            
            # Pattern 3: Look for specific Delaware counties in any context
            delaware_counties = [
                'New Castle', 'Kent', 'Sussex'
            ]
            
            for county in delaware_counties:
                # Check for county name in various formats, but be more precise
                county_pattern = r'\b' + re.escape(county) + r'\b'
                if re.search(county_pattern, office_str, re.IGNORECASE):
                    return county
            
            return None
        
        # Apply county extraction FIRST to the original Office column
        df['county'] = df['Office'].apply(extract_county_from_office)
        logger.info(f"County extraction completed. Sample results: {df['county'].value_counts().head(5).to_dict()}")
        
        # Apply city extraction AFTER county extraction to avoid conflicts
        df['city'] = df['Office'].apply(extract_city_from_office)
        logger.info(f"City extraction completed. Sample results: {df['city'].value_counts().head(5).to_dict()}")
        
        # Clean up problematic city values
        def clean_city_value(city_val):
            if pd.isna(city_val):
                return None
            
            city_str = str(city_val).strip()
            
            # Filter out common words that aren't city names
            if city_str.lower() in ['of', 'president', 'pres', 'city', 'council', 'member', 'district', 'at', 'large', 'mayor', 'treasurer']:
                return None
            
            return city_str
        
        df['city'] = df['city'].apply(clean_city_value)
        
        # Standardize city names (convert all caps to title case)
        def standardize_city_name(city_val):
            if pd.isna(city_val):
                return None
            
            city_str = str(city_val).strip()
            
            # Convert all caps to title case for consistency
            if city_str.isupper() and len(city_str) > 1:
                return city_str.title()
            
            return city_str
        
        df['city'] = df['city'].apply(standardize_city_name)
        

        
        # Clean up problematic county values
        def clean_county_value(county_val):
            if pd.isna(county_val):
                return None
            
            county_str = str(county_val).strip()
            
            # Handle specific problematic patterns
            if 'New Castle County President Of' in county_str:
                return 'New Castle'
            elif 'New Castle President Of' in county_str:
                return 'New Castle'
            elif 'President Of' in county_str:
                # Extract the county name before "President Of"
                # Look for Delaware county names
                delaware_counties = ['New Castle', 'Kent', 'Sussex']
                for county in delaware_counties:
                    if county in county_str:
                        return county
                return None
            
            # Filter out standalone common words that aren't county names
            if county_str.lower() in ['of', 'president', 'pres', 'city', 'council', 'member', 'district', 'at', 'large', 'mayor', 'treasurer']:
                return None
            
            return county_str
        
        df['county'] = df['county'].apply(clean_county_value)
        
        # Log city and county extraction results for debugging
        city_counts = df['city'].value_counts()
        county_counts = df['county'].value_counts()
        logger.info(f"City extraction results: {city_counts.to_dict()}")
        logger.info(f"County extraction results: {county_counts.to_dict()}")
        logger.info(f"Records with city extracted: {df['city'].notna().sum()}")
        logger.info(f"Records with county extracted: {df['county'].notna().sum()}")
        logger.info(f"Records without city: {df['city'].isna().sum()}")
        logger.info(f"Records without county: {df['county'].isna().sum()}")
        
        # Standardize office names
        df = self._standardize_office_names(df)
        
        return df
    
    def _standardize_office_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize office names to use consistent capitalization and formatting."""
        logger.info("Standardizing office names...")
        
        def standardize_office(office_str: str) -> str:
            if pd.isna(office_str):
                return None
            
            office_str = str(office_str).strip()
            
            # Normalize spaces (remove extra spaces, tabs, etc.)
            office_str = re.sub(r'\s+', ' ', office_str).strip()
            
            # Handle specific office types with proper capitalization
            office_mappings = {
                # US Federal offices
                'PRESIDENT': 'US President',
                'US PRESIDENT': 'US President',
                'U.S. REPRESENTATIVE': 'US Representative',
                'US REPRESENTATIVE': 'US Representative',
                'U.S. SENATOR': 'US Senator',
                'US SENATOR': 'US Senator',
                
                # State offices
                'GOVERNOR': 'Governor',
                'SHERIFF': 'Sheriff',
                
                # City offices
                'WILMINGTON CITY COUNCIL AT LARGE': 'City Council Member At-Large',
                'WILMINGTON CITY COUNCIL': 'City Council Member',
                
                # County offices
                'COUNTY COUNCIL MEMBER': 'County Council Member',
                'COUNTY LEVY COURT MEMBER': 'County Levy Court Member',
                'COUNTY CLERK OF THE PEACE': 'County Clerk of the Peace',
                'COUNTY REGISTER OF WILLS': 'County Register of Wills',
                
                # Legislative offices
                'STATE REPRESENTATIVE': 'State Representative',
                'STATE SENATOR': 'State Senator',
                'STATE REP': 'State Representative',
                'STATE SEN': 'State Senator',
            }
            
            # Check for exact matches first
            if office_str.upper() in office_mappings:
                return office_mappings[office_str.upper()]
            
            # Handle partial matches and apply title case for others
            for pattern, replacement in office_mappings.items():
                if office_str.upper() == pattern:
                    return replacement
            
            # For any remaining offices, apply title case
            return office_str.title()
        
        # Apply standardization to the office column
        df['office'] = df['office'].apply(standardize_office)
        
        # Log the standardization results
        office_counts = df['office'].value_counts()
        logger.info(f"Office standardization completed. Top offices: {office_counts.head(10).to_dict()}")
        
        return df
    
    def _process_full_name_displays(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process candidate names."""
        logger.info("Processing candidate names...")
        
        def clean_name(row) -> str:
            name_str = row['Name']
            office_str = row['Office']
            
            if pd.isna(name_str):
                return None
            
            name_str = str(name_str).strip()
            
            # Remove extra whitespace and quotes
            cleaned = re.sub(r'\s+', ' ', name_str).strip().strip('"\'')
            
            # For president candidates, handle pipe-separated names (take first candidate only)
            if '|' in cleaned:
                # Split on pipe and take the first candidate's name
                first_candidate = cleaned.split('|')[0].strip()
                cleaned = first_candidate
                logger.info(f"Split pipe-separated name: {name_str} -> {cleaned}")
            
            # Handle concatenated names for President candidates (e.g., "Chase OliverMike ter Maat", "Donald J. TrumpJD Vance")
            # Look for specific patterns that indicate concatenated candidate names
            if pd.notna(office_str) and 'President' in str(office_str):
                # Look for specific known concatenated patterns
                if 'TrumpJD' in cleaned:
                    # Split at "TrumpJD" - take only "Donald J. Trump"
                    cleaned = cleaned.split('TrumpJD')[0].strip() + ' Trump'
                    logger.info(f"Fixed Trump concatenated name: {name_str} -> {cleaned}")
                elif 'OliverMike' in cleaned:
                    # Split at "OliverMike" - take only "Chase Oliver"
                    cleaned = cleaned.split('OliverMike')[0].strip() + ' Oliver'
                    logger.info(f"Fixed Oliver concatenated name: {name_str} -> {cleaned}")
                elif 'HarrisTim' in cleaned:
                    # Split at "HarrisTim" - take only "Kamala D. Harris"
                    cleaned = cleaned.split('HarrisTim')[0].strip() + ' Harris'
                    logger.info(f"Fixed Harris concatenated name: {name_str} -> {cleaned}")
                elif 'SupremeJonathan' in cleaned:
                    # Split at "SupremeJonathan" - take only "Vermin Supreme"
                    cleaned = cleaned.split('SupremeJonathan')[0].strip() + ' Supreme'
                    logger.info(f"Fixed Supreme concatenated name: {name_str} -> {cleaned}")
                # Add more specific patterns as needed
            
            return cleaned
        
        # Apply name cleaning using apply with axis=1 to access both Name and Office columns
        df['full_name_display'] = df.apply(clean_name, axis=1)
        
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
            # Handle nested quotes by looking for the innermost quoted text
            # Use a more specific pattern that distinguishes between quotes and apostrophes
            nickname_match = re.search(r'["""\u201c\u201d]([^""""\u201c\u201d]+)["""\u201c\u201d]', original_str)
            if nickname_match:
                nickname = nickname_match.group(1)
                # Remove nickname from the name for further processing
                # Use the original name for this replacement to avoid issues with already processed name
                # Remove the quoted nickname part completely
                name = re.sub(r'["""\u201c\u201d][^""""\u201c\u201d]+["""\u201c\u201d]', '', original_str).strip()
                # Clean up any extra spaces that might be left
                name = re.sub(r'\s+', ' ', name).strip()
        
        # Handle names with apostrophes intelligently (after nickname removal)
        name = self._handle_apostrophe_names(name)
        
        # Extract suffix from the end of the name
        suffix_pattern = r'\b(Jr|Sr|II|III|IV|V|VI|VII|VIII|IX|X)\b'
        suffix_match = re.search(suffix_pattern, name, re.IGNORECASE)
        if suffix_match:
            suffix = suffix_match.group(1)
            # Remove suffix from the name for further processing
            name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE).strip()
            # Clean up any trailing punctuation that might be left behind
            name = re.sub(r'[^\w\s]+$', '', name).strip()
        
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
        """Standardize party names (Delaware doesn't have party info)."""
        logger.info("Standardizing party names...")
        
        # Delaware data doesn't have party information, so set to None (like Alaska)
        df['party'] = pd.NA
        
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
        
        # Clean email addresses (Delaware doesn't have email)
        def clean_email(email_str: str) -> str:
            return None  # Delaware doesn't have email addresses
        
        # Clean addresses (Delaware doesn't have addresses)
        def clean_address(address_str: str) -> str:
            return None  # Delaware doesn't have addresses
        
        # Clean websites
        def clean_website(website_str: str) -> str:
            if pd.isna(website_str):
                return None
            
            website = str(website_str).strip()
            if website and website.lower() != 'nan':
                return website
            
            return None
        
        # Apply cleaning
        df['phone'] = df['Phone'].apply(clean_phone)
        df['email'] = df.apply(lambda x: clean_email(None), axis=1)  # Always None
        df['address'] = df.apply(lambda x: clean_address(None), axis=1)  # Always None
        df['website'] = df['Website'].apply(clean_website)
        
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
        
        # Ensure original_election_year is int64 to match Alaska's format
        # Handle NaN values by filling with 0 first, then converting
        df['original_election_year'] = df['original_election_year'].fillna(0).astype('int64')
        # Convert 0 back to None for proper handling
        df['original_election_year'] = df['original_election_year'].replace(0, None)
        
        # Ensure the column maintains int64 type even with None values
        df['original_election_year'] = df['original_election_year'].astype('Int64')
        
        # Add missing columns with None values
        required_columns = [
            'county', 'city', 'zip_code', 'filing_date', 
            'election_date', 'facebook', 'twitter', 'prefix', 'suffix', 'nickname'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Set ID columns to None (will be generated later, like Alaska)
        df['id'] = pd.NA
        df['stable_id'] = pd.NA
        
        # County information is now extracted from office names in _process_office_and_district
        # No need to set county to None since it's already populated during office processing
        
        # Map filing date
        if 'Date Filed' in df.columns:
            df['filing_date'] = df['Date Filed'].fillna('').astype(str).replace('nan', '')
        
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
                'original_office', 'party', 'district', 'county'
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
    
    def _handle_apostrophe_names(self, name: str) -> str:
        """
        Intelligently handle names with apostrophes.
        Split contracted names like "Sherae'a" but preserve Irish names like "O'Mara".
        """
        if "'" not in name:
            return name
        
        # List of Irish prefixes that should not be split
        irish_prefixes = ['O\'', 'Mc', 'Mac', 'Fitz', 'Van', 'De', 'La', 'Le']
        
        # Check if the name starts with an Irish prefix
        for prefix in irish_prefixes:
            if name.lower().startswith(prefix.lower()):
                return name  # Don't split Irish names
        
        # For other names with apostrophes, split them
        # This handles cases like "Sherae'a" -> "Sherae a"
        # Use regex to find the pattern: word + apostrophe + single character + space or end
        import re
        match = re.match(r'^(\w+)\'([aeiou])(?:\s|$)', name, re.IGNORECASE)
        if match:
            first_part = match.group(1)
            second_part = match.group(2)
            return f"{first_part} {second_part}"
        
        return name
    
    def _fix_concatenated_names(self, name: str) -> str:
        """
        Fix concatenated names like "Donald J. TrumpJD Vance" -> "Donald J. Trump".
        Detects patterns where a lowercase letter is followed immediately by uppercase letters.
        """
        if not name or len(name) < 3:
            return name
        
        # Look for patterns like "pJD" where lowercase is followed by uppercase
        # This indicates where two names got concatenated
        import re
        
        # List of common prefixes that should not be split
        common_prefixes = ['Mc', 'Mac', 'O\'', 'De', 'La', 'Le', 'Van', 'Fitz', 'Di', 'Da', 'McC', 'McD', 'McG', 'McH', 'McK', 'McL', 'McM', 'McN', 'McP', 'McQ', 'McR', 'McS', 'McT', 'McU', 'McV', 'McW', 'McX', 'McY', 'McZ']
        
        # Pattern: lowercase letter followed by uppercase letter (indicating concatenation)
        # But be more careful about common prefixes
        match = re.search(r'([a-z])([A-Z][a-z]+)', name)
        if match:
            # Check if the second part looks like a common prefix
            second_part = match.group(2)
            if any(second_part.startswith(prefix) for prefix in common_prefixes):
                # This is likely a legitimate name, not concatenation
                return name
            
            # Find the position where the concatenation occurs
            pos = match.start(1)
            # Take everything up to that position
            fixed_name = name[:pos + 1].strip()
            logger.info(f"Fixed concatenated name: '{name}' -> '{fixed_name}'")
            return fixed_name
        
        # Also look for patterns like "TrumpJD" where a word ends and another starts
        # This handles cases where there's no lowercase letter before the concatenation
        # But be more careful about this pattern
        match = re.search(r'([a-z])([A-Z][A-Za-z]+)', name)
        if match:
            second_part = match.group(2)
            if any(second_part.startswith(prefix) for prefix in common_prefixes):
                # This is likely a legitimate name, not concatenation
                return name
            
            pos = match.start(1)
            fixed_name = name[:pos + 1].strip()
            logger.info(f"Fixed concatenated name: '{name}' -> '{fixed_name}'")
            return fixed_name
        
        return name
    
    def _fix_concatenated_names_v2(self, name: str) -> str:
        """
        Fix concatenated names like "Donald J. TrumpJD Vance" -> "Donald J. Trump".
        More targeted approach to avoid cutting off legitimate names.
        """
        if not name or len(name) < 3:
            return name
        
        import re
        
        # Look for specific patterns that indicate concatenation
        # Pattern 1: "TrumpJD" where a word ends and another starts without space
        # This is the most common pattern for concatenated names
        match = re.search(r'([a-z])([A-Z][A-Za-z]+)', name)
        if match:
            # Check if this looks like a legitimate name part
            second_part = match.group(2)
            
            # If the second part is very short (1-2 chars), it's likely not concatenation
            if len(second_part) <= 2:
                return name
            
            # If the second part looks like a common name pattern, don't split
            if re.match(r'^[A-Z][a-z]+$', second_part):
                # This could be a legitimate name, be more careful
                # Only split if the pattern is very clear (like "TrumpJD")
                if len(second_part) >= 3 and not any(second_part.startswith(prefix) for prefix in ['Mc', 'Mac', 'O\'', 'De', 'La', 'Le', 'Van', 'Fitz']):
                    # This looks like concatenation
                    pos = match.start(1)
                    fixed_name = name[:pos + 1].strip()
                    logger.info(f"Fixed concatenated name: '{name}' -> '{fixed_name}'")
                    return fixed_name
        
        return name
    
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
    
    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorder columns to match Alaska's exact schema order."""
        logger.info("Reordering columns to match Alaska schema...")
        
        # Alaska's exact column order
        alaska_column_order = [
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
        
        # Only include columns that exist in the dataframe
        existing_columns = [col for col in alaska_column_order if col in df.columns]
        
        # Add any remaining columns that weren't in the Alaska order
        remaining_columns = [col for col in df.columns if col not in existing_columns]
        final_column_order = existing_columns + remaining_columns
        
        # Reorder the dataframe
        df = df[final_column_order]
        
        logger.info(f"Reordered columns to match Alaska schema. Final order: {list(df.columns)}")
        return df

def clean_delaware_candidates(input_file: str, output_file: str = None, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean Delaware candidate data.
    
    Args:
        input_file: Path to the input Excel file
        output_file: Optional path to save the cleaned data (if None, will use default naming in output_dir)
        output_dir: Directory to save cleaned data (default: "cleaned_data")
        
    Returns:
        Cleaned DataFrame
    """
    # Load the data
    logger.info(f"Loading Delaware data from {input_file}...")
    df = pd.read_excel(input_file)
    
    # Initialize cleaner with output directory
    cleaner = DelawareCleaner(output_dir=output_dir)
    
    # Clean the data
    cleaned_df = cleaner.clean_delaware_data(df)
    
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
    
    # Create a properly typed version of the CSV for verification
    # This ensures the CSV has the correct data types when read back
    typed_df = cleaned_df.copy()
    
    # Explicitly set data types to match Alaska's format
    typed_df['election_year'] = typed_df['election_year'].astype('Int64')
    typed_df['original_election_year'] = typed_df['original_election_year'].astype('Int64')
    typed_df['party'] = typed_df['party'].astype('object')
    typed_df['id'] = typed_df['id'].astype('object')
    typed_df['stable_id'] = typed_df['stable_id'].astype('object')
    
    # Save the properly typed version
    typed_df.to_csv(csv_output, index=False)
    logger.info(f"Data saved with proper types to CSV!")
    
    return cleaned_df

if __name__ == "__main__":
    # Show available input files
    print("Available Delaware input files:")
    available_files = list_available_input_files()
    if available_files:
        for i, file_path in enumerate(available_files, 1):
            print(f"  {i}. {os.path.basename(file_path)}")
    else:
        print("  No Delaware Excel files found in input directory")
        exit(1)
    
    # Example usage - use the first available file
    if available_files:
        input_file = available_files[0]  # Use the first available file
        output_dir = DEFAULT_OUTPUT_DIR
        
        print(f"\nProcessing: {os.path.basename(input_file)}")
        print(f"Output directory: {output_dir}")
        
        # Clean the data and save to the new output directory
        cleaned_data = clean_delaware_candidates(input_file, output_dir=output_dir)
        print(f"\nCleaned {len(cleaned_data)} records")
        print(f"Columns: {cleaned_data.columns.tolist()}")
        print(f"Data saved to: {output_dir}/")
    else:
        print("No input files available for processing") 