#!/usr/bin/env python3
"""
Main Address Parser for CandidateFilings.com Data Processing

This module extracts and refactors the complex address parsing logic from the main pipeline
into a clean, testable, and maintainable module.
"""

import pandas as pd
import re
import logging
from typing import Tuple, Optional
from .zip_extractor import ZipExtractor
from .state_extractor import StateExtractor
from .city_extractor import CityExtractor
from .address_cleaner import AddressCleaner

logger = logging.getLogger(__name__)

class AddressParser:
    """
    Comprehensive address parser for extracting ZIP codes, cities, states, and street addresses.
    
    This class handles complex address formats including:
    - "Street, City, State ZIP" format
    - "Street City, State" format (Alaska-style)
    - PO Box detection and handling
    - Multiple state code formats
    - Fallback parsing strategies
    """
    
    def __init__(self):
        """Initialize the address parser with specialized extractors."""
        self.zip_extractor = ZipExtractor()
        self.state_extractor = StateExtractor()
        self.city_extractor = CityExtractor()
        self.address_cleaner = AddressCleaner()
        
        # Common non-state abbreviations to avoid
        self.non_state_abbrevs = {
            'ST', 'RD', 'DR', 'LN', 'CT', 'BL', 'APT', 'STE', 'UNIT', 
            'PO', 'BOX', 'AVE', 'WAY', 'PL', 'CR', 'CRT', 'CIR', 'HWY', 
            'US', 'SR', 'INC', 'LLC', 'LTD', 'CORP'
        }
        
        # Alaska-specific city names for special handling
        self.alaska_cities = [
            'Anchorage', 'Ketchikan', 'Fairbanks', 'Juneau', 'Sitka', 
            'Kodiak', 'Palmer', 'Wasilla', 'Kenai', 'Soldotna'
        ]
    
    def parse_address_comprehensive(
        self, 
        addr: str, 
        existing_zip: Optional[str] = None,
        existing_city: Optional[str] = None, 
        existing_state: Optional[str] = None
    ) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
        """
        Parse address to extract ZIP, city, state and return clean street address.
        
        Args:
            addr: Raw address string
            existing_zip: Existing ZIP code (if any)
            existing_city: Existing city (if any)
            existing_state: Existing state (if any)
            
        Returns:
            Tuple of (zip_code, city, state, street_address)
        """
        if not isinstance(addr, str) or not addr.strip():
            return existing_zip, existing_city, existing_state, addr
        
        addr = addr.strip()
        original_addr = addr
        
        # Step 1: Extract ZIP code first (most reliable pattern)
        extracted_zip = self.zip_extractor.extract_zip(addr)
        if extracted_zip:
            # Remove ZIP from address for further processing
            addr = self.zip_extractor.remove_zip_from_address(addr)
        
        # Step 2: Extract state code (try multiple approaches)
        extracted_state = self.state_extractor.extract_state(addr, self.non_state_abbrevs)
        if not extracted_state:
            # Try to find state in the original address before ZIP removal
            extracted_state = self.state_extractor.extract_state(original_addr, self.non_state_abbrevs)
        
        if extracted_state:
            # Remove state from address for further processing
            addr = self.state_extractor.remove_state_from_address(addr, extracted_state)
        
        # Step 3: Extract city
        extracted_city = self.city_extractor.extract_city(addr, extracted_state)
        if extracted_city:
            # Remove city from address for further processing
            addr = self.city_extractor.remove_city_from_address(addr, extracted_city)
        
        # Step 4: Special handling for Alaska format: "Street City, State"
        if not extracted_city and extracted_state:
            extracted_city = self._extract_alaska_city(addr)
            if extracted_city:
                addr = addr.replace(extracted_city, '').strip().rstrip(',')
        
        # Step 5: Clean up the final street address
        street_address = self.address_cleaner.clean_street_address(addr)
        
        # Step 6: Use extracted values if existing ones are empty
        final_zip = extracted_zip if extracted_zip and (not existing_zip or str(existing_zip).strip() == '') else existing_zip
        final_city = extracted_city if extracted_city and (not existing_city or str(existing_city).strip() == '') else existing_city
        final_state = extracted_state if extracted_state and (not existing_state or str(existing_state).strip() == '') else existing_state
        
        return final_zip, final_city, final_state, street_address
    
    def _extract_alaska_city(self, addr: str) -> Optional[str]:
        """
        Extract Alaska city names from address strings.
        
        Args:
            addr: Address string to search
            
        Returns:
            Extracted city name or None
        """
        for city in self.alaska_cities:
            if city.lower() in addr.lower():
                return city
        return None
    
    def parse_dataframe_addresses(
        self, 
        df: pd.DataFrame,
        address_col: str = 'address',
        zip_col: str = 'zip_code',
        city_col: str = 'city',
        state_col: str = 'address_state'
    ) -> pd.DataFrame:
        """
        Parse addresses for an entire DataFrame.
        
        Args:
            df: DataFrame containing address data
            address_col: Name of the address column
            zip_col: Name of the ZIP code column
            city_col: Name of the city column
            state_col: Name of the state column
            
        Returns:
            DataFrame with parsed address components
        """
        if address_col not in df.columns:
            logger.warning(f"Address column '{address_col}' not found in DataFrame")
            return df
        
        try:
            logger.info(f"Starting comprehensive address parsing for {len(df)} records")
            
            # Apply comprehensive address parsing
            address_results = df.apply(
                lambda row: self.parse_address_comprehensive(
                    row.get(address_col),
                    row.get(zip_col),
                    row.get(city_col),
                    row.get(state_col)
                ),
                axis=1,
                result_type='expand'
            )
            
            # Update columns with parsed results
            df[zip_col] = address_results[0]
            df[city_col] = address_results[1]
            df[state_col] = address_results[2]
            df[address_col] = address_results[3]
            
            logger.info("Comprehensive address parsing completed successfully")
            
        except Exception as e:
            logger.error(f"Comprehensive address parsing failed: {e}")
            # Fallback to basic ZIP extraction
            df = self._fallback_zip_extraction(df, address_col, zip_col)
        
        return df
    
    def _fallback_zip_extraction(
        self, 
        df: pd.DataFrame,
        address_col: str = 'address',
        zip_col: str = 'zip_code'
    ) -> pd.DataFrame:
        """
        Fallback ZIP extraction when comprehensive parsing fails.
        
        Args:
            df: DataFrame to process
            address_col: Name of the address column
            zip_col: Name of the ZIP code column
            
        Returns:
            DataFrame with fallback ZIP extraction
        """
        try:
            logger.info("Applying fallback ZIP extraction")
            
            zip_pattern = re.compile(r'\b\d{5}(?:-\d{4})?\b')
            
            def extract_zip_fallback(addr, existing_zip):
                if pd.notna(existing_zip) and str(existing_zip).strip() != "":
                    return existing_zip, addr
                if isinstance(addr, str):
                    match = zip_pattern.search(addr)
                    if match:
                        zip_code = match.group(0)
                        street = zip_pattern.sub("", addr).strip().rstrip(',')
                        return zip_code, street
                return existing_zip, addr
            
            zip_results = df.apply(
                lambda row: extract_zip_fallback(row.get(address_col), row.get(zip_col)),
                axis=1,
                result_type='expand'
            )
            
            df[zip_col] = zip_results[0]
            df[address_col] = zip_results[1]
            
            logger.info("Fallback ZIP extraction completed")
            
        except Exception as e:
            logger.error(f"Fallback ZIP extraction also failed: {e}")
        
        return df
    
    def normalize_address_states(self, df: pd.DataFrame, state_col: str = 'address_state') -> pd.DataFrame:
        """
        Normalize address_state values to 2-letter USPS codes.
        
        Args:
            df: DataFrame to process
            state_col: Name of the state column
            
        Returns:
            DataFrame with normalized state codes
        """
        if state_col not in df.columns:
            return df
        
        try:
            logger.info("Normalizing address state values to USPS codes")
            
            # USPS state mappings
            state_to_usps = {
                'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
                'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
                'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
                'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
                'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
                'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
                'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
                'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
                'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
                'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
                'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
                'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
                'Wisconsin': 'WI', 'Wyoming': 'WY'
            }
            
            def normalize_state(state_val):
                if pd.isna(state_val) or not state_val:
                    return None
                
                state_str = str(state_val).strip().upper()
                
                # Already a 2-letter code
                if len(state_str) == 2 and state_str.isalpha():
                    return state_str
                
                # Full name to USPS code
                if state_str in state_to_usps:
                    return state_to_usps[state_str]
                
                # Title case for full names
                state_title = state_str.title()
                if state_title in state_to_usps:
                    return state_to_usps[state_title]
                
                return state_val
            
            # Normalize existing address_state values
            df[state_col] = df[state_col].apply(normalize_state)
            
            logger.info("Address state normalization completed")
            
        except Exception as e:
            logger.error(f"Address state normalization failed: {e}")
        
        return df
    
    def backfill_missing_states(self, df: pd.DataFrame, state_col: str = 'address_state', main_state_col: str = 'state') -> pd.DataFrame:
        """
        Backfill missing address_state values from the main state column.
        
        Args:
            df: DataFrame to process
            state_col: Name of the address state column
            main_state_col: Name of the main state column
            
        Returns:
            DataFrame with backfilled state values
        """
        if state_col not in df.columns or main_state_col not in df.columns:
            return df
        
        try:
            logger.info("Backfilling missing address state values")
            
            def fill_missing_state(row):
                current = row.get(state_col)
                if pd.notna(current) and str(current).strip() != "":
                    return current
                
                main_state = row.get(main_state_col)
                if pd.notna(main_state) and str(main_state).strip() != "":
                    # Convert main state to USPS code if needed
                    state_str = str(main_state).strip().upper()
                    if len(state_str) == 2 and state_str.isalpha():
                        return state_str
                    # Could add full name to USPS conversion here if needed
                    return main_state
                
                return current
            
            df[state_col] = df.apply(fill_missing_state, axis=1)
            
            logger.info("Address state backfilling completed")
            
        except Exception as e:
            logger.error(f"Address state backfilling failed: {e}")
        
        return df
