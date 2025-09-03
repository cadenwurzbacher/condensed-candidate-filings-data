"""
Unified Address Parser.

This module provides a single, unified address parsing system that handles
both state-specific pre-processing and universal address parsing logic.
"""

import logging
import pandas as pd
import re
from typing import Dict, Optional

# Import the existing working address parser
from .address_parser import AddressParser

logger = logging.getLogger(__name__)

class UnifiedAddressParser:
    """
    Unified address parser that handles both state-specific pre-processing
    and universal address parsing logic.
    
    This parser eliminates duplication by centralizing all address parsing
    while properly handling state-specific requirements.
    """
    
    def __init__(self):
        """Initialize the unified address parser."""
        # Use the existing working address parser
        self.address_parser = AddressParser()
        logger.info("Initialized UnifiedAddressParser with state-specific handling")
    
    def parse_dataframe_addresses(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse addresses in a DataFrame with state-specific pre-processing.
        
        This method applies state-specific pre-processing before using the
        universal address parsing logic.
        
        Args:
            df: DataFrame containing 'street_address' and 'state' columns
            
        Returns:
            DataFrame with parsed address columns added
        """
        if df.empty:
            logger.warning("Empty DataFrame, skipping address parsing")
            return df
        
        logger.info(f"Starting unified address parsing for {len(df)} records")
        
        # Apply state-specific pre-processing first
        df = self._apply_state_specific_preprocessing(df)
        
        # Use the existing working address parser
        if 'address' in df.columns:
            # Create a copy of the DataFrame without address_state to avoid fallback issues
            parse_df = df.copy()
            if 'address_state' in parse_df.columns:
                parse_df['address_state'] = None  # Clear address_state to force fresh extraction
            
            # Use existing parser directly with address column
            result_df = self.address_parser.parse_dataframe_addresses(
                parse_df,
                address_col='address',
                zip_col='zip_code',
                city_col='city',
                state_col='address_state'
            )
            
            # Update the original DataFrame with parsed results
            df['zip_code'] = result_df['zip_code']
            df['city'] = result_df['city']
            df['street_address'] = result_df['address']  # Use the parsed street address from result_df
            
            # Only populate address_state if there's actually a valid address
            # Check if the address field has meaningful content (not just empty/NaN)
            valid_address_mask = (
                df['address'].notna() & 
                (df['address'] != '') & 
                (df['address'].str.strip() != '') &
                (df['address'].str.len() > 3)  # Has meaningful content (more than just whitespace)
            )
            
            # Only update address_state for rows with valid addresses
            df.loc[valid_address_mask, 'address_state'] = result_df.loc[valid_address_mask, 'address_state']
            
            # Clear address_state for rows without valid addresses
            df.loc[~valid_address_mask, 'address_state'] = None
            
            logger.info(f"Updated address_state for {valid_address_mask.sum()} records with valid addresses")
            logger.info(f"Cleared address_state for {len(df) - valid_address_mask.sum()} records without valid addresses")
        
        logger.info("Unified address parsing completed successfully")
        return df
    
    def _apply_state_specific_preprocessing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply state-specific address pre-processing.
        
        This handles state-specific formatting, abbreviations, and patterns
        that need to be normalized before universal parsing.
        
        Args:
            df: DataFrame with address data
            
        Returns:
            DataFrame with state-specific pre-processing applied
        """
        if 'address' not in df.columns or 'state' not in df.columns:
            return df
        
        logger.info("Applying state-specific address pre-processing")
        
        # Apply state-specific pre-processing to each row
        for idx, row in df.iterrows():
            address = row.get('address')
            state = row.get('state')
            
            if pd.notna(address) and pd.notna(state):
                # Apply state-specific pre-processing
                processed_address = self._apply_state_specific_cleaning(address, state)
                df.at[idx, 'address'] = processed_address
        
        return df
    
    def _apply_state_specific_cleaning(self, address: str, state: str) -> str:
        """
        Apply state-specific address cleaning.
        
        Args:
            address: Address string to clean
            state: State code for state-specific logic
            
        Returns:
            Cleaned address string
        """
        if not address or not isinstance(address, str):
            return address
        
        # Normalize state code
        state_lower = str(state).lower().strip()
        
        # Apply state-specific cleaning based on state
        if state_lower in ['fl', 'florida']:
            address = self._clean_florida_address(address)
        elif state_lower in ['ak', 'alaska']:
            address = self._clean_alaska_address(address)
        elif state_lower in ['co', 'colorado']:
            address = self._clean_colorado_address(address)
        # Add more states as needed
        
        return address
    
    def _clean_florida_address(self, address: str) -> str:
        """Apply Florida-specific address cleaning."""
        # Standardize state abbreviations
        address = re.sub(r'\bFL\b', 'Florida', address)
        address = re.sub(r'\bfl\b', 'Florida', address)
        
        # Remove trailing punctuation
        address = re.sub(r'[.,;]+$', '', address)
        
        # Remove extra whitespace
        address = re.sub(r'\s+', ' ', address.strip())
        
        return address
    
    def _clean_alaska_address(self, address: str) -> str:
        """Apply Alaska-specific address cleaning."""
        # Handle common Alaska address patterns
        # (e.g., PO Box formats, rural route formats, building references)
        
        # Remove extra whitespace
        address = re.sub(r'\s+', ' ', address.strip())
        
        return address
    
    def _clean_colorado_address(self, address: str) -> str:
        """Apply Colorado-specific address cleaning."""
        # Handle common Colorado address patterns
        # (e.g., PO Box formats, rural route formats)
        
        # Remove extra whitespace
        address = re.sub(r'\s+', ' ', address.strip())
        
        return address
    
    def parse_address(self, address: str, state: Optional[str] = None) -> Dict[str, Optional[str]]:
        """
        Parse a single address with state-specific pre-processing.
        
        Args:
            address: Address string to parse
            state: State code or name for state-specific pre-processing
            
        Returns:
            Dictionary with parsed address components
        """
        if not address or not isinstance(address, str):
            return {'street': None, 'city': None, 'state': None, 'zip': None}
        
        # Apply state-specific pre-processing first
        if state:
            address = self._apply_state_specific_cleaning(address, state)
        
        # Use the existing working parser
        zip_code, city, state_code, street_address = self.address_parser.parse_address_comprehensive(
            address,
            existing_zip=None,
            existing_city=None,
            existing_state=state
        )
        
        return {
            'street': street_address,
            'city': city,
            'state': state_code,
            'zip': zip_code
        }
    
    def normalize_address_states(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize address states to USPS codes using existing logic.
        
        Args:
            df: DataFrame with parsed address data
            
        Returns:
            DataFrame with normalized state codes
        """
        if df.empty:
            return df
        
        logger.info("Normalizing address states to USPS codes")
        
        # Use the existing working logic
        if 'parsed_state' in df.columns:
            df['parsed_state'] = df['parsed_state'].apply(self._normalize_state_code)
        
        # Only normalize address_state column, NOT the main state column
        # (main state column should be full names, not abbreviations)
        if 'address_state' in df.columns:
            df['address_state'] = df['address_state'].apply(self._normalize_state_code)
        
        logger.info("Address state normalization completed")
        return df
    
    def _normalize_state_code(self, state: any) -> Optional[str]:
        """Normalize state to USPS code using existing logic."""
        if pd.isna(state) or not state:
            return None
        
        state_str = str(state).strip()
        
        # Use comprehensive state mappings for both full names and abbreviations
        state_mappings = {
            # Full names to abbreviations
            'ALASKA': 'AK',
            'ARIZONA': 'AZ',
            'ARKANSAS': 'AR',
            'CALIFORNIA': 'CA',
            'COLORADO': 'CO',
            'CONNECTICUT': 'CT',
            'DELAWARE': 'DE',
            'FLORIDA': 'FL',
            'GEORGIA': 'GA',
            'HAWAII': 'HI',
            'IDAHO': 'ID',
            'ILLINOIS': 'IL',
            'INDIANA': 'IN',
            'IOWA': 'IA',
            'KANSAS': 'KS',
            'KENTUCKY': 'KY',
            'LOUISIANA': 'LA',
            'MAINE': 'ME',
            'MARYLAND': 'MD',
            'MASSACHUSETTS': 'MA',
            'MICHIGAN': 'MI',
            'MINNESOTA': 'MN',
            'MISSISSIPPI': 'MS',
            'MISSOURI': 'MO',
            'MONTANA': 'MT',
            'NEBRASKA': 'NE',
            'NEVADA': 'NV',
            'NEW HAMPSHIRE': 'NH',
            'NEW JERSEY': 'NJ',
            'NEW MEXICO': 'NM',
            'NEW YORK': 'NY',
            'NORTH CAROLINA': 'NC',
            'NORTH DAKOTA': 'ND',
            'OHIO': 'OH',
            'OKLAHOMA': 'OK',
            'OREGON': 'OR',
            'PENNSYLVANIA': 'PA',
            'RHODE ISLAND': 'RI',
            'SOUTH CAROLINA': 'SC',
            'SOUTH DAKOTA': 'SD',
            'TENNESSEE': 'TN',
            'TEXAS': 'TX',
            'UTAH': 'UT',
            'VERMONT': 'VT',
            'VIRGINIA': 'VA',
            'WASHINGTON': 'WA',
            'WEST VIRGINIA': 'WV',
            'WISCONSIN': 'WI',
            'WYOMING': 'WY',
            # Already abbreviated states (keep as-is)
            'AK': 'AK', 'AZ': 'AZ', 'AR': 'AR', 'CA': 'CA', 'CO': 'CO',
            'CT': 'CT', 'DE': 'DE', 'FL': 'FL', 'GA': 'GA', 'HI': 'HI',
            'ID': 'ID', 'IL': 'IL', 'IN': 'IN', 'IA': 'IA', 'KS': 'KS',
            'KY': 'KY', 'LA': 'LA', 'ME': 'ME', 'MD': 'MD', 'MA': 'MA',
            'MI': 'MI', 'MN': 'MN', 'MS': 'MS', 'MO': 'MO', 'MT': 'MT',
            'NE': 'NE', 'NV': 'NV', 'NH': 'NH', 'NJ': 'NJ', 'NM': 'NM',
            'NY': 'NY', 'NC': 'NC', 'ND': 'ND', 'OH': 'OH', 'OK': 'OK',
            'OR': 'OR', 'PA': 'PA', 'RI': 'RI', 'SC': 'SC', 'SD': 'SD',
            'TN': 'TN', 'TX': 'TX', 'UT': 'UT', 'VT': 'VT', 'VA': 'VA',
            'WA': 'WA', 'WV': 'WV', 'WI': 'WI', 'WY': 'WY'
        }
        
        # Convert to uppercase for lookup
        state_upper = state_str.upper()
        return state_mappings.get(state_upper, state_str)
    
    def backfill_missing_states(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Backfill missing parsed states from main state column.
        
        Args:
            df: DataFrame with parsed address data
            
        Returns:
            DataFrame with backfilled state data
        """
        if df.empty:
            return df
        
        logger.info("Backfilling missing parsed states from main state column")
        
        # Backfill missing parsed_state from main state column
        if 'parsed_state' in df.columns and 'state' in df.columns:
            mask = df['parsed_state'].isna() & df['state'].notna()
            df.loc[mask, 'parsed_state'] = df.loc[mask, 'state']
        
        logger.info("State backfilling completed")
        return df
