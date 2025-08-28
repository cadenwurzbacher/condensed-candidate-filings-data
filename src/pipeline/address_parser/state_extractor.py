#!/usr/bin/env python3
"""
State Code Extractor for Address Parsing

This module handles state code extraction from address strings, including
2-letter codes, full names, and filtering of non-state abbreviations.
"""

import re
import logging
from typing import Optional, Set

logger = logging.getLogger(__name__)

class StateExtractor:
    """
    Extracts state codes from address strings with intelligent filtering.
    
    Handles:
    - 2-letter state codes (AL, AK, AZ, etc.)
    - Full state names (Alabama, Alaska, Arizona, etc.)
    - Non-state abbreviation filtering (ST, RD, DR, etc.)
    - State code removal from address strings
    """
    
    def __init__(self):
        """Initialize the state extractor with regex patterns."""
        # Multiple state patterns for different address formats (in order of specificity)
        self.state_patterns = [
            # State at end with optional comma: "Street, City, AL,"
            r',\s*([A-Z]{2})\s*,?\s*$',
            # State at end: "Street, City, AL"
            r',\s*([A-Z]{2})\s*$',
            # State before ZIP: "Street, City, AL 12345"
            r',\s*([A-Z]{2})\s+\d{5}',
            # State in middle with comma: "Street, City, AL, ZIP"
            r',\s*([A-Z]{2})\s*,',
            # State followed by ZIP: "Street, City, AL 12345" (no comma)
            r',\s*([A-Z]{2})\s+\d{5}',
            # Any 2-letter code (most aggressive, filtered later)
            r'\b([A-Z]{2})\b'
        ]
        
        # Compile patterns for efficiency
        self.compiled_patterns = [re.compile(pattern) for pattern in self.state_patterns]
    
    def extract_state(self, address: str, non_state_abbrevs: Set[str]) -> Optional[str]:
        """
        Extract state code from address string.
        
        Args:
            address: Address string to extract state from
            non_state_abbrevs: Set of abbreviations that are NOT states
            
        Returns:
            Extracted state code or None if not found
        """
        if not isinstance(address, str) or not address.strip():
            return None
        
        address = address.strip()
        
        # Try each pattern in order of specificity
        for pattern in self.compiled_patterns:
            state_match = pattern.search(address)
            if state_match:
                state_code = state_match.group(1)
                
                # Filter out common non-state abbreviations
                if state_code not in non_state_abbrevs:
                    # Additional validation: ensure it's a valid US state code
                    if self.validate_state_code(state_code):
                        return state_code
        
        return None
    
    def remove_state_from_address(self, address: str, state_code: str) -> str:
        """
        Remove state code from address string for further processing.
        
        Args:
            address: Address string with state code
            state_code: State code to remove
            
        Returns:
            Address string with state code removed
        """
        if not isinstance(address, str) or not address.strip():
            return address
        
        # Remove state code and clean up
        # Handle different patterns where state might appear
        patterns_to_remove = [
            rf',\s*{re.escape(state_code)}\s*,?\s*$',  # End with comma
            rf',\s*{re.escape(state_code)}\s*$',        # End without comma
            rf'\s+{re.escape(state_code)}\s+\d{{5}}',   # Before ZIP
            rf'\b{re.escape(state_code)}\b'             # Anywhere
        ]
        
        cleaned_address = address
        for pattern in patterns_to_remove:
            cleaned_address = re.sub(pattern, '', cleaned_address)
        
        # Clean up extra whitespace and commas
        cleaned_address = cleaned_address.strip().rstrip(',')
        return cleaned_address
    
    def extract_state_with_context(self, address: str, non_state_abbrevs: Set[str]) -> tuple[Optional[str], str]:
        """
        Extract state code and return both state and cleaned address.
        
        Args:
            address: Address string to process
            non_state_abbrevs: Set of abbreviations that are NOT states
            
        Returns:
            Tuple of (state_code, cleaned_address)
        """
        state_code = self.extract_state(address, non_state_abbrevs)
        if state_code:
            cleaned_address = self.remove_state_from_address(address, state_code)
            return state_code, cleaned_address
        
        return None, address
    
    def validate_state_code(self, state_code: str) -> bool:
        """
        Validate if a 2-letter code is a valid US state code.
        
        Args:
            state_code: 2-letter code to validate
            
        Returns:
            True if valid US state code, False otherwise
        """
        if not state_code or len(state_code) != 2:
            return False
        
        valid_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
        }
        
        return state_code.upper() in valid_states
    
    def get_state_name(self, state_code: str) -> Optional[str]:
        """
        Get full state name from 2-letter code.
        
        Args:
            state_code: 2-letter state code
            
        Returns:
            Full state name or None if invalid
        """
        state_names = {
            'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
            'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
            'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
            'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
            'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
            'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
            'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
            'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
            'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
            'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
            'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
            'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
            'WI': 'Wisconsin', 'WY': 'Wyoming'
        }
        
        return state_names.get(state_code.upper())
    
    def get_state_code(self, state_name: str) -> Optional[str]:
        """
        Get 2-letter state code from full state name.
        
        Args:
            state_name: Full state name
            
        Returns:
            2-letter state code or None if not found
        """
        state_codes = {
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
        
        # Try exact match first
        if state_name in state_codes:
            return state_codes[state_name]
        
        # Try case-insensitive match
        for name, code in state_codes.items():
            if name.lower() == state_name.lower():
                return code
        
        return None
