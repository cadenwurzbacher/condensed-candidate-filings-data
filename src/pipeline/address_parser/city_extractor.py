#!/usr/bin/env python3
"""
City Extractor for Address Parsing

This module handles city extraction from address strings after ZIP codes
and state codes have been removed, handling various address formats.
"""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class CityExtractor:
    """
    Extracts city names from address strings with intelligent format detection.
    
    Handles:
    - "Street, City" format
    - "Street, City, State" format
    - "Street City, State" format (Alaska-style)
    - Multiple comma-separated formats
    - City name extraction and removal
    """
    
    def __init__(self):
        """Initialize the city extractor."""
        pass
    
    def extract_city(self, address: str, extracted_state: Optional[str] = None) -> Optional[str]:
        """
        Extract city name from address string.
        
        Args:
            address: Address string to extract city from
            extracted_state: Previously extracted state code (if any)
            
        Returns:
            Extracted city name or None if not found
        """
        if not isinstance(address, str) or not address.strip():
            return None
        
        address = address.strip()
        
        # If no commas, try to extract city from end of address
        if ',' not in address:
            return self._extract_city_from_end(address, extracted_state)
        
        # Split by commas and clean up
        parts = [p.strip() for p in address.split(',') if p.strip()]
        
        if len(parts) < 2:
            return None
        
        # Handle different address formats
        if len(parts) == 2:
            # "Street, City" format
            return parts[1]
        
        elif len(parts) >= 3:
            # "Street, City, State" format - city is middle part
            # But first check if the last part is actually a state code
            last_part = parts[-1].strip()
            
            if extracted_state and self._is_likely_state(last_part, extracted_state):
                # Last part is state, second-to-last is city
                return parts[-2]
            elif self._is_likely_state(last_part, None):  # Check if it looks like a state
                # Last part looks like a state, second-to-last is city
                return parts[-2]
            else:
                # Last part is not state, so city is last part
                return parts[-1]
        
        return None
    
    def _extract_city_from_end(self, address: str, extracted_state: Optional[str] = None) -> Optional[str]:
        """
        Extract city name from the end of an address string that has no commas.
        
        Args:
            address: Address string to extract city from
            extracted_state: Previously extracted state code (if any)
            
        Returns:
            Extracted city name or None if not found
        """
        if not address or len(address) < 3:
            return None
        
        # Split by spaces and look for city at the end
        words = address.split()
        if len(words) < 2:
            return None
        
        # Check if the last word looks like a city name
        last_word = words[-1]
        
        # Skip if it's a number (like PO Box number)
        if last_word.isdigit():
            return None
        
        # Skip if it's a state code
        if extracted_state and last_word.upper() == extracted_state.upper():
            return None
        
        # Skip if it's a common street suffix
        street_suffixes = {'ST', 'STREET', 'RD', 'ROAD', 'DR', 'DRIVE', 'AVE', 'AVENUE', 
                          'BLVD', 'BOULEVARD', 'LN', 'LANE', 'CT', 'COURT', 'PL', 'PLACE',
                          'WAY', 'CIR', 'CIRCLE', 'HWY', 'HIGHWAY', 'PKWY', 'PARKWAY'}
        if last_word.upper() in street_suffixes:
            return None
        
        # Check if it's a valid city name (not too short, not all caps, etc.)
        if len(last_word) >= 3 and not last_word.isupper() and not last_word.isdigit():
            # This looks like a city name
            return last_word
        
        return None
    
    def remove_city_from_address(self, address: str, city_name: str) -> str:
        """
        Remove city name from address string for further processing.
        
        Args:
            address: Address string with city name
            city_name: City name to remove
            
        Returns:
            Address string with city name removed
        """
        if not isinstance(address, str) or not address.strip() or not city_name:
            return address
        
        # Remove city name and clean up
        # Handle different patterns where city might appear
        patterns_to_remove = [
            rf',\s*{re.escape(city_name)}\s*,?\s*$',  # End with comma
            rf',\s*{re.escape(city_name)}\s*$',        # End without comma
            rf'^{re.escape(city_name)}\s*,',            # Beginning with comma
            rf',\s*{re.escape(city_name)}\s*,',        # Middle with commas
            rf'\b{re.escape(city_name)}\b'              # Anywhere (fallback)
        ]
        
        cleaned_address = address
        for pattern in patterns_to_remove:
            cleaned_address = re.sub(pattern, ',', cleaned_address)
        
        # Clean up extra commas and whitespace
        cleaned_address = re.sub(r',+', ',', cleaned_address)  # Multiple commas to single
        cleaned_address = cleaned_address.strip().rstrip(',').lstrip(',')
        return cleaned_address
    
    def extract_city_with_context(self, address: str, extracted_state: Optional[str] = None) -> tuple[Optional[str], str]:
        """
        Extract city name and return both city and cleaned address.
        
        Args:
            address: Address string to process
            extracted_state: Previously extracted state code (if any)
            
        Returns:
            Tuple of (city_name, cleaned_address)
        """
        city_name = self.extract_city(address, extracted_state)
        if city_name:
            cleaned_address = self.remove_city_from_address(address, city_name)
            return city_name, cleaned_address
        
        return None, address
    
    def _is_likely_state(self, text: str, known_state: str) -> bool:
        """
        Check if text is likely a state code or name.
        
        Args:
            text: Text to check
            known_state: Known state code to compare against
            
        Returns:
            True if text is likely a state, False otherwise
        """
        if not text:
            return False
        
        text_upper = text.upper()
        
        # If we have a known state, check for direct match
        if known_state:
            known_state_upper = known_state.upper()
            if text_upper == known_state_upper:
                return True
        
        # 2-letter code pattern (most reliable indicator)
        if len(text) == 2 and text.isalpha():
            # Check if it's a valid US state code
            valid_states = {
                'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
            }
            if text_upper in valid_states:
                return True
        
        # Common state name patterns
        state_indicators = ['STATE', 'ST', 'COUNTY', 'CTY', 'DISTRICT', 'DIST']
        if any(indicator in text_upper for indicator in state_indicators):
            return True
        
        return False
    
    def detect_address_format(self, address: str) -> str:
        """
        Detect the format of an address string.
        
        Args:
            address: Address string to analyze
            
        Returns:
            String describing the detected format
        """
        if not isinstance(address, str) or not address.strip():
            return "empty"
        
        address = address.strip()
        
        if ',' not in address:
            return "single_part"
        
        parts = [p.strip() for p in address.split(',') if p.strip()]
        
        if len(parts) == 2:
            return "street_city"
        elif len(parts) == 3:
            return "street_city_state"
        elif len(parts) > 3:
            return "complex_multipart"
        else:
            return "unknown"
    
    def get_city_candidates(self, address: str) -> list[str]:
        """
        Get potential city names from address string.
        
        Args:
            address: Address string to analyze
            
        Returns:
            List of potential city names
        """
        if not isinstance(address, str) or not address.strip():
            return []
        
        address = address.strip()
        
        if ',' not in address:
            return []
        
        parts = [p.strip() for p in address.split(',') if p.strip()]
        
        if len(parts) < 2:
            return []
        
        # Return all parts except the first (street) and last (if it's a state)
        candidates = parts[1:-1] if len(parts) > 2 else parts[1:]
        
        # Filter out very short candidates (likely not cities)
        candidates = [c for c in candidates if len(c) > 2]
        
        return candidates
    
    def validate_city_name(self, city_name: str) -> bool:
        """
        Basic validation of city name.
        
        Args:
            city_name: City name to validate
            
        Returns:
            True if valid city name, False otherwise
        """
        if not city_name or not isinstance(city_name, str):
            return False
        
        city_clean = city_name.strip()
        
        # Must have at least 2 characters
        if len(city_clean) < 2:
            return False
        
        # Must contain at least one letter
        if not re.search(r'[a-zA-Z]', city_clean):
            return False
        
        # Must not be all numbers
        if city_clean.isdigit():
            return False
        
        # Must not be common non-city words
        non_city_words = {'PO', 'BOX', 'APT', 'STE', 'UNIT', 'FLOOR', 'FL', 'SUITE'}
        if city_clean.upper() in non_city_words:
            return False
        
        return True
    
    def clean_city_name(self, city_name: str) -> str:
        """
        Clean and standardize city name.
        
        Args:
            city_name: Raw city name
            
        Returns:
            Cleaned city name
        """
        if not city_name or not isinstance(city_name, str):
            return ""
        
        # Basic cleaning
        cleaned = city_name.strip()
        
        # Remove extra whitespace
        cleaned = ' '.join(cleaned.split())
        
        # Title case (but preserve existing capitalization)
        # Only change if it's all lowercase or all uppercase
        if cleaned.islower():
            cleaned = cleaned.title()
        elif cleaned.isupper():
            cleaned = cleaned.title()
        
        return cleaned
