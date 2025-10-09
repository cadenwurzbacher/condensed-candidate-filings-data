#!/usr/bin/env python3
"""
Address Cleaner for Address Parsing

This module handles final address cleaning and formatting after ZIP codes,
cities, and states have been extracted, providing clean street addresses.
"""

import re
import logging

logger = logging.getLogger(__name__)

class AddressCleaner:
    """
    Cleans and formats street addresses after component extraction.
    
    Handles:
    - Whitespace normalization
    - Comma cleanup
    - Address formatting standardization
    - Common address pattern cleaning
    """
    
    def __init__(self):
        """Initialize the address cleaner."""
    
    def clean_street_address(self, address: str) -> str:
        """
        Clean and format street address for final output.
        
        Args:
            address: Raw street address string
            
        Returns:
            Cleaned and formatted street address
        """
        if not isinstance(address, str) or not address.strip():
            return ""
        
        # Step 1: Basic cleaning
        cleaned = self._basic_cleaning(address)
        
        # Step 2: Comma cleanup
        cleaned = self._cleanup_commas(cleaned)
        
        # Step 3: Whitespace normalization
        cleaned = self._normalize_whitespace(cleaned)
        
        # Step 4: Address pattern standardization
        cleaned = self._standardize_patterns(cleaned)
        
        return cleaned
    
    def _basic_cleaning(self, address: str) -> str:
        """
        Perform basic address cleaning.
        
        Args:
            address: Raw address string
            
        Returns:
            Basic cleaned address
        """
        if not address:
            return ""
        
        # Remove leading/trailing whitespace
        cleaned = address.strip()
        
        # Remove multiple spaces
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Remove leading/trailing punctuation
        cleaned = cleaned.strip('.,;:')
        
        return cleaned
    
    def _cleanup_commas(self, address: str) -> str:
        """
        Clean up comma usage in address.
        
        Args:
            address: Address string to clean
            
        Returns:
            Address with cleaned commas
        """
        if not address:
            return ""
        
        # Remove multiple consecutive commas
        cleaned = re.sub(r',+', ',', address)
        
        # Remove leading/trailing commas
        cleaned = cleaned.strip(',')
        
        # Remove spaces around commas
        cleaned = re.sub(r'\s*,\s*', ', ', cleaned)
        
        return cleaned
    
    def _normalize_whitespace(self, address: str) -> str:
        """
        Normalize whitespace in address.
        
        Args:
            address: Address string to normalize
            
        Returns:
            Address with normalized whitespace
        """
        if not address:
            return ""
        
        # Replace multiple spaces with single space
        cleaned = re.sub(r'\s+', ' ', address)
        
        # Remove leading/trailing whitespace
        cleaned = cleaned.strip()
        
        return cleaned
    
    def _standardize_patterns(self, address: str) -> str:
        """
        Standardize common address patterns.
        
        Args:
            address: Address string to standardize
            
        Returns:
            Address with standardized patterns
        """
        if not address:
            return ""
        
        # Common address abbreviations
        abbreviations = {
            r'\bSt\b': 'Street',
            r'\bRd\b': 'Road',
            r'\bDr\b': 'Drive',
            r'\bAve\b': 'Avenue',
            r'\bBlvd\b': 'Boulevard',
            r'\bLn\b': 'Lane',
            r'\bCt\b': 'Court',
            r'\bPl\b': 'Place',
            r'\bWay\b': 'Way',
            r'\bCir\b': 'Circle',
            r'\bCrt\b': 'Court',
            r'\bHwy\b': 'Highway',
            r'\bFwy\b': 'Freeway',
            r'\bPkwy\b': 'Parkway',
            r'\bSq\b': 'Square',
            r'\bTer\b': 'Terrace',
            r'\bTrl\b': 'Trail',
            r'\bExt\b': 'Extension',
            r'\bN\b': 'North',
            r'\bS\b': 'South',
            r'\bE\b': 'East',
            r'\bW\b': 'West',
            r'\bNE\b': 'Northeast',
            r'\bNW\b': 'Northwest',
            r'\bSE\b': 'Southeast',
            r'\bSW\b': 'Southwest'
        }
        
        cleaned = address
        
        # Apply abbreviations (case-insensitive)
        for pattern, replacement in abbreviations.items():
            cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)
        
        return cleaned
    
    def format_address_for_display(self, address: str) -> str:
        """
        Format address for human-readable display.
        
        Args:
            address: Raw address string
            
        Returns:
            Formatted address string
        """
        if not address:
            return ""
        
        # Clean the address first
        cleaned = self.clean_street_address(address)
        
        # Title case for better readability
        cleaned = cleaned.title()
        
        # Ensure proper spacing around punctuation
        cleaned = re.sub(r'\s*,\s*', ', ', cleaned)
        cleaned = re.sub(r'\s*\.\s*', '. ', cleaned)
        
        return cleaned
    
    def validate_address_format(self, address: str) -> bool:
        """
        Basic validation of address format.
        
        Args:
            address: Address string to validate
            
        Returns:
            True if address format is valid, False otherwise
        """
        if not address or not isinstance(address, str):
            return False
        
        address_clean = address.strip()
        
        # Must have at least 5 characters
        if len(address_clean) < 5:
            return False
        
        # Must contain at least one letter
        if not re.search(r'[a-zA-Z]', address_clean):
            return False
        
        # Must contain at least one number (street number)
        if not re.search(r'\d', address_clean):
            return False
        
        # Must not be all numbers
        if address_clean.replace(' ', '').isdigit():
            return False
        
        return True
    
    def get_address_components(self, address: str) -> dict:
        """
        Get detailed breakdown of address components.
        
        Args:
            address: Address string to analyze
            
        Returns:
            Dictionary with address component information
        """
        if not address:
            return {}
        
        components = {
            'original': address,
            'cleaned': self.clean_street_address(address),
            'formatted': self.format_address_for_display(address),
            'length': len(address),
            'word_count': len(address.split()),
            'has_numbers': bool(re.search(r'\d', address)),
            'has_letters': bool(re.search(r'[a-zA-Z]', address)),
            'has_commas': ',' in address,
            'has_periods': '.' in address,
            'is_valid': self.validate_address_format(address)
        }
        
        return components
    
    def suggest_improvements(self, address: str) -> list[str]:
        """
        Suggest improvements for address formatting.
        
        Args:
            address: Address string to analyze
            
        Returns:
            List of improvement suggestions
        """
        suggestions = []
        
        if not address:
            return ["Address is empty"]
        
        # Check for common issues
        if len(address.strip()) < 10:
            suggestions.append("Address seems too short - may be incomplete")
        
        if not re.search(r'\d', address):
            suggestions.append("Missing street number")
        
        if not re.search(r'[a-zA-Z]', address):
            suggestions.append("Missing street name")
        
        if re.search(r'\s{2,}', address):
            suggestions.append("Multiple consecutive spaces detected")
        
        if re.search(r',{2,}', address):
            suggestions.append("Multiple consecutive commas detected")
        
        if address.count(',') > 3:
            suggestions.append("Too many commas - may have incorrect formatting")
        
        # Check for common abbreviations that could be expanded
        abbreviations = ['St', 'Rd', 'Dr', 'Ave', 'Blvd', 'Ln', 'Ct', 'Pl']
        for abbrev in abbreviations:
            if re.search(rf'\b{abbrev}\b', address, re.IGNORECASE):
                suggestions.append(f"Consider expanding '{abbrev}' to full word")
        
        return suggestions
