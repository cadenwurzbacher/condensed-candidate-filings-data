#!/usr/bin/env python3
"""
ZIP Code Extractor for Address Parsing

This module handles ZIP code extraction from address strings, including
PO Box detection and ZIP code removal for further processing.
"""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class ZipExtractor:
    """
    Extracts ZIP codes from address strings with PO Box detection.
    
    Handles:
    - Standard 5-digit ZIP codes
    - ZIP+4 format (12345-6789)
    - PO Box number detection (avoid extracting as ZIP)
    - ZIP code removal from address strings
    """
    
    def __init__(self):
        """Initialize the ZIP extractor with regex patterns."""
        # ZIP code pattern: 5 digits optionally followed by -4 digits
        self.zip_pattern = re.compile(r'\b\d{5}(?:-\d{4})?\b')
        
        # PO Box pattern to avoid false ZIP extraction
        self.po_box_pattern = re.compile(r'\bPO\s+BOX\s*', re.IGNORECASE)
    
    def extract_zip(self, address: str) -> Optional[str]:
        """
        Extract ZIP code from address string.
        
        Args:
            address: Address string to extract ZIP from
            
        Returns:
            Extracted ZIP code or None if not found
        """
        if not isinstance(address, str) or not address.strip():
            return None
        
        address = address.strip()
        
        # Find ZIP code match
        zip_match = self.zip_pattern.search(address)
        if not zip_match:
            return None
        
        zip_code = zip_match.group(0)
        
        # Check if this is part of a PO Box (don't extract PO Box numbers as ZIP)
        if self._is_po_box_zip(address, zip_code):
            return None
        
        return zip_code
    
    def remove_zip_from_address(self, address: str) -> str:
        """
        Remove ZIP code from address string for further processing.
        
        Args:
            address: Address string with ZIP code
            
        Returns:
            Address string with ZIP code removed
        """
        if not isinstance(address, str) or not address.strip():
            return address
        
        # Remove ZIP code and clean up
        cleaned_address = self.zip_pattern.sub('', address).strip().rstrip(',')
        return cleaned_address
    
    def _is_po_box_zip(self, address: str, zip_code: str) -> bool:
        """
        Check if ZIP code is part of a PO Box number.
        
        Args:
            address: Full address string
            zip_code: Extracted ZIP code to check
            
        Returns:
            True if ZIP is part of PO Box, False otherwise
        """
        # Look for PO Box pattern near the ZIP code
        po_box_match = self.po_box_pattern.search(address)
        if not po_box_match:
            return False
        
        # Check if ZIP code appears after PO Box
        po_box_end = po_box_match.end()
        zip_start = address.find(zip_code)
        
        # If ZIP appears after PO Box, check if it's actually a PO Box number
        if zip_start > po_box_end:
            # Look for a number pattern right after PO Box
            po_box_section = address[po_box_end:zip_start].strip()
            
            # If there's a number pattern (like "524") before the ZIP, 
            # then the ZIP is likely a real ZIP code, not a PO Box number
            number_pattern = re.compile(r'\d+')
            if number_pattern.search(po_box_section):
                return False  # This is a real ZIP code, not a PO Box number
            
            # If there's no number pattern, it might be a PO Box number
            chars_between = zip_start - po_box_end
            if chars_between <= 10:  # Reasonable distance for PO Box number
                return True
        
        return False
    
    def extract_zip_with_context(self, address: str) -> tuple[Optional[str], str]:
        """
        Extract ZIP code and return both ZIP and cleaned address.
        
        Args:
            address: Address string to process
            
        Returns:
            Tuple of (zip_code, cleaned_address)
        """
        zip_code = self.extract_zip(address)
        if zip_code:
            cleaned_address = self.remove_zip_from_address(address)
            return zip_code, cleaned_address
        
        return None, address
    
    def validate_zip_format(self, zip_code: str) -> bool:
        """
        Validate ZIP code format.
        
        Args:
            zip_code: ZIP code to validate
            
        Returns:
            True if valid format, False otherwise
        """
        if not zip_code:
            return False
        
        # Check if it matches ZIP pattern
        return bool(self.zip_pattern.fullmatch(zip_code))
    
    def format_zip(self, zip_code: str) -> str:
        """
        Format ZIP code to standard format.
        
        Args:
            zip_code: ZIP code to format
            
        Returns:
            Formatted ZIP code
        """
        if not zip_code:
            return ""
        
        # Remove any non-digit characters except dash
        cleaned = re.sub(r'[^\d-]', '', zip_code)
        
        # Ensure proper format: 12345 or 12345-6789
        if len(cleaned) == 5:
            return cleaned
        elif len(cleaned) == 9 and '-' in cleaned:
            return cleaned
        elif len(cleaned) == 9 and '-' not in cleaned:
            return f"{cleaned[:5]}-{cleaned[5:]}"
        else:
            return zip_code  # Return original if can't format
