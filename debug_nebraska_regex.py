#!/usr/bin/env python3
"""
Debug script to test Nebraska address cleaning regex patterns
"""

import re

def test_nebraska_regex():
    """Test the regex patterns for cleaning Nebraska addresses."""
    
    print("=== NEBRASKA REGEX DEBUG ===")
    
    # Test addresses that have multiple street addresses
    test_addresses = [
        '8542 S. 160th Ave. 18406 Pasadena Ave.',
        '2005 12th St. 1126 Ave. A',
        '2910 Pinnacle Dr. P.O. Box 81041',
        '713 Caniglia Plz. P.O. Box 746'
    ]
    
    for test_addr in test_addresses:
        print(f"\nTest address: {test_addr}")
        
        # Test the complex regex pattern
        pattern1 = r'\d+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:St|Ave|Dr|Rd|Blvd|Ln|Pl|Ct|Way|Hwy|Circle|Cir|Terrace|Ter)\.?\s+\d+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:St|Ave|Dr|Rd|Blvd|Ln|Pl|Ct|Way|Hwy|Circle|Cir|Terrace|Ter)\.?'
        match1 = re.search(pattern1, test_addr, re.IGNORECASE)
        print(f"  Complex pattern match: {bool(match1)}")
        
        # Test the simpler pattern
        pattern2 = r'\d+\s+[A-Z][a-z]+.*\s+\d+\s+[A-Z][a-z]+'
        match2 = re.search(pattern2, test_addr, re.IGNORECASE)
        print(f"  Simple pattern match: {bool(match2)}")
        
        # Test the word-based approach
        parts = test_addr.split()
        print(f"  Parts: {parts}")
        
        for i, part in enumerate(parts):
            is_digit = part.isdigit()
            starts_upper = part[0].isupper() if part else False
            print(f"    Part {i}: '{part}' (isdigit: {is_digit}, starts with upper: {starts_upper})")
        
        # Test the simple logic
        second_number_idx = None
        for i, part in enumerate(parts):
            if part.isdigit() and i > 0:  # Found a number that's not the first word
                if i + 1 < len(parts) and parts[i + 1][0].isupper():
                    second_number_idx = i
                    break
        
        if second_number_idx:
            cleaned = ' '.join(parts[:second_number_idx]).strip()
            print(f"  Simple logic result: '{cleaned}'")
        else:
            print(f"  Simple logic result: No second address found")

if __name__ == "__main__":
    test_nebraska_regex()
