#!/usr/bin/env python3
"""
Test script for smart case standardization
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.pipeline.national_standards import NationalStandards

def test_case_standardization():
    """Test the case standardization functionality"""
    
    # Initialize national standards
    ns = NationalStandards()
    
    # Test cases
    test_cases = [
        # Names
        ("JD VANCE", "JD Vance"),
        ("mary jane smith", "Mary Jane Smith"),
        ("DR. JOHN DOE", "Dr. John Doe"),
        ("JOHN SMITH III", "John Smith III"),
        
        # Offices
        ("US PRESIDENT", "US President"),
        ("JUDGE DISTRICT 1", "Judge District 1"),
        ("CIRCUIT COURT JUDGE", "Circuit Court Judge"),
        ("WARD 2 COMMISSIONER", "Ward 2 Commissioner"),
        
        # Counties
        ("JEFFERSON COUNTY", "Jefferson County"),
        ("ST. LOUIS COUNTY", "St. Louis County"),
        
        # Parties
        ("REPUBLICAN", "Republican"),
        ("DEMOCRATIC PARTY", "Democratic Party"),
        ("INDEPENDENT", "Independent"),
        
        # Cities
        ("NEW YORK CITY", "New York City"),
        ("SAN FRANCISCO", "San Francisco"),
        
        # Edge cases
        ("123 MAIN ST", "123 Main St"),
        ("A1 DISTRICT", "A1 District"),
        ("2B WARD", "2B Ward"),
        ("", ""),
        (None, None)
    ]
    
    print("=== Testing Smart Case Standardization ===\n")
    
    # Test proper case
    print("Testing Proper Case (Names, Offices, Counties, Cities):")
    print("-" * 50)
    for original, expected in test_cases:
        if original is not None:
            result = ns._smart_proper_case(original)
            status = "✅" if result == expected else "❌"
            print(f"{status} '{original}' -> '{result}' (expected: '{expected}')")
        else:
            result = ns._smart_proper_case(original)
            status = "✅" if result == expected else "❌"
            print(f"{status} None -> '{result}' (expected: None)")
    
    print("\n" + "=" * 60 + "\n")
    
    # Test title case
    print("Testing Title Case (Parties):")
    print("-" * 50)
    party_tests = [
        ("REPUBLICAN", "Republican"),
        ("DEMOCRATIC", "Democratic"),
        ("INDEPENDENT", "Independent"),
        ("LIBERTARIAN", "Libertarian"),
        ("GREEN PARTY", "Green Party")
    ]
    
    for original, expected in party_tests:
        result = ns._smart_title_case(original)
        status = "✅" if result == expected else "❌"
        print(f"{status} '{original}' -> '{result}' (expected: '{expected}')")
    
    print("\n" + "=" * 60 + "\n")
    
    # Test acronym preservation
    print("Testing Acronym Preservation:")
    print("-" * 50)
    acronym_tests = [
        ("US PRESIDENT", "US President"),
        ("US SENATE", "US Senate"),
        ("JD VANCE", "JD Vance"),
        ("MD JOHNSON", "MD Johnson"),
        ("PHD CANDIDATE", "PhD Candidate")
    ]
    
    for original, expected in acronym_tests:
        result = ns._smart_proper_case(original)
        status = "✅" if result == expected else "❌"
        print(f"{status} '{original}' -> '{result}' (expected: '{expected}')")
    
    print("\n" + "=" * 60 + "\n")
    
    # Test pattern preservation
    print("Testing Pattern Preservation:")
    print("-" * 50)
    pattern_tests = [
        ("DISTRICT 1", "District 1"),
        ("WARD 2", "Ward 2"),
        ("CIRCUIT 3", "Circuit 3"),
        ("PRECINCT 4", "Precinct 4")
    ]
    
    for original, expected in pattern_tests:
        result = ns._smart_proper_case(original)
        status = "✅" if result == expected else "❌"
        print(f"{status} '{original}' -> '{result}' (expected: '{expected}')")

if __name__ == "__main__":
    test_case_standardization()
