#!/usr/bin/env python3
"""
Kentucky Address Fix Example

This script demonstrates how to fix address separation issues specific to Kentucky data.
Kentucky addresses often have inconsistent separators and field ordering.
"""

import pandas as pd
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_kentucky_addresses(df: pd.DataFrame, address_column: str = 'address') -> pd.DataFrame:
    """
    Fix Kentucky address formatting issues.
    
    Common issues in Kentucky:
    - Mixed separators (commas, semicolons, pipes)
    - Inconsistent field ordering
    - Extra whitespace
    - Missing city/state/zip separations
    """
    
    if address_column not in df.columns:
        logger.warning(f"Address column '{address_column}' not found in DataFrame")
        return df
    
    logger.info(f"Fixing Kentucky addresses in column '{address_column}'")
    
    # Create a copy to avoid modifying original
    df_fixed = df.copy()
    
    # Track changes
    total_fixed = 0
    
    for idx, row in df_fixed.iterrows():
        address = row[address_column]
        
        if pd.isna(address) or not isinstance(address, str):
            continue
        
        original_address = address
        fixed_address = address
        
        # Fix 1: Standardize separators (replace semicolons and pipes with commas)
        fixed_address = re.sub(r'[;|]', ',', fixed_address)
        
        # Fix 2: Remove extra whitespace and normalize
        fixed_address = re.sub(r'\s+', ' ', fixed_address).strip()
        
        # Fix 3: Fix common Kentucky address patterns
        # Pattern: "City, KY ZIP" -> "City, KY, ZIP"
        fixed_address = re.sub(r'([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', r'\1, \2', fixed_address)
        
        # Pattern: "Street, City KY ZIP" -> "Street, City, KY, ZIP"
        fixed_address = re.sub(r'([A-Za-z\s]+),\s*([A-Za-z\s]+)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', 
                              r'\1, \2, \3, \4', fixed_address)
        
        # Fix 4: Ensure proper comma separation for city, state, zip
        # Look for patterns like "City KY 12345" and add commas
        fixed_address = re.sub(r'([A-Za-z\s]+)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', 
                              r'\1, \2, \3', fixed_address)
        
        # Fix 5: Remove double commas
        fixed_address = re.sub(r',\s*,', ',', fixed_address)
        fixed_address = re.sub(r',\s*$', '', fixed_address)  # Remove trailing comma
        
        # Update if changed
        if fixed_address != original_address:
            df_fixed.at[idx, address_column] = fixed_address
            total_fixed += 1
    
    logger.info(f"Fixed {total_fixed} Kentucky addresses")
    return df_fixed

def validate_kentucky_addresses(df: pd.DataFrame, address_column: str = 'address') -> dict:
    """
    Validate Kentucky address formatting after fixes.
    
    Returns a dictionary with validation results.
    """
    
    if address_column not in df.columns:
        return {'error': f"Address column '{address_column}' not found"}
    
    validation_results = {
        'total_addresses': 0,
        'properly_formatted': 0,
        'still_has_issues': 0,
        'issues_found': []
    }
    
    for address in df[address_column].dropna():
        if not isinstance(address, str):
            continue
            
        validation_results['total_addresses'] += 1
        
        # Check for common issues
        issues = []
        
        # Check for mixed separators
        if ',' in address and (';' in address or '|' in address):
            issues.append("Mixed separators")
        
        # Check for missing city-state-zip pattern
        if not re.search(r'[A-Z]{2},\s*\d{5}(?:-\d{4})?', address):
            issues.append("Missing proper city-state-zip format")
        
        # Check for double spaces
        if '  ' in address:
            issues.append("Double spaces")
        
        # Check for trailing commas
        if address.endswith(','):
            issues.append("Trailing comma")
        
        if issues:
            validation_results['still_has_issues'] += 1
            validation_results['issues_found'].extend(issues)
        else:
            validation_results['properly_formatted'] += 1
    
    # Remove duplicates from issues list
    validation_results['issues_found'] = list(set(validation_results['issues_found']))
    
    return validation_results

def main():
    """Example usage of Kentucky address fixing."""
    
    # Example data (you would load your actual Kentucky data)
    example_data = {
        'candidate_name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
        'address': [
            '123 Main St; Louisville KY 40202',
            '456 Oak Ave, Lexington, KY 40508',
            '789 Pine Rd|Frankfort KY 40601'
        ]
    }
    
    df = pd.DataFrame(example_data)
    
    print("=== KENTUCKY ADDRESS FIX EXAMPLE ===")
    print("\nOriginal addresses:")
    for _, row in df.iterrows():
        print(f"  {row['candidate_name']}: {row['address']}")
    
    # Fix addresses
    df_fixed = fix_kentucky_addresses(df)
    
    print("\nFixed addresses:")
    for _, row in df_fixed.iterrows():
        print(f"  {row['candidate_name']}: {row['address']}")
    
    # Validate results
    validation = validate_kentucky_addresses(df_fixed)
    
    print(f"\nValidation Results:")
    print(f"  Total addresses: {validation['total_addresses']}")
    print(f"  Properly formatted: {validation['properly_formatted']}")
    print(f"  Still has issues: {validation['still_has_issues']}")
    
    if validation['issues_found']:
        print(f"  Issues found: {', '.join(validation['issues_found'])}")

if __name__ == "__main__":
    main()
