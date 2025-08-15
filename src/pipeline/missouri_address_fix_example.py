#!/usr/bin/env python3
"""
Missouri Address Fix Example

This script demonstrates how to fix address separation issues specific to Missouri data.
Missouri addresses have different patterns than Kentucky and need state-specific handling.
"""

import pandas as pd
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_missouri_addresses(df: pd.DataFrame, address_column: str = 'address') -> pd.DataFrame:
    """
    Fix Missouri address formatting issues.
    
    Common issues in Missouri:
    - Different separator patterns than Kentucky
    - Rural route addresses (RR, Rural Route)
    - PO Box variations
    - County-specific formatting
    """
    
    if address_column not in df.columns:
        logger.warning(f"Address column '{address_column}' not found in DataFrame")
        return df
    
    logger.info(f"Fixing Missouri addresses in column '{address_column}'")
    
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
        
        # Fix 1: Standardize separators (Missouri uses different patterns)
        # Replace pipes and semicolons with commas
        fixed_address = re.sub(r'[;|]', ',', fixed_address)
        
        # Fix 2: Handle Missouri-specific patterns
        
        # Rural Route addresses: "RR 1 Box 123" -> "RR 1, Box 123"
        fixed_address = re.sub(r'(RR\s+\d+)\s+(Box\s+\d+)', r'\1, \2', fixed_address)
        
        # PO Box variations: "PO Box 123" -> "PO Box 123"
        fixed_address = re.sub(r'(P\.?O\.?\s+Box\s+\d+)', r'\1', fixed_address)
        
        # County addresses: "City, County, MO ZIP" -> "City, County, MO, ZIP"
        fixed_address = re.sub(r'([A-Za-z\s]+),\s*([A-Za-z\s]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', 
                              r'\1, \2, \3, \4', fixed_address)
        
        # Fix 3: Standardize Missouri ZIP code patterns
        # Missouri ZIP codes: 63xxx, 64xxx, 65xxx
        fixed_address = re.sub(r'([A-Z]{2})\s+(6[3-5]\d{3}(?:-\d{4})?)', r'\1, \2', fixed_address)
        
        # Fix 4: Handle apartment/unit numbers
        # "Apt 123" -> ", Apt 123"
        fixed_address = re.sub(r'\s+(Apt|Unit|Suite|#)\s+(\d+)', r', \1 \2', fixed_address)
        
        # Fix 5: Clean up whitespace and normalize
        fixed_address = re.sub(r'\s+', ' ', fixed_address).strip()
        
        # Fix 6: Remove double commas and trailing commas
        fixed_address = re.sub(r',\s*,', ',', fixed_address)
        fixed_address = re.sub(r',\s*$', '', fixed_address)
        
        # Update if changed
        if fixed_address != original_address:
            df_fixed.at[idx, address_column] = fixed_address
            total_fixed += 1
    
    logger.info(f"Fixed {total_fixed} Missouri addresses")
    return df_fixed

def validate_missouri_addresses(df: pd.DataFrame, address_column: str = 'address') -> dict:
    """
    Validate Missouri address formatting after fixes.
    
    Returns a dictionary with validation results.
    """
    
    if address_column not in df.columns:
        return {'error': f"Address column '{address_column}' not found"}
    
    validation_results = {
        'total_addresses': 0,
        'properly_formatted': 0,
        'still_has_issues': 0,
        'issues_found': [],
        'missouri_zip_codes': 0,
        'rural_routes': 0,
        'po_boxes': 0
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
        
        # Check for Missouri ZIP codes
        if re.search(r'6[3-5]\d{3}(?:-\d{4})?', address):
            validation_results['missouri_zip_codes'] += 1
        
        # Check for rural routes
        if re.search(r'RR\s+\d+', address, re.IGNORECASE):
            validation_results['rural_routes'] += 1
        
        # Check for PO boxes
        if re.search(r'P\.?O\.?\s+Box', address, re.IGNORECASE):
            validation_results['po_boxes'] += 1
        
        # Check for proper formatting
        if not re.search(r'[A-Z]{2},\s*\d{5}(?:-\d{4})?', address):
            issues.append("Missing proper state-zip format")
        
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
    """Example usage of Missouri address fixing."""
    
    # Example Missouri data
    example_data = {
        'candidate_name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Mary Wilson'],
        'address': [
            '123 Main St; Kansas City MO 64111',
            'RR 1 Box 123, Springfield, MO 65802',
            'PO Box 456|Jefferson City MO 65101',
            '789 Pine Rd, St. Louis County, MO 63101'
        ]
    }
    
    df = pd.DataFrame(example_data)
    
    print("=== MISSOURI ADDRESS FIX EXAMPLE ===")
    print("\nOriginal addresses:")
    for _, row in df.iterrows():
        print(f"  {row['candidate_name']}: {row['address']}")
    
    # Fix addresses
    df_fixed = fix_missouri_addresses(df)
    
    print("\nFixed addresses:")
    for _, row in df_fixed.iterrows():
        print(f"  {row['candidate_name']}: {row['address']}")
    
    # Validate results
    validation = validate_missouri_addresses(df_fixed)
    
    print(f"\nValidation Results:")
    print(f"  Total addresses: {validation['total_addresses']}")
    print(f"  Properly formatted: {validation['properly_formatted']}")
    print(f"  Still has issues: {validation['still_has_issues']}")
    print(f"  Missouri ZIP codes: {validation['missouri_zip_codes']}")
    print(f"  Rural routes: {validation['rural_routes']}")
    print(f"  PO boxes: {validation['po_boxes']}")
    
    if validation['issues_found']:
        print(f"  Issues found: {', '.join(validation['issues_found'])}")

if __name__ == "__main__":
    main()
