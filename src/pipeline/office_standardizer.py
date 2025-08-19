#!/usr/bin/env python3
"""
Simple Office Name Standardization System

This script standardizes office names using a simple approach:
1. Maps only the 12 key offices specified by the user
2. Cleans up capitalization for everything else (no more ALL CAPS)
3. Everything else remains as-is for filtering purposes
"""

import pandas as pd
import re
import logging
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import json
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OfficeStandardizer:
    """Simple office standardizer that maps key offices and cleans capitalization."""
    
    def __init__(self):
        """Initialize the standardizer with the 12 key office mappings."""
        self.key_office_mappings = self._create_key_office_mappings()
        
    def _create_key_office_mappings(self) -> Dict[str, str]:
        """Create mappings for only the 12 key offices specified by the user."""
        return {
            # Federal offices
            'U.S. SENATE': 'U.S. Senate',
            'U.S. HOUSE': 'U.S. House',
            'US SENATE': 'U.S. Senate',
            'US HOUSE': 'U.S. House',
            'UNITED STATES SENATE': 'U.S. Senate',
            'UNITED STATES HOUSE': 'U.S. House',
            'UNITED STATES REPRESENTATIVE': 'U.S. House',
            'UNITED STATES SENATOR': 'U.S. Senate',
            'FEDERAL SENATE': 'U.S. Senate',
            'FEDERAL HOUSE': 'U.S. House',
            
            # State executive offices
            'GOVERNOR': 'Governor',
            'STATE ATTORNEY GENERAL': 'State Attorney General',
            'ATTORNEY GENERAL': 'State Attorney General',
            'STATE TREASURER': 'State Treasurer',
            'TREASURER': 'State Treasurer',
            'SECRETARY OF STATE': 'Secretary of State',
            
            # State legislative offices
            'STATE SENATE': 'State Senate',
            'STATE SENATOR': 'State Senate',
            'STATE HOUSE': 'State House',
            'STATE REPRESENTATIVE': 'State House',
            'ASSEMBLY': 'State House',
            'ASSEMBLY MEMBER': 'State House',
            'ASSEMBLYMAN': 'State House',
            'ASSEMBLYWOMAN': 'State House',
            
            # City offices
            'MAYOR': 'Mayor',
            'CITY COUNCIL': 'City Council',
            'CITY COUNCIL MEMBER': 'City Council',
            'CITY COUNCILMAN': 'City Council',
            'CITY COUNCILWOMAN': 'City Council',
            'CITY COMMISSIONER': 'City Council',
            'CITY COMMISSION': 'City Council',
            
            # County offices
            'COUNTY COMMISSION': 'County Commission',
            'COUNTY COMMISSIONER': 'County Commission',
            'COUNTY JUDGE EXECUTIVE': 'County Commission',
            'COUNTY EXECUTIVE': 'County Commission',
            
            # School board
            'SCHOOL BOARD': 'School Board',
            'SCHOOL BOARD MEMBER': 'School Board',
            'BOARD OF EDUCATION': 'School Board',
            'COUNTY SCHOOL BOARD': 'School Board',
            'IND SCHOOL BOARD': 'School Board',
            'INDEPENDENT SCHOOL BOARD': 'School Board'
        }
    
    def clean_capitalization(self, office_name: str) -> str:
        """Clean up capitalization - convert from ALL CAPS to Title Case."""
        if pd.isna(office_name) or not str(office_name).strip():
            return str(office_name)
        
        office_str = str(office_name).strip()
        
        # If it's already in mixed case, leave it as-is
        if not office_str.isupper():
            return office_str
        
        # Convert from ALL CAPS to Title Case
        # Handle special cases like "U.S." and "Jr."
        words = office_str.split()
        cleaned_words = []
        
        for word in words:
            # Handle abbreviations and special cases
            if word in ['U.S.', 'U.S.A.', 'JR.', 'SR.', 'II', 'III', 'IV']:
                cleaned_words.append(word)
            elif '.' in word:  # Abbreviations with periods
                cleaned_words.append(word.title())
            else:
                cleaned_words.append(word.title())
        
        return ' '.join(cleaned_words)
    
    def standardize_office_name(self, office_name: str) -> Tuple[str, float]:
        """
        Standardize an office name using the simple approach.
        
        Returns:
            Tuple of (standardized_name, confidence_score)
        """
        if pd.isna(office_name) or not str(office_name).strip():
            return str(office_name), 0.0
        
        office_str = str(office_name).strip()
        
        # First, check if this office matches one of our key mappings
        for key, standard in self.key_office_mappings.items():
            if office_str.upper() == key.upper():
                return standard, 1.0
        
        # If no key mapping found, just clean up capitalization
        cleaned_name = self.clean_capitalization(office_str)
        return cleaned_name, 0.5  # Lower confidence since it's just cleaned, not standardized
    
    def standardize_dataset(self, df: pd.DataFrame, office_column: str = 'office') -> pd.DataFrame:
        """Standardize office names in the entire dataset using the simple approach."""
        logger.info(f"Starting simple office name standardization for {len(df):,} records...")
        
        if office_column not in df.columns:
            logger.error(f"Office column '{office_column}' not found in dataset")
            return df
        
        # Create a copy to avoid modifying the original
        df_standardized = df.copy()
        
        # Ensure we have original_office column for audit trail
        if 'original_office' not in df_standardized.columns:
            df_standardized['original_office'] = df_standardized[office_column]
            logger.info("Created original_office column from existing office data")
        
        # Initialize the confidence column
        df_standardized['office_confidence'] = None
        
        # Process each office name
        standardization_results = []
        key_offices_count = 0
        cleaned_offices_count = 0
        
        for idx, row in df_standardized.iterrows():
            office_name = row[office_column]
            standardized, confidence = self.standardize_office_name(office_name)
            
            # Store the standardized result directly in the main office field
            df_standardized.at[idx, office_column] = standardized
            df_standardized.at[idx, 'office_confidence'] = confidence
            
            # Track statistics
            if confidence == 1.0:
                key_offices_count += 1
            else:
                cleaned_offices_count += 1
                
            standardization_results.append({
                'original': row['original_office'],
                'standardized': standardized,
                'confidence': confidence
            })
        
        # Generate standardization statistics
        self._generate_standardization_stats(standardization_results, key_offices_count, cleaned_offices_count)
        
        logger.info("✅ Simple office name standardization completed!")
        logger.info("  • Key offices mapped to standard names")
        logger.info("  • All other offices cleaned for proper capitalization")
        logger.info("  • 'original_office' preserves raw data for audit")
        
        return df_standardized
    
    def _generate_standardization_stats(self, results: List[Dict], key_offices: int, cleaned_offices: int) -> None:
        """Generate and log standardization statistics."""
        total_records = len(results)
        
        # Count confidence levels
        high_confidence = sum(1 for r in results if r['confidence'] == 1.0)
        medium_confidence = sum(1 for r in results if r['confidence'] == 0.5)
        low_confidence = sum(1 for r in results if r['confidence'] == 0.0)
        
        logger.info("Standardization Statistics:")
        logger.info(f"  Total records: {total_records:,}")
        logger.info(f"  Key offices mapped: {key_offices:,} ({key_offices/total_records*100:.1f}%)")
        logger.info(f"  Offices cleaned (capitalization): {cleaned_offices:,} ({cleaned_offices/total_records*100:.1f}%)")
        logger.info(f"  High confidence (mapped): {high_confidence:,} ({high_confidence/total_records*100:.1f}%)")
        logger.info(f"  Medium confidence (cleaned): {medium_confidence:,} ({medium_confidence/total_records*100:.1f}%)")
        logger.info(f"  Low confidence (unchanged): {low_confidence:,} ({low_confidence/total_records*100:.1f}%)")
        
        # Show some examples of key office mappings
        key_office_examples = [r for r in results if r['confidence'] == 1.0][:5]
        if key_office_examples:
            logger.info("  Key office mapping examples:")
            for example in key_office_examples:
                logger.info(f"    '{example['original']}' → '{example['standardized']}'")
        
        # Show some examples of cleaned offices
        cleaned_examples = [r for r in results if r['confidence'] == 0.5][:5]
        if cleaned_examples:
            logger.info("  Capitalization cleaning examples:")
            for example in cleaned_examples:
                logger.info(f"    '{example['original']}' → '{example['standardized']}'")

def main():
    """Main function for testing the office standardizer."""
    # Example usage
    standardizer = OfficeStandardizer()
    
    # Test some office names
    test_offices = [
        "U.S. SENATE",
        "GOVERNOR", 
        "STATE SENATE",
        "MAYOR",
        "CITY COUNCIL MEMBER",
        "COUNTY COMMISSIONER",
        "SCHOOL BOARD",
        "SOME RANDOM OFFICE",
        "ANOTHER ALL CAPS OFFICE"
    ]
    
    print("Testing office standardization:")
    for office in test_offices:
        standardized, confidence = standardizer.standardize_office_name(office)
        print(f"  '{office}' → '{standardized}' (confidence: {confidence})")

if __name__ == "__main__":
    main()
