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
        """Create comprehensive office mappings with simplified naming."""
        return {
            # Federal offices - simplified naming
            'U.S. SENATE': 'US Senate',
            'U.S. HOUSE': 'US House',
            'US SENATE': 'US Senate',
            'US HOUSE': 'US House',
            'UNITED STATES SENATE': 'US Senate',
            'UNITED STATES HOUSE': 'US House',
            'UNITED STATES REPRESENTATIVE': 'US Representative',
            'UNITED STATES SENATOR': 'US Senate',
            'MEMBER, UNITED STATES SENATE': 'US Senate',
            'MEMBER, US SENATE': 'US Senate',
            'FEDERAL SENATE': 'US Senate',
            'FEDERAL HOUSE': 'US House',
            'U.S. SENATOR': 'US Senate',
            'U.S. REPRESENTATIVE': 'US Representative',
            'US REPRESENTATIVE': 'US Representative',
            'US SENATE': 'US Senate',
            'US HOUSE': 'US House',
            'CONGRESSIONAL': 'US Representative',
            'CONGRESS': 'US Representative',
            'U.S. PRESIDENT': 'US President',
            'U.S. VICE PRESIDENT': 'US Vice President',
            'PRESIDENT OF THE UNITED STATES': 'US President',
            'PRESIDENT AND VICE PRESIDENT OF THE UNITED STATES': 'US President',
            'UNITED STATES PRESIDENT': 'US President',
            'UNITED STATES PRESIDENT / VICE PRESIDENT': 'US President',
            'PRESIDENT OF THE UNITED STATES - DEMOCRATIC PARTY': 'US President',
            'PRESIDENT OF THE UNITED STATES - REPUBLICAN PARTY': 'US President',
            'PRESIDENT AND VICE PRESIDENT': 'US President',
            'PRESIDENT/VICE PRESIDENT': 'US President',
            'PRESIDENT / VICE PRESIDENT': 'US President',
            'PRESIDENTVICE PRESIDENT': 'US President',
            'US PRESIDENT /US VICE PRESIDENT': 'US President',
            'UNITED STATES PRESIDENT / VICE PRESIDENT': 'US President',
            
            # State executive offices
            'GOVERNOR': 'Governor',
            'STATE ATTORNEY GENERAL': 'State Attorney General',
            'ATTORNEY GENERAL': 'State Attorney General',
            'STATE TREASURER': 'State Treasurer',
            'TREASURER': 'State Treasurer',
            'SECRETARY OF STATE': 'Secretary of State',
            'LIEUTENANT GOVERNOR': 'Lieutenant Governor',
            'NC LIEUTENANT GOVERNOR': 'Lieutenant Governor',
            
            # State legislative offices
            'STATE SENATE': 'State Senate',
            'STATE SENATOR': 'State Senate',
            'STATE HOUSE': 'State House',
            'STATE REPRESENTATIVE': 'State House',
            'STATE DELEGATE': 'State Delegate',
            'ASSEMBLY': 'State House',
            'ASSEMBLY MEMBER': 'State House',
            'ASSEMBLYMAN': 'State House',
            'ASSEMBLYWOMAN': 'State House',
            
            # City offices
            'MAYOR': 'Mayor',
            'MAYOR (A2)': 'Mayor',
            'CITY COUNCIL': 'City Council',
            'CITY COUNCIL MEMBER': 'City Council',
            'CITY COUNCILMAN': 'City Council',
            'CITY COUNCILWOMAN': 'City Council',
            'CITY COMMISSIONER': 'City Commissioner',
            'CITY COMMISSIONER (A)': 'City Commissioner',
            'CITY COMMISSIONER (J)': 'City Commissioner',
            'CITY COMMISSION': 'City Commission',
            
            # County offices
            'COUNTY COMMISSION': 'County Commission',
            'COUNTY COMMISSIONER': 'County Commissioner',
            'COUNTY JUDGE EXECUTIVE': 'County Judge Executive',
            'COUNTY EXECUTIVE': 'County Executive',
            'SHERIFF': 'Sheriff',
            'JAILER': 'Jailer',
            'CONSTABLE': 'Constable',
            
            # School board
            'SCHOOL BOARD': 'School Board',
            'SCHOOL BOARD MEMBER': 'School Board Member',
            'BOARD OF EDUCATION': 'School Board',
            'COUNTY SCHOOL BOARD': 'County School Board',
            'COUNTY SCHOOL BOARD MEMBER(2000)': 'County School Board Member',
            'COUNTY SCHOOL BOARD MEMBER(1998)': 'County School Board Member',
            'IND SCHOOL BOARD': 'Independent School Board',
            'INDEPENDENT SCHOOL BOARD': 'Independent School Board',
            
            # Judicial offices
            'MAGISTRATE/JUSTICE OF THE PEACE': 'Magistrate/Justice of the Peace',
            'DISTRICT COURT JUDGE': 'District Court Judge',
            'JUDGE': 'Judge',
            'JUSTICE': 'Justice'
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
    
    def _extract_district_info(self, office_name: str) -> Tuple[str, Optional[str]]:
        """Extract district information from office name and return clean office + district."""
        if pd.isna(office_name) or not str(office_name).strip():
            return str(office_name), None
        
        office_str = str(office_name).strip()
        district_info = None
        
        # IMPORTANT: Skip complex judicial offices with divisions/subdistricts FIRST
        # These should NOT extract district numbers as they have more complex structure
        if re.search(r'(?:Division|Subdistrict|Sub\.)', office_str, re.IGNORECASE):
            return office_str, None
        
        # Enhanced pattern matching for better coverage
        
        # Pattern 1: "District X" or "District X (Party)" - most general
        district_match = re.search(r'District\s+(\d+)(?:\s*\([DRILGC]\))?', office_str, re.IGNORECASE)
        if district_match:
            district_info = district_match.group(1)
            # Remove district info from office name
            office_clean = re.sub(r'District\s+\d+(?:\s*\([DRILGC]\))?', '', office_str, flags=re.IGNORECASE).strip()
            # Clean up any trailing punctuation
            office_clean = re.sub(r'[,\s-]+$', '', office_clean).strip()
            return office_clean, district_info
        
        # Pattern 2: "United States Representative, District X (Party)" - with comma
        us_rep_comma_match = re.search(r'United States Representative,\s*District\s+(\d+)(?:\s*\([DRILGC]\))?', office_str, re.IGNORECASE)
        if us_rep_comma_match:
            district_info = us_rep_comma_match.group(1)
            office_clean = 'US Representative'
            return office_clean, district_info
        
        # Pattern 3: "U.S. Representative - District X" - with dash
        us_rep_dash_match = re.search(r'U\.S\.\s*Representative\s*-\s*District\s+(\d+)', office_str, re.IGNORECASE)
        if us_rep_dash_match:
            district_info = us_rep_dash_match.group(1)
            office_clean = 'US Representative'
            return office_clean, district_info
        
        # Pattern 4: "United States Representative District X" - no separator
        us_rep_no_sep_match = re.search(r'United States Representative\s+District\s+(\d+)', office_str, re.IGNORECASE)
        if us_rep_no_sep_match:
            district_info = us_rep_no_sep_match.group(1)
            office_clean = 'US Representative'
            return office_clean, district_info
        
        # Pattern 5: "State Senate District X" or "State House District X"
        state_district_match = re.search(r'(State\s+(?:Senate|House))\s+District\s+(\d+)', office_str, re.IGNORECASE)
        if state_district_match:
            district_info = state_district_match.group(2)
            office_clean = state_district_match.group(1)
            return office_clean, district_info
        
        # Pattern 6: "City Council District X"
        city_district_match = re.search(r'(City\s+Council)\s+District\s+(\d+)', office_str, re.IGNORECASE)
        if city_district_match:
            district_info = city_district_match.group(2)
            office_clean = city_district_match.group(1)
            return office_clean, district_info
        
        # Pattern 7: "County Commission District X"
        county_district_match = re.search(r'(County\s+Commission)\s+District\s+(\d+)', office_str, re.IGNORECASE)
        if county_district_match:
            district_info = county_district_match.group(2)
            office_clean = county_district_match.group(1)
            return office_clean, district_info
        
        # Pattern 8: "Assembly District X" (for states like California)
        assembly_district_match = re.search(r'(Assembly)\s+District\s+(\d+)', office_str, re.IGNORECASE)
        if assembly_district_match:
            district_info = assembly_district_match.group(2)
            office_clean = assembly_district_match.group(1)
            return office_clean, district_info
        
        # Pattern 9: "Congressional District X" (alternative US House format)
        congressional_district_match = re.search(r'Congressional\s+District\s+(\d+)', office_str, re.IGNORECASE)
        if congressional_district_match:
            district_info = congressional_district_match.group(1)
            office_clean = 'US Representative'
            return office_clean, district_info
        
        # Pattern 10: "State District Court XX" or "State Superior Court XX" - extract court number
        state_court_match = re.search(r'(State\s+(?:District|Superior)\s+Court)\s+(\d+)', office_str, re.IGNORECASE)
        if state_court_match:
            district_info = state_court_match.group(2)
            office_clean = state_court_match.group(1)
            return office_clean, district_info
        
        # Pattern 11: "State District Court XX" or "State Superior Court XX" - alternative format
        state_court_alt_match = re.search(r'(State\s+(?:District|Superior)\s+Court)\s+(\d{2})', office_str, re.IGNORECASE)
        if state_court_alt_match:
            district_info = state_court_alt_match.group(2)
            office_clean = state_court_alt_match.group(1)
            return office_clean, district_info
        
        # Pattern 12: "100th Representative" → "State House", district: "100"
        ordinal_rep_match = re.search(r'^(\d+)(?:st|nd|rd|th)\s+(?:Representative|Rep)(?:\s+District)?(?:\s*,\s*Office\s*[AB])?', office_str, re.IGNORECASE)
        if ordinal_rep_match:
            district_info = ordinal_rep_match.group(1)
            office_clean = 'State House'
            return office_clean, district_info
        
        # Pattern 13: "100th Senator" → "State Senate", district: "100"
        ordinal_sen_match = re.search(r'^(\d+)(?:st|nd|rd|th)\s+(?:Senator|Sen)(?:\s+District)?(?:\s*,\s*Office\s*[AB])?', office_str, re.IGNORECASE)
        if ordinal_sen_match:
            district_info = ordinal_sen_match.group(1)
            office_clean = 'State Senate'
            return office_clean, district_info
        
        # No district found, return original office name
        return office_str, None
    
    def _post_process_office_name(self, office_name: str) -> str:
        """Post-process office names to clean up any remaining issues."""
        if pd.isna(office_name) or not str(office_name).strip():
            return str(office_name)
        
        office_str = str(office_name).strip()
        
        # Clean up trailing punctuation and whitespace
        office_str = re.sub(r'[,\s-]+$', '', office_str).strip()
        office_str = re.sub(r'^[,\s-]+', '', office_str).strip()
        
        # Fix common abbreviations and inconsistencies
        office_str = re.sub(r'\bU\.S\.\b', 'US', office_str)
        office_str = re.sub(r'\bUnited States\b', 'US', office_str)
        
        # Handle edge cases where office names are incomplete
        if office_str in ['United States Representative,', 'U.S. Representative -']:
            office_str = 'US Representative'
        
        # Ensure proper spacing
        office_str = re.sub(r'\s+', ' ', office_str).strip()
        
        return office_str
    
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
        """Comprehensive office standardization with district extraction."""
        logger.info(f"Starting comprehensive office standardization for {len(df):,} records...")
        
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
        district_extracted_count = 0
        
        for idx, row in df_standardized.iterrows():
            office_name = row[office_column]
            
            # Step 1: Extract district information
            office_clean, district_info = self._extract_district_info(office_name)
            
            # Step 2: Standardize office name
            standardized, confidence = self.standardize_office_name(office_clean)
            
            # Step 3: Store results
            df_standardized.at[idx, office_column] = standardized
            df_standardized.at[idx, 'office_confidence'] = confidence
            
            # Step 4: Update district column if district was found
            if district_info and 'district' in df_standardized.columns:
                df_standardized.at[idx, 'district'] = district_info
                district_extracted_count += 1
            
            # Step 5: Post-process office names for final cleanup
            final_office = self._post_process_office_name(standardized)
            df_standardized.at[idx, office_column] = final_office
            
            # Track statistics
            if confidence == 1.0:
                key_offices_count += 1
            else:
                cleaned_offices_count += 1
                
            standardization_results.append({
                'original': row['original_office'],
                'standardized': final_office,
                'confidence': confidence,
                'district_extracted': district_info
            })
        
        # Generate standardization statistics
        self._generate_standardization_stats(standardization_results, key_offices_count, cleaned_offices_count, district_extracted_count)
        
        logger.info("✅ Comprehensive office standardization completed!")
        logger.info("  • Key offices mapped to standard names")
        logger.info("  • All other offices cleaned for proper capitalization")
        logger.info(f"  • District information extracted for {district_extracted_count:,} records")
        logger.info("  • Post-processing cleanup applied")
        logger.info("  • 'original_office' preserves raw data for audit")
        
        return df_standardized
    
    def _generate_standardization_stats(self, results: List[Dict], key_offices: int, cleaned_offices: int, district_extracted: int) -> None:
        """Generate and log comprehensive standardization statistics."""
        total_records = len(results)
        
        # Count confidence levels
        high_confidence = sum(1 for r in results if r['confidence'] == 1.0)
        medium_confidence = sum(1 for r in results if r['confidence'] == 0.5)
        low_confidence = sum(1 for r in results if r['confidence'] == 0.0)
        
        # Count district extractions
        districts_found = sum(1 for r in results if r.get('district_extracted'))
        
        logger.info("📊 Comprehensive Office Standardization Statistics:")
        logger.info(f"  • Total records processed: {total_records:,}")
        logger.info(f"  • Key offices mapped (confidence 1.0): {key_offices:,} ({key_offices/total_records*100:.1f}%)")
        logger.info(f"  • Offices cleaned (confidence 0.5): {cleaned_offices:,} ({cleaned_offices/total_records*100:.1f}%)")
        logger.info(f"  • District information extracted: {district_extracted:,} ({district_extracted/total_records*100:.1f}%)")
        logger.info(f"  • High confidence: {high_confidence:,} ({high_confidence/total_records*100:.1f}%)")
        logger.info(f"  • Medium confidence: {medium_confidence:,} ({medium_confidence/total_records*100:.1f}%)")
        logger.info(f"  • Low confidence: {low_confidence:,} ({low_confidence/total_records*100:.1f}%)")
        
        # Show sample of high-confidence standardizations
        high_conf_results = [r for r in results if r['confidence'] == 1.0][:5]
        if high_conf_results:
            logger.info(f"  • Sample high-confidence mappings:")
            for result in high_conf_results:
                district_info = f" (District: {result['district_extracted']})" if result.get('district_extracted') else ""
                logger.info(f"    '{result['original']}' → '{result['standardized']}'{district_info}")
        
        # Show sample of medium-confidence cleanups
        medium_conf_results = [r for r in results if r['confidence'] == 0.5][:5]
        if medium_conf_results:
            logger.info(f"  • Sample medium-confidence cleanups:")
            for result in medium_conf_results:
                district_info = f" (District: {result['standardized']})" if result.get('district_extracted') else ""
                logger.info(f"    '{result['standardized']}'{district_info}")
        
        # Show sample of district extractions
        district_results = [r for r in results if r.get('district_extracted')][:5]
        if district_results:
            logger.info(f"  • Sample district extractions:")
            for result in district_results:
                logger.info(f"    '{result['original']}' → Office: '{result['standardized']}', District: {result['district_extracted']}")

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
