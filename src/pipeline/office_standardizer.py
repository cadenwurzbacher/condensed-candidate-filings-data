#!/usr/bin/env python3
"""
Office Name Standardization System

This script standardizes the 6,791 unique office names in the candidate filings data
into a manageable set of standardized categories. It uses pattern matching, fuzzy matching,
and hierarchical classification to group similar offices together.
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
    """Standardizes office names using pattern matching and classification."""
    
    def __init__(self):
        """Initialize the standardizer with office classification rules."""
        self.office_mappings = self._create_office_mappings()
        self.standard_categories = self._create_standard_categories()
        
    def _create_standard_categories(self) -> Dict[str, str]:
        """Create the main office categories with descriptions matching CandidateFilings.com."""
        return {
            'ALL_OFFICES': 'All Offices',
            'US_SENATE': 'U.S. Senate',
            'US_HOUSE': 'U.S. House',
            'GOVERNOR': 'Governor',
            'STATE_SENATE': 'State Senate',
            'STATE_HOUSE': 'State House',
            'STATE_ATTORNEY_GENERAL': 'State Attorney General',
            'STATE_TREASURER': 'State Treasurer',
            'SECRETARY_OF_STATE': 'Secretary of State',
            'MAYOR': 'Mayor',
            'CITY_COUNCIL': 'City Council',
            'COUNTY_COMMISSION': 'County Commission',
            'SCHOOL_BOARD': 'School Board',
            'OTHER_LOCAL_OFFICE': 'Other Local Office'
        }
    
    def _create_office_mappings(self) -> Dict[str, str]:
        """Create specific office name mappings matching CandidateFilings.com categories."""
        mappings = {}
        
        # Federal offices
        federal_patterns = {
            r'\bUS\s+SENATOR?\b': 'US_SENATE',
            r'\bUS\s+REPRESENTATIVE\b': 'US_HOUSE',
            r'\bUNITED\s+STATES\s+SENATOR?\b': 'US_SENATE',
            r'\bUNITED\s+STATES\s+REPRESENTATIVE\b': 'US_HOUSE',
            r'\bFEDERAL\s+SENATOR?\b': 'US_SENATE',
            r'\bFEDERAL\s+REPRESENTATIVE\b': 'US_HOUSE'
        }
        
        # State executive offices
        state_exec_patterns = {
            r'\bGOVERNOR\b': 'GOVERNOR',
            r'\bSTATE\s+ATTORNEY\s+GENERAL\b': 'STATE_ATTORNEY_GENERAL',
            r'\bATTORNEY\s+GENERAL\b': 'STATE_ATTORNEY_GENERAL',
            r'\bSECRETARY\s+OF\s+STATE\b': 'SECRETARY_OF_STATE',
            r'\bSTATE\s+TREASURER\b': 'STATE_TREASURER',
            r'\bTREASURER\b': 'STATE_TREASURER'
        }
        
        # State legislative offices
        state_leg_patterns = {
            r'\bSTATE\s+SENATOR?\b': 'STATE_SENATE',
            r'\bSTATE\s+SENATE\b': 'STATE_SENATE',
            r'\bSTATE\s+REPRESENTATIVE\b': 'STATE_HOUSE',
            r'\bSTATE\s+HOUSE\b': 'STATE_HOUSE',
            r'\bASSEMBLY\s+MEMBER\b': 'STATE_HOUSE',
            r'\bASSEMBLYMAN\b': 'STATE_HOUSE',
            r'\bASSEMBLYWOMAN\b': 'STATE_HOUSE'
        }
        
        # City offices
        city_patterns = {
            r'\bMAYOR\b': 'MAYOR',
            r'\bCITY\s+COUNCIL\s+MEMBER\b': 'CITY_COUNCIL',
            r'\bCITY\s+COUNCILMAN\b': 'CITY_COUNCIL',
            r'\bCITY\s+COUNCILWOMAN\b': 'CITY_COUNCIL',
            r'\bCITY\s+COMMISSIONER\b': 'CITY_COUNCIL',
            r'\bCITY\s+COMMISSION\b': 'CITY_COUNCIL'
        }
        
        # County offices
        county_patterns = {
            r'\bCOUNTY\s+COMMISSIONER\b': 'COUNTY_COMMISSION',
            r'\bCOUNTY\s+JUDGE\s+EXECUTIVE\b': 'COUNTY_COMMISSION',
            r'\bCOUNTY\s+EXECUTIVE\b': 'COUNTY_COMMISSION',
            r'\bCOUNTY\s+CLERK\b': 'COUNTY_COMMISSION',
            r'\bCOUNTY\s+TREASURER\b': 'COUNTY_COMMISSION',
            r'\bCOUNTY\s+AUDITOR\b': 'COUNTY_COMMISSION',
            r'\bCOUNTY\s+ATTORNEY\b': 'COUNTY_COMMISSION',
            r'\bCOUNTY\s+SHERIFF\b': 'COUNTY_COMMISSION',
            r'\bCOUNTY\s+CORONER\b': 'COUNTY_COMMISSION',
            r'\bCOUNTY\s+JAILER\b': 'COUNTY_COMMISSION',
            r'\bCOUNTY\s+SURVEYOR\b': 'COUNTY_COMMISSION',
            r'\bCOUNTY\s+ASSESSOR\b': 'COUNTY_COMMISSION',
            r'\bCOUNTY\s+COLLECTOR\b': 'COUNTY_COMMISSION',
            r'\bCOUNTY\s+RECORDER\b': 'COUNTY_COMMISSION',
            r'\bCOUNTY\s+REGISTRAR\b': 'COUNTY_COMMISSION'
        }
        
        # School board offices
        school_patterns = {
            r'\bSCHOOL\s+BOARD\s+MEMBER\b': 'SCHOOL_BOARD',
            r'\bCOUNTY\s+SCHOOL\s+BOARD\b': 'SCHOOL_BOARD',
            r'\bIND\s+SCHOOL\s+BOARD\b': 'SCHOOL_BOARD',
            r'\bINDEPENDENT\s+SCHOOL\s+BOARD\b': 'SCHOOL_BOARD',
            r'\bBOARD\s+OF\s+EDUCATION\b': 'SCHOOL_BOARD'
        }
        
        # Other local offices (catch-all for remaining local positions)
        local_patterns = {
            r'\bMAGISTRATE\b': 'OTHER_LOCAL_OFFICE',
            r'\bJUSTICE\s+OF\s+THE\s+PEACE\b': 'OTHER_LOCAL_OFFICE',
            r'\bCONSTABLE\b': 'OTHER_LOCAL_OFFICE',
            r'\bSOIL\s+CONSERVATION\s+OFFICER\b': 'OTHER_LOCAL_OFFICE',
            r'\bPROPERTY\s+VALUATION\s+ADMINISTRATOR\b': 'OTHER_LOCAL_OFFICE',
            r'\bPRECINCT\s+COMMITTEE\s+MEMBER\b': 'OTHER_LOCAL_OFFICE',
            r'\bCIRCUIT\s+JUDGE\b': 'OTHER_LOCAL_OFFICE',
            r'\bDISTRICT\s+JUDGE\b': 'OTHER_LOCAL_OFFICE',
            r'\bCIRCUIT\s+COURT\b': 'OTHER_LOCAL_OFFICE',
            r'\bDISTRICT\s+COURT\b': 'OTHER_LOCAL_OFFICE'
        }
        
        # Combine all patterns
        all_patterns = {
            **federal_patterns,
            **state_exec_patterns,
            **state_leg_patterns,
            **city_patterns,
            **county_patterns,
            **school_patterns,
            **local_patterns
        }
        
        # Create regex mappings
        for pattern, category in all_patterns.items():
            mappings[pattern] = category
        
        return mappings
    
    def standardize_office_name(self, office_name: str) -> Tuple[str, str, float]:
        """
        Standardize an office name to a category.
        
        Returns:
            Tuple of (standardized_category, confidence_score, original_name)
        """
        if pd.isna(office_name) or not str(office_name).strip():
            return 'UNKNOWN', 0.0, str(office_name)
        
        office_str = str(office_name).upper().strip()
        original_name = office_name
        
        # Try exact pattern matching first
        for pattern, category in self.office_mappings.items():
            if re.search(pattern, office_str, re.IGNORECASE):
                return category, 1.0, original_name
        
        # Try partial matching for common words
        confidence = 0.0
        best_match = 'UNKNOWN'
        
        # Check for key words that indicate office type
        if any(word in office_str for word in ['SENATOR', 'SENATE']):
            if 'STATE' in office_str or 'US' not in office_str:
                best_match = 'STATE_SENATE'
                confidence = 0.8
            else:
                best_match = 'US_SENATE'
                confidence = 0.8
        elif any(word in office_str for word in ['REPRESENTATIVE', 'HOUSE']):
            if 'STATE' in office_str or 'US' not in office_str:
                best_match = 'STATE_HOUSE'
                confidence = 0.8
            else:
                best_match = 'US_HOUSE'
                confidence = 0.8
        elif 'MAYOR' in office_str:
            best_match = 'MAYOR'
            confidence = 0.9
        elif 'GOVERNOR' in office_str:
            best_match = 'GOVERNOR'
            confidence = 0.9
        elif 'ATTORNEY' in office_str and 'GENERAL' in office_str:
            best_match = 'STATE_ATTORNEY_GENERAL'
            confidence = 0.9
        elif 'TREASURER' in office_str:
            if 'STATE' in office_str:
                best_match = 'STATE_TREASURER'
                confidence = 0.8
            else:
                best_match = 'COUNTY_COMMISSION'
                confidence = 0.8
        elif 'SECRETARY' in office_str and 'STATE' in office_str:
            best_match = 'SECRETARY_OF_STATE'
            confidence = 0.9
        elif 'COUNCIL' in office_str:
            best_match = 'CITY_COUNCIL'
            confidence = 0.8
        elif 'COMMISSIONER' in office_str:
            if 'COUNTY' in office_str:
                best_match = 'COUNTY_COMMISSION'
                confidence = 0.8
            elif 'CITY' in office_str:
                best_match = 'CITY_COUNCIL'
                confidence = 0.8
            else:
                best_match = 'COUNTY_COMMISSION'
                confidence = 0.6
        elif 'SCHOOL' in office_str and 'BOARD' in office_str:
            best_match = 'SCHOOL_BOARD'
            confidence = 0.9
        elif any(word in office_str for word in ['SHERIFF', 'CLERK', 'CORONER', 'JAILER', 'SURVEYOR', 'ASSESSOR', 'COLLECTOR', 'RECORDER', 'REGISTRAR']):
            best_match = 'COUNTY_COMMISSION'
            confidence = 0.8
        elif any(word in office_str for word in ['MAGISTRATE', 'JUSTICE OF THE PEACE', 'CONSTABLE', 'JUDGE', 'COURT']):
            best_match = 'OTHER_LOCAL_OFFICE'
            confidence = 0.7
        
        # If we still don't have a good match, try to classify by context
        if confidence < 0.5:
            if any(word in office_str for word in ['COUNTY', 'PARISH', 'BOROUGH']):
                best_match = 'COUNTY_COMMISSION'
                confidence = 0.4
            elif any(word in office_str for word in ['STATE', 'GOVERNMENT']):
                best_match = 'OTHER_LOCAL_OFFICE'
                confidence = 0.4
            elif any(word in office_str for word in ['CITY', 'TOWN', 'VILLAGE']):
                best_match = 'CITY_COUNCIL'
                confidence = 0.4
            else:
                best_match = 'OTHER_LOCAL_OFFICE'
                confidence = 0.3
        
        return best_match, confidence, original_name
    
    def standardize_dataset(self, df: pd.DataFrame, office_column: str = 'office') -> pd.DataFrame:
        """Standardize office names in the entire dataset."""
        logger.info(f"Starting office name standardization for {len(df):,} records...")
        
        if office_column not in df.columns:
            logger.error(f"Office column '{office_column}' not found in dataset")
            return df
        
        # Create new columns for standardization
        df_standardized = df.copy()
        df_standardized['office_standardized'] = None
        df_standardized['office_confidence'] = None
        df_standardized['office_category'] = None
        
        # Process each office name
        standardization_results = []
        for idx, row in df_standardized.iterrows():
            office_name = row[office_column]
            standardized, confidence, original = self.standardize_office_name(office_name)
            
            df_standardized.at[idx, 'office_standardized'] = standardized
            df_standardized.at[idx, 'office_confidence'] = confidence
            df_standardized.at[idx, 'office_category'] = self.standard_categories.get(standardized, 'Unknown')
            
            standardization_results.append({
                'original': original,
                'standardized': standardized,
                'confidence': confidence,
                'category': self.standard_categories.get(standardized, 'Unknown')
            })
        
        # Generate standardization statistics
        self._generate_standardization_stats(standardization_results)
        
        logger.info("Office name standardization completed!")
        return df_standardized
    
    def _generate_standardization_stats(self, results: List[Dict]) -> None:
        """Generate statistics about the standardization process."""
        total_records = len(results)
        successful_standardizations = sum(1 for r in results if r['confidence'] > 0.5)
        high_confidence = sum(1 for r in results if r['confidence'] > 0.8)
        medium_confidence = sum(1 for r in results if 0.5 < r['confidence'] <= 0.8)
        low_confidence = sum(1 for r in results if r['confidence'] <= 0.5)
        
        # Count by category
        category_counts = {}
        for result in results:
            category = result['standardized']
            category_counts[category] = category_counts.get(category, 0) + 1
        
        logger.info(f"Standardization Statistics:")
        logger.info(f"  Total records: {total_records:,}")
        logger.info(f"  Successfully standardized: {successful_standardizations:,} ({successful_standardizations/total_records*100:.1f}%)")
        logger.info(f"  High confidence (>80%): {high_confidence:,} ({high_confidence/total_records*100:.1f}%)")
        logger.info(f"  Medium confidence (50-80%): {medium_confidence:,} ({medium_confidence/total_records*100:.1f}%)")
        logger.info(f"  Low confidence (<50%): {low_confidence:,} ({low_confidence/total_records*100:.1f}%)")
        
        logger.info(f"  Categories created: {len(category_counts)}")
        logger.info(f"  Top 10 categories:")
        for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            logger.info(f"    {category}: {count:,} records")
    
    def create_standardization_report(self, df: pd.DataFrame, output_file: str = None) -> str:
        """Create a detailed report of the standardization process."""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"office_standardization_report_{timestamp}.txt"
        
        logger.info(f"Generating standardization report: {output_file}")
        
        with open(output_file, 'w') as f:
            f.write("OFFICE NAME STANDARDIZATION REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Records: {len(df):,}\n\n")
            
            # Summary statistics
            if 'office_standardized' in df.columns:
                f.write("STANDARDIZATION SUMMARY\n")
                f.write("-" * 30 + "\n")
                
                total_standardized = df['office_standardized'].notna().sum()
                f.write(f"Records standardized: {total_standardized:,}\n")
                
                confidence_stats = df['office_confidence'].describe()
                f.write(f"Confidence score - Mean: {confidence_stats.get('mean', 0):.3f}\n")
                f.write(f"Confidence score - Median: {confidence_stats.get('50%', 0):.3f}\n")
                f.write(f"Confidence score - Std Dev: {confidence_stats.get('std', 0):.3f}\n\n")
                
                # Category breakdown
                f.write("CATEGORY BREAKDOWN\n")
                f.write("-" * 30 + "\n")
                category_counts = df['office_standardized'].value_counts()
                for category, count in category_counts.head(20).items():
                    percentage = (count / len(df)) * 100
                    f.write(f"{category}: {count:,} records ({percentage:.1f}%)\n")
                
                f.write("\n")
                
                # Low confidence examples
                f.write("LOW CONFIDENCE EXAMPLES\n")
                f.write("-" * 30 + "\n")
                low_confidence = df[df['office_confidence'] < 0.5]
                if not low_confidence.empty:
                    f.write(f"Records with low confidence (<50%): {len(low_confidence):,}\n")
                    f.write("Sample of low confidence standardizations:\n")
                    for _, row in low_confidence.head(10).iterrows():
                        f.write(f"  '{row['office']}' → {row['office_standardized']} (confidence: {row['office_confidence']:.2f})\n")
                else:
                    f.write("No low confidence standardizations found.\n")
                
                f.write("\n")
                
                # Unknown category examples
                f.write("UNKNOWN CATEGORY EXAMPLES\n")
                f.write("-" * 30 + "\n")
                unknown_cat = df[df['office_standardized'] == 'UNKNOWN']
                if not unknown_cat.empty:
                    f.write(f"Records in UNKNOWN category: {len(unknown_cat):,}\n")
                    f.write("Sample of unknown offices:\n")
                    for _, row in unknown_cat.head(10).iterrows():
                        f.write(f"  '{row['office']}'\n")
                else:
                    f.write("No unknown category offices found.\n")
            
            f.write("\nRECOMMENDATIONS\n")
            f.write("-" * 30 + "\n")
            f.write("1. Review low confidence standardizations for accuracy\n")
            f.write("2. Add specific patterns for unknown office types\n")
            f.write("3. Consider creating subcategories for large groups\n")
            f.write("4. Validate standardization results with domain experts\n")
        
        logger.info(f"Standardization report saved: {output_file}")
        return output_file

def main():
    """Main execution function."""
    print("Office Name Standardization Tool")
    print("=" * 40)
    
    # Initialize standardizer
    standardizer = OfficeStandardizer()
    
    # Load the merged dataset
    print("Loading merged dataset...")
    try:
        df = pd.read_excel("merged_data/all_states_merged_20250814_104014_deduplicated_20250814_104204.xlsx")
        print(f"Loaded {len(df):,} records")
    except Exception as e:
        print(f"Error loading dataset: {e}")
        return
    
    # Standardize office names
    print("\nStandardizing office names...")
    df_standardized = standardizer.standardize_dataset(df)
    
    # Save standardized dataset
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"merged_data_standardized_offices_{timestamp}.xlsx"
    
    print(f"\nSaving standardized dataset to {output_file}...")
    df_standardized.to_excel(output_file, index=False)
    
    # Generate report
    report_file = standardizer.create_standardization_report(df_standardized)
    
    print(f"\n✅ Standardization completed successfully!")
    print(f"📁 Standardized dataset: {output_file}")
    print(f"📊 Report: {report_file}")
    print(f"📈 Office names reduced from 6,791 to {df_standardized['office_standardized'].nunique()} categories")

if __name__ == "__main__":
    main()
