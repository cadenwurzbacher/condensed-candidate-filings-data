#!/usr/bin/env python3
"""
Enhanced Office Standardizer for CandidateFilings.com Data Processing

This module provides comprehensive office name standardization, mapping various
raw office names to standardized categories while preserving the original
office name in a source_office column for reference and debugging.
"""

import pandas as pd
import re
import logging
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

class OfficeStandardizer:
    """
    Standardizes office names to predefined categories with safety validation.
    
    Target standardized office names:
    - US President, US House, US Senate
    - State House, State Senate, Governor
    - State Attorney General, State Treasurer, Lieutenant Governor, Secretary of State
    - City Council, City Commission, County Commission, School Board
    - Judicial: Justice of the Peace, Circuit Judge, District Judge, County Judge
    - County: Sheriff, Constable, Coroner, Surveyor, County Clerk, County Attorney
    - Special: Soil Conservation Officer, Property Valuation Administrator, Jailer
    """
    
    def __init__(self):
        """Initialize the office standardizer with comprehensive mappings."""
        self.office_mappings = self._build_office_mappings()
        self.district_patterns = self._build_district_patterns()
        
    def _build_office_mappings(self) -> Dict[str, str]:
        """
        Build comprehensive office name mappings.
        
        Returns:
            Dictionary mapping source office names to standardized names
        """
        mappings = {
            # US President variations (very specific to avoid false matches)
            'president of the united states': 'US President',
            'president of united states': 'US President',
            'u.s. president': 'US President',
            'us president': 'US President',
            'united states president': 'US President',
            'president and vice president': 'US President',
            
            # US House variations
            'u.s. representative': 'US House',
            'us representative': 'US House',
            'united states representative': 'US House',
            'u.s. house': 'US House',
            'us house': 'US House',
            'united states house': 'US House',
            'u.s. house of representatives': 'US House',
            'us house of representatives': 'US House',
            'united states house of representatives': 'US House',
            
            # US Senate variations
            'u.s. senator': 'US Senate',
            'us senator': 'US Senate',
            'united states senator': 'US Senate',
            'u.s. senate': 'US Senate',
            'us senate': 'US Senate',
            'united states senate': 'US Senate',
            
            # State House variations
            'state representative': 'State House',
            'state house': 'State House',
            'state house of representatives': 'State House',
            'house of representatives': 'State House',
            'representative': 'State House',
            'house member': 'State House',
            'state house member': 'State House',
            
            # State Senate variations
            'state senator': 'State Senate',
            'state senate': 'State Senate',
            'senator': 'State Senate',
            'senate member': 'State Senate',
            'state senate member': 'State Senate',
            
            # Governor variations
            'governor': 'Governor',
            'governor / lt. governor': 'Governor',
            'governor and lieutenant governor': 'Governor',
            
            # Lieutenant Governor variations
            'lieutenant governor': 'Lieutenant Governor',
            'lt. governor': 'Lieutenant Governor',
            'lt governor': 'Lieutenant Governor',
            
            # State Attorney General variations
            'attorney general': 'State Attorney General',
            'nc attorney general': 'State Attorney General',
            'attorney general - statewide': 'State Attorney General',
            'solicitor general - state court': 'State Attorney General',
            
            # State Treasurer variations
            'state treasurer': 'State Treasurer',
            'treasurer': 'State Treasurer',
            'state treasurer - statewide': 'State Treasurer',
            
            # Secretary of State variations
            'secretary of state': 'Secretary of State',
            'state secretary': 'Secretary of State',
            
            # City Council variations
            'city council member': 'City Council',
            'city council': 'City Council',
            'alderman': 'City Council',
            'member town council': 'City Council',
            'member city council': 'City Council',
            'council member': 'City Council',
            'city councilman': 'City Council',
            'city councilwoman': 'City Council',
            
            # Town Council variations
            'town council': 'Town Council',
            'town council member': 'Town Council',
            'member town council': 'Town Council',
            'town of chapel hill town council': 'Town Council',
            'town of indian trail council': 'Town Council',
            
            # City Commission variations
            'city commission': 'City Commission',
            'city commissioner': 'City Commission',
            'member city commission': 'City Commission',
            
            # County Commission variations
            'county commission': 'County Commission',
            'county commissioner': 'County Commission',
            'member county commission': 'County Commission',
            'county board': 'County Commission',
            'county board member': 'County Commission',
            
            # School Board variations
            'school board': 'School Board',
            'school board member': 'School Board',
            'board of education': 'School Board',
            'board member': 'School Board',
            'ind school board member': 'School Board',
            'university board of regents': 'School Board',
            
            # Judicial Office variations (enhanced)
            'justice of the peace': 'Justice of the Peace',
            'judge of the court of common pleas': 'Judge of the Court of Common Pleas',
            'judge of the orphans court': 'Judge of the Orphans Court',
            'judge of the orphans\' court': 'Judge of the Orphans Court',
            'judge of the municipal court': 'Judge of the Municipal Court',
            'judge of the circuit court': 'Circuit Judge',
            'circuit judge': 'Circuit Judge',
            'district court judge': 'District Judge',
            'district judge': 'District Judge',
            'district magistrate judge': 'District Magistrate Judge',
            'magistrate': 'Magistrate',
            'judge': 'Judge',
            
            # County Office variations (enhanced)
            'constable': 'Constable',
            'sheriff': 'Sheriff',
            'county judge executive': 'County Judge Executive',
            'county judge': 'County Judge',
            'county clerk': 'County Clerk',
            'county attorney': 'County Attorney',
            'coroner': 'Coroner',
            'surveyor': 'Surveyor',
            'jailer': 'Jailer',
            
            # Special District variations (enhanced)
            'soil conservation officer': 'Soil Conservation Officer',
            'soil and water conservation district supervisor': 'Soil Conservation Officer',
            'soil and water conservation director': 'Soil Conservation Officer',
            'soil and water district commission': 'Special District Commission',
            'property valuation administrator': 'Property Valuation Administrator',
            'levee and sanitary district': 'Special District',
            'levee district': 'Special District',
            'sanitary district': 'Special District',
            
            # Mayor variations
            'mayor': 'Mayor',
            'city mayor': 'Mayor',
            'town mayor': 'Mayor',
        }
        
        return mappings
    
    def _build_district_patterns(self) -> List[str]:
        """
        Build regex patterns for district number removal.
        
        Returns:
            List of regex patterns for district identification
        """
        patterns = [
            # District number patterns
            r'\s*-\s*\d+[a-z]*\s*district\s*$',  # " - 3rd District"
            r'\s*district\s*\d+[a-z]*\s*$',      # "District 3"
            r'\s*,\s*district\s*\d+[a-z]*\s*$',  # ", District 3"
            r'\s*\d+[a-z]*\s*district\s*$',      # "3rd District"
            
            # Parentheses patterns (enhanced)
            r'\s*\(\d+[a-z]*\)\s*$',             # "(3rd)"
            r'\s*\(\d+\)\s*$',                    # "(4)"
            r'\s*\(\d+[a-z]*\s*district\)\s*$',  # "(3rd District)"
            
            # Position and division patterns
            r'\s*position\s*\d+\s*$',            # "Position 12"
            r'\s*division\s*[a-z]-\d+\s*$',     # "Division A-3"
            r'\s*division\s*\d+\s*$',            # "Division 3"
            
            # Seat and place patterns
            r'\s*seat\s*\d+\s*$',                # "Seat 2"
            r'\s*place\s*\d+\s*$',               # "Place 1"
            r'\s*ward\s*\d+\s*$',                # "Ward 3"
            
            # County-specific patterns
            r'\s*,\s*[a-z\s]+county\s*$',       # ", Harney County"
            r'\s*\([a-z\s]+county\)\s*$',        # "(Harney County)"
        ]
        return patterns
    
    def _clean_office_name(self, office: str) -> str:
        """
        Clean office name by removing district numbers and basic cleaning.
        
        Args:
            office: Original office name
            
        Returns:
            Cleaned office name
        """
        if not office or pd.isna(office):
            return office
        
        office_str = str(office).strip()
        
        # Remove district patterns
        for pattern in self.district_patterns:
            office_str = re.sub(pattern, '', office_str, flags=re.IGNORECASE)
        
        # Remove party indicators in parentheses
        office_str = re.sub(r'\s*\([rd]\)\s*$', '', office_str, flags=re.IGNORECASE)
        office_str = re.sub(r'\s*for\s+', '', office_str, flags=re.IGNORECASE)
        
        # Remove county prefixes (enhanced)
        office_str = re.sub(r'^[a-z\s]+county\s+', '', office_str, flags=re.IGNORECASE)
        office_str = re.sub(r'^county\s+', '', office_str, flags=re.IGNORECASE)
        
        # Remove city prefixes
        office_str = re.sub(r'^city\s+of\s+', '', office_str, flags=re.IGNORECASE)
        office_str = re.sub(r'^town\s+of\s+', '', office_str, flags=re.IGNORECASE)
        
        # Clean up extra whitespace
        office_str = re.sub(r'\s+', ' ', office_str).strip()
        
        return office_str
    
    def _find_best_match(self, office: str) -> Optional[str]:
        """
        Find the best matching standardized office name.
        
        Args:
            office: Original office name
            
        Returns:
            Standardized office name or None if no match found
        """
        if not office or pd.isna(office):
            return None
        
        office_str = str(office).strip()
        
        # Try exact match first (case-insensitive)
        office_lower = office_str.lower()
        for source, target in self.office_mappings.items():
            if office_lower == source.lower():
                return target
        
        # Try exact match with original case
        if office_str in self.office_mappings:
            return self.office_mappings[office_str]
        
        # Try exact word matches (more precise, case-insensitive)
        office_words = set(office_lower.split())
        
        for source, target in self.office_mappings.items():
            source_words = set(source.lower().split())
            
            # Only match if there's significant overlap (at least 2 words)
            # AND the source is not significantly longer than the office
            if (len(office_words.intersection(source_words)) >= 2 and 
                len(source_words) <= len(office_words) + 1):
                
                # Additional safety checks for specific office types
                if self._is_safe_match(office_str, source, target):
                    return target
        
        return None
    
    def _is_safe_match(self, office: str, source: str, target: str) -> bool:
        """
        Check if a match is safe (prevents incorrect mappings).
        
        Args:
            office: Original office name
            source: Source pattern from mappings
            target: Target standardized name
            
        Returns:
            True if the match is safe
        """
        office_lower = office.lower()
        source_lower = source.lower()
        
        # Prevent judicial offices from being mapped to executive offices
        judicial_keywords = ['judge', 'justice', 'court', 'magistrate', 'orphan']
        executive_keywords = ['president', 'governor', 'mayor']
        
        if any(keyword in office_lower for keyword in judicial_keywords):
            if any(keyword in target.lower() for keyword in executive_keywords):
                return False
        
        # Prevent state offices from being mapped to federal offices
        state_keywords = ['state', 'county', 'city', 'local']
        federal_keywords = ['us ', 'united states', 'federal']
        
        if any(keyword in office_lower for keyword in state_keywords):
            if any(keyword in target.lower() for keyword in federal_keywords):
                return False
        
        # Additional safety checks
        if 'justice of the peace' in office_lower and 'president' in target.lower():
            return False
        
        if 'judge' in office_lower and 'president' in target.lower():
            return False
        
        return True
    
    def standardize_offices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize office names in the DataFrame.
        
        Args:
            df: Input DataFrame with 'office' column
            
        Returns:
            DataFrame with standardized 'office' and new 'source_office' columns
        """
        if 'office' not in df.columns:
            logger.warning("No 'office' column found, skipping office standardization")
            return df
        
        logger.info(f"Starting office standardization for {len(df):,} records...")
        
        # Make a copy to avoid modifying original
        result_df = df.copy()
        
        # Add source_office column to preserve original names
        result_df['source_office'] = result_df['office']
        
        # Track standardization results
        total_records = len(result_df)
        standardized_count = 0
        unmatched_offices = set()
        
        # Apply standardization
        for idx, row in result_df.iterrows():
            original_office = row['office']
            if pd.isna(original_office):
                continue
            
            # Clean the office name
            cleaned_office = self._clean_office_name(original_office)
            
            # Find best match
            standardized_office = self._find_best_match(cleaned_office)
            
            if standardized_office:
                result_df.at[idx, 'office'] = standardized_office
                standardized_count += 1
            else:
                # Keep original office name if no match found
                unmatched_offices.add(cleaned_office)
        
        # Log results
        logger.info(f"Office standardization completed:")
        logger.info(f"  Total records: {total_records:,}")
        logger.info(f"  Standardized: {standardized_count:,} ({standardized_count/total_records*100:.1f}%)")
        logger.info(f"  Unmatched: {len(unmatched_offices):,} unique office types")
        
        if unmatched_offices:
            logger.info("Sample unmatched offices:")
            for office in list(unmatched_offices)[:10]:
                logger.info(f"  - {office}")
        
        return result_df
    
    def get_unmatched_offices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Get summary of unmatched offices for analysis.
        
        Args:
            df: Dataframe with 'office' and 'source_office' columns
            
        Returns:
            DataFrame with unmatched office summary
        """
        if 'source_office' not in df.columns:
            logger.warning("No 'source_office' column found. Run standardize_offices first.")
            return pd.DataFrame()
        
        # Find offices that weren't standardized (source_office != office)
        unmatched = df[df['source_office'] != df['office']]
        
        if unmatched.empty:
            return pd.DataFrame()
        
        # Group by source_office and count
        unmatched_summary = unmatched.groupby('source_office').size().reset_index(name='count')
        unmatched_summary = unmatched_summary.sort_values('count', ascending=False)
        
        return unmatched_summary
