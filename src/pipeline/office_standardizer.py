#!/usr/bin/env python3
"""
Office Standardizer

This module handles the standardization of office names across all states.
It maps various office names to standardized categories and preserves the original
office name in a source_office column for reference and debugging.
"""

import re
import pandas as pd
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class OfficeStandardizer:
    """
    Standardizes office names to predefined categories while preserving original names.
    """
    
    def __init__(self):
        """Initialize the office standardizer with mapping rules."""
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
            'house of representatives': 'US House',
            'house representative': 'US House',
            'u.s. house': 'US House',
            'us house': 'US House',
            'united states house': 'US House',
            'congress': 'US House',
            'congressional': 'US House',
            'u.s. representative': 'US House',
            'us representative': 'US House',
            'united states representative': 'US House',
            
            # US Senate variations
            'senate': 'US Senate',
            'u.s. senate': 'US Senate',
            'us senate': 'US Senate',
            'united states senate': 'US Senate',
            'u.s. senator': 'US Senate',
            'us senator': 'US Senate',
            'united states senator': 'US Senate',
            
            # State House variations
            'state house': 'State House',
            'state representative': 'State House',
            'state house of representatives': 'State House',
            'house of delegates': 'State House',
            'assembly': 'State House',
            'general assembly': 'State House',
            'house of representatives': 'State House',
            'representative': 'State House',
            'delegate': 'State House',
            
            # State Senate variations
            'state senate': 'State Senate',
            'state senator': 'State Senate',
            'senator': 'State Senate',
            
            # Governor variations
            'governor': 'Governor',
            'state governor': 'Governor',
            
            # State Attorney General variations
            'attorney general': 'State Attorney General',
            'state attorney general': 'State Attorney General',
            'ag': 'State Attorney General',
            
            # State Treasurer variations
            'treasurer': 'State Treasurer',
            'state treasurer': 'State Treasurer',
            
            # Lieutenant Governor variations
            'lieutenant governor': 'Lieutenant Governor',
            'lt. governor': 'Lieutenant Governor',
            'lt governor': 'Lieutenant Governor',
            
            # Secretary of State variations
            'secretary of state': 'Secretary of State',
            'state secretary': 'Secretary of State',
            
            # City Council variations
            'city council': 'City Council',
            'city council member': 'City Council',
            'council member': 'City Council',
            'councilman': 'City Council',
            'councilwoman': 'City Council',
            'alderman': 'City Council',
            'alderwoman': 'City Council',
            
            # City Commission variations
            'city commission': 'City Commission',
            'city commissioner': 'City Commission',
            'commissioner': 'City Commission',
            
            # County Commission variations
            'county commission': 'County Commission',
            'county commissioner': 'County Commission',
            'county board': 'County Commission',
            'county board member': 'County Commission',
            
            # School Board variations
            'school board': 'School Board',
            'school board member': 'School Board',
            'board of education': 'School Board',
            'board member': 'School Board',
            
            # Judicial Office variations
            'justice of the peace': 'Justice of the Peace',
            'judge of the court of common pleas': 'Judge of the Court of Common Pleas',
            'judge of the orphans court': 'Judge of the Orphans Court',
            'judge of the orphans\' court': 'Judge of the Orphans Court',
            'judge of the municipal court': 'Judge of the Municipal Court',
            'judge of the circuit court': 'Judge of the Circuit Court',
            'magistrate': 'Magistrate',
            'judge': 'Judge',
            
            # Other common offices
            'constable': 'Constable',
            'sheriff': 'Sheriff',
            'mayor': 'Mayor',
            'county judge executive': 'County Judge Executive',
        }
        
        return mappings
    
    def _build_district_patterns(self) -> List[Tuple[str, str]]:
        """
        Build patterns to remove district numbers from office names.
        
        Returns:
            List of (pattern, replacement) tuples
        """
        patterns = [
            # Remove district numbers and related text
            (r'\s*district\s*\d+', ''),
            (r'\s*#\s*\d+', ''),
            (r'\s*number\s*\d+', ''),
            (r'\s*seat\s*\d+', ''),
            (r'\s*position\s*\d+', ''),
            (r'\s*place\s*\d+', ''),
            (r'\s*ward\s*\d+', ''),
            (r'\s*precinct\s*\d+', ''),
            (r'\s*area\s*\d+', ''),
            (r'\s*zone\s*\d+', ''),
            (r'\s*region\s*\d+', ''),
            (r'\s*division\s*\d+', ''),
            (r'\s*section\s*\d+', ''),
            (r'\s*part\s*\d+', ''),
            (r'\s*portion\s*\d+', ''),
            
            # Remove specific district formats
            (r'\s*1st\s*district', ''),
            (r'\s*2nd\s*district', ''),
            (r'\s*3rd\s*district', ''),
            (r'\s*\d+th\s*district', ''),
            (r'\s*first\s*district', ''),
            (r'\s*second\s*district', ''),
            (r'\s*third\s*district', ''),
            (r'\s*\d+st\s*district', ''),
            (r'\s*\d+nd\s*district', ''),
            (r'\s*\d+rd\s*district', ''),
            
            # Clean up extra whitespace
            (r'\s+', ' '),
            (r'^\s+|\s+$', ''),
        ]
        
        return patterns
    
    def _clean_office_name(self, office: str) -> str:
        """
        Clean office name by removing district numbers and standardizing format.
        
        Args:
            office: Raw office name
            
        Returns:
            Cleaned office name without district numbers
        """
        if pd.isna(office) or not isinstance(office, str):
            return office
            
        cleaned = office.lower().strip()
        
        # Apply district removal patterns
        for pattern, replacement in self.district_patterns:
            cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)
        
        # Clean up extra whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    
    def _find_best_match(self, office: str) -> Optional[str]:
        """
        Find the best matching standardized office name.
        
        Args:
            office: Cleaned office name
            
        Returns:
            Standardized office name or None if no match found
        """
        if not office:
            return None
            
        # Try exact match first
        if office in self.office_mappings:
            return self.office_mappings[office]
        
        # Try exact word matches (more precise)
        office_words = set(office.lower().split())
        
        for source, target in self.office_mappings.items():
            source_words = set(source.lower().split())
            
            # Only match if there's significant overlap (at least 2 words)
            # AND the source is not significantly longer than the office
            if (len(office_words.intersection(source_words)) >= 2 and 
                len(source_words) <= len(office_words) + 1):
                
                # Additional safety checks for specific office types
                if self._is_safe_match(office, source, target):
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
        Standardize office names in the dataframe.
        
        Args:
            df: Input dataframe with 'office' column
            
        Returns:
            Dataframe with standardized 'office' and new 'source_office' columns
        """
        if 'office' not in df.columns:
            logger.warning("No 'office' column found in dataframe")
            return df
        
        # Create a copy to avoid modifying the original
        result_df = df.copy()
        
        # Add source_office column to preserve original names
        result_df['source_office'] = result_df['office']
        
        # Standardize office names
        standardized_offices = []
        match_counts = {}
        
        for idx, office in enumerate(result_df['office']):
            if pd.isna(office):
                standardized_offices.append(office)
                continue
                
            # Clean the office name
            cleaned_office = self._clean_office_name(office)
            
            # Find best match
            standardized = self._find_best_match(cleaned_office)
            
            if standardized:
                standardized_offices.append(standardized)
                match_counts[standardized] = match_counts.get(standardized, 0) + 1
            else:
                # Keep original if no match found
                standardized_offices.append(office)
        
        # Update the office column
        result_df['office'] = standardized_offices
        
        # Log standardization results
        total_records = len(result_df)
        matched_records = sum(match_counts.values())
        match_rate = (matched_records / total_records) * 100 if total_records > 0 else 0
        
        logger.info(f"Office standardization completed:")
        logger.info(f"  Total records: {total_records:,}")
        logger.info(f"  Matched records: {matched_records:,}")
        logger.info(f"  Match rate: {match_rate:.1f}%")
        
        # Log top standardized offices
        if match_counts:
            logger.info("  Top standardized offices:")
            for office, count in sorted(match_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                logger.info(f"    {office}: {count:,} records")
        
        return result_df
    
    def get_unmatched_offices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Get offices that couldn't be standardized for analysis.
        
        Args:
            df: Dataframe with 'office' and 'source_office' columns
            
        Returns:
            Dataframe with unmatched offices and their counts
        """
        if 'source_office' not in df.columns:
            logger.warning("No 'source_office' column found. Run standardize_offices first.")
            return pd.DataFrame()
        
        # Find offices that weren't standardized (source_office != office)
        unmatched = df[df['source_office'] != df['office']]
        
        if len(unmatched) == 0:
            logger.info("All offices were successfully standardized!")
            return pd.DataFrame()
        
        # Group by source office and count
        unmatched_summary = unmatched.groupby('source_office').size().reset_index(name='count')
        unmatched_summary = unmatched_summary.sort_values('count', ascending=False)
        
        logger.info(f"Found {len(unmatched_summary):,} unmatched office types:")
        for _, row in unmatched_summary.head(20).iterrows():
            logger.info(f"  {row['source_office']}: {row['count']:,} records")
        
        return unmatched_summary
