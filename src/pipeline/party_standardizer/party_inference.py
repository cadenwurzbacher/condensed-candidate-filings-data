#!/usr/bin/env python3
"""
Party Inference for CandidateFilings.com Data Processing

This module handles inferring missing party information from office context,
using patterns and indicators found in office names.
"""

import pandas as pd
import re
import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

class PartyInference:
    """
    Infers missing party information from office context.
    
    This class analyzes office names to find party indicators and
    infers missing party information for candidates.
    """
    
    def __init__(self):
        """Initialize the party inference with patterns and indicators."""
        self.party_indicators = self._create_party_indicators()
    
    def _create_party_indicators(self) -> Dict[str, str]:
        """
        Create comprehensive party indicators for office-based inference.
        
        Returns:
            Dictionary mapping regex patterns to standardized party names
        """
        return {
            # Single letter indicators in parentheses
            r'\(D\)': 'Democratic',
            r'\(R\)': 'Republican', 
            r'\(I\)': 'Independent',
            r'\(L\)': 'Libertarian',
            r'\(G\)': 'Green Party',
            r'\(C\)': 'Constitution Party',
            r'\(P\)': 'Progressive',
            r'\(W\)': 'Working Families',
            r'\(N\)': 'Natural Law',
            r'\(S\)': 'Socialist',
            r'\(A\)': 'American Independent',
            r'\(F\)': 'Peace and Freedom',
            r'\(E\)': 'Women\'s Equality',
            r'\(T\)': 'Tea Party',
            r'\(B\)': 'Reform',
            r'\(M\)': 'Moderate',
            r'\(K\)': 'Conservative',
            r'\(H\)': 'Liberal',
            
            # Full party names
            r'Democratic Party': 'Democratic',
            r'Republican Party': 'Republican',
            r'Independent Party': 'Independent',
            r'Libertarian Party': 'Libertarian',
            r'Green Party': 'Green Party',
            r'Constitution Party': 'Constitution Party',
            r'Progressive Party': 'Progressive',
            r'Working Families Party': 'Working Families',
            r'Natural Law Party': 'Natural Law',
            r'Socialist Party': 'Socialist',
            r'American Independent Party': 'American Independent',
            r'Peace and Freedom Party': 'Peace and Freedom',
            r'Women\'s Equality Party': 'Women\'s Equality',
            r'Tea Party': 'Tea Party',
            r'Reform Party': 'Reform Party',
            r'Moderate Party': 'Moderate',
            r'Conservative Party': 'Conservative Party',
            r'Liberal Party': 'Liberal Party',
            
            # Abbreviated party names
            r'\bDEM\b': 'Democratic',
            r'\bREP\b': 'Republican',
            r'\bIND\b': 'Independent',
            r'\bLIB\b': 'Libertarian',
            r'\bGRE\b': 'Green Party',
            r'\bCST\b': 'Constitution Party',
            r'\bPRO\b': 'Progressive',
            r'\bWFP\b': 'Working Families',
            r'\bNLP\b': 'Natural Law',
            r'\bSOC\b': 'Socialist',
            r'\bAIP\b': 'American Independent',
            r'\bPFP\b': 'Peace and Freedom',
            r'\bWEP\b': 'Women\'s Equality',
            r'\bTEA\b': 'Tea Party',
            r'\bREF\b': 'Reform Party',
            r'\bMOD\b': 'Moderate',
            r'\bCON\b': 'Conservative Party',
            r'\bLIB\b': 'Liberal Party',
            
            # Party indicators in different positions
            r'- Democratic': 'Democratic',
            r'- Republican': 'Republican',
            r'- Independent': 'Independent',
            r'- Libertarian': 'Libertarian',
            r'- Green': 'Green Party',
            r'- Constitution': 'Constitution Party',
            r'- Progressive': 'Progressive',
            r'- Working Families': 'Working Families',
            r'- Natural Law': 'Natural Law',
            r'- Socialist': 'Socialist',
            r'- American Independent': 'American Independent',
            r'- Peace and Freedom': 'Peace and Freedom',
            r'- Women\'s Equality': 'Women\'s Equality',
            r'- Tea Party': 'Tea Party',
            r'- Reform': 'Reform Party',
            r'- Moderate': 'Moderate',
            r'- Conservative': 'Conservative Party',
            r'- Liberal': 'Liberal Party',
            
            # Party indicators at the beginning
            r'^Democratic\s+': 'Democratic',
            r'^Republican\s+': 'Republican',
            r'^Independent\s+': 'Independent',
            r'^Libertarian\s+': 'Libertarian',
            r'^Green\s+': 'Green Party',
            r'^Constitution\s+': 'Constitution Party',
            r'^Progressive\s+': 'Progressive',
            r'^Working Families\s+': 'Working Families',
            r'^Natural Law\s+': 'Natural Law',
            r'^Socialist\s+': 'Socialist',
            r'^American Independent\s+': 'American Independent',
            r'^Peace and Freedom\s+': 'Peace and Freedom',
            r'^Women\'s Equality\s+': 'Women\'s Equality',
            r'^Tea Party\s+': 'Tea Party',
            r'^Reform\s+': 'Reform Party',
            r'^Moderate\s+': 'Moderate',
            r'^Conservative\s+': 'Conservative Party',
            r'^Liberal\s+': 'Liberal Party',
            
            # State-specific party indicators
            r'Democratic-Farmer-Labor': 'Democratic',
            r'DFL': 'Democratic',
            r'Alaska Independence': 'Alaska Independence Party',
            r'Vermont Progressive': 'Vermont Progressive Party',
            r'Working Families': 'Working Families',
            r'Independence': 'Independence Party'
        }
    
    def infer_party_from_office(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Infer missing party information from office context.
        
        Args:
            df: DataFrame containing office and party data
            
        Returns:
            DataFrame with inferred party information
        """
        if 'office' not in df.columns:
            logger.warning("No 'office' column found for party inference")
            return df
        
        try:
            # Create a copy to avoid modifying original
            df_inferred = df.copy()
            inferred_count = 0
            
            logger.info("Starting party inference from office context...")
            
            for idx, row in df_inferred.iterrows():
                # Only infer if party is missing or null
                if self._is_party_missing(row.get('party')):
                    office_str = str(row['office']) if pd.notna(row['office']) else ''
                    
                    # Check for party indicators
                    inferred_party = self._find_party_in_office(office_str)
                    if inferred_party:
                        df_inferred.at[idx, 'party'] = inferred_party
                        inferred_count += 1
            
            if inferred_count > 0:
                logger.info(f"Inferred party for {inferred_count:,} records from office context")
            else:
                logger.info("No additional party information inferred from office context")
            
            return df_inferred
            
        except Exception as e:
            logger.error(f"Party inference from office failed: {e}")
            return df
    
    def _is_party_missing(self, party_value) -> bool:
        """
        Check if party value is missing or null.
        
        Args:
            party_value: Party value to check
            
        Returns:
            True if party is missing, False otherwise
        """
        if pd.isna(party_value):
            return True
        
        party_str = str(party_value).strip().lower()
        missing_values = ['', 'nan', 'none', 'null', 'unknown', 'n/a']
        
        return party_str in missing_values
    
    def _find_party_in_office(self, office_str: str) -> str:
        """
        Find party indicators in office string.
        
        Args:
            office_str: Office string to analyze
            
        Returns:
            Inferred party name or empty string if none found
        """
        if not office_str:
            return ""
        
        # Check for party indicators in order of specificity
        for pattern, party in self.party_indicators.items():
            if re.search(pattern, office_str, re.IGNORECASE):
                return party
        
        return ""
    
    def get_inference_patterns(self) -> Dict[str, str]:
        """
        Get all party inference patterns.
        
        Returns:
            Dictionary of regex patterns and their party mappings
        """
        return self.party_indicators.copy()
    
    def add_inference_pattern(self, pattern: str, party: str) -> None:
        """
        Add a new party inference pattern.
        
        Args:
            pattern: Regex pattern to match
            party: Party name to infer
        """
        self.party_indicators[pattern] = party
    
    def remove_inference_pattern(self, pattern: str) -> bool:
        """
        Remove a party inference pattern.
        
        Args:
            pattern: Regex pattern to remove
            
        Returns:
            True if pattern was removed, False if not found
        """
        if pattern in self.party_indicators:
            del self.party_indicators[pattern]
            return True
        return False
    
    def test_inference_patterns(self, test_offices: list) -> Dict[str, list]:
        """
        Test inference patterns against sample office names.
        
        Args:
            test_offices: List of office names to test
            
        Returns:
            Dictionary mapping patterns to matching office names
        """
        results = {}
        
        for pattern, party in self.party_indicators.items():
            matches = []
            for office in test_offices:
                if re.search(pattern, office, re.IGNORECASE):
                    matches.append(office)
            
            if matches:
                results[pattern] = matches
        
        return results
    
    def get_inference_coverage_stats(self) -> Dict[str, int]:
        """
        Get statistics about inference pattern coverage.
        
        Returns:
            Dictionary with coverage statistics
        """
        total_patterns = len(self.party_indicators)
        unique_parties = len(set(self.party_indicators.values()))
        
        return {
            'total_patterns': total_patterns,
            'unique_parties': unique_parties,
            'patterns_per_party': total_patterns / unique_parties if unique_parties > 0 else 0
        }
