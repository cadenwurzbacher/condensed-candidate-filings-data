#!/usr/bin/env python3
"""
Party Mappings for CandidateFilings.com Data Processing

This module contains comprehensive mappings of party name variations
to standardized party names.
"""

from typing import Dict

class PartyMappings:
    """
    Comprehensive party name mappings for standardization.
    
    Contains mappings from various party name variations to standardized names,
    covering abbreviations, common misspellings, and alternative formats.
    """
    
    def __init__(self):
        """Initialize party mappings."""
        self._mappings = self._create_party_mappings()
    
    def _create_party_mappings(self) -> Dict[str, str]:
        """
        Create comprehensive party name mappings.
        
        Returns:
            Dictionary mapping party variations to standardized names
        """
        return {
            # Democratic variations
            'democrat': 'Democratic',
            'democratic': 'Democratic',
            'dem': 'Democratic',
            'dem.': 'Democratic',
            'd': 'Democratic',
            'democratic party': 'Democratic',
            'democrats': 'Democratic',
            'democratic-farmer-labor': 'Democratic',
            'democratic-farmer-labor party': 'Democratic',
            'dfl': 'Democratic',
            
            # Republican variations
            'republican': 'Republican',
            'republicans': 'Republican',
            'rep': 'Republican',
            'rep.': 'Republican',
            'r': 'Republican',
            'gop': 'Republican',
            'grand old party': 'Republican',
            'republican party': 'Republican',
            
            # Independent variations
            'independent': 'Independent',
            'ind': 'Independent',
            'independents': 'Independent',
            'no party': 'Independent',
            'no party preference': 'Independent',
            'unaffiliated': 'Independent',
            'una': 'Independent',
            'none': 'Independent',
            'nonpartisan': 'Independent',
            'non-partisan': 'Independent',
            'non partisan': 'Independent',
            'nonpartisan judicial': 'Independent',
            'non-partisan judicial': 'Independent',
            
            # Unaffiliated variations
            'npa': 'Unaffiliated',
            
            # Nonpartisan variations
            'non': 'Nonpartisan',
            
            # Libertarian variations
            'libertarian': 'Libertarian',
            'lib': 'Libertarian',
            'libertarians': 'Libertarian',
            'lbt': 'Libertarian',
            'libertarian party': 'Libertarian',
            
            # Green Party variations
            'green': 'Green Party',
            'green party': 'Green Party',
            'gre': 'Green Party',
            'greens': 'Green Party',
            'green party of the united states': 'Green Party',
            
            # Constitution Party variations
            'constitution': 'Constitution Party',
            'constitution party': 'Constitution Party',
            'cst': 'Constitution Party',
            'constitutional': 'Constitution Party',
            'constitutional party': 'Constitution Party',
            
            # Progressive variations
            'progressive': 'Progressive',
            'progressive party': 'Progressive',
            'prog': 'Progressive',
            
            # Working Families variations
            'working families': 'Working Families',
            'working families party': 'Working Families',
            'wfp': 'Working Families',
            
            # Natural Law variations
            'natural law': 'Natural Law Party',
            'natural law party': 'Natural Law Party',
            'nlp': 'Natural Law Party',
            
            # Socialist variations
            'socialist': 'Socialist Party',
            'socialist party': 'Socialist Party',
            'soc': 'Socialist Party',
            'socialist workers': 'Socialist Party',
            
            # American Independent variations
            'american independent': 'American Independent Party',
            'american independent party': 'American Independent Party',
            'aip': 'American Independent Party',
            
            # Peace and Freedom variations
            'peace and freedom': 'Peace and Freedom Party',
            'peace and freedom party': 'Peace and Freedom Party',
            'pfp': 'Peace and Freedom Party',
            
            # Women's Equality variations
            'women\'s equality': 'Women\'s Equality Party',
            'women\'s equality party': 'Women\'s Equality Party',
            'wep': 'Women\'s Equality Party',
            
            # Independence Party variations
            'independence': 'Independence Party',
            'independence party': 'Independence Party',
            'indp': 'Independence Party',
            
            # Conservative variations
            'conservative': 'Conservative Party',
            'conservative party': 'Conservative Party',
            'con': 'Conservative Party',
            
            # Liberal variations
            'liberal': 'Liberal Party',
            'liberal party': 'Liberal Party',
            'lib': 'Liberal Party',
            
            # Moderate variations
            'moderate': 'Moderate Party',
            'moderate party': 'Moderate Party',
            'mod': 'Moderate Party',
            
            # Tea Party variations
            'tea party': 'Tea Party',
            'tea party movement': 'Tea Party',
            'tea': 'Tea Party',
            
            # Reform variations
            'reform': 'Reform Party',
            'reform party': 'Reform Party',
            'ref': 'Reform Party',
            
            # Communist variations
            'communist': 'Communist Party',
            'communist party': 'Communist Party',
            'com': 'Communist Party',
            
            # Write-in variations
            'write-in': 'Write-In',
            'writein': 'Write-In',
            'write in': 'Write-In',
            
            # Other variations
            'other': 'Other',
            'unknown': 'Other',
            'misc': 'Other',
            'miscellaneous': 'Other',
            'third party': 'Other',
            'minor party': 'Other',
            
            # State-specific parties
            'alaska independence': 'Alaska Independence Party',
            'alaska independence party': 'Alaska Independence Party',
            'vermont progressive': 'Vermont Progressive Party',
            'vermont progressive party': 'Vermont Progressive Party',
            'minnesota democratic-farmer-labor': 'Democratic',
            'minnesota democratic-farmer-labor party': 'Democratic',
            
            # Additional party mappings from state cleaners
            'CONSTITUTION': 'Constitution Party',
            'DEM': 'Democratic',
            'DEMOCRAT': 'Democratic',
            'DEMOCRATIC': 'Democratic',
            'GREEN': 'Green Party',
            'GREEN PARTY': 'Green Party',
            'IND': 'Independent',
            'INDEPENDENT': 'Independent',
            'LIB': 'Libertarian',
            'LIBERTARIAN': 'Libertarian',
            'NO PARTY AFFILIATION': 'Unaffiliated',
            'NPA': 'Unaffiliated',
            'REFORM': 'Reform Party',
            'REFORM PARTY': 'Reform Party',
            'REP': 'Republican',
            'REPUBLICAN': 'Republican',
            'unenrolled': 'Unaffiliated',
            'nonpartisan special': 'Nonpartisan',
        }
    
    def get_party_mappings(self) -> Dict[str, str]:
        """
        Get the complete party mappings dictionary.
        
        Returns:
            Dictionary of party name variations to standardized names
        """
        return self._mappings.copy()
    
    def add_party_mapping(self, variation: str, standardized: str) -> None:
        """
        Add a new party mapping.
        
        Args:
            variation: Party name variation to map
            standardized: Standardized party name
        """
        self._mappings[variation.lower()] = standardized
    
    def remove_party_mapping(self, variation: str) -> bool:
        """
        Remove a party mapping.
        
        Args:
            variation: Party name variation to remove
            
        Returns:
            True if mapping was removed, False if not found
        """
        if variation.lower() in self._mappings:
            del self._mappings[variation.lower()]
            return True
        return False
    
    def get_standardized_parties(self) -> set:
        """
        Get the set of all standardized party names.
        
        Returns:
            Set of standardized party names
        """
        return set(self._mappings.values())
    
    def get_variations_for_party(self, standardized_party: str) -> list:
        """
        Get all variations for a specific standardized party.
        
        Args:
            standardized_party: Standardized party name
            
        Returns:
            List of variations that map to this party
        """
        return [var for var, std in self._mappings.items() if std == standardized_party]
    
    def search_party_mappings(self, query: str) -> Dict[str, str]:
        """
        Search party mappings for a specific query.
        
        Args:
            query: Search query (case-insensitive)
            
        Returns:
            Dictionary of matching mappings
        """
        query_lower = query.lower()
        return {
            var: std for var, std in self._mappings.items() 
            if query_lower in var.lower() or query_lower in std.lower()
        }
    
    def get_mapping_coverage_stats(self) -> Dict[str, int]:
        """
        Get statistics about mapping coverage.
        
        Returns:
            Dictionary with coverage statistics
        """
        total_variations = len(self._mappings)
        unique_standardized = len(self.get_standardized_parties())
        
        return {
            'total_variations': total_variations,
            'unique_standardized_parties': unique_standardized,
            'average_variations_per_party': total_variations / unique_standardized if unique_standardized > 0 else 0
        }
