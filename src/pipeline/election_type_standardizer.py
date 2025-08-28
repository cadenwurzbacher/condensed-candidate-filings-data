#!/usr/bin/env python3
"""
Election Type Standardizer

This module converts inconsistent election_type strings into standardized
binary columns for better data quality and easier querying.
"""

import pandas as pd
import logging
from typing import Dict, List, Tuple
import re

logger = logging.getLogger(__name__)

class ElectionTypeStandardizer:
    """
    Standardizes election types into binary columns
    """
    
    def __init__(self):
        # Define election type patterns and their binary mappings
        self.election_patterns = {
            'primary': [
                r'\bprimary\b', r'\bpri\b', r'\bprim\b', r'\bpresidential primary\b',
                r'\bstate primary\b', r'\bprimary election\b'
            ],
            'general': [
                r'\bgeneral\b', r'\bgen\b', r'\bgeneral election\b', r'\bnovember\b',
                r'\bgeneral ballot\b'
            ],
            'special': [
                r'\bspecial\b', r'\bspec\b', r'\bspecial election\b', r'\bspecial ballot\b',
                r'\bby.?election\b', r'\bfill vacancy\b'
            ]
        }
        
        # Compile regex patterns for efficiency
        self.compiled_patterns = {}
        for election_type, patterns in self.election_patterns.items():
            self.compiled_patterns[election_type] = [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
    
    def standardize_election_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert election_type column to binary columns
        
        Args:
            df: DataFrame with election_type column
            
        Returns:
            DataFrame with binary election type columns
        """
        logger.info("Standardizing election types to binary columns...")
        
        if df.empty or 'election_type' not in df.columns:
            logger.warning("No election_type column found, adding default binary columns")
            return self._add_default_binary_columns(df)
        
        # Create a copy to avoid modifying original
        result_df = df.copy()
        
        # Initialize binary columns
        result_df['ran_in_primary'] = False
        result_df['ran_in_general'] = False
        result_df['ran_in_special'] = False
        result_df['election_type_notes'] = ''
        
        # Process each election type
        for idx, row in result_df.iterrows():
            election_type = row.get('election_type')
            
            if pd.isna(election_type) or not election_type:
                # No election type - keep as False for all binary columns
                continue
            
            # Convert to string and clean
            election_str = str(election_type).strip()
            
            # Check for multiple election types (e.g., "Primary, General")
            if ',' in election_str or ';' in election_str:
                # Split and process multiple types
                types = [t.strip() for t in re.split(r'[,;]', election_str)]
                notes = f"Original: {election_str}"
            else:
                types = [election_str]
                notes = ""
            
            # Process each individual type
            for election_type_single in types:
                self._set_election_type_flags(result_df, idx, election_type_single, notes)
        
        # Remove the original election_type column
        if 'election_type' in result_df.columns:
            result_df = result_df.drop(columns=['election_type'])
            logger.info("Removed original election_type column")
        
        # Log results
        primary_count = result_df['ran_in_primary'].sum()
        general_count = result_df['ran_in_general'].sum()
        special_count = result_df['ran_in_special'].sum()
        
        logger.info(f"Election type standardization complete:")
        logger.info(f"  Primary candidates: {primary_count:,}")
        logger.info(f"  General candidates: {general_count:,}")
        logger.info(f"  Special candidates: {special_count:,}")
        
        return result_df
    
    def _set_election_type_flags(self, df: pd.DataFrame, idx: int, election_type: str, notes: str):
        """Set the appropriate binary flags for an election type"""
        
        election_type_lower = election_type.lower().strip()
        
        # Check each pattern type
        for election_category, patterns in self.compiled_patterns.items():
            for pattern in patterns:
                if pattern.search(election_type_lower):
                    # Set the appropriate binary column
                    if election_category == 'primary':
                        df.loc[idx, 'ran_in_primary'] = True
                    elif election_category == 'general':
                        df.loc[idx, 'ran_in_general'] = True
                    elif election_category == 'special':
                        df.loc[idx, 'ran_in_special'] = True
                    
                    # Add notes if we have them
                    if notes:
                        current_notes = df.loc[idx, 'election_type_notes']
                        if current_notes:
                            df.loc[idx, 'election_type_notes'] = f"{current_notes}; {notes}"
                        else:
                            df.loc[idx, 'election_type_notes'] = notes
                    
                    return  # Found a match, no need to check other patterns
        
        # If no pattern matched, log it for review
        logger.debug(f"Unrecognized election type: '{election_type}' for record {idx}")
        
        # Add to notes for manual review
        current_notes = df.loc[idx, 'election_type_notes']
        if current_notes:
            df.loc[idx, 'election_type_notes'] = f"{current_notes}; Unknown: {election_type}"
        else:
            df.loc[idx, 'election_type_notes'] = f"Unknown: {election_type}"
    
    def _add_default_binary_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add default binary columns when no election_type exists"""
        result_df = df.copy()
        
        result_df['ran_in_primary'] = False
        result_df['ran_in_general'] = False
        result_df['ran_in_special'] = False
        result_df['election_type_notes'] = 'No election type data available'
        
        logger.info("Added default binary election type columns")
        return result_df
    
    def get_election_summary(self, df: pd.DataFrame) -> Dict[str, int]:
        """Get summary of election type distribution"""
        if df.empty:
            return {}
        
        summary = {}
        
        if 'ran_in_primary' in df.columns:
            summary['primary'] = int(df['ran_in_primary'].sum())
        if 'ran_in_general' in df.columns:
            summary['general'] = int(df['ran_in_general'].sum())
        if 'ran_in_special' in df.columns:
            summary['special'] = int(df['ran_in_special'].sum())
        
        # Calculate combinations
        if all(col in df.columns for col in ['ran_in_primary', 'ran_in_general']):
            summary['primary_and_general'] = int((df['ran_in_primary'] & df['ran_in_general']).sum())
        
        if all(col in df.columns for col in ['ran_in_primary', 'ran_in_special']):
            summary['primary_and_special'] = int((df['ran_in_primary'] & df['ran_in_special']).sum())
        
        if all(col in df.columns for col in ['ran_in_general', 'ran_in_special']):
            summary['general_and_special'] = int((df['ran_in_general'] & df['ran_in_special']).sum())
        
        summary['total_records'] = len(df)
        
        return summary
