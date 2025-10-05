#!/usr/bin/env python3
"""
Main Party Standardizer for CandidateFilings.com Data Processing

This module extracts and refactors the complex party standardization logic from the main pipeline
into a clean, testable, and maintainable module.
"""

import pandas as pd
import logging
from typing import Dict, Any
from .party_mappings import PartyMappings
from .party_inference import PartyInference

logger = logging.getLogger(__name__)

class PartyStandardizer:
    """
    Comprehensive party name standardizer for political party data.
    
    This class handles:
    - Party name mapping and standardization
    - Office-based party inference
    - Coverage improvement tracking
    - Fallback mechanisms
    """
    
    def __init__(self):
        """Initialize the party standardizer with specialized components."""
        self.party_mappings = PartyMappings()
        self.party_inference = PartyInference()
    
    def standardize_parties(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Comprehensive party name standardization.
        
        Args:
            df: DataFrame containing party data
            
        Returns:
            DataFrame with standardized party names
        """
        if 'party' not in df.columns:
            logger.warning("No 'party' column found for standardization")
            return df
        
        try:
            # Log initial party coverage
            initial_coverage = df['party'].notna().sum()
            logger.info(f"Initial party coverage: {initial_coverage:,} records")
            
            # Step 1: Infer party from office context if party is missing
            df = self.party_inference.infer_party_from_office(df)
            
            # Step 2: Standardize party names using comprehensive mappings
            df = self._apply_party_mappings(df)
            
            # Step 3: Clean up and finalize
            df = self._finalize_party_standardization(df)
            
            # Log final statistics
            final_coverage = df['party'].notna().sum()
            improvement = final_coverage - initial_coverage
            logger.info(f"Party coverage improved from {initial_coverage:,} to {final_coverage:,} records (+{improvement:,})")
            logger.info(f"Final party coverage after standardization: {final_coverage:,} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Party standardization failed: {e}")
            # Return original data if standardization fails
            return df
    
    def _apply_party_mappings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply comprehensive party name mappings.
        
        Args:
            df: DataFrame with party data
            
        Returns:
            DataFrame with mapped party names
        """
        try:
            # Get party mappings
            party_series = df['party'].astype(str)
            
            # Handle case variations first (ALL CAPS → Title Case)
            party_series = party_series.str.title()
            
            # Apply comprehensive party mappings
            df['party_standardized'] = party_series.str.lower().map(
                self.party_mappings.get_party_mappings()
            ).fillna(party_series)
            
            logger.info("Party name mappings applied successfully")
            
        except Exception as e:
            logger.error(f"Party mapping application failed: {e}")
            # Fallback to original party names
            df['party_standardized'] = df['party']
        
        return df
    
    def _finalize_party_standardization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Finalize party standardization by cleaning up and replacing original column.
        
        Args:
            df: DataFrame with party_standardized column
            
        Returns:
            DataFrame with final party standardization
        """
        try:
            # Clean up any remaining issues
            df['party_standardized'] = df['party_standardized'].replace({
                'nan': None,
                'None': None,
                '': None
            })
            
            # Replace the original party column with the standardized version
            df['party'] = df['party_standardized']
            
            # Drop the temporary column
            if 'party_standardized' in df.columns:
                df = df.drop(columns=['party_standardized'])
            
            logger.info("Party standardization finalized successfully")
            
        except Exception as e:
            logger.error(f"Party standardization finalization failed: {e}")
            # Keep original party column if finalization fails
            if 'party_standardized' in df.columns:
                df = df.drop(columns=['party_standardized'])
        
        return df
    
    def get_party_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get comprehensive party statistics for analysis.
        
        Args:
            df: DataFrame with party data
            
        Returns:
            Dictionary containing party statistics
        """
        if 'party' not in df.columns:
            return {}
        
        try:
            party_counts = df['party'].value_counts()
            total_records = len(df)
            party_coverage = df['party'].notna().sum()
            
            stats = {
                'total_records': total_records,
                'party_coverage': party_coverage,
                'coverage_percentage': (party_coverage / total_records * 100) if total_records > 0 else 0,
                'unique_parties': len(party_counts),
                'top_parties': party_counts.head(10).to_dict(),
                'party_distribution': party_counts.to_dict()
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Party statistics calculation failed: {e}")
            return {}
    
    def validate_party_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate party data quality and consistency.
        
        Args:
            df: DataFrame with party data
            
        Returns:
            Dictionary containing validation results
        """
        if 'party' not in df.columns:
            return {'valid': False, 'error': 'No party column found'}
        
        try:
            validation_results = {
                'valid': True,
                'total_records': len(df),
                'null_count': df['party'].isna().sum(),
                'empty_string_count': (df['party'] == '').sum(),
                'unique_parties': df['party'].nunique(),
                'case_consistency': self._check_case_consistency(df),
                'mapping_coverage': self._check_mapping_coverage(df)
            }
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Party validation failed: {e}")
            return {'valid': False, 'error': str(e)}
    
    def _check_case_consistency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Check case consistency in party names.
        
        Args:
            df: DataFrame with party data
            
        Returns:
            Dictionary with case consistency analysis
        """
        try:
            party_series = df['party'].dropna()
            
            # Check for mixed case patterns
            all_upper = party_series.str.isupper().sum()
            all_lower = party_series.str.islower().sum()
            title_case = party_series.str.istitle().sum()
            mixed_case = len(party_series) - all_upper - all_lower - title_case
            
            return {
                'all_upper': all_upper,
                'all_lower': all_lower,
                'title_case': title_case,
                'mixed_case': mixed_case,
                'total': len(party_series)
            }
            
        except Exception as e:
            logger.error(f"Case consistency check failed: {e}")
            return {}
    
    def _check_mapping_coverage(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Check how many party names are covered by our mappings.
        
        Args:
            df: DataFrame with party data
            
        Returns:
            Dictionary with mapping coverage analysis
        """
        try:
            party_series = df['party'].dropna().str.lower()
            mappings = self.party_mappings.get_party_mappings()
            
            # Count how many are covered by mappings
            covered = party_series.isin(mappings.keys()).sum()
            total = len(party_series)
            
            return {
                'covered_by_mappings': covered,
                'not_covered': total - covered,
                'total': total,
                'coverage_percentage': (covered / total * 100) if total > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Mapping coverage check failed: {e}")
            return {}
