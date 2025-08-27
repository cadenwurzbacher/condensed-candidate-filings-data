#!/usr/bin/env python3
"""
Base State Cleaner Class

This class provides common functionality for all state cleaners, eliminating
duplication of column management and data structure logic.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class BaseStateCleaner(ABC):
    """
    Base class for all state cleaners.
    
    This class handles:
    - Column management and ordering
    - Common data cleaning operations
    - State-specific logic (abstract methods)
    
    Note: Party standardization is handled at the national level in the main pipeline.
    """
    
    # Standard column order for all states
    STANDARD_COLUMNS = [
        'candidate_name',
        'full_name_display',
        'first_name',
        'middle_name',
        'last_name',
        'prefix',
        'suffix',
        'nickname',
        'office',
        'district',
        'party',
        'county',
        'city',
        'state',
        'zip_code',
        'address',
        'phone',
        'email',
        'filing_date',
        'election_type',
        'election_year',
        'incumbent',
        'write_in',
        'ballot_order',
        'candidate_id',
        'office_id',
        'district_id',
        'county_id',
        'city_id',
        'state_id',
        'party_id',
        'election_id',
        'filing_id',
        'source_file',
        'source_url',
        'last_updated',
        'notes',
        'validation_status'
    ]
    
    def __init__(self, state_name: str):
        """
        Initialize the base state cleaner.
        
        Args:
            state_name: Name of the state (e.g., 'Alaska', 'Arizona')
        """
        self.state_name = state_name
        self.logger = logging.getLogger(f"{__name__}.{state_name}")
        
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main cleaning method that orchestrates the entire cleaning process.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting data cleaning for {self.state_name}")
        
        # Step 1: State-specific structural cleaning
        df = self._clean_state_specific_structure(df)
        
        # Step 2: Ensure required columns exist
        df = self._add_required_columns(df)
        
        # Step 3: Remove duplicate columns
        df = self._remove_duplicate_columns(df)
        
        # Step 4: Standardize column order
        df = self._standardize_column_order(df)
        
        # Step 5: State-specific content cleaning
        df = self._clean_state_specific_content(df)
        
        # Step 6: Name parsing (state-specific)
        df = self._parse_names(df)
        
        # Step 7: Final validation
        df = self._validate_final_data(df)
        
        self.logger.info(f"Completed data cleaning for {self.state_name}")
        return df
    
    def _add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add any missing required columns with default values.
        
        This replaces the duplicate _add_required_columns method that was
        in every single state cleaner.
        
        Args:
            df: DataFrame to add columns to
            
        Returns:
            DataFrame with all required columns
        """
        for col in self.STANDARD_COLUMNS:
            if col not in df.columns:
                df[col] = None
                self.logger.debug(f"Added missing column: {col}")
        
        return df
    
    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate columns (keeping first occurrence).
        
        This replaces the duplicate _remove_duplicate_columns method that was
        in every single state cleaner.
        
        Args:
            df: DataFrame to clean
            
        Returns:
            DataFrame with duplicate columns removed
        """
        # Get duplicate column names
        duplicate_cols = df.columns[df.columns.duplicated()].tolist()
        
        if duplicate_cols:
            self.logger.info(f"Removing duplicate columns: {duplicate_cols}")
            df = df.loc[:, ~df.columns.duplicated()]
        
        return df
    
    def _standardize_column_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure columns are in the standard order.
        
        Args:
            df: DataFrame to reorder
            
        Returns:
            DataFrame with columns in standard order
        """
        # Get columns that exist in the DataFrame
        existing_cols = [col for col in self.STANDARD_COLUMNS if col in df.columns]
        
        # Get any additional columns not in standard list
        additional_cols = [col for col in df.columns if col not in self.STANDARD_COLUMNS]
        
        # Reorder columns: standard first, then additional
        final_cols = existing_cols + additional_cols
        
        # Reorder DataFrame
        df = df[final_cols]
        
        self.logger.debug(f"Reordered columns to standard order")
        return df
    
    def _validate_final_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform final validation on the cleaned data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Validated DataFrame
        """
        # Check for required columns
        missing_cols = [col for col in self.STANDARD_COLUMNS if col not in df.columns]
        if missing_cols:
            self.logger.warning(f"Missing required columns: {missing_cols}")
        
        # Check for empty DataFrame
        if df.empty:
            self.logger.warning("DataFrame is empty after cleaning")
        
        # Log final column count
        self.logger.info(f"Final DataFrame has {len(df.columns)} columns and {len(df)} rows")
        
        return df
    
    # ============================================================================
    # ABSTRACT METHODS - Must be implemented by each state
    # ============================================================================
    
    @abstractmethod
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean state-specific data structure (e.g., name parsing, address parsing).
        
        This is where state-specific logic goes - NOT party standardization,
        NOT column management, NOT office standardization.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with state-specific structure cleaned
        """
        pass
    
    @abstractmethod
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean state-specific content (e.g., county mappings, special formatting).
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with state-specific content cleaned
        """
        pass
    
    @abstractmethod
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix, nickname components.
        Each state may have different name parsing logic based on their data format.
        
        Args:
            df: DataFrame with candidate names
            
        Returns:
            DataFrame with parsed name components
        """
        pass
    
    # ============================================================================
    # UTILITY METHODS - Available to all state cleaners
    # ============================================================================
    
    def _extract_name_parts(self, full_name: str) -> Tuple[str, str, str]:
        """
        Extract first, middle, and last name from full name.
        
        Args:
            full_name: Full name string
            
        Returns:
            Tuple of (first, middle, last) names
        """
        if pd.isna(full_name) or not full_name:
            return None, None, None
        
        name_parts = str(full_name).strip().split()
        
        if len(name_parts) == 1:
            return name_parts[0], None, None
        elif len(name_parts) == 2:
            return name_parts[0], None, name_parts[1]
        else:
            return name_parts[0], ' '.join(name_parts[1:-1]), name_parts[-1]
    
    def _clean_address(self, address: str) -> str:
        """
        Clean and standardize address strings.
        
        Args:
            address: Raw address string
            
        Returns:
            Cleaned address string
        """
        if pd.isna(address) or not address:
            return None
        
        # Basic address cleaning
        cleaned = str(address).strip()
        cleaned = ' '.join(cleaned.split())  # Normalize whitespace
        
        return cleaned
    
    def _standardize_case(self, text: str, case_type: str = 'title') -> str:
        """
        Standardize text case.
        
        Args:
            text: Text to standardize
            case_type: 'title', 'upper', 'lower', 'proper'
            
        Returns:
            Standardized text
        """
        if pd.isna(text) or not text:
            return None
        
        text = str(text).strip()
        
        if case_type == 'title':
            return text.title()
        elif case_type == 'upper':
            return text.upper()
        elif case_type == 'lower':
            return text.lower()
        elif case_type == 'proper':
            # Proper case: first letter of each word capitalized
            return ' '.join(word.capitalize() for word in text.split())
        else:
            return text
