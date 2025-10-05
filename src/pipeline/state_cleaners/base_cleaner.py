#!/usr/bin/env python3
"""
Base State Cleaner for CandidateFilings.com Data Processing

This abstract base class provides common functionality for all state cleaners,
eliminating code duplication while preserving state-specific logic.
"""

import pandas as pd
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any

class BaseStateCleaner(ABC):
    """
    Abstract base class for state-specific data cleaners.
    
    This class provides common functionality while requiring each state
    to implement its own specific cleaning logic.
    """
    
    # Standard columns that all state cleaners should produce
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
        'address',
        'city',
        'zip_code',
        'address_state',
        'phone',
        'email',
        'website',
        'election_year',
        'state',
        'stable_id'
    ]
    
    def __init__(self, state_name: str):
        """
        Initialize the base state cleaner.
        
        Args:
            state_name: Name of the state this cleaner handles
        """
        self.state_name = state_name
        self.logger = logging.getLogger(f"{__name__}.{state_name}")
        
        # Initialize standard columns
        self._initialize_standard_columns()
    
    def _initialize_standard_columns(self):
        """Initialize standard columns with default values."""
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main data cleaning orchestration method.
        
        This method coordinates the entire cleaning process by calling
        state-specific methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        try:
            self.logger.info(f"Starting {self.state_name} data cleaning process")
            
            # Step 1: Clean state-specific structure
            df = self._clean_state_specific_structure(df)
            
            # Step 2: Add standard columns if missing
            df = self._ensure_standard_columns(df)
            
            # Step 3: Clean state-specific content
            df = self._clean_state_specific_content(df)
            
            # Step 4: Standardize data types
            df = self._standardize_data_types(df)
            
            # Step 5: Validate data integrity
            df = self._validate_data_integrity(df)
            
            # Step 6: Parse names into components
            df = self._parse_names(df)
            
            # Step 7: Final validation and cleanup
            df = self._final_validation(df)
            
            self.logger.info(f"Completed {self.state_name} data cleaning")
            return df
            
        except Exception as e:
            self.logger.error(f"Error during {self.state_name} data cleaning: {e}")
            # Return original data if cleaning fails
            return df
    
    @abstractmethod
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean state-specific data structure.
        
        This method should be implemented by each state to handle:
        - State-specific column names and formats
        - Multi-sheet file handling
        - Complex data structures unique to the state
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            DataFrame with state-specific structure cleaned
        """
    
    @abstractmethod
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean state-specific content.
        
        This method should be implemented by each state to handle:
        - County/borough standardization
        - Office standardization
        - State-specific formatting rules
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with state-specific content cleaned
        """
    
    @abstractmethod
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into components.
        
        This method should be implemented by each state to handle:
        - Name parsing logic specific to the state
        - Prefix/suffix extraction
        - Nickname handling
        - State-specific name formats
        
        Args:
            df: DataFrame with candidate names
            
        Returns:
            DataFrame with parsed name components
        """
    
    def _ensure_standard_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure all standard columns exist in the DataFrame.
        
        Args:
            df: DataFrame to check
            
        Returns:
            DataFrame with all standard columns present
        """
        for col in self.STANDARD_COLUMNS:
            if col not in df.columns:
                df[col] = None
        
        return df
    
    def _standardize_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize data types for consistency.
        
        Args:
            df: DataFrame to standardize
            
        Returns:
            DataFrame with standardized data types
        """
        # Convert string columns to proper types
        if 'election_year' in df.columns:
            df['election_year'] = pd.to_numeric(df['election_year'], errors='coerce')
        
        # Ensure state column is consistent
        if 'state' in df.columns:
            df['state'] = df['state'].astype(str)
        
        return df
    
    def _validate_data_integrity(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate data integrity and consistency.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame with validation results
        """
        # Basic validation - can be extended by subclasses
        if 'candidate_name' in df.columns:
            # Remove rows with completely empty candidate names
            df = df.dropna(subset=['candidate_name'])
        
        return df
    
    def _final_validation(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Final validation and cleanup.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame with final validation applied
        """
        # Remove duplicate rows, excluding raw_data column which contains dicts
        columns_to_check = [col for col in df.columns if col != 'raw_data']
        df = df.drop_duplicates(subset=columns_to_check)
        
        # Reset index
        df = df.reset_index(drop=True)
        
        return df
    
    def get_cleaning_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get statistics about the cleaning process.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with cleaning statistics
        """
        stats = {
            'state': self.state_name,
            'total_records': len(df),
            'columns': len(df.columns),
            'missing_data': df.isnull().sum().to_dict(),
            'data_types': df.dtypes.to_dict()
        }
        
        return stats
