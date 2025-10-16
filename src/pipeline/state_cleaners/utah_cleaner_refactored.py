#!/usr/bin/env python3
"""
Utah State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class UtahCleaner(BaseStateCleaner):
    """
    Utah-specific data cleaner.
    
    This cleaner handles Utah-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Utah")
        
        # County mappings removed - not needed
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Utah candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Utah data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Utah-specific data structure.
        
        Args:
            df: DataFrame with basic structure
            
        Returns:
            DataFrame with Utah-specific cleaning applied
        """
        self.logger.info("Applying Utah-specific structural cleaning")
        
        # Utah data is already well-structured from the Excel file
        # Just need to handle some specific cases
        
        # Clean district values - convert to string and handle NaN
        if 'district' in df.columns:
            df['district'] = df['district'].astype(str)
            df['district'] = df['district'].replace('nan', None)
            df['district'] = df['district'].replace('None', None)
        
        # Clean office names - remove extra whitespace
        if 'office' in df.columns:
            df['office'] = df['office'].str.strip()
        
        # Clean party names - remove extra whitespace
        if 'party' in df.columns:
            df['party'] = df['party'].str.strip()
        
        # Clean email addresses
        if 'email' in df.columns:
            df['email'] = df['email'].str.strip()
            df['email'] = df['email'].replace('', None)
        
        # Clean website URLs
        if 'website' in df.columns:
            df['website'] = df['website'].str.strip()
            df['website'] = df['website'].replace('', None)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Utah-specific content.
        
        This method handles Utah-specific data cleaning including:
        - State-specific formatting rules
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Utah-specific content cleaned
        """
        self.logger.info("Cleaning Utah-specific content")
        
        # Utah-specific content cleaning
        # Since Utah data is already well-structured, we mainly need to:
        # 1. Handle any state-specific formatting
        
        # Clean office names - remove extra whitespace
        if 'office' in df.columns:
            df['office'] = df['office'].str.strip()
        
        # Clean party names - remove extra whitespace  
        if 'party' in df.columns:
            df['party'] = df['party'].str.strip()
        
        # County standardization removed - not needed
        # Office/Party mappings moved to national standards phase
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names for Utah data.
        
        Utah data already has separate name fields, but we need to:
        1. Handle missing middle names
        2. Create full_name_display
        3. Handle suffixes properly
        
        Args:
            df: DataFrame with name fields
            
        Returns:
            DataFrame with parsed names
        """
        self.logger.info("Parsing Utah candidate names")
        
        # Utah already has separate name fields, so we mainly need to:
        # 1. Clean up the existing fields
        # 2. Create full_name_display
        # 3. Handle missing values
        
        # Clean first names
        if 'first_name' in df.columns:
            df['first_name'] = df['first_name'].str.strip()
            df['first_name'] = df['first_name'].replace('', None)
        
        # Clean middle names
        if 'middle_name' in df.columns:
            df['middle_name'] = df['middle_name'].str.strip()
            df['middle_name'] = df['middle_name'].replace('', None)
            df['middle_name'] = df['middle_name'].replace('nan', None)
        
        # Clean last names
        if 'last_name' in df.columns:
            df['last_name'] = df['last_name'].str.strip()
            df['last_name'] = df['last_name'].replace('', None)
        
        # Clean suffixes
        if 'suffix' in df.columns:
            df['suffix'] = df['suffix'].str.strip()
            df['suffix'] = df['suffix'].replace('', None)
            df['suffix'] = df['suffix'].replace('nan', None)
        
        # Create full_name_display
        def create_display_name(row):
            parts = []
            
            # Add first name
            if pd.notna(row.get('first_name')):
                parts.append(row['first_name'])
            
            # Add middle name (abbreviated)
            if pd.notna(row.get('middle_name')):
                middle = row['middle_name']
                if len(middle) > 1:
                    parts.append(f"{middle[0]}.")
                else:
                    parts.append(middle)
            
            # Add last name
            if pd.notna(row.get('last_name')):
                parts.append(row['last_name'])
            
            # Add suffix
            if pd.notna(row.get('suffix')):
                parts.append(row['suffix'])
            
            return ' '.join(parts) if parts else None
        
        df['full_name_display'] = df.apply(create_display_name, axis=1)
        
        # If we have name_on_ballot, use it as fallback for full_name_display
        if 'name_on_ballot' in df.columns:
            mask = df['full_name_display'].isna() & df['name_on_ballot'].notna()
            df.loc[mask, 'full_name_display'] = df.loc[mask, 'name_on_ballot']
        
        return df
    
    def _extract_districts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract and clean district information for Utah.
        
        Utah districts are already extracted, but we need to:
        1. Handle statewide offices (no district)
        2. Clean district formatting
        3. Handle special cases
        
        Args:
            df: DataFrame with office and district info
            
        Returns:
            DataFrame with cleaned district information
        """
        self.logger.info("Extracting Utah district information")
        
        if 'district' not in df.columns or 'office' not in df.columns:
            return df
        
        # Define statewide offices (no district)
        statewide_offices = [
            'President', 'US Senate', 'Governor', 'Attorney General',
            'State Auditor', 'State Treasurer'
        ]
        
        # Set district to None for statewide offices
        statewide_mask = df['office'].isin(statewide_offices)
        df.loc[statewide_mask, 'district'] = None
        
        # Clean district values for districted offices
        districted_mask = ~statewide_mask & df['district'].notna()
        
        if districted_mask.any():
            # Convert to string and clean
            df.loc[districted_mask, 'district'] = df.loc[districted_mask, 'district'].astype(str)
            
            # Remove decimal points for whole numbers (e.g., "3.0" -> "3")
            df.loc[districted_mask, 'district'] = df.loc[districted_mask, 'district'].str.replace(r'\.0$', '', regex=True)
            
            # Handle special cases
            df.loc[districted_mask, 'district'] = df.loc[districted_mask, 'district'].replace('nan', None)
            df.loc[districted_mask, 'district'] = df.loc[districted_mask, 'district'].replace('None', None)
        
        return df
    
    def _clean_addresses(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean address information for Utah.
        
        Utah data doesn't have address information in the source file,
        so we'll set these fields to None.
        
        Args:
            df: DataFrame with address fields
            
        Returns:
            DataFrame with cleaned address information
        """
        self.logger.info("Cleaning Utah address information")
        
        # Utah data doesn't include addresses, so set to None
        address_fields = ['address', 'city', 'zip_code', 'county']
        for field in address_fields:
            if field in df.columns:
                df[field] = None
        
        return df
    
    def _clean_contact_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean contact information for Utah.
        
        Args:
            df: DataFrame with contact fields
            
        Returns:
            DataFrame with cleaned contact information
        """
        self.logger.info("Cleaning Utah contact information")
        
        # Clean email addresses
        if 'email' in df.columns:
            df['email'] = df['email'].str.strip()
            df['email'] = df['email'].replace('', None)
            df['email'] = df['email'].replace('nan', None)
        
        # Clean website URLs
        if 'website' in df.columns:
            df['website'] = df['website'].str.strip()
            df['website'] = df['website'].replace('', None)
            df['website'] = df['website'].replace('nan', None)
        
        # Utah doesn't have phone numbers in the source data
        if 'phone' in df.columns:
            df['phone'] = None
        
        return df
    
    def _add_election_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add election information for Utah.
        
        Args:
            df: DataFrame to add election info to
            
        Returns:
            DataFrame with election information added
        """
        self.logger.info("Adding Utah election information")
        
        # Set election year (assuming 2024 based on filename)
        df['election_year'] = 2024
        
        # Set election type (general election)
        df['election_type'] = 'General'
        
        # Set election date (November 5, 2024 for general election)
        df['election_date'] = '2024-11-05'
        
        # Utah doesn't have filing dates in the source data
        df['filing_date'] = None
        
        return df
