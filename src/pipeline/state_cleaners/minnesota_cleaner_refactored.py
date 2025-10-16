#!/usr/bin/env python3
"""
Minnesota State Cleaner - Refactored Version

This refactored version demonstrates how the BaseStateCleaner eliminates
thousands of lines of duplicate code while preserving state-specific logic.
"""

import pandas as pd
import logging
import re
from typing import Dict, Any
from .base_cleaner import BaseStateCleaner

class MinnesotaCleaner(BaseStateCleaner):
    """
    Minnesota-specific data cleaner.
    
    This cleaner handles Minnesota-specific data structure and content,
    while inheriting all common functionality from BaseStateCleaner.
    """
    
    def __init__(self):
        super().__init__("Minnesota")
        
        # County mappings removed - not needed
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Minnesota candidate filing data.
        
        This method orchestrates the entire cleaning process by calling
        the base class methods in the correct order.
        
        Args:
            df: Raw DataFrame from structural cleaner
            
        Returns:
            Cleaned DataFrame with standardized structure
        """
        self.logger.info(f"Starting Minnesota data cleaning")
        return super().clean_data(df)
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Minnesota-specific data structure.
        
        Args:
            df: DataFrame with basic structure
            
        Returns:
            DataFrame with Minnesota-specific cleaning applied
        """
        self.logger.info("Applying Minnesota-specific structural cleaning")
        
        # Clean office names - remove extra whitespace
        if 'office' in df.columns:
            df['office'] = df['office'].str.strip()
        
        # Clean party names - remove extra whitespace
        if 'party' in df.columns:
            df['party'] = df['party'].str.strip()
        
        # Clean candidate names
        if 'candidate_name' in df.columns:
            df['candidate_name'] = df['candidate_name'].str.strip()
        
        # Clean address fields
        if 'address' in df.columns:
            df['address'] = df['address'].str.strip()
            df['address'] = df['address'].replace('', None)
        
        if 'city' in df.columns:
            df['city'] = df['city'].str.strip()
            df['city'] = df['city'].replace('', None)
        
        if 'zip_code' in df.columns:
            df['zip_code'] = df['zip_code'].astype(str)
            df['zip_code'] = df['zip_code'].replace('nan', None)
            df['zip_code'] = df['zip_code'].replace('', None)
        
        # Clean contact info
        if 'phone' in df.columns:
            df['phone'] = df['phone'].astype(str)
            df['phone'] = df['phone'].replace('nan', None)
            df['phone'] = df['phone'].replace('', None)
        
        if 'email' in df.columns:
            df['email'] = df['email'].str.strip()
            df['email'] = df['email'].replace('', None)
        
        if 'website' in df.columns:
            df['website'] = df['website'].str.strip()
            df['website'] = df['website'].replace('', None)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Minnesota-specific content.
        
        This method handles Minnesota-specific data cleaning including:
        - State-specific formatting rules
        
        Args:
            df: DataFrame with standardized structure
            
        Returns:
            DataFrame with Minnesota-specific content cleaned
        """
        self.logger.info("Cleaning Minnesota-specific content")
        
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
        Parse candidate names for Minnesota data.
        
        Minnesota data has names in a single field, so we need to:
        1. Parse first, middle, last names
        2. Handle suffixes
        3. Create full_name_display
        
        Args:
            df: DataFrame with name fields
            
        Returns:
            DataFrame with parsed names
        """
        self.logger.info("Parsing Minnesota candidate names")
        
        if 'candidate_name' not in df.columns:
            return df
        
        # Parse names from the single candidate_name field
        def parse_name(name_str):
            if pd.isna(name_str) or not name_str:
                return None, None, None, None, None
            
            name_str = str(name_str).strip()
            
            # Common suffixes
            suffixes = ['Jr.', 'Sr.', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X']
            
            # Extract suffix
            suffix = None
            for s in suffixes:
                if name_str.endswith(f' {s}'):
                    suffix = s
                    name_str = name_str[:-len(f' {s}')].strip()
                    break
            
            # Split name into parts
            parts = name_str.split()
            
            if len(parts) == 1:
                return parts[0], None, None, None, suffix
            elif len(parts) == 2:
                return parts[0], None, parts[1], None, suffix
            elif len(parts) == 3:
                return parts[0], parts[1], parts[2], None, suffix
            else:
                # More than 3 parts - assume first is first name, last is last name, rest are middle
                return parts[0], ' '.join(parts[1:-1]), parts[-1], None, suffix
        
        # Apply name parsing
        parsed_names = df['candidate_name'].apply(parse_name)
        
        df['first_name'] = parsed_names.apply(lambda x: x[0] if x else None)
        df['middle_name'] = parsed_names.apply(lambda x: x[1] if x else None)
        df['last_name'] = parsed_names.apply(lambda x: x[2] if x else None)
        df['suffix'] = parsed_names.apply(lambda x: x[4] if x else None)
        
        # Create full_name_display
        def create_display_name(row):
            parts = []
            
            if pd.notna(row.get('first_name')):
                parts.append(row['first_name'])
            
            if pd.notna(row.get('middle_name')):
                middle = row['middle_name']
                if len(middle.split()) > 1:
                    # Multiple middle names - abbreviate
                    parts.append(' '.join([m[0] + '.' for m in middle.split()]))
                else:
                    parts.append(middle)
            
            if pd.notna(row.get('last_name')):
                parts.append(row['last_name'])
            
            if pd.notna(row.get('suffix')):
                parts.append(row['suffix'])
            
            return ' '.join(parts) if parts else None
        
        df['full_name_display'] = df.apply(create_display_name, axis=1)
        
        # Fallback to original candidate_name if parsing failed
        mask = df['full_name_display'].isna() & df['candidate_name'].notna()
        df.loc[mask, 'full_name_display'] = df.loc[mask, 'candidate_name']
        
        return df
    
    def _extract_districts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract and clean district information for Minnesota.
        
        Minnesota districts are in the district field, but we need to:
        1. Handle statewide offices (no district)
        2. Clean district formatting
        3. Handle special cases
        
        Args:
            df: DataFrame with office and district info
            
        Returns:
            DataFrame with cleaned district information
        """
        self.logger.info("Extracting Minnesota district information")
        
        if 'district' not in df.columns or 'office' not in df.columns:
            return df
        
        # Define statewide offices (no district)
        statewide_offices = [
            'US President', 'US Senate', 'Governor', 'Lieutenant Governor',
            'Secretary of State', 'State Auditor', 'State Attorney General', 'State Treasurer'
        ]
        
        # Set district to None for statewide offices
        statewide_mask = df['office'].isin(statewide_offices)
        df.loc[statewide_mask, 'district'] = None
        
        # Clean district values for districted offices
        districted_mask = ~statewide_mask & df['district'].notna()
        
        if districted_mask.any():
            # Convert to string and clean
            df.loc[districted_mask, 'district'] = df.loc[districted_mask, 'district'].astype(str)
            
            # Remove leading zeros (e.g., "0149" -> "149")
            df.loc[districted_mask, 'district'] = df.loc[districted_mask, 'district'].str.lstrip('0')
            
            # Handle empty strings after removing zeros
            df.loc[districted_mask, 'district'] = df.loc[districted_mask, 'district'].replace('', None)
            
            # Handle special cases
            df.loc[districted_mask, 'district'] = df.loc[districted_mask, 'district'].replace('nan', None)
            df.loc[districted_mask, 'district'] = df.loc[districted_mask, 'district'].replace('None', None)
        
        return df
    
    def _clean_addresses(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean address information for Minnesota.
        
        Args:
            df: DataFrame with address fields
            
        Returns:
            DataFrame with cleaned address information
        """
        self.logger.info("Cleaning Minnesota address information")
        
        # Minnesota data already has address fields from structural cleaner
        # Just need to clean them up
        
        if 'address' in df.columns:
            df['address'] = df['address'].str.strip()
            df['address'] = df['address'].replace('', None)
        
        if 'city' in df.columns:
            df['city'] = df['city'].str.strip()
            df['city'] = df['city'].replace('', None)
        
        if 'zip_code' in df.columns:
            df['zip_code'] = df['zip_code'].astype(str)
            df['zip_code'] = df['zip_code'].replace('nan', None)
            df['zip_code'] = df['zip_code'].replace('', None)
        
        # County is not provided in Minnesota data, set to None
        df['county'] = None
        
        return df
    
    def _clean_contact_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean contact information for Minnesota.
        
        Args:
            df: DataFrame with contact fields
            
        Returns:
            DataFrame with cleaned contact information
        """
        self.logger.info("Cleaning Minnesota contact information")
        
        # Clean phone numbers
        if 'phone' in df.columns:
            df['phone'] = df['phone'].astype(str)
            df['phone'] = df['phone'].replace('nan', None)
            df['phone'] = df['phone'].replace('', None)
            
            # Format phone numbers (remove any non-digit characters)
            mask = df['phone'].notna()
            df.loc[mask, 'phone'] = df.loc[mask, 'phone'].str.replace(r'\D', '', regex=True)
            
            # Format as (XXX) XXX-XXXX if 10 digits
            mask = df['phone'].str.len() == 10
            df.loc[mask, 'phone'] = df.loc[mask, 'phone'].apply(
                lambda x: f"({x[:3]}) {x[3:6]}-{x[6:]}" if len(x) == 10 else x
            )
        
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
        
        return df
    
    def _add_election_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add election information for Minnesota.
        
        Args:
            df: DataFrame to add election info to
            
        Returns:
            DataFrame with election information added
        """
        self.logger.info("Adding Minnesota election information")
        
        # Election year is already extracted from filename in structural cleaner
        # Set election type (general election)
        df['election_type'] = 'General'
        
        # Set election date (November 4, 2025 for general election)
        df['election_date'] = '2025-11-04'
        
        # Minnesota doesn't have filing dates in the source data
        df['filing_date'] = None
        
        return df
