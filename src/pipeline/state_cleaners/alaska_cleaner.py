#!/usr/bin/env python3
"""
Alaska State Data Cleaner

This module contains functions to clean and standardize Alaska political candidate data
according to the final database schema requirements.

Modified for new pipeline structure:
- Receives DataFrame with stable IDs from structural cleaner
- Focuses on data quality improvements only
- No structural parsing or file I/O
"""

import pandas as pd
import re
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlaskaCleaner:
    """
    Handles cleaning and standardization of Alaska political candidate data.
    
    Modified for new pipeline structure:
    - Receives DataFrame with stable IDs from structural cleaner
    - Focuses on data quality improvements only
    - No structural parsing or file I/O
    """
    
    def __init__(self):
        self.state_name = "Alaska"
        
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize Alaska candidate data.
        
        Args:
            df: DataFrame from structural cleaner with stable IDs
            
        Returns:
            Cleaned DataFrame with improved data quality
        """
        logger.info(f"Starting Alaska data cleaning for {len(df)} records")
        
        if df.empty:
            logger.warning("No data to clean")
            return df
        
        # Make a copy to avoid modifying original
        df = df.copy()
        
        # Apply data quality improvements
        df = self._clean_candidate_names(df)
        df = self._clean_office_names(df)
        df = self._clean_party_names(df)
        df = self._clean_addresses(df)
        df = self._clean_county_names(df)
        df = self._standardize_election_data(df)
        
        # Ensure all required columns exist
        df = self._ensure_required_columns(df)
        
        logger.info(f"Alaska data cleaning complete: {len(df)} records")
        return df
    
    def _clean_candidate_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize candidate names"""
        logger.info("Cleaning candidate names")
        
        if 'candidate_name' not in df.columns:
            logger.warning("No candidate_name column found")
            return df
        
        # Clean name formatting
        df['candidate_name'] = df['candidate_name'].apply(self._clean_single_name)
        
        # Extract name components
        df['first_name'] = df['candidate_name'].apply(self._extract_first_name)
        df['middle_name'] = df['candidate_name'].apply(self._extract_middle_name)
        df['last_name'] = df['candidate_name'].apply(self._extract_last_name)
        df['prefix'] = df['candidate_name'].apply(self._extract_prefix)
        df['suffix'] = df['candidate_name'].apply(self._extract_suffix)
        df['nickname'] = df['candidate_name'].apply(self._extract_nickname)
        
        # Build display name
        df['full_name_display'] = df.apply(
            lambda row: self._build_display_name(
                row['first_name'], row['middle_name'], row['last_name'], 
                row['suffix'], row['nickname']
            ), axis=1
        )
        
        return df
    
    def _clean_single_name(self, name: str) -> str:
        """Clean a single candidate name"""
        if pd.isna(name) or not name:
            return ""
        
        name = str(name).strip()
        
        # Remove extra whitespace
        name = re.sub(r'\s+', ' ', name)
        
        # Handle common formatting issues
        name = name.replace('  ', ' ')
        
        return name
    
    def _extract_first_name(self, name: str) -> str:
        """Extract first name from full name"""
        if pd.isna(name) or not name:
            return ""
        
        name = str(name).strip()
        
        # Split by spaces and get first part
        parts = name.split()
        if parts:
            first = parts[0]
            # Skip if it's a prefix
            if first.lower() in ['mr', 'mrs', 'ms', 'dr', 'sen', 'rep', 'gov', 'lt', 'hon']:
                if len(parts) > 1:
                    return parts[1]
                else:
                    return ""
            return first
        
        return ""
    
    def _extract_middle_name(self, name: str) -> str:
        """Extract middle name from full name"""
        if pd.isna(name) or not name:
            return ""
        
        name = str(name).strip()
        parts = name.split()
        
        if len(parts) > 2:
            # Check if middle part is not a suffix
            middle = parts[1]
            if not self._is_suffix(middle):
                return middle
        
        return ""
    
    def _extract_last_name(self, name: str) -> str:
        """Extract last name from full name"""
        if pd.isna(name) or not name:
            return ""
        
        name = str(name).strip()
        parts = name.split()
        
        if len(parts) > 1:
            # Find last non-suffix part
            for i in range(len(parts) - 1, -1, -1):
                if not self._is_suffix(parts[i]):
                    return parts[i]
        
        return ""
    
    def _extract_prefix(self, name: str) -> str:
        """Extract prefix from full name"""
        if pd.isna(name) or not name:
            return ""
        
        name = str(name).strip()
        parts = name.split()
        
        if parts:
            first = parts[0].lower()
            if first in ['mr', 'mrs', 'ms', 'dr', 'sen', 'rep', 'gov', 'lt', 'hon']:
                return parts[0]
        
        return ""
    
    def _extract_suffix(self, name: str) -> str:
        """Extract suffix from full name"""
        if pd.isna(name) or not name:
            return ""
        
        name = str(name).strip()
        parts = name.split()
        
        if len(parts) > 1:
            last = parts[-1].lower()
            if self._is_suffix(last):
                return parts[-1]
        
        return ""
    
    def _extract_nickname(self, name: str) -> str:
        """Extract nickname from full name"""
        if pd.isna(name) or not name:
            return ""
        
        name = str(name).strip()
        
        # Look for quoted nicknames
        nickname_match = re.search(r'["\']([^"\']+)["\']', name)
        if nickname_match:
            return nickname_match.group(1)
        
        return ""
    
    def _is_suffix(self, part: str) -> bool:
        """Check if a name part is a suffix"""
        suffixes = ['jr', 'sr', 'ii', 'iii', 'iv', 'v', 'vi', 'vii', 'viii', 'ix', 'x']
        return part.lower().replace('.', '') in suffixes
    
    def _build_display_name(self, first_name: str, middle_name: str, last_name: str, suffix: str, nickname: str) -> str:
        """Build a display name from components"""
        if not first_name:
            return ""
        
        parts = [first_name]
        
        # Add nickname if present
        if nickname:
            parts.append(f'"{nickname}"')
        
        # Add middle name if present
        if middle_name:
            parts.append(middle_name)
        
        # Add last name if present
        if last_name:
            parts.append(last_name)
        
        # Add suffix if present
        if suffix:
            parts.append(suffix)
        
        return ' '.join(parts).strip()
    
    def _clean_office_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize office names"""
        logger.info("Cleaning office names")
        
        if 'office' not in df.columns:
            logger.warning("No office column found")
            return df
        
        # Standardize common office names
        office_mappings = {
            'GOVERNOR': 'Governor',
            'LT. GOVERNOR': 'Lieutenant Governor',
            'LIEUTENANT GOVERNOR': 'Lieutenant Governor',
            'ATTORNEY GENERAL': 'Attorney General',
            'SECRETARY OF STATE': 'Secretary of State',
            'TREASURER': 'Treasurer',
            'AUDITOR': 'Auditor',
            'US SENATE': 'US Senate',
            'US HOUSE': 'US Representative',
            'REPRESENTATIVE': 'State Representative',
            'SENATOR': 'State Senator'
        }
        
        df['office'] = df['office'].apply(
            lambda x: office_mappings.get(str(x).upper(), x) if pd.notna(x) else x
        )
        
        return df
    
    def _clean_party_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize party names"""
        logger.info("Cleaning party names")
        
        if 'party' not in df.columns:
            logger.warning("No party column found")
            return df
        
        # Standardize party names
        party_mappings = {
            'REPUBLICAN': 'Republican',
            'DEMOCRATIC': 'Democratic',
            'DEMOCRAT': 'Democratic',
            'INDEPENDENT': 'Independent',
            'LIBERTARIAN': 'Libertarian',
            'GREEN': 'Green',
            'NONPARTISAN': 'Nonpartisan',
            'N/A': None,
            '': None
        }
        
        df['party'] = df['party'].apply(
            lambda x: party_mappings.get(str(x).upper(), x) if pd.notna(x) else x
        )
        
        return df
    
    def _clean_addresses(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize addresses"""
        logger.info("Cleaning addresses")
        
        if 'address' not in df.columns:
            logger.warning("No address column found")
            return df
        
        # Clean address formatting
        df['address'] = df['address'].apply(self._clean_single_address)
        
        # Extract city, state, zip if not already present
        if 'city' not in df.columns or df['city'].isna().all():
            df['city'] = df['address'].apply(self._extract_city_from_address)
        
        if 'zip_code' not in df.columns or df['zip_code'].isna().all():
            df['zip_code'] = df['address'].apply(self._extract_zip_from_address)
        
        return df
    
    def _clean_single_address(self, address: str) -> str:
        """Clean a single address"""
        if pd.isna(address) or not address:
            return ""
        
        address = str(address).strip()
        
        # Remove extra whitespace
        address = re.sub(r'\s+', ' ', address)
        
        # Handle common formatting issues
        address = address.replace('  ', ' ')
        
        return address
    
    def _extract_city_from_address(self, address: str) -> str:
        """Extract city from address string"""
        if pd.isna(address) or not address:
            return ""
        
        # Simple city extraction - look for common city patterns
        # This is a basic implementation that can be enhanced
        address = str(address)
        
        # Look for city patterns (e.g., "Anchorage, AK" or "Anchorage AK")
        city_match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*[,]?\s*(?:AK|Alaska)', address)
        if city_match:
            return city_match.group(1)
        
        return ""
    
    def _extract_zip_from_address(self, address: str) -> str:
        """Extract ZIP code from address string"""
        if pd.isna(address) or not address:
            return ""
        
        address = str(address)
        
        # Look for ZIP code patterns
        zip_match = re.search(r'\b\d{5}(?:-\d{4})?\b', address)
        if zip_match:
            return zip_match.group()
        
        return ""
    
    def _clean_county_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize county names"""
        logger.info("Cleaning county names")
        
        if 'county' not in df.columns:
            logger.warning("No county column found")
            return df
        
        # Fix Alaska county capitalization (from ALL CAPS to Proper Case)
        df['county'] = df['county'].apply(
            lambda x: x.title() if pd.notna(x) and str(x).isupper() else x
        )
        
        return df
    
    def _standardize_election_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize election-related data"""
        logger.info("Standardizing election data")
        
        # Ensure election year is consistent
        if 'election_year' in df.columns:
            df['election_year'] = df['election_year'].astype(str)
        
        # Ensure state is consistent
        if 'state' in df.columns:
            df['state'] = 'Alaska'
        
        # Ensure address_state is consistent
        if 'address_state' in df.columns:
            df['address_state'] = df['address_state'].fillna('Alaska')
        
        return df
    
    def _ensure_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all required columns exist"""
        required_columns = [
            'stable_id', 'candidate_name', 'office', 'party', 'county', 'district',
            'address', 'city', 'state', 'zip_code', 'phone', 'email',
            'filing_date', 'election_year', 'election_type', 'address_state',
            'first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname',
            'full_name_display'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = None
        
        return df 