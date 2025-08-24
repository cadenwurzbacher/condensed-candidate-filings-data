import pandas as pd
import logging
import re
from typing import Dict, List, Tuple
import hashlib

# Import processors
from .office_standardizer import OfficeStandardizer
from .election_type_standardizer import ElectionTypeStandardizer

# Import database utilities
from ..config.database import get_db_connection

logger = logging.getLogger(__name__)

class NationalStandards:
    """
    National Standards - Phase 4 of new pipeline
    
    Focus: Apply consistent standards across all states
    - Case standardization (proper case for names, offices, counties)
    - Preserve important acronyms (US, JD, etc.)
    - Data deduplication and quality improvements
    """
    
    def __init__(self):
        # Specific acronyms that should remain uppercase
        self.preserve_uppercase = {
            'JD', 'MD', 'PhD', 'MBA', 'CPA', 'Esq', 'Jr', 'Sr', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'US'
        }
        
        # Office patterns that should preserve certain uppercase elements
        self.office_patterns = {
        }
        self.preserve_uppercase.add('US')
        
        # Columns that should be proper case (first letter of each word capitalized)
        self.proper_case_columns = [
            'candidate_name',
            'office', 
            'county',
            'city',
            'district'
        ]
        
        # Columns that should be title case (first letter of each word capitalized, rest lowercase)
        self.title_case_columns = [
            'party'
        ]
    
    def apply_standards(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all national standards to the dataset
        
        Args:
            df: Input DataFrame with all state data
            
        Returns:
            pd.DataFrame: Standardized data
        """
        logger.info("Applying national standards")
        
        if df.empty:
            logger.warning("Empty DataFrame, skipping national standards")
            return df
        
        # Make a copy to avoid modifying original
        df_standardized = df.copy()
        
        # Apply smart case standardization
        df_standardized = self._apply_smart_case_standardization(df_standardized)
        
        # Dedupe statewide candidates (remove county-based duplicates)
        df_standardized = self._dedupe_statewide_candidates(df_standardized)
        
        # Standardize election types to binary columns
        df_standardized = self._standardize_election_types(df_standardized)
        
        # Apply final deduplication based on stable IDs
        df_standardized = self._dedupe_by_stable_id(df_standardized)
        
        logger.info("National standards applied successfully")
        return df_standardized
    
    def _apply_smart_case_standardization(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply smart case standardization to appropriate columns"""
        logger.info("Applying case standardization")
        
        # Apply proper case to specified columns
        for col in self.proper_case_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: self._smart_proper_case(x) if pd.notna(x) else x)
                logger.info(f"Applied proper case to {col}")
        
        # Apply title case to specified columns
        for col in self.title_case_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: self._smart_title_case(x) if pd.notna(x) else x)
                logger.info(f"Applied title case to {col}")
        
        return df
    
    def _smart_proper_case(self, text: str) -> str:
        """
        Apply smart proper case while preserving important acronyms
        
        Args:
            text: Input text string
            
        Returns:
            str: Properly cased text
        """
        if not isinstance(text, str) or not text.strip():
            return text
        
        # Handle special cases first
        text = self._preserve_important_acronyms(text)
        
        # Apply proper case to the rest
        words = text.split()
        cased_words = []
        
        for word in words:
            if self._should_preserve_case(word):
                cased_words.append(word)  # Keep as-is
            else:
                cased_words.append(word.capitalize())
        
        return ' '.join(cased_words)
    
    def _smart_title_case(self, text: str) -> str:
        """
        Apply smart title case (first letter of each word capitalized, rest lowercase)
        
        Args:
            text: Input text string
            
        Returns:
            str: Title cased text
        """
        if not isinstance(text, str) or not text.strip():
            return text
        
        # Handle special cases first
        text = self._preserve_important_acronyms(text)
        
        # Apply title case to the rest
        words = text.split()
        cased_words = []
        
        for word in words:
            if self._should_preserve_case(word):
                cased_words.append(word)  # Keep as-is
            else:
                cased_words.append(word.capitalize())
        
        return ' '.join(cased_words)
    
    def _preserve_important_acronyms(self, text: str) -> str:
        """
        Preserve important acronyms and patterns in text
        
        Args:
            text: Input text string
            
        Returns:
            str: Text with preserved acronyms
        """
        # Preserve US in specific office contexts
        for pattern, replacement in self.office_patterns.items():
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        
        # Preserve common name initials and suffixes
        for acronym in self.preserve_uppercase:
            text = re.sub(rf'\b{acronym}\b', acronym, text, flags=re.IGNORECASE)
        
        return text
    
    def _should_preserve_case(self, word: str) -> bool:
        """
        Determine if a word should preserve its current case
        
        Args:
            word: Input word
            
        Returns:
            bool: True if case should be preserved
        """
        # Only preserve specific acronyms and patterns, not all uppercase words
        
        # Preserve specific acronyms
        if word.upper() in self.preserve_uppercase:
            return True
        
        # Preserve specific patterns
        preserve_patterns = [
            r'^\d+$',  # Numbers
            r'^\d+[A-Z]$',  # Numbers followed by letter (e.g., 1A, 2B)
            r'^[A-Z]\d+$',  # Letter followed by numbers (e.g., A1, B2)
            r'^\d+\.\d+$',  # Decimal numbers
            r'^\d+-\d+$',   # Number ranges
        ]
        
        for pattern in preserve_patterns:
            if re.match(pattern, word):
                return True
        
        return False

    def _dedupe_statewide_candidates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Dedupe statewide candidates that appear in multiple counties
        
        This detects records that are identical except for county values,
        which indicates statewide candidates (Governor, US Senator, etc.)
        """
        logger.info("Deduplicating statewide candidates...")
        
        if df.empty:
            return df
        
        # Get all columns except county and stable_id (since stable_id includes county)
        dedupe_columns = [col for col in df.columns if col not in ['county', 'stable_id']]
        
        # Find records that are duplicates except for county
        county_duplicates = df.duplicated(subset=dedupe_columns, keep=False)
        
        if county_duplicates.sum() == 0:
            logger.info("No county-based duplicates found")
            return df
        
        logger.info(f"Found {county_duplicates.sum()} records with county-based duplicates")
        
        # Create a copy to work with
        result_df = df.copy()
        
        # Group by all columns except county and stable_id
        grouped = df[county_duplicates].groupby(dedupe_columns)
        
        removed_count = 0
        
        for group_key, group_df in grouped:
            if len(group_df) > 1:
                # This is a group of duplicates - keep the first one, remove county
                first_record_idx = group_df.index[0]
                result_df.loc[first_record_idx, 'county'] = None
                
                # Remove all other duplicates
                duplicate_indices = group_df.index[1:]
                result_df = result_df.drop(duplicate_indices)
                
                removed_count += len(group_df) - 1
                
                logger.debug(f"Deduped {len(group_df)} records for {group_df.iloc[0].get('full_name_display', 'Unknown')} - {group_df.iloc[0].get('office', 'Unknown')}")
        
        logger.info(f"Removed {removed_count} county-based duplicate records")
        logger.info(f"Records reduced from {len(df)} to {len(result_df)}")
        
        return result_df
    
    def _standardize_election_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize election types to binary columns
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with standardized election type columns
        """
        logger.info("Standardizing election types...")
        
        # Initialize the election type standardizer
        election_standardizer = ElectionTypeStandardizer()
        
        # Apply standardization
        result_df = election_standardizer.standardize_election_types(df)
        
        # Get summary for logging
        summary = election_standardizer.get_election_summary(result_df)
        
        logger.info(f"Election type standardization complete:")
        logger.info(f"  Primary: {summary.get('primary', 0):,}")
        logger.info(f"  General: {summary.get('general', 0):,}")
        logger.info(f"  Special: {summary.get('special', 0):,}")
        logger.info(f"  Primary + General: {summary.get('primary_and_general', 0):,}")
        
        return result_df
    
    def _dedupe_by_stable_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate records based on stable_id
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with duplicates removed
        """
        logger.info("Removing duplicates based on stable_id...")
        
        if df.empty:
            return df
        
        original_count = len(df)
        
        # Remove duplicates based on stable_id, keeping the first occurrence
        df_deduped = df.drop_duplicates(subset=['stable_id'], keep='first')
        
        removed_count = original_count - len(df_deduped)
        
        if removed_count > 0:
            logger.info(f"Removed {removed_count} duplicate records based on stable_id")
            logger.info(f"Records reduced from {original_count} to {len(df_deduped)}")
        else:
            logger.info("No duplicate stable_ids found")
        
        return df_deduped
    
    def _generate_stable_id(self, name: str, state: str, office: str, election_year: str, county: str = None) -> str:
        """
        Generate stable ID for a candidate
        
        Args:
            name: Candidate name
            state: State
            office: Office
            election_year: Election year
            county: County (optional, set to None for statewide candidates)
            
        Returns:
            Stable ID string
        """
        # Clean and standardize inputs
        clean_name = str(name).strip().upper() if pd.notna(name) else ""
        clean_state = str(state).strip().upper() if pd.notna(state) else ""
        clean_office = str(office).strip().upper() if pd.notna(office) else ""
        clean_election = str(election_year).strip() if pd.notna(election_year) else ""
        
        # Only include county if it's not None and not empty
        county_part = f"_{str(county).strip().upper()}" if county and pd.notna(county) and str(county).strip() else ""
        
        # Create stable ID string
        stable_id_string = f"{clean_name}_{clean_state}_{clean_office}_{clean_election}{county_part}"
        
        # Generate MD5 hash
        stable_id = hashlib.md5(stable_id_string.encode('utf-8')).hexdigest()
        
        return stable_id
