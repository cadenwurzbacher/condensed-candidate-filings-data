import pandas as pd
import logging
import re
from typing import Optional
from .base_cleaner import BaseStateCleaner

logger = logging.getLogger(__name__)

class HawaiiCleaner(BaseStateCleaner):
    """
    Hawaii State Cleaner - Phase 3 of new pipeline
    
    Focus: State-specific data cleaning and standardization
    Input: Structured data from Phase 1
    Output: Cleaned and standardized data
    """
    
    def __init__(self):
        super().__init__('hawaii')
        
        # County mappings removed - not needed
        
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Hawaii-specific structural issues
        
        Args:
            df: DataFrame to clean
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        logger.info("Cleaning Hawaii-specific structure")
        
        # Parse names from the ballot name field
        df = self._parse_names(df)
        
        # Clean party information - MOVED TO NATIONAL STANDARDS PHASE
        # df = self._clean_hawaii_party(df)
        
        # Note: Office standardization is handled by national standards phase
        # Do NOT standardize offices here
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Hawaii-specific content issues
        
        Args:
            df: DataFrame to clean
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        logger.info("Cleaning Hawaii-specific content")
        
        # Standardize party names - MOVED TO NATIONAL STANDARDS PHASE
        # df = self._standardize_hawaii_parties(df)
        
        # Note: Office standardization is handled by national standards phase
        # Do NOT standardize offices here
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix components.
        Hawaii-specific name parsing logic using ballot_name field.
        """
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None

        if 'candidate_name' in df.columns:
            df['full_name_display'] = df['candidate_name']

        for idx, row in df.iterrows():
            candidate_name = row.get('candidate_name')
            if pd.notna(candidate_name) and str(candidate_name).strip():
                name_str = str(candidate_name).strip()
                
                # Handle Hawaii-specific name format: "LAST, FIRST (Nickname)"
                # Example: "ACAIN, Aileen R. (Lily)"
                
                # Extract nickname if present
                nickname_match = re.search(r'\(([^)]+)\)', name_str)
                if nickname_match:
                    nickname = nickname_match.group(1)
                    df.at[idx, 'nickname'] = nickname
                    name_str = re.sub(r'\s*\([^)]+\)', '', name_str)  # Remove nickname
                
                # Split by comma to separate last name from first/middle
                if ',' in name_str:
                    parts = [p.strip() for p in name_str.split(',', 1)]
                    if len(parts) == 2:
                        last_name = parts[0]
                        first_middle = parts[1]
                        
                        # Set last name
                        df.at[idx, 'last_name'] = last_name
                        
                        # Parse first and middle names
                        name_parts = [p.strip() for p in first_middle.split() if p.strip()]
                        if len(name_parts) >= 1:
                            df.at[idx, 'first_name'] = name_parts[0]
                        if len(name_parts) > 1:
                            df.at[idx, 'middle_name'] = ' '.join(name_parts[1:])
                else:
                    # Fallback to standard parsing if no comma
                    # Handle suffixes
                    suffix_pattern = r'\b(Jr|Sr|II|III|IV|V|VI|VII|VIII|IX|X)\b'
                    suffix_match = re.search(suffix_pattern, name_str, re.IGNORECASE)
                    if suffix_match:
                        suffix = suffix_match.group(1)
                        df.at[idx, 'suffix'] = suffix
                        name_str = re.sub(suffix_pattern, '', name_str, flags=re.IGNORECASE).strip()
                    else:
                        df.at[idx, 'suffix'] = None

                    # Handle prefixes
                    prefix_pattern = r'^(Dr|Mr|Mrs|Ms|Miss|Prof|Rev|Hon|Sen|Rep|Gov|Lt|Col|Gen|Adm|Capt|Maj|Sgt|Cpl|Pvt)\.?\s+'
                    prefix_match = re.match(prefix_pattern, name_str, re.IGNORECASE)
                    if prefix_match:
                        prefix = prefix_match.group(1)
                        df.at[idx, 'prefix'] = prefix
                        name_str = re.sub(prefix_pattern, '', name_str, flags=re.IGNORECASE).strip()
                    else:
                        df.at[idx, 'prefix'] = None

                    # Split remaining name parts
                    parts = [p.strip() for p in name_str.split() if p.strip()]

                    if len(parts) >= 1:
                        df.at[idx, 'first_name'] = parts[0]
                    if len(parts) >= 2:
                        df.at[idx, 'last_name'] = parts[-1]
                    if len(parts) > 2:
                        df.at[idx, 'middle_name'] = ' '.join(parts[1:-1])
        
        return df
    
    def _clean_hawaii_party(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean party information for Hawaii"""
        # Party standardization moved to national standards phase
        # if 'party' in df.columns:
        #     df['party'] = df['party'].apply(lambda x: self._standardize_party(x))
        return df
    
    def _clean_hawaii_office(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean office information for Hawaii"""
        if 'office' in df.columns:
            df['office'] = df['office'].apply(lambda x: self._standardize_office(x))
        return df
    
    def _standardize_party(self, party: str) -> Optional[str]:
        """Standardize party names - MOVED TO NATIONAL STANDARDS"""
        # Party standardization moved to national standards phase
        return party
    
    def _standardize_office(self, office: str) -> Optional[str]:
        """Standardize office names - MOVED TO NATIONAL STANDARDS"""
        # Office standardization moved to national standards phase
        return office
    
    def _standardize_hawaii_parties(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply Hawaii-specific party standardization - MOVED TO NATIONAL STANDARDS"""
        # Party standardization moved to national standards phase
        return df
    
    def _standardize_hawaii_offices(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply Hawaii-specific office standardization - MOVED TO NATIONAL STANDARDS"""
        # Office standardization moved to national standards phase
        return df
