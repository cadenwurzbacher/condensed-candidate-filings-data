import pandas as pd
import logging
import re
from typing import Optional
from .base_cleaner import BaseStateCleaner

logger = logging.getLogger(__name__)

class MassachusettsCleaner(BaseStateCleaner):
    """
    Massachusetts State Cleaner - Phase 3 of new pipeline
    
    Focus: State-specific data cleaning and standardization
    Input: Structured data from Phase 1
    Output: Cleaned and standardized data
    """
    
    def __init__(self):
        super().__init__('massachusetts')
        
        # County mappings removed - not needed
        
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Massachusetts-specific structural issues
        
        Args:
            df: DataFrame to clean
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        logger.info("Cleaning Massachusetts-specific structure")
        
        # Parse names from the Name field
        df = self._parse_names(df)
        
        # Clean district information
        df = self._clean_massachusetts_district(df)
        
        # Clean party information - MOVED TO NATIONAL STANDARDS PHASE
        # df = self._clean_massachusetts_party(df)
        
        # Clean office information
        df = self._clean_massachusetts_office(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Massachusetts-specific content issues
        
        Args:
            df: DataFrame to clean
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        logger.info("Cleaning Massachusetts-specific content")
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Standardize party names - MOVED TO NATIONAL STANDARDS PHASE
        # df = self._standardize_massachusetts_parties(df)
        
        # Standardize office names
        df = self._standardize_massachusetts_offices(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse candidate names into first, middle, last, prefix, suffix components.
        Massachusetts-specific name parsing logic.
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
    
    def _clean_massachusetts_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean district information for Massachusetts"""
        if 'district' in df.columns:
            df['district'] = df['district'].apply(lambda x: self._standardize_district(x))
        return df
    
    def _clean_massachusetts_party(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean party information for Massachusetts"""
        # Party standardization moved to national standards phase
        # if 'party' in df.columns:
        #     df['party'] = df['party'].apply(lambda x: self._standardize_party(x))
        return df
    
    def _clean_massachusetts_office(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean office information for Massachusetts"""
        if 'office' in df.columns:
            df['office'] = df['office'].apply(lambda x: self._standardize_office(x))
        return df
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_massachusetts_address method removed - now handled centrally
    
    def _standardize_district(self, district: str) -> Optional[str]:
        """Standardize district format"""
        if pd.isna(district) or not str(district).strip():
            return None
        
        district_str = str(district).strip()
        
        # Remove common prefixes/suffixes
        district_str = re.sub(r'^(district|dist|d)\s*', '', district_str, flags=re.IGNORECASE)
        district_str = re.sub(r'\s*(district|dist|d)$', '', district_str, flags=re.IGNORECASE)
        
        # Clean up formatting
        district_str = district_str.strip()
        
        return district_str if district_str else None
    
    def _standardize_party(self, party: str) -> Optional[str]:
        """Standardize party names - MOVED TO NATIONAL STANDARDS"""
        # Party standardization moved to national standards phase
        return party
    
    def _standardize_office(self, office: str) -> Optional[str]:
        """Standardize office names - MOVED TO NATIONAL STANDARDS"""
        # Office standardization moved to national standards phase
        return office
    
    def _standardize_address(self, address: str) -> Optional[str]:
        """Standardize address format"""
        if pd.isna(address) or not str(address).strip():
            return None
        
        address_str = str(address).strip()
        
        # Clean up common address issues
        address_str = re.sub(r'\s+', ' ', address_str)  # Multiple spaces to single
        address_str = address_str.strip()
        
        return address_str
    
    def _standardize_massachusetts_parties(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply Massachusetts-specific party standardization - MOVED TO NATIONAL STANDARDS"""
        # Party standardization moved to national standards phase
        return df
    
    def _standardize_massachusetts_offices(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply Massachusetts-specific office standardization - MOVED TO NATIONAL STANDARDS"""
        # Office standardization moved to national standards phase
        return df
