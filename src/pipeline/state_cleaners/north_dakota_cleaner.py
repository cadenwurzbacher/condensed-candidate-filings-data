import pandas as pd
import logging
import re
from typing import Optional
from .base_cleaner import BaseStateCleaner

logger = logging.getLogger(__name__)

class NorthDakotaCleaner(BaseStateCleaner):
    """
    North Dakota State Cleaner - Phase 3 of new pipeline
    
    Focus: State-specific data cleaning and standardization
    Input: Structured data from Phase 1
    Output: Cleaned and standardized data
    """
    
    def __init__(self):
        super().__init__('north_dakota')
        
        # County mappings removed - not needed
        
        # Office mappings removed - handled by national standards
    
    def _clean_state_specific_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean North Dakota-specific structural issues
        
        Args:
            df: DataFrame to clean
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        logger.info("Cleaning North Dakota-specific structure")
        
        # Parse names from the combined name field
        df = self._parse_names(df)
        
        # Clean district information
        df = self._clean_north_dakota_district(df)
        
        # Clean party information - MOVED TO NATIONAL STANDARDS PHASE
        # df = self._clean_north_dakota_party(df)
        
        # Clean office information
        df = self._clean_north_dakota_office(df)
        
        return df
    
    def _clean_state_specific_content(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean North Dakota-specific content issues
        
        Args:
            df: DataFrame to clean
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        logger.info("Cleaning North Dakota-specific content")
        
        # Clean addresses (moved to unified address parser)
        # Address processing now handled in Phase 4 by UnifiedAddressParser
        
        # Standardize party names - MOVED TO NATIONAL STANDARDS PHASE
        # df = self._standardize_north_dakota_parties(df)
        
        # Standardize office names
        df = self._standardize_north_dakota_offices(df)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse candidate names into components using base class standard parsing."""
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname', 'full_name_display']
        for col in name_columns:
            if col not in df.columns:
                df[col] = None

        for idx, row in df.iterrows():
            candidate_name = row.get('candidate_name')
            if pd.notna(candidate_name) and str(candidate_name).strip():
                first, middle, last, prefix, suffix, nickname = self._parse_name_parts(candidate_name)
                df.at[idx, 'first_name'] = first
                df.at[idx, 'middle_name'] = middle
                df.at[idx, 'last_name'] = last
                df.at[idx, 'prefix'] = prefix
                df.at[idx, 'suffix'] = suffix
                df.at[idx, 'nickname'] = nickname
                df.at[idx, 'full_name_display'] = self._build_display_name(first, middle, last, prefix, suffix, nickname)

        return df
    
    def _clean_north_dakota_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean district information for North Dakota"""
        if 'district' in df.columns:
            df['district'] = df['district'].apply(lambda x: self._standardize_district(x))
        return df
    
    def _clean_north_dakota_party(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean party information for North Dakota"""
        # Party standardization moved to national standards phase
        # if 'party' in df.columns:
        #     df['party'] = df['party'].apply(lambda x: self._standardize_party(x))
        return df
    
    def _clean_north_dakota_office(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean office information for North Dakota"""
        if 'office' in df.columns:
            df['office'] = df['office'].apply(lambda x: self._standardize_office(x))
        return df
    
    # Address cleaning moved to UnifiedAddressParser (Phase 4)
    # _clean_north_dakota_address method removed - now handled centrally
    
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
    
    def _standardize_north_dakota_parties(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply North Dakota-specific party standardization - MOVED TO NATIONAL STANDARDS"""
        # Party standardization moved to national standards phase
        return df
    
    def _standardize_north_dakota_offices(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply North Dakota-specific office standardization - MOVED TO NATIONAL STANDARDS"""
        # Office standardization moved to national standards phase
        return df

