import pandas as pd
import logging
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
