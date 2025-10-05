#!/usr/bin/env python3
"""
Data Processor for CandidateFilings.com Pipeline

This module handles all data processing operations for the pipeline,
including structural cleaning, state cleaning, and national standardization.
"""

import pandas as pd
import logging
import re
from typing import Dict
from datetime import datetime

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Handles all data processing operations for the pipeline.
    
    This class manages:
    - Structural cleaning (Phase 1)
    - State cleaning (Phase 3)
    - National standardization (Phase 4)
    - Final processing (Phase 5)
    - Data transformation and validation
    """
    
    def __init__(self, config):
        """
        Initialize the data processor.
        
        Args:
            config: PipelineConfig instance
        """
        self.config = config
        
        # Initialize processors based on configuration
        self.office_standardizer = None
        self.party_standardizer = None
        self.address_parser = None
        self.national_standards = None
        
        if self.config.enable_office_standardization:
            from ..office_standardizer import OfficeStandardizer
            self.office_standardizer = OfficeStandardizer()
        
        if self.config.enable_party_standardization:
            from ..party_standardizer import PartyStandardizer
            self.party_standardizer = PartyStandardizer()
        
        if self.config.enable_address_parsing:
            from ..address_parser import UnifiedAddressParser
            self.address_parser = UnifiedAddressParser()
        
        if self.config.enable_phase_4_national_standards:
            from ..national_standards import NationalStandards
            self.national_standards = NationalStandards()
        
        # Initialize structural and state cleaners
        self._initialize_cleaners()
    
    def _initialize_cleaners(self):
        """Initialize structural and state cleaners."""
        try:
            # Import structural cleaners
            from ..structural_cleaners import (
                AlaskaStructuralCleaner, ArizonaStructuralCleaner, ArkansasStructuralCleaner,
                ColoradoStructuralCleaner, DelawareStructuralCleaner, FloridaStructuralCleaner,
                GeorgiaStructuralCleaner, HawaiiStructuralCleaner, IdahoStructuralCleaner, IllinoisStructuralCleaner,
                IndianaStructuralCleaner, IowaStructuralCleaner, KansasStructuralCleaner,
                KentuckyStructuralCleaner, LouisianaStructuralCleaner, MarylandStructuralCleaner,
                MassachusettsStructuralCleaner, MissouriStructuralCleaner, MontanaStructuralCleaner,
                NebraskaStructuralCleaner, NewMexicoStructuralCleaner, NewYorkStructuralCleaner,
                NorthCarolinaStructuralCleaner, NorthDakotaStructuralCleaner, OklahomaStructuralCleaner, OregonStructuralCleaner,
                PennsylvaniaStructuralCleaner, SouthCarolinaStructuralCleaner, SouthDakotaStructuralCleaner,
                VermontStructuralCleaner, VirginiaStructuralCleaner, WashingtonStructuralCleaner, WestVirginiaStructuralCleaner,
                WyomingStructuralCleaner
            )
            
            # Import state cleaners individually since __init__.py doesn't export them
            from ..state_cleaners.alaska_cleaner_refactored import AlaskaCleaner
            from ..state_cleaners.arizona_cleaner_refactored import ArizonaCleaner
            from ..state_cleaners.arkansas_cleaner_refactored import ArkansasCleaner
            from ..state_cleaners.colorado_cleaner_refactored import ColoradoCleaner
            from ..state_cleaners.delaware_cleaner_refactored import DelawareCleaner
            from ..state_cleaners.florida_cleaner_refactored import FloridaCleaner
            from ..state_cleaners.georgia_cleaner_refactored import GeorgiaCleaner
            from ..state_cleaners.hawaii_cleaner_refactored import HawaiiCleaner
            from ..state_cleaners.idaho_cleaner_refactored import IdahoCleaner
            from ..state_cleaners.illinois_cleaner_refactored import IllinoisCleaner
            from ..state_cleaners.indiana_cleaner_refactored import IndianaCleaner
            from ..state_cleaners.iowa_cleaner_refactored import IowaCleaner
            from ..state_cleaners.kansas_cleaner_refactored import KansasCleaner
            from ..state_cleaners.kentucky_cleaner_refactored import KentuckyCleaner
            from ..state_cleaners.louisiana_cleaner_refactored import LouisianaCleaner
            from ..state_cleaners.maryland_cleaner_refactored import MarylandCleaner
            from ..state_cleaners.massachusetts_cleaner_refactored import MassachusettsCleaner
            from ..state_cleaners.missouri_cleaner_refactored import MissouriCleaner
            from ..state_cleaners.montana_cleaner_refactored import MontanaCleaner
            from ..state_cleaners.nebraska_cleaner_refactored import NebraskaCleaner
            from ..state_cleaners.new_mexico_cleaner_refactored import NewMexicoCleaner
            from ..state_cleaners.new_york_cleaner_refactored import NewYorkCleaner
            from ..state_cleaners.north_carolina_cleaner_refactored import NorthCarolinaCleaner
            from ..state_cleaners.oklahoma_cleaner_refactored import OklahomaCleaner
            from ..state_cleaners.oregon_cleaner_refactored import OregonCleaner
            from ..state_cleaners.pennsylvania_cleaner_refactored import PennsylvaniaCleaner
            from ..state_cleaners.south_carolina_cleaner_refactored import SouthCarolinaCleaner
            from ..state_cleaners.south_dakota_cleaner_refactored import SouthDakotaCleaner
            from ..state_cleaners.vermont_cleaner_refactored import VermontCleaner
            from ..state_cleaners.virginia_cleaner_refactored import VirginiaCleaner
            from ..state_cleaners.washington_cleaner_refactored import WashingtonCleaner
            from ..state_cleaners.west_virginia_cleaner_refactored import WestVirginiaCleaner
            from ..state_cleaners.wyoming_cleaner_refactored import WyomingCleaner
            from ..state_cleaners.north_dakota_cleaner_refactored import NorthDakotaCleaner
            
            # Structural cleaner mapping
            self.structural_cleaners = {
                'alaska': AlaskaStructuralCleaner(),
                'arizona': ArizonaStructuralCleaner(),
                'arkansas': ArkansasStructuralCleaner(),
                'colorado': ColoradoStructuralCleaner(),
                'delaware': DelawareStructuralCleaner(),
                'florida': FloridaStructuralCleaner(),
                'georgia': GeorgiaStructuralCleaner(),
                'hawaii': HawaiiStructuralCleaner(),
                'idaho': IdahoStructuralCleaner(),
                'illinois': IllinoisStructuralCleaner(),
                'indiana': IndianaStructuralCleaner(),
                'iowa': IowaStructuralCleaner(),
                'kansas': KansasStructuralCleaner(),
                'kentucky': KentuckyStructuralCleaner(),
                'louisiana': LouisianaStructuralCleaner(),
                'maryland': MarylandStructuralCleaner(),
                'massachusetts': MassachusettsStructuralCleaner(),
                'missouri': MissouriStructuralCleaner(),
                'montana': MontanaStructuralCleaner(),
                'nebraska': NebraskaStructuralCleaner(),
                'new_mexico': NewMexicoStructuralCleaner(),
                'new_york': NewYorkStructuralCleaner(),
                'north_carolina': NorthCarolinaStructuralCleaner(),
                'oklahoma': OklahomaStructuralCleaner(),
                'oregon': OregonStructuralCleaner(),
                'pennsylvania': PennsylvaniaStructuralCleaner(),
                'south_carolina': SouthCarolinaStructuralCleaner(),
                'vermont': VermontStructuralCleaner(),
                'virginia': VirginiaStructuralCleaner(),
                'washington': WashingtonStructuralCleaner(),
                'west_virginia': WestVirginiaStructuralCleaner(),
                'wyoming': WyomingStructuralCleaner(),
                'south_dakota': SouthDakotaStructuralCleaner(),
                'north_dakota': NorthDakotaStructuralCleaner(),
            }
            
            # State cleaner mapping
            self.state_cleaners = {
                'alaska': AlaskaCleaner(),
                'arizona': ArizonaCleaner(),
                'arkansas': ArkansasCleaner(),
                'colorado': ColoradoCleaner(),
                'delaware': DelawareCleaner(),
                'florida': FloridaCleaner(),
                'georgia': GeorgiaCleaner(),
                'hawaii': HawaiiCleaner(),
                'idaho': IdahoCleaner(),
                'illinois': IllinoisCleaner(),
                'indiana': IndianaCleaner(),
                'iowa': IowaCleaner(),
                'kansas': KansasCleaner(),
                'kentucky': KentuckyCleaner(),
                'louisiana': LouisianaCleaner(),
                'maryland': MarylandCleaner(),
                'massachusetts': MassachusettsCleaner(),
                'missouri': MissouriCleaner(),
                'montana': MontanaCleaner(),
                'nebraska': NebraskaCleaner(),
                'new_mexico': NewMexicoCleaner(),
                'new_york': NewYorkCleaner(),
                'north_carolina': NorthCarolinaCleaner(),
                'oklahoma': OklahomaCleaner(),
                'oregon': OregonCleaner(),
                'pennsylvania': PennsylvaniaCleaner(),
                'south_carolina': SouthCarolinaCleaner(),
                'south_dakota': SouthDakotaCleaner(),
                'vermont': VermontCleaner(),
                'virginia': VirginiaCleaner(),
                'washington': WashingtonCleaner(),
                'west_virginia': WestVirginiaCleaner(),
                'wyoming': WyomingCleaner(),
                'north_dakota': NorthDakotaCleaner()
            }
            
            logger.info(f"Initialized {len(self.structural_cleaners)} structural cleaners")
            logger.info(f"Initialized {len(self.state_cleaners)} state cleaners")
            
        except Exception as e:
            logger.error(f"Failed to initialize cleaners: {e}")
            self.structural_cleaners = {}
            self.state_cleaners = {}
    
    def run_structural_cleaners(self, raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Run structural cleaners (Phase 1).
        
        Args:
            raw_data: Dictionary mapping state names to raw DataFrames
            
        Returns:
            Dictionary mapping state names to structured DataFrames
        """
        if not self.config.enable_phase_1_structural:
            logger.info("Phase 1: Skipped (disabled)")
            return raw_data
        
        logger.info("Phase 1: Running structural cleaners")
        
        structured_data = {}
        
        for state, df in raw_data.items():
            try:
                if state in self.structural_cleaners:
                    logger.info(f"Running structural cleaner for {state}")
                    cleaner = self.structural_cleaners[state]
                    
                    # Set the data directory for the cleaner
                    cleaner.data_dir = self.config.data_dir
                    
                    # Run the cleaner with DataFrame (new pipeline)
                    if hasattr(cleaner, 'clean_dataframe'):
                        structured_df = cleaner.clean_dataframe(df)
                    else:
                        # Fallback to old file-based method
                        structured_df = cleaner.clean()
                    structured_data[state] = structured_df
                    
                    logger.info(f"Structural cleaning complete for {state}: {len(structured_df)} records")
                else:
                    logger.warning(f"No structural cleaner found for {state}, using raw data")
                    structured_data[state] = df
                    
            except Exception as e:
                logger.error(f"Structural cleaning failed for {state}: {e}")
                if not self.config.continue_on_state_error:
                    raise
                # Use raw data if cleaning fails
                structured_data[state] = df
                continue
        
        logger.info(f"Phase 1 complete: {len(structured_data)} states processed")
        return structured_data
    
    def generate_stable_ids(self, structured_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Generate stable IDs (Phase 2).
        
        Args:
            structured_data: Dictionary mapping state names to structured DataFrames
            
        Returns:
            Dictionary mapping state names to DataFrames with stable IDs
        """
        if not self.config.enable_phase_2_id_generation:
            logger.info("Phase 2: Skipped (disabled)")
            return structured_data
        
        logger.info("Phase 2: Generating stable IDs")
        
        # Track existing IDs for first ingestion date preservation
        self.existing_ids = {}
        self.duplicate_count = 0
        
        for state, df in structured_data.items():
            try:
                logger.info(f"Generating stable IDs for {state}")
                df_with_ids = self._add_stable_ids_to_dataframe(df, state)
                structured_data[state] = df_with_ids
                logger.info(f"ID generation complete for {state}")
            except Exception as e:
                logger.error(f"ID generation failed for {state}: {e}")
                if not self.config.continue_on_state_error:
                    raise
                continue
        
        logger.info("Phase 2 complete: Stable IDs generated for all states")
        if self.duplicate_count > 0:
            logger.warning(f"Found {self.duplicate_count} duplicate stable IDs across all states")
        return structured_data
    
    def _add_stable_ids_to_dataframe(self, df: pd.DataFrame, state: str) -> pd.DataFrame:
        """Add stable IDs to a single state's dataframe."""
        df = df.copy()
        
        # Filter out records with missing critical data BEFORE stable ID generation
        logger.info(f"Filtering records with missing critical data for {state}...")
        initial_count = len(df)
        
        # Remove records with null/empty candidate_name, office, or election_year
        df = df.dropna(subset=['candidate_name', 'office', 'election_year'])
        
        # Also remove records with empty strings and invalid names
        invalid_names = [
            'nan', 'none', 'null', '', 'no nominations', 'no nomination',
            'vacant', 'unopposed', 'write-in', 'write in', 'writein',
            'no candidate', 'no candidates', 'no candidate filed',
            'no candidates filed', 'no filing', 'no filings'
        ]
        
        df = df[
            (df['candidate_name'].str.strip() != '') & 
            (df['office'].str.strip() != '') & 
            (df['election_year'].notna()) &
            (~df['candidate_name'].str.lower().isin([name.lower() for name in invalid_names]))
        ]
        
        filtered_count = len(df)
        removed_count = initial_count - filtered_count
        
        if removed_count > 0:
            logger.warning(f"Removed {removed_count} records with missing critical data in {state}")
            logger.info(f"Proceeding with {filtered_count} valid records for stable ID generation")
        
        if df.empty:
            logger.warning(f"No valid records remaining for {state} after filtering")
            # Return empty DataFrame with required columns
            df['stable_id'] = []
            df['first_added_date'] = []
            df['last_updated_date'] = []
            df['action_type'] = []
            return df
        
        # Generate stable IDs based on core candidate data
        valid_records = []
        stable_ids = []
        
        for idx, row in df.iterrows():
            try:
                # Create stable ID from core fields (before any cleaning)
                stable_id, _ = self._generate_stable_id(row, state)
                valid_records.append(row.to_dict())  # Convert to dict to avoid index issues
                stable_ids.append(stable_id)
                    
            except Exception as e:
                logger.warning(f"Failed to generate ID for row {idx} in {state}: {e}")
                # Skip this record entirely
                continue
        
        if not valid_records:
            logger.warning(f"No valid records remaining for {state} after ID generation")
            # Return empty DataFrame with required columns
            return pd.DataFrame(columns=df.columns.tolist() + ['stable_id', 'first_added_date', 'last_updated_date', 'action_type'])
        
        # Create new DataFrame with only valid records and reset index
        df = pd.DataFrame(valid_records).reset_index(drop=True)
        df['stable_id'] = stable_ids
        
        # Add placeholder columns (will be populated in Phase 5)
        df['first_added_date'] = None
        df['last_updated_date'] = None
        df['action_type'] = 'INSERT'  # Default, will be updated in Phase 5
        
        return df
    
    def _generate_stable_id(self, row: pd.Series, state: str) -> tuple:
        """Generate a stable ID for a candidate record."""
        import hashlib
        
        # Use core fields for ID generation (before any cleaning/standardization)
        candidate_name = str(row.get('candidate_name', '')).strip()
        office = str(row.get('office', '')).strip()
        election_year_raw = row.get('election_year')
        
        # Handle election_year conversion (float to string)
        if pd.isna(election_year_raw):
            election_year = ''
        elif isinstance(election_year_raw, (int, float)):
            election_year = str(int(election_year_raw))  # Convert 2016.0 to "2016"
        else:
            election_year = str(election_year_raw).strip()
        
        # Additional validation - these should have been filtered out already
        if not candidate_name or not office or not election_year:
            raise ValueError(f"Missing required fields: name='{candidate_name}', office='{office}', year='{election_year}'")
        
        # Create deterministic hash with normalized values
        normalized_name = candidate_name.lower().strip()
        normalized_office = office.lower().strip()
        normalized_state = state.lower().strip()
        normalized_year = election_year.strip()
        
        key = f"{normalized_name}|{normalized_state}|{normalized_office}|{normalized_year}"
        stable_id = hashlib.md5(key.encode()).hexdigest()[:12]
        
        # Check if we've seen this ID before
        if stable_id in self.existing_ids:
            # Use existing first_ingestion_date
            first_ingestion_date = self.existing_ids[stable_id]
            self.duplicate_count += 1
        else:
            # New candidate - set current timestamp
            first_ingestion_date = datetime.now()
            self.existing_ids[stable_id] = first_ingestion_date
        
        return stable_id, first_ingestion_date
    
    def run_state_cleaners(self, structured_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Run state cleaners (Phase 3).
        
        Args:
            structured_data: Dictionary mapping state names to structured DataFrames
            
        Returns:
            Dictionary mapping state names to cleaned DataFrames
        """
        if not self.config.enable_phase_3_state_cleaning:
            logger.info("Phase 3: Skipped (disabled)")
            return structured_data
        
        logger.info("Phase 3: Running state data cleaners")
        
        cleaned_data = {}
        
        for state, df in structured_data.items():
            logger.info(f"Processing state cleaner for {state}")
            if state in self.state_cleaners and self.state_cleaners[state] is not None:
                try:
                    logger.info(f"Running data cleaner for {state}")
                    cleaner = self.state_cleaners[state]
                    
                    # Call the clean_data method from BaseStateCleaner
                    if hasattr(cleaner, 'clean_data'):
                        cleaned_df = cleaner.clean_data(df)
                    else:
                        # Fallback to alternative methods if clean_data doesn't exist
                        method_name = f"clean_{state}_data"
                        if hasattr(cleaner, method_name):
                            cleaned_df = getattr(cleaner, method_name)(df)
                        else:
                            # Try other alternative method names
                            alt_methods = [
                                f"clean_{state}_candidates",
                                "clean",
                                "process"
                            ]
                            method_found = False
                            for alt_method in alt_methods:
                                if hasattr(cleaner, alt_method):
                                    cleaned_df = getattr(cleaner, alt_method)(df)
                                    method_found = True
                                    break
                            if not method_found:
                                logger.warning(f"No cleaning method found for {state}, using original data")
                                cleaned_df = df
                    
                    cleaned_data[state] = cleaned_df
                    logger.info(f"Data cleaning complete for {state}: {len(cleaned_df)} records")
                    
                except Exception as e:
                    logger.error(f"Data cleaning failed for {state}: {e}")
                    if not self.config.continue_on_state_error:
                        raise
                    # Use structured data if cleaning fails
                    cleaned_data[state] = df
            else:
                # No data cleaner available, use structured data
                cleaned_data[state] = df
        
        logger.info(f"Phase 3 complete: {len(cleaned_data)} states processed")
        return cleaned_data
    
    def apply_national_standards(self, cleaned_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Apply national standards (Phase 4).
        
        Args:
            cleaned_data: Dictionary mapping state names to cleaned DataFrames
            
        Returns:
            Combined DataFrame with national standards applied
        """
        if not self.config.enable_phase_4_national_standards:
            logger.info("Phase 4: Skipped (disabled)")
            # Combine data without applying national standards
            return self._combine_state_data(cleaned_data)
        
        logger.info("Phase 4: Applying national standards")
        
        try:
            # Validate input
            if not isinstance(cleaned_data, dict):
                logger.error(f"cleaned_data is not a dict, it's {type(cleaned_data)}")
                return pd.DataFrame()
            
            logger.info(f"cleaned_data type: {type(cleaned_data)}")
            logger.info(f"cleaned_data keys: {list(cleaned_data.keys())}")
            
            # Combine all state data
            all_records = []
            for state, df in cleaned_data.items():
                logger.info(f"Processing {state} with type {type(df)} and {len(df) if hasattr(df, '__len__') else 'N/A'} records")
                if not hasattr(df, 'columns'):
                    logger.error(f"Data for {state} is not a DataFrame, it's {type(df)}")
                    continue
                logger.info(f"DataFrame columns: {df.columns.tolist()}")
                df['state'] = self._format_state_name(state)  # Ensure state column exists
                all_records.append(df)
            
            if not all_records:
                logger.warning("No data to process")
                return pd.DataFrame()
            
            # Ensure unique columns across all frames before concatenation
            deduped_records = []
            for df in all_records:
                if df is not None and hasattr(df, 'columns'):
                    df = df.loc[:, ~df.columns.duplicated()]
                    deduped_records.append(df)
            combined_df = pd.concat(deduped_records, ignore_index=True)
            logger.info(f"Combined data: {len(combined_df)} records")
            
            # Apply national standards using the dedicated module
            logger.info("Applying national standards...")
            try:
                combined_df = self.national_standards.apply_standards(combined_df)
                logger.info("National standards applied successfully")
            except Exception as e:
                logger.error(f"National standards application failed: {e}")
                # Fall back to basic standards if the module fails
                combined_df = self._standardize_parties(combined_df)
            
            logger.info("National standards application completed successfully")
            return combined_df
            
        except Exception as e:
            logger.error(f"National standards application failed: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            # Return empty DataFrame instead of dict
            return pd.DataFrame()
    
    def _combine_state_data(self, state_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Combine state data into a single DataFrame."""
        all_records = []
        for state, df in state_data.items():
            if not df.empty:
                df['state'] = self._format_state_name(state)
                # Remove duplicate columns and reset index
                df = df.loc[:, ~df.columns.duplicated()].reset_index(drop=True)
                
                        # Ensure consistent column names for address components
        # Note: Keep 'address' as the main column name for consistency
                
                all_records.append(df)
        
        if not all_records:
            return pd.DataFrame()
        
        # Simple concatenation with ignore_index
        return pd.concat(all_records, ignore_index=True)
    
    def _format_state_name(self, state: str) -> str:
        """Format state name from underscore format to proper display format."""
        # State name mapping for proper formatting
        state_mapping = {
            'new_york': 'New York',
            'north_carolina': 'North Carolina', 
            'south_carolina': 'South Carolina',
            'south_dakota': 'South Dakota',
            'north_dakota': 'North Dakota',
            'west_virginia': 'West Virginia',
            'new_mexico': 'New Mexico',
            'new_jersey': 'New Jersey',
            'new_hampshire': 'New Hampshire',
            'rhode_island': 'Rhode Island',
            # Single-word states (for consistency and future-proofing)
            'alabama': 'Alabama', 'alaska': 'Alaska', 'arizona': 'Arizona',
            'arkansas': 'Arkansas', 'california': 'California', 'colorado': 'Colorado',
            'connecticut': 'Connecticut', 'delaware': 'Delaware', 'florida': 'Florida',
            'georgia': 'Georgia', 'hawaii': 'Hawaii', 'idaho': 'Idaho',
            'illinois': 'Illinois', 'indiana': 'Indiana', 'iowa': 'Iowa',
            'kansas': 'Kansas', 'kentucky': 'Kentucky', 'louisiana': 'Louisiana',
            'maine': 'Maine', 'maryland': 'Maryland', 'massachusetts': 'Massachusetts',
            'michigan': 'Michigan', 'minnesota': 'Minnesota', 'mississippi': 'Mississippi',
            'missouri': 'Missouri', 'montana': 'Montana', 'nebraska': 'Nebraska',
            'nevada': 'Nevada', 'ohio': 'Ohio', 'oklahoma': 'Oklahoma',
            'oregon': 'Oregon', 'pennsylvania': 'Pennsylvania', 'tennessee': 'Tennessee',
            'texas': 'Texas', 'utah': 'Utah', 'vermont': 'Vermont',
            'virginia': 'Virginia', 'washington': 'Washington', 'wisconsin': 'Wisconsin',
            'wyoming': 'Wyoming'
        }
        
        # Check if we have a specific mapping
        if state in state_mapping:
            return state_mapping[state]
        
        # For single-word states, just use title case
        if '_' not in state:
            return state.title()
        
        # For other multi-word states, replace underscores with spaces and title case
        return state.replace('_', ' ').title()
    
    def _standardize_parties(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize party names using the party standardizer."""
        if not self.party_standardizer:
            logger.warning("Party standardizer not available, skipping party standardization")
            return df
        
        try:
            logger.info("Standardizing party names...")
            df = self.party_standardizer.standardize_parties(df)
            logger.info("Party standardization complete")
            return df
        except Exception as e:
            logger.error(f"Party standardization failed: {e}")
            return df
    
    def final_processing(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Final processing (Phase 5).
        
        Args:
            data: Combined DataFrame from Phase 4
            
        Returns:
            Final processed DataFrame
        """
        if not self.config.enable_phase_5_final_processing:
            logger.info("Phase 5: Skipped (disabled)")
            return data
        
        logger.info("Phase 5: Final processing and output preparation")
        
        if data.empty:
            logger.warning("No data for final processing")
            return data
        
        try:
            # Ensure core columns exist
            core_columns = [
                'stable_id', 'full_name_display', 'state', 'office', 'election_year',
                'street_address', 'city', 'zip_code', 'address_state', 'raw_data'
            ]
            for col in core_columns:
                if col not in data.columns:
                    data[col] = None
            
            # Prefer full_name_display and drop candidate_name in final output
            if 'full_name_display' not in data.columns or data['full_name_display'].isna().all():
                if 'candidate_name' in data.columns:
                    data['full_name_display'] = data['candidate_name']
            else:
                if 'candidate_name' in data.columns:
                    # If full_name_display is null but candidate_name has value, fill forward
                    data['full_name_display'] = data['full_name_display'].fillna(data['candidate_name'])
            
            # Normalize raw_data to JSON strings or None
            try:
                if 'raw_data' not in data.columns:
                    data['raw_data'] = None
                else:
                    def _to_json_safe(value):
                        if value is None or (isinstance(value, float) and pd.isna(value)):
                            return None
                        if isinstance(value, str):
                            return value
                        try:
                            import json
                            return json.dumps(value, default=str)
                        except Exception:
                            return None
                    data['raw_data'] = data['raw_data'].apply(_to_json_safe)
            except Exception as e:
                logger.warning(f"raw_data normalization failed: {e}")
            
            # Use address parser if enabled
            if self.config.enable_address_parsing and self.address_parser:
                try:
                    # Parse addresses using the address parser
                    data = self.address_parser.parse_dataframe_addresses(data)
                    
                    # Normalize address states to USPS codes
                    data = self.address_parser.normalize_address_states(data)
                    
                    # Backfill missing states from main state column
                    data = self.address_parser.backfill_missing_states(data)
                    
                    logger.info("Address parsing completed using address parser")
                    
                except Exception as e:
                    logger.warning(f"Address parser failed: {e}")
            
            # Initialize nickname column if it doesn't exist
            try:
                if 'nickname' not in data.columns:
                    data['nickname'] = None
                    logger.info("Initialized missing nickname column")
            except Exception as e:
                logger.warning(f"Error initializing nickname column: {e}")
            
            # Convert name columns from ALL CAPS to proper case
            try:
                logger.info("Converting name columns to proper case...")
                
                # Don't include suffix - it's already standardized in Phase 4 (National Standards)
                name_columns = ['first_name', 'middle_name', 'last_name', 'prefix']
                for col in name_columns:
                    if col in data.columns:
                        # Convert to proper case (first letter capitalized, rest lowercase)
                        data[col] = data[col].apply(lambda x: x.title() if pd.notna(x) and isinstance(x, str) else x)
                        logger.info(f"Converted {col} to proper case")
                
                logger.info("Name case conversion completed successfully")
                
            except Exception as e:
                logger.warning(f"Error during name case conversion: {e}")
            
            # Convert email addresses to lowercase
            try:
                logger.info("Converting email addresses to lowercase...")
                
                if 'email' in data.columns:
                    # Convert emails to lowercase and handle None values properly
                    data['email'] = data['email'].apply(lambda x: x.lower() if pd.notna(x) and isinstance(x, str) else x)
                    logger.info("Converted email addresses to lowercase")
                
                logger.info("Email case conversion completed successfully")
                
            except Exception as e:
                logger.warning(f"Error during email case conversion: {e}")
            
            # Apply phone standardization as backup (in case national standards failed)
            try:
                logger.info("Applying phone standardization backup...")
                import re
                
                if 'phone' in data.columns:
                    # Convert to string first, then apply standardization
                    data['phone'] = data['phone'].astype(str)
                    data['phone'] = data['phone'].apply(
                        lambda x: re.sub(r'[^\d]', '', str(x)) if pd.notna(x) and str(x).strip() and str(x) != 'nan' else None
                    )
                    # Ensure it stays as string type
                    data['phone'] = data['phone'].astype('object')
                    logger.info("Phone standardization backup completed")
                
            except Exception as e:
                logger.warning(f"Error during phone standardization backup: {e}")
            
            # Fix phone number data types (ensure string type and remove .0 suffix)
            try:
                logger.info("Fixing phone number data types...")
                
                if 'phone' in data.columns:
                    # Convert to string and remove .0 suffix
                    data['phone'] = data['phone'].astype(str)
                    data['phone'] = data['phone'].apply(
                        lambda x: x.replace('.0', '') if pd.notna(x) and str(x).endswith('.0') else x
                    )
                    # Convert 'nan' strings back to None
                    data['phone'] = data['phone'].apply(
                        lambda x: None if pd.isna(x) or str(x) == 'nan' else x
                    )
                    # Ensure it stays as string type
                    data['phone'] = data['phone'].astype('object')
                    logger.info("Phone number data type fix completed")
                
            except Exception as e:
                logger.warning(f"Error during phone number data type fix: {e}")
            
            # Fix ZIP code formatting (remove .0 suffix and ensure string type)
            try:
                logger.info("Fixing ZIP code formatting...")
                
                if 'zip_code' in data.columns:
                    # Convert to string and remove .0 suffix
                    data['zip_code'] = data['zip_code'].astype(str)
                    data['zip_code'] = data['zip_code'].apply(
                        lambda x: x.replace('.0', '') if pd.notna(x) and str(x).endswith('.0') else x
                    )
                    # Convert 'nan' strings back to None
                    data['zip_code'] = data['zip_code'].apply(
                        lambda x: None if pd.isna(x) or str(x) == 'nan' else x
                    )
                    # Ensure it stays as string type
                    data['zip_code'] = data['zip_code'].astype('object')
                    logger.info("ZIP code formatting fix completed")
                
            except Exception as e:
                logger.warning(f"Error during ZIP code formatting fix: {e}")
            
            # Fix district values for statewide offices
            try:
                logger.info("Fixing district values for statewide offices...")
                
                if 'district' in data.columns and 'office' in data.columns:
                    # Define statewide offices that shouldn't have districts
                    statewide_offices = [
                        'Governor', 'Secretary of State', 'State Attorney General', 
                        'State Treasurer', 'US Senate', 'US President'
                    ]
                    
                    # Set district to None for statewide offices with district 0 or 0.0
                    statewide_mask = (
                        data['office'].isin(statewide_offices) & 
                        ((data['district'] == 0) | (data['district'] == 0.0))
                    )
                    
                    if statewide_mask.any():
                        data.loc[statewide_mask, 'district'] = None
                        logger.info(f"Cleared district for {statewide_mask.sum()} statewide office records")
                    
                    logger.info("District value fix completed")
                
            except Exception as e:
                logger.warning(f"Error during district value fix: {e}")
            
            # Clean address formatting
            try:
                logger.info("Cleaning address formatting...")
                
                if 'address' in data.columns:
                    import re
                    # Fix float street numbers (e.g., "123.0 Main St" -> "123 Main St")
                    data['address'] = data['address'].apply(lambda x: 
                        re.sub(r'^(\d+)\.0\b', r'\1', str(x)) if pd.notna(x) else x
                    )
                    
                    # Fix other common address formatting issues
                    data['address'] = data['address'].apply(lambda x: 
                        re.sub(r'\s+', ' ', str(x).strip()) if pd.notna(x) else x
                    )
                    
                    logger.info("Cleaned address formatting")
                
                logger.info("Address cleaning completed successfully")
                
            except Exception as e:
                logger.warning(f"Error during address cleaning: {e}")
            
            # Backfill missing stable_id deterministically from core fields
            try:
                import hashlib
                def _gen_stable(row):
                    if isinstance(row.get('stable_id'), str) and row.get('stable_id').strip():
                        return row.get('stable_id')
                    key_parts = [
                        str(row.get('full_name_display') or ''),
                        str(row.get('state') or ''),
                        str(row.get('election_year') or ''),
                        str(row.get('office') or '')
                    ]
                    key = '|'.join(key_parts)
                    return hashlib.md5(key.encode()).hexdigest()[:12]
                data['stable_id'] = data.apply(_gen_stable, axis=1)
            except Exception as e:
                logger.warning(f"stable_id backfill failed: {e}")
            
            # Sort by state, office, full_name_display
            data = data.sort_values(['state', 'office', 'full_name_display']).reset_index(drop=True)
            
            # Drop candidate_name from final output if present
            if 'candidate_name' in data.columns:
                try:
                    data = data.drop(columns=['candidate_name'])
                except Exception:
                    pass
            
            # Remove duplicate/conflicting columns
            duplicate_columns = ['parsed_street', 'parsed_city', 'parsed_state', 'parsed_zip', 'address']
            for col in duplicate_columns:
                if col in data.columns:
                    try:
                        data = data.drop(columns=[col])
                        logger.info(f"Removed duplicate column: {col}")
                    except Exception:
                        pass
            
            # Reorder columns to match preferred order
            preferred_order = [
                'stable_id',
                'state',
                'full_name_display',
                'prefix',
                'first_name',
                'middle_name',
                'last_name',
                'suffix',
                'nickname',
                'office',
                'district',
                'party',
                'street_address',
                'city',
                'address_state',
                'zip_code',
                'phone',
                'email',
                'website',
                'facebook',
                'twitter',
                'filing_date',
                'election_date',
                'first_added_date',
                'last_updated_date',
                # Additional columns after preferred order
                'county',
                'election_year',
                'action_type',
                'raw_data',
                'file_source',
                'row_index',
                'source_office',
                'source_district',
                'source_party',
                'ran_in_primary',
                'ran_in_general',
                'ran_in_special',
                'election_type_notes',
                'processing_timestamp',
                'pipeline_version',
                'data_source'
            ]
            
            # Add any missing columns from preferred order
            for col in preferred_order:
                if col not in data.columns:
                    data[col] = None
                    logger.info(f"Added missing column: {col}")
            
            # Reorder columns (keep any additional columns at the end)
            existing_columns = [col for col in preferred_order if col in data.columns]
            additional_columns = [col for col in data.columns if col not in preferred_order]
            final_order = existing_columns + additional_columns
            
            data = data[final_order]
            logger.info(f"Reordered columns to match preferred order")
            
            # Final logging
            logger.info(f"Final dataset: {len(data)} records")
            logger.info(f"States represented: {data['state'].nunique()}")
            logger.info(f"Offices represented: {data['office'].nunique()}")

            # Apply final data fixes
            data = self._apply_final_fixes(data)

            # Fix election date formatting (remove -GEN suffix and format properly)
            if 'election_date' in data.columns:
                data['election_date'] = data['election_date'].apply(
                    lambda x: self._format_election_date(x) if pd.notna(x) and str(x).strip() else x
                )
                logger.info("Fixed election date formatting")

            return data

        except Exception as e:
            logger.error(f"Final processing failed: {e}")
            if not self.config.continue_on_phase_error:
                raise
            return data
    
    def _apply_final_fixes(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply final data fixes for phone, ZIP, and district issues."""
        logger.info("Applying final data fixes...")
        
        # Fix phone number data types (ensure string type and remove .0 suffix)
        try:
            logger.info("Fixing phone number data types...")
            
            if 'phone' in data.columns:
                # Convert to string and remove .0 suffix
                data['phone'] = data['phone'].astype(str)
                data['phone'] = data['phone'].apply(
                    lambda x: x.replace('.0', '') if pd.notna(x) and str(x).endswith('.0') else x
                )
                # Convert 'nan' strings back to None
                data['phone'] = data['phone'].apply(
                    lambda x: None if pd.isna(x) or str(x) == 'nan' else x
                )
                # Ensure it stays as string type
                data['phone'] = data['phone'].astype('object')
                logger.info("Phone number data type fix completed")
            
        except Exception as e:
            logger.warning(f"Error during phone number data type fix: {e}")
        
        # Fix ZIP code formatting (remove .0 suffix and ensure string type)
        try:
            logger.info("Fixing ZIP code formatting...")
            
            if 'zip_code' in data.columns:
                # Convert to string and remove .0 suffix
                data['zip_code'] = data['zip_code'].astype(str)
                data['zip_code'] = data['zip_code'].apply(
                    lambda x: x.replace('.0', '') if pd.notna(x) and str(x).endswith('.0') else x
                )
                # Convert 'nan' strings back to None
                data['zip_code'] = data['zip_code'].apply(
                    lambda x: None if pd.isna(x) or str(x) == 'nan' else x
                )
                # Ensure it stays as string type
                data['zip_code'] = data['zip_code'].astype('object')
                logger.info("ZIP code formatting fix completed")
            
        except Exception as e:
            logger.warning(f"Error during ZIP code formatting fix: {e}")
        
        # Fix district values for statewide offices
        try:
            logger.info("Fixing district values for statewide offices...")
            
            if 'district' in data.columns and 'office' in data.columns:
                # Define statewide offices that shouldn't have districts
                statewide_offices = [
                    'Governor', 'Secretary of State', 'State Attorney General', 
                    'State Treasurer', 'US Senate', 'US President'
                ]
                
                # Set district to None for statewide offices with district 0 or 0.0
                # Handle both numeric and string district values
                statewide_mask = (
                    data['office'].isin(statewide_offices) & 
                    (
                        (data['district'].astype(str) == '0') | 
                        (data['district'].astype(str) == '0.0') |
                        (pd.to_numeric(data['district'], errors='coerce') == 0.0)
                    )
                )
                
                if statewide_mask.any():
                    data.loc[statewide_mask, 'district'] = None
                    logger.info(f"Cleared district for {statewide_mask.sum()} statewide office records")
                
                logger.info("District value fix completed")
            
        except Exception as e:
            logger.warning(f"Error during district value fix: {e}")
        
        logger.info("Final data fixes completed")
        return data
    
    def _format_election_date(self, date_str: str) -> str:
        """
        Format election date by removing -GEN suffix and converting to proper format
        
        Args:
            date_str: Date string like "20201103-GEN"
            
        Returns:
            Formatted date string like "2020-11-03"
        """
        if pd.isna(date_str) or not isinstance(date_str, str):
            return date_str
        
        # Remove -GEN suffix
        date_str = date_str.replace('-GEN', '').strip()
        
        # Handle YYYYMMDD format
        if re.match(r'^\d{8}$', date_str):
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:8]
            return f"{year}-{month}-{day}"
        
        # Handle other formats (keep as-is if not YYYYMMDD)
        return date_str
    
    def add_processing_metadata(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add processing metadata to the dataframe."""
        logger.info("Adding processing metadata...")
        
        try:
            # Add processing metadata
            data['processing_timestamp'] = datetime.now()
            data['pipeline_version'] = '1.0'
            data['data_source'] = 'state_filings'
            
            logger.info("Processing metadata added")
        except Exception as e:
            logger.error(f"Processing metadata addition failed: {e}")
            # Add basic metadata even if processing fails
            data['processing_timestamp'] = datetime.now()
            data['pipeline_version'] = '1.0'
            data['data_source'] = 'state_filings'
        
        return data
