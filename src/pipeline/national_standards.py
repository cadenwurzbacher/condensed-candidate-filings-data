import pandas as pd
import logging
import re
from typing import Dict, List, Tuple
import hashlib

# Import processors
from .office_standardizer import OfficeStandardizer
from .election_type_standardizer import ElectionTypeStandardizer

# Import database utilities
try:
    from config.database import get_db_connection
except ImportError:
    # Fallback for when running as module
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent))
    from config.database import get_db_connection

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
        
        # Apply office standardization (preserves source_office column)
        df_standardized = self._apply_office_standardization(df_standardized)
        
        # Apply party standardization (preserves source_party column)
        df_standardized = self._apply_party_standardization(df_standardized)
        
        # Apply smart case standardization
        df_standardized = self._apply_smart_case_standardization(df_standardized)
        
        # Apply state standardization
        df_standardized = self._apply_state_standardization(df_standardized)
        
        # Fix election date formatting (remove -GEN suffix and format properly)
        if 'election_date' in df.columns:
            df['election_date'] = df['election_date'].apply(
                lambda x: self._format_election_date(x) if pd.notna(x) and str(x).strip() else x
            )
            logger.info("Fixed election date formatting")
        
        # Fix county abbreviations
        if 'county' in df.columns:
            df['county'] = df['county'].apply(
                lambda x: self._standardize_county(x) if pd.notna(x) and str(x).strip() else x
            )
            logger.info("Fixed county abbreviations")
        
        # Fix missing last names for single-word names
        if 'first_name' in df.columns and 'last_name' in df.columns:
            # Find records where first_name exists but last_name is missing
            missing_last_mask = df['first_name'].notna() & df['last_name'].isna()
            
            if missing_last_mask.any():
                # For single-word names, treat them as last names
                single_word_mask = missing_last_mask & (df['first_name'].str.count(' ') == 0)
                if single_word_mask.any():
                    df.loc[single_word_mask, 'last_name'] = df.loc[single_word_mask, 'first_name']
                    df.loc[single_word_mask, 'first_name'] = None
                    logger.info(f"Fixed {single_word_mask.sum()} single-word names by treating as last names")
        
        # Apply name standardization
        df_standardized = self._apply_name_standardization(df_standardized)
        
        # Apply phone standardization
        df_standardized = self._apply_phone_standardization(df_standardized)
        
        # Apply Delaware county standardization
        df_standardized = self._apply_delaware_county_standardization(df_standardized)
        
        # Apply office standardization
        df_standardized = self._apply_office_standardization(df_standardized)
        
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
    
    def _apply_state_standardization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize state names according to DATA_STANDARDS.md
        
        - state column: Full state names (Alaska, North Carolina, etc.)
        - address_state column: Two-letter abbreviations (AK, NC, etc.)
        """
        logger.info("Applying state standardization")
        
        # State name mappings for full names
        state_full_names = {
            'ALASKA': 'Alaska',
            'KENTUCKY': 'Kentucky',
            'FL': 'Florida',
            'IL': 'Illinois',
            'INDIANA': 'Indiana',
            'NORTH CAROLINA': 'North Carolina',
            'NORTH CAROLINA': 'North Carolina',
            'SOUTH CAROLINA': 'South Carolina',
            'NEW YORK': 'New York',
            'NEW MEXICO': 'New Mexico',
            'NORTH DAKOTA': 'North Dakota',
            'SOUTH DAKOTA': 'South Dakota',
            'WEST VIRGINIA': 'West Virginia',
            'RHODE ISLAND': 'Rhode Island',
            'NEW JERSEY': 'New Jersey',
            'NEW HAMPSHIRE': 'New Hampshire',
            'ARKANSAS': 'Arkansas',
            'ARIZONA': 'Arizona',
            'COLORADO': 'Colorado',
            'CO': 'Colorado',
            'DELAWARE': 'Delaware',
            'GEORGIA': 'Georgia',
            'HAWAII': 'Hawaii',
            'IDAHO': 'Idaho',
            'IOWA': 'Iowa',
            'KANSAS': 'Kansas',
            'LOUISIANA': 'Louisiana',
            'MARYLAND': 'Maryland',
            'MASSACHUSETTS': 'Massachusetts',
            'MISSOURI': 'Missouri',
            'MONTANA': 'Montana',
            'NEBRASKA': 'Nebraska',
            'OKLAHOMA': 'Oklahoma',
            'OREGON': 'Oregon',
            'PENNSYLVANIA': 'Pennsylvania',
            'VERMONT': 'Vermont',
            'VIRGINIA': 'Virginia',
            'WASHINGTON': 'Washington',
            'WYOMING': 'Wyoming'
        }
        
        # State abbreviation mappings
        state_abbreviations = {
            'alaska': 'AK',
            'kentucky': 'KY',
            'florida': 'FL',
            'illinois': 'IL',
            'indiana': 'IN',
            'north carolina': 'NC',
            'south carolina': 'SC',
            'new york': 'NY',
            'new mexico': 'NM',
            'north dakota': 'ND',
            'south dakota': 'SD',
            'west virginia': 'WV',
            'rhode island': 'RI',
            'new jersey': 'NJ',
            'new hampshire': 'NH',
            'arkansas': 'AR',
            'arizona': 'AZ',
            'colorado': 'CO',
            'delaware': 'DE',
            'georgia': 'GA',
            'hawaii': 'HI',
            'idaho': 'ID',
            'iowa': 'IA',
            'kansas': 'KS',
            'louisiana': 'LA',
            'maryland': 'MD',
            'massachusetts': 'MA',
            'missouri': 'MO',
            'montana': 'MT',
            'nebraska': 'NE',
            'oklahoma': 'OK',
            'oregon': 'OR',
            'pennsylvania': 'PA',
            'vermont': 'VT',
            'virginia': 'VA',
            'washington': 'WA',
            'wyoming': 'WY'
        }
        
        # Standardize state column (full names)
        if 'state' in df.columns:
            df['state'] = df['state'].apply(
                lambda x: state_full_names.get(str(x).upper(), str(x).title()) if pd.notna(x) else x
            )
            logger.info("Standardized state column to full names")
        
        # Standardize address_state column (abbreviations)
        if 'address_state' in df.columns:
            df['address_state'] = df['address_state'].apply(
                lambda x: state_abbreviations.get(str(x).lower(), str(x).upper()) if pd.notna(x) else x
            )
            logger.info("Standardized address_state column to abbreviations")
        
        return df
    
    def _apply_name_standardization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize name components according to DATA_STANDARDS.md
        
        - Keep middle name periods
        - Standardize suffixes with periods
        - Remove party information from names
        - Ensure proper case
        """
        logger.info("Applying name standardization")
        
        # Keep middle names as-is (with periods)
        if 'middle_name' in df.columns:
            logger.info("Keeping middle name periods as per standards")
        
        # Remove party information from names (e.g., "(Nonpartisan)", "(Registered Republican)")
        name_columns = ['first_name', 'middle_name', 'last_name', 'full_name_display']
        for col in name_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: re.sub(r'\s*\([^)]*\)', '', str(x)).strip() if pd.notna(x) and str(x).strip() else x
                )
                logger.info(f"Removed party information from {col}")
        
        # Remove "Congressman" prefix from names (Arkansas-specific)
        for col in name_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: re.sub(r'^Congressman\s+', '', str(x)).strip() if pd.notna(x) and str(x).strip() else x
                )
                logger.info(f"Removed 'Congressman' prefix from {col}")
        
        # Remove party letter codes from names and update party column
        party_letter_mappings = {
            r'\(R\)': 'Republican',
            r'\(r\)': 'Republican',
            r'\(D\)': 'Democratic',
            r'\(d\)': 'Democratic',
            r'\(G\)': 'Green',
            r'\(g\)': 'Green',
            r'\(L\)': 'Libertarian',
            r'\(l\)': 'Libertarian',
            r'\(I\)': 'Independent',
            r'\(i\)': 'Independent'
        }
        
        # Process each name column for party letter codes
        for col in name_columns:
            if col in df.columns:
                for pattern, party_name in party_letter_mappings.items():
                    # Find rows with this pattern
                    mask = df[col].str.contains(pattern, regex=True, na=False)
                    if mask.any():
                        # Remove the pattern from the name
                        df.loc[mask, col] = df.loc[mask, col].str.replace(pattern, '', regex=True).str.strip()
                        
                        # Update party column if it's empty
                        if 'party' in df.columns:
                            party_mask = mask & (df['party'].isna() | (df['party'] == ''))
                            df.loc[party_mask, 'party'] = party_name
                        
                        logger.info(f"Removed {pattern} from {col} and updated party to {party_name}")
        
        # Standardize suffixes (add periods if missing)
        if 'suffix' in df.columns:
            suffix_mappings = {
                'Sr': 'Sr.',
                'Jr': 'Jr.',
                'II': 'II',
                'III': 'III',
                'IV': 'IV',
                'V': 'V',
                'VI': 'VI',
                'VII': 'VII',
                'VIII': 'VIII',
                'IX': 'IX',
                'X': 'X',
                # Add missing variations with case handling
                'Ii': 'II',
                'Iii': 'III',
                'Iv': 'IV',
                'Vi': 'VI',
                'ii': 'II',
                'iii': 'III',
                'iv': 'IV',
                'vi': 'VI',
                'sr': 'Sr.',
                'jr': 'Jr.',
                # Add uppercase variations
                'SR': 'Sr.',
                'JR': 'Jr.'
            }
            df['suffix'] = df['suffix'].apply(
                lambda x: suffix_mappings.get(str(x), str(x)) if pd.notna(x) and str(x).strip() else x
            )
            logger.info("Standardized suffixes")
        
        # Ensure proper case for name components
        name_columns = ['first_name', 'middle_name', 'last_name', 'prefix', 'nickname']  # Remove suffix from case conversion
        for col in name_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: str(x).title() if pd.notna(x) and str(x).strip() else x
                )
        
        # Apply proper case to full_name_display, preserving legitimate abbreviations
        if 'full_name_display' in df.columns:
            df['full_name_display'] = df['full_name_display'].apply(
                lambda x: self._apply_proper_case_to_name(str(x)) if pd.notna(x) and str(x).strip() else x
            )
            logger.info("Applied proper case to full_name_display")
        
        logger.info("Applied proper case to name components")
        return df
    
    def _apply_proper_case_to_name(self, name: str) -> str:
        """
        Apply proper case to a name while preserving legitimate abbreviations.
        
        Args:
            name: Name string to convert
            
        Returns:
            Name with proper case applied
        """
        if not name or pd.isna(name):
            return name
        
        # Check if it's already proper case
        if not name.isupper():
            return name
        
        # Patterns for names that should remain all caps
        preserve_caps_patterns = [
            r'^(DE LA|DEL|VAN|VON|MC|MAC|O\'|ST\.|SANTA|SANTO|EL|LA|LOS|LAS)\s+[A-Z]',  # Spanish/foreign prefixes
            r'^[A-Z]{1,3}\s+[A-Z]{1,3}$',  # Short abbreviations like "JD SMITH"
            r'^[A-Z]{1,2}\s+[A-Z]',  # Initial + name like "J SMITH"
        ]
        
        # Check if name matches any preserve patterns
        for pattern in preserve_caps_patterns:
            if re.match(pattern, name, re.IGNORECASE):
                return name  # Keep as-is
        
        # Apply proper case conversion
        return name.title()
    
    def _apply_delaware_county_standardization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize Delaware county abbreviations to full names
        K = Kent, N = New Castle, S = Sussex
        """
        if 'county' in df.columns:
            county_mapping = {
                'K': 'Kent',
                'N': 'New Castle', 
                'S': 'Sussex',
                'K, S': 'Kent, Sussex',
                'KS': 'Kent, Sussex',
                'N, K': 'New Castle, Kent',
                'N,K': 'New Castle, Kent',
                'N/K': 'New Castle/Kent',
                'NK': 'New Castle, Kent',
                # Add missing variations
                'Nk': 'New Castle',
                'Ks': 'Kent',
                'N,k': 'New Castle, Kent',
                'N/k': 'New Castle/Kent'
            }
            
            df['county'] = df['county'].apply(
                lambda x: county_mapping.get(str(x), str(x)) if pd.notna(x) and str(x).strip() else x
            )
            logger.info("Standardized Delaware county abbreviations")
        
        return df
    
    def _apply_phone_standardization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize phone numbers to digits only according to DATA_STANDARDS.md
        
        Remove all parentheses, dashes, spaces, and special characters
        """
        logger.info("Applying phone standardization")
        
        if 'phone' in df.columns:
            # Convert to string first, then apply standardization
            df['phone'] = df['phone'].astype(str)
            df['phone'] = df['phone'].apply(
                lambda x: re.sub(r'[^\d]', '', str(x)) if pd.notna(x) and str(x).strip() and str(x) != 'nan' else None
            )
            # Ensure it stays as string type
            df['phone'] = df['phone'].astype('object')
            logger.info("Standardized phone numbers to digits only")
        
        return df
    
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
        
        # Get all columns except county, stable_id, and raw_data (since stable_id includes county and raw_data may contain dicts)
        dedupe_columns = [col for col in df.columns if col not in ['county', 'stable_id', 'raw_data']]
        
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
                
                # Get candidate name and office safely
                candidate_name = group_df.iloc[0].get('full_name_display', group_df.iloc[0].get('candidate_name', 'Unknown'))
                office = group_df.iloc[0].get('office', 'Unknown')
                logger.debug(f"Deduped {len(group_df)} records for {candidate_name} - {office}")
        
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
        
        # Check if stable_id column exists
        if 'stable_id' not in df.columns:
            logger.warning("No 'stable_id' column found, skipping deduplication")
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
    
    def _apply_party_standardization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply party standardization to all records
        
        Args:
            df: Input DataFrame with 'party' column
            
        Returns:
            DataFrame with standardized 'party' and new 'source_party' column
        """
        logger.info("Applying party standardization...")
        
        if 'party' not in df.columns:
            logger.warning("No 'party' column found, skipping party standardization")
            return df
        
        # Make a copy to avoid modifying original
        df_standardized = df.copy()
        
        # Add source_party column to preserve original values
        df_standardized['source_party'] = df_standardized['party'].copy()
        
        # Apply party standardization
        df_standardized['party'] = df_standardized['party'].apply(self._standardize_party)
        
        # Log the results
        original_parties = df['party'].nunique()
        standardized_parties = df_standardized['party'].nunique()
        reduction = original_parties - standardized_parties
        
        logger.info(f"Party standardization complete:")
        logger.info(f"  Original unique parties: {original_parties}")
        logger.info(f"  Standardized unique parties: {standardized_parties}")
        logger.info(f"  Reduction: {reduction} party variations")
        
        return df_standardized
    
    def _standardize_party(self, party: str) -> str:
        """
        Standardize party names using conservative mapping
        
        Args:
            party: Original party name
            
        Returns:
            str: Standardized party name
        """
        if pd.isna(party) or not isinstance(party, str):
            return party
        
        party = party.strip()
        
        # Handle special cases first
        # PA: Change dash (-) to Unaffiliated
        if party == '-':
            return 'Unaffiliated'
        
        # NC: Change date-like values to Unknown
        # Check if party looks like a date (contains numbers and common date separators)
        if re.match(r'^\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}$', party) or \
           re.match(r'^\d{4}[/\-]\d{1,2}[/\-]\d{1,2}$', party) or \
           re.match(r'^\d{1,2}[/\-]\d{1,2}$', party) or \
           re.match(r'^\d{1,2}\.\d{1,2}\.\d{2,4}$', party) or \
           re.match(r'^\d{4}\.\d{1,2}\.\d{1,2}$', party) or \
           re.match(r'^\d{1,2}\.\d{1,2}$', party):
            return 'Unknown'
        
        # Handle other common data entry errors
        # Empty strings, single characters, or obvious typos
        if party in ['', ' ', 'N/A', 'NA', 'n/a', 'na', 'None', 'none', 'NULL', 'null']:
            return 'Unknown'
        
        # Single character parties (likely data entry errors)
        if len(party) == 1 and party not in ['D', 'R', 'I', 'L']:
            return 'Unknown'
        
        # Conservative party mapping - only standardize obvious duplicates/abbreviations
        party_mapping = {
            # Democratic variations
            'Democrat': 'Democratic',
            'Dem': 'Democratic', 
            'DEM': 'Democratic',
            'Democratic Party': 'Democratic',
            'Florida Democratic Party': 'Democratic',
            'Democractic': 'Democratic',  # Typo fix
            'D': 'Democratic',
            
            # Republican variations
            'Rep': 'Republican',
            'REP': 'Republican',
            'Republican Party': 'Republican',
            'Republican Party Of Florida': 'Republican',
            'R': 'Republican',
            'G.o.p.': 'Republican',
            'Gop': 'Republican',
            'G.o.p': 'Republican',
            'Grand Old': 'Republican',
            'The Republican': 'Republican',
            
            # Independent/Nonpartisan variations
            'Ind': 'Independent',
            'IND': 'Independent',
            'I': 'Independent',
            'Non-partisan': 'Nonpartisan',
            'Non Partisan': 'Nonpartisan',
            'Non': 'Nonpartisan',
            'NON': 'Nonpartisan',
            'Np': 'Nonpartisan',
            'Nonaffiliated': 'Nonpartisan',
            'Unaffiliated': 'Nonpartisan',
            'Una': 'Nonpartisan',
            'No Party': 'Nonpartisan',
            'No Party Preference': 'Nonpartisan',
            'States No Party Preference': 'Nonpartisan',
            'No Party Affiliation (nonpartisan Offices)': 'Nonpartisan',
            'No Party Affiliation (nonpartisan offices)': 'Nonpartisan',
            'No Party Affiliation (partisan)': 'Nonpartisan',
            'No Party Affiliation': 'Nonpartisan',
            'No Affiliation': 'Nonpartisan',
            'Undeclared': 'Nonpartisan',
            'Write-in': '',  # Map to blank party column
            'Constitution': 'Constitution',
            'Nonpartisan Special': 'Nonpartisan',
            'Nonpartisan Special': 'Nonpartisan',
            
            # Libertarian variations
            'Lib': 'Libertarian',
            'Lbt': 'Libertarian',
            'LBT': 'Libertarian',
            'L': 'Libertarian',
            
            # Green variations
            'Green Party': 'Green',
            'Grn': 'Green',
            'GRN': 'Green',
            'Gre': 'Green',
            
            # Constitution variations
            'Constitution Party': 'Constitution',
            'Constitution Party Nominee': 'Constitution',
            'Constitution Party Of Illinois': 'Constitution',
            'Con': 'Constitution',
            'CON': 'Constitution',
            'Cst': 'Constitution',
        }
        
        # Apply mapping if party exists in mapping
        return party_mapping.get(party, party)
    
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
    
    def _standardize_county(self, county: str) -> str:
        """
        Standardize county abbreviations to full names
        
        Args:
            county: County name or abbreviation
            
        Returns:
            Standardized county name
        """
        if pd.isna(county) or not isinstance(county, str):
            return county
        
        county = county.strip()
        
        # Common county abbreviations (add more as needed)
        county_mappings = {
            # Florida counties
            'Lee': 'Lee County',
            'Hil': 'Hillsborough County',
            'Dad': 'Dade County',
            'Bro': 'Broward County',
            'Pal': 'Palm Beach County',
            'Pas': 'Pasco County',
            'Ora': 'Orange County',
            'Man': 'Manatee County',
            'Stj': 'St. Johns County',
            'Cll': 'Collier County',
            # Add more as needed
        }
        
        return county_mappings.get(county, county)
    
    def _apply_office_standardization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply office standardization to all records
        
        Args:
            df: Input DataFrame with 'office' column
            
        Returns:
            DataFrame with standardized 'office' and new 'source_office' columns
        """
        logger.info("Applying office standardization...")
        
        if 'office' not in df.columns:
            logger.warning("No 'office' column found, skipping office standardization")
            return df
        
        # Initialize the office standardizer
        office_standardizer = OfficeStandardizer()
        
        # Apply standardization
        result_df = office_standardizer.standardize_offices(df)
        
        # Get unmatched offices for analysis
        unmatched_summary = office_standardizer.get_unmatched_offices(result_df)
        
        if not unmatched_summary.empty:
            logger.info(f"Found {len(unmatched_summary):,} unmatched office types")
            logger.info("Top unmatched offices:")
            for _, row in unmatched_summary.head(10).iterrows():
                logger.info(f"  {row['source_office']}: {row['count']:,} records")
        else:
            logger.info("All offices were successfully standardized!")
        
        return result_df
    
    def _generate_stable_id(self, name: str, state: str, office: str, election_year: str, county: str = None) -> str:
        """
        DEPRECATED: Stable IDs are now generated only by Main Pipeline in Phase 2
        
        This method is kept for backward compatibility but should not be used.
        Stable IDs are generated once in the Main Pipeline and maintained throughout.
        
        Args:
            name: Candidate name
            state: State
            office: Office
            election_year: Election year
            county: County (ignored)
            
        Returns:
            None - stable IDs should come from Main Pipeline
        """
        logger.warning("_generate_stable_id called - stable IDs should come from Main Pipeline")
        return None
