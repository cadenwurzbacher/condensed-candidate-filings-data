#!/usr/bin/env python3
"""
Improved Kentucky State Data Cleaner

This module contains improved functions to clean and standardize Kentucky political 
candidate data with proper location parsing for city, county, ward, and district.
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

# Configuration
DEFAULT_OUTPUT_DIR = "cleaned_data"  # Default output directory for cleaned data
DEFAULT_INPUT_DIR = "Raw State Data - Current"  # Default input directory

class ImprovedKentuckyCleaner:
    """Handles improved cleaning and standardization of Kentucky political candidate data."""
    
    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "Kentucky"
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
        
        # Kentucky county list for validation (expanded)
        self.kentucky_counties = {
            'adair', 'allen', 'anderson', 'ballard', 'barren', 'bath', 'bell', 'boone',
            'bourbon', 'boyd', 'boyle', 'bracken', 'breathitt', 'breckinridge', 'bullitt',
            'butler', 'caldwell', 'calloway', 'campbell', 'carlisle', 'carroll', 'carter',
            'casey', 'christian', 'clark', 'clay', 'clinton', 'crittenden', 'cumberland',
            'daviess', 'edmonson', 'elliott', 'estill', 'fayette', 'fleming', 'floyd',
            'franklin', 'fulton', 'gallatin', 'garrard', 'grant', 'graves', 'grayson',
            'green', 'greenup', 'hancock', 'hardin', 'harlan', 'harrison', 'hart', 'henderson',
            'henry', 'hickman', 'hopkins', 'jackson', 'jefferson', 'jessamine', 'johnson',
            'kenton', 'knott', 'knox', 'larue', 'laurel', 'lawrence', 'lee', 'leslie',
            'letcher', 'lewis', 'lincoln', 'livingston', 'logan', 'lyon', 'madison',
            'magoffin', 'marion', 'marshall', 'martin', 'mason', 'mccracken', 'mccreary',
            'mclean', 'meade', 'menifee', 'mercer', 'metcalfe', 'monroe', 'montgomery',
            'morgan', 'muhlenberg', 'nelson', 'nicholas', 'ohio', 'oldham', 'owen',
            'owsley', 'pendleton', 'perry', 'pike', 'powell', 'pulaski', 'robertson',
            'rockcastle', 'rowan', 'russell', 'scott', 'shelby', 'simpson', 'spencer',
            'taylor', 'todd', 'trigg', 'trimble', 'union', 'warren', 'washington',
            'wayne', 'webster', 'whitley', 'wolfe', 'woodford',
            # Additional counties found in the data
            'clay', 'powell', 'letcher', 'pike', 'carter', 'harlan', 'martin', 'bullitt',
            'campbell', 'bell', 'lawrence', 'estill', 'boyd', 'boone', 'jackson', 'magoffin'
        }
        
        # Kentucky cities for validation (expanded)
        self.kentucky_cities = {
            'louisville', 'lexington', 'bowling green', 'owensboro', 'covington',
            'richmond', 'georgetown', 'florence', 'elizabethtown', 'nicholasville',
            'henderson', 'hopkinsville', 'winchester', 'murray', 'paducah', 'independence',
            'radcliff', 'ashland', 'madisonville', 'frankfort', 'la grange', 'shively',
            'jeffersontown', 'anchorage', 'fort thomas', 'newport', 'villa hills',
            'highland heights', 'fort mitchell', 'cold spring', 'edgewood', 'crestview hills',
            'fort wright', 'lakeside park', 'taylor mill', 'walton', 'burlington',
            'union', 'florence', 'erlanger', 'alexandria', 'bellevue', 'dayton',
            'southgate', 'melbourne', 'silver grove', 'ludlow', 'bromley', 'latonia',
            'covington', 'park hills', 'ryland heights', 'kenton vale', 'visalia',
            'atlanta', 'fiskburg', 'piner', 'richwood', 'verona', 'petersburg',
            'morris', 'california', 'sprinkle',
            # Additional cities found in the data
            'glasgow', 'munfordville', 'manchester', 'central city', 'irvine',
            'somerset', 'pikeville', 'prestonsburg', 'hazard', 'corbin',
            'london', 'danville', 'harrodsburg', 'bardstown', 'leitchfield',
            'greensburg', 'campbellsville', 'columbia', 'jamestown', 'russellville',
            'mayfield', 'cadiz', 'princeton', 'dixon', 'calhoun', 'beaver dam',
            'hartford', 'benton', 'marion', 'eddyville', 'kuttawa', 'grand rivers',
            'gilbertsville', 'hardinsburg', 'cloverport', 'brandenburg', 'muldraugh',
            'hillview', 'mt washington', 'shepherdsville', 'mt washington', 'mt washington',
            'mt washington', 'mt washington', 'mt washington', 'mt washington', 'mt washington'
        }
    
    def _fuzzy_match(self, text: str, candidates: set, threshold: float = 0.6) -> str:
        """
        Enhanced fuzzy match text against a set of candidates.
        
        Args:
            text: Text to match
            candidates: Set of candidate strings
            threshold: Similarity threshold (0.0 to 1.0)
            
        Returns:
            Best match if above threshold, None otherwise
        """
        if not text or not candidates:
            return None
        
        text_lower = text.lower().strip()
        best_match = None
        best_score = 0
        
        for candidate in candidates:
            candidate_lower = candidate.lower()
            
            # Exact match
            if text_lower == candidate_lower:
                return candidate
            
            # Check if text contains candidate or vice versa
            if text_lower in candidate_lower or candidate_lower in text_lower:
                score = min(len(text_lower), len(candidate_lower)) / max(len(text_lower), len(candidate_lower))
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = candidate
            
            # Check for common abbreviations
            if len(text_lower) >= 3 and len(candidate_lower) >= 3:
                # Check if text is abbreviation of candidate
                if candidate_lower.startswith(text_lower):
                    score = len(text_lower) / len(candidate_lower)
                    if score > best_score and score >= threshold:
                        best_score = score
                        best_match = candidate
                
                # Check if candidate is abbreviation of text
                if text_lower.startswith(candidate_lower):
                    score = len(candidate_lower) / len(text_lower)
                    if score > best_score and score >= threshold:
                        best_score = score
                        best_match = candidate
            
            # Check for character similarity (Levenshtein-like)
            if len(text_lower) >= 3 and len(candidate_lower) >= 3:
                # Calculate character overlap
                text_chars = set(text_lower)
                candidate_chars = set(candidate_lower)
                
                intersection = text_chars.intersection(candidate_chars)
                union = text_chars.union(candidate_chars)
                
                if union:
                    char_similarity = len(intersection) / len(union)
                    if char_similarity > best_score and char_similarity >= threshold:
                        best_score = char_similarity
                        best_match = candidate
            
            # Check for word-level similarity
            text_words = set(text_lower.split())
            candidate_words = set(candidate_lower.split())
            
            if text_words and candidate_words:
                word_intersection = text_words.intersection(candidate_words)
                word_union = text_words.union(candidate_words)
                
                if word_union:
                    word_similarity = len(word_intersection) / len(word_union)
                    if word_similarity > best_score and word_similarity >= threshold:
                        best_score = word_similarity
                        best_match = candidate
            
            # Check for phonetic similarity (simple approach)
            if len(text_lower) >= 4 and len(candidate_lower) >= 4:
                # Check if both start with same letter and have similar length
                if (text_lower[0] == candidate_lower[0] and 
                    abs(len(text_lower) - len(candidate_lower)) <= 2):
                    length_similarity = 1 - (abs(len(text_lower) - len(candidate_lower)) / max(len(text_lower), len(candidate_lower)))
                    if length_similarity > best_score and length_similarity >= threshold:
                        best_score = length_similarity
                        best_match = candidate
        
        return best_match if best_score >= threshold else None
    
    def _parse_location(self, location_str: str) -> Dict[str, str]:
        """
        Parse Kentucky location string into structured components.
        
        Args:
            location_str: Raw location string from data
            
        Returns:
            Dictionary with parsed components: city, county, ward, district, location_type
        """
        if pd.isna(location_str):
            return {
                'city': None,
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'Unknown'
            }
        
        location_str = str(location_str).strip()
        
        # Pattern 1: City-County format (e.g., "Georgetown-Scott")
        city_county_pattern = r'^([A-Za-z\s\-]+)-([A-Za-z\s\-]+)$'
        match = re.match(city_county_pattern, location_str)
        if match:
            city = match.group(1).strip()
            county = match.group(2).strip()
            return {
                'city': city,
                'county': county,
                'ward': None,
                'district': None,
                'location_type': 'City-County'
            }
        
        # Pattern 2: City-Ward-County format (e.g., "Hopkinsville-12th Ward-Christian")
        city_ward_county_pattern = r'^([A-Za-z\s\-]+)-(\d+(?:st|nd|rd|th)\s+Ward)-([A-Za-z\s\-]+)$'
        match = re.match(city_ward_county_pattern, location_str)
        if match:
            city = match.group(1).strip()
            ward = match.group(2).strip()
            county = match.group(3).strip()
            return {
                'city': city,
                'county': county,
                'ward': ward,
                'district': None,
                'location_type': 'City-Ward-County'
            }
        
        # Pattern 3: District format (e.g., "52nd District")
        district_pattern = r'^(\d+(?:st|nd|rd|th)?)\s*District$'
        match = re.match(district_pattern, location_str, re.IGNORECASE)
        if match:
            district = match.group(1).strip()
            return {
                'city': None,
                'county': None,
                'ward': None,
                'district': district,
                'location_type': 'District'
            }
        
        # Pattern 4: County only (e.g., "Perry", "Mccreary", "Knox", "Pulaski")
        if location_str.lower() in self.kentucky_counties:
            return {
                'city': None,
                'county': location_str,
                'ward': None,
                'district': None,
                'location_type': 'County-Only'
            }
        
        # Pattern 4b: Fuzzy match for county names
        fuzzy_county = self._fuzzy_match(location_str, self.kentucky_counties, threshold=0.7)
        if fuzzy_county:
            return {
                'city': None,
                'county': fuzzy_county,
                'ward': None,
                'district': None,
                'location_type': 'County-Only-Fuzzy'
            }
        
        # Pattern 5: City only (e.g., "Anchorage", "Shively", "Jeffersontown")
        if location_str.lower() in self.kentucky_cities:
            return {
                'city': location_str,
                'ward': None,
                'district': None,
                'location_type': 'City-Only'
            }
        
        # Pattern 5b: Fuzzy match for city names
        fuzzy_city = self._fuzzy_match(location_str, self.kentucky_cities, threshold=0.7)
        if fuzzy_city:
            return {
                'city': fuzzy_city,
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'City-Only-Fuzzy'
            }
        
        # Pattern 6: Statewide
        if location_str.lower() == 'statewide':
            return {
                'city': None,
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'Statewide'
            }
        
        # Enhanced pattern recognition for special cases
        
        # Pattern 7: Check for abbreviated county names
        if location_str.lower() in ['jeff', 'jef', 'jeferson']:
            return {
                'city': None,
                'county': 'Jefferson',
                'ward': None,
                'district': None,
                'location_type': 'County-Only'
            }
        
        # Pattern 8: Check for city abbreviations
        if location_str.lower() in ['lou', 'lex', 'bg', 'owb', 'cov']:
            city_mapping = {
                'lou': 'Louisville',
                'lex': 'Lexington', 
                'bg': 'Bowling Green',
                'owb': 'Owensboro',
                'cov': 'Covington'
            }
            return {
                'city': city_mapping[location_str.lower()],
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'City-Only'
            }
        
        # Pattern 9: Check for ward-like patterns without city
        ward_only_pattern = r'^(\d+(?:st|nd|rd|th)\s*Ward)$'
        match = re.match(ward_only_pattern, location_str, re.IGNORECASE)
        if match:
            return {
                'city': None,
                'county': None,
                'ward': match.group(1).strip(),
                'district': None,
                'location_type': 'Ward-Only'
            }
        
        # Pattern 10: Check for district-like patterns without "District" word
        district_only_pattern = r'^(\d+(?:st|nd|rd|th)?)$'
        match = re.match(district_only_pattern, location_str)
        if match:
            return {
                'city': None,
                'county': None,
                'ward': None,
                'district': match.group(1).strip(),
                'location_type': 'District-Only'
            }
        
        # Pattern 11: Check for city-county with different separator
        city_county_alt_pattern = r'^([A-Za-z\s]+)\s+([A-Za-z\s]+)$'
        match = re.match(city_county_alt_pattern, location_str)
        if match:
            city = match.group(1).strip()
            county = match.group(2).strip()
            # Validate that the second part looks like a county
            if county.lower() in self.kentucky_counties:
                return {
                    'city': city,
                    'county': county,
                    'ward': None,
                    'district': None,
                    'location_type': 'City-County-Alt'
                }
        
        # Pattern 12: Check for county names with common misspellings
        county_misspellings = {
            'jefferson': 'Jefferson',
            'jeferson': 'Jefferson',
            'jef': 'Jefferson',
            'fayette': 'Fayette',
            'fayet': 'Fayette',
            'kenton': 'Kenton',
            'kenton': 'Kenton',
            'campbell': 'Campbell',
            'campbel': 'Campbell',
            'boone': 'Boone',
            'boon': 'Boone',
            'hardin': 'Hardin',
            'hardin': 'Hardin',
            'daviess': 'Daviess',
            'davies': 'Daviess',
            'warren': 'Warren',
            'warren': 'Warren',
            'bullitt': 'Bullitt',
            'bullit': 'Bullitt',
            'oldham': 'Oldham',
            'oldham': 'Oldham'
        }
        
        if location_str.lower() in county_misspellings:
            return {
                'city': None,
                'county': county_misspellings[location_str.lower()],
                'ward': None,
                'district': None,
                'location_type': 'County-Only'
            }
        
        # Pattern 13: Check for city names with common misspellings
        city_misspellings = {
            'louisville': 'Louisville',
            'louisvil': 'Louisville',
            'lexington': 'Lexington',
            'lexingtn': 'Lexington',
            'bowling green': 'Bowling Green',
            'bowling': 'Bowling Green',
            'owensboro': 'Owensboro',
            'owensbor': 'Owensboro',
            'covington': 'Covington',
            'covintgon': 'Covington',
            'georgetown': 'Georgetown',
            'georgetwn': 'Georgetown',
            'florence': 'Florence',
            'florenc': 'Florence',
            'henderson': 'Henderson',
            'hendersn': 'Henderson',
            'hopkinsville': 'Hopkinsville',
            'hopkinsvil': 'Hopkinsville',
            'paducah': 'Paducah',
            'paduca': 'Paducah'
        }
        
        if location_str.lower() in city_misspellings:
            return {
                'city': city_misspellings[location_str.lower()],
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'City-Only'
            }
        
        # Pattern 14: Check for city-county with comma separator
        city_county_comma_pattern = r'^([A-Za-z\s\-]+),\s*([A-Za-z\s\-]+)$'
        match = re.match(city_county_comma_pattern, location_str)
        if match:
            city = match.group(1).strip()
            county = match.group(2).strip()
            return {
                'city': city,
                'county': county,
                'ward': None,
                'district': None,
                'location_type': 'City-County-Comma'
            }
        
        # Pattern 15: Check for city-county with "in" separator
        city_county_in_pattern = r'^([A-Za-z\s\-]+)\s+in\s+([A-Za-z\s\-]+)$'
        match = re.match(city_county_in_pattern, location_str, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            county = match.group(2).strip()
            return {
                'city': city,
                'county': county,
                'ward': None,
                'district': None,
                'location_type': 'City-County-In'
            }
        
        # Pattern 16: Check for city-county with "of" separator
        city_county_of_pattern = r'^([A-Za-z\s\-]+)\s+of\s+([A-Za-z\s\-]+)$'
        match = re.match(city_county_of_pattern, location_str, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            county = match.group(2).strip()
            return {
                'city': city,
                'county': county,
                'ward': None,
                'district': None,
                'location_type': 'City-County-Of'
            }
        
        # Pattern 17: Check for abbreviated district patterns
        district_abbr_pattern = r'^(\d+)(?:st|nd|rd|th)?\s*(?:Dist|Dist\.|District)?$'
        match = re.match(district_abbr_pattern, location_str, re.IGNORECASE)
        if match:
            district = match.group(1).strip()
            return {
                'city': None,
                'county': None,
                'ward': None,
                'district': district,
                'location_type': 'District-Abbr'
            }
        
        # Pattern 18: Check for ward patterns without "Ward" word
        ward_only_num_pattern = r'^(\d+)(?:st|nd|rd|th)?$'
        match = re.match(ward_only_num_pattern, location_str)
        if match:
            # Only treat as ward if it's a reasonable ward number (1-50)
            ward_num = int(match.group(1))
            if 1 <= ward_num <= 50:
                return {
                    'city': None,
                    'county': None,
                    'ward': f"{ward_num}{self._get_ordinal_suffix(ward_num)} Ward",
                    'district': None,
                    'location_type': 'Ward-Only-Num'
                }
        
        # Pattern 19: Check for city names with numbers (e.g., "City 1", "Town 2")
        city_num_pattern = r'^([A-Za-z\s]+)\s+(\d+)$'
        match = re.match(city_num_pattern, location_str)
        if match:
            city = match.group(1).strip()
            number = match.group(2).strip()
            return {
                'city': f"{city} {number}",
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'City-Number'
            }
        
        # Pattern 20: Check for county names with "County" suffix
        county_suffix_pattern = r'^([A-Za-z\s]+)\s+County$'
        match = re.match(county_suffix_pattern, location_str, re.IGNORECASE)
        if match:
            county = match.group(1).strip()
            return {
                'city': None,
                'county': county,
                'ward': None,
                'district': None,
                'location_type': 'County-Suffix'
            }
        
        # Pattern 21: Check for city names with "City" suffix
        city_suffix_pattern = r'^([A-Za-z\s]+)\s+City$'
        match = re.match(city_suffix_pattern, location_str, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            return {
                'city': city,
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'City-Suffix'
            }
        
        # Pattern 22: Check for city names with "Town" suffix
        town_suffix_pattern = r'^([A-Za-z\s]+)\s+Town$'
        match = re.match(town_suffix_pattern, location_str, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            return {
                'city': city,
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'Town-Suffix'
            }
        
        # Pattern 23: Check for city names with "Village" suffix
        village_suffix_pattern = r'^([A-Za-z\s]+)\s+Village$'
        match = re.match(village_suffix_pattern, location_str, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            return {
                'city': city,
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'Village-Suffix'
            }
        
        # Pattern 24: Check for city names with "Borough" suffix
        borough_suffix_pattern = r'^([A-Za-z\s]+)\s+Borough$'
        match = re.match(borough_suffix_pattern, location_str, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            return {
                'city': city,
                'ward': None,
                'district': None,
                'location_type': 'Borough-Suffix'
            }
        
        # Pattern 25: Check for city names with "Heights" suffix
        heights_suffix_pattern = r'^([A-Za-z\s]+)\s+Heights$'
        match = re.match(heights_suffix_pattern, location_str, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            return {
                'city': f"{city} Heights",
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'Heights-Suffix'
            }
        
        # Pattern 26: Check for city names with "Park" suffix
        park_suffix_pattern = r'^([A-Za-z\s]+)\s+Park$'
        match = re.match(park_suffix_pattern, location_str, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            return {
                'city': f"{city} Park",
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'Park-Suffix'
            }
        
        # Pattern 27: Check for city names with "Hills" suffix
        hills_suffix_pattern = r'^([A-Za-z\s]+)\s+Hills$'
        match = re.match(hills_suffix_pattern, location_str, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            return {
                'city': f"{city} Hills",
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'Hills-Suffix'
            }
        
        # Pattern 28: Check for city names with "Springs" suffix
        springs_suffix_pattern = r'^([A-Za-z\s]+)\s+Springs$'
        match = re.match(springs_suffix_pattern, location_str, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            return {
                'city': f"{city} Springs",
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'Springs-Suffix'
            }
        
        # Pattern 29: Check for city names with "Ridge" suffix
        ridge_suffix_pattern = r'^([A-Za-z\s]+)\s+Ridge$'
        match = re.match(ridge_suffix_pattern, location_str, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            return {
                'city': f"{city} Ridge",
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'Ridge-Suffix'
            }
        
        # Pattern 30: Check for city names with "Valley" suffix
        valley_suffix_pattern = r'^([A-Za-z\s]+)\s+Valley$'
        match = re.match(valley_suffix_pattern, location_str, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            return {
                'city': f"{city} Valley",
                'county': None,
                'ward': None,
                'district': None,
                'location_type': 'Valley-Suffix'
            }
        
        # Pattern 31: Check for common Kentucky location patterns that might be missed
        # This is a fallback for patterns that don't fit the above categories
        
        # Check if it looks like a city name (starts with capital letter, reasonable length)
        if (len(location_str) >= 3 and len(location_str) <= 20 and 
            location_str[0].isupper() and 
            not location_str.isupper() and  # Not all caps
            not location_str.islower() and  # Not all lowercase
            not re.match(r'^\d', location_str)):  # Doesn't start with number
            
            # Try to match against known cities with lower threshold
            fuzzy_city_low = self._fuzzy_match(location_str, self.kentucky_cities, threshold=0.5)
            if fuzzy_city_low:
                return {
                    'city': fuzzy_city_low,
                    'county': None,
                    'ward': None,
                    'district': None,
                    'location_type': 'City-Only-Low-Confidence'
                }
            
            # Try to match against known counties with lower threshold
            fuzzy_county_low = self._fuzzy_match(location_str, self.kentucky_counties, threshold=0.5)
            if fuzzy_county_low:
                return {
                    'city': None,
                    'county': fuzzy_county_low,
                    'ward': None,
                    'district': None,
                    'location_type': 'County-Only-Low-Confidence'
                }
        
        # Pattern 32: Check for very short entries that might be abbreviations
        if len(location_str) <= 4 and location_str.isalpha():
            # Try to match against cities and counties with very low threshold
            fuzzy_city_vlow = self._fuzzy_match(location_str, self.kentucky_cities, threshold=0.4)
            if fuzzy_city_vlow:
                return {
                    'city': fuzzy_city_vlow,
                    'county': None,
                    'ward': None,
                    'district': None,
                    'location_type': 'City-Only-Very-Low-Confidence'
                }
            
            fuzzy_county_vlow = self._fuzzy_match(location_str, self.kentucky_counties, threshold=0.4)
            if fuzzy_county_vlow:
                return {
                    'city': None,
                    'county': fuzzy_county_vlow,
                    'ward': None,
                    'district': None,
                    'location_type': 'County-Only-Very-Low-Confidence'
                }
        
        # Default case - treat as unknown
        return {
            'city': None,
            'county': None,
            'ward': None,
            'district': None,
            'location_type': 'Unknown'
        }
    
    def _clean_parsed_location_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize parsed location data."""
        logger.info("Cleaning and standardizing parsed location data...")
        
        # Clean city names
        if 'city' in df.columns:
            df['city'] = df['city'].apply(lambda x: self._standardize_name(x) if pd.notna(x) else x)
        
        # Clean county names
        if 'county' in df.columns:
            df['county'] = df['county'].apply(lambda x: self._standardize_name(x) if pd.notna(x) else x)
        
        # Clean ward names
        if 'ward' in df.columns:
            df['ward'] = df['ward'].apply(lambda x: self._standardize_ward(x) if pd.notna(x) else x)
        
        # Clean district names
        if 'district' in df.columns:
            df['district'] = df['district'].apply(lambda x: self._standardize_district(x) if pd.notna(x) else x)
        
        return df
    
    def _standardize_name(self, name: str) -> str:
        """Standardize city/county names."""
        if pd.isna(name) or not name:
            return name
        
        name = str(name).strip()
        
        # Handle common abbreviations
        name_mapping = {
            'jeff': 'Jefferson',
            'jef': 'Jefferson',
            'fayet': 'Fayette',
            'kenton': 'Kenton',
            'campbel': 'Campbell',
            'boon': 'Boone',
            'hardin': 'Hardin',
            'davies': 'Daviess',
            'warren': 'Warren',
            'bullit': 'Bullitt',
            'oldham': 'Oldham',
            'lou': 'Louisville',
            'lex': 'Lexington',
            'bg': 'Bowling Green',
            'owb': 'Owensboro',
            'cov': 'Covington'
        }
        
        if name.lower() in name_mapping:
            return name_mapping[name.lower()]
        
        # Proper case formatting
        return name.title()
    
    def _standardize_ward(self, ward: str) -> str:
        """Standardize ward names."""
        if pd.isna(ward) or not ward:
            return ward
        
        ward = str(ward).strip()
        
        # Ensure consistent ward formatting
        if re.match(r'^\d+', ward):
            # Extract number and add "th" suffix if missing
            number_match = re.match(r'^(\d+)', ward)
            if number_match:
                number = int(number_match.group(1))
                suffix = self._get_ordinal_suffix(number)
                return f"{number}{suffix} Ward"
        
        return ward
    
    def _standardize_district(self, district: str) -> str:
        """Standardize district names."""
        if pd.isna(district) or not district:
            return district
        
        district = str(district).strip()
        
        # Ensure consistent district formatting
        if re.match(r'^\d+', district):
            # Extract number and add "th" suffix if missing
            number_match = re.match(r'^(\d+)', district)
            if number_match:
                number = int(number_match.group(1))
                suffix = self._get_ordinal_suffix(number)
                return f"{number}{suffix} District"
        
        return district
    
    def _get_ordinal_suffix(self, number: int) -> str:
        """Get ordinal suffix for a number."""
        if 10 <= number % 100 <= 20:
            return 'th'
        else:
            return {1: 'st', 2: 'nd', 3: 'rd'}.get(number % 10, 'th')
    
    def _clean_contact_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean contact information and parse location data."""
        logger.info("Cleaning contact information and parsing location data...")
        
        # Parse location data into structured components
        location_parsing_results = []
        for location in df['location']:
            parsed = self._parse_location(location)
            location_parsing_results.append(parsed)
        
        # Extract parsed components
        df['city'] = [result['city'] for result in location_parsing_results]
        df['county'] = [result['county'] for result in location_parsing_results]
        df['ward'] = [result['ward'] for result in location_parsing_results]
        df['district'] = [result['district'] for result in location_parsing_results]
        df['location_type'] = [result['location_type'] for result in location_parsing_results]
        
        # Clean addresses from location column
        def clean_address(address_str: str) -> str:
            if pd.isna(address_str):
                return None
            
            # Remove extra whitespace and quotes
            cleaned = str(address_str).strip().strip('"\'')
            # Remove multiple spaces
            cleaned = re.sub(r'\s+', ' ', cleaned)
            return cleaned
        
        # Kentucky data doesn't have phone, email, or website, so set to NULL
        df['phone'] = pd.NA
        df['email'] = pd.NA
        df['website'] = pd.NA
        
        # Map location column to address if available
        if 'location' in df.columns:
            df['address'] = df['location'].apply(clean_address)
        else:
            df['address'] = pd.NA
        
        # Add ZIP code column (will be populated later if geocoding is available)
        df['zip_code'] = pd.NA
        
        # Clean and standardize parsed location data
        df = self._clean_parsed_location_data(df)
        
        return df
    
    def clean_kentucky_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize Kentucky candidate data according to final schema.
        
        Args:
            df: Raw Kentucky candidate data DataFrame
            
        Returns:
            Cleaned DataFrame conforming to final schema
        """
        logger.info(f"Starting improved Kentucky data cleaning for {len(df)} records...")
        
        # Create a copy to avoid modifying original
        cleaned_df = df.copy()
        
        # Step 1: Handle election year and type
        cleaned_df = self._process_election_data(cleaned_df)
        
        # Step 2: Clean and standardize office and district information
        cleaned_df = self._process_office_and_district(cleaned_df)
        
        # Step 3: Clean candidate names
        cleaned_df = self._process_candidate_names(cleaned_df)
        
        # Step 4: Standardize party names
        cleaned_df = self._standardize_parties(cleaned_df)
        
        # Step 5: Clean contact information and parse location data
        cleaned_df = self._clean_contact_info(cleaned_df)
        
        # Step 6: Add required columns for final schema
        cleaned_df = self._add_required_columns(cleaned_df)
        
        # Step 7: Remove duplicate columns (AFTER processing names)
        cleaned_df = self._remove_duplicate_columns(cleaned_df)
        
        # Final step: Ensure column order matches Alaska's exact structure
        cleaned_df = self.ensure_column_order(cleaned_df)
        
        logger.info(f"Improved Kentucky data cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove original columns that have been replaced by cleaned versions."""
        logger.info("Removing duplicate columns...")
        
        # Columns to remove (original versions) - but NOT the new ones we created
        columns_to_remove = [
            'office_sought', 'election_date', 'election_type', 'Source_File'
        ]
        
        # Only remove if they exist and we have cleaned versions
        columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove)
            logger.info(f"Removed {len(columns_to_remove)} duplicate columns: {columns_to_remove}")
        
        return df
    
    def _process_election_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process election year and type from election_date and election_type columns."""
        logger.info("Processing election data...")
        
        # Use the actual column names
        election_date_col = 'election_date'
        election_type_col = 'election_type'
        
        def extract_election_year(election_date_str: str) -> Optional[int]:
            if pd.isna(election_date_str):
                return None
            
            election_date_str = str(election_date_str).strip()
            
            # Extract year from election date string
            year_match = re.search(r'20\d{2}', election_date_str)
            if year_match:
                return int(year_match.group())
            
            return None
        
        def standardize_election_type(election_type_str: str) -> str:
            if pd.isna(election_type_str):
                return "General"  # Default
            
            election_type_str = str(election_type_str).strip().lower()
            
            if 'primary' in election_type_str:
                return "Primary"
            elif 'general' in election_type_str:
                return "General"
            elif 'special' in election_type_str:
                return "Special"
            else:
                return "General"  # Default
        
        # Apply election processing
        df['election_year'] = df[election_date_col].apply(extract_election_year)
        df['election_type'] = df[election_type_col].apply(standardize_election_type)
        
        return df
    
    def _process_office_and_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize office and district information."""
        logger.info("Processing office and district information...")
        
        # Use the actual column names
        office_col = 'office_sought'
        
        def process_office_district(office_str: str) -> Tuple[str, Optional[str]]:
            if pd.isna(office_str):
                return None, None
            
            office_str = str(office_str).strip()
            
            # Handle US President
            if "president" in office_str.lower():
                return "US President", None
            
            # Handle US Representative
            if "representative" in office_str.lower() and "united states" in office_str.lower():
                return "US Representative", None
            
            # Handle US Senator
            if "senator" in office_str.lower() and "united states" in office_str.lower():
                return "US Senator", None
            
            # Handle State Senate
            if "senate" in office_str.lower() and "state" in office_str.lower():
                # Extract district if present
                district_match = re.search(r'district\s+(\d+)', office_str, re.IGNORECASE)
                if district_match:
                    return "State Senate", district_match.group(1)
                return "State Senate", None
            
            # Handle State House
            if "house" in office_str.lower() and "state" in office_str.lower():
                # Extract district if present
                district_match = re.search(r'district\s+(\d+)', office_str, re.IGNORECASE)
                if district_match:
                    return "State House", district_match.group(1)
                return "State House", None
            
            # Handle Governor
            if "governor" in office_str.lower():
                return "Governor", None
            
            # Handle Lieutenant Governor
            if "lieutenant governor" in office_str.lower():
                return "Lieutenant Governor", None
            
            # Handle Attorney General
            if "attorney general" in office_str.lower():
                return "Attorney General", None
            
            # Handle Secretary of State
            if "secretary of state" in office_str.lower():
                return "Secretary of State", None
            
            # Handle Auditor
            if "auditor" in office_str.lower():
                return "State Auditor", None
            
            # Handle Treasurer
            if "treasurer" in office_str.lower():
                return "State Treasurer", None
            
            # Handle Commissioner of Agriculture
            if "commissioner of agriculture" in office_str.lower():
                return "Commissioner of Agriculture", None
            
            # Handle Magistrate/Justice of the Peace
            if "magistrate" in office_str.lower() or "justice of the peace" in office_str.lower():
                return "Magistrate/Justice of the Peace", None
            
            # Handle City Council Member
            if "city council member" in office_str.lower():
                return "City Council Member", None
            
            # Handle other offices (keep as is)
            return office_str, None
        
        # Apply office and district processing
        office_results = df[office_col].apply(process_office_district)
        df['office'] = [result[0] for result in office_results]
        df['district'] = [result[1] for result in office_results]
        df['district'] = df['district'].astype('object')
        
        return df
    
    def _process_candidate_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process candidate names."""
        logger.info("Processing candidate names...")
        
        # Use the actual column names from raw data
        first_name_col = 'first_name'
        last_name_col = 'last_name'
        
        # Create candidate_name by combining first and last names
        df['candidate_name'] = df.apply(
            lambda row: f"{row[first_name_col]} {row[last_name_col]}".strip() 
            if pd.notna(row[first_name_col]) and pd.notna(row[last_name_col])
            else (row[first_name_col] if pd.notna(row[first_name_col]) else row[last_name_col]),
            axis=1
        )
        
        # Parse names into components
        df = self._parse_names(df, first_name_col, last_name_col)
        
        return df
    
    def _parse_names(self, df: pd.DataFrame, first_name_col: str, last_name_col: str) -> pd.DataFrame:
        """Parse candidate names into first, middle, last, prefix, suffix, nickname, and display components."""
        logger.info("Parsing candidate names...")
        
        # Store original names before processing
        original_first_names = df[first_name_col].copy()
        original_last_names = df[last_name_col].copy()
        
        # Initialize new columns with clean names (no quotes)
        df['middle_name'] = pd.NA
        df['prefix'] = pd.NA
        df['suffix'] = pd.NA
        df['nickname'] = pd.NA
        df['full_name_display'] = pd.NA
        
        # Create clean first_name and last_name columns
        clean_first_names = []
        clean_last_names = []
        
        for idx in range(len(df)):
            first_name = str(original_first_names.iloc[idx]).strip() if pd.notna(original_first_names.iloc[idx]) else ""
            last_name = str(original_last_names.iloc[idx]).strip() if pd.notna(original_last_names.iloc[idx]) else ""
            
            # Check for suffix in last name
            suffix = None
            if last_name:
                suffix_pattern = r'\b(Jr|Sr|II|III|IV|V|VI|VII|VIII|IX|X)\b'
                suffix_match = re.search(suffix_pattern, last_name, re.IGNORECASE)
                if suffix_match:
                    suffix = suffix_match.group(1)
                    # Remove suffix from last name
                    last_name = re.sub(suffix_pattern, '', last_name, flags=re.IGNORECASE).strip()
            
            # Check for prefix in first name
            prefix = None
            if first_name:
                prefix_pattern = r'\b(Dr|Mr|Mrs|Ms|Miss|Prof|Rev|Hon|Sen|Rep|Gov|Lt|Col|Gen|Adm|Capt|Maj|Sgt|Cpl|Pvt)\b'
                prefix_match = re.match(prefix_pattern, first_name, re.IGNORECASE)
                if prefix_match:
                    prefix = prefix_match.group(1)
                    # Remove prefix from first name
                    first_name = re.sub(f'^{prefix_pattern}\\s*', '', first_name, flags=re.IGNORECASE).strip()
            
            # Build display name
            display_parts = []
            if prefix:
                display_parts.append(prefix)
            if first_name:
                display_parts.append(first_name)
            if last_name:
                display_parts.append(last_name)
            if suffix:
                display_parts.append(suffix)
            
            display_name = ' '.join(display_parts).strip()
            
            # Store clean names
            clean_first_names.append(first_name)
            clean_last_names.append(last_name)
            
            # Assign parsed components
            df.at[idx, 'prefix'] = prefix
            df.at[idx, 'suffix'] = suffix
            df.at[idx, 'full_name_display'] = display_name
        
        # Assign clean names to the dataframe
        df['first_name'] = clean_first_names
        df['last_name'] = clean_last_names
        
        return df
    
    def _standardize_parties(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize party names."""
        logger.info("Standardizing party names...")
        
        # Check if party data exists in the raw data
        party_mapping = {
            'democrat': 'Democratic',
            'democratic': 'Democratic',
            'republican': 'Republican',
            'independent': 'Independent',
            'libertarian': 'Libertarian',
            'green': 'Green',
            'constitution': 'Constitution',
            'reform': 'Reform',
            'natural law': 'Natural Law',
            'socialist': 'Socialist',
            'communist': 'Communist',
            'american independent': 'American Independent',
            'peace and freedom': 'Peace and Freedom',
            'working families': 'Working Families',
            'women\'s equality': 'Women\'s Equality',
            'independence': 'Independence',
            'conservative': 'Conservative',
            'liberal': 'Liberal',
            'moderate': 'Moderate',
            'progressive': 'Progressive',
            'tea party': 'Tea Party',
            'no party preference': 'No Party Preference',
            'nonpartisan': 'Nonpartisan',
            'unaffiliated': 'Unaffiliated',
            'decline to state': 'Decline to State',
            'write-in': 'Write-in',
            'other': 'Other'
        }
        
        def standardize_party(party_str: str) -> str:
            if pd.isna(party_str):
                return None
            
            party_lower = str(party_str).strip().lower()
            return party_mapping.get(party_lower, party_str)
        
        # Look for party column in raw data
        party_columns = ['party', 'Party', 'PARTY', 'party_affiliation', 'Party Affiliation']
        party_col = None
        for col in party_columns:
            if col in df.columns:
                party_col = col
                break
        
        if party_col and df[party_col].notna().any():
            # Process existing party data
            df['party'] = df[party_col].apply(standardize_party)
            logger.info(f"Found and processed party data from column: {party_col}")
        else:
            # No party data available
            df['party'] = pd.NA
            logger.info("No party data found in raw data")
        
        return df
    
    def _add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all required columns for the final schema."""
        logger.info("Adding required columns...")
        
        # Use the actual column names
        first_name_col = 'first_name'
        last_name_col = 'last_name'
        office_col = 'office_sought'
        election_year_col = 'election_year'
        
        # Add state column
        df['state'] = self.state_name
        
        # Add original data preservation columns
        df['original_name'] = df.apply(lambda row: f"{row[first_name_col]} {row[last_name_col]}".strip(), axis=1)
        df['original_state'] = df['state'].copy()
        df['original_election_year'] = df[election_year_col].copy()
        df['original_office'] = df[office_col].copy()
        df['original_filing_date'] = pd.NA  # Not available in Kentucky data
        
        # Add missing columns with None values
        required_columns = [
            'id', 'stable_id', 'filing_date', 'election_date', 'facebook', 'twitter'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Set id to empty string (will be generated later in process)
        df['id'] = ""
        
        return df
    
    def ensure_column_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure columns match Alaska's exact order."""
        ALASKA_COLUMN_ORDER = [
            'election_year', 'election_type', 'office', 'district', 'candidate_name',
            'first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname',
            'full_name_display', 'party', 'phone', 'email', 'address', 'website',
            'state', 'original_name', 'original_state', 'original_election_year',
            'original_office', 'original_filing_date', 'id', 'stable_id', 'county',
            'city', 'zip_code', 'filing_date', 'election_date', 'facebook', 'twitter',
            'ward', 'location_type'  # Additional columns for Kentucky
        ]
        
        # Only add missing columns, don't overwrite existing ones
        for col in ALASKA_COLUMN_ORDER:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Create a new dataframe with columns in the correct order, preserving data
        result_df = pd.DataFrame()
        for col in ALASKA_COLUMN_ORDER:
            if col in df.columns:
                result_df[col] = df[col]
            else:
                result_df[col] = pd.NA
        
        return result_df

def clean_kentucky_candidates_improved(input_file: str, output_file: str = None, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean Kentucky candidate data with improved location parsing.
    
    Args:
        input_file: Path to the input Excel file
        output_file: Optional path to save the cleaned data (if None, will use default naming in output_dir)
        output_dir: Directory to save cleaned data (default: "cleaned_data")
        
    Returns:
        Cleaned DataFrame
    """
    # Load the data
    logger.info(f"Loading Kentucky data from {input_file}...")
    
    # Handle different file types
    if input_file.endswith('.csv'):
        df = pd.read_csv(input_file)
    else:
        df = pd.read_excel(input_file)
    
    # Initialize improved cleaner with output directory
    cleaner = ImprovedKentuckyCleaner(output_dir=output_dir)
    
    # Clean the data
    cleaned_df = cleaner.clean_kentucky_data(df)
    
    # Generate output filename if not provided
    if output_file is None:
        # Extract base name from input file
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"{base_name}_improved_cleaned_{timestamp}.xlsx")
    
    # Ensure output file is in the output directory
    if not os.path.dirname(output_file):
        output_file = os.path.join(output_dir, output_file)
    
    # Save the cleaned data
    logger.info(f"Saving improved cleaned data to {output_file}...")
    cleaned_df.to_excel(output_file, index=False)
    logger.info(f"Data saved successfully!")
    
    return cleaned_df

if __name__ == "__main__":
    # Show available input files
    print("Available input files:")
    available_files = []
    for file in os.listdir(DEFAULT_INPUT_DIR):
        if file.endswith(('.xlsx', '.xls')) and not file.startswith('~$'):
            available_files.append(os.path.join(DEFAULT_INPUT_DIR, file))
    
    if available_files:
        for i, file_path in enumerate(available_files, 1):
            print(f"  {i}. {os.path.basename(file_path)}")
    else:
        print("  No Excel files found in input directory")
        exit(1)
    
    # Find Kentucky files specifically
    kentucky_files = [f for f in available_files if 'kentucky' in os.path.basename(f).lower()]
    
    if not kentucky_files:
        print("No Kentucky files found in input directory")
        exit(1)
    
    # Use the first Kentucky file
    input_file = kentucky_files[0]
    output_dir = DEFAULT_OUTPUT_DIR
    
    print(f"\nProcessing Kentucky file: {os.path.basename(input_file)}")
    print(f"Output directory: {output_dir}")
    
    # Clean the data and save to the new output directory
    cleaned_data = clean_kentucky_candidates_improved(input_file, output_dir=output_dir)
    print(f"\nCleaned {len(cleaned_data)} records")
    print(f"Columns: {cleaned_data.columns.tolist()}")
    print(f"Data saved to: {output_dir}/")
