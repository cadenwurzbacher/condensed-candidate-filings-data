#!/usr/bin/env python3
"""
Enhanced Office Standardizer for CandidateFilings.com Data Processing

This module provides comprehensive office name standardization, mapping various
raw office names to standardized categories while preserving the original
office name in a source_office column for reference and debugging.
"""

import pandas as pd
import re
import logging
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

class OfficeStandardizer:
    """
    Standardizes office names to predefined categories with safety validation.
    
    Target standardized office names:
    - US President, US House, US Senate
    - State House, State Senate, Governor
    - State Attorney General, State Treasurer, Lieutenant Governor, Secretary of State
    - City Council, City Commission, County Commission, School Board
    - Judicial: Justice of the Peace, Circuit Judge, District Judge, County Judge
    - County: Sheriff, Constable, Coroner, Surveyor, County Clerk, County Attorney
    - Special: Soil Conservation Officer, Property Valuation Administrator, Jailer
    """
    
    def __init__(self):
        """Initialize the office standardizer with comprehensive mappings."""
        self.office_mappings = self._build_office_mappings()
        self.district_patterns = self._build_district_patterns()
        
    def _build_office_mappings(self) -> Dict[str, str]:
        """
        Build comprehensive office name mappings.
        
        Returns:
            Dictionary mapping source office names to standardized names
        """
        mappings = {
            # US President variations (very specific to avoid false matches)
            'president of the united states': 'US President',
            'president of united states': 'US President',
            'u.s. president': 'US President',
            'us president': 'US President',
            'united states president': 'US President',
            'president and vice president': 'US President',
            'president/vice president': 'US President',
            'President/vice President': 'US President',
            
            # Judge variations with /
            'probate/magistrate judge': 'Judge',
            'Probate/magistrate Judge': 'Judge',
            'probate judge/chief magistrate': 'Judge',
            'Probate Judge/chief Magistrate': 'Judge',
            'probate judge/magistrate': 'Judge',
            'Probate Judge/magistrate': 'Judge',
            'probate judge/magistrate judge': 'Judge',
            'Probate Judge/magistrate Judge': 'Judge',
            
            # City Council variations with /
            'city commissioner/council': 'City Council',
            'City Commissioner/council': 'City Council',
            
            # Additional / variations
            'officer (mayor/chair/attorney)': 'Officer',
            'Officer (mayor/chair/attorney)': 'Officer',
            'clerk/treasurer': 'Clerk/Treasurer',
            'Clerk/treasurer': 'Clerk/Treasurer',
            'comissioner chairman/person': 'Commissioner',
            'Comissioner Chairman/person': 'Commissioner',
            
            # US House variations
            'u.s. representative': 'US House',
            'us representative': 'US House',
            'united states representative': 'US House',
            'u.s. house': 'US House',
            'us house': 'US House',
            'united states house': 'US House',
            'u.s. house of representatives': 'US House',
            'us house of representatives': 'US House',
            'united states house of representatives': 'US House',
            
            # US House - map all district variations to clean "US House"
            'us representative, 1st district': 'US House',
            'US Representative, 1st District': 'US House',
            'us representative, 2nd district': 'US House',
            'US Representative, 2nd District': 'US House',
            'us representative, 3rd district': 'US House',
            'US Representative, 3rd District': 'US House',
            'us representative, 4th district': 'US House',
            'US Representative, 4th District': 'US House',
            'us representative, 5th district': 'US House',
            'US Representative, 5th District': 'US House',
            'us representative, 6th district': 'US House',
            'US Representative, 6th District': 'US House',
            'us representative, 7th district': 'US House',
            'US Representative, 7th District': 'US House',
            'us representative, 8th district': 'US House',
            'US Representative, 8th District': 'US House',
            'us representative, 9th district': 'US House',
            'US Representative, 9th District': 'US House',
            'us representative, 10th district': 'US House',
            'US Representative, 10th District': 'US House',
            'us representative, 11th district': 'US House',
            'us representative, 12th district': 'US House',
            'us representative, 13th district': 'US House',
            'us representative, 14th district': 'US House',
            'us representative, 15th district': 'US House',
            'us representative, 16th district': 'US House',
            'us representative, 17th district': 'US House',
            'us representative, 18th district': 'US House',
            'us representative, 19th district': 'US House',
            'us representative, 20th district': 'US House',
            'us representative, 21st district': 'US House',
            'us representative, 22nd district': 'US House',
            'us representative, 23rd district': 'US House',
            'us representative, 24th district': 'US House',
            'us representative, 25th district': 'US House',
            'us representative, 26th district': 'US House',
            'us representative, 27th district': 'US House (District: 27)',
            'us representative, 28th district': 'US House (District: 28)',
            'us representative, 29th district': 'US House (District: 29)',
            'us representative, 30th district': 'US House (District: 30)',
            'us representative, 31st district': 'US House (District: 31)',
            'us representative, 32nd district': 'US House (District: 32)',
            'us representative, 33rd district': 'US House (District: 33)',
            'us representative, 34th district': 'US House (District: 34)',
            'us representative, 35th district': 'US House (District: 35)',
            'us representative, 36th district': 'US House (District: 36)',
            'us representative, 37th district': 'US House (District: 37)',
            'us representative, 38th district': 'US House (District: 38)',
            'us representative, 39th district': 'US House (District: 39)',
            'us representative, 40th district': 'US House (District: 40)',
            'us representative, 41st district': 'US House (District: 41)',
            'us representative, 42nd district': 'US House (District: 42)',
            'us representative, 43rd district': 'US House (District: 43)',
            'us representative, 44th district': 'US House (District: 44)',
            'us representative, 45th district': 'US House (District: 45)',
            'us representative, 46th district': 'US House (District: 46)',
            'us representative, 47th district': 'US House (District: 47)',
            'us representative, 48th district': 'US House (District: 48)',
            'us representative, 49th district': 'US House (District: 49)',
            'us representative, 50th district': 'US House (District: 50)',
            'us representative, 51st district': 'US House (District: 51)',
            'us representative, 52nd district': 'US House (District: 52)',
            'us representative, 53rd district': 'US House (District: 53)',
            'us representative, 54th district': 'US House (District: 54)',
            'us representative, 55th district': 'US House (District: 55)',
            'us representative, 56th district': 'US House (District: 56)',
            'us representative, 57th district': 'US House (District: 57)',
            'us representative, 58th district': 'US House (District: 58)',
            'us representative, 59th district': 'US House (District: 59)',
            'us representative, 60th district': 'US House (District: 60)',
            'us representative, 61st district': 'US House (District: 61)',
            'us representative, 62nd district': 'US House (District: 62)',
            'us representative, 63rd district': 'US House (District: 63)',
            'us representative, 64th district': 'US House (District: 64)',
            'us representative, 65th district': 'US House (District: 65)',
            'us representative, 66th district': 'US House (District: 66)',
            'us representative, 67th district': 'US House (District: 67)',
            'us representative, 68th district': 'US House (District: 68)',
            'us representative, 69th district': 'US House (District: 69)',
            'us representative, 70th district': 'US House (District: 70)',
            'us representative, 71st district': 'US House (District: 71)',
            'us representative, 72nd district': 'US House (District: 72)',
            'us representative, 73rd district': 'US House (District: 73)',
            'us representative, 74th district': 'US House (District: 74)',
            'us representative, 75th district': 'US House (District: 75)',
            'us representative, 76th district': 'US House (District: 76)',
            'us representative, 77th district': 'US House (District: 77)',
            'us representative, 78th district': 'US House (District: 78)',
            'us representative, 79th district': 'US House (District: 79)',
            'us representative, 80th district': 'US House (District: 80)',
            'us representative, 81st district': 'US House (District: 81)',
            'us representative, 82nd district': 'US House (District: 82)',
            'us representative, 83rd district': 'US House (District: 83)',
            'us representative, 84th district': 'US House (District: 84)',
            'us representative, 85th district': 'US House (District: 85)',
            'us representative, 86th district': 'US House (District: 86)',
            'us representative, 87th district': 'US House (District: 87)',
            'us representative, 88th district': 'US House (District: 88)',
            'us representative, 89th district': 'US House (District: 89)',
            'us representative, 90th district': 'US House (District: 90)',
            'us representative, 91st district': 'US House (District: 91)',
            'us representative, 92nd district': 'US House (District: 92)',
            'us representative, 93rd district': 'US House (District: 93)',
            'us representative, 94th district': 'US House (District: 94)',
            'us representative, 95th district': 'US House (District: 95)',
            'us representative, 96th district': 'US House (District: 96)',
            'us representative, 97th district': 'US House (District: 97)',
            'us representative, 98th district': 'US House (District: 98)',
            'us representative, 99th district': 'US House (District: 99)',
            'us representative, 100th district': 'US House (District: 100)',
            
            # "Representative To The 119th United States Congress - District X" pattern
            'representative to the 119th united states congress - district 1': 'US House (District: 1)',
            'Representative To The 119th United States Congress - District 1': 'US House (District: 1)',
            'representative to the 119th united states congress - district 2': 'US House (District: 2)',
            'Representative To The 119th United States Congress - District 2': 'US House (District: 2)',
            'representative to the 119th united states congress - district 3': 'US House (District: 3)',
            'Representative To The 119th United States Congress - District 3': 'US House (District: 3)',
            'representative to the 119th united states congress - district 4': 'US House (District: 4)',
            'Representative To The 119th United States congress - District 4': 'US House (District: 4)',
            'representative to the 119th united states congress - district 5': 'US House (District: 5)',
            'Representative To The 119th United States Congress - District 5': 'US House (District: 5)',
            'representative to the 119th united states congress - district 6': 'US House (District: 6)',
            'Representative To The 119th United States Congress - District 6': 'US House (District: 6)',
            'representative to the 119th united states congress - district 7': 'US House (District: 7)',
            'Representative To The 119th United States Congress - District 7': 'US House (District: 7)',
            'representative to the 119th united states congress - district 8': 'US House (District: 8)',
            'Representative To The 119th United States Congress - District 8': 'US House (District: 8)',
            'representative to the 119th united states congress - district 9': 'US House (District: 9)',
            'Representative To The 119th United States Congress - District 9': 'US House (District: 9)',
            'representative to the 119th united states congress - district 10': 'US House (District: 10)',
            'Representative To The 119th United States Congress - District 10': 'US House (District: 10)',
            'representative to the 119th united states congress - district 11': 'US House (District: 11)',
            'representative to the 119th united states congress - district 12': 'US House (District: 12)',
            'representative to the 119th united states congress - district 13': 'US House (District: 13)',
            'representative to the 119th united states congress - district 14': 'US House (District: 14)',
            'representative to the 119th united states congress - district 15': 'US House (District: 15)',
            'representative to the 119th united states congress - district 16': 'US House (District: 16)',
            'representative to the 119th united states congress - district 17': 'US House (District: 17)',
            'representative to the 119th united states congress - district 18': 'US House (District: 18)',
            'representative to the 119th united states congress - district 19': 'US House (District: 19)',
            'representative to the 119th united states congress - district 20': 'US House (District: 20)',
            'representative to the 119th united states congress - district 21': 'US House (District: 21)',
            'representative to the 119th united states congress - district 22': 'US House (District: 22)',
            'representative to the 119th united states congress - district 23': 'US House (District: 23)',
            'representative to the 119th united states congress - district 24': 'US House (District: 24)',
            'representative to the 119th united states congress - district 25': 'US House (District: 25)',
            'representative to the 119th united states congress - district 26': 'US House (District: 26)',
            'representative to the 119th united states congress - district 27': 'US House (District: 27)',
            'representative to the 119th united states congress - district 28': 'US House (District: 28)',
            'representative to the 119th united states congress - district 29': 'US House (District: 29)',
            'representative to the 119th united states congress - district 30': 'US House (District: 30)',
            'representative to the 119th united states congress - district 31': 'US House (District: 31)',
            'representative to the 119th united states congress - district 32': 'US House (District: 32)',
            'representative to the 119th united states congress - district 33': 'US House (District: 33)',
            'representative to the 119th united states congress - district 34': 'US House (District: 34)',
            'representative to the 119th united states congress - district 35': 'US House (District: 35)',
            'representative to the 119th united states congress - district 36': 'US House (District: 36)',
            'representative to the 119th united states congress - district 37': 'US House (District: 37)',
            'representative to the 119th united states congress - district 38': 'US House (District: 38)',
            'representative to the 119th united states congress - district 39': 'US House (District: 39)',
            'representative to the 119th united states congress - district 40': 'US House (District: 40)',
            'representative to the 119th united states congress - district 41': 'US House (District: 41)',
            'representative to the 119th united states congress - district 42': 'US House (District: 42)',
            'representative to the 119th united states congress - district 43': 'US House (District: 43)',
            'representative to the 119th united states congress - district 44': 'US House (District: 44)',
            'representative to the 119th united states congress - district 45': 'US House (District: 45)',
            'representative to the 119th united states congress - district 46': 'US House (District: 46)',
            'representative to the 119th united states congress - district 47': 'US House (District: 47)',
            'representative to the 119th united states congress - district 48': 'US House (District: 48)',
            'representative to the 119th united states congress - district 49': 'US House (District: 49)',
            'representative to the 119th united states congress - district 50': 'US House (District: 50)',
            'representative to the 119th united states congress - district 51': 'US House (District: 51)',
            'representative to the 119th united states congress - district 52': 'US House (District: 52)',
            'representative to the 119th united states congress - district 53': 'US House (District: 53)',
            'representative to the 119th united states congress - district 54': 'US House (District: 54)',
            'representative to the 119th united states congress - district 55': 'US House (District: 55)',
            'representative to the 119th united states congress - district 56': 'US House (District: 56)',
            'representative to the 119th united states congress - district 57': 'US House (District: 57)',
            'representative to the 119th united states congress - district 58': 'US House (District: 58)',
            'representative to the 119th united states congress - district 59': 'US House (District: 59)',
            'representative to the 119th united states congress - district 60': 'US House (District: 60)',
            'representative to the 119th united states congress - district 61': 'US House (District: 61)',
            'representative to the 119th united states congress - district 62': 'US House (District: 62)',
            'representative to the 119th united states congress - district 63': 'US House (District: 63)',
            'representative to the 119th united states congress - district 64': 'US House (District: 64)',
            'representative to the 119th united states congress - district 65': 'US House (District: 65)',
            'representative to the 119th united states congress - district 66': 'US House (District: 66)',
            'representative to the 119th united states congress - district 67': 'US House (District: 67)',
            'representative to the 119th united states congress - district 68': 'US House (District: 68)',
            'representative to the 119th united states congress - district 69': 'US House (District: 69)',
            'representative to the 119th united states congress - district 70': 'US House (District: 70)',
            'representative to the 119th united states congress - district 71': 'US House (District: 71)',
            'representative to the 119th united states congress - district 72': 'US House (District: 72)',
            'representative to the 119th united states congress - district 73': 'US House (District: 73)',
            'representative to the 119th united states congress - district 74': 'US House (District: 74)',
            'representative to the 119th united states congress - district 75': 'US House (District: 75)',
            'representative to the 119th united states congress - district 76': 'US House (District: 76)',
            'representative to the 119th united states congress - district 77': 'US House (District: 77)',
            'representative to the 119th united states congress - district 78': 'US House (District: 78)',
            'representative to the 119th united states congress - district 79': 'US House (District: 79)',
            'representative to the 119th united states congress - district 80': 'US House (District: 80)',
            'representative to the 119th united states congress - district 81': 'US House (District: 81)',
            'representative to the 119th united states congress - district 82': 'US House (District: 82)',
            'representative to the 119th united states congress - district 83': 'US House (District: 83)',
            'representative to the 119th united states congress - district 84': 'US House (District: 84)',
            'representative to the 119th united states congress - district 85': 'US House (District: 85)',
            'representative to the 119th united states congress - district 86': 'US House (District: 86)',
            'representative to the 119th united states congress - district 87': 'US House (District: 87)',
            'representative to the 119th united states congress - district 88': 'US House (District: 88)',
            'representative to the 119th united states congress - district 89': 'US House (District: 89)',
            'representative to the 119th united states congress - district 90': 'US House (District: 90)',
            'representative to the 119th united states congress - district 91': 'US House (District: 91)',
            'representative to the 119th united states congress - district 92': 'US House (District: 92)',
            'representative to the 119th united states congress - district 93': 'US House (District: 93)',
            'representative to the 119th united states congress - district 94': 'US House (District: 94)',
            'representative to the 119th united states congress - district 95': 'US House (District: 95)',
            'representative to the 119th united states congress - district 96': 'US House (District: 96)',
            'representative to the 119th united states congress - district 97': 'US House (District: 97)',
            'representative to the 119th united states congress - district 98': 'US House (District: 98)',
            'representative to the 119th united states congress - district 99': 'US House (District: 99)',
            'representative to the 119th united states congress - district 100': 'US House (District: 100)',
            
            # "United States Representative, District X" pattern
            'united states representative, district 1 (d)': 'US House (District: 1)',
            'United States Representative, District 1 (d)': 'US House (District: 1)',
            'united states representative, district 1 (r)': 'US House (District: 1)',
            'United States Representative, District 1 (r)': 'US House (District: 1)',
            'united states representative, district 2 (d)': 'US House (District: 2)',
            'United States Representative, District 2 (d)': 'US House (District: 2)',
            'united states representative, district 2 (r)': 'US House (District: 2)',
            'United States Representative, District 2 (r)': 'US House (District: 2)',
            'united states representative, district 3 (d)': 'US House (District: 3)',
            'United States Representative, District 3 (d)': 'US House (District: 3)',
            'united states representative, district 3 (r)': 'US House (District: 3)',
            'United States Representative, District 3 (r)': 'US House (District: 3)',
            'united states representative, district 4 (d)': 'US House (District: 4)',
            'United States Representative, District 4 (d)': 'US House (District: 4)',
            'united states representative, district 4 (r)': 'US House (District: 4)',
            'United States Representative, District 4 (r)': 'US House (District: 4)',
            'united states representative, district 5 (d)': 'US House (District: 5)',
            'United States Representative, District 5 (d)': 'US House (District: 5)',
            'united states representative, district 5 (r)': 'US House (District: 5)',
            'United States Representative, District 5 (r)': 'US House (District: 5)',
            'united states representative, district 6 (d)': 'US House (District: 6)',
            'United States Representative, District 6 (d)': 'US House (District: 6)',
            'united states representative, district 6 (r)': 'US House (District: 6)',
            'United States Representative, District 6 (r)': 'US House (District: 6)',
            'united states representative, district 7 (d)': 'US House (District: 7)',
            'United States Representative, District 7 (d)': 'US House (District: 7)',
            'united states representative, district 7 (r)': 'US House (District: 7)',
            'United States Representative, District 7 (r)': 'US House (District: 7)',
            'united states representative, district 8 (d)': 'US House (District: 8)',
            'United States Representative, District 8 (d)': 'US House (District: 8)',
            'united states representative, district 8 (r)': 'US House (District: 8)',
            'United States Representative, District 8 (r)': 'US House (District: 8)',
            'united states representative, district 9 (d)': 'US House (District: 9)',
            'United States Representative, District 9 (d)': 'US House (District: 9)',
            'united states representative, district 9 (r)': 'US House (District: 9)',
            'United States Representative, District 9 (r)': 'US House (District: 9)',
            'united states representative, district 10 (d)': 'US House (District: 10)',
            'United States Representative, District 10 (d)': 'US House (District: 10)',
            'united states representative, district 10 (r)': 'US House (District: 10)',
            'United States Representative, District 10 (r)': 'US House (District: 10)',
            'united states representative, district 11 (d)': 'US House (District: 11)',
            'United States Representative, District 11 (d)': 'US House (District: 11)',
            'united states representative, district 11 (r)': 'US House (District: 11)',
            'United States Representative, District 11 (r)': 'US House (District: 11)',
            'united states representative, district 12 (d)': 'US House (District: 12)',
            'United States Representative, District 12 (d)': 'US House (District: 12)',
            'united states representative, district 12 (r)': 'US House (District: 12)',
            'United States Representative, District 12 (r)': 'US House (District: 12)',
            'united states representative, district 13 (d)': 'US House (District: 13)',
            'United States Representative, District 13 (d)': 'US House (District: 13)',
            'united states representative, district 13 (r)': 'US House (District: 13)',
            'United States Representative, District 13 (r)': 'US House (District: 13)',
            'united states representative, district 14 (d)': 'US House (District: 14)',
            'United States Representative, District 14 (d)': 'US House (District: 14)',
            'united states representative, district 14 (r)': 'US House (District: 14)',
            'United States Representative, District 14 (r)': 'US House (District: 14)',
            
            # "United States Representative, Xth District" pattern
            'united states representative, first district': 'US House (District: 1)',
            'united states representative, second district': 'US House (District: 2)',
            'united states representative, third district': 'US House (District: 3)',
            'united states representative, fourth district': 'US House (District: 4)',
            'united states representative, fifth district': 'US House (District: 5)',
            'united states representative, sixth district': 'US House (District: 6)',
            'united states representative, seventh district': 'US House (District: 7)',
            'united states representative, eighth district': 'US House (District: 8)',
            'united states representative, ninth district': 'US House (District: 9)',
            'united states representative, tenth district': 'US House (District: 10)',
            'united states representative, eleventh district': 'US House (District: 11)',
            'united states representative, twelfth district': 'US House (District: 12)',
            'united states representative, thirteenth district': 'US House (District: 13)',
            'united states representative, fourteenth district': 'US House (District: 14)',
            
            # Arizona-specific US House mappings
            'u.s. representative in congress - district no. 1': 'US House',
            'U.S. Representative in Congress - District No. 1': 'US House',
            'u.s. representative in congress - district no. 2': 'US House',
            'U.S. Representative in Congress - District No. 2': 'US House',
            'u.s. representative in congress - district no. 3': 'US House',
            'U.S. Representative in Congress - District No. 3': 'US House',
            'u.s. representative in congress - district no. 4': 'US House',
            'U.S. Representative in Congress - District No. 4': 'US House',
            'u.s. representative in congress - district no. 5': 'US House',
            'U.S. Representative in Congress - District No. 5': 'US House',
            'u.s. representative in congress - district no. 6': 'US House',
            'U.S. Representative in Congress - District No. 6': 'US House',
            'u.s. representative in congress - district no. 7': 'US House',
            'U.S. Representative in Congress - District No. 7': 'US House',
            'u.s. representative in congress - district no. 8': 'US House',
            'U.S. Representative in Congress - District No. 8': 'US House',
            'u.s. representative in congress - district no. 9': 'US House',
            'U.S. Representative in Congress - District No. 9': 'US House',
            
            # Other US House variations
            'u. s. representative': 'US House',
            'u.s. rep.': 'US House',
            'rep in congress': 'US House',
            'representative in congress': 'US House',
            'representative to congress': 'US House',
            
            # Arkansas-specific US House mapping
            'u.s. congress': 'US House',
            
            # Colorado-specific US House mappings
            'representative to the 119th united states congress - district 1': 'US House',
            'Representative to the 119th United States Congress - District 1': 'US House',
            'representative to the 119th united states congress - district 2': 'US House',
            'Representative to the 119th United States Congress - District 2': 'US House',
            'representative to the 119th united states congress - district 3': 'US House',
            'Representative to the 119th United States Congress - District 3': 'US House',
            'representative to the 119th united states congress - district 4': 'US House',
            'Representative to the 119th United States Congress - District 4': 'US House',
            'representative to the 119th united states congress - district 5': 'US House',
            'Representative to the 119th United States Congress - District 5': 'US House',
            'representative to the 119th united states congress - district 6': 'US House',
            'Representative to the 119th United States Congress - District 6': 'US House',
            'representative to the 119th united states congress - district 7': 'US House',
            'Representative to the 119th United States Congress - District 7': 'US House',
            'representative to the 119th united states congress - district 8': 'US House',
            'Representative to the 119th United States Congress - District 8': 'US House',
            
            # US Senate variations
            'u.s. senator': 'US Senate',
            'us senator': 'US Senate',
            'united states senator': 'US Senate',
            'u.s. senate': 'US Senate',
            'us senate': 'US Senate',
            'united states senate': 'US Senate',
            
            # Arizona-specific US Senate mapping
            'u.s. senator': 'US Senate',
            
            # Arizona-specific US President mapping
            'president of the united states': 'US President',
            
            # State House variations
            'state representative': 'State House',
            'state house': 'State House',
            'state house of representatives': 'State House',
            'house of representatives': 'State House',
            'representative': 'State House',
            'house member': 'State House',
            'state house member': 'State House',
            
            # Comprehensive State House District Mappings
            # "House District XX" pattern (Alaska-style)
            'house district 01': 'State House (District: 1)',
            'House District 01': 'State House (District: 1)',
            'house district 02': 'State House (District: 2)',
            'House District 02': 'State House (District: 2)',
            'house district 03': 'State House (District: 3)',
            'House District 03': 'State House (District: 3)',
            'house district 04': 'State House (District: 4)',
            'House District 04': 'State House (District: 4)',
            'house district 05': 'State House (District: 5)',
            'House District 05': 'State House (District: 5)',
            'house district 06': 'State House (District: 6)',
            'House District 06': 'State House (District: 6)',
            'house district 07': 'State House (District: 7)',
            'House District 07': 'State House (District: 7)',
            'house district 08': 'State House (District: 8)',
            'House District 08': 'State House (District: 8)',
            'house district 09': 'State House (District: 9)',
            'House District 09': 'State House (District: 9)',
            'house district 10': 'State House (District: 10)',
            'House District 10': 'State House (District: 10)',
            'house district 11': 'State House (District: 11)',
            'house district 12': 'State House (District: 12)',
            'house district 13': 'State House (District: 13)',
            'house district 14': 'State House (District: 14)',
            'house district 15': 'State House (District: 15)',
            'house district 16': 'State House (District: 16)',
            'house district 17': 'State House (District: 17)',
            'house district 18': 'State House (District: 18)',
            'house district 19': 'State House (District: 19)',
            'house district 20': 'State House (District: 20)',
            'house district 21': 'State House (District: 21)',
            'house district 22': 'State House (District: 22)',
            'house district 23': 'State House (District: 23)',
            'house district 24': 'State House (District: 24)',
            'house district 25': 'State House (District: 25)',
            'house district 26': 'State House (District: 26)',
            'house district 27': 'State House (District: 27)',
            'house district 28': 'State House (District: 28)',
            'house district 29': 'State House (District: 29)',
            'house district 30': 'State House (District: 30)',
            'house district 31': 'State House (District: 31)',
            'house district 32': 'State House (District: 32)',
            'house district 33': 'State House (District: 33)',
            'house district 34': 'State House (District: 34)',
            'house district 35': 'State House (District: 35)',
            'house district 36': 'State House (District: 36)',
            'house district 37': 'State House (District: 37)',
            'house district 38': 'State House (District: 38)',
            'house district 39': 'State House (District: 39)',
            'house district 40': 'State House (District: 40)',
            'house district 41': 'State House (District: 41)',
            'house district 42': 'State House (District: 42)',
            'house district 43': 'State House (District: 43)',
            'house district 44': 'State House (District: 44)',
            'house district 45': 'State House (District: 45)',
            'house district 46': 'State House (District: 46)',
            'house district 47': 'State House (District: 47)',
            'house district 48': 'State House (District: 48)',
            'house district 49': 'State House (District: 49)',
            'house district 50': 'State House (District: 50)',
            'house district 51': 'State House (District: 51)',
            'house district 52': 'State House (District: 52)',
            'house district 53': 'State House (District: 53)',
            'house district 54': 'State House (District: 54)',
            'house district 55': 'State House (District: 55)',
            'house district 56': 'State House (District: 56)',
            'house district 57': 'State House (District: 57)',
            'house district 58': 'State House (District: 58)',
            'house district 59': 'State House (District: 59)',
            'house district 60': 'State House (District: 60)',
            'house district 61': 'State House (District: 61)',
            'house district 62': 'State House (District: 62)',
            'house district 63': 'State House (District: 63)',
            'house district 64': 'State House (District: 64)',
            'house district 65': 'State House (District: 65)',
            'house district 66': 'State House (District: 66)',
            'house district 67': 'State House (District: 67)',
            'house district 68': 'State House (District: 68)',
            'house district 69': 'State House (District: 69)',
            'house district 70': 'State House (District: 70)',
            'house district 71': 'State House (District: 71)',
            'house district 72': 'State House (District: 72)',
            'house district 73': 'State House (District: 73)',
            'house district 74': 'State House (District: 74)',
            'house district 75': 'State House (District: 75)',
            'house district 76': 'State House (District: 76)',
            'house district 77': 'State House (District: 77)',
            'house district 78': 'State House (District: 78)',
            'house district 79': 'State House (District: 79)',
            'house district 80': 'State House (District: 80)',
            'house district 81': 'State House (District: 81)',
            'house district 82': 'State House (District: 82)',
            'house district 83': 'State House (District: 83)',
            'house district 84': 'State House (District: 84)',
            'house district 85': 'State House (District: 85)',
            'house district 86': 'State House (District: 86)',
            'house district 87': 'State House (District: 87)',
            'house district 88': 'State House (District: 88)',
            'house district 89': 'State House (District: 89)',
            'house district 90': 'State House (District: 90)',
            'house district 91': 'State House (District: 91)',
            'house district 92': 'State House (District: 92)',
            'house district 93': 'State House (District: 93)',
            'house district 94': 'State House (District: 94)',
            'house district 95': 'State House (District: 95)',
            'house district 96': 'State House (District: 96)',
            'house district 97': 'State House (District: 97)',
            'house district 98': 'State House (District: 98)',
            'house district 99': 'State House (District: 99)',
            'house district 100': 'State House (District: 100)',
            
            # "State Rep Dis X" pattern
            'state rep dis 1': 'State House (District: 1)',
            'state rep dis 2': 'State House (District: 2)',
            'state rep dis 3': 'State House (District: 3)',
            'state rep dis 4': 'State House (District: 4)',
            'state rep dis 5': 'State House (District: 5)',
            'state rep dis 6': 'State House (District: 6)',
            'state rep dis 7': 'State House (District: 7)',
            'state rep dis 8': 'State House (District: 8)',
            'state rep dis 9': 'State House (District: 9)',
            'state rep dis 10': 'State House (District: 10)',
            'state rep dis 11': 'State House (District: 11)',
            'state rep dis 12': 'State House (District: 12)',
            'state rep dis 13': 'State House (District: 13)',
            'state rep dis 14': 'State House (District: 14)',
            'state rep dis 15': 'State House (District: 15)',
            'state rep dis 16': 'State House (District: 16)',
            'state rep dis 17': 'State House (District: 17)',
            'state rep dis 18': 'State House (District: 18)',
            'state rep dis 19': 'State House (District: 19)',
            'state rep dis 20': 'State House (District: 20)',
            'state rep dis 21': 'State House (District: 21)',
            'state rep dis 22': 'State House (District: 22)',
            'state rep dis 23': 'State House (District: 23)',
            'state rep dis 24': 'State House (District: 24)',
            'state rep dis 25': 'State House (District: 25)',
            'state rep dis 26': 'State House (District: 26)',
            'state rep dis 27': 'State House (District: 27)',
            'state rep dis 28': 'State House (District: 28)',
            'state rep dis 29': 'State House (District: 29)',
            'state rep dis 30': 'State House (District: 30)',
            'state rep dis 31': 'State House (District: 31)',
            'state rep dis 32': 'State House (District: 32)',
            'state rep dis 33': 'State House (District: 33)',
            'state rep dis 34': 'State House (District: 34)',
            'state rep dis 35': 'State House (District: 35)',
            'state rep dis 36': 'State House (District: 36)',
            'state rep dis 37': 'State House (District: 37)',
            'state rep dis 38': 'State House (District: 38)',
            'state rep dis 39': 'State House (District: 39)',
            'state rep dis 40': 'State House (District: 40)',
            'state rep dis 41': 'State House (District: 41)',
            
            # "State Representative, Xth District" pattern
            'state representative, 1st district': 'State House (District: 1)',
            'State Representative, 1st District': 'State House (District: 1)',
            'state representative, 2nd district': 'State House (District: 2)',
            'State Representative, 2nd District': 'State House (District: 2)',
            'state representative, 3rd district': 'State House (District: 3)',
            'State Representative, 3rd District': 'State House (District: 3)',
            'state representative, 4th district': 'State House (District: 4)',
            'State Representative, 4th District': 'State House (District: 4)',
            'state representative, 5th district': 'State House (District: 5)',
            'State Representative, 5th District': 'State House (District: 5)',
            'state representative, 6th district': 'State House (District: 6)',
            'State Representative, 6th District': 'State House (District: 6)',
            'state representative, 7th district': 'State House (District: 7)',
            'State Representative, 7th District': 'State House (District: 7)',
            'state representative, 8th district': 'State House (District: 8)',
            'State Representative, 8th District': 'State House (District: 8)',
            'state representative, 9th district': 'State House (District: 9)',
            'State Representative, 9th District': 'State House (District: 9)',
            'state representative, 10th district': 'State House (District: 10)',
            'State Representative, 10th District': 'State House (District: 10)',
            'state representative, 11th district': 'State House (District: 11)',
            'state representative, 12th district': 'State House (District: 12)',
            'state representative, 13th district': 'State House (District: 13)',
            'state representative, 14th district': 'State House (District: 14)',
            'state representative, 15th district': 'State House (District: 15)',
            'state representative, 16th district': 'State House (District: 16)',
            'state representative, 17th district': 'State House (District: 17)',
            'state representative, 18th district': 'State House (District: 18)',
            'state representative, 19th district': 'State House (District: 19)',
            'state representative, 20th district': 'State House (District: 20)',
            'state representative, 21st district': 'State House (District: 21)',
            'state representative, 22nd district': 'State House (District: 22)',
            'state representative, 23rd district': 'State House (District: 23)',
            'state representative, 24th district': 'State House (District: 24)',
            'state representative, 25th district': 'State House (District: 25)',
            'state representative, 26th district': 'State House (District: 26)',
            'state representative, 27th district': 'State House (District: 27)',
            'state representative, 28th district': 'State House (District: 28)',
            'state representative, 29th district': 'State House (District: 29)',
            'state representative, 30th district': 'State House (District: 30)',
            'state representative, 31st district': 'State House (District: 31)',
            'state representative, 32nd district': 'State House (District: 32)',
            'state representative, 33rd district': 'State House (District: 33)',
            'state representative, 34th district': 'State House (District: 34)',
            'state representative, 35th district': 'State House (District: 35)',
            'state representative, 36th district': 'State House (District: 36)',
            'state representative, 37th district': 'State House (District: 37)',
            'state representative, 38th district': 'State House (District: 38)',
            'state representative, 39th district': 'State House (District: 39)',
            'state representative, 40th district': 'State House (District: 40)',
            'state representative, 41st district': 'State House (District: 41)',
            'state representative, 42nd district': 'State House (District: 42)',
            'state representative, 43rd district': 'State House (District: 43)',
            'state representative, 44th district': 'State House (District: 44)',
            'state representative, 45th district': 'State House (District: 45)',
            'state representative, 46th district': 'State House (District: 46)',
            'state representative, 47th district': 'State House (District: 47)',
            'state representative, 48th district': 'State House (District: 48)',
            'state representative, 49th district': 'State House (District: 49)',
            'state representative, 50th district': 'State House (District: 50)',
            'state representative, 51st district': 'State House (District: 51)',
            'state representative, 52nd district': 'State House (District: 52)',
            'state representative, 53rd district': 'State House (District: 53)',
            'state representative, 54th district': 'State House (District: 54)',
            'state representative, 55th district': 'State House (District: 55)',
            'state representative, 56th district': 'State House (District: 56)',
            'state representative, 57th district': 'State House (District: 57)',
            'state representative, 58th district': 'State House (District: 58)',
            'state representative, 59th district': 'State House (District: 59)',
            'state representative, 60th district': 'State House (District: 60)',
            'state representative, 61st district': 'State House (District: 61)',
            'state representative, 62nd district': 'State House (District: 62)',
            'state representative, 63rd district': 'State House (District: 63)',
            'state representative, 64th district': 'State House (District: 64)',
            'state representative, 65th district': 'State House (District: 65)',
            'state representative, 66th district': 'State House (District: 66)',
            'state representative, 67th district': 'State House (District: 67)',
            'state representative, 68th district': 'State House (District: 68)',
            'state representative, 69th district': 'State House (District: 69)',
            'state representative, 70th district': 'State House (District: 70)',
            'state representative, 71st district': 'State House (District: 71)',
            'state representative, 72nd district': 'State House (District: 72)',
            'state representative, 73rd district': 'State House (District: 73)',
            'state representative, 74th district': 'State House (District: 74)',
            'state representative, 75th district': 'State House (District: 75)',
            'state representative, 76th district': 'State House (District: 76)',
            'state representative, 77th district': 'State House (District: 77)',
            'state representative, 78th district': 'State House (District: 78)',
            'state representative, 79th district': 'State House (District: 79)',
            'state representative, 80th district': 'State House (District: 80)',
            'state representative, 81st district': 'State House (District: 81)',
            'state representative, 82nd district': 'State House (District: 82)',
            'state representative, 83rd district': 'State House (District: 83)',
            'state representative, 84th district': 'State House (District: 84)',
            'state representative, 85th district': 'State House (District: 85)',
            'state representative, 86th district': 'State House (District: 86)',
            'state representative, 87th district': 'State House (District: 87)',
            'state representative, 88th district': 'State House (District: 88)',
            'state representative, 89th district': 'State House (District: 89)',
            'state representative, 90th district': 'State House (District: 90)',
            'state representative, 91st district': 'State House (District: 91)',
            'state representative, 92nd district': 'State House (District: 92)',
            'state representative, 93rd district': 'State House (District: 93)',
            'state representative, 94th district': 'State House (District: 94)',
            'state representative, 95th district': 'State House (District: 95)',
            'state representative, 96th district': 'State House (District: 96)',
            'state representative, 97th district': 'State House (District: 97)',
            'state representative, 98th district': 'State House (District: 98)',
            'state representative, 99th district': 'State House (District: 99)',
            'state representative, 100th district': 'State House (District: 100)',
            
            # "State Representative, District XXX" pattern
            'state representative, district 001': 'State House (District: 1)',
            'state representative, district 002': 'State House (District: 2)',
            'state representative, district 003': 'State House (District: 3)',
            'state representative, district 004': 'State House (District: 4)',
            'state representative, district 005': 'State House (District: 5)',
            'state representative, district 006': 'State House (District: 6)',
            'state representative, district 007': 'State House (District: 7)',
            'state representative, district 008': 'State House (District: 8)',
            'state representative, district 009': 'State House (District: 9)',
            'state representative, district 010': 'State House (District: 10)',
            'state representative, district 011': 'State House (District: 11)',
            'state representative, district 012': 'State House (District: 12)',
            'state representative, district 013': 'State House (District: 13)',
            'state representative, district 014': 'State House (District: 14)',
            'state representative, district 015': 'State House (District: 15)',
            'state representative, district 016': 'State House (District: 16)',
            'state representative, district 017': 'State House (District: 17)',
            'state representative, district 018': 'State House (District: 18)',
            'state representative, district 019': 'State House (District: 19)',
            'state representative, district 020': 'State House (District: 20)',
            'state representative, district 021': 'State House (District: 21)',
            'state representative, district 022': 'State House (District: 22)',
            'state representative, district 023': 'State House (District: 23)',
            'state representative, district 024': 'State House (District: 24)',
            'state representative, district 025': 'State House (District: 25)',
            'state representative, district 026': 'State House (District: 26)',
            'state representative, district 027': 'State House (District: 27)',
            'state representative, district 028': 'State House (District: 28)',
            'state representative, district 029': 'State House (District: 29)',
            'state representative, district 030': 'State House (District: 30)',
            'state representative, district 031': 'State House (District: 31)',
            'state representative, district 032': 'State House (District: 32)',
            'state representative, district 033': 'State House (District: 33)',
            'state representative, district 034': 'State House (District: 34)',
            'state representative, district 035': 'State House (District: 35)',
            'state representative, district 036': 'State House (District: 36)',
            'state representative, district 037': 'State House (District: 37)',
            'state representative, district 038': 'State House (District: 38)',
            'state representative, district 039': 'State House (District: 39)',
            'state representative, district 040': 'State House (District: 40)',
            'state representative, district 041': 'State House (District: 41)',
            'state representative, district 042': 'State House (District: 42)',
            'state representative, district 043': 'State House (District: 43)',
            'state representative, district 044': 'State House (District: 44)',
            'state representative, district 045': 'State House (District: 45)',
            'state representative, district 046': 'State House (District: 46)',
            'state representative, district 047': 'State House (District: 47)',
            'state representative, district 048': 'State House (District: 48)',
            'state representative, district 049': 'State House (District: 49)',
            'state representative, district 050': 'State House (District: 50)',
            'state representative, district 051': 'State House (District: 51)',
            'state representative, district 052': 'State House (District: 52)',
            'state representative, district 053': 'State House (District: 53)',
            'state representative, district 054': 'State House (District: 54)',
            'state representative, district 055': 'State House (District: 55)',
            'state representative, district 056': 'State House (District: 56)',
            'state representative, district 057': 'State House (District: 57)',
            'state representative, district 058': 'State House (District: 58)',
            'state representative, district 059': 'State House (District: 59)',
            'state representative, district 060': 'State House (District: 60)',
            'state representative, district 061': 'State House (District: 61)',
            'state representative, district 062': 'State House (District: 62)',
            'state representative, district 063': 'State House (District: 63)',
            'state representative, district 064': 'State House (District: 64)',
            'state representative, district 065': 'State House (District: 65)',
            'state representative, district 066': 'State House (District: 66)',
            'state representative, district 067': 'State House (District: 67)',
            'state representative, district 068': 'State House (District: 68)',
            'state representative, district 069': 'State House (District: 69)',
            'state representative, district 070': 'State House (District: 70)',
            'state representative, district 071': 'State House (District: 71)',
            'state representative, district 072': 'State House (District: 72)',
            'state representative, district 073': 'State House (District: 73)',
            'state representative, district 074': 'State House (District: 74)',
            'state representative, district 075': 'State House (District: 75)',
            'state representative, district 076': 'State House (District: 76)',
            'state representative, district 077': 'State House (District: 77)',
            'state representative, district 078': 'State House (District: 78)',
            'state representative, district 079': 'State House (District: 79)',
            'state representative, district 080': 'State House (District: 80)',
            'state representative, district 081': 'State House (District: 81)',
            'state representative, district 082': 'State House (District: 82)',
            'state representative, district 083': 'State House (District: 83)',
            'state representative, district 084': 'State House (District: 84)',
            'state representative, district 085': 'State House (District: 85)',
            'state representative, district 086': 'State House (District: 86)',
            'state representative, district 087': 'State House (District: 87)',
            'state representative, district 088': 'State House (District: 88)',
            'state representative, district 089': 'State House (District: 89)',
            'state representative, district 090': 'State House (District: 90)',
            'state representative, district 091': 'State House (District: 91)',
            'state representative, district 092': 'State House (District: 92)',
            'state representative, district 093': 'State House (District: 93)',
            'state representative, district 094': 'State House (District: 94)',
            'state representative, district 095': 'State House (District: 95)',
            'state representative, district 096': 'State House (District: 96)',
            'state representative, district 097': 'State House (District: 97)',
            'state representative, district 098': 'State House (District: 98)',
            'state representative, district 099': 'State House (District: 99)',
            'state representative, district 100': 'State House (District: 100)',
            
            # "Xth Representative" pattern
            '1st representative': 'State House (District: 1)',
            '2nd representative': 'State House (District: 2)',
            '3rd representative': 'State House (District: 3)',
            '4th representative': 'State House (District: 4)',
            '5th representative': 'State House (District: 5)',
            '6th representative': 'State House (District: 6)',
            '7th representative': 'State House (District: 7)',
            '8th representative': 'State House (District: 8)',
            '9th representative': 'State House (District: 9)',
            '10th representative': 'State House (District: 10)',
            '11th representative': 'State House (District: 11)',
            '12th representative': 'State House (District: 12)',
            '13th representative': 'State House (District: 13)',
            '14th representative': 'State House (District: 14)',
            '15th representative': 'State House (District: 15)',
            '16th representative': 'State House (District: 16)',
            '17th representative': 'State House (District: 17)',
            '18th representative': 'State House (District: 18)',
            '19th representative': 'State House (District: 19)',
            '20th representative': 'State House (District: 20)',
            '21st representative': 'State House (District: 21)',
            '22nd representative': 'State House (District: 22)',
            '23rd representative': 'State House (District: 23)',
            '24th representative': 'State House (District: 24)',
            '25th representative': 'State House (District: 25)',
            '26th representative': 'State House (District: 26)',
            '27th representative': 'State House (District: 27)',
            '28th representative': 'State House (District: 28)',
            '29th representative': 'State House (District: 29)',
            '30th representative': 'State House (District: 30)',
            '31st representative': 'State House (District: 31)',
            '32nd representative': 'State House (District: 32)',
            '33rd representative': 'State House (District: 33)',
            '34th representative': 'State House (District: 34)',
            '35th representative': 'State House (District: 35)',
            '36th representative': 'State House (District: 36)',
            '37th representative': 'State House (District: 37)',
            '38th representative': 'State House (District: 38)',
            '39th representative': 'State House (District: 39)',
            '40th representative': 'State House (District: 40)',
            '41st representative': 'State House (District: 41)',
            '42nd representative': 'State House (District: 42)',
            '43rd representative': 'State House (District: 43)',
            '44th representative': 'State House (District: 44)',
            '45th representative': 'State House (District: 45)',
            '46th representative': 'State House (District: 46)',
            '47th representative': 'State House (District: 47)',
            '48th representative': 'State House (District: 48)',
            '49th representative': 'State House (District: 49)',
            '50th representative': 'State House (District: 50)',
            '51st representative': 'State House (District: 51)',
            '52nd representative': 'State House (District: 52)',
            '53rd representative': 'State House (District: 53)',
            '54th representative': 'State House (District: 54)',
            '55th representative': 'State House (District: 55)',
            '56th representative': 'State House (District: 56)',
            '57th representative': 'State House (District: 57)',
            '58th representative': 'State House (District: 58)',
            '59th representative': 'State House (District: 59)',
            '60th representative': 'State House (District: 60)',
            '61st representative': 'State House (District: 61)',
            '62nd representative': 'State House (District: 62)',
            '63rd representative': 'State House (District: 63)',
            '64th representative': 'State House (District: 64)',
            '65th representative': 'State House (District: 65)',
            '66th representative': 'State House (District: 66)',
            '67th representative': 'State House (District: 67)',
            '68th representative': 'State House (District: 68)',
            '69th representative': 'State House (District: 69)',
            '70th representative': 'State House (District: 70)',
            '71st representative': 'State House (District: 71)',
            '72nd representative': 'State House (District: 72)',
            '73rd representative': 'State House (District: 73)',
            '74th representative': 'State House (District: 74)',
            '75th representative': 'State House (District: 75)',
            '76th representative': 'State House (District: 76)',
            '77th representative': 'State House (District: 77)',
            '78th representative': 'State House (District: 78)',
            '79th representative': 'State House (District: 79)',
            '80th representative': 'State House (District: 80)',
            '81st representative': 'State House (District: 81)',
            '82nd representative': 'State House (District: 82)',
            '83rd representative': 'State House (District: 83)',
            '84th representative': 'State House (District: 84)',
            '85th representative': 'State House (District: 85)',
            '86th representative': 'State House (District: 86)',
            '87th representative': 'State House (District: 87)',
            '88th representative': 'State House (District: 88)',
            '89th representative': 'State House (District: 89)',
            '90th representative': 'State House (District: 90)',
            '91st representative': 'State House (District: 91)',
            '92nd representative': 'State House (District: 92)',
            '93rd representative': 'State House (District: 93)',
            '94th representative': 'State House (District: 94)',
            '95th representative': 'State House (District: 95)',
            '96th representative': 'State House (District: 96)',
            '97th representative': 'State House (District: 97)',
            '98th representative': 'State House (District: 98)',
            '99th representative': 'State House (District: 99)',
            '100th representative': 'State House (District: 100)',
            '101st representative': 'State House (District: 101)',
            '102nd representative': 'State House (District: 102)',
            '103rd representative': 'State House (District: 103)',
            '104th representative': 'State House (District: 104)',
            '105th representative': 'State House (District: 105)',
            '106th representative': 'State House (District: 106)',
            '107th representative': 'State House (District: 107)',
            '108th representative': 'State House (District: 108)',
            '109th representative': 'State House (District: 109)',
            '110th representative': 'State House (District: 110)',
            '111th representative': 'State House (District: 111)',
            '112th representative': 'State House (District: 112)',
            '113th representative': 'State House (District: 113)',
            '114th representative': 'State House (District: 114)',
            '115th representative': 'State House (District: 115)',
            '116th representative': 'State House (District: 116)',
            '117th representative': 'State House (District: 117)',
            '118th representative': 'State House (District: 118)',
            
            # Arizona-specific State House mappings
            'state representative - district no. 1': 'State House',
            'State Representative - District No. 1': 'State House',
            'state representative - district no. 2': 'State House',
            'State Representative - District No. 2': 'State House',
            'state representative - district no. 3': 'State House',
            'State Representative - District No. 3': 'State House',
            'state representative - district no. 4': 'State House',
            'State Representative - District No. 4': 'State House',
            'state representative - district no. 5': 'State House',
            'State Representative - District No. 5': 'State House',
            'state representative - district no. 6': 'State House',
            'State Representative - District No. 6': 'State House',
            'state representative - district no. 7': 'State House',
            'State Representative - District No. 7': 'State House',
            'state representative - district no. 8': 'State House',
            'State Representative - District No. 8': 'State House',
            'state representative - district no. 9': 'State House',
            'State Representative - District No. 9': 'State House',
            'state representative - district no. 10': 'State House',
            'State Representative - District No. 10': 'State House',
            'state representative - district no. 11': 'State House',
            'State Representative - District No. 11': 'State House',
            'state representative - district no. 12': 'State House',
            'State Representative - District No. 12': 'State House',
            'state representative - district no. 13': 'State House',
            'State Representative - District No. 13': 'State House',
            'state representative - district no. 14': 'State House',
            'State Representative - District No. 14': 'State House',
            'state representative - district no. 15': 'State House',
            'State Representative - District No. 15': 'State House',
            'state representative - district no. 16': 'State House',
            'State Representative - District No. 16': 'State House',
            'state representative - district no. 17': 'State House',
            'State Representative - District No. 17': 'State House',
            'state representative - district no. 18': 'State House',
            'State Representative - District No. 18': 'State House',
            'state representative - district no. 19': 'State House',
            'State Representative - District No. 19': 'State House',
            'state representative - district no. 20': 'State House',
            'State Representative - District No. 20': 'State House',
            'state representative - district no. 21': 'State House',
            'State Representative - District No. 21': 'State House',
            'state representative - district no. 22': 'State House',
            'State Representative - District No. 22': 'State House',
            'state representative - district no. 23': 'State House',
            'State Representative - District No. 23': 'State House',
            'state representative - district no. 24': 'State House',
            'State Representative - District No. 24': 'State House',
            'state representative - district no. 25': 'State House',
            'State Representative - District No. 25': 'State House',
            'state representative - district no. 26': 'State House',
            'State Representative - District No. 26': 'State House',
            'state representative - district no. 27': 'State House',
            'State Representative - District No. 27': 'State House',
            'state representative - district no. 28': 'State House',
            'State Representative - District No. 28': 'State House',
            'state representative - district no. 29': 'State House',
            'State Representative - District No. 29': 'State House',
            'state representative - district no. 30': 'State House',
            'State Representative - District No. 30': 'State House',
            
            # Colorado-specific State House mappings
            'state representative - district 1': 'State House',
            'State Representative - District 1': 'State House',
            'state representative - district 2': 'State House',
            'State Representative - District 2': 'State House',
            'state representative - district 3': 'State House',
            'State Representative - District 3': 'State House',
            'state representative - district 4': 'State House',
            'State Representative - District 4': 'State House',
            'state representative - district 5': 'State House',
            'State Representative - District 5': 'State House',
            'state representative - district 6': 'State House',
            'State Representative - District 6': 'State House',
            'state representative - district 7': 'State House',
            'State Representative - District 7': 'State House',
            'state representative - district 8': 'State House',
            'State Representative - District 8': 'State House',
            'state representative - district 9': 'State House',
            'State Representative - District 9': 'State House',
            'state representative - district 10': 'State House',
            'State Representative - District 10': 'State House',
            'state representative - district 11': 'State House',
            'State Representative - District 11': 'State House',
            'state representative - district 12': 'State House',
            'State Representative - District 12': 'State House',
            'state representative - district 13': 'State House',
            'State Representative - District 13': 'State House',
            'state representative - district 14': 'State House',
            'State Representative - District 14': 'State House',
            'state representative - district 15': 'State House',
            'State Representative - District 15': 'State House',
            'state representative - district 16': 'State House',
            'State Representative - District 16': 'State House',
            'state representative - district 17': 'State House',
            'State Representative - District 17': 'State House',
            'state representative - district 18': 'State House',
            'State Representative - District 18': 'State House',
            'state representative - district 19': 'State House',
            'State Representative - District 19': 'State House',
            'state representative - district 20': 'State House',
            'State Representative - District 20': 'State House',
            'state representative - district 21': 'State House',
            'State Representative - District 21': 'State House',
            'state representative - district 22': 'State House',
            'State Representative - District 22': 'State House',
            'state representative - district 23': 'State House',
            'State Representative - District 23': 'State House',
            'state representative - district 24': 'State House',
            'State Representative - District 24': 'State House',
            'state representative - district 25': 'State House',
            'State Representative - District 25': 'State House',
            'state representative - district 26': 'State House',
            'State Representative - District 26': 'State House',
            'state representative - district 27': 'State House',
            'State Representative - District 27': 'State House',
            'state representative - district 28': 'State House',
            'State Representative - District 28': 'State House',
            'state representative - district 29': 'State House',
            'State Representative - District 29': 'State House',
            'state representative - district 30': 'State House',
            'State Representative - District 30': 'State House',
            'state representative - district 31': 'State House',
            'State Representative - District 31': 'State House',
            'state representative - district 32': 'State House',
            'State Representative - District 32': 'State House',
            'state representative - district 33': 'State House',
            'State Representative - District 33': 'State House',
            'state representative - district 34': 'State House',
            'State Representative - District 34': 'State House',
            'state representative - district 35': 'State House',
            'State Representative - District 35': 'State House',
            'state representative - district 36': 'State House',
            'State Representative - District 36': 'State House',
            'state representative - district 37': 'State House',
            'State Representative - District 37': 'State House',
            'state representative - district 38': 'State House',
            'State Representative - District 38': 'State House',
            'state representative - district 39': 'State House',
            'State Representative - District 39': 'State House',
            'state representative - district 40': 'State House',
            'State Representative - District 40': 'State House',
            'state representative - district 41': 'State House',
            'State Representative - District 41': 'State House',
            'state representative - district 42': 'State House',
            'State Representative - District 42': 'State House',
            'state representative - district 43': 'State House',
            'State Representative - District 43': 'State House',
            'state representative - district 44': 'State House',
            'State Representative - District 44': 'State House',
            'state representative - district 45': 'State House',
            'State Representative - District 45': 'State House',
            'state representative - district 46': 'State House',
            'State Representative - District 46': 'State House',
            'state representative - district 47': 'State House',
            'State Representative - District 47': 'State House',
            'state representative - district 48': 'State House',
            'State Representative - District 48': 'State House',
            'state representative - district 49': 'State House',
            'State Representative - District 49': 'State House',
            'state representative - district 50': 'State House',
            'State Representative - District 50': 'State House',
            'state representative - district 51': 'State House',
            'State Representative - District 51': 'State House',
            'state representative - district 52': 'State House',
            'State Representative - District 52': 'State House',
            'state representative - district 53': 'State House',
            'State Representative - District 53': 'State House',
            'state representative - district 54': 'State House',
            'State Representative - District 54': 'State House',
            'state representative - district 55': 'State House',
            'State Representative - District 55': 'State House',
            'state representative - district 56': 'State House',
            'State Representative - District 56': 'State House',
            'state representative - district 57': 'State House',
            'State Representative - District 57': 'State House',
            'state representative - district 58': 'State House',
            'State Representative - District 58': 'State House',
            'state representative - district 59': 'State House',
            'State Representative - District 59': 'State House',
            'state representative - district 60': 'State House',
            'State Representative - District 60': 'State House',
            'state representative - district 61': 'State House',
            'State Representative - District 61': 'State House',
            'state representative - district 62': 'State House',
            'State Representative - District 62': 'State House',
            'state representative - district 63': 'State House',
            'State Representative - District 63': 'State House',
            'state representative - district 64': 'State House',
            'State Representative - District 64': 'State House',
            'state representative - district 65': 'State House',
            'State Representative - District 65': 'State House',
            
            # Colorado-specific Regent mappings
            'regent of the university of colorado - congressional district 1': 'Regent',
            'Regent of the University of Colorado - Congressional District 1': 'Regent',
            'regent of the university of colorado - congressional district 2': 'Regent',
            'Regent of the University of Colorado - Congressional District 2': 'Regent',
            'regent of the university of colorado - congressional district 3': 'Regent',
            'Regent of the University of Colorado - Congressional District 3': 'Regent',
            'regent of the university of colorado - congressional district 4': 'Regent',
            'Regent of the University of Colorado - Congressional District 4': 'Regent',
            'regent of the university of colorado - congressional district 5': 'Regent',
            'Regent of the University of Colorado - Congressional District 5': 'Regent',
            'regent of the university of colorado - congressional district 6': 'Regent',
            'Regent of the University of Colorado - Congressional District 6': 'Regent',
            'regent of the university of colorado - congressional district 7': 'Regent',
            'Regent of the University of Colorado - Congressional District 7': 'Regent',
            'regent of the university of colorado - congressional district 8': 'Regent',
            'Regent of the University of Colorado - Congressional District 8': 'Regent',
            
            # Colorado-specific State Board of Education mappings
            'state board of education member - congressional district 1': 'State Board of Education',
            'State Board of Education Member - Congressional District 1': 'State Board of Education',
            'state board of education member - congressional district 2': 'State Board of Education',
            'State Board of Education Member - Congressional District 2': 'State Board of Education',
            'state board of education member - congressional district 3': 'State Board of Education',
            'State Board of Education Member - Congressional District 3': 'State Board of Education',
            'state board of education member - congressional district 4': 'State Board of Education',
            'State Board of Education Member - Congressional District 4': 'State Board of Education',
            'state board of education member - congressional district 5': 'State Board of Education',
            'State Board of Education Member - Congressional District 5': 'State Board of Education',
            'state board of education member - congressional district 6': 'State Board of Education',
            'State Board of Education Member - Congressional District 6': 'State Board of Education',
            'state board of education member - congressional district 7': 'State Board of Education',
            'State Board of Education Member - Congressional District 7': 'State Board of Education',
            'state board of education member - congressional district 8': 'State Board of Education',
            'State Board of Education Member - Congressional District 8': 'State Board of Education',
            
            # Kansas-specific State Board of Education mapping
            'member, state board of education': 'State Board of Education',
            'Member, State Board of Education': 'State Board of Education',
            
            # Kansas-specific House mapping
            'kansas house of representatives': 'State House',
            'Kansas House of Representatives': 'State House',
            
            # Delaware-specific office mappings
            # Levy Court mappings
            '1st lc dist': 'Levy Court',
            '1ST LC DIST': 'Levy Court',
            '2nd lc dist': 'Levy Court',
            '2ND LC DIST': 'Levy Court',
            '3rd lc dist': 'Levy Court',
            '3RD LC DIST': 'Levy Court',
            '4th lc dist': 'Levy Court',
            '4TH LC DIST': 'Levy Court',
            '5th lc dist': 'Levy Court',
            '5TH LC DIST': 'Levy Court',
            '6th lc dist': 'Levy Court',
            '6TH LC DIST': 'Levy Court',
            'lc at lrg': 'Levy Court',
            'LC AT LRG': 'Levy Court',
            'levy court': 'Levy Court',
            'Levy Court': 'Levy Court',
            
            # City Council mappings
            'city cncl at lrg': 'City Council',
            'CITY CNCL AT LRG': 'City Council',
            'city cncl dis 1': 'City Council',
            'CITY CNCL DIS 1': 'City Council',
            'city cncl dis 2': 'City Council',
            'CITY CNCL DIS 2': 'City Council',
            'city cncl dis 3': 'City Council',
            'CITY CNCL DIS 3': 'City Council',
            'city cncl dis 4': 'City Council',
            'CITY CNCL DIS 4': 'City Council',
            'city cncl dis 5': 'City Council',
            'CITY CNCL DIS 5': 'City Council',
            'city cncl dis 6': 'City Council',
            'CITY CNCL DIS 6': 'City Council',
            'city cncl dis 7': 'City Council',
            'CITY CNCL DIS 7': 'City Council',
            'city cncl dis 8': 'City Council',
            'CITY CNCL DIS 8': 'City Council',
            'city council': 'City Council',
            'City Council': 'City Council',
            
            # County Council mappings
            'cnty cncl dis 1': 'County Council',
            'CNTY CNCL DIS 1': 'County Council',
            'cnty cncl dis 2': 'County Council',
            'CNTY CNCL DIS 2': 'County Council',
            'cnty cncl dis 3': 'County Council',
            'CNTY CNCL DIS 3': 'County Council',
            'cnty cncl dis 4': 'County Council',
            'CNTY CNCL DIS 4': 'County Council',
            'cnty cncl dis 5': 'County Council',
            'CNTY CNCL DIS 5': 'County Council',
            'cnty cncl dis 6': 'County Council',
            'CNTY CNCL DIS 6': 'County Council',
            'cnty cncl dis 7': 'County Council',
            'CNTY CNCL DIS 7': 'County Council',
            'cnty cncl dis 8': 'County Council',
            'CNTY CNCL DIS 8': 'County Council',
            'cnty cncl dis 9': 'County Council',
            'CNTY CNCL DIS 9': 'County Council',
            'cnty cncl dis 10': 'County Council',
            'CNTY CNCL DIS 10': 'County Council',
            'cnty cncl dis 11': 'County Council',
            'CNTY CNCL DIS 11': 'County Council',
            'cnty cncl dis 12': 'County Council',
            'CNTY CNCL DIS 12': 'County Council',
            
            # Wilmington specific mappings
            'city of wilmington city council district 1': 'City Council',
            'City of Wilmington City Council District 1': 'City Council',
            'city of wilmington city council district 2': 'City Council',
            'City of Wilmington City Council District 2': 'City Council',
            'city of wilmington city council district 3': 'City Council',
            'City of Wilmington City Council District 3': 'City Council',
            'city of wilmington city council district 4': 'City Council',
            'City of Wilmington City Council District 4': 'City Council',
            'city of wilmington city council district 5': 'City Council',
            'City of Wilmington City Council District 5': 'City Council',
            'city of wilmington city council district 6': 'City Council',
            'City of Wilmington City Council District 6': 'City Council',
            'city of wilmington city council district 7': 'City Council',
            'City of Wilmington City Council District 7': 'City Council',
            'city of wilmington city council district 8': 'City Council',
            'City of Wilmington City Council District 8': 'City Council',
            'city of wilmington city council member at-large': 'City Council',
            'City of Wilmington City Council Member At-Large': 'City Council',
            'wilmington city council at large': 'City Council',
            'WILMINGTON CITY COUNCIL AT LARGE': 'City Council',
            'wilmington city council district 1': 'City Council',
            'WILMINGTON CITY COUNCIL DISTRICT 1': 'City Council',
            'wilmington city council district 2': 'City Council',
            'WILMINGTON CITY COUNCIL DISTRICT 2': 'City Council',
            'wilmington city council district 3': 'City Council',
            'WILMINGTON CITY COUNCIL DISTRICT 3': 'City Council',
            'wilmington city council district 4': 'City Council',
            'WILMINGTON CITY COUNCIL DISTRICT 4': 'City Council',
            'wilmington city council district 5': 'City Council',
            'WILMINGTON CITY COUNCIL DISTRICT 5': 'City Council',
            'wilmington city council district 6': 'City Council',
            'WILMINGTON CITY COUNCIL DISTRICT 6': 'City Council',
            'wilmington city council district 7': 'City Council',
            'WILMINGTON CITY COUNCIL DISTRICT 7': 'City Council',
            'wilmington city council district 8': 'City Council',
            'WILMINGTON CITY COUNCIL DISTRICT 8': 'City Council',
            
            # New Castle County specific mappings
            'new castle county council district 1': 'County Council',
            'NEW CASTLE COUNTY COUNCIL DISTRICT 1': 'County Council',
            'new castle county council district 2': 'County Council',
            'NEW CASTLE COUNTY COUNCIL DISTRICT 2': 'County Council',
            'new castle county council district 3': 'County Council',
            'NEW CASTLE COUNTY COUNCIL DISTRICT 3': 'County Council',
            'new castle county council district 4': 'County Council',
            'NEW CASTLE COUNTY COUNCIL DISTRICT 4': 'County Council',
            'new castle county council district 5': 'County Council',
            'NEW CASTLE COUNTY COUNCIL DISTRICT 5': 'County Council',
            'new castle county council district 6': 'County Council',
            'NEW CASTLE COUNTY COUNCIL DISTRICT 6': 'County Council',
            'new castle county council district 7': 'County Council',
            'NEW CASTLE COUNTY COUNCIL DISTRICT 7': 'County Council',
            'new castle county council district 8': 'County Council',
            'NEW CASTLE COUNTY COUNCIL DISTRICT 8': 'County Council',
            'new castle county council district 9': 'County Council',
            'NEW CASTLE COUNTY COUNCIL DISTRICT 9': 'County Council',
            'new castle county council district 10': 'County Council',
            'NEW CASTLE COUNTY COUNCIL DISTRICT 10': 'County Council',
            'new castle county council district 11': 'County Council',
            'NEW CASTLE COUNTY COUNCIL DISTRICT 11': 'County Council',
            'new castle county council district 12': 'County Council',
            'NEW CASTLE COUNTY COUNCIL DISTRICT 12': 'County Council',
            'new castle county executive': 'County Executive',
            'NEW CASTLE COUNTY EXECUTIVE': 'County Executive',
            'new castle county president of county council': 'County Council President',
            'NEW CASTLE COUNTY PRESIDENT OF COUNTY COUNCIL': 'County Council President',
            'new castle president of county council': 'County Council President',
            'NEW CASTLE PRESIDENT OF COUNTY COUNCIL': 'County Council President',
            
            # Sussex County specific mappings
            'sussex county council district 1': 'County Council',
            'SUSSEX COUNTY COUNCIL DISTRICT 1': 'County Council',
            'sussex county council district 2': 'County Council',
            'SUSSEX COUNTY COUNCIL DISTRICT 2': 'County Council',
            'sussex county council district 3': 'County Council',
            'SUSSEX COUNTY COUNCIL DISTRICT 3': 'County Council',
            
            # Kent County specific mappings
            'kent county levy court commissioner dist 1': 'Levy Court',
            'KENT COUNTY LEVY COURT COMMISSIONER DIST 1': 'Levy Court',
            'kent county levy court commissioner dist 3': 'Levy Court',
            'KENT COUNTY LEVY COURT COMMISSIONER DIST 3': 'Levy Court',
            'kent county levy court commissioner dist 5': 'Levy Court',
            'KENT COUNTY LEVY COURT COMMISSIONER DIST 5': 'Levy Court',
            'kent county levy court commissioner district 1': 'Levy Court',
            'KENT COUNTY LEVY COURT COMMISSIONER DISTRICT 1': 'Levy Court',
            'kent county levy court district 1': 'Levy Court',
            'Kent County Levy Court District 1': 'Levy Court',
            'kent county levy court district 3': 'Levy Court',
            'Kent County Levy Court District 3': 'Levy Court',
            'kent county levy court district 5': 'Levy Court',
            'Kent County Levy Court District 5': 'Levy Court',
            
            # Other Delaware-specific mappings
            'auditor of accts': 'Auditor of Accounts',
            'AUDITOR OF ACCTS': 'Auditor of Accounts',
            'auditor of accounts': 'Auditor of Accounts',
            'Auditor of Accounts': 'Auditor of Accounts',
            'clk peace': 'Clerk of Peace',
            'CLK PEACE': 'Clerk of Peace',
            'clerk of peace': 'Clerk of Peace',
            'Clerk of Peace': 'Clerk of Peace',
            'clerk of the peace': 'Clerk of Peace',
            'Clerk of the Peace': 'Clerk of Peace',
            'kent county clerk of the peace': 'Clerk of Peace',
            'KENT COUNTY CLERK OF THE PEACE': 'Clerk of Peace',
            'kent county clerk of peace': 'Clerk of Peace',
            'Kent County Clerk of the Peace': 'Clerk of Peace',
            'new castle clerk of the peace': 'Clerk of Peace',
            'NEW CASTLE CLERK OF THE PEACE': 'Clerk of Peace',
            'new castle county clerk of the peace': 'Clerk of Peace',
            'New Castle County Clerk of the Peace': 'Clerk of Peace',
            'sussex county clerk of the peace': 'Clerk of Peace',
            'SUSSEX COUNTY CLERK OF THE PEACE': 'Clerk of Peace',
            'sussex county clerk of peace': 'Clerk of Peace',
            'Sussex County Clerk of the Peace': 'Clerk of Peace',
            'pres city cncl': 'City Council President',
            'PRES CITY CNCL': 'City Council President',
            'pres county cncl': 'County Council President',
            'PRES COUNTY CNCL': 'County Council President',
            'president city council': 'City Council President',
            'President City Council': 'City Council President',
            'president of city council': 'City Council President',
            'President of City Council': 'City Council President',
            'president of county council': 'County Council President',
            'President Of County Council': 'County Council President',
            'wilmington president of city council': 'City Council President',
            'WILMINGTON PRESIDENT OF CITY COUNCIL': 'City Council President',
            'city of wilmington president of city council': 'City Council President',
            'City of Wilmington President of City Council': 'City Council President',
            'rec deeds': 'Recorder of Deeds',
            'REC DEEDS': 'Recorder of Deeds',
            'rec of deeds': 'Recorder of Deeds',
            'REC OF DEEDS': 'Recorder of Deeds',
            'recorder of deeds': 'Recorder of Deeds',
            'Recorder of Deeds': 'Recorder of Deeds',
            'reg of wills': 'Register of Wills',
            'REG OF WILLS': 'Register of Wills',
            'reg wills': 'Register of Wills',
            'REG WILLS': 'Register of Wills',
            'register of wills': 'Register of Wills',
            'Register of Wills': 'Register of Wills',
            'kent county register of wills': 'Register of Wills',
            'KENT COUNTY REGISTER OF WILLS': 'Register of Wills',
            'kent county register of will': 'Register of Wills',
            'Kent County Register of Wills': 'Register of Wills',
            'insurance comm': 'Insurance Commissioner',
            'INSURANCE COMM': 'Insurance Commissioner',
            'insurance commissioner': 'Insurance Commissioner',
            'Insurance Commissioner': 'Insurance Commissioner',
            'state insurance commissioner': 'Insurance Commissioner',
            'State Insurance Commissioner': 'Insurance Commissioner',
            'city treasurer': 'City Treasurer',
            'CITY TREASURER': 'City Treasurer',
            'city of wilmington city treasurer': 'City Treasurer',
            'City of Wilmington City Treasurer': 'City Treasurer',
            'wilmington city treasurer': 'City Treasurer',
            'WILMINGTON CITY TREASURER': 'City Treasurer',
            'comptroller': 'Comptroller',
            'Comptroller': 'Comptroller',
            'state treasurer': 'State Treasurer',
            'STATE TREASURER': 'State Treasurer',
            'treasurer': 'State Treasurer',
            'Treasurer': 'State Treasurer',
            'kc sheriff': 'Sheriff',
            'KC Sheriff': 'Sheriff',
            'ncc sheriff': 'Sheriff',
            'NCC Sheriff': 'Sheriff',
            'sc sheriff': 'Sheriff',
            'SC SHERIFF': 'Sheriff',
            'sc sheriff': 'Sheriff',
            'SC Sheriff': 'Sheriff',
            'sheriff': 'Sheriff',
            'Sheriff': 'Sheriff',
            'lt governor': 'Lieutenant Governor',
            'LT GOVERNOR': 'Lieutenant Governor',
            'lt. governor': 'Lieutenant Governor',
            'LT. GOVERNOR': 'Lieutenant Governor',
            'lieutenant governor': 'Lieutenant Governor',
            'Lieutenant Governor': 'Lieutenant Governor',
            'lt. governor': 'Lieutenant Governor',
            'Lt. Governor': 'Lieutenant Governor',
            'presidentvice persident': 'US President',
            'PRESIDENTVICE PERSIDENT': 'US President',
            'presidentvice president': 'US President',
            'PRESIDENTVICE PRESIDENT': 'US President',
            'presidentvice president': 'US President',
            'PresidentVice President': 'US President',
            'u.s. vice president': 'US Vice President',
            'U.S. Vice President': 'US Vice President',
            'vice president': 'US Vice President',
            'VICE PRESIDENT': 'US Vice President',
            'city of wilmington - mayor': 'Mayor',
            'CITY OF WILMINGTON - MAYOR': 'Mayor',
            'city of wilmington mayor': 'Mayor',
            'City of Wilmington Mayor': 'Mayor',
            'mayor': 'Mayor',
            'Mayor': 'Mayor',
            
            # Add exact matches
            'State House': 'State House',
            
            # State Senate variations
            'state senator': 'State Senate',
            'state senate': 'State Senate',
            'senator': 'State Senate',
            'senate member': 'State Senate',
            'state senate member': 'State Senate',
            
            # Arizona-specific State Senate mappings
            'state senator - district no. 1': 'State Senate',
            'State Senator - District No. 1': 'State Senate',
            'state senator - district no. 2': 'State Senate',
            'State Senator - District No. 2': 'State Senate',
            'state senator - district no. 3': 'State Senate',
            'State Senator - District No. 3': 'State Senate',
            'state senator - district no. 4': 'State Senate',
            'State Senator - District No. 4': 'State Senate',
            'state senator - district no. 5': 'State Senate',
            'State Senator - District No. 5': 'State Senate',
            'state senator - district no. 6': 'State Senate',
            'State Senator - District No. 6': 'State Senate',
            'state senator - district no. 7': 'State Senate',
            'State Senator - District No. 7': 'State Senate',
            'state senator - district no. 8': 'State Senate',
            'State Senator - District No. 8': 'State Senate',
            'state senator - district no. 9': 'State Senate',
            'State Senator - District No. 9': 'State Senate',
            'state senator - district no. 10': 'State Senate',
            'State Senator - District No. 10': 'State Senate',
            'state senator - district no. 11': 'State Senate',
            'State Senator - District No. 11': 'State Senate',
            'state senator - district no. 12': 'State Senate',
            'State Senator - District No. 12': 'State Senate',
            'state senator - district no. 13': 'State Senate',
            'State Senator - District No. 13': 'State Senate',
            'state senator - district no. 14': 'State Senate',
            'State Senator - District No. 14': 'State Senate',
            'state senator - district no. 15': 'State Senate',
            'State Senator - District No. 15': 'State Senate',
            'state senator - district no. 16': 'State Senate',
            'State Senator - District No. 16': 'State Senate',
            'state senator - district no. 17': 'State Senate',
            'State Senator - District No. 17': 'State Senate',
            'state senator - district no. 18': 'State Senate',
            'State Senator - District No. 18': 'State Senate',
            'state senator - district no. 19': 'State Senate',
            'State Senator - District No. 19': 'State Senate',
            'state senator - district no. 20': 'State Senate',
            'State Senator - District No. 20': 'State Senate',
            'state senator - district no. 21': 'State Senate',
            'State Senator - District No. 21': 'State Senate',
            'state senator - district no. 22': 'State Senate',
            'State Senator - District No. 22': 'State Senate',
            'state senator - district no. 23': 'State Senate',
            'State Senator - District No. 23': 'State Senate',
            'state senator - district no. 24': 'State Senate',
            'State Senator - District No. 24': 'State Senate',
            'state senator - district no. 25': 'State Senate',
            'State Senator - District No. 25': 'State Senate',
            'state senator - district no. 26': 'State Senate',
            'State Senator - District No. 26': 'State Senate',
            'state senator - district no. 27': 'State Senate',
            'State Senator - District No. 27': 'State Senate',
            'state senator - district no. 28': 'State Senate',
            'State Senator - District No. 28': 'State Senate',
            'state senator - district no. 29': 'State Senate',
            'State Senator - District No. 29': 'State Senate',
            'state senator - district no. 30': 'State Senate',
            'State Senator - District No. 30': 'State Senate',
            
            # Colorado-specific State Senate mappings
            'state senator - district 1': 'State Senate',
            'State Senator - District 1': 'State Senate',
            'state senator - district 2': 'State Senate',
            'State Senator - District 2': 'State Senate',
            'state senator - district 3': 'State Senate',
            'State Senator - District 3': 'State Senate',
            'state senator - district 4': 'State Senate',
            'State Senator - District 4': 'State Senate',
            'state senator - district 5': 'State Senate',
            'State Senator - District 5': 'State Senate',
            'state senator - district 6': 'State Senate',
            'State Senator - District 6': 'State Senate',
            'state senator - district 7': 'State Senate',
            'State Senator - District 7': 'State Senate',
            'state senator - district 8': 'State Senate',
            'State Senator - District 8': 'State Senate',
            'state senator - district 9': 'State Senate',
            'State Senator - District 9': 'State Senate',
            'state senator - district 10': 'State Senate',
            'State Senator - District 10': 'State Senate',
            'state senator - district 11': 'State Senate',
            'State Senator - District 11': 'State Senate',
            'state senator - district 12': 'State Senate',
            'State Senator - District 12': 'State Senate',
            'state senator - district 13': 'State Senate',
            'State Senator - District 13': 'State Senate',
            'state senator - district 14': 'State Senate',
            'State Senator - District 14': 'State Senate',
            'state senator - district 15': 'State Senate',
            'State Senator - District 15': 'State Senate',
            'state senator - district 16': 'State Senate',
            'State Senator - District 16': 'State Senate',
            'state senator - district 17': 'State Senate',
            'State Senator - District 17': 'State Senate',
            'state senator - district 18': 'State Senate',
            'State Senator - District 18': 'State Senate',
            'state senator - district 19': 'State Senate',
            'State Senator - District 19': 'State Senate',
            'state senator - district 20': 'State Senate',
            'State Senator - District 20': 'State Senate',
            'state senator - district 21': 'State Senate',
            'State Senator - District 21': 'State Senate',
            'state senator - district 22': 'State Senate',
            'State Senator - District 22': 'State Senate',
            'state senator - district 23': 'State Senate',
            'State Senator - District 23': 'State Senate',
            'state senator - district 24': 'State Senate',
            'State Senator - District 24': 'State Senate',
            'state senator - district 25': 'State Senate',
            'State Senator - District 25': 'State Senate',
            'state senator - district 26': 'State Senate',
            'State Senator - District 26': 'State Senate',
            'state senator - district 27': 'State Senate',
            'State Senator - District 27': 'State Senate',
            'state senator - district 28': 'State Senate',
            'State Senator - District 28': 'State Senate',
            'state senator - district 29': 'State Senate',
            'State Senator - District 29': 'State Senate',
            'state senator - district 30': 'State Senate',
            'State Senator - District 30': 'State Senate',
            'state senator - district 31': 'State Senate',
            'State Senator - District 31': 'State Senate',
            'state senator - district 32': 'State Senate',
            'State Senator - District 32': 'State Senate',
            'state senator - district 33': 'State Senate',
            'State Senator - District 33': 'State Senate',
            'state senator - district 34': 'State Senate',
            'State Senator - District 34': 'State Senate',
            'state senator - district 35': 'State Senate',
            'State Senator - District 35': 'State Senate',
            
            # Add exact matches
            'State Senate': 'State Senate',
            
            # Governor variations
            # Governor variations (remove lieutenant governor)
            'governor': 'Governor',
            'guv': 'Governor',
            'governor / lieutenant governor': 'Governor',
            'GOVERNOR / LIEUTENANT GOVERNOR': 'Governor',
            
            # Court of Appeals variations (fix case)
            'court of appeals': 'Court of Appeals Judge',
            'court of appeals judge': 'Court of Appeals Judge',
            'COURT OF APPEALS': 'Court of Appeals Judge',
            'governor / lt. governor': 'Governor',
            'governor and lieutenant governor': 'Governor',
            
            # Add exact matches
            'Governor': 'Governor',
            
            # Lieutenant Governor variations
            'lieutenant governor': 'Lieutenant Governor',
            'lt. governor': 'Lieutenant Governor',
            'lt governor': 'Lieutenant Governor',
            
            # Add exact matches
            'Lieutenant Governor': 'Lieutenant Governor',
            
            # State Attorney General variations
            'attorney general': 'State Attorney General',
            'nc attorney general': 'State Attorney General',
            'attorney general - statewide': 'State Attorney General',
            'solicitor general - state court': 'State Attorney General',
            
            # Add exact matches
            'State Attorney General': 'State Attorney General',
            
            # State Treasurer variations
            'state treasurer': 'State Treasurer',
            'treasurer': 'State Treasurer',
            'state treasurer - statewide': 'State Treasurer',
            
            # Add exact matches
            'State Treasurer': 'State Treasurer',
            
            # Secretary of State variations
            'secretary of state': 'Secretary of State',
            'state secretary': 'Secretary of State',
            
            # Add exact matches
            'Secretary of State': 'Secretary of State',
            
            # City Council variations
            'city council member': 'City Council',
            'city council': 'City Council',
            'alderman': 'City Council',
            'member town council': 'City Council',
            'member city council': 'City Council',
            'council member': 'City Council',
            'city councilman': 'City Council',
            'city councilwoman': 'City Council',
            
            # Town Council variations
            'town council': 'Town Council',
            'town council member': 'Town Council',
            'member town council': 'Town Council',
            'town of chapel hill town council': 'Town Council',
            'town of indian trail council': 'Town Council',
            
            # Add the exact matches that were missing
            'City Council': 'City Council',
            'County Commission': 'County Commission',
            'State House': 'State House',
            'City Commission': 'City Commission',
            'School Board': 'School Board',
            'Sheriff': 'Sheriff',
            
            # City Commission variations
            'city commission': 'City Commission',
            'city commissioner': 'City Commission',
            'member city commission': 'City Commission',
            
            # County Commission variations
            'county commission': 'County Commission',
            'county commissioner': 'County Commissioner',  # Changed from 'County Commission' to preserve 'commissioner'
            'member county commission': 'County Commission',
            'county board': 'County Commission',
            'county board member': 'County Commission',
            
            # Add exact matches
            'County Commission': 'County Commission',
            
            # School Board variations
            'school board': 'School Board',
            'school board member': 'School Board',
            'board of education': 'School Board',
            'board member': 'School Board',
            'ind school board member': 'School Board',
            'university board of regents': 'School Board',
            
            # Judicial Office variations (enhanced)
            'justice of the peace': 'Justice of the Peace',
            'justice of peace': 'Justice of the Peace',
            'jop': 'Justice of the Peace',
            
            # National Convention Delegate (preserve original)
            'national convention delegate': 'National Convention Delegate',
            
            # Community Development District (preserve original)
            'community development district': 'Community Development District',
            'judge of the court of common pleas': 'Judge of the Court of Common Pleas',
            'judge of the orphans court': 'Judge of the Orphans Court',
            'judge of the orphans\' court': 'Judge of the Orphans Court',
            'judge of the municipal court': 'Judge of the Municipal Court',
            'judge of the circuit court': 'Circuit Judge',
            'circuit judge': 'Circuit Judge',
            'district court judge': 'District Judge',
            'district judge': 'District Judge',
            'district magistrate judge': 'District Magistrate Judge',
            'magistrate': 'Magistrate',
            'judge': 'Judge',
            
            # Add exact matches
            'Justice of the Peace': 'Justice of the Peace',
            'Judge of the Court of Common Pleas': 'Judge of the Court of Common Pleas',
            'Judge of the Orphans Court': 'Judge of the Orphans Court',
            'Judge of the Municipal Court': 'Judge of the Municipal Court',
            'Circuit Judge': 'Circuit Judge',
            'District Judge': 'District Judge',
            'District Magistrate Judge': 'District Magistrate Judge',
            'Magistrate': 'Magistrate',
            'Judge': 'Judge',
            
            # County Office variations (enhanced)
            'constable': 'Constable',
            'county judge executive': 'County Judge Executive',
            'county judge': 'County Judge',
            'county clerk': 'County Clerk',
            'county attorney': 'County Attorney',
            'coroner': 'Coroner',
            'surveyor': 'Surveyor',
            'jailer': 'Jailer',
            
            # Add exact matches
            'Constable': 'Constable',
            'County Judge Executive': 'County Judge Executive',
            'County Judge': 'County Judge',
            'County Clerk': 'County Clerk',
            'County Attorney': 'County Attorney',
            'Coroner': 'Coroner',
            'Surveyor': 'Surveyor',
            'Jailer': 'Jailer',
            
            # Special District variations (enhanced)
            'soil conservation officer': 'Soil Conservation Officer',
            'soil and water conservation district supervisor': 'Soil Conservation Officer',
            'soil and water conservation director': 'Soil Conservation Officer',
            'soil and water district commission': 'Special District Commission',
            'property valuation administrator': 'Property Valuation Administrator',
            'levee and sanitary district': 'Special District',
            'levee district': 'Special District',
            'sanitary district': 'Special District',
            
            # Add exact matches
            'Soil Conservation Officer': 'Soil Conservation Officer',
            'Property Valuation Administrator': 'Property Valuation Administrator',
            'Special District Commission': 'Special District Commission',
            'Special District': 'Special District',
            
            # Mayor variations
            'mayor': 'Mayor',
            'city mayor': 'Mayor',
            'town mayor': 'Mayor',
            
            # Add exact matches
            'Mayor': 'Mayor',
            
            # New office categories identified in Phase 2.5
            'district attorney': 'District Attorney',
            'delegate to republican national convention': 'National Convention Delegate',
            'delegate to democratic national convention': 'National Convention Delegate',
            'delegate to national convention': 'National Convention Delegate',
            
            # Hawaii-specific office mappings
            'state representative': 'State House',
            'state senator': 'State Senate',
            'maui councilmember': 'County Council',
            'hawaii councilmember': 'County Council',
            'honolulu councilmember': 'County Council',
            'kauai councilmember': 'County Council',
            'oha at-large trustee': 'Special District',
            'oha kauai resident trustee': 'Special District',
            'oha hawaii resident trustee': 'Special District',
            'oha molokai resident trustee': 'Special District',
            'honolulu prosecuting attorney': 'District Attorney',
            'hawaii prosecuting attorney': 'District Attorney',
            'kauai prosecuting attorney': 'District Attorney',
            'hawaii mayor': 'Mayor',
            'honolulu mayor': 'Mayor',
            
            # Additional office mappings from state cleaners
            'administrator': 'Administrator',
            'alderman': 'Alderman',
            'analyst': 'Analyst',
            # Assembly variations (preserve original if unclear mapping)
            'assembly': 'Assembly',
            'assembly member': 'Assembly Member',
            'borough assembly member': 'Borough Assembly Member',
            'auditor general': 'Auditor General',
            'auditor of public accounts': 'Auditor of Public Accounts',
            'borough assembly member': 'Borough Assembly Member',
            'borough mayor': 'Borough Mayor',
            'chair': 'Chair',
            'co-chair': 'Co-Chair',
            'commissioner of agriculture': 'Commissioner of Agriculture',
            'commissioner of agriculture and forestry': 'Commissioner of Agriculture and Forestry',
            'commissioner of insurance': 'Commissioner of Insurance',
            'commissioner of labor': 'Commissioner of Labor',
            'commissioner of public lands': 'Commissioner of Public Lands',
            'commissioner of school and public lands': 'Commissioner of School and Public Lands',
            'commonwealth\'s attorney': 'Commonwealth\'s Attorney',
            'comptroller': 'Comptroller',
            'comptroller general': 'Comptroller General',
            'coordinator': 'Coordinator',
            'corporation commissioner': 'Corporation Commissioner',
            'council member': 'Council Member',
            'councilor': 'Councilor',
            'county assessor': 'County Assessor',
            'county attorney': 'County Attorney',
            'county auditor': 'County Auditor',
            'county board member': 'County Board Member',
            'county board president': 'County Board President',
            'county board secretary': 'County Board Secretary',
            'county board treasurer': 'County Board Treasurer',
            'county board vice president': 'County Board Vice President',
            'county clerk': 'County Clerk',
            'county clerk and recorder': 'County Clerk and Recorder',
            'county clerk of the peace': 'County Clerk of the Peace',
            'county collector': 'County Collector',
            'county coroner': 'County Coroner',
            'county council member': 'County Council Member',
            'county council president': 'County Council President',
            'county council secretary': 'County Council Secretary',
            'county council vice president': 'County Council Vice President',
            'county court judge': 'County Court Judge',
            'county district attorney': 'County District Attorney',
            'county engineer': 'County Engineer',
            'county executive': 'County Executive',
            'county judge': 'County Judge',
            'county legislator': 'County Legislator',
            'county levy court member': 'County Levy Court Member',
            'county levy court commissioner': 'County Levy Court Commissioner',  # Added to preserve 'commissioner'
            'county magistrate': 'County Magistrate',
            'county prosecuting attorney': 'County Prosecuting Attorney',
            'county prosecutor': 'County Prosecutor',
            'county prothonotary': 'County Prothonotary',
            'county public administrator': 'County Public Administrator',
            'county public defender': 'County Public Defender',
            'county recorder of deeds': 'County Recorder of Deeds',
            'county register of wills': 'County Register of Wills',
            'county sheriff': 'County Sheriff',
            'county solicitor': 'County Solicitor',
            'county state\'s attorney': 'County State\'s Attorney',
            'county supervisor': 'County Supervisor',
            'county surveyor': 'County Surveyor',
            'county treasurer': 'County Treasurer',
            'county trustee': 'County Trustee',
            'court of appeals judge': 'Court of Appeals Judge',
            'delegate': 'Delegate',
            'director': 'Director',
            'executive': 'Executive',
            'fire district board member': 'Fire District Board Member',
            'fire district commissioner': 'Fire District Commissioner',
            'fire district trustee': 'Fire District Trustee',
            'hospital district board member': 'Hospital District Board Member',
            'hospital district commissioner': 'Hospital District Commissioner',
            'insurance commissioner': 'Insurance Commissioner',
            'labor commissioner': 'Labor Commissioner',
            'levee district board member': 'Levee District Board Member',
            'library board member': 'Library Board Member',
            'library district trustee': 'Library District Trustee',
            'manager': 'Manager',
            'member': 'Member',
            'metro councilor': 'Metro Councilor',
            'natural resources district board member': 'Natural Resources District Board Member',
            'parish assessor': 'Parish Assessor',
            'parish auditor': 'Parish Auditor',
            'parish clerk of court': 'Parish Clerk of Court',
            'parish commissioner': 'Parish Commissioner',
            'parish coroner': 'Parish Coroner',
            'parish district attorney': 'Parish District Attorney',
            'parish engineer': 'Parish Engineer',
            'parish executive': 'Parish Executive',
            'parish judge': 'Parish Judge',
            'parish legislator': 'Parish Legislator',
            'parish magistrate': 'Parish Magistrate',
            'parish prosecutor': 'Parish Prosecutor',
            'parish public defender': 'Parish Public Defender',
            'parish sheriff': 'Parish Sheriff',
            'parish supervisor': 'Parish Supervisor',
            'parish surveyor': 'Parish Surveyor',
            'parish treasurer': 'Parish Treasurer',
            'parish trustee': 'Parish Trustee',
            'park board member': 'Park Board Member',
            'park district commissioner': 'Park District Commissioner',
            'port authority board member': 'Port Authority Board Member',
            'port commissioner': 'Port Commissioner',
            'president': 'President',
            'public service commissioner': 'Public Service Commissioner',
            'public utility district board member': 'Public Utility District Board Member',
            'public utility district commissioner': 'Public Utility District Commissioner',
            'regent of the university of alaska': 'University Regent',
            'regent of the university of arkansas': 'University Regent',
            'regent of the university of colorado': 'University Regent',
            'regent of the university of georgia': 'University Regent',
            'regent of the university of idaho': 'University Regent',
            'regent of the university of illinois': 'University Regent',
            'regent of the university of indiana': 'University Regent',
            'regent of the university of iowa': 'University Regent',
            'regent of the university of kansas': 'University Regent',
            'regent of the university of kentucky': 'University Regent',
            'regent of the university of louisiana': 'University Regent',
            'regent of the university of maryland': 'University Regent',
            'regent of the university of missouri': 'University Regent',
            'regent of the university of montana': 'University Regent',
            'regent of the university of nebraska': 'University Regent',
            'regent of the university of new mexico': 'University Regent',
            'regent of the university of north carolina': 'University Regent',
            'regent of the university of oklahoma': 'University Regent',
            'regent of the university of oregon': 'University Regent',
            'regent of the university of pennsylvania': 'University Regent',
            'regent of the university of south carolina': 'University Regent',
            'regent of the university of south dakota': 'University Regent',
            'regent of the university of the state of new york': 'University Regent',
            'regent of the university of vermont': 'University Regent',
            'regent of the university of virginia': 'University Regent',
            'regent of the university of west virginia': 'University Regent',
            'regent of the university of wisconsin': 'University Regent',
            'regent of the university of wyoming': 'University Regent',
            'representative': 'Representative',
            'sanitary district trustee': 'Sanitary District Trustee',
            'secretary': 'Secretary',
            'secretary of agriculture': 'Secretary of Agriculture',
            'senator': 'Senator',
            'sewer district board member': 'Sewer District Board Member',
            'sewer district commissioner': 'Sewer District Commissioner',
            'soil and water conservation district supervisor': 'Soil and Water Conservation District Supervisor',
            'special district board member': 'Special District Board Member',
            'specialist': 'Specialist',
            'state assembly member': 'State Assembly Member',
            'state comptroller': 'State Comptroller',
            'state delegate': 'State Delegate',
            'state legislator': 'State Legislator',
            'state mine inspector': 'State Mine Inspector',
            'superintendent': 'Superintendent',
            'superintendent of education': 'Superintendent of Education',
            'superintendent of public instruction': 'Superintendent of Public Instruction',
            'superintendent of schools': 'Superintendent of Schools',
            'superior court judge': 'Superior Court Judge',
            'town assessor': 'Town Assessor',
            'town clerk': 'Town Clerk',
            'town commissioner': 'Town Commissioner',
            'town council member': 'Town Council Member',
            'town highway superintendent': 'Town Highway Superintendent',
            'town supervisor': 'Town Supervisor',
            'town treasurer': 'Town Treasurer',
            'township assessor': 'Township Assessor',
            'township clerk': 'Township Clerk',
            'township highway commissioner': 'Township Highway Commissioner',
            'township supervisor': 'Township Supervisor',
            'township treasurer': 'Township Treasurer',
            'trustee': 'Trustee',
            'u.s. representative': 'US Representative',
            'u.s. senator': 'US Senator',
            'vice president': 'Vice President',
            'village clerk': 'Village Clerk',
            'village mayor': 'Village Mayor',
            'village trustee': 'Village Trustee',
            'water district board member': 'Water District Board Member',
            'water district commissioner': 'Water District Commissioner',
            'water district trustee': 'Water District Trustee',
            'watershed council member': 'Watershed Council Member',
            'wilmington city council': 'City Council Member',
            'wilmington city council at large': 'City Council Member At-Large',
            'selectman': 'Selectman',
            
            # Alaska-specific mappings (case-insensitive)
            'house district': 'State House',
            'HOUSE DISTRICT': 'State House',
            'House District': 'State House',
            'house district 01': 'State House',
            'HOUSE DISTRICT 01': 'State House',
            'House District 01': 'State House',
            'house district 02': 'State House',
            'HOUSE DISTRICT 02': 'State House',
            'House District 02': 'State House',
            'house district 03': 'State House',
            'HOUSE DISTRICT 03': 'State House',
            'House District 03': 'State House',
            'house district 04': 'State House',
            'HOUSE DISTRICT 04': 'State House',
            'House District 04': 'State House',
            'house district 05': 'State House',
            'HOUSE DISTRICT 05': 'State House',
            'House District 05': 'State House',
            'house district 06': 'State House',
            'HOUSE DISTRICT 06': 'State House',
            'House District 06': 'State House',
            'house district 07': 'State House',
            'HOUSE DISTRICT 07': 'State House',
            'House District 07': 'State House',
            'house district 08': 'State House',
            'HOUSE DISTRICT 08': 'State House',
            'House District 08': 'State House',
            'house district 09': 'State House',
            'HOUSE DISTRICT 09': 'State House',
            'House District 09': 'State House',
            'house district 10': 'State House',
            'HOUSE DISTRICT 10': 'State House',
            'House District 10': 'State House',
            'house district 11': 'State House',
            'HOUSE DISTRICT 11': 'State House',
            'House District 11': 'State House',
            'house district 12': 'State House',
            'HOUSE DISTRICT 12': 'State House',
            'House District 12': 'State House',
            'house district 13': 'State House',
            'HOUSE DISTRICT 13': 'State House',
            'House District 13': 'State House',
            'house district 14': 'State House',
            'HOUSE DISTRICT 14': 'State House',
            'House District 14': 'State House',
            'house district 15': 'State House',
            'HOUSE DISTRICT 15': 'State House',
            'House District 15': 'State House',
            'house district 16': 'State House',
            'HOUSE DISTRICT 16': 'State House',
            'House District 16': 'State House',
            'house district 17': 'State House',
            'HOUSE DISTRICT 17': 'State House',
            'House District 17': 'State House',
            'house district 18': 'State House',
            'HOUSE DISTRICT 18': 'State House',
            'House District 18': 'State House',
            'house district 19': 'State House',
            'HOUSE DISTRICT 19': 'State House',
            'House District 19': 'State House',
            'house district 20': 'State House',
            'HOUSE DISTRICT 20': 'State House',
            'House District 20': 'State House',
            'house district 21': 'State House',
            'HOUSE DISTRICT 21': 'State House',
            'House District 21': 'State House',
            'house district 22': 'State House',
            'HOUSE DISTRICT 22': 'State House',
            'House District 22': 'State House',
            'house district 23': 'State House',
            'HOUSE DISTRICT 23': 'State House',
            'House District 23': 'State House',
            'house district 24': 'State House',
            'HOUSE DISTRICT 24': 'State House',
            'House District 24': 'State House',
            'house district 25': 'State House',
            'HOUSE DISTRICT 25': 'State House',
            'House District 25': 'State House',
            'house district 26': 'State House',
            'HOUSE DISTRICT 26': 'State House',
            'House District 26': 'State House',
            'house district 27': 'State House',
            'HOUSE DISTRICT 27': 'State House',
            'House District 27': 'State House',
            'house district 28': 'State House',
            'HOUSE DISTRICT 28': 'State House',
            'House District 28': 'State House',
            'house district 29': 'State House',
            'HOUSE DISTRICT 29': 'State House',
            'House District 29': 'State House',
            'house district 30': 'State House',
            'HOUSE DISTRICT 30': 'State House',
            'House District 30': 'State House',
            'house district 31': 'State House',
            'HOUSE DISTRICT 31': 'State House',
            'House District 31': 'State House',
            'house district 32': 'State House',
            'HOUSE DISTRICT 32': 'State House',
            'House District 32': 'State House',
            'house district 33': 'State House',
            'HOUSE DISTRICT 33': 'State House',
            'House District 33': 'State House',
            'house district 34': 'State House',
            'HOUSE DISTRICT 34': 'State House',
            'House District 34': 'State House',
            'house district 35': 'State House',
            'HOUSE DISTRICT 35': 'State House',
            'House District 35': 'State House',
            'house district 36': 'State House',
            'HOUSE DISTRICT 36': 'State House',
            'House District 36': 'State House',
            'house district 37': 'State House',
            'HOUSE DISTRICT 37': 'State House',
            'House District 37': 'State House',
            'house district 38': 'State House',
            'HOUSE DISTRICT 38': 'State House',
            'House District 38': 'State House',
            'house district 39': 'State House',
            'HOUSE DISTRICT 39': 'State House',
            'House District 39': 'State House',
            'house district 40': 'State House',
            'HOUSE DISTRICT 40': 'State House',
            'House District 40': 'State House',
            
            # Alaska-specific State Senate mappings
            'senate district a': 'State Senate',
            'senate district b': 'State Senate',
            'senate district c': 'State Senate',
            'senate district d': 'State Senate',
            'senate district e': 'State Senate',
            'senate district f': 'State Senate',
            'senate district g': 'State Senate',
            'senate district h': 'State Senate',
            'senate district i': 'State Senate',
            'senate district j': 'State Senate',
            'senate district k': 'State Senate',
            'senate district l': 'State Senate',
            'senate district m': 'State Senate',
            'senate district n': 'State Senate',
            'senate district o': 'State Senate',
            'senate district p': 'State Senate',
            'senate district q': 'State Senate',
            'senate district r': 'State Senate',
            'senate district s': 'State Senate',
            'senate district t': 'State Senate',
            # Uppercase variations
            'Senate District A': 'State Senate',
            'Senate District B': 'State Senate',
            'Senate District C': 'State Senate',
            'Senate District D': 'State Senate',
            'Senate District E': 'State Senate',
            'Senate District F': 'State Senate',
            'Senate District G': 'State Senate',
            'Senate District H': 'State Senate',
            'Senate District I': 'State Senate',
            'Senate District J': 'State Senate',
            'Senate District K': 'State Senate',
            'Senate District L': 'State Senate',
            'Senate District M': 'State Senate',
            'Senate District N': 'State Senate',
            'Senate District O': 'State Senate',
            'Senate District P': 'State Senate',
            'Senate District Q': 'State Senate',
            'Senate District R': 'State Senate',
            'Senate District S': 'State Senate',
            'Senate District T': 'State Senate',
            
            # Illinois-specific Congress mappings
            '1st congress': 'US House',
            '2nd congress': 'US House',
            '3rd congress': 'US House',
            '4th congress': 'US House',
            '5th congress': 'US House',
            '6th congress': 'US House',
            '7th congress': 'US House',
            '8th congress': 'US House',
            '9th congress': 'US House',
            '10th congress': 'US House',
            '11th congress': 'US House',
            '12th congress': 'US House',
            '13th congress': 'US House',
            '14th congress': 'US House',
            '15th congress': 'US House',
            '16th congress': 'US House',
            '17th congress': 'US House',
            '18th congress': 'US House',
            '19th congress': 'US House',
            '20th congress': 'US House',
            # Uppercase variations
            '1st Congress': 'US House',
            '2nd Congress': 'US House',
            '3rd Congress': 'US House',
            '4th Congress': 'US House',
            '5th Congress': 'US House',
            '6th Congress': 'US House',
            '7th Congress': 'US House',
            '8th Congress': 'US House',
            '9th Congress': 'US House',
            '10th Congress': 'US House',
            '11th Congress': 'US House',
            '12th Congress': 'US House',
            '13th Congress': 'US House',
            '14th Congress': 'US House',
            '15th Congress': 'US House',
            '16th Congress': 'US House',
            '17th Congress': 'US House',
            '18th Congress': 'US House',
            '19th Congress': 'US House',
            '20th Congress': 'US House',
            
            # Illinois-specific State Senate mappings
            '1st senate': 'State Senate',
            '2nd senate': 'State Senate',
            '3rd senate': 'State Senate',
            '4th senate': 'State Senate',
            '5th senate': 'State Senate',
            '6th senate': 'State Senate',
            '7th senate': 'State Senate',
            '8th senate': 'State Senate',
            '9th senate': 'State Senate',
            '10th senate': 'State Senate',
            '11th senate': 'State Senate',
            '12th senate': 'State Senate',
            '13th senate': 'State Senate',
            '14th senate': 'State Senate',
            '15th senate': 'State Senate',
            '16th senate': 'State Senate',
            '17th senate': 'State Senate',
            '18th senate': 'State Senate',
            '19th senate': 'State Senate',
            '20th senate': 'State Senate',
            '21st senate': 'State Senate',
            '22nd senate': 'State Senate',
            '23rd senate': 'State Senate',
            '24th senate': 'State Senate',
            '25th senate': 'State Senate',
            '26th senate': 'State Senate',
            '27th senate': 'State Senate',
            '28th senate': 'State Senate',
            '29th senate': 'State Senate',
            '30th senate': 'State Senate',
            '31st senate': 'State Senate',
            '32nd senate': 'State Senate',
            '33rd senate': 'State Senate',
            '34th senate': 'State Senate',
            '35th senate': 'State Senate',
            '36th senate': 'State Senate',
            '37th senate': 'State Senate',
            '38th senate': 'State Senate',
            '39th senate': 'State Senate',
            '40th senate': 'State Senate',
            '41st senate': 'State Senate',
            '42nd senate': 'State Senate',
            '43rd senate': 'State Senate',
            '44th senate': 'State Senate',
            '45th senate': 'State Senate',
            '46th senate': 'State Senate',
            '47th senate': 'State Senate',
            '48th senate': 'State Senate',
            '49th senate': 'State Senate',
            '50th senate': 'State Senate',
            '51st senate': 'State Senate',
            '52nd senate': 'State Senate',
            '53rd senate': 'State Senate',
            '54th senate': 'State Senate',
            '55th senate': 'State Senate',
            '56th senate': 'State Senate',
            '57th senate': 'State Senate',
            '58th senate': 'State Senate',
            '59th senate': 'State Senate',
            '60th senate': 'State Senate',
            # Uppercase variations
            '1st Senate': 'State Senate',
            '2nd Senate': 'State Senate',
            '3rd Senate': 'State Senate',
            '4th Senate': 'State Senate',
            '5th Senate': 'State Senate',
            '6th Senate': 'State Senate',
            '7th Senate': 'State Senate',
            '8th Senate': 'State Senate',
            '9th Senate': 'State Senate',
            '10th Senate': 'State Senate',
            '11th Senate': 'State Senate',
            '12th Senate': 'State Senate',
            '13th Senate': 'State Senate',
            '14th Senate': 'State Senate',
            '15th Senate': 'State Senate',
            '16th Senate': 'State Senate',
            '17th Senate': 'State Senate',
            '18th Senate': 'State Senate',
            '19th Senate': 'State Senate',
            '20th Senate': 'State Senate',
            '21st Senate': 'State Senate',
            '22nd Senate': 'State Senate',
            '23rd Senate': 'State Senate',
            '24th Senate': 'State Senate',
            '25th Senate': 'State Senate',
            '26th Senate': 'State Senate',
            '27th Senate': 'State Senate',
            '28th Senate': 'State Senate',
            '29th Senate': 'State Senate',
            '30th Senate': 'State Senate',
            '31st Senate': 'State Senate',
            '32nd Senate': 'State Senate',
            '33rd Senate': 'State Senate',
            '34th Senate': 'State Senate',
            '35th Senate': 'State Senate',
            '36th Senate': 'State Senate',
            '37th Senate': 'State Senate',
            '38th Senate': 'State Senate',
            '39th Senate': 'State Senate',
            '40th Senate': 'State Senate',
            '41st Senate': 'State Senate',
            '42nd Senate': 'State Senate',
            '43rd Senate': 'State Senate',
            '44th Senate': 'State Senate',
            '45th Senate': 'State Senate',
            '46th Senate': 'State Senate',
            '47th Senate': 'State Senate',
            '48th Senate': 'State Senate',
            '49th Senate': 'State Senate',
            '50th Senate': 'State Senate',
            '51st Senate': 'State Senate',
            '52nd Senate': 'State Senate',
            '53rd Senate': 'State Senate',
            '54th Senate': 'State Senate',
            '55th Senate': 'State Senate',
            '56th Senate': 'State Senate',
            '57th Senate': 'State Senate',
            '58th Senate': 'State Senate',
            '59th Senate': 'State Senate',
            '60th Senate': 'State Senate',
            
            # Kansas-specific Senate mapping
            'kansas senate': 'State Senate',
            'Kansas Senate': 'State Senate',
            'house district 32': 'State House',
            'HOUSE DISTRICT 32': 'State House',
            'House District 32': 'State House',
            'house district 33': 'State House',
            'HOUSE DISTRICT 33': 'State House',
            'House District 33': 'State House',
            'house district 34': 'State House',
            'HOUSE DISTRICT 34': 'State House',
            'House District 34': 'State House',
            'house district 35': 'State House',
            'HOUSE DISTRICT 35': 'State House',
            'House District 35': 'State House',
            'house district 36': 'State House',
            'HOUSE DISTRICT 36': 'State House',
            'House District 36': 'State House',
            'house district 37': 'State House',
            'HOUSE DISTRICT 37': 'State House',
            'House District 37': 'State House',
            'house district 38': 'State House',
            'HOUSE DISTRICT 38': 'State House',
            'House District 38': 'State House',
            'house district 39': 'State House',
            'HOUSE DISTRICT 39': 'State House',
            'House District 39': 'State House',
            'house district 40': 'State House',
            'HOUSE DISTRICT 40': 'State House',
            'House District 40': 'State House',
        }
        
        return mappings
    
    def _build_district_patterns(self) -> List[str]:
        """
        Build regex patterns for district number removal.
        
        Returns:
            List of regex patterns for district identification
        """
        patterns = [
            # District number patterns
            r'\s*-\s*\d+[a-z]*\s*district\s*$',  # " - 3rd District"
            r'\s*district\s*\d+[a-z]*\s*$',      # "District 3"
            r'\s*,\s*district\s*\d+[a-z]*\s*$',  # ", District 3"
            r'\s*\d+[a-z]*\s*district\s*$',      # "3rd District"
            
            # Hawaii-specific patterns
            r',\s*dist\s+\d+\s*$',               # ", DIST 11"
            r',\s*dist\s+[ivxlcdm]+\s*$',        # ", DIST VII" (Roman numerals)
            r'\s*dist\s+\d+\s*$',                # "DIST 11"
            r'\s*dist\s+[ivxlcdm]+\s*$',          # "DIST VII" (Roman numerals)
            
            # Parentheses patterns (enhanced)
            r'\s*\(\d+[a-z]*\)\s*$',             # "(3rd)"
            r'\s*\(\d+\)\s*$',                    # "(4)"
            r'\s*\(\d+[a-z]*\s*district\)\s*$',  # "(3rd District)"
            
            # Position and division patterns
            r'\s*position\s*\d+\s*$',            # "Position 12"
            r'\s*division\s*[a-z]-\d+\s*$',     # "Division A-3"
            r'\s*division\s*\d+\s*$',            # "Division 3"
            
            # Seat and place patterns
            r'\s*seat\s*\d+\s*$',                # "Seat 2"
            r'\s*place\s*\d+\s*$',               # "Place 1"
            r'\s*ward\s*\d+\s*$',                # "Ward 3"
            
            # County-specific patterns
            r'\s*,\s*[a-z\s]+county\s*$',       # ", Harney County"
            r'\s*\([a-z\s]+county\)\s*$',        # "(Harney County)"
            
            # Enhanced district patterns for Phase 2.5
            r'\s*,\s*district\s*\d+\s*$',       # ", District 3"
            r'\s*district\s*\d+\s*$',            # "District 3"
            r'\s*\([a-z]\d+\)\s*$',              # "(a2)", "(j2)"
            r'\s*\(district:\s*\d+\)\s*$',       # "(District: 1)"
            r'\s*\(dist:\s*\d+\)\s*$',           # "(Dist: 1)"
            r',\s*\d+(?:st|nd|rd|th)?\s+district\s*$',  # ", 1st District"
        ]
        return patterns
    
    def _clean_office_name(self, office: str) -> str:
        """
        Clean office name by removing district numbers and basic cleaning.
        
        Args:
            office: Original office name
            
        Returns:
            Cleaned office name
        """
        if not office or pd.isna(office):
            return office
        
        office_str = str(office).strip()
        
        # Remove district patterns
        for pattern in self.district_patterns:
            office_str = re.sub(pattern, '', office_str, flags=re.IGNORECASE)
        
        # Remove party indicators in parentheses
        office_str = re.sub(r'\s*\([rd]\)\s*$', '', office_str, flags=re.IGNORECASE)
        office_str = re.sub(r'\s*for\s+', '', office_str, flags=re.IGNORECASE)
        
        # Remove county prefixes (enhanced)
        office_str = re.sub(r'^[a-z\s]+county\s+', '', office_str, flags=re.IGNORECASE)
        office_str = re.sub(r'^county\s+', '', office_str, flags=re.IGNORECASE)
        
        # Remove city prefixes
        office_str = re.sub(r'^city\s+of\s+', '', office_str, flags=re.IGNORECASE)
        office_str = re.sub(r'^town\s+of\s+', '', office_str, flags=re.IGNORECASE)
        
        # Clean up extra whitespace and trailing commas
        office_str = re.sub(r'\s+', ' ', office_str).strip()
        office_str = re.sub(r',\s*$', '', office_str)  # Remove trailing comma
        
        return office_str
    
    def _find_best_match(self, office: str) -> Optional[str]:
        """
        Find the best matching standardized office name.
        
        Args:
            office: Original office name
            
        Returns:
            Standardized office name or None if no match found
        """
        if not office or pd.isna(office):
            return None
        
        office_str = str(office).strip()
        
        # Try exact match first (case-insensitive)
        office_lower = office_str.lower()
        for source, target in self.office_mappings.items():
            if office_lower == source.lower():
                return target
        
        # Try exact match with original case
        if office_str in self.office_mappings:
            return self.office_mappings[office_str]
        
        # Try exact word matches (more precise, case-insensitive)
        office_words = set(office_lower.split())
        
        for source, target in self.office_mappings.items():
            source_words = set(source.lower().split())
            
            # Only match if there's significant overlap (at least 2 words)
            # AND the source is not significantly longer than the office
            if (len(office_words.intersection(source_words)) >= 2 and 
                len(source_words) <= len(office_words) + 1):
                
                # Additional safety checks for specific office types
                if self._is_safe_match(office_str, source, target):
                    return target
        
        return None
    
    def _is_safe_match(self, office: str, source: str, target: str) -> bool:
        """
        Check if a match is safe (prevents incorrect mappings).
        
        Args:
            office: Original office name
            source: Source pattern from mappings
            target: Target standardized name
            
        Returns:
            True if the match is safe
        """
        office_lower = office.lower()
        source_lower = source.lower()
        
        # Prevent judicial offices from being mapped to executive offices
        judicial_keywords = ['judge', 'justice', 'court', 'magistrate', 'orphan']
        executive_keywords = ['president', 'governor', 'mayor']
        
        if any(keyword in office_lower for keyword in judicial_keywords):
            if any(keyword in target.lower() for keyword in executive_keywords):
                return False
        
        # Prevent state offices from being mapped to federal offices
        state_keywords = ['state', 'county', 'city', 'local']
        federal_keywords = ['us ', 'united states', 'federal']
        
        if any(keyword in office_lower for keyword in state_keywords):
            if any(keyword in target.lower() for keyword in federal_keywords):
                return False
        
        # Additional safety checks
        if 'justice of the peace' in office_lower and 'president' in target.lower():
            return False
        
        if 'judge' in office_lower and 'president' in target.lower():
            return False
        
        return True
    
    def _extract_district_from_office(self, office: str) -> Optional[str]:
        """
        Extract district number from office name.
        
        Args:
            office: Office name that may contain district information
            
        Returns:
            District number as string, or None if no district found
        """
        if not office or pd.isna(office):
            return None
        
        office_str = str(office).strip()
        
        # Patterns to extract district numbers
        district_patterns = [
            r'(\d+)(?:st|nd|rd|th)?\s+(?:district|dist\.?)',  # "1st District", "2nd Dist"
            r'(?:district|dist\.?)\s*(\d+)',  # "District 1", "Dist 2"
            r'\(district:\s*(\d+)\)',  # "(District: 1)"
            r'\(dist:\s*(\d+)\)',  # "(Dist: 1)"
            r'(?:district|dist\.?)\s+no\.?\s*(\d+)',  # "District No. 1", "Dist No. 2"
            r'(\d+)(?:st|nd|rd|th)?\s+(?:congressional\s+)?district',  # "1st Congressional District"
            r'(?:congressional\s+)?district\s*(\d+)',  # "Congressional District 1"
            
            # Delaware-specific district patterns
            r'(\d+)(?:st|nd|rd|th)?\s+lc\s+dist',  # "1st LC Dist", "2ND LC DIST"
            r'(\d+)(?:st|nd|rd|th)?\s+cncl\s+dis',  # "1st CNCL DIS", "2ND CNCL DIS"
            r'cncl\s+dis\s*(\d+)',  # "CNCL DIS 1", "CNCL DIS 2"
            r'(\d+)(?:st|nd|rd|th)?\s+sen\s+dis',  # "1ST SEN DIS", "2ND SEN DIS"
            r'sen\s+dis\s*(\d+)',  # "SEN DIS 1", "SEN DIS 2"
            r'(\d+)(?:st|nd|rd|th)?\s+rep\s+dis',  # "1ST REP DIS", "2ND REP DIS"
            r'rep\s+dis\s*(\d+)',  # "REP DIS 1", "REP DIS 2"
            r',\s*(\d+)(?:st|nd|rd|th)?\s+district',  # ", 1st District"
            # Hawaii-specific patterns
            r',\s*dist\s+(\d+)',  # ", DIST 11"
            r',\s*dist\s+([ivxlcdm]+)',  # ", DIST VII" (Roman numerals)
            r'dist\s+(\d+)',  # "DIST 11"
            r'dist\s+([ivxlcdm]+)',  # "DIST VII" (Roman numerals)
            # Alaska-specific Senate district patterns
            r'senate\s+district\s+([a-z])',  # "Senate District A", "senate district a"
            r'SENATE\s+DISTRICT\s+([A-Z])',  # "SENATE DISTRICT A"
            # Illinois-specific Congress and Senate patterns
            r'(\d+)(?:st|nd|rd|th)?\s+congress',  # "1st Congress", "2nd Congress", "10th Congress"
            r'(\d+)(?:st|nd|rd|th)?\s+senate',  # "1st Senate", "2nd Senate", "35th Senate"
        ]
        
        for pattern in district_patterns:
            match = re.search(pattern, office_str, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def _clean_district_value(self, district_value) -> Optional[str]:
        """
        Clean district value to ensure it's a clean integer string.
        
        Args:
            district_value: District value that might be float, int, or string
            
        Returns:
            Clean district string or None if invalid
        """
        if pd.isna(district_value) or district_value is None:
            return None
        
        # Convert to string first
        district_str = str(district_value).strip()
        
        # If it's a float (e.g., "12.0"), convert to clean integer
        if '.' in district_str:
            try:
                district_num = int(float(district_str))
                return str(district_num)
            except ValueError:
                return None
        
        # If it's already a clean integer string, return as is
        try:
            district_num = int(district_str)
            return str(district_num)
        except ValueError:
            # If it's not a number, return as is (might be text like "At-Large")
            return district_str
    
    def standardize_offices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize office names in the DataFrame.
        
        Args:
            df: Input DataFrame with 'office' column
        
        Returns:
            DataFrame with standardized 'office' and new 'source_office' columns
        """
        if 'office' not in df.columns:
            logger.warning("No 'office' column found, skipping office standardization")
            return df
        
        logger.info(f"Starting office standardization for {len(df):,} records...")
        
        # Make a copy to avoid modifying original
        result_df = df.copy()
        
        # Add source_office column to preserve original names
        result_df['source_office'] = result_df['office']
        
        # Add source_district column to preserve original district information
        if 'district' in result_df.columns:
            result_df['source_district'] = result_df['district']
            # Clean up any existing float values in district column
            result_df['district'] = result_df['district'].apply(self._clean_district_value)
        else:
            result_df['source_district'] = None
            # Create empty district column
            result_df['district'] = None
        
        # Track standardization results
        total_records = len(result_df)
        standardized_count = 0
        unmatched_offices = set()
        
        # Apply standardization
        for idx, row in result_df.iterrows():
            original_office = row['office']
            if pd.isna(original_office):
                continue
            
            # Extract district information from office name
            extracted_district = self._extract_district_from_office(original_office)
            if extracted_district and pd.isna(result_df.at[idx, 'district']):
                # Convert to clean integer string (no decimal places)
                try:
                    district_num = int(extracted_district)
                    result_df.at[idx, 'district'] = str(district_num)
                except ValueError:
                    # If conversion fails, keep as string but clean it
                    result_df.at[idx, 'district'] = extracted_district.strip()
            
            # Try to find a match with the original office name first
            standardized_office = self._find_best_match(original_office)
            
            if not standardized_office:
                # If no match found with original name, try with cleaned name
                cleaned_office = self._clean_office_name(original_office)
                standardized_office = self._find_best_match(cleaned_office)
            
            if standardized_office:
                result_df.at[idx, 'office'] = standardized_office
                standardized_count += 1
            else:
                # Keep cleaned office name if no match found
                cleaned_office = self._clean_office_name(original_office)
                result_df.at[idx, 'office'] = cleaned_office
                unmatched_offices.add(cleaned_office)
        
        # Log results
        logger.info(f"Office standardization completed:")
        logger.info(f"  Total records: {total_records:,}")
        logger.info(f"  Standardized: {standardized_count:,} ({standardized_count/total_records*100:.1f}%)")
        logger.info(f"  Unmatched: {len(unmatched_offices):,} unique office types")
        
        if unmatched_offices:
            logger.info("Sample unmatched offices:")
            for office in list(unmatched_offices)[:10]:
                logger.info(f"  - {office}")
        
        return result_df
    
    def get_unmatched_offices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Get summary of unmatched offices for analysis.
        
        Args:
            df: Dataframe with 'office' and 'source_office' columns
            
        Returns:
            DataFrame with unmatched office summary
        """
        if 'source_office' not in df.columns:
            logger.warning("No 'source_office' column found. Run standardize_offices first.")
            return pd.DataFrame()
        
        # Find offices that weren't standardized (source_office != office)
        unmatched = df[df['source_office'] != df['office']]
        
        if unmatched.empty:
            return pd.DataFrame()
        
        # Group by source_office and count
        unmatched_summary = unmatched.groupby('source_office').size().reset_index(name='count')
        unmatched_summary = unmatched_summary.sort_values('count', ascending=False)
        
        return unmatched_summary
