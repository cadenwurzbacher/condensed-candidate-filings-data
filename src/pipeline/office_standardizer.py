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
            
            # Comprehensive US House District Mappings
            # "US Representative, Xth District" pattern
            'us representative, 1st district': 'US House (District: 1)',
            'US Representative, 1st District': 'US House (District: 1)',
            'us representative, 2nd district': 'US House (District: 2)',
            'US Representative, 2nd District': 'US House (District: 2)',
            'us representative, 3rd district': 'US House (District: 3)',
            'US Representative, 3rd District': 'US House (District: 3)',
            'us representative, 4th district': 'US House (District: 4)',
            'US Representative, 4th District': 'US House (District: 4)',
            'us representative, 5th district': 'US House (District: 5)',
            'US Representative, 5th District': 'US House (District: 5)',
            'us representative, 6th district': 'US House (District: 6)',
            'US Representative, 6th District': 'US House (District: 6)',
            'us representative, 7th district': 'US House (District: 7)',
            'US Representative, 7th District': 'US House (District: 7)',
            'us representative, 8th district': 'US House (District: 8)',
            'US Representative, 8th District': 'US House (District: 8)',
            'us representative, 9th district': 'US House (District: 9)',
            'US Representative, 9th District': 'US House (District: 9)',
            'us representative, 10th district': 'US House (District: 10)',
            'US Representative, 10th District': 'US House (District: 10)',
            'us representative, 11th district': 'US House (District: 11)',
            'us representative, 12th district': 'US House (District: 12)',
            'us representative, 13th district': 'US House (District: 13)',
            'us representative, 14th district': 'US House (District: 14)',
            'us representative, 15th district': 'US House (District: 15)',
            'us representative, 16th district': 'US House (District: 16)',
            'us representative, 17th district': 'US House (District: 17)',
            'us representative, 18th district': 'US House (District: 18)',
            'us representative, 19th district': 'US House (District: 19)',
            'us representative, 20th district': 'US House (District: 20)',
            'us representative, 21st district': 'US House (District: 21)',
            'us representative, 22nd district': 'US House (District: 22)',
            'us representative, 23rd district': 'US House (District: 23)',
            'us representative, 24th district': 'US House (District: 24)',
            'us representative, 25th district': 'US House (District: 25)',
            'us representative, 26th district': 'US House (District: 26)',
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
            
            # Other US House variations
            'u. s. representative': 'US House',
            'u.s. rep.': 'US House',
            'rep in congress': 'US House',
            'representative in congress': 'US House',
            'representative to congress': 'US House',
            
            # US Senate variations
            'u.s. senator': 'US Senate',
            'us senator': 'US Senate',
            'united states senator': 'US Senate',
            'u.s. senate': 'US Senate',
            'us senate': 'US Senate',
            'united states senate': 'US Senate',
            
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
            
            # Add exact matches
            'State House': 'State House',
            
            # State Senate variations
            'state senator': 'State Senate',
            'state senate': 'State Senate',
            'senator': 'State Senate',
            'senate member': 'State Senate',
            'state senate member': 'State Senate',
            
            # Add exact matches
            'State Senate': 'State Senate',
            
            # Governor variations
            'governor': 'Governor',
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
            'county commissioner': 'County Commission',
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
        
        # Clean up extra whitespace
        office_str = re.sub(r'\s+', ' ', office_str).strip()
        
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
        else:
            result_df['source_district'] = None
        
        # Track standardization results
        total_records = len(result_df)
        standardized_count = 0
        unmatched_offices = set()
        
        # Apply standardization
        for idx, row in result_df.iterrows():
            original_office = row['office']
            if pd.isna(original_office):
                continue
            
            # Try to find a match with the original office name first
            standardized_office = self._find_best_match(original_office)
            
            if not standardized_office:
                # If no match found, try with cleaned office name
                cleaned_office = self._clean_office_name(original_office)
                standardized_office = self._find_best_match(cleaned_office)
            
            if standardized_office:
                result_df.at[idx, 'office'] = standardized_office
                standardized_count += 1
            else:
                # Keep original office name if no match found
                unmatched_offices.add(cleaned_office if 'cleaned_office' in locals() else original_office)
        
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
