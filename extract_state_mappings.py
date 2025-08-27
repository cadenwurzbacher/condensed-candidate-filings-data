#!/usr/bin/env python3
"""
Extract State Mappings Script

This script audits all state cleaners to extract office and party mappings
for consolidation into national standards (OfficeStandardizer and NationalStandards).
"""

import os
import re
import glob
import json
from pathlib import Path
from typing import Dict, List, Set, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class StateMappingExtractor:
    """Extract office and party mappings from all state cleaners."""
    
    def __init__(self, state_cleaners_dir: str = "src/pipeline/state_cleaners"):
        self.state_cleaners_dir = state_cleaners_dir
        self.office_patterns = {}
        self.party_patterns = {}
        self.county_patterns = {}
        
    def extract_all_mappings(self) -> Tuple[Dict, Dict, Dict]:
        """Extract all mappings from state cleaners."""
        logger.info("Starting extraction of state mappings...")
        
        # Find all state cleaner files
        state_cleaner_files = glob.glob(f"{self.state_cleaners_dir}/*_cleaner.py")
        logger.info(f"Found {len(state_cleaner_files)} state cleaner files")
        
        for file_path in state_cleaner_files:
            state_name = self._extract_state_name(file_path)
            logger.info(f"Processing {state_name}...")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                    # Extract mappings for this state
                    self._extract_office_mappings(content, state_name)
                    self._extract_party_mappings(content, state_name)
                    self._extract_county_mappings(content, state_name)
                    
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                continue
        
        logger.info(f"Extraction complete:")
        logger.info(f"  Office patterns: {len(self.office_patterns)}")
        logger.info(f"  Party patterns: {len(self.party_patterns)}")
        logger.info(f"  County patterns: {len(self.county_patterns)}")
        
        return self.office_patterns, self.party_patterns, self.county_patterns
    
    def _extract_state_name(self, file_path: str) -> str:
        """Extract state name from file path."""
        filename = os.path.basename(file_path)
        state_name = filename.replace('_cleaner.py', '').replace('_', ' ').title()
        return state_name
    
    def _extract_party_mappings(self, content: str, state_name: str):
        """Extract party mappings from state cleaner content."""
        # Look for party_mapping or party_mappings dictionaries (both class level and method level)
        patterns = [
            r'party_mapping\s*=\s*\{([^}]+)\}',
            r'party_mappings\s*=\s*\{([^}]+)\}',
            r'party_mappings\s*:\s*\{([^}]+)\}',  # YAML-style
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.DOTALL)
            if matches:
                logger.info(f"Found {len(matches)} party mapping matches for {state_name}")
                for i, match in enumerate(matches):
                    logger.debug(f"Match {i+1} for {state_name}: {match[:100]}...")
                    # Parse the party mappings
                    mappings = self._parse_mapping_dict(match)
                    logger.debug(f"Parsed {len(mappings)} mappings for {state_name}")
                    for key, value in mappings.items():
                        if key not in self.party_patterns:
                            self.party_patterns[key] = []
                        self.party_patterns[key].append({
                            'state': state_name,
                            'mapping': value,
                            'source': 'state_cleaner'
                        })
            else:
                logger.debug(f"No party mappings found for {state_name}")
    
    def _extract_office_mappings(self, content: str, state_name: str):
        """Extract office mappings from state cleaner content."""
        # Look for office_mapping or office_mappings dictionaries (both class level and method level)
        patterns = [
            r'office_mapping\s*=\s*\{([^}]+)\}',
            r'office_mappings\s*=\s*\{([^}]+)\}',
            r'office_mappings\s*:\s*\{([^}]+)\}',  # YAML-style
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.DOTALL)
            if matches:
                logger.info(f"Found {len(matches)} office mapping matches for {state_name}")
                for i, match in enumerate(matches):
                    logger.debug(f"Match {i+1} for {state_name}: {match[:100]}...")
                    # Parse the office mappings
                    mappings = self._parse_mapping_dict(match)
                    logger.debug(f"Parsed {len(mappings)} mappings for {state_name}")
                    for key, value in mappings.items():
                        if key not in self.office_patterns:
                            self.office_patterns[key] = []
                        self.office_patterns[key].append({
                            'state': state_name,
                            'mapping': value,
                            'source': 'state_cleaner'
                        })
            else:
                logger.debug(f"No office mappings found for {state_name}")
    
    def _extract_county_mappings(self, content: str, state_name: str):
        """Extract county mappings from state cleaner content."""
        # Look for county_mapping or county_mappings dictionaries (both class level and method level)
        patterns = [
            r'county_mapping\s*=\s*\{([^}]+)\}',
            r'county_mappings\s*=\s*\{([^}]+)\}',
            r'county_mappings\s*:\s*\{([^}]+)\}',  # YAML-style
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.DOTALL)
            if matches:
                logger.info(f"Found {len(matches)} county mapping matches for {state_name}")
                for i, match in enumerate(matches):
                    logger.debug(f"Match {i+1} for {state_name}: {match[:100]}...")
                    # Parse the county mappings
                    mappings = self._parse_mapping_dict(match)
                    logger.debug(f"Parsed {len(mappings)} mappings for {state_name}")
                    for key, value in mappings.items():
                        if key not in self.county_patterns:
                            self.county_patterns[key] = []
                        self.county_patterns[key].append({
                            'state': state_name,
                            'mapping': value,
                            'source': 'state_cleaner'
                        })
            else:
                logger.debug(f"No county mappings found for {state_name}")
    
    def _parse_mapping_dict(self, dict_content: str) -> Dict[str, str]:
        """Parse a dictionary string into key-value pairs."""
        mappings = {}
        
        # Split by lines and process each line
        lines = dict_content.split('\n')
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            # Look for simple key: value patterns (most common in state cleaners)
            # Format: 'key': 'value',
            match = re.match(r"['\"]([^'\"]+)['\"]\s*:\s*['\"]([^'\"]+)['\"]", line)
            if match:
                key = match.group(1)
                value = match.group(2)
                mappings[key] = value
                continue
            
            # Look for key: value patterns with different quote styles
            # Format: "key": "value",
            match = re.match(r'["\']([^"\']+)["\']\s*:\s*["\']([^"\']+)["\']', line)
            if match:
                key = match.group(1)
                value = match.group(2)
                mappings[key] = value
                continue
            
            # Look for key, value patterns (comma-separated)
            if ',' in line:
                parts = line.split(',')
                for part in parts:
                    part = part.strip()
                    if ':' in part:
                        key_value = part.split(':', 1)
                        if len(key_value) == 2:
                            key = key_value[0].strip().strip('"\'')
                            value = key_value[1].strip().strip('"\'')
                            if key and value:
                                mappings[key] = value
        
        return mappings
    
    def generate_consolidated_mappings(self) -> Dict:
        """Generate consolidated mappings for national standards."""
        logger.info("Generating consolidated mappings...")
        
        consolidated = {
            'office_mappings': {},
            'party_mappings': {},
            'county_mappings': {},
            'summary': {
                'total_office_patterns': len(self.office_patterns),
                'total_party_patterns': len(self.party_patterns),
                'total_county_patterns': len(self.county_patterns),
                'states_processed': len(set([mapping['state'] for mappings in self.office_patterns.values() for mapping in mappings]))
            }
        }
        
        # Consolidate office mappings
        for pattern, mappings in self.office_patterns.items():
            # Find the most common mapping for this pattern
            mapping_counts = {}
            for mapping in mappings:
                value = mapping['mapping']
                mapping_counts[value] = mapping_counts.get(value, 0) + 1
            
            # Use the most common mapping
            most_common = max(mapping_counts.items(), key=lambda x: x[1])
            consolidated['office_mappings'][pattern] = most_common[0]
        
        # Consolidate party mappings
        for pattern, mappings in self.party_patterns.items():
            # Find the most common mapping for this pattern
            mapping_counts = {}
            for mapping in mappings:
                value = mapping['mapping']
                mapping_counts[value] = mapping_counts.get(value, 0) + 1
            
            # Use the most common mapping
            most_common = max(mapping_counts.items(), key=lambda x: x[1])
            consolidated['party_mappings'][pattern] = most_common[0]
        
        # Consolidate county mappings
        for pattern, mappings in self.county_patterns.items():
            # Find the most common mapping for this pattern
            mapping_counts = {}
            for mapping in mappings:
                value = mapping['mapping']
                mapping_counts[value] = mapping_counts.get(value, 0) + 1
            
            # Use the most common mapping
            most_common = max(mapping_counts.items(), key=lambda x: x[1])
            consolidated['county_mappings'][pattern] = most_common[0]
        
        return consolidated
    
    def save_mappings(self, output_dir: str = "extracted_mappings"):
        """Save extracted mappings to files."""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Save detailed mappings
        with open(f"{output_dir}/detailed_mappings.json", 'w') as f:
            json.dump({
                'office_patterns': self.office_patterns,
                'party_patterns': self.party_patterns,
                'county_patterns': self.county_patterns
            }, f, indent=2)
        
        # Save consolidated mappings
        consolidated = self.generate_consolidated_mappings()
        with open(f"{output_dir}/consolidated_mappings.json", 'w') as f:
            json.dump(consolidated, f, indent=2)
        
        # Save Python code for easy integration
        self._save_python_code(output_dir, consolidated)
        
        logger.info(f"Mappings saved to {output_dir}/")
    
    def _save_python_code(self, output_dir: str, consolidated: Dict):
        """Save mappings as Python code for easy integration."""
        
        # Office mappings
        office_code = "# Auto-generated office mappings from state cleaners\n"
        office_code += "COMPREHENSIVE_OFFICE_MAPPINGS = {\n"
        for pattern, mapping in consolidated['office_mappings'].items():
            office_code += f"    '{pattern}': '{mapping}',\n"
        office_code += "}\n"
        
        with open(f"{output_dir}/office_mappings.py", 'w') as f:
            f.write(office_code)
        
        # Party mappings
        party_code = "# Auto-generated party mappings from state cleaners\n"
        party_code += "COMPREHENSIVE_PARTY_MAPPINGS = {\n"
        for pattern, mapping in consolidated['party_mappings'].items():
            party_code += f"    '{pattern}': '{mapping}',\n"
        party_code += "}\n"
        
        with open(f"{output_dir}/party_mappings.py", 'w') as f:
            f.write(party_code)
        
        # County mappings
        county_code = "# Auto-generated county mappings from state cleaners\n"
        county_code += "COMPREHENSIVE_COUNTY_MAPPINGS = {\n"
        for pattern, mapping in consolidated['county_mappings'].items():
            county_code += f"    '{pattern}': '{mapping}',\n"
        county_code += "}\n"
        
        with open(f"{output_dir}/county_mappings.py", 'w') as f:
            f.write(county_code)

def main():
    """Main extraction function."""
    logger.info("Starting state mapping extraction...")
    
    extractor = StateMappingExtractor()
    
    try:
        # Extract all mappings
        office_patterns, party_patterns, county_patterns = extractor.extract_all_mappings()
        
        # Save mappings to files
        extractor.save_mappings()
        
        # Print summary
        print("\n" + "="*60)
        print("EXTRACTION SUMMARY")
        print("="*60)
        print(f"Office Patterns: {len(office_patterns)}")
        print(f"Party Patterns: {len(party_patterns)}")
        print(f"County Patterns: {len(county_patterns)}")
        print("="*60)
        
        # Show some examples
        if office_patterns:
            print("\nSample Office Patterns:")
            for i, (pattern, mappings) in enumerate(list(office_patterns.items())[:5]):
                print(f"  {pattern} → {mappings[0]['mapping']} (from {mappings[0]['state']})")
        
        if party_patterns:
            print("\nSample Party Patterns:")
            for i, (pattern, mappings) in enumerate(list(party_patterns.items())[:5]):
                print(f"  {pattern} → {mappings[0]['mapping']} (from {mappings[0]['state']})")
        
        print("\nExtraction complete! Check the 'extracted_mappings' directory for results.")
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    main()
