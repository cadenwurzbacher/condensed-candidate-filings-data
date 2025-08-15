#!/usr/bin/env python3
"""
Fix Address Separation Issues

This script fixes address field separation issues across all state data.
It handles inconsistent separators, field ordering, and formatting problems.
"""

import pandas as pd
import os
import glob
import re
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime

# Import state-specific fixers
from kentucky_address_fix_example import fix_kentucky_addresses
from missouri_address_fix_example import fix_missouri_addresses

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressFixer:
    """Handles address fixing across all states with state-specific logic."""
    
    def __init__(self):
        # State-specific address fixers
        self.state_fixers = {
            'kentucky': fix_kentucky_addresses,
            'missouri': fix_missouri_addresses,
            # Add more state-specific fixers as needed
        }
        
        # Generic address fixing patterns
        self.generic_patterns = [
            # Standardize separators
            (r'[;|]', ','),
            # Fix double spaces
            (r'\s+', ' '),
            # Fix "City, STATE ZIP" -> "City, STATE, ZIP"
            (r'([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', r'\1, \2'),
            # Remove trailing commas
            (r',\s*$', ''),
            # Remove double commas
            (r',\s*,', ',')
        ]
    
    def fix_addresses_generic(self, df: pd.DataFrame, address_column: str = 'address') -> pd.DataFrame:
        """Apply generic address fixes to all addresses."""
        
        if address_column not in df.columns:
            logger.warning(f"Address column '{address_column}' not found")
            return df
        
        logger.info(f"Applying generic address fixes to column '{address_column}'")
        
        df_fixed = df.copy()
        total_fixed = 0
        
        for idx, row in df_fixed.iterrows():
            address = row[address_column]
            
            if pd.isna(address) or not isinstance(address, str):
                continue
            
            original_address = address
            fixed_address = address
            
            # Apply all generic patterns
            for pattern, replacement in self.generic_patterns:
                fixed_address = re.sub(pattern, replacement, fixed_address)
            
            # Clean up whitespace
            fixed_address = fixed_address.strip()
            
            # Update if changed
            if fixed_address != original_address:
                df_fixed.at[idx, address_column] = fixed_address
                total_fixed += 1
        
        logger.info(f"Generic fixes applied to {total_fixed} addresses")
        return df_fixed
    
    def fix_addresses_by_state(self, df: pd.DataFrame, state: str, address_column: str = 'address') -> pd.DataFrame:
        """Apply state-specific address fixes if available."""
        
        state_lower = state.lower()
        
        if state_lower in self.state_fixers:
            logger.info(f"Applying {state} specific address fixes")
            return self.state_fixers[state_lower](df, address_column)
        else:
            logger.info(f"No state-specific fixer for {state}, using generic fixes")
            return self.fix_addresses_generic(df, address_column)
    
    def fix_all_state_addresses(self, data_dir: str = "data/processed") -> Dict[str, str]:
        """Fix addresses for all state data files."""
        
        # Find all processed files
        pattern = os.path.join(data_dir, "*_cleaned_*.xlsx")
        files = glob.glob(pattern)
        
        if not files:
            logger.warning(f"No processed files found in {data_dir}")
            return {}
        
        fixed_files = {}
        
        for file_path in files:
            try:
                # Extract state name from filename
                filename = Path(file_path).stem
                state = filename.split('_')[0].title()
                
                logger.info(f"Processing {state} addresses from {filename}")
                
                # Load data
                df = pd.read_excel(file_path)
                
                # Apply state-specific fixes
                df_fixed = self.fix_addresses_by_state(df, state)
                
                # Save fixed data
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"{filename}_addresses_fixed_{timestamp}.xlsx"
                output_path = os.path.join(data_dir, output_filename)
                
                df_fixed.to_excel(output_path, index=False)
                fixed_files[state] = output_path
                
                logger.info(f"✅ {state} addresses fixed and saved to: {output_filename}")
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
        
        return fixed_files
    
    def validate_address_fixes(self, original_file: str, fixed_file: str) -> Dict:
        """Validate that address fixes improved the data."""
        
        try:
            df_original = pd.read_excel(original_file)
            df_fixed = pd.read_excel(fixed_file)
            
            # Find address columns
            address_cols = [col for col in df_original.columns if 'address' in col.lower()]
            
            validation_results = {
                'file': Path(original_file).name,
                'address_columns': len(address_cols),
                'total_records': len(df_original),
                'improvements': []
            }
            
            for col in address_cols:
                if col in df_fixed.columns:
                    # Count issues before and after
                    issues_before = self._count_address_issues(df_original[col])
                    issues_after = self._count_address_issues(df_fixed[col])
                    
                    improvement = {
                        'column': col,
                        'issues_before': issues_before,
                        'issues_after': issues_after,
                        'improvement': issues_before - issues_after
                    }
                    
                    validation_results['improvements'].append(improvement)
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Error validating fixes: {e}")
            return {'error': str(e)}
    
    def _count_address_issues(self, address_series: pd.Series) -> int:
        """Count common address issues in a series."""
        
        issues = 0
        
        for address in address_series.dropna():
            if isinstance(address, str):
                # Check for mixed separators
                if ',' in address and (';' in address or '|' in address):
                    issues += 1
                
                # Check for double spaces
                if '  ' in address:
                    issues += 1
                
                # Check for trailing commas
                if address.endswith(','):
                    issues += 1
        
        return issues

def main():
    """Run address fixing for all state data."""
    
    logger.info("Starting address separation fixes...")
    
    fixer = AddressFixer()
    
    # Fix addresses for all states
    fixed_files = fixer.fix_all_state_addresses()
    
    if fixed_files:
        logger.info(f"✅ Address fixing completed for {len(fixed_files)} states")
        
        # Display summary
        print("\n=== ADDRESS FIXING SUMMARY ===")
        for state, file_path in fixed_files.items():
            print(f"  {state}: {Path(file_path).name}")
        
        print(f"\nTotal states processed: {len(fixed_files)}")
        print("Address fixes applied and saved to processed data directory")
        
    else:
        logger.warning("No files were processed")

if __name__ == "__main__":
    main()
