#!/usr/bin/env python3
"""
Address Separation Audit

This script audits address field separation across state data to identify
inconsistent formatting and separators.
"""

import pandas as pd
import os
import glob
from pathlib import Path
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_address_separation(data_dir: str = "data/processed") -> pd.DataFrame:
    """Analyze address field separation patterns across all state data."""
    
    # Find all processed files
    pattern = os.path.join(data_dir, "*_cleaned_*.xlsx")
    files = glob.glob(pattern)
    
    if not files:
        logger.warning(f"No processed files found in {data_dir}")
        return pd.DataFrame()
    
    separation_analysis = []
    
    for file_path in files:
        try:
            df = pd.read_excel(file_path)
            state = Path(file_path).stem.split('_')[0].title()
            
            # Check for address-related columns
            address_cols = [col for col in df.columns if 'address' in col.lower()]
            
            for col in address_cols:
                # Get non-null address values
                addresses = df[col].dropna()
                
                if len(addresses) == 0:
                    continue
                
                # Analyze separation patterns
                separators = {}
                field_counts = {}
                max_fields = 0
                
                for addr in addresses:
                    if isinstance(addr, str):
                        # Count different separator types
                        if ',' in addr:
                            separators[','] = separators.get(',', 0) + 1
                        if ';' in addr:
                            separators[';'] = separators.get(';', 0) + 1
                        if '|' in addr:
                            separators['|'] = separators.get('|', 0) + 1
                        if '\n' in addr:
                            separators['\\n'] = separators.get('\\n', 0) + 1
                        
                        # Count fields (split by any separator)
                        fields = re.split(r'[,;|\n]+', addr.strip())
                        field_count = len([f for f in fields if f.strip()])
                        field_counts[field_count] = field_counts.get(field_count, 0) + 1
                        max_fields = max(max_fields, field_count)
                
                # Determine most common separator
                primary_separator = max(separators.items(), key=lambda x: x[1])[0] if separators else 'None'
                
                # Determine most common field count
                primary_field_count = max(field_counts.items(), key=lambda x: x[1])[0] if field_counts else 0
                
                separation_analysis.append({
                    'state': state,
                    'file': Path(file_path).name,
                    'column': col,
                    'total_addresses': len(addresses),
                    'primary_separator': primary_separator,
                    'separator_counts': str(separators),
                    'primary_field_count': primary_field_count,
                    'field_count_distribution': str(field_counts),
                    'max_fields_found': max_fields,
                    'consistency_score': len(set(separators.keys())) if separators else 0
                })
                
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
    
    return pd.DataFrame(separation_analysis)

def identify_inconsistent_states(analysis_df: pd.DataFrame) -> list:
    """Identify states with inconsistent address separation."""
    inconsistent = []
    
    for _, row in analysis_df.iterrows():
        # Check for multiple separators (inconsistency)
        if row['consistency_score'] > 1:
            inconsistent.append({
                'state': row['state'],
                'issue': f"Multiple separators: {row['separator_counts']}"
            })
        
        # Check for high field count variation
        field_dist = eval(row['field_count_distribution'])
        if len(field_dist) > 3:  # More than 3 different field counts
            inconsistent.append({
                'state': row['state'],
                'issue': f"High field count variation: {row['field_count_distribution']}"
            })
    
    return inconsistent

def main():
    """Run address separation audit."""
    logger.info("Starting address separation audit...")
    
    results = analyze_address_separation()
    
    if not results.empty:
        # Save audit results
        output_file = "data/reports/address_separation_audit_results.xlsx"
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        results.to_excel(output_file, index=False)
        
        logger.info(f"Audit completed. Results saved to: {output_file}")
        
        # Display summary
        print("\n=== ADDRESS SEPARATION AUDIT SUMMARY ===")
        print(f"Total files audited: {len(results['file'].unique())}")
        print(f"Total address columns checked: {len(results)}")
        
        # Show consistency analysis
        consistent_states = results[results['consistency_score'] <= 1]
        inconsistent_states = results[results['consistency_score'] > 1]
        
        print(f"\n✅ Consistent states: {len(consistent_states)}")
        print(f"⚠️  Inconsistent states: {len(inconsistent_states)}")
        
        if not inconsistent_states.empty:
            print("\nInconsistent states found:")
            for _, row in inconsistent_states.iterrows():
                print(f"  {row['state']}: {row['separator_counts']}")
        
        # Show field count analysis
        print(f"\nField count analysis:")
        for _, row in results.iterrows():
            print(f"  {row['state']}: {row['primary_field_count']} fields (max: {row['max_fields_found']})")
            
    else:
        logger.warning("No audit results generated")

if __name__ == "__main__":
    main()
