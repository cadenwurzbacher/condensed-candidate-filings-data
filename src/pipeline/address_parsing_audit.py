#!/usr/bin/env python3
"""
Address Parsing Audit

This script audits address fields across all state data to identify parsing issues
and ensure consistent formatting.
"""

import pandas as pd
import os
import glob
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def audit_address_fields(data_dir: str = "data/processed") -> pd.DataFrame:
    """Audit address fields across all processed state data."""
    
    # Find all processed files
    pattern = os.path.join(data_dir, "*_cleaned_*.xlsx")
    files = glob.glob(pattern)
    
    if not files:
        logger.warning(f"No processed files found in {data_dir}")
        return pd.DataFrame()
    
    audit_results = []
    
    for file_path in files:
        try:
            df = pd.read_excel(file_path)
            state = Path(file_path).stem.split('_')[0].title()
            
            # Check for address-related columns
            address_cols = [col for col in df.columns if 'address' in col.lower()]
            
            for col in address_cols:
                # Sample some values
                sample_values = df[col].dropna().head(10).tolist()
                
                # Check for common issues
                issues = []
                if df[col].dtype == 'object':
                    # Check for mixed separators
                    separators = []
                    for val in sample_values:
                        if isinstance(val, str):
                            if ',' in val:
                                separators.append(',')
                            if ';' in val:
                                separators.append(';')
                            if '|' in val:
                                separators.append('|')
                    
                    if len(set(separators)) > 1:
                        issues.append("Mixed separators detected")
                    
                    # Check for inconsistent formatting
                    if any('  ' in str(val) for val in sample_values):
                        issues.append("Double spaces detected")
                
                audit_results.append({
                    'state': state,
                    'file': Path(file_path).name,
                    'column': col,
                    'total_records': len(df),
                    'non_null_count': df[col].count(),
                    'null_count': df[col].isnull().sum(),
                    'sample_values': str(sample_values[:3]),
                    'issues': '; '.join(issues) if issues else 'None'
                })
                
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
    
    return pd.DataFrame(audit_results)

def main():
    """Run address field audit."""
    logger.info("Starting address field audit...")
    
    results = audit_address_fields()
    
    if not results.empty:
        # Save audit results
        output_file = "data/reports/address_parsing_audit_results.xlsx"
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        results.to_excel(output_file, index=False)
        
        logger.info(f"Audit completed. Results saved to: {output_file}")
        
        # Display summary
        print("\n=== ADDRESS PARSING AUDIT SUMMARY ===")
        print(f"Total files audited: {len(results['file'].unique())}")
        print(f"Total columns checked: {len(results)}")
        
        # Show issues
        issues = results[results['issues'] != 'None']
        if not issues.empty:
            print(f"\nIssues found: {len(issues)}")
            for _, row in issues.iterrows():
                print(f"  {row['state']} - {row['column']}: {row['issues']}")
        else:
            print("\n✅ No issues found!")
            
    else:
        logger.warning("No audit results generated")

if __name__ == "__main__":
    main()
