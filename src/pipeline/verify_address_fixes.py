#!/usr/bin/env python3
"""
Verify Address Fixes

This script verifies that address fixes were applied correctly and
provides a summary of improvements made.
"""

import pandas as pd
import os
import glob
from pathlib import Path
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_address_fixes(data_dir: str = "data/processed") -> pd.DataFrame:
    """Verify that address fixes were applied correctly."""
    
    # Find all address-fixed files
    pattern = os.path.join(data_dir, "*_addresses_fixed_*.xlsx")
    fixed_files = glob.glob(pattern)
    
    if not fixed_files:
        logger.warning(f"No address-fixed files found in {data_dir}")
        return pd.DataFrame()
    
    verification_results = []
    
    for file_path in fixed_files:
        try:
            filename = Path(file_path).name
            state = filename.split('_')[0].title()
            
            logger.info(f"Verifying {state} address fixes...")
            
            # Load fixed data
            df_fixed = pd.read_excel(file_path)
            
            # Find address columns
            address_cols = [col for col in df_fixed.columns if 'address' in col.lower()]
            
            for col in address_cols:
                # Count various address quality metrics
                total_addresses = len(df_fixed[col].dropna())
                
                if total_addresses == 0:
                    continue
                
                # Check for common issues
                mixed_separators = 0
                double_spaces = 0
                trailing_commas = 0
                proper_formatting = 0
                
                for address in df_fixed[col].dropna():
                    if isinstance(address, str):
                        # Check for mixed separators
                        if ',' in address and (';' in address or '|' in address):
                            mixed_separators += 1
                        
                        # Check for double spaces
                        if '  ' in address:
                            double_spaces += 1
                        
                        # Check for trailing commas
                        if address.endswith(','):
                            trailing_commas += 1
                        
                        # Check for proper formatting (has state, zip pattern)
                        if re.search(r'[A-Z]{2},\s*\d{5}(?:-\d{4})?', address):
                            proper_formatting += 1
                
                # Calculate quality score
                issues = mixed_separators + double_spaces + trailing_commas
                quality_score = max(0, 100 - (issues / total_addresses * 100)) if total_addresses > 0 else 0
                
                verification_results.append({
                    'state': state,
                    'file': filename,
                    'column': col,
                    'total_addresses': total_addresses,
                    'mixed_separators': mixed_separators,
                    'double_spaces': double_spaces,
                    'trailing_commas': trailing_commas,
                    'proper_formatting': proper_formatting,
                    'total_issues': issues,
                    'quality_score': quality_score
                })
                
        except Exception as e:
            logger.error(f"Error verifying {file_path}: {e}")
    
    return pd.DataFrame(verification_results)

def compare_before_after(data_dir: str = "data/processed") -> pd.DataFrame:
    """Compare address quality before and after fixes."""
    
    # Find original cleaned files and fixed files
    original_pattern = os.path.join(data_dir, "*_cleaned_*.xlsx")
    fixed_pattern = os.path.join(data_dir, "*_addresses_fixed_*.xlsx")
    
    original_files = glob.glob(original_pattern)
    fixed_files = glob.glob(fixed_pattern)
    
    if not original_files or not fixed_files:
        logger.warning("Need both original and fixed files for comparison")
        return pd.DataFrame()
    
    comparison_results = []
    
    for fixed_file in fixed_files:
        try:
            # Find corresponding original file
            state = Path(fixed_file).stem.split('_')[0]
            original_file = None
            
            for orig_file in original_files:
                if state.lower() in Path(orig_file).stem.lower():
                    original_file = orig_file
                    break
            
            if not original_file:
                logger.warning(f"No original file found for {state}")
                continue
            
            # Load both datasets
            df_original = pd.read_excel(original_file)
            df_fixed = pd.read_excel(fixed_file)
            
            # Find address columns
            address_cols = [col for col in df_original.columns if 'address' in col.lower()]
            
            for col in address_cols:
                if col not in df_fixed.columns:
                    continue
                
                # Count issues in original
                original_issues = count_address_issues(df_original[col])
                
                # Count issues in fixed
                fixed_issues = count_address_issues(df_fixed[col])
                
                # Calculate improvement
                improvement = original_issues - fixed_issues
                improvement_pct = (improvement / original_issues * 100) if original_issues > 0 else 0
                
                comparison_results.append({
                    'state': state.title(),
                    'column': col,
                    'original_issues': original_issues,
                    'fixed_issues': fixed_issues,
                    'issues_fixed': improvement,
                    'improvement_percentage': improvement_pct,
                    'status': 'Improved' if improvement > 0 else 'No Change'
                })
                
        except Exception as e:
            logger.error(f"Error comparing {fixed_file}: {e}")
    
    return pd.DataFrame(comparison_results)

def count_address_issues(address_series: pd.Series) -> int:
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

def generate_verification_report(verification_df: pd.DataFrame, comparison_df: pd.DataFrame):
    """Generate a comprehensive verification report."""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save detailed results
    os.makedirs("data/reports", exist_ok=True)
    
    report_file = f"data/reports/address_fix_verification_{timestamp}.xlsx"
    
    with pd.ExcelWriter(report_file, engine='openpyxl') as writer:
        verification_df.to_excel(writer, sheet_name='Verification_Results', index=False)
        comparison_df.to_excel(writer, sheet_name='Before_After_Comparison', index=False)
        
        # Add summary sheet
        summary_data = {
            'Metric': [
                'Total States Verified',
                'Average Quality Score',
                'States with Issues Remaining',
                'Total Issues Found',
                'Average Improvement %'
            ],
            'Value': [
                len(verification_df['state'].unique()),
                f"{verification_df['quality_score'].mean():.1f}",
                len(verification_df[verification_df['total_issues'] > 0]),
                verification_df['total_issues'].sum(),
                f"{comparison_df['improvement_percentage'].mean():.1f}%" if not comparison_df.empty else "N/A"
            ]
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
    
    logger.info(f"Verification report saved to: {report_file}")
    return report_file

def main():
    """Run address fix verification."""
    
    logger.info("Starting address fix verification...")
    
    # Verify fixes
    verification_results = verify_address_fixes()
    
    # Compare before/after
    comparison_results = compare_before_after()
    
    if not verification_results.empty:
        # Generate report
        report_file = generate_verification_report(verification_results, comparison_results)
        
        # Display summary
        print("\n=== ADDRESS FIX VERIFICATION SUMMARY ===")
        print(f"Total states verified: {len(verification_results['state'].unique())}")
        print(f"Average quality score: {verification_results['quality_score'].mean():.1f}/100")
        
        # Show quality analysis
        high_quality = verification_results[verification_results['quality_score'] >= 90]
        medium_quality = verification_results[(verification_results['quality_score'] >= 70) & (verification_results['quality_score'] < 90)]
        low_quality = verification_results[verification_results['quality_score'] < 70]
        
        print(f"\nQuality breakdown:")
        print(f"  High quality (90+): {len(high_quality)}")
        print(f"  Medium quality (70-89): {len(medium_quality)}")
        print(f"  Low quality (<70): {len(low_quality)}")
        
        # Show comparison results
        if not comparison_results.empty:
            print(f"\nImprovement analysis:")
            improved = comparison_results[comparison_results['status'] == 'Improved']
            print(f"  States improved: {len(improved)}")
            print(f"  Average improvement: {comparison_results['improvement_percentage'].mean():.1f}%")
            
            if not improved.empty:
                print(f"\nTop improvements:")
                top_improvements = improved.nlargest(3, 'improvement_percentage')
                for _, row in top_improvements.iterrows():
                    print(f"  {row['state']}: {row['improvement_percentage']:.1f}% improvement")
        
        print(f"\n📊 Detailed report saved to: {report_file}")
        
    else:
        logger.warning("No verification results generated")

if __name__ == "__main__":
    import re
    main()
