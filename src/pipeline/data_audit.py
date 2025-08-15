#!/usr/bin/env python3
"""
Data Audit Script

This script performs comprehensive data quality audits across all state data
to identify issues, inconsistencies, and areas for improvement.
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

def audit_data_quality(data_dir: str = "data/processed") -> pd.DataFrame:
    """Audit data quality across all processed state data."""
    
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
            
            # Basic data quality metrics
            total_records = len(df)
            null_counts = df.isnull().sum()
            
            # Check for required columns
            required_cols = ['candidate_name', 'office', 'party']
            missing_required = [col for col in required_cols if col not in df.columns]
            
            # Check for duplicate records
            duplicates = df.duplicated().sum()
            
            # Check for empty strings
            empty_strings = 0
            for col in df.columns:
                if df[col].dtype == 'object':
                    empty_strings += (df[col] == '').sum()
            
            # Check for whitespace-only values
            whitespace_only = 0
            for col in df.columns:
                if df[col].dtype == 'object':
                    whitespace_only += df[col].astype(str).str.strip().eq('').sum()
            
            audit_results.append({
                'state': state,
                'file': Path(file_path).name,
                'total_records': total_records,
                'total_columns': len(df.columns),
                'missing_required_columns': str(missing_required),
                'duplicate_records': duplicates,
                'null_values_total': null_counts.sum(),
                'empty_strings': empty_strings,
                'whitespace_only_values': whitespace_only,
                'data_quality_score': calculate_quality_score(
                    total_records, duplicates, null_counts.sum(), empty_strings, whitespace_only
                )
            })
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
    
    return pd.DataFrame(audit_results)

def calculate_quality_score(total_records, duplicates, null_values, empty_strings, whitespace_only):
    """Calculate a data quality score (0-100)."""
    if total_records == 0:
        return 0
    
    # Penalize various issues
    duplicate_penalty = (duplicates / total_records) * 30
    null_penalty = (null_values / (total_records * 10)) * 20  # Assuming ~10 columns
    empty_penalty = (empty_strings / total_records) * 25
    whitespace_penalty = (whitespace_only / total_records) * 25
    
    score = 100 - duplicate_penalty - null_penalty - empty_penalty - whitespace_penalty
    return max(0, min(100, score))

def audit_column_consistency(data_dir: str = "data/processed") -> pd.DataFrame:
    """Audit column consistency across all state data."""
    
    pattern = os.path.join(data_dir, "*_cleaned_*.xlsx")
    files = glob.glob(pattern)
    
    if not files:
        return pd.DataFrame()
    
    column_analysis = {}
    
    for file_path in files:
        try:
            df = pd.read_excel(file_path)
            state = Path(file_path).stem.split('_')[0].title()
            
            for col in df.columns:
                if col not in column_analysis:
                    column_analysis[col] = {
                        'column_name': col,
                        'states_with_column': [],
                        'data_types': set(),
                        'sample_values': []
                    }
                
                column_analysis[col]['states_with_column'].append(state)
                column_analysis[col]['data_types'].add(str(df[col].dtype))
                
                # Sample some values
                sample = df[col].dropna().head(3).tolist()
                column_analysis[col]['sample_values'].extend(sample)
                
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
    
    # Convert to DataFrame
    consistency_results = []
    for col, info in column_analysis.items():
        consistency_results.append({
            'column_name': col,
            'states_with_column': len(info['states_with_column']),
            'states_list': str(info['states_with_column']),
            'data_types': str(list(info['data_types'])),
            'sample_values': str(info['sample_values'][:5])  # Limit to 5 samples
        })
    
    return pd.DataFrame(consistency_results)

def generate_audit_report(audit_df: pd.DataFrame, consistency_df: pd.DataFrame):
    """Generate a comprehensive audit report."""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save detailed results
    os.makedirs("data/reports", exist_ok=True)
    
    audit_file = f"data/reports/data_quality_audit_{timestamp}.xlsx"
    consistency_file = f"data/reports/column_consistency_audit_{timestamp}.xlsx"
    
    with pd.ExcelWriter(audit_file, engine='openpyxl') as writer:
        audit_df.to_excel(writer, sheet_name='Data_Quality', index=False)
        
        # Add summary sheet
        summary_data = {
            'Metric': [
                'Total States Audited',
                'Average Quality Score',
                'States with Duplicates',
                'States with Missing Required Columns',
                'Total Records Processed'
            ],
            'Value': [
                len(audit_df),
                f"{audit_df['data_quality_score'].mean():.1f}",
                len(audit_df[audit_df['duplicate_records'] > 0]),
                len(audit_df[audit_df['missing_required_columns'] != '[]']),
                audit_df['total_records'].sum()
            ]
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
    
    consistency_df.to_excel(consistency_file, index=False)
    
    logger.info(f"Audit reports saved:")
    logger.info(f"  - Data Quality: {audit_file}")
    logger.info(f"  - Column Consistency: {consistency_file}")
    
    return audit_file, consistency_file

def main():
    """Run comprehensive data audit."""
    logger.info("Starting comprehensive data audit...")
    
    # Run data quality audit
    quality_results = audit_data_quality()
    
    # Run column consistency audit
    consistency_results = audit_column_consistency()
    
    if not quality_results.empty:
        # Generate reports
        audit_file, consistency_file = generate_audit_report(quality_results, consistency_results)
        
        # Display summary
        print("\n=== DATA AUDIT SUMMARY ===")
        print(f"Total states audited: {len(quality_results)}")
        print(f"Average quality score: {quality_results['data_quality_score'].mean():.1f}/100")
        
        # Show quality issues
        low_quality = quality_results[quality_results['data_quality_score'] < 70]
        if not low_quality.empty:
            print(f"\n⚠️  States with quality issues (<70):")
            for _, row in low_quality.iterrows():
                print(f"  {row['state']}: {row['data_quality_score']:.1f}/100")
        
        # Show column consistency
        print(f"\nColumn consistency analysis:")
        print(f"  Total unique columns found: {len(consistency_results)}")
        
        # Show columns that appear in all states
        all_states = len(quality_results)
        universal_columns = consistency_results[consistency_results['states_with_column'] == all_states]
        print(f"  Columns in all states: {len(universal_columns)}")
        
        if not universal_columns.empty:
            print("  Universal columns:")
            for _, row in universal_columns.iterrows():
                print(f"    - {row['column_name']}")
        
    else:
        logger.warning("No audit results generated")

if __name__ == "__main__":
    main()
