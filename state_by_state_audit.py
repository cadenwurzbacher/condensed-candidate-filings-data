#!/usr/bin/env python3
"""
Comprehensive State-by-State Audit

This script will:
1. Test each state cleaner individually
2. Verify raw data is being processed correctly
3. Check column mappings and data transformations
4. Identify any remaining issues
5. Provide a detailed report on data processing quality
"""

import os
import sys
import pandas as pd
from pathlib import Path
import traceback
import logging

# Add src to path for imports
sys.path.append('src')

def test_state_cleaner(state_name, cleaner_func, raw_file_path):
    """Test a single state cleaner and return detailed results."""
    print(f"\n{'='*60}")
    print(f"TESTING {state_name.upper()}")
    print(f"{'='*60}")
    
    try:
        # Check if raw file exists
        if not os.path.exists(raw_file_path):
            return {
                'status': 'ERROR',
                'error': f'Raw file not found: {raw_file_path}',
                'raw_rows': 0,
                'processed_rows': 0,
                'name_columns': {},
                'office_columns': {},
                'party_columns': {},
                'election_columns': {}
            }
        
        # Load raw data
        raw_df = pd.read_excel(raw_file_path)
        raw_rows = len(raw_df)
        print(f"Raw data: {raw_rows} rows")
        print(f"Raw columns: {list(raw_df.columns)}")
        
        # Test the cleaner
        result = cleaner_func(raw_file_path)
        
        if result is None:
            return {
                'status': 'ERROR',
                'error': 'Cleaner returned None',
                'raw_rows': raw_rows,
                'processed_rows': 0,
                'name_columns': {},
                'office_columns': {},
                'party_columns': {},
                'election_columns': {}
            }
        
        # Check if output file was created
        output_files = list(Path('cleaned_data').glob(f'{state_name.lower()}_*_cleaned_*.xlsx'))
        if not output_files:
            return {
                'status': 'ERROR',
                'error': 'No output file created',
                'raw_rows': raw_rows,
                'processed_rows': 0,
                'name_columns': {},
                'office_columns': {},
                'party_columns': {},
                'election_columns': {}
            }
        
        # Load the most recent output file
        latest_output = max(output_files, key=lambda x: x.stat().st_mtime)
        processed_df = pd.read_excel(latest_output)
        processed_rows = len(processed_df)
        
        print(f"Processed data: {processed_rows} rows")
        print(f"Output file: {latest_output.name}")
        
        # Analyze data quality
        name_analysis = analyze_name_columns(processed_df)
        office_analysis = analyze_office_columns(processed_df)
        party_analysis = analyze_party_columns(processed_df)
        election_analysis = analyze_election_columns(processed_df)
        
        # Check for critical issues
        critical_issues = []
        if processed_rows == 0:
            critical_issues.append("No rows processed")
        if name_analysis['total_names'] == 0:
            critical_issues.append("No names processed")
        if office_analysis['total_offices'] == 0:
            critical_issues.append("No offices processed")
        
        status = 'SUCCESS' if not critical_issues else 'WARNING' if processed_rows > 0 else 'ERROR'
        
        return {
            'status': status,
            'error': None,
            'critical_issues': critical_issues,
            'raw_rows': raw_rows,
            'processed_rows': processed_rows,
            'output_file': str(latest_output),
            'name_columns': name_analysis,
            'office_columns': office_analysis,
            'party_columns': party_analysis,
            'election_columns': election_analysis
        }
        
    except Exception as e:
        error_msg = f"Exception: {str(e)}\n{traceback.format_exc()}"
        print(f"ERROR: {error_msg}")
        return {
            'status': 'ERROR',
            'error': error_msg,
            'raw_rows': 0,
            'processed_rows': 0,
            'name_columns': {},
            'office_columns': {},
            'party_columns': {},
            'election_columns': {}
        }

def analyze_name_columns(df):
    """Analyze name column quality."""
    name_cols = ['full_name_display', 'first_name', 'last_name', 'original_name']
    analysis = {}
    
    for col in name_cols:
        if col in df.columns:
            non_null_count = df[col].notna().sum()
            total_count = len(df)
            analysis[col] = {
                'count': non_null_count,
                'total': total_count,
                'percentage': (non_null_count / total_count * 100) if total_count > 0 else 0
            }
        else:
            analysis[col] = {'count': 0, 'total': 0, 'percentage': 0}
    
    analysis['total_names'] = analysis.get('full_name_display', {}).get('count', 0)
    return analysis

def analyze_office_columns(df):
    """Analyze office column quality."""
    office_cols = ['office', 'district', 'original_office']
    analysis = {}
    
    for col in office_cols:
        if col in df.columns:
            non_null_count = df[col].notna().sum()
            total_count = len(df)
            analysis[col] = {
                'count': non_null_count,
                'total': total_count,
                'percentage': (non_null_count / total_count * 100) if total_count > 0 else 0
            }
        else:
            analysis[col] = {'count': 0, 'total': 0, 'percentage': 0}
    
    analysis['total_offices'] = analysis.get('office', {}).get('count', 0)
    return analysis

def analyze_party_columns(df):
    """Analyze party column quality."""
    party_cols = ['party', 'original_party']
    analysis = {}
    
    for col in party_cols:
        if col in df.columns:
            non_null_count = df[col].notna().sum()
            total_count = len(df)
            analysis[col] = {
                'count': non_null_count,
                'total': total_count,
                'percentage': (non_null_count / total_count * 100) if total_count > 0 else 0
            }
        else:
            analysis[col] = {'count': 0, 'total': 0, 'percentage': 0}
    
    analysis['total_parties'] = analysis.get('party', {}).get('count', 0)
    return analysis

def analyze_election_columns(df):
    """Analyze election column quality."""
    election_cols = ['election_year', 'election_type', 'original_election']
    analysis = {}
    
    for col in election_cols:
        if col in df.columns:
            non_null_count = df[col].notna().sum()
            total_count = len(df)
            analysis[col] = {
                'count': non_null_count,
                'total': total_count,
                'percentage': (non_null_count / total_count * 100) if total_count > 0 else 0
            }
        else:
            analysis[col] = {'count': 0, 'total': 0, 'percentage': 0}
    
    analysis['total_elections'] = analysis.get('election_year', {}).get('count', 0)
    return analysis

def main():
    """Run comprehensive state-by-state audit."""
    print("🚀 COMPREHENSIVE STATE-BY-STATE AUDIT")
    print("=" * 80)
    
    # Define state cleaners and their raw data files
    state_configs = [
        ('alaska', 'data/raw/alaska_candidates_2024.xlsx'),
        ('iowa', 'data/raw/iowa_candidates_2024.xlsx'),
        ('maryland', 'data/raw/maryland_candidates_2024.xlsx'),
        ('new_york', 'data/raw/new_york_candidates_2024.xlsx'),
        ('north_carolina', 'data/raw/north_carolina_candidates_2024.xlsx'),
        ('pennsylvania', 'data/raw/pennsylvania_candidates_2016.xlsx'),
        ('virginia', 'data/raw/virginia_candidates_2024.xlsx'),
        # Add more states as needed
    ]
    
    results = {}
    
    for state_name, raw_file in state_configs:
        try:
            # Import the cleaner function
            module_name = f"src.pipeline.state_cleaners.{state_name}_cleaner"
            cleaner_module = __import__(module_name, fromlist=[f'clean_{state_name}_candidates'])
            cleaner_func = getattr(cleaner_module, f'clean_{state_name}_candidates')
            
            # Test the cleaner
            result = test_state_cleaner(state_name, cleaner_func, raw_file)
            results[state_name] = result
            
        except Exception as e:
            print(f"\n❌ Failed to import {state_name} cleaner: {e}")
            results[state_name] = {
                'status': 'ERROR',
                'error': f'Import failed: {str(e)}',
                'raw_rows': 0,
                'processed_rows': 0
            }
    
    # Generate comprehensive report
    print("\n" + "="*80)
    print("📊 COMPREHENSIVE AUDIT REPORT")
    print("="*80)
    
    success_count = 0
    warning_count = 0
    error_count = 0
    
    for state_name, result in results.items():
        status = result['status']
        if status == 'SUCCESS':
            success_count += 1
            status_icon = '✅'
        elif status == 'WARNING':
            warning_count += 1
            status_icon = '⚠️'
        else:
            error_count += 1
            status_icon = '❌'
        
        print(f"\n{status_icon} {state_name.upper()}: {status}")
        print(f"   Raw rows: {result['raw_rows']}")
        print(f"   Processed rows: {result['processed_rows']}")
        
        if result['error']:
            print(f"   Error: {result['error'][:100]}...")
        
        if 'name_columns' in result and result['name_columns']:
            names = result['name_columns']
            print(f"   Names: {names.get('total_names', 0)} processed")
        
        if 'office_columns' in result and result['office_columns']:
            offices = result['office_columns']
            print(f"   Offices: {offices.get('total_offices', 0)} processed")
    
    print(f"\n{'='*80}")
    print(f"SUMMARY: {success_count} SUCCESS, {warning_count} WARNING, {error_count} ERROR")
    print(f"{'='*80}")
    
    if success_count > 0:
        print(f"\n🎉 {success_count} states are processing data successfully!")
    
    if warning_count > 0:
        print(f"\n⚠️ {warning_count} states have warnings but are processing data")
    
    if error_count > 0:
        print(f"\n❌ {error_count} states have critical errors and need attention")

if __name__ == "__main__":
    main()
