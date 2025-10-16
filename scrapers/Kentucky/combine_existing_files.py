#!/usr/bin/env python3
"""
Script to combine all existing Kentucky candidate Excel files into one combined file.
"""

import pandas as pd
import os
import glob
from datetime import datetime
import openpyxl

def combine_kentucky_files():
    """Combine all existing Kentucky candidate Excel files into one file."""
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Find all Excel files in the directory
    excel_files = glob.glob(os.path.join(script_dir, "kentucky_candidates_*.xlsx"))
    
    if not excel_files:
        print("No Kentucky candidate Excel files found!")
        return
    
    print(f"Found {len(excel_files)} Excel files to combine:")
    for file in excel_files:
        print(f"  - {os.path.basename(file)}")
    
    # Read and combine all files
    all_dataframes = []
    
    for file_path in excel_files:
        try:
            print(f"Reading {os.path.basename(file_path)}...")
            
            # Read the Excel file
            df = pd.read_excel(file_path)
            
            # Add source file information
            df['Source_File'] = os.path.basename(file_path)
            
            all_dataframes.append(df)
            
            print(f"  - Loaded {len(df)} rows")
            
        except Exception as e:
            print(f"  - Error reading {os.path.basename(file_path)}: {e}")
    
    if not all_dataframes:
        print("No data could be loaded from any files!")
        return
    
    # Combine all dataframes
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    print(f"\nCombined data: {len(combined_df)} total rows")
    
    # Remove duplicate rows based on key columns (if they exist)
    if 'Name' in combined_df.columns and 'Office' in combined_df.columns:
        before_dedup = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=['Name', 'Office', 'Year'], keep='first')
        after_dedup = len(combined_df)
        print(f"Removed {before_dedup - after_dedup} duplicate rows")
    
    # Create the combined file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    combined_filename = f'kentucky_candidates_combined_{timestamp}.xlsx'
    raw_dir = os.path.join('data', 'raw')
    os.makedirs(raw_dir, exist_ok=True)
    combined_path = os.path.join(raw_dir, combined_filename)
    
    print(f"\nCreating combined file: {combined_filename}")
    
    with pd.ExcelWriter(combined_path, engine='openpyxl') as writer:
        combined_df.to_excel(writer, index=False, sheet_name='Candidates')
        
        workbook = writer.book
        worksheet = writer.sheets['Candidates']
        
        # Format headers
        for cell in worksheet[1]:
            cell.font = cell.font.copy(bold=True)
            cell.fill = openpyxl.styles.PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
        
        # Auto-adjust column widths
        for column in worksheet.columns:
            max_length = 0
            column_letter = openpyxl.utils.get_column_letter(column[0].column)
            
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            
            adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
            worksheet.column_dimensions[column_letter].width = adjusted_width
    
    print(f"Successfully created: {combined_filename}")
    print(f"Total candidates: {len(combined_df)}")
    
    # Show summary by year if Year column exists
    if 'Year' in combined_df.columns:
        print("\nSummary by year:")
        year_counts = combined_df['Year'].value_counts().sort_index()
        for year, count in year_counts.items():
            print(f"  {year}: {count} candidates")
    
    # Show summary by source file
    print("\nSummary by source file:")
    file_counts = combined_df['Source_File'].value_counts()
    for file, count in file_counts.items():
        print(f"  {file}: {count} candidates")

if __name__ == "__main__":
    combine_kentucky_files() 