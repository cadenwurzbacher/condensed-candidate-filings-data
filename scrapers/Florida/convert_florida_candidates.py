#!/usr/bin/env python3
"""
Convert Florida Candidate List from tab-separated text to Excel format.
"""

import pandas as pd
import os
from datetime import datetime

def convert_candidate_list_to_excel(input_file, output_folder):
    """
    Convert the Florida candidate list from tab-separated text to Excel format.
    
    Args:
        input_file (str): Path to the input text file
        output_folder (str): Path to the output folder for the Excel file
    """
    
    # Read the tab-separated file
    print(f"Reading data from {input_file}...")
    
    # Try different encodings
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    df = None
    
    for encoding in encodings:
        try:
            df = pd.read_csv(input_file, sep='\t', encoding=encoding)
            print(f"Successfully read file with {encoding} encoding")
            break
        except UnicodeDecodeError:
            print(f"Failed to read with {encoding} encoding, trying next...")
            continue
    
    if df is None:
        raise Exception("Could not read file with any of the attempted encodings")
    
    # Display basic information about the data
    print(f"Data shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"First few rows:")
    print(df.head())
    
    # Create output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"florida_candidates_{timestamp}.xlsx"
    output_path = os.path.join(output_folder, output_filename)
    
    # Write to Excel file
    print(f"Writing to Excel file: {output_path}")
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Candidates', index=False)
        
        # Auto-adjust column widths
        worksheet = writer.sheets['Candidates']
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
            worksheet.column_dimensions[column_letter].width = adjusted_width
    
    print(f"Successfully converted to Excel: {output_path}")
    print(f"Total candidates: {len(df)}")
    
    # Display some statistics
    print("\nData Summary:")
    print(f"Office types: {df['OfficeDesc'].value_counts().head()}")
    print(f"Party distribution: {df['PartyDesc'].value_counts().head()}")
    print(f"Status distribution: {df['StatusDesc'].value_counts().head()}")
    
    return output_path

def main():
    """Main function to run the conversion."""
    
    # Input file path (adjust as needed)
    input_file = "/Users/johnglessner/Downloads/CandidateList (2).txt"
    
    # Output folder (current directory)
    output_folder = os.path.dirname(os.path.abspath(__file__))
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file not found: {input_file}")
        print("Please update the input_file path in the script.")
        return
    
    try:
        # Convert the file
        output_path = convert_candidate_list_to_excel(input_file, output_folder)
        print(f"\nConversion completed successfully!")
        print(f"Excel file saved to: {output_path}")
        
    except Exception as e:
        print(f"Error during conversion: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 