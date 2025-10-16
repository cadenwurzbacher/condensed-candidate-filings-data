import pandas as pd
from datetime import datetime
import glob
import os
import openpyxl

def convert_to_excel():
    # Find the most recent CSV file
    csv_files = glob.glob('arkansas_candidates_*.csv')
    if not csv_files:
        print("No CSV files found.")
        return
        
    latest_csv = max(csv_files, key=os.path.getctime)
    
    # Read the CSV file
    df = pd.read_csv(latest_csv)
    
    # Create Excel filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_filename = f"arkansas_candidates_{timestamp}.xlsx"
    
    # Create Excel writer object
    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
        # Write the dataframe to Excel
        df.to_excel(writer, sheet_name='Candidates', index=False)
        
        # Get the workbook and the worksheet
        workbook = writer.book
        worksheet = writer.sheets['Candidates']
        
        # Format the header row
        for cell in worksheet[1]:
            cell.font = cell.font.copy(bold=True)
            cell.fill = openpyxl.styles.PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
        
        # Adjust column widths
        for column in worksheet.columns:
            max_length = 0
            column_letter = openpyxl.utils.get_column_letter(column[0].column)
            
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            
            adjusted_width = (max_length + 2)
            worksheet.column_dimensions[column_letter].width = adjusted_width
    
    print(f"Successfully converted CSV to Excel file: {excel_filename}")

if __name__ == "__main__":
    convert_to_excel() 