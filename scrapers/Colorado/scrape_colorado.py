import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from datetime import datetime
import openpyxl

def extract_district(office):
    """Extract district number from office name if it exists."""
    match = re.search(r'District (\d+)', office)
    return match.group(1) if match else None

def scrape_colorado_candidates():
    # Get the directory of the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # URL of the Colorado Secretary of State's candidate list
    url = "https://www.coloradosos.gov/pubs/elections/vote/primaryCandidates.html"
    
    try:
        # Create a cloudscraper session
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'darwin',
                'desktop': True
            }
        )
        
        # Get the page content
        response = scraper.get(url)
        response.raise_for_status()
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the table
        table = soup.find('table', class_='w3-table')
        if not table:
            raise ValueError("Could not find the candidates table on the webpage")
        
        # Initialize lists to store data
        candidates = []
        
        # Process each row in the table
        for row in table.find_all('tr')[1:]:  # Skip header row
            cols = row.find_all('td')
            if len(cols) >= 4:  # Ensure we have all columns
                office = cols[0].text.strip()
                party = cols[1].text.strip()
                name = cols[2].text.strip()
                on_ballot = cols[3].text.strip()
                
                # Skip candidates who are not on the ballot
                if on_ballot != 'Y':
                    continue
                    
                # Extract district number if it exists
                district = extract_district(office)
                
                candidates.append({
                    'name': name,
                    'office': office,
                    'district': district,
                    'party': party
                })
        
        # Convert to DataFrame
        df = pd.DataFrame(candidates)
        
        # Save to CSV
        csv_path = os.path.join(script_dir, 'colorado_candidates.csv')
        df.to_csv(csv_path, index=False)
        print(f"Successfully scraped {len(candidates)} candidates from Colorado")
        
        # Create Excel file with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_file = f'colorado_candidates_{timestamp}.xlsx'
        excel_path = os.path.join(script_dir, excel_file)
        
        # Create Excel writer
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Candidates')
            
            # Get the workbook and worksheet
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
                
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"Created Excel file: {excel_file}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    scrape_colorado_candidates() 