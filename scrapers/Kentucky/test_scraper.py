import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime
import openpyxl
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_kentucky_scraper():
    """Test the Kentucky scraper with just the first page"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Base URL for Kentucky candidate search
    url = "https://secure.kentucky.gov/kref/publicsearch/CandidateSearch?FirstName=&LastName=&ElectionDate=01%2F01%2F0001&PoliticalParty=&ExemptionStatus=All&IsActiveFlag="
    
    try:
        # Create a cloudscraper session
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'darwin',
                'desktop': True
            }
        )
        
        logging.info("Testing connection to Kentucky website...")
        
        # Get the page content
        response = scraper.get(url)
        response.raise_for_status()
        
        logging.info("Successfully connected to Kentucky website")
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the table
        table = soup.find('table')
        if not table:
            logging.error("Could not find the candidates table on the webpage")
            return
        
        logging.info("Found table on the page")
        
        # Find all rows in the table body
        rows = table.find_all('tr')
        if not rows:
            logging.error("No rows found in the table")
            return
        
        logging.info(f"Found {len(rows)} rows in the table")
        
        # Test parsing the first few rows
        test_candidates = []
        for i, row in enumerate(rows[1:6]):  # Test first 5 data rows
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 6:
                # Extract data using correct column mapping
                # cells[0]: Name (link)
                # cells[1]: Office
                # cells[3]: Location
                # cells[4]: Date
                # cells[5]: Election
                office = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                location = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                election_date = cells[4].get_text(strip=True) if len(cells) > 4 else ""
                election_type = cells[5].get_text(strip=True) if len(cells) > 5 else ""
                
                # Extract name from the first cell
                name_link = cells[0].find('a')
                if name_link:
                    name_text = name_link.get_text(strip=True)
                    name_parts = name_text.split()
                    if len(name_parts) >= 2:
                        first_name = name_parts[0]
                        last_name = ' '.join(name_parts[1:])
                    else:
                        first_name = name_text
                        last_name = ""
                else:
                    first_name = ""
                    last_name = ""
                
                test_candidates.append({
                    'last_name': last_name,
                    'first_name': first_name,
                    'office_sought': office,
                    'location': location,
                    'election_date': election_date,
                    'election_type': election_type
                })
                
                logging.info(f"Parsed candidate: {first_name} {last_name} - {office} - {location} - {election_date} - {election_type}")
        
        if test_candidates:
            # Create a test Excel file
            df = pd.DataFrame(test_candidates)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            test_filename = f'kentucky_test_{timestamp}.xlsx'
            test_path = os.path.join(script_dir, test_filename)
            
            with pd.ExcelWriter(test_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Test Candidates')
                
                workbook = writer.book
                worksheet = writer.sheets['Test Candidates']
                
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
            
            logging.info(f"Created test Excel file: {test_filename} with {len(test_candidates)} test candidates")
            logging.info("Test completed successfully!")
        
    except Exception as e:
        logging.error(f"Error during test: {e}")
        raise

if __name__ == "__main__":
    test_kentucky_scraper() 