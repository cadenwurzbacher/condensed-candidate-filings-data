import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import sys
import os

# Add parent directory to path to import scraper_utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper_utils import ensure_raw_data_dir, save_to_formatted_excel, extract_district_from_office

def scrape_colorado_candidates():
    # Ensure raw output directory exists
    raw_dir = ensure_raw_data_dir()
    
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
                district = extract_district_from_office(office)
                
                candidates.append({
                    'name': name,
                    'office': office,
                    'district': district,
                    'party': party
                })
        
        # Convert to DataFrame
        df = pd.DataFrame(candidates)
        
        # Save to CSV
        csv_path = os.path.join(raw_dir, 'colorado_candidates.csv')
        df.to_csv(csv_path, index=False)
        print(f"Successfully scraped {len(candidates)} candidates from Colorado")

        # Create Excel file with timestamp using utility function
        excel_path = save_to_formatted_excel(
            df=df,
            state_name='colorado',
            output_dir=raw_dir,
            header_color='CCCCCC'
        )

        print(f"Created Excel file: {os.path.basename(excel_path)}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    scrape_colorado_candidates() 