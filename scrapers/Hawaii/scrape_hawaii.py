#!/usr/bin/env python3
"""
Hawaii Candidate Scraper

This script scrapes candidate information from the Hawaii Campaign Spending Commission website
and exports the data to an Excel file.

Website: https://ags.hawaii.gov/campaign/ballot-legal-name/
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
import sys
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hawaii_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class HawaiiCandidateScraper:
    def __init__(self):
        self.url = "https://ags.hawaii.gov/campaign/ballot-legal-name/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.candidates = []

    def fetch_page(self):
        """Fetch the webpage content"""
        try:
            logger.info(f"Fetching data from: {self.url}")
            response = self.session.get(self.url, timeout=30)
            response.raise_for_status()
            logger.info("Successfully fetched webpage")
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching webpage: {e}")
            raise

    def parse_candidates(self, html_content):
        """Parse candidate data from the HTML table"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find the table containing candidate data
            table = soup.find('table')
            if not table:
                logger.error("Could not find candidate table")
                return
            
            # Find all rows in the table
            rows = table.find_all('tr')
            logger.info(f"Found {len(rows)} rows in the table")
            
            # Skip the header row and process data rows
            for i, row in enumerate(rows[1:], 1):
                cells = row.find_all('td')
                if len(cells) >= 5:  # Ensure we have all 5 columns
                    candidate = {
                        'ballot_name': cells[0].get_text(strip=True),
                        'legal_name': cells[1].get_text(strip=True),
                        'office': cells[2].get_text(strip=True),
                        'party': cells[3].get_text(strip=True),
                        'filing_date': cells[4].get_text(strip=True)
                    }
                    self.candidates.append(candidate)
                    logger.debug(f"Parsed candidate {i}: {candidate['ballot_name']}")
                else:
                    logger.warning(f"Row {i} has insufficient columns: {len(cells)}")
            
            logger.info(f"Successfully parsed {len(self.candidates)} candidates")
            
        except Exception as e:
            logger.error(f"Error parsing candidates: {e}")
            raise

    def save_to_excel(self):
        """Save candidate data to Excel file"""
        if not self.candidates:
            logger.error("No candidate data to save")
            return None
        
        try:
            # Create DataFrame
            df = pd.DataFrame(self.candidates)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"hawaii_candidates_{timestamp}.xlsx"
            
            # Save to Excel
            df.to_excel(filename, index=False, engine='openpyxl')
            logger.info(f"Successfully saved {len(self.candidates)} candidates to {filename}")
            
            # Print summary
            print(f"\n{'='*60}")
            print(f"HAWAII CANDIDATE SCRAPING COMPLETE")
            print(f"{'='*60}")
            print(f"Total candidates scraped: {len(self.candidates)}")
            print(f"Output file: {filename}")
            print(f"File location: {os.path.abspath(filename)}")
            
            # Print party breakdown
            party_counts = df['party'].value_counts()
            print(f"\nParty breakdown:")
            for party, count in party_counts.items():
                print(f"  {party}: {count}")
            
            return filename
            
        except Exception as e:
            logger.error(f"Error saving to Excel: {e}")
            raise

    def run(self):
        """Main execution method"""
        try:
            logger.info("Starting Hawaii candidate scraping...")
            
            # Fetch webpage
            html_content = self.fetch_page()
            
            # Parse candidates
            self.parse_candidates(html_content)
            
            # Save to Excel
            filename = self.save_to_excel()
            
            if filename:
                logger.info("Scraping completed successfully!")
                return filename
            else:
                logger.error("Scraping failed - no data saved")
                return None
                
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise

def main():
    """Main function"""
    scraper = HawaiiCandidateScraper()
    try:
        filename = scraper.run()
        if filename:
            print(f"\n✅ Scraping completed successfully!")
            print(f"📁 Output file: {filename}")
        else:
            print("\n❌ Scraping failed!")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
