#!/usr/bin/env python3
"""
Massachusetts Candidate Data Scraper

This script scrapes candidate data from the Massachusetts Secretary of State's
candidate list archive website. It navigates through election years (2002-2024)
and candidate types to collect comprehensive candidate information.

Website: https://www.sec.state.ma.us/divisions/elections/research-and-statistics/candidate-list-archive.htm
"""

import time
import logging
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import re
import os

class MassachusettsCandidateScraper:
    def __init__(self, headless=True, delay=2):
        """
        Initialize the Massachusetts candidate scraper.
        
        Args:
            headless (bool): Run browser in headless mode
            delay (int): Delay between requests in seconds
        """
        self.base_url = "https://www.sec.state.ma.us/divisions/elections/research-and-statistics/candidate-list-archive.htm"
        self.delay = delay
        self.candidates_data = []
        self.setup_logging()
        self.setup_driver(headless)
        
    def setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('massachusetts_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_driver(self, headless=True):
        """Set up Chrome WebDriver with appropriate options."""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.wait = WebDriverWait(self.driver, 10)
            self.logger.info("Chrome WebDriver initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize WebDriver: {e}")
            raise
            
    def navigate_to_archive_page(self):
        """Navigate to the main candidate list archive page."""
        try:
            self.logger.info(f"Navigating to: {self.base_url}")
            self.driver.get(self.base_url)
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(self.delay)
            self.logger.info("Successfully loaded archive page")
            return True
        except Exception as e:
            self.logger.error(f"Failed to navigate to archive page: {e}")
            return False
            
    def get_election_years(self):
        """Extract all available election year links from the archive page."""
        try:
            # Look for links that contain years (2003-2024, skipping 2002 which is a PDF)
            year_links = []
            links = self.driver.find_elements(By.TAG_NAME, "a")
            
            for link in links:
                href = link.get_attribute("href")
                text = link.text.strip()
                
                # Check if link contains a year pattern and matches the expected format
                # Based on the HTML example: href="candidates2024.htm" with text="2024"
                if href and text and re.match(r'^\d{4}$', text):
                    year = text
                    # Skip 2002 as it leads to a PDF, start from 2003
                    # Also skip any links that point to PDF files
                    if 2003 <= int(year) <= 2024 and not href.lower().endswith('.pdf'):
                        year_links.append({
                            'year': year,
                            'text': text,
                            'href': href,
                            'element': link
                        })
                
                # Fallback: also check for year patterns in href (like candidates2024.htm)
                elif href and re.search(r'candidates(\d{4})\.htm', href):
                    year_match = re.search(r'candidates(\d{4})\.htm', href)
                    if year_match:
                        year = year_match.group(1)
                        if 2003 <= int(year) <= 2024:
                            year_links.append({
                                'year': year,
                                'text': text or year,
                                'href': href,
                                'element': link
                            })
            
            # Sort by year in descending order (newest first)
            year_links.sort(key=lambda x: int(x['year']), reverse=True)
            self.logger.info(f"Found {len(year_links)} election years (2003-2024, skipping 2002 PDF): {[y['year'] for y in year_links]}")
            return year_links
            
        except Exception as e:
            self.logger.error(f"Failed to extract election years: {e}")
            return []
            
    def get_candidate_type_links(self):
        """Extract candidate type links from the current year page."""
        try:
            candidate_type_links = []
            links = self.driver.find_elements(By.TAG_NAME, "a")
            
            for link in links:
                href = link.get_attribute("href")
                text = link.text.strip()
                
                # Look for candidate type links with more specific criteria
                if href and text:
                    # Check for href patterns that indicate candidate data pages
                    # Examples: "2024_state_election_candidates.htm", "2024_state_primary_candidates.htm"
                    if re.search(r'\d{4}_.*_candidates\.htm', href):
                        candidate_type_links.append({
                            'text': text,
                            'href': href,
                            'element': link
                        })
                    # Also check for text patterns that indicate candidate types
                    elif any(keyword in text.lower() for keyword in 
                        ['state election candidates', 'state primary candidates', 'presidential primary candidates', 
                         'special election candidates', 'municipal candidates']):
                        candidate_type_links.append({
                            'text': text,
                            'href': href,
                            'element': link
                        })
            
            self.logger.info(f"Found {len(candidate_type_links)} candidate type links: {[link['text'] for link in candidate_type_links]}")
            return candidate_type_links
            
        except Exception as e:
            self.logger.error(f"Failed to extract candidate type links: {e}")
            return []
            
    def scrape_candidate_data(self, year, candidate_type):
        """Scrape candidate data from the current page."""
        try:
            candidates = []
            
            # Wait for page to load
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(self.delay)
            
            # Look for sections with candidate data (based on the HTML example)
            sections = self.driver.find_elements(By.TAG_NAME, "section")
            
            for section in sections:
                try:
                    # Get the office/position from h2 header
                    office_header = section.find_element(By.CSS_SELECTOR, "h2.h2-candidate-list-job")
                    office = office_header.text.strip()
                    
                    # Look for district headers (h3 tags) within this section
                    district_headers = section.find_elements(By.TAG_NAME, "h3")
                    
                    if district_headers:
                        # Process each district
                        for district_header in district_headers:
                            district = district_header.text.strip()
                            
                            # Get candidate paragraphs that come after this district header
                            # Find all elements after the h3 until the next h3 or end of section
                            candidate_paragraphs = []
                            current_element = district_header
                            
                            while current_element:
                                current_element = current_element.find_element(By.XPATH, "following-sibling::*[1]")
                                if current_element.tag_name == 'h3':
                                    break
                                elif current_element.tag_name == 'p':
                                    candidate_paragraphs.append(current_element)
                                elif current_element.tag_name == 'hr':
                                    break
                            
                            # Process candidates for this district
                            for p in candidate_paragraphs:
                                candidate_text = p.text.strip()
                                if candidate_text:
                                    # Parse candidate information
                                    # Format: "Name, Address, City, Party"
                                    candidate_parts = [part.strip() for part in candidate_text.split(',')]
                                    
                                    candidate_data = {
                                        'Election Year': year,
                                        'Election Type': candidate_type,
                                        'Office': office,
                                        'District': district,
                                        'Name': '',
                                        'Address': '',
                                        'City': '',
                                        'Party': '',
                                        'Raw Candidate Info': candidate_text
                                    }
                                    
                                    # Parse structured data if possible
                                    if len(candidate_parts) >= 4:
                                        candidate_data.update({
                                            'Name': candidate_parts[0],
                                            'Address': candidate_parts[1],
                                            'City': candidate_parts[2],
                                            'Party': candidate_parts[3]
                                        })
                                    elif len(candidate_parts) >= 3:
                                        candidate_data.update({
                                            'Name': candidate_parts[0],
                                            'Address': candidate_parts[1],
                                            'City': candidate_parts[2]
                                        })
                                    elif len(candidate_parts) >= 2:
                                        candidate_data.update({
                                            'Name': candidate_parts[0],
                                            'Address': candidate_parts[1]
                                        })
                                    else:
                                        candidate_data['Name'] = candidate_parts[0] if candidate_parts else candidate_text
                                    
                                    candidates.append(candidate_data)
                    else:
                        # No district headers found, process candidates directly under the office
                        candidate_paragraphs = section.find_elements(By.TAG_NAME, "p")
                        
                        for p in candidate_paragraphs:
                            candidate_text = p.text.strip()
                            if candidate_text:
                                # Parse candidate information
                                # Format: "Name, Address, City, Party"
                                candidate_parts = [part.strip() for part in candidate_text.split(',')]
                                
                                candidate_data = {
                                    'Election Year': year,
                                    'Election Type': candidate_type,
                                    'Office': office,
                                    'District': '',  # No district found
                                    'Name': '',
                                    'Address': '',
                                    'City': '',
                                    'Party': '',
                                    'Raw Candidate Info': candidate_text
                                }
                                
                                # Parse structured data if possible
                                if len(candidate_parts) >= 4:
                                    candidate_data.update({
                                        'Name': candidate_parts[0],
                                        'Address': candidate_parts[1],
                                        'City': candidate_parts[2],
                                        'Party': candidate_parts[3]
                                    })
                                elif len(candidate_parts) >= 3:
                                    candidate_data.update({
                                        'Name': candidate_parts[0],
                                        'Address': candidate_parts[1],
                                        'City': candidate_parts[2]
                                    })
                                elif len(candidate_parts) >= 2:
                                    candidate_data.update({
                                        'Name': candidate_parts[0],
                                        'Address': candidate_parts[1]
                                    })
                                else:
                                    candidate_data['Name'] = candidate_parts[0] if candidate_parts else candidate_text
                                
                                candidates.append(candidate_data)
                            
                except NoSuchElementException:
                    # This section doesn't have the expected structure, skip it
                    continue
                except Exception as e:
                    self.logger.warning(f"Error processing section: {e}")
                    continue
            
            # Fallback: if no sections found, look for tables or other structured data
            if not candidates:
                self.logger.info("No sections found, looking for tables or other structured data")
                
                # Look for tables
                tables = self.driver.find_elements(By.TAG_NAME, "table")
                if tables:
                    for table_idx, table in enumerate(tables):
                        try:
                            rows = table.find_elements(By.TAG_NAME, "tr")
                            headers = []
                            
                            # Get headers from first row
                            if rows:
                                header_cells = rows[0].find_elements(By.TAG_NAME, ["th", "td"])
                                headers = [cell.text.strip() for cell in header_cells]
                            
                            # Process data rows
                            for row_idx, row in enumerate(rows[1:], 1):
                                cells = row.find_elements(By.TAG_NAME, ["td", "th"])
                                if len(cells) >= 2:  # Ensure we have at least some data
                                    candidate_data = {
                                        'Election Year': year,
                                        'Election Type': candidate_type,
                                        'Office': '',
                                        'District': '',
                                        'Table Index': table_idx,
                                        'Row Index': row_idx
                                    }
                                    
                                    # Map cell data to headers
                                    for i, cell in enumerate(cells):
                                        if i < len(headers):
                                            candidate_data[headers[i]] = cell.text.strip()
                                        else:
                                            candidate_data[f'column_{i}'] = cell.text.strip()
                                    
                                    candidates.append(candidate_data)
                                    
                        except Exception as e:
                            self.logger.warning(f"Error processing table {table_idx}: {e}")
                            continue
                else:
                    # Look for lists or other structured content
                    lists = self.driver.find_elements(By.TAG_NAME, "ul")
                    for list_idx, ul in enumerate(lists):
                        items = ul.find_elements(By.TAG_NAME, "li")
                        for item_idx, item in enumerate(items):
                            text = item.text.strip()
                            if text:
                                candidates.append({
                                    'Election Year': year,
                                    'Election Type': candidate_type,
                                    'Office': '',
                                    'District': '',
                                    'List Index': list_idx,
                                    'Item Index': item_idx,
                                    'Candidate Info': text
                                })
            
            self.logger.info(f"Scraped {len(candidates)} candidates from {year} - {candidate_type}")
            return candidates
            
        except Exception as e:
            self.logger.error(f"Failed to scrape candidate data: {e}")
            return []
            
    def scrape_all_data(self):
        """Main method to scrape all candidate data."""
        try:
            # Navigate to archive page
            if not self.navigate_to_archive_page():
                return False
                
            # Get all election years
            year_links = self.get_election_years()
            if not year_links:
                self.logger.error("No election years found")
                return False
                
            # Process each year
            for year_info in year_links:
                year = year_info['year']
                self.logger.info(f"Processing election year: {year}")
                
                try:
                    # Navigate to the year page using the href instead of clicking the element
                    year_url = year_info['href']
                    if not year_url.startswith('http'):
                        # Handle relative URLs
                        base_url = self.base_url.rsplit('/', 1)[0]
                        year_url = f"{base_url}/{year_url}"
                    
                    self.logger.info(f"Navigating to year page: {year_url}")
                    self.driver.get(year_url)
                    time.sleep(self.delay)
                    
                    # Get candidate type links for this year
                    candidate_type_links = self.get_candidate_type_links()
                    
                    if not candidate_type_links:
                        self.logger.warning(f"No candidate type links found for year {year}")
                        # Continue to next year
                        continue
                    
                    # Process each candidate type
                    for type_info in candidate_type_links:
                        candidate_type = type_info['text']
                        self.logger.info(f"Processing candidate type: {candidate_type}")
                        
                        try:
                            # Navigate to candidate type page using href
                            type_url = type_info['href']
                            if not type_url.startswith('http'):
                                # Handle relative URLs
                                base_url = self.base_url.rsplit('/', 1)[0]
                                type_url = f"{base_url}/{type_url}"
                            
                            self.logger.info(f"Navigating to candidate type page: {type_url}")
                            self.driver.get(type_url)
                            time.sleep(self.delay)
                            
                            # Scrape candidate data
                            candidates = self.scrape_candidate_data(year, candidate_type)
                            self.candidates_data.extend(candidates)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing candidate type {candidate_type}: {e}")
                            continue
                    
                    # No need to go back to main page - we'll navigate directly to each year
                    
                except Exception as e:
                    self.logger.error(f"Error processing year {year}: {e}")
                    # Continue to next year - no need to go back to main page
                    continue
                    
            self.logger.info(f"Scraping completed. Total candidates collected: {len(self.candidates_data)}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to scrape all data: {e}")
            return False
            
    def save_to_excel(self, filename=None):
        """Save scraped data to Excel file."""
        if not self.candidates_data:
            self.logger.warning("No data to save")
            return False
            
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"massachusetts_candidates_{timestamp}.xlsx"
            
        try:
            df = pd.DataFrame(self.candidates_data)
            df.to_excel(filename, index=False)
            self.logger.info(f"Data saved to {filename}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to save data to Excel: {e}")
            return False
            
    def save_to_csv(self, filename=None):
        """Save scraped data to CSV file."""
        if not self.candidates_data:
            self.logger.warning("No data to save")
            return False
            
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"massachusetts_candidates_2024_test_{timestamp}.csv"
            
        try:
            df = pd.DataFrame(self.candidates_data)
            df.to_csv(filename, index=False)
            self.logger.info(f"Data saved to {filename}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to save data to CSV: {e}")
            return False
            
    def reinitialize_driver(self):
        """Reinitialize the WebDriver if it gets closed."""
        try:
            if hasattr(self, 'driver'):
                self.driver.quit()
        except:
            pass
        
        self.setup_driver(headless=False)  # Keep non-headless for debugging
        self.logger.info("WebDriver reinitialized")
        
    def is_driver_alive(self):
        """Check if the WebDriver is still alive."""
        try:
            self.driver.current_url
            return True
        except:
            return False
            
    def close(self):
        """Close the WebDriver."""
        try:
            if hasattr(self, 'driver'):
                self.driver.quit()
                self.logger.info("WebDriver closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing WebDriver: {e}")

def main():
    """Main function to run the scraper."""
    scraper = None
    try:
        # Initialize scraper
        scraper = MassachusettsCandidateScraper(headless=False, delay=3)  # Set headless=False for debugging
        
        # Scrape all data
        success = scraper.scrape_all_data()
        
        if success and scraper.candidates_data:
            # Save data to Excel only
            scraper.save_to_excel()
            
            print(f"\nScraping completed successfully!")
            print(f"Total candidates collected: {len(scraper.candidates_data)}")
        else:
            print("Scraping failed or no data collected")
            
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if scraper:
            scraper.close()

if __name__ == "__main__":
    main()
