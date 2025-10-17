#!/usr/bin/env python3
"""
Michigan Election Candidate Data Scraper

Scrapes candidate information from Michigan Secretary of State elections website.
Navigates through election listings and extracts candidate data to Excel format.
"""

import os
import re
import sys
import time
import logging
import subprocess
import platform
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PreventSleep:
    """Context manager to prevent computer from sleeping during long-running operations."""
    
    def __init__(self):
        self.process = None
        self.os_type = platform.system()
        
    def __enter__(self):
        """Start preventing sleep."""
        try:
            if self.os_type == 'Darwin':  # macOS
                logger.info("Preventing sleep on macOS using caffeinate...")
                # caffeinate prevents sleep while the process is running
                self.process = subprocess.Popen(['caffeinate', '-d'])
                
            elif self.os_type == 'Windows':
                logger.info("Preventing sleep on Windows...")
                # On Windows, use powercfg to prevent sleep
                # ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED
                import ctypes
                ctypes.windll.kernel32.SetThreadExecutionState(0x80000002)
                
            elif self.os_type == 'Linux':
                logger.info("Preventing sleep on Linux...")
                # Try to use systemd-inhibit if available
                try:
                    self.process = subprocess.Popen([
                        'systemd-inhibit',
                        '--what=idle:sleep',
                        '--who=Michigan Scraper',
                        '--why=Scraping election data',
                        '--mode=block',
                        'sleep', 'infinity'
                    ])
                except FileNotFoundError:
                    logger.warning("systemd-inhibit not found, sleep prevention may not work on Linux")
            else:
                logger.warning(f"Unknown OS: {self.os_type}, sleep prevention not available")
                
        except Exception as e:
            logger.warning(f"Could not prevent sleep: {str(e)}")
            
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop preventing sleep."""
        try:
            if self.os_type == 'Darwin' or self.os_type == 'Linux':
                if self.process:
                    logger.info("Re-enabling sleep...")
                    self.process.terminate()
                    self.process.wait(timeout=5)
                    
            elif self.os_type == 'Windows':
                # Reset Windows sleep settings
                import ctypes
                ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)
                logger.info("Re-enabling sleep...")
                
        except Exception as e:
            logger.warning(f"Error while re-enabling sleep: {str(e)}")


class MichiganElectionScraper:
    """Scraper for Michigan Secretary of State election candidate data."""
    
    BASE_URL = "https://www.michigan.gov/sos/elections/election-results-and-data"
    
    def __init__(self, headless: bool = True, output_dir: str = "output"):
        """
        Initialize the scraper.
        
        Args:
            headless: Run browser in headless mode (no GUI)
            output_dir: Directory to save output Excel files
        """
        self.headless = headless
        self.output_dir = output_dir
        self.driver = None
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
    def setup_driver(self):
        """Set up Selenium WebDriver with Chrome."""
        logger.info("Setting up Chrome WebDriver...")
        
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Install ChromeDriver and get the path
        driver_path = ChromeDriverManager().install()
        logger.info(f"ChromeDriver path from manager: {driver_path}")
        
        # Fix for webdriver-manager bug: ensure we're using the actual chromedriver executable
        # Check if the path is pointing to the wrong file (like THIRD_PARTY_NOTICES.chromedriver)
        if os.path.isdir(driver_path) or not os.path.basename(driver_path) == 'chromedriver':
            logger.info("ChromeDriver path is incorrect, searching for actual executable...")
            # Find the actual chromedriver file
            driver_dir = driver_path if os.path.isdir(driver_path) else os.path.dirname(driver_path)
            found = False
            for root, dirs, files in os.walk(driver_dir):
                for file in files:
                    if file == 'chromedriver':
                        driver_path = os.path.join(root, file)
                        found = True
                        break
                if found:
                    break
        
        # Ensure chromedriver is executable
        if os.path.exists(driver_path) and not os.access(driver_path, os.X_OK):
            logger.info(f"Making ChromeDriver executable: {driver_path}")
            os.chmod(driver_path, 0o755)
        
        logger.info(f"Using ChromeDriver at: {driver_path}")
        
        service = Service(driver_path)
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver.implicitly_wait(10)
        
        logger.info("WebDriver setup complete")
        
    def get_candidate_listing_links(self) -> List[Dict[str, str]]:
        """
        Scrape the main elections page to find all candidate listing links.
        Handles pagination to collect links from all pages.
        Only includes links where the link text explicitly says "Candidate listing".
        
        Returns:
            List of dictionaries containing election info and candidate listing URLs
        """
        logger.info(f"Navigating to main page: {self.BASE_URL}")
        self.driver.get(self.BASE_URL)
        
        # Wait for page to load and DataTables to initialize
        time.sleep(5)
        
        # Find all candidate listing links across all pages
        candidate_links = []
        all_urls_seen = set()  # Track URLs to avoid duplicates
        
        page_num = 1
        
        while True:
            logger.info(f"Scraping page {page_num} of elections...")
            
            # Get current page source and parse
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Find candidate listing links on current page
            page_links = self._extract_candidate_links_from_page(soup, all_urls_seen)
            
            if page_links:
                candidate_links.extend(page_links)
                logger.info(f"Found {len(page_links)} new candidate listings on page {page_num}")
            
            # Try to find and click the "Next" button
            next_button_found = self._click_next_page()
            
            if not next_button_found:
                logger.info("No more pages to navigate")
                break
            
            # Wait for new page to load
            time.sleep(2)
            page_num += 1
            
            # Safety check to prevent infinite loops
            if page_num > 100:
                logger.warning("Reached maximum page limit (100), stopping pagination")
                break
        
        logger.info(f"Found total of {len(candidate_links)} candidate listing links across {page_num} page(s)")
        return candidate_links
    
    def _extract_candidate_links_from_page(self, soup, seen_urls: set) -> List[Dict[str, str]]:
        """
        Extract candidate listing links from the current page.
        
        Args:
            soup: BeautifulSoup object of the current page
            seen_urls: Set of URLs already collected (to avoid duplicates)
            
        Returns:
            List of new candidate listing link dictionaries
        """
        page_links = []
        
        # Look for links where the text is exactly "Candidate listing" (case-insensitive)
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Only match links that explicitly say "Candidate listing"
            # This filters out other button types like "Results", "Data", etc.
            if text.lower() == 'candidate listing':
                # Make URL absolute if it's relative
                full_url = urljoin(self.BASE_URL, href)
                
                # Skip if we've already seen this URL
                if full_url in seen_urls:
                    continue
                
                seen_urls.add(full_url)
                
                # Extract election information from context
                election_info = self._extract_election_context(link)
                
                page_links.append({
                    'election_name': election_info['election_name'],
                    'election_year': election_info['election_year'],
                    'election_date': election_info['election_date'],
                    'election_type': election_info['election_type'],
                    'url': full_url,
                    'link_text': text
                })
                
                logger.info(f"Found candidate listing: {election_info['election_name']} ({election_info['election_year']}) - {full_url}")
        
        return page_links
    
    def _click_next_page(self) -> bool:
        """
        Try to find and click the "Next" button for pagination.
        
        Returns:
            True if next button was found and clicked, False otherwise
        """
        try:
            # Try multiple selectors to find the Next button
            selectors = [
                # Direct text match
                "//a[contains(@class, 'page-link') and text()='Next']",
                # DataTables pagination
                "//a[contains(@class, 'page-link') and contains(text(), 'Next')]",
                # Alternative pagination styles
                "//a[@aria-label='Next' or @aria-label='Next page']",
                "//button[contains(text(), 'Next')]",
                # DataTables next button (using aria-controls)
                "//a[contains(@aria-controls, 'DataTables') and contains(text(), 'Next')]"
            ]
            
            for selector in selectors:
                try:
                    next_button = self.driver.find_element(By.XPATH, selector)
                    
                    # Check if button is enabled (not disabled)
                    if next_button.is_enabled() and next_button.is_displayed():
                        # Check if it has a disabled parent or class
                        parent_class = next_button.find_element(By.XPATH, "..").get_attribute("class") or ""
                        button_class = next_button.get_attribute("class") or ""
                        
                        if "disabled" not in parent_class.lower() and "disabled" not in button_class.lower():
                            logger.info("Clicking 'Next' button to load more elections...")
                            
                            # Scroll to button and click
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                            time.sleep(0.5)
                            next_button.click()
                            
                            return True
                except Exception as e:
                    # Try next selector
                    continue
            
            # If we get here, no working next button was found
            logger.debug("No enabled 'Next' button found")
            return False
            
        except Exception as e:
            logger.debug(f"Error while looking for next button: {str(e)}")
            return False
    
    def _extract_election_context(self, link_tag) -> Dict[str, str]:
        """
        Extract election information from the context around a link.
        Captures election name, year, date, and type from the table row.
        
        Args:
            link_tag: BeautifulSoup tag object for the link
            
        Returns:
            Dictionary with election details (name, year, date, type)
        """
        election_info = {
            'election_name': 'Unknown Election',
            'election_year': None,
            'election_date': None,
            'election_type': None
        }
        
        # Try to find parent row (tr) or cell (td)
        parent_row = link_tag.find_parent('tr')
        if parent_row:
            # Get all cells from the row
            cells = parent_row.find_all(['td', 'th'])
            
            # Extract text from each cell (excluding the candidate listing link cell)
            cell_texts = []
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                # Skip the cell containing the candidate listing link
                if cell_text.lower() != 'candidate listing' and cell_text:
                    cell_texts.append(cell_text)
            
            # Try to parse the election information from the cells
            # Common patterns: [Date] [Election Name] or [Election Name] [Year]
            if cell_texts:
                full_text = ' '.join(cell_texts)
                election_info['election_name'] = full_text
                
                # Extract year (look for 4-digit year)
                year_match = re.search(r'\b(19\d{2}|20\d{2})\b', full_text)
                if year_match:
                    election_info['election_year'] = year_match.group(1)
                
                # Extract date (look for date patterns like MM/DD/YYYY or Month DD, YYYY)
                date_patterns = [
                    r'\b(\d{1,2}/\d{1,2}/\d{4})\b',
                    r'\b(\d{1,2}-\d{1,2}-\d{4})\b',
                    r'\b((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})\b'
                ]
                for pattern in date_patterns:
                    date_match = re.search(pattern, full_text, re.IGNORECASE)
                    if date_match:
                        election_info['election_date'] = date_match.group(1)
                        break
                
                # Extract election type (primary, general, special)
                election_type_patterns = [
                    (r'\bprimary\b', 'Primary'),
                    (r'\bgeneral\b', 'General'),
                    (r'\bspecial\b', 'Special'),
                    (r'\brunoff\b', 'Runoff'),
                    (r'\bpresidential\b', 'Presidential')
                ]
                for pattern, election_type in election_type_patterns:
                    if re.search(pattern, full_text, re.IGNORECASE):
                        election_info['election_type'] = election_type
                        break
        
        # Fallback: try to extract from URL
        if election_info['election_name'] == 'Unknown Election':
            href = link_tag.get('href', '')
            if 'candlist' in href:
                # Extract election code from URL (e.g., "98pri" from "/candlist/98pri/98pri_cl.htm")
                parts = href.split('/')
                for part in parts:
                    if part and part != 'candlist' and not part.endswith('.htm'):
                        election_info['election_name'] = f"Election {part}"
                        
                        # Try to extract year from code (e.g., "98" from "98pri")
                        year_match = re.search(r'^(\d{2})', part)
                        if year_match:
                            year_short = year_match.group(1)
                            # Convert 2-digit year to 4-digit (98 -> 1998, 24 -> 2024)
                            year_int = int(year_short)
                            if year_int >= 0 and year_int <= 30:
                                election_info['election_year'] = str(2000 + year_int)
                            else:
                                election_info['election_year'] = str(1900 + year_int)
                        
                        # Try to extract type from code (e.g., "pri" from "98pri")
                        if 'pri' in part.lower():
                            election_info['election_type'] = 'Primary'
                        elif 'gen' in part.lower():
                            election_info['election_type'] = 'General'
                        elif 'spe' in part.lower():
                            election_info['election_type'] = 'Special'
                        
                        break
        
        return election_info
    
    def scrape_candidate_data(self, url: str, election_info: Dict[str, str]) -> pd.DataFrame:
        """
        Scrape candidate data from a specific candidate listing page.
        Each row in the output DataFrame represents one candidate with all their data.
        
        Args:
            url: URL of the candidate listing page
            election_info: Dictionary containing election details (name, year, date, type)
            
        Returns:
            DataFrame containing candidate data (one candidate per row)
        """
        election_name = election_info.get('election_name', 'Unknown Election')
        logger.info(f"Scraping candidates from: {url}")
        
        try:
            self.driver.get(url)
            time.sleep(2)
            
            # Get page source and parse
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Try to find tables with candidate data
            tables = soup.find_all('table')
            
            if not tables:
                logger.warning(f"No tables found on page: {url}")
                return pd.DataFrame()
            
            # Process each table and combine data
            all_candidates = []
            
            for table_idx, table in enumerate(tables):
                logger.info(f"Processing table {table_idx + 1}/{len(tables)}")
                
                # Extract headers from the table
                headers = self._extract_table_headers(table)
                
                if not headers:
                    logger.warning(f"No headers found in table {table_idx + 1}, skipping")
                    continue
                
                logger.info(f"Found headers: {headers}")
                
                # Extract data rows (each row = one candidate)
                rows = table.find_all('tr')
                
                # Determine which row is the header (usually first, but could be different)
                header_row_idx = 0
                for idx, row in enumerate(rows):
                    # Check if this row contains header elements (th tags or looks like headers)
                    if row.find('th') or self._is_header_row(row, headers):
                        header_row_idx = idx
                        break
                
                # Process data rows (skip header row)
                # Track current office from office header rows
                current_office = None
                
                for row_idx, row in enumerate(rows[header_row_idx + 1:], start=header_row_idx + 1):
                    cells = row.find_all(['td', 'th'])
                    
                    # Skip empty rows
                    if not cells or all(not cell.get_text(strip=True) for cell in cells):
                        continue
                    
                    # Check if this is an office header row (has class 'offhdr')
                    first_cell = cells[0] if cells else None
                    if first_cell and 'offhdr' in first_cell.get('class', []):
                        # This is an office header row - extract the office name
                        office_text = first_cell.get_text(strip=True)
                        current_office = office_text
                        logger.debug(f"Found office header: {current_office}")
                        continue  # Skip to next row (this row doesn't have candidate data)
                    
                    # Create a record for this candidate with election info
                    candidate_data = {
                        'election': election_info.get('election_name'),
                        'election_year': election_info.get('election_year'),
                        'election_date': election_info.get('election_date'),
                        'election_type': election_info.get('election_type'),
                        'office': current_office,  # Add the tracked office
                        'source_url': url,
                        'table_number': table_idx + 1,
                        'row_number': row_idx
                    }
                    
                    # Extract data from each cell
                    for col_idx, cell in enumerate(cells):
                        # Get the header name for this column
                        header = headers[col_idx] if col_idx < len(headers) else f'Column_{col_idx+1}'
                        
                        # Clean the cell text
                        cell_text = cell.get_text(strip=True)
                        
                        # Normalize common header names to standard fields
                        normalized_header = self._normalize_header_name(header)
                        
                        candidate_data[normalized_header] = cell_text
                    
                    # Only add if there's actual candidate data
                    # A valid candidate row must have at least a name (Column_2/name field)
                    has_candidate_data = False
                    
                    # Check if we have meaningful candidate information
                    # Look for name field (could be 'name', 'Column_2', or other name variations)
                    name_fields = ['name', 'Column_2', 'candidate_name']
                    for field in name_fields:
                        if field in candidate_data and candidate_data[field] and candidate_data[field].strip():
                            has_candidate_data = True
                            break
                    
                    # Only include rows with actual candidate data
                    if has_candidate_data:
                        all_candidates.append(candidate_data)
            
            if all_candidates:
                df = pd.DataFrame(all_candidates)
                logger.info(f"Scraped {len(df)} candidate records from {election_name}")
                
                # Log the columns we found
                candidate_columns = [col for col in df.columns 
                                   if col not in ['election', 'election_year', 'election_date', 'election_type',
                                                 'source_url', 'table_number', 'row_number']]
                logger.info(f"Candidate data columns: {', '.join(candidate_columns)}")
                
                return df
            else:
                logger.warning(f"No candidate data found on page: {url}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}", exc_info=True)
            return pd.DataFrame()
    
    def _extract_table_headers(self, table) -> List[str]:
        """
        Extract column headers from a table.
        
        Args:
            table: BeautifulSoup table element
            
        Returns:
            List of header names
        """
        headers = []
        
        # Try to find header row (tr with th elements)
        header_row = None
        for row in table.find_all('tr'):
            if row.find('th'):
                header_row = row
                break
        
        if header_row:
            # Extract text from th elements
            headers = [th.get_text(strip=True) for th in header_row.find_all('th')]
        else:
            # If no th elements, try first row with td elements
            first_row = table.find('tr')
            if first_row:
                potential_headers = [td.get_text(strip=True) for td in first_row.find_all('td')]
                # Check if these look like headers (contain common header words)
                header_keywords = ['name', 'party', 'office', 'address', 'city', 'date', 
                                 'filed', 'candidate', 'district', 'county', 'zip']
                if any(keyword in ' '.join(potential_headers).lower() for keyword in header_keywords):
                    headers = potential_headers
        
        # If still no headers, create generic ones based on column count
        if not headers or all(not h for h in headers):
            first_row = table.find('tr')
            if first_row:
                num_cols = len(first_row.find_all(['td', 'th']))
                headers = [f'Column_{i+1}' for i in range(num_cols)]
        
        # Clean up empty headers
        headers = [h if h else f'Column_{i+1}' for i, h in enumerate(headers)]
        
        return headers
    
    def _is_header_row(self, row, headers: List[str]) -> bool:
        """
        Check if a row is likely a header row.
        
        Args:
            row: BeautifulSoup tr element
            headers: List of potential headers
            
        Returns:
            True if this looks like a header row
        """
        cells = row.find_all(['td', 'th'])
        cell_texts = [cell.get_text(strip=True) for cell in cells]
        
        # Check if cells match known headers
        if cell_texts and headers:
            # If most cells match headers, it's probably a header row
            matches = sum(1 for cell in cell_texts if cell in headers)
            if matches > len(cells) / 2:
                return True
        
        return False
    
    def _normalize_header_name(self, header: str) -> str:
        """
        Normalize header names to standard field names.
        
        Args:
            header: Original header name from the table
            
        Returns:
            Normalized header name
        """
        header_lower = header.lower().strip()
        
        # Map common variations to standard names
        if 'name' in header_lower and 'candidate' in header_lower:
            return 'candidate_name'
        elif 'name' in header_lower:
            return 'name'
        elif 'party' in header_lower:
            return 'party'
        elif 'office' in header_lower:
            return 'office'
        elif 'address' in header_lower and 'mailing' not in header_lower:
            return 'address'
        elif 'mailing' in header_lower and 'address' in header_lower:
            return 'mailing_address'
        elif 'city' in header_lower:
            return 'city'
        elif 'state' in header_lower:
            return 'state'
        elif 'zip' in header_lower:
            return 'zip'
        elif 'date' in header_lower and 'filed' in header_lower:
            return 'date_filed'
        elif 'filed' in header_lower:
            return 'date_filed'
        elif 'district' in header_lower:
            return 'district'
        elif 'county' in header_lower:
            return 'county'
        elif 'phone' in header_lower:
            return 'phone'
        elif 'email' in header_lower:
            return 'email'
        elif 'petition' in header_lower:
            return 'petition_info'
        elif 'withdrawal' in header_lower:
            return 'withdrawal_date'
        else:
            # Keep original header but clean it up
            return header.strip()
    
    def scrape_all_elections(self) -> pd.DataFrame:
        """
        Scrape candidate data from all elections.
        
        Returns:
            DataFrame containing all candidate data from all elections
        """
        # Get all candidate listing links
        candidate_links = self.get_candidate_listing_links()
        
        if not candidate_links:
            logger.error("No candidate listing links found!")
            return pd.DataFrame()
        
        # Scrape data from each link
        all_data = []
        
        for idx, link_info in enumerate(candidate_links, 1):
            election_display = f"{link_info['election_name']} ({link_info.get('election_year', 'N/A')})"
            logger.info(f"Processing election {idx}/{len(candidate_links)}: {election_display}")
            
            # Create election info dict for this election
            election_info = {
                'election_name': link_info['election_name'],
                'election_year': link_info.get('election_year'),
                'election_date': link_info.get('election_date'),
                'election_type': link_info.get('election_type')
            }
            
            df = self.scrape_candidate_data(
                url=link_info['url'],
                election_info=election_info
            )
            
            if not df.empty:
                all_data.append(df)
            
            # Be polite - add a small delay between requests
            time.sleep(1)
        
        # Combine all data
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"Total candidate records scraped: {len(combined_df)}")
            
            # Reorder columns to put election info first
            election_cols = ['election', 'election_year', 'election_date', 'election_type']
            other_cols = [col for col in combined_df.columns if col not in election_cols]
            combined_df = combined_df[election_cols + other_cols]
            
            return combined_df
        else:
            logger.warning("No data was scraped from any elections")
            return pd.DataFrame()
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and rename columns in the DataFrame for better readability.
        
        Args:
            df: Raw DataFrame from scraping
            
        Returns:
            Cleaned DataFrame with proper column names
        """
        if df.empty:
            return df
        
        logger.info("Cleaning and renaming columns...")
        
        # Create a copy to avoid modifying the original
        df_clean = df.copy()
        
        # Rename generic Column_X columns to meaningful names
        column_mapping = {
            'Column_1': 'party',
            'Column_2': 'name',
            'Column_3': 'address',
            'Column_4': 'date_filed'
        }
        
        # Only rename columns that exist
        columns_to_rename = {old: new for old, new in column_mapping.items() if old in df_clean.columns}
        df_clean.rename(columns=columns_to_rename, inplace=True)
        
        # Drop unnecessary columns
        columns_to_drop = ['election_date', 'table_number', 'row_number', 'Column_5']
        existing_columns_to_drop = [col for col in columns_to_drop if col in df_clean.columns]
        
        if existing_columns_to_drop:
            logger.info(f"Dropping columns: {', '.join(existing_columns_to_drop)}")
            df_clean.drop(columns=existing_columns_to_drop, inplace=True)
        
        # Reorder columns to put important ones first
        priority_columns = ['election', 'election_year', 'election_type', 'name', 'party', 
                           'office', 'address', 'city', 'state', 'zip', 'date_filed']
        
        # Get columns in priority order (only those that exist)
        ordered_columns = [col for col in priority_columns if col in df_clean.columns]
        
        # Add remaining columns that weren't in the priority list
        remaining_columns = [col for col in df_clean.columns if col not in ordered_columns]
        final_column_order = ordered_columns + remaining_columns
        
        df_clean = df_clean[final_column_order]
        
        logger.info(f"Final columns: {', '.join(df_clean.columns)}")
        
        return df_clean
    
    def save_to_excel(self, df: pd.DataFrame, filename: Optional[str] = None):
        """
        Save DataFrame to Excel file.
        
        Args:
            df: DataFrame to save
            filename: Output filename (optional, will generate timestamp-based name if not provided)
        """
        if df.empty:
            logger.warning("DataFrame is empty, nothing to save")
            return
        
        # Clean the DataFrame before saving
        df_clean = self._clean_dataframe(df)
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"michigan_candidates_{timestamp}.xlsx"
        
        filepath = os.path.join(self.output_dir, filename)
        
        logger.info(f"Saving data to Excel: {filepath}")
        df_clean.to_excel(filepath, index=False, engine='openpyxl')
        logger.info(f"Data saved successfully to {filepath}")
        
        # Also save a CSV version for easier viewing
        csv_filepath = filepath.replace('.xlsx', '.csv')
        df_clean.to_csv(csv_filepath, index=False)
        logger.info(f"CSV version saved to {csv_filepath}")
    
    def run(self, output_filename: Optional[str] = None):
        """
        Run the complete scraping process.
        Prevents computer from sleeping during execution.
        
        Args:
            output_filename: Optional output filename for Excel file
        """
        # Use context manager to prevent sleep during scraping
        with PreventSleep():
            try:
                logger.info("Starting Michigan election candidate scraper...")
                
                # Setup driver
                self.setup_driver()
                
                # Scrape all elections
                df = self.scrape_all_elections()
                
                # Save to Excel
                if not df.empty:
                    self.save_to_excel(df, output_filename)
                    logger.info("Scraping completed successfully!")
                else:
                    logger.warning("No data was scraped")
                    
            except Exception as e:
                logger.error(f"Error during scraping: {str(e)}")
                raise
            finally:
                # Clean up
                if self.driver:
                    logger.info("Closing browser...")
                    self.driver.quit()


def main():
    """Main entry point for the scraper."""
    # Create scraper instance
    scraper = MichiganElectionScraper(
        headless=False,  # Set to True to run without opening browser window
        output_dir="output"
    )
    
    # Run the scraper
    scraper.run()


if __name__ == "__main__":
    main()

