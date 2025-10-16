import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from datetime import datetime
import openpyxl
from urllib.parse import urljoin
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def extract_district(office):
    """Extract district number from office name if it exists."""
    match = re.search(r'District (\d+)', office)
    return match.group(1) if match else None

def clean_text(text):
    """Clean and format text data."""
    if not text:
        return ""
    # Remove HTML tags and extra whitespace
    text = re.sub(r'<[^>]+>', '', text)
    # Remove email protection messages
    text = re.sub(r'\[email\s+protected\]', '', text, flags=re.IGNORECASE)
    text = re.sub(r'email\s+protected', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\[email\]', '', text, flags=re.IGNORECASE)
    text = re.sub(r'email\s*#?\d*', '', text, flags=re.IGNORECASE)
    # Remove phone labels
    text = re.sub(r'phone\s*#?\d*:', '', text, flags=re.IGNORECASE)
    text = re.sub(r'phone:', '', text, flags=re.IGNORECASE)
    # Remove website labels
    text = re.sub(r'website:', '', text, flags=re.IGNORECASE)
    text = re.sub(r'web:', '', text, flags=re.IGNORECASE)
    # Remove residential/mailing address prefixes
    text = re.sub(r'(?:Residential|Mailing)\s+Address:', '', text, flags=re.IGNORECASE)
    # Remove any remaining phone numbers
    text = re.sub(r'\d{3}[-.]?\d{3}[-.]?\d{4}', '', text)
    text = re.sub(r'\(\d{3}\)\s*\d{3}[-.]?\d{4}', '', text)
    text = re.sub(r'\+\d{1,2}\s*\d{3}[-.]?\d{3}[-.]?\d{4}', '', text)
    text = re.sub(r'\d{3}\s*\d{3}\s*\d{4}', '', text)
    # Remove any remaining addresses
    text = re.sub(r'\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Court|Ct|Circle|Cir|Way|Place|Pl|Highway|Hwy|Parkway|Pkwy|Terrace|Ter|Square|Sq)[,\s]+[A-Za-z\s]+,\s*(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\s*\d{5}(?:-\d{4})?', '', text, flags=re.IGNORECASE)
    text = re.sub(r'P\.?O\.?\s+Box\s+\d+[,\s]+[A-Za-z\s]+,\s*(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\s*\d{5}(?:-\d{4})?', '', text, flags=re.IGNORECASE)
    text = re.sub(r'[A-Za-z\s]+,\s*(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\s*\d{5}(?:-\d{4})?', '', text, flags=re.IGNORECASE)
    # Remove any remaining website URLs
    text = re.sub(r'https?://[^\s<>"]+|www\.[^\s<>"]+', '', text, flags=re.IGNORECASE)
    text = re.sub(r'[^\s<>"]+\.(?:com|org|net|gov|edu|io)[^\s<>"]*', '', text, flags=re.IGNORECASE)
    # Remove any remaining colons and their surrounding whitespace
    text = re.sub(r'\s*:\s*', '', text)
    # Clean up whitespace
    text = ' '.join(text.split())
    return text.strip()

def extract_phone(text):
    """Extract phone number from text."""
    if not text:
        return ""
    # Match various phone number formats
    patterns = [
        r'\d{3}[-.]?\d{3}[-.]?\d{4}',  # Standard format
        r'\(\d{3}\)\s*\d{3}[-.]?\d{4}',  # (XXX) XXX-XXXX
        r'\+\d{1,2}\s*\d{3}[-.]?\d{3}[-.]?\d{4}',  # +1 XXX-XXX-XXXX
        r'\d{3}\s*\d{3}\s*\d{4}'  # XXX XXX XXXX
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            # Clean up the phone number
            phone = match.group(0)
            # Remove any non-digit characters except for the first + if present
            phone = re.sub(r'[^\d+]', '', phone)
            # Format as XXX-XXX-XXXX
            if len(phone) == 10:
                return f"{phone[:3]}-{phone[3:6]}-{phone[6:]}"
            elif len(phone) == 11 and phone.startswith('1'):
                return f"+1-{phone[1:4]}-{phone[4:7]}-{phone[7:]}"
            return phone
    return ""

def extract_email(text):
    """Extract email from text."""
    if not text:
        return ""
    # Match email addresses, including those with common prefixes/suffixes
    patterns = [
        r'[\w\.-]+@[\w\.-]+\.\w+',  # Standard email
        r'mailto:[\w\.-]+@[\w\.-]+\.\w+',  # mailto: prefix
        r'[\w\.-]+@[\w\.-]+\.\w+\s*\([^)]*\)',  # Email with description
        r'[\w\.-]+@[\w\.-]+\.\w+\s*\[[^\]]*\]'  # Email with brackets
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            email = match.group(0)
            # Remove any prefixes/suffixes
            email = re.sub(r'^mailto:', '', email, flags=re.IGNORECASE)
            email = re.sub(r'\s*\([^)]*\)', '', email)
            email = re.sub(r'\s*\[[^\]]*\]', '', email)
            return email.strip().lower()
    return ""

def extract_website(text):
    """Extract website from text."""
    if not text:
        return ""
    # Match various website formats
    patterns = [
        r'https?://[^\s<>"]+|www\.[^\s<>"]+',  # Standard URL
        r'[^\s<>"]+\.(?:com|org|net|gov|edu|io)[^\s<>"]*',  # Common TLDs
        r'[^\s<>"]+\.(?:com|org|net|gov|edu|io)/[^\s<>"]*'  # URLs with paths
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            url = match.group(0)
            # Add https:// if missing
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            return url.strip().lower()
    return ""

def extract_name(text):
    """Extract just the name from the text, minimal cleaning."""
    if not text:
        return ""
    # Split by newlines or commas, take the first non-empty part
    parts = [p.strip() for p in re.split(r'[\n,]', text) if p.strip()]
    if not parts:
        return ""
    name = parts[0]
    # Only remove obvious labels and email protection
    name = re.sub(r'\[email\s+protected\]', '', name, flags=re.IGNORECASE)
    name = re.sub(r'email\s+protected', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\[email\]', '', name, flags=re.IGNORECASE)
    name = re.sub(r'email\s*#?\d*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'phone\s*#?\d*:', '', name, flags=re.IGNORECASE)
    name = re.sub(r'phone:', '', name, flags=re.IGNORECASE)
    name = re.sub(r'website:', '', name, flags=re.IGNORECASE)
    name = re.sub(r'web:', '', name, flags=re.IGNORECASE)
    name = re.sub(r'(?:Residential|Mailing)\s+Address:', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*:\s*', '', name)
    name = ' '.join(name.split())
    return name.strip()

def extract_address(text):
    """Extract address from text, using everything after the first line or comma."""
    if not text:
        return ""
    # Common street types
    street_types = r'(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Court|Ct|Circle|Cir|Way|Place|Pl|Highway|Hwy|Parkway|Pkwy|Terrace|Ter|Square|Sq)'
    # Common state abbreviations
    states = r'(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)'
    # Split by newlines or commas, skip the first part (name)
    parts = [p.strip() for p in re.split(r'[\n,]', text) if p.strip()]
    address_candidates = parts[1:] if len(parts) > 1 else []
    address = ''
    for part in address_candidates:
        # Look for address patterns
        if re.search(rf'\d+\s+[A-Za-z\s]+{street_types}', part, re.IGNORECASE) or \
           re.search(rf'[A-Za-z\s]+,\s*{states}\s*\d{{5}}(?:-\d{{4}})?', part, re.IGNORECASE) or \
           re.search(rf'P\.?O\.?\s+Box\s+\d+', part, re.IGNORECASE):
            if address:
                address += ', '
            address += part
    if address:
        address = re.sub(r'\s+', ' ', address)
        address = re.sub(r',\s*,', ',', address)
        address = re.sub(r'^(?:Residential|Mailing)\s+Address:\s*', '', address, flags=re.IGNORECASE)
        return address.strip()
    return ""

def parse_name_cell(text):
    """Parse the name cell to extract name and contact information (no address)."""
    if not text:
        return {
            'name': "",
            'phone': "",
            'website': ""
        }
    # Extract name first
    name = extract_name(text)
    # Extract contact information
    phone = extract_phone(text)
    website = extract_website(text)
    return {
        'name': name,
        'phone': phone,
        'website': website
    }

def is_valid_candidate(name, office):
    """Check if the row contains valid candidate data."""
    if not name or not office:
        return False
        
    # Skip rows that are just headers or section titles
    invalid_patterns = [
        r'^nominating',
        r'^district',
        r'^county',
        r'^office',
        r'^candidate',
        r'^write-in',
        r'^protected'
    ]
    
    name = name.lower()
    office = office.lower()
    
    for pattern in invalid_patterns:
        if re.search(pattern, name, re.IGNORECASE) or re.search(pattern, office, re.IGNORECASE):
            return False
    
    return True

def scrape_delaware_candidates():
    # Ensure raw output directory exists
    raw_dir = os.path.join('data', 'raw')
    os.makedirs(raw_dir, exist_ok=True)
    base_url = "https://elections.delaware.gov/candidates/candidatelist/"
    try:
        print("Initializing scraper...")
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'darwin',
                'desktop': True
            }
        )
        print("Fetching main page...")
        response = scraper.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        all_candidates = []
        # Find all year sections
        year_sections = soup.find_all('h2')
        for section in year_sections:
            year = section.text.strip()
            if not year.lower().endswith('candidates'):
                continue
            year_label = year.replace('Candidates', '').strip()
            print(f"\nProcessing {year_label} candidates...")
            election_links = section.find_next('ul').find_all('a')
            total_elections = len(election_links)
            for election_index, link in enumerate(election_links, 1):
                election_url = urljoin(base_url, link['href'])
                election_name = link.text.strip()
                print(f"Processing election {election_index} of {total_elections}: {election_name}")
                try:
                    election_response = scraper.get(election_url)
                    election_response.raise_for_status()
                    election_soup = BeautifulSoup(election_response.text, 'html.parser')
                    tables = election_soup.find_all('table')
                    candidates_found = 0
                    for table in tables:
                        if not table.find('tr') or not table.find('td'):
                            continue
                        header_row = table.find('tr')
                        headers = [th.text.strip().lower() for th in header_row.find_all(['th', 'td'])]
                        col_indices = {
                            'name': None,
                            'office': None,
                            'county': None,
                            'date_filed': None
                        }
                        for i, header in enumerate(headers):
                            header = header.lower()
                            if 'name' in header:
                                col_indices['name'] = i
                            elif 'office' in header:
                                col_indices['office'] = i
                            elif 'county' in header:
                                col_indices['county'] = i
                            elif 'date' in header or 'filed' in header:
                                col_indices['date_filed'] = i
                        for row in table.find_all('tr')[1:]:
                            cols = row.find_all('td')
                            if len(cols) < 2:
                                continue
                            name_cell = cols[col_indices['name']].text if col_indices['name'] is not None and len(cols) > col_indices['name'] else ""
                            contact_info = parse_name_cell(name_cell)
                            office = clean_text(cols[col_indices['office']].text) if col_indices['office'] is not None and len(cols) > col_indices['office'] else ""
                            if not is_valid_candidate(contact_info['name'], office):
                                continue
                            candidate_data = {
                                'Year': year_label,
                                'Election': election_name,
                                'Name': contact_info['name'],
                                'Office': office,
                                'District': extract_district(office),
                                'County': clean_text(cols[col_indices['county']].text) if col_indices['county'] is not None and len(cols) > col_indices['county'] else "",
                                'Date Filed': clean_text(cols[col_indices['date_filed']].text) if col_indices['date_filed'] is not None and len(cols) > col_indices['date_filed'] else "",
                                'Website': contact_info['website'],
                                'Phone': contact_info['phone']
                            }
                            all_candidates.append(candidate_data)
                            candidates_found += 1
                    print(f"Found {candidates_found} candidates in this election")
                except Exception as e:
                    print(f"Error processing election {election_name}: {e}")
                    continue
        if not all_candidates:
            print("No candidates found!")
            return
        print("\nProcessing data...")
        df = pd.DataFrame(all_candidates)
        df = df.drop_duplicates(subset=['Name', 'Office', 'Year', 'Election'])
        df = df.sort_values(['Year', 'Name'], ascending=[False, True])
        print("Creating Excel file...")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_file = f'delaware_candidates_{timestamp}.xlsx'
        excel_path = os.path.join(raw_dir, excel_file)
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Candidates')
            workbook = writer.book
            worksheet = writer.sheets['Candidates']
            header_font = openpyxl.styles.Font(bold=True, size=12)
            header_fill = openpyxl.styles.PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
            header_font_color = openpyxl.styles.Font(color='FFFFFF')
            for cell in worksheet[1]:
                cell.font = header_font
                cell.fill = header_fill
                cell.font = header_font_color
            for column in worksheet.columns:
                max_length = 0
                column_letter = openpyxl.utils.get_column_letter(column[0].column)
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
            worksheet.auto_filter.ref = worksheet.dimensions
        print(f"\nSuccessfully scraped {len(all_candidates)} candidates from Delaware")
        print(f"Created Excel file: {excel_file}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    scrape_delaware_candidates() 