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

def decode_cloudflare_email(encoded_string):
    """Decode Cloudflare-protected email address.
    
    Cloudflare encodes emails using a simple XOR cipher where the first byte
    is the key and subsequent bytes are XORed with it.
    
    Args:
        encoded_string: Hex-encoded email from href or data-cfemail attribute
        
    Returns:
        Decoded email address
    """
    try:
        # Remove any leading # if present
        encoded_string = encoded_string.lstrip('#')
        
        # Convert hex string to bytes
        encoded_bytes = bytes.fromhex(encoded_string)
        
        # First byte is the XOR key
        key = encoded_bytes[0]
        
        # Decode the rest by XORing with the key
        decoded = ''.join(chr(byte ^ key) for byte in encoded_bytes[1:])
        
        return decoded
    except Exception as e:
        return ""

def extract_email(html_content):
    """Extract email from HTML content, including Cloudflare-protected emails.
    
    Args:
        html_content: HTML string that may contain emails
        
    Returns:
        Decoded email address
    """
    if not html_content:
        return ""
    
    # First, try to find Cloudflare-protected emails in href attribute
    # Pattern: href="/cdn-cgi/l/email-protection#HEXSTRING"
    cf_href_match = re.search(r'href="/cdn-cgi/l/email-protection#([a-f0-9]+)"', html_content, re.IGNORECASE)
    if cf_href_match:
        encoded_email = cf_href_match.group(1)
        decoded_email = decode_cloudflare_email(encoded_email)
        if decoded_email and '@' in decoded_email:
            return decoded_email.strip().lower()
    
    # Also try data-cfemail attribute (alternative Cloudflare encoding)
    cf_email_match = re.search(r'data-cfemail="([a-f0-9]+)"', html_content, re.IGNORECASE)
    if cf_email_match:
        encoded_email = cf_email_match.group(1)
        decoded_email = decode_cloudflare_email(encoded_email)
        if decoded_email and '@' in decoded_email:
            return decoded_email.strip().lower()
    
    # Fallback: try to find plain email addresses
    patterns = [
        r'mailto:([\w\.-]+@[\w\.-]+\.\w+)',  # mailto: link
        r'[\w\.-]+@[\w\.-]+\.\w+',  # Standard email
    ]
    
    for pattern in patterns:
        match = re.search(pattern, html_content, re.IGNORECASE)
        if match:
            # Get the email (group 1 if it exists, otherwise group 0)
            email = match.group(1) if match.lastindex else match.group(0)
            # Don't return if it's just the placeholder text
            if not re.match(r'^\[?email\s+protected\]?$', email, re.IGNORECASE):
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
    """Extract full address from text including street, city, state, and zip."""
    if not text:
        return ""
    
    # Common street types
    street_types = r'(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Court|Ct|Circle|Cir|Way|Place|Pl|Highway|Hwy|Parkway|Pkwy|Terrace|Ter|Square|Sq)'
    # Common state abbreviations
    states = r'(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)'
    
    # Split by newlines, skip the first part (name)
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    # Skip the name (first line) and any labels
    address_lines = []
    skip_first = True
    
    for line in lines:
        # Skip name line and address labels
        if skip_first and not re.search(r'\d', line):
            continue
        skip_first = False
        
        # Skip lines that are just labels
        if re.match(r'^(?:Residential|Mailing)\s+Address:?\s*$', line, re.IGNORECASE):
            continue
        
        # Remove label from start of line if present
        line = re.sub(r'^(?:Residential|Mailing)\s+Address:\s*', '', line, flags=re.IGNORECASE)
        
        # Check if this line looks like an address component
        is_address = (
            re.search(rf'\d+\s+[A-Za-z\s#\.]+(?:{street_types})?', line, re.IGNORECASE) or  # Street number and name
            re.search(rf'P\.?O\.?\s+Box\s+\d+', line, re.IGNORECASE) or  # PO Box
            re.search(rf'[A-Za-z\s]+,\s*{states}\s*\d{{5}}(?:-\d{{4}})?', line, re.IGNORECASE) or  # City, State ZIP
            re.search(rf'^{states}\s*\d{{5}}(?:-\d{{4}})?', line, re.IGNORECASE) or  # State ZIP (without city)
            re.search(rf'^[A-Za-z\s]+,\s*{states}', line, re.IGNORECASE)  # City, State (without ZIP)
        )
        
        if is_address and line:
            address_lines.append(line)
    
    if address_lines:
        # Join address lines with comma and space
        address = ', '.join(address_lines)
        # Clean up multiple spaces and commas
        address = re.sub(r'\s+', ' ', address)
        address = re.sub(r',\s*,', ',', address)
        address = re.sub(r',\s*$', '', address)
        return address.strip()
    
    return ""

def parse_name_cell(html_content, text_content):
    """Parse the name cell to extract name and contact information including address and email.
    
    Args:
        html_content: Full HTML content of the cell (to extract emails from mailto: links)
        text_content: Plain text content of the cell (for name and address extraction)
    """
    if not html_content and not text_content:
        return {
            'name': "",
            'phone': "",
            'website': "",
            'email': "",
            'address': ""
        }
    
    # Use text content for most fields (name, address, phone)
    name = extract_name(text_content) if text_content else ""
    phone = extract_phone(text_content) if text_content else ""
    address = extract_address(text_content) if text_content else ""
    
    # Use HTML content for email and website (to get mailto: and href links)
    email = extract_email(html_content) if html_content else ""
    website = extract_website(html_content) if html_content else ""
    
    # Fallback to text if HTML didn't find anything
    if not email and text_content:
        email = extract_email(text_content)
    if not website and text_content:
        website = extract_website(text_content)
    
    return {
        'name': name,
        'phone': phone,
        'website': website,
        'email': email,
        'address': address
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
                            'party': None,
                            'county': None,
                            'date_filed': None
                        }
                        for i, header in enumerate(headers):
                            header = header.lower()
                            if 'name' in header:
                                col_indices['name'] = i
                            elif 'office' in header:
                                col_indices['office'] = i
                            elif 'party' in header:
                                col_indices['party'] = i
                            elif 'county' in header:
                                col_indices['county'] = i
                            elif 'date' in header or 'filed' in header:
                                col_indices['date_filed'] = i
                        for row in table.find_all('tr')[1:]:
                            cols = row.find_all('td')
                            if len(cols) < 2:
                                continue
                            # Get the full HTML content of the name cell to preserve mailto: links
                            name_cell_html = str(cols[col_indices['name']]) if col_indices['name'] is not None and len(cols) > col_indices['name'] else ""
                            # Also get the text version for name extraction
                            name_cell_text = cols[col_indices['name']].text if col_indices['name'] is not None and len(cols) > col_indices['name'] else ""
                            # Pass both HTML and text to extract different fields appropriately
                            contact_info = parse_name_cell(name_cell_html, name_cell_text)
                            office = clean_text(cols[col_indices['office']].text) if col_indices['office'] is not None and len(cols) > col_indices['office'] else ""
                            if not is_valid_candidate(contact_info['name'], office):
                                continue
                            candidate_data = {
                                'Year': year_label,
                                'Election': election_name,
                                'Name': contact_info['name'],
                                'Party': clean_text(cols[col_indices['party']].text) if col_indices['party'] is not None and len(cols) > col_indices['party'] else "",
                                'Office': office,
                                'District': extract_district(office),
                                'County': clean_text(cols[col_indices['county']].text) if col_indices['county'] is not None and len(cols) > col_indices['county'] else "",
                                'Date Filed': clean_text(cols[col_indices['date_filed']].text) if col_indices['date_filed'] is not None and len(cols) > col_indices['date_filed'] else "",
                                'Address': contact_info['address'],
                                'Email': contact_info['email'],
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