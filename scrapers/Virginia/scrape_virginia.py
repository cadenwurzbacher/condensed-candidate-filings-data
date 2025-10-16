import os
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re
import sys
import subprocess
import signal
import requests
import pandas as pd
from io import BytesIO
import logging
import yaml

# Set up logging for unknown columns
logging.basicConfig(filename='unknown_columns.log', level=logging.INFO, format='%(asctime)s %(message)s')

def prevent_sleep():
    global caffeinate_process
    try:
        caffeinate_process = subprocess.Popen(['caffeinate', '-i'])
        print("Computer sleep prevention activated")
    except Exception as e:
        print(f"Could not prevent sleep: {e}")

def allow_sleep():
    global caffeinate_process
    if caffeinate_process:
        try:
            caffeinate_process.terminate()
            print("Computer sleep prevention deactivated")
        except Exception as e:
            print(f"Could not deactivate sleep prevention: {e}")

def signal_handler(signum, frame):
    print("Received interrupt signal, cleaning up...")
    allow_sleep()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def process_table_html(html_content, election_name, year):
    """Process HTML table content and return structured data"""
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table')
    if not table:
        print(f"No table found in HTML content")
        return []
    
    # Try to find the table structure
    rows = table.find_all('tr')
    if not rows:
        return []
    
    # Check if first row has data-label attributes
    first_row = rows[0]
    uses_data_labels = first_row.find('td', attrs={'data-label'}) or first_row.find('th', attrs={'data-label'})
    
    if uses_data_labels:
        return process_data_label_table(table, election_name)
    else:
        return process_header_table(table, election_name)

def process_data_label_table(table, election_name):
    """Process table that uses data-label attributes"""
    tbody = table.find('tbody')
    if not tbody:
        return []
    
    data = []
    for row in tbody.find_all('tr'):
        row_data = {}
        for cell in row.find_all(['td', 'th']):
            label = cell.get('data-label')
            if label:
                row_data[label.strip()] = cell.get_text(strip=True)
        if row_data:
            data.append(row_data)
    
    return map_table_data(data, election_name)

def process_header_table(table, election_name):
    """Process table that uses header rows"""
    rows = table.find_all('tr')
    if len(rows) < 2:
        return []
    
    # Find header row - look for row with "Office" or "Title" in first cell
    header_row_idx = 0
    for i, row in enumerate(rows):
        cells = row.find_all(['td', 'th'])
        if cells:
            first_cell_text = cells[0].get_text(strip=True).lower()
            if 'office' in first_cell_text or 'title' in first_cell_text:
                header_row_idx = i
                break
    
    # Extract headers
    header_row = rows[header_row_idx]
    headers = [cell.get_text(strip=True) for cell in header_row.find_all(['td', 'th'])]
    
    # Process data rows
    data = []
    for row in rows[header_row_idx + 1:]:
        cells = row.find_all(['td', 'th'])
        if len(cells) == len(headers):
            row_data = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    row_data[headers[i]] = cell.get_text(strip=True)
            if row_data:
                data.append(row_data)
    
    return map_table_data(data, election_name)

def map_table_data(data, election_name):
    """Map table data to standard format using direct column matching and debugging"""
    out_cols = ['election', 'office title', 'district', 'political party', 'name', 'email', 'phone', 'website', 'address', 'city', 'state', 'zip', 'date']
    processed_data = []
    
    if not data:
        return processed_data
    
    # Print debugging info
    print(f"\nDEBUG: Processing {len(data)} rows for election: {election_name}")
    if data:
        print(f"DEBUG: Available columns: {list(data[0].keys())}")
        print(f"DEBUG: First row sample: {dict(list(data[0].items())[:3])}")
    
    # Direct column name mappings (exact matches first)
    exact_mappings = {
        # Office/District
        'office': 'office title',
        'office title': 'office title', 
        'position': 'office title',
        'candidate for': 'office title',
        'seeking': 'office title',
        'district': 'district',
        'dist': 'district',
        'congressional district': 'district',
        'house district': 'district',
        'senate district': 'district',
        
        # Candidate info
        'candidate name': 'name',
        'name': 'name',
        'candidate': 'name',
        'nominee': 'name',
        'party': 'political party',
        'political party': 'political party',
        'affiliation': 'political party',
        
        # Contact info
        'email': 'email',
        'e-mail': 'email',
        'email address': 'email',
        'phone': 'phone',
        'telephone': 'phone',
        'phone number': 'phone',
        'website': 'website',
        'web site': 'website',
        'web': 'website',
        
        # Address
        'address': 'address',
        'campaign address': 'address',
        'mailing address': 'address',
        'city': 'city',
        'municipality': 'city',
        'state': 'state',
        'zip': 'zip',
        'zip code': 'zip',
        'postal code': 'zip',
        
        # Date
        'date': 'date',
        'filing date': 'date'
    }
    
    # Multi-language mappings
    multi_lang_mappings = {
        'candidato': 'name',
        'tên ứng cử viên': 'name',
        '후보자 이름': 'name',
        'partido': 'political party',
        'đảng phái': 'political party',
        '정당': 'political party',
        'correo electrónico': 'email',
        'thư điện tử': 'email',
        '이메일': 'email',
        'teléfono': 'phone',
        'điện thoại': 'phone',
        '전화번호': 'phone',
        'sitio web': 'website',
        'trang web': 'website',
        '웹사이트': 'website',
        'dirección': 'address',
        'địa chỉ': 'address',
        '주소': 'address',
        'ciudad': 'city',
        'thành phố': 'city',
        '도시': 'city',
        'estado': 'state',
        'tiểu bang': 'state',
        '주': 'state',
        'código postal': 'zip',
        'mã bưu điện': 'zip',
        '우편번호': 'zip',
        'fecha': 'date',
        'ngày': 'date',
        '날짜': 'date'
    }
    
    def normalize_column_name(col_name):
        """Normalize column name for matching"""
        if not col_name:
            return ''
        return str(col_name).strip().lower().replace('_', ' ').replace('-', ' ')
    
    # Get all column names
    all_columns = set()
    for row in data:
        all_columns.update(row.keys())
    
    # Create column mapping
    column_mapping = {}
    unmapped_columns = []
    
    for col in all_columns:
        col_normalized = normalize_column_name(col)
        
        # Try exact mapping first
        if col_normalized in exact_mappings:
            output_col = exact_mappings[col_normalized]
            column_mapping[output_col] = col
            print(f"DEBUG: Mapped '{col}' -> '{output_col}' (exact match)")
            continue
        
        # Try multi-language mapping
        if col_normalized in multi_lang_mappings:
            output_col = multi_lang_mappings[col_normalized]
            column_mapping[output_col] = col
            print(f"DEBUG: Mapped '{col}' -> '{output_col}' (multi-language)")
            continue
        
        # Try partial matching
        mapped = False
        for pattern, output_col in exact_mappings.items():
            if pattern in col_normalized or col_normalized in pattern:
                if output_col not in column_mapping:  # Don't overwrite exact matches
                    column_mapping[output_col] = col
                    print(f"DEBUG: Mapped '{col}' -> '{output_col}' (partial match)")
                    mapped = True
                    break
        
        if not mapped:
            unmapped_columns.append(col)
    
    print(f"DEBUG: Unmapped columns: {unmapped_columns}")
    print(f"DEBUG: Final column mapping: {column_mapping}")
    
    # Process each row
    for i, row in enumerate(data):
        row_out = {'election': election_name}
        
        # Initialize all output columns
        for out_col in out_cols[1:]:
            row_out[out_col] = ''
        
        # Map known columns
        for out_col, source_col in column_mapping.items():
            if source_col in row:
                value = str(row[source_col]).strip()
                if value and value != '-':
                    row_out[out_col] = value
        
        # Special handling for embedded districts in names
        if not row_out['district'] and row_out['name']:
            district_patterns = [
                r'(\d+(st|nd|rd|th)\s+district)',
                r'(district\s+\d+)',
                r'(congressional\s+district\s+\d+)',
                r'(house\s+district\s+\d+)',
                r'(senate\s+district\s+\d+)'
            ]
            for pattern in district_patterns:
                match = re.search(pattern, row_out['name'], re.IGNORECASE)
                if match:
                    row_out['district'] = match.group(1)
                    row_out['name'] = re.sub(pattern, '', row_out['name'], flags=re.IGNORECASE).strip()
                    print(f"DEBUG: Extracted district '{row_out['district']}' from name '{row_out['name']}'")
                    break
        
        # Clean up data
        for key in row_out:
            if row_out[key] == '-':
                row_out[key] = ''
            row_out[key] = ' '.join(row_out[key].split())
        
        processed_data.append(row_out)
        
        # Debug first few rows
        if i < 3:
            print(f"DEBUG: Row {i+1} output: {dict(list(row_out.items())[:5])}")
    
    print(f"DEBUG: Processed {len(processed_data)} rows\n")
    return processed_data

def load_column_mapping(year, filename):
    """Load column mapping from YAML config for a given year and filename."""
    config_path = os.path.join(os.path.dirname(__file__), 'virginia_column_mappings.yaml')
    if not os.path.exists(config_path):
        print(f"Mapping config not found: {config_path}")
        return None
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    year_map = config.get(str(year), {})
    # Try exact filename, then fallback to first mapping for the year
    mapping = year_map.get(filename, None)
    if not mapping and year_map:
        mapping = list(year_map.values())[0]  # fallback to first mapping
    return mapping

def extract_and_save_candidates(links, page):
    # Accumulate candidates by year
    year_buckets = {str(y): [] for y in range(2021, 2026)}
    for i, link in enumerate(links):
        url = link['url']
        year = link['year']
        election = link['text']
        filename = url.split('/')[-1].split('?')[0]
        print(f"Processing: {url}")
        
        # Handle different file types
        if url.endswith('.xlsx'):
            processed_data = process_excel_file(url, election, year=year, filename=filename)
            year_buckets[year].extend(processed_data)
        elif url.endswith('.csv'):
            processed_data = process_csv_file(url, election, year=year, filename=filename)
            year_buckets[year].extend(processed_data)
        elif url.endswith('.pdf'):
            processed_data = process_pdf_file(url, election)
            year_buckets[year].extend(processed_data)
        elif url.endswith('.docx') or url.endswith('.doc'):
            processed_data = process_word_file(url, election)
            year_buckets[year].extend(processed_data)
        elif url.endswith('/') or url.endswith('.html') or url.endswith('.htm'):
            processed_data = process_html_page(url, page, election)
            year_buckets[year].extend(processed_data)
        else:
            print(f"Unsupported file type: {url}")
            logging.warning(f"Unsupported file type encountered: {url}")
    
    # Save one file per year
    out_cols = ['election', 'office title', 'district', 'political party', 'name', 'email', 'phone', 'website', 'address', 'city', 'state', 'zip']
    for year, rows in year_buckets.items():
        if rows:
            out_df = pd.DataFrame(rows, columns=out_cols)
            raw_dir = os.path.join('data', 'raw')
            os.makedirs(raw_dir, exist_ok=True)
            out_path = os.path.join(raw_dir, f'virginia_candidates_{year}.xlsx')
            out_df.to_excel(out_path, index=False)
            print(f"Saved {out_path}")

def process_excel_file(url, election, year=None, filename=None):
    """Process Excel files with explicit mapping."""
    try:
        resp = requests.get(url)
        df = pd.read_excel(BytesIO(resp.content))
        return map_excel_data(df, election, year=year, filename=filename)
    except Exception as e:
        print(f"Error processing Excel file {url}: {e}")
        logging.error(f"Excel processing error for {url}: {e}")
        return []

def process_csv_file(url, election, year=None, filename=None):
    """Process CSV files with explicit mapping."""
    try:
        resp = requests.get(url)
        df = pd.read_csv(BytesIO(resp.content))
        return map_excel_data(df, election, year=year, filename=filename)
    except Exception as e:
        print(f"Error processing CSV file {url}: {e}")
        logging.error(f"CSV processing error for {url}: {e}")
        return []

def process_pdf_file(url, election):
    """Process PDF files using robust, stateful, multi-line extraction logic."""
    try:
        # Try to install pdfplumber if not available
        try:
            import pdfplumber
        except ImportError:
            print("Installing pdfplumber for PDF processing...")
            import subprocess, sys
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pdfplumber"])
            import pdfplumber
        import re
        import requests
        from io import BytesIO
        resp = requests.get(url)
        pdf = pdfplumber.open(BytesIO(resp.content))

        candidates = []
        current_district = None
        current_office = None
        current_lines = []
        failed_records = []

        def process_candidate_record(lines, office, district):
            if not lines:
                return None
            record = ' '.join(lines).strip()
            # Try to extract email
            email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', record)
            email = email_match.group(1) if email_match else ''
            before_email = record[:email_match.start()].strip() if email_match else record
            after_email = record[email_match.end():].strip() if email_match else ''
            # Try to extract party
            party_match = re.search(r'\b(Democratic|Republican|Independent|Libertarian|Green|Constitution)\b', before_email)
            party = party_match.group(1) if party_match else ''
            # Try to extract name (from after party to before address)
            name = ''
            address = ''
            city = state = zip_code = ''
            # Find city, state, zip
            city_state_zip = re.search(r'([A-Za-z .\"]+), ([A-Z]{2}) (\d{5}(?:-\d{4})?)', record)
            if city_state_zip:
                city = city_state_zip.group(1).strip()
                state = city_state_zip.group(2)
                zip_code = city_state_zip.group(3)
                address_start = before_email.find(city_state_zip.group(0))
                if address_start > 0:
                    address = before_email[address_start-len(city_state_zip.group(0)):].strip()
                else:
                    address = city_state_zip.group(0)
            # Name: after party, before address
            if party:
                party_idx = before_email.find(party)
                after_party = before_email[party_idx+len(party):].strip()
                # Remove leading 'Yes' or 'No' (incumbent)
                after_party = re.sub(r'^(Yes|No)\s+', '', after_party)
                # Remove address part
                if city_state_zip:
                    name_end = after_party.find(city_state_zip.group(0))
                    if name_end > 0:
                        name = after_party[:name_end].strip()
                    else:
                        # Try to split at first address indicator
                        addr_ind = re.search(r'(P\.O\.|Box|Apt|Suite|Dr|Rd|St|Ave|Ln|Ct|Pl|Way|Blvd|Ter|\d+)', after_party)
                        if addr_ind:
                            name = after_party[:addr_ind.start()].strip()
                        else:
                            name = after_party.strip()
                else:
                    name = after_party.strip()
            else:
                # Fallback: take first 3-5 words as name
                name = ' '.join(before_email.split()[:5])
            # Clean up name
            name = re.sub(r'\s+(P\.O\.|Box|Apt|Suite|Dr|Rd|St|Ave|Ln|Ct|Pl|Way|Blvd|Ter).*$', '', name)
            name = re.sub(r'\s+\d+.*$', '', name)
            # If we have at least a name and city/state/zip, consider it a candidate
            if name and city and state and zip_code:
                return {
                    'Office Title': office,
                    'district': district,
                    'political party': party,
                    'name': name,
                    'email': email,
                    'address': address,
                    'city': city,
                    'state': state,
                    'zip': zip_code
                }
            else:
                failed_records.append(record)
                return None

        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
            lines = text.split('\n')
            for line in lines:
                line = line.strip()
                # Detect office
                if line == 'Member, House Of Delegates' or line == 'Member, Senate of Virginia':
                    candidate = process_candidate_record(current_lines, current_office, current_district)
                    if candidate:
                        candidates.append(candidate)
                    current_lines = []
                    current_office = line
                    continue
                # Detect district
                m = re.match(r'^(\d+)(st|nd|rd|th) District$', line)
                if m:
                    candidate = process_candidate_record(current_lines, current_office, current_district)
                    if candidate:
                        candidates.append(candidate)
                    current_lines = []
                    current_district = m.group(0)
                    continue
                # Skip headers and empty lines
                if not line or line.startswith('COMMONWEALTH') or line.startswith('DEPARTMENT') or line.startswith('List of Candidates') or line.startswith('2023 June') or line.startswith('In districts') or line.startswith('that district.') or line.startswith('candidate for the respective district') or line.startswith('Generated on') or line == 'Incumbent Party Filing Date Candidate Address E-Mail Address':
                    continue
                # If line looks like a new candidate (party + address + zip or email), process previous
                if (re.search(r'\b(Democratic|Republican|Independent|Libertarian|Green|Constitution)\b', line) and re.search(r'\d{5}', line)) or re.search(r'@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', line):
                    candidate = process_candidate_record(current_lines, current_office, current_district)
                    if candidate:
                        candidates.append(candidate)
                    current_lines = [line]
                else:
                    current_lines.append(line)
        candidate = process_candidate_record(current_lines, current_office, current_district)
        if candidate:
            candidates.append(candidate)
        # Map to standard output format
        return map_table_data(candidates, election)
    except Exception as e:
        print(f"Error processing PDF file {url}: {e}")
        import logging
        logging.error(f"PDF processing error for {url}: {e}")
        return []

def process_word_file(url, election):
    """Process Word documents"""
    try:
        # Try to install python-docx if not available
        try:
            from docx import Document
        except ImportError:
            print("Installing python-docx for Word processing...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "python-docx"])
            from docx import Document
        
        resp = requests.get(url)
        doc = Document(BytesIO(resp.content))
        
        # Extract tables from Word document
        data = []
        for table in doc.tables:
            if table.rows:
                # Get headers from first row
                headers = [cell.text.strip() for cell in table.rows[0].cells]
                
                # Process data rows
                for row in table.rows[1:]:
                    row_data = {}
                    for i, cell in enumerate(row.cells):
                        if i < len(headers):
                            row_data[headers[i]] = cell.text.strip()
                    if row_data:
                        data.append(row_data)
        
        return map_table_data(data, election)
    except Exception as e:
        print(f"Error processing Word file {url}: {e}")
        logging.error(f"Word processing error for {url}: {e}")
        return []

def process_html_page(url, page, election):
    """Process HTML pages with tables"""
    try:
        import time
        page.goto(url)
        time.sleep(2)
        html = page.content()
        return process_table_html(html, election, "unknown")
    except Exception as e:
        print(f"Error processing HTML page {url}: {e}")
        logging.error(f"HTML processing error for {url}: {e}")
        return []

def map_excel_data(df, election, year=None, filename=None):
    """Map Excel/CSV data to standard format using explicit mapping from YAML config."""
    if year is None or filename is None:
        print("Year and filename required for mapping.")
        return []
    mapping = load_column_mapping(year, filename)
    print(f"\nColumns in file '{filename}': {list(df.columns)}")
    if not mapping:
        print(f"\nNo mapping found for year {year}, file '{filename}'.")
        print("Please add the following columns to 'virginia_column_mappings.yaml':")
        for col in df.columns:
            print(f"  # {col}")
        print("Example:")
        print(f"{year}:")
        print(f"  {filename}:")
        for out_col in ['office title','district','political party','name','email','phone','website','address','city','state','zip']:
            print(f"    {out_col}: '<column name>'")
        print("\nUpdate the YAML file and rerun.")
        sys.exit(1)
    out_cols = ['election', 'office title', 'district', 'political party', 'name', 'email', 'phone', 'website', 'address', 'city', 'state', 'zip']
    processed_data = []
    for idx, row in df.iterrows():
        row_out = {'election': election}
        for out_col in out_cols[1:]:
            col_name = mapping.get(out_col, None)
            row_out[out_col] = row[col_name] if col_name in row else ''
        processed_data.append(row_out)
    return processed_data

def scrape_virginia_candidate_links():
    url = "https://www.elections.virginia.gov/casting-a-ballot/previous-candidate-lists/"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        # Find all h2 headers and links below them
        results = []
        for h2 in soup.find_all('h2'):
            header_text = h2.get_text(strip=True)
            # Try to extract a year from the header
            year_match = re.search(r'(20\d{2}|19\d{2})', header_text)
            if not year_match:
                continue
            year = year_match.group(1)
            # Only include years 2025-2021
            if int(year) > 2025 or int(year) < 2021:
                continue
            # Collect all links until the next h2
            for sib in h2.find_next_siblings():
                if sib.name == 'h2':
                    break
                for a in sib.find_all('a', href=True):
                    results.append({
                        'year': year,
                        'text': a.get_text(strip=True),
                        'url': a['href'] if a['href'].startswith('http') else f"https://www.elections.virginia.gov{a['href']}"
                    })
        extract_and_save_candidates(results, page)
        browser.close()

# Test mode for a single file
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--testfile', type=str, help='Test mapping for a single Excel/CSV file (local path)')
    parser.add_argument('--year', type=str, help='Year for mapping')
    args = parser.parse_args()
    if args.testfile and args.year:
        filename = os.path.basename(args.testfile)
        df = pd.read_excel(args.testfile) if args.testfile.endswith('.xlsx') else pd.read_csv(args.testfile)
        sample = map_excel_data(df, 'TEST ELECTION', year=args.year, filename=filename)
        print(f"\nSample mapped output (first 3 rows):")
        for row in sample[:3]:
            print(row)
        sys.exit(0)
    prevent_sleep()
    try:
        scrape_virginia_candidate_links()
    finally:
        allow_sleep() 