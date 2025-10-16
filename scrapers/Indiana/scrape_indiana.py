import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime
import openpyxl
import pdfplumber
import tabula
import re
import time
from urllib.parse import urljoin
import difflib

def clean_text(text):
    """Clean and format text data."""
    if not text:
        return ""
    # Remove extra whitespace and normalize
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def extract_district(office):
    """Extract district number from office name if it exists."""
    match = re.search(r'District (\d+)', office)
    return match.group(1) if match else None

def is_valid_candidate(name, office):
    """Check if the row contains valid candidate data."""
    if not name or not office:
        return False
    
    # Clean the inputs
    name = str(name).strip()
    office = str(office).strip()
    
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
        r'^protected',
        r'^total',
        r'^page',
        r'^deadline',
        r'^application',
        r'^filing',
        r'^registration',
        r'^election',
        r'^primary',
        r'^general',
        r'^voter',
        r'^board',
        r'^committee',
        r'^party',
        r'^libertarian',
        r'^democratic',
        r'^republican',
        r'^independent',
        r'^minor',
        r'^convention',
        r'^certify',
        r'^mail',
        r'^submit',
        r'^file',
        r'^noon',
        r'^close',
        r'^business',
        r'^confined',
        r'^caring',
        r'^monday',
        r'^friday',
        r'^july',
        r'^may',
        r'^202[0-9]',
        r'^january',
        r'^february',
        r'^march',
        r'^april',
        r'^june',
        r'^august',
        r'^september',
        r'^october',
        r'^november',
        r'^december'
    ]
    
    name_lower = name.lower()
    office_lower = office.lower()
    
    # Check if name or office contains invalid patterns
    for pattern in invalid_patterns:
        if re.search(pattern, name_lower, re.IGNORECASE) or re.search(pattern, office_lower, re.IGNORECASE):
            return False
    
    # Check for valid name patterns (should contain at least one letter and look like a name)
    if not re.search(r'[A-Za-z]', name):
        return False
    
    # Skip if name is too long (likely not a person's name)
    if len(name) > 100:
        return False
    
    # Skip if office is too long (likely not an office title)
    if len(office) > 200:
        return False
    
    # Skip if name contains too many words (likely not a person's name)
    if len(name.split()) > 10:
        return False
    
    return True

def download_pdf(url, filename):
    """Download PDF file from URL."""
    try:
        print(f"Downloading {filename}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Successfully downloaded {filename}")
        return True
    except Exception as e:
        print(f"Error downloading {filename}: {e}")
        return False

def extract_tables_from_pdf(pdf_path):
    """Extract tables from PDF using tabula-py."""
    try:
        print(f"Extracting tables from {pdf_path}...")
        # Try to extract tables using tabula-py
        tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True)
        print(f"Found {len(tables)} tables using tabula-py")
        return tables
    except Exception as e:
        print(f"Error extracting tables with tabula-py: {e}")
        return []

def extract_text_from_pdf(pdf_path):
    """Extract text from PDF using pdfplumber as fallback."""
    try:
        print(f"Extracting text from {pdf_path} using pdfplumber...")
        text_data = []
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text()
                if text:
                    text_data.append({
                        'page': page_num + 1,
                        'text': text
                    })
        return text_data
    except Exception as e:
        print(f"Error extracting text with pdfplumber: {e}")
        return []

def parse_text_to_candidates(text_data):
    """Parse extracted text to find candidate information."""
    candidates = []
    
    for page_data in text_data:
        text = page_data['text']
        lines = text.split('\n')
        
        current_candidate = {}
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Look for patterns that indicate candidate information
            # This is a simplified parser - you may need to adjust based on actual PDF format
            if re.search(r'[A-Z][a-z]+ [A-Z][a-z]+', line):  # Name pattern
                if current_candidate and 'name' in current_candidate:
                    # Save previous candidate
                    if is_valid_candidate(current_candidate.get('name', ''), current_candidate.get('office', '')):
                        candidates.append(current_candidate)
                    current_candidate = {}
                
                current_candidate['name'] = clean_text(line)
            elif re.search(r'(?:for|running for|candidate for)', line, re.IGNORECASE):
                current_candidate['office'] = clean_text(line)
            elif re.search(r'(?:party|democrat|republican|independent|libertarian)', line, re.IGNORECASE):
                current_candidate['party'] = clean_text(line)
        
        # Don't forget the last candidate
        if current_candidate and 'name' in current_candidate:
            if is_valid_candidate(current_candidate.get('name', ''), current_candidate.get('office', '')):
                candidates.append(current_candidate)
    
    return candidates

def find_best_column(columns, keywords):
    """Find the best matching column for a set of keywords using fuzzy matching."""
    for keyword in keywords:
        for col in columns:
            if keyword in str(col).lower():
                return col
    # Fallback: use difflib for fuzzy matching
    matches = difflib.get_close_matches(keywords[0], [str(c).lower() for c in columns], n=1, cutoff=0.7)
    if matches:
        for col in columns:
            if matches[0] in str(col).lower():
                return col
    return None

def extract_party(text):
    """Extract party from a string using common party names."""
    if not text:
        return ''
    parties = [
        'Republican', 'Democratic', 'Democrat', 'Libertarian', 'Independent', 'Green', 'Constitution',
        'Nonpartisan', 'Write-In', 'Unaffiliated', 'Other'
    ]
    for party in parties:
        if party.lower() in text.lower():
            return party
    return ''

def split_name_office(text):
    """Split a string like 'John Doe - Judge, District 5' into name and office."""
    if not text or '-' not in text:
        return text, ''
    parts = text.split('-', 1)
    name = parts[0].strip()
    office = parts[1].strip()
    return name, office

def scrape_indiana_candidates():
    """Main function to scrape Indiana candidate information."""
    # Ensure raw output directory exists
    raw_dir = os.path.join('data', 'raw')
    os.makedirs(raw_dir, exist_ok=True)
    base_url = "https://www.in.gov/sos/elections/candidate-information/"
    
    try:
        print("Initializing Indiana candidate scraper...")
        
        # Create session for requests
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        print("Fetching main page...")
        response = session.get(base_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all PDF links for candidate information
        pdf_links = []
        
        # Look for links containing candidate information
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Focus specifically on candidate lists, not guides or calendars
            if ('.pdf' in href.lower() and 
                any(keyword in text.lower() for keyword in ['candidate', 'election']) and
                not any(exclude in text.lower() for exclude in ['guide', 'calendar', 'brochure'])):
                
                full_url = urljoin(base_url, href)
                pdf_links.append({
                    'url': full_url,
                    'title': text,
                    'filename': os.path.basename(href)
                })
        
        print(f"Found {len(pdf_links)} candidate PDF files")
        
        all_candidates = []
        
        for pdf_info in pdf_links:
            print(f"\nProcessing: {pdf_info['title']}")
            
            # Download PDF
            pdf_filename = os.path.join(script_dir, pdf_info['filename'])
            if not download_pdf(pdf_info['url'], pdf_filename):
                continue
            
            # Extract year and election type from title
            year_match = re.search(r'(\d{4})', pdf_info['title'])
            year = year_match.group(1) if year_match else "Unknown"
            
            election_type = "Primary" if "primary" in pdf_info['title'].lower() else "General"
            
            # Try to extract tables first
            tables = extract_tables_from_pdf(pdf_filename)
            
            if tables:
                # Process tables
                for table_idx, table in enumerate(tables):
                    if table.empty:
                        continue
                    
                    print(f"Processing table {table_idx + 1} with {len(table)} rows")
                    
                    # Try to identify columns
                    columns = table.columns.tolist()
                    
                    # Debug: Print column names for first few tables
                    if table_idx < 5:
                        print(f"  Table {table_idx + 1} columns: {columns}")
                        # Print first few rows to understand data structure
                        print(f"  Table {table_idx + 1} first 3 rows:")
                        for i in range(min(3, len(table))):
                            print(f"    Row {i}: {table.iloc[i].tolist()}")

                    # Check if first row contains headers
                    if len(table) > 0:
                        first_row = table.iloc[0].tolist()
                        if 'BALLOT NAME' in str(first_row) or 'PARTY' in str(first_row) or 'FILED DATE' in str(first_row):
                            # Use first row as headers
                            header_row = first_row
                            print(f"    Using first row as headers: {header_row}")
                            

                            
                            # Map columns based on headers
                            name_col = None
                            party_col = None
                            office_col = None
                            date_filed_col = None
                            
                            for i, header in enumerate(header_row):
                                header_str = str(header).upper()
                                if 'BALLOT NAME' in header_str or 'NAME' in header_str:
                                    name_col = columns[i]
                                elif 'PARTY' in header_str:
                                    party_col = columns[i]
                                elif 'OFFICE TITLE' in header_str or 'OFFICE' in header_str:
                                    office_col = columns[i]
                                elif 'FILED DATE' in header_str or 'DATE' in header_str:
                                    date_filed_col = columns[i]
                            
                            # Skip the header row in processing
                            start_row = 1
                        else:
                            # Use improved matching for tables without clear headers
                            name_col = find_best_column(columns, ['name', 'candidate'])
                            office_col = find_best_column(columns, ['office', 'position', 'seat'])
                            party_col = find_best_column(columns, ['party', 'political party'])
                            date_filed_col = find_best_column(columns, ['date filed', 'filing date', 'filed', 'date', 'filing'])
                            start_row = 0
                            
                            # If we have unnamed columns, try to identify them by content
                            if not name_col or not office_col or not party_col or not date_filed_col:
                                unnamed_cols = [col for col in columns if 'unnamed' in str(col).lower()]
                                if unnamed_cols:
                                    # Analyze the content of unnamed columns to determine their purpose
                                    for col in unnamed_cols:
                                        col_data = table[col].dropna().astype(str)
                                        if len(col_data) > 0:
                                            # Check for date patterns
                                            date_patterns = col_data.str.contains(r'\d{1,2}/\d{1,2}/\d{2,4}', na=False)
                                            if date_patterns.sum() > len(col_data) * 0.3 and not date_filed_col:
                                                date_filed_col = col
                                                print(f"    Found date column: {col}")
                                            
                                            # Check for party patterns
                                            party_patterns = col_data.str.contains(r'(?:Republican|Democratic|Democrat|Libertarian|Independent|Green)', na=False)
                                            if party_patterns.sum() > len(col_data) * 0.2 and not party_col:
                                                party_col = col
                                                print(f"    Found party column: {col}")
                                            
                                            # Check for name patterns (first letter capitalized, contains spaces)
                                            name_patterns = col_data.str.contains(r'^[A-Z][a-z]+ [A-Z][a-z]+', na=False)
                                            if name_patterns.sum() > len(col_data) * 0.3 and not name_col:
                                                name_col = col
                                                print(f"    Found name column: {col}")
                    else:
                        start_row = 0
                        name_col = None
                        office_col = None
                        party_col = None
                        date_filed_col = None
                    
                    # Debug: Print column matches for first few tables
                    if table_idx < 5:
                        print(f"  Table {table_idx + 1} matches - Name: {name_col}, Office: {office_col}, Party: {party_col}, Date: {date_filed_col}")

                    # If only one column, try to split it
                    if not name_col and not office_col and len(columns) == 1:
                        name_col = columns[0]
                        office_col = None  # will split below

                    for idx, row in table.iterrows():
                        # Skip header rows
                        if idx < start_row:
                            continue

                        name = clean_text(str(row.get(name_col, ''))) if name_col else ''
                        office = clean_text(str(row.get(office_col, ''))) if office_col else ''
                        party = clean_text(str(row.get(party_col, ''))) if party_col else ''
                        date_filed = clean_text(str(row.get(date_filed_col, ''))) if date_filed_col else ''

                        # If name and office are combined, split them
                        if name and not office and '-' in name:
                            name, office = split_name_office(name)

                        # Fallback: try to extract party from name or office if not found
                        if not party:
                            party = extract_party(name) or extract_party(office)

                        # Additional filtering for table data
                        if not name or name.lower() in ['name', 'candidate', 'office', 'position', 'party']:
                            continue
                        if not office or office.lower() in ['office', 'position', 'title', 'seat']:
                            continue
                        if name.lower() == office.lower():
                            continue
                        if is_valid_candidate(name, office):
                            candidate_data = {
                                'Year': year,
                                'Election': election_type,
                                'Name': name,
                                'Office': office,
                                'Party': party,
                                'Date Filed': date_filed,
                                'Source': pdf_info['title']
                            }
                            all_candidates.append(candidate_data)
            else:
                # Fallback to text extraction
                print("No tables found, trying text extraction...")
                text_data = extract_text_from_pdf(pdf_filename)
                
                if text_data:
                    candidates = parse_text_to_candidates(text_data)
                    for candidate in candidates:
                        candidate.update({
                            'Year': year,
                            'Election': election_type,
                            'Source': pdf_info['title']
                        })
                        all_candidates.extend([candidate])
            
            # Clean up downloaded PDF
            try:
                os.remove(pdf_filename)
                print(f"Cleaned up {pdf_filename}")
            except:
                pass
            
            # Small delay between downloads
            time.sleep(1)
        
        if not all_candidates:
            print("No candidates found!")
            return
        
        print(f"\nProcessing {len(all_candidates)} candidates...")
        
        # Create DataFrame and clean up
        df = pd.DataFrame(all_candidates)
        # Remove District if present
        if 'District' in df.columns:
            df = df.drop(columns=['District'])
        # Ensure Date Filed is present
        if 'Date Filed' not in df.columns:
            df['Date Filed'] = ''
        df = df.drop_duplicates(subset=['Name', 'Office', 'Year', 'Election'])
        df = df.sort_values(['Year', 'Name'], ascending=[False, True])
        
        # Create Excel file
        print("Creating Excel file...")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_file = f'indiana_candidates_{timestamp}.xlsx'
        excel_path = os.path.join(raw_dir, excel_file)
        
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Candidates')
            
            # Format the Excel file
            workbook = writer.book
            worksheet = writer.sheets['Candidates']
            
            # Style headers
            header_font = openpyxl.styles.Font(bold=True, size=12)
            header_fill = openpyxl.styles.PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
            header_font_color = openpyxl.styles.Font(color='FFFFFF')
            
            for cell in worksheet[1]:
                cell.font = header_font
                cell.fill = header_fill
                cell.font = header_font_color
            
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
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # Add auto-filter
            worksheet.auto_filter.ref = worksheet.dimensions
        
        print(f"\nSuccessfully scraped {len(df)} candidates from Indiana")
        print(f"Created Excel file: {excel_file}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    scrape_indiana_candidates() 