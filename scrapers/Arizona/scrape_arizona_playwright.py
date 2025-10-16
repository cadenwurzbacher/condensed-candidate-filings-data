import asyncio
import time
import json
import random
import re
from datetime import datetime
import os
import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

def clean_text(text):
    """Clean up text by removing extra whitespace and newlines"""
    return ' '.join(text.strip().split())

def format_phone(phone_text):
    """Format phone number"""
    # Remove all non-digit characters
    phone = re.sub(r'[^\d]', '', phone_text)
    if len(phone) == 10:
        return f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
    elif len(phone) == 11 and phone.startswith('1'):
        return f"({phone[1:4]}) {phone[4:7]}-{phone[7:]}"
    return phone_text

async def scrape_arizona_vote_playwright():
    base_url = "https://apps.arizona.vote/electioninfo"
    election_url = f"{base_url}/Election/47"
    
    print("Setting up Playwright scraper...")
    
    async with async_playwright() as p:
        # Launch browser with stealth options
        browser = await p.chromium.launch(
            headless=True,  # Changed to headless for better stability
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu'
            ]
        )
        
        # Create context with realistic viewport and user agent
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
        )
        
        # Create page
        page = await context.new_page()
        
        try:
            # Add a delay to appear more human-like
            print("Adding initial delay...")
            await asyncio.sleep(random.uniform(2, 4))
            
            # First navigate to the main page
            print("Navigating to main election page...")
            await page.goto(base_url, wait_until='domcontentloaded')
            await asyncio.sleep(random.uniform(3, 5))
            print("Main page loaded successfully")
            
            # Then navigate to the specific election
            print(f"Navigating to election page: {election_url}")
            await page.goto(election_url, wait_until='domcontentloaded')
            await asyncio.sleep(random.uniform(3, 5))
            print("Election page loaded successfully")
            
            # Wait for content to load
            await page.wait_for_load_state('domcontentloaded')
            
            # Get the page content
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Debug: Print page title and first 500 characters of HTML
            print(f"[DEBUG] Page title: {soup.title.text if soup.title else 'No title'}")
            print(f"[DEBUG] First 500 chars of HTML: {str(soup)[:500]}")
            
            # Initialize dictionary to store scraped data
            election_data = {
                'scrape_timestamp': datetime.now().isoformat(),
                'url': election_url,
                'election_name': soup.title.text if soup.title else "Unknown Election",
                'candidates': {}
            }
            
            # Find all office headers
            print("\nLooking for office headers...")
            office_headers = soup.find_all('h3')
            print(f"Found {len(office_headers)} office headers")
            
            for i, header in enumerate(office_headers):
                try:
                    office_name = clean_text(header.text)
                    if not office_name:
                        continue
                        
                    print(f"\nProcessing office {i+1}/{len(office_headers)}: {office_name}")
                    election_data['candidates'][office_name] = []
                    
                    # Find all candidate divs in the next row
                    candidate_divs = header.find_next('div', class_='row').find_all('div', class_='col-md-4 mb-3')
                    print(f"Found {len(candidate_divs)} candidates")
                    
                    for j, candidate_div in enumerate(candidate_divs):
                        try:
                            # Get candidate name and party
                            name_header = candidate_div.find('h4')
                            if not name_header:
                                continue
                                
                            name_text = name_header.text.strip()
                            name_match = re.match(r'(.*?)\s*\((.*?)\)', name_text)
                            if not name_match:
                                continue
                                
                            name = name_match.group(1).strip()
                            party = name_match.group(2).strip()
                            
                            print(f"Processing candidate {j+1}/{len(candidate_divs)}: {name}")
                            
                            # Initialize candidate data
                            candidate_data = {
                                'name': name,
                                'party': party,
                                'contact_info': {}
                            }
                            
                            # Get contact info
                            info_div = candidate_div.find('div', class_='card card-body candidate-info')
                            if info_div:
                                # Get phone
                                phone_elem = info_div.find('p', string=re.compile('Phone Number:'))
                                if phone_elem:
                                    phone_text = phone_elem.text.replace('Phone Number:', '').strip()
                                    if phone_text:
                                        candidate_data['contact_info']['phone'] = format_phone(phone_text)
                                        print(f"Found phone for {name}: {phone_text}")
                                
                                # Get email
                                email_elem = info_div.find('p', string=re.compile('Email Address:'))
                                if email_elem:
                                    email_link = email_elem.find('a')
                                    if email_link and email_link.get('title', '').startswith('Link to mailto:'):
                                        email = email_link['title'].replace('Link to mailto:', '').strip()
                                        candidate_data['contact_info']['email'] = email
                                        print(f"Found email for {name}: {email}")
                                
                                # Get website and social media
                                for link in info_div.find_all('a'):
                                    href = link.get('href')
                                    if not href:
                                        continue
                                        
                                    text = link.text.strip().lower()
                                    if 'website' in text:
                                        candidate_data['contact_info']['website'] = href
                                        print(f"Found website for {name}: {href}")
                                    elif 'facebook' in text or 'fb.com' in href:
                                        candidate_data['contact_info']['facebook'] = href
                                        print(f"Found Facebook for {name}: {href}")
                                    elif 'twitter' in text or 'x.com' in href:
                                        candidate_data['contact_info']['twitter'] = href
                                        print(f"Found Twitter for {name}: {href}")
                            
                            election_data['candidates'][office_name].append(candidate_data)
                            
                        except Exception as e:
                            print(f"Error processing candidate: {e}")
                except Exception as e:
                    print(f"Error processing office: {e}")
            
            # Remove any empty offices
            election_data['candidates'] = {k: v for k, v in election_data['candidates'].items() 
                                         if v}
            
            # Convert to Excel format
            print("\nConverting data to Excel format...")
            candidates_list = []
            
            # Iterate through offices and candidates
            for office, candidates in election_data['candidates'].items():
                for candidate in candidates:
                    # Create a flattened dictionary for each candidate
                    candidate_dict = {
                        'Office': office,
                        'Candidate Name': candidate['name'],
                        'Party': candidate['party'],
                        'Phone': candidate['contact_info'].get('phone', ''),
                        'Email': candidate['contact_info'].get('email', ''),
                        'Website': candidate['contact_info'].get('website', ''),
                        'Facebook': candidate['contact_info'].get('facebook', ''),
                        'Twitter': candidate['contact_info'].get('twitter', '')
                    }
                    candidates_list.append(candidate_dict)
            
            # Check if we found any candidates
            if not candidates_list:
                print("No candidates found. The website may be blocking automated access.")
                print("This could be due to Cloudflare protection or the page structure has changed.")
                return
            
            # Create DataFrame
            df = pd.DataFrame(candidates_list)
            
            # Sort by Office and Party
            df = df.sort_values(['Office', 'Party', 'Candidate Name'])
            
            # Generate filename and ensure raw output directory exists
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            raw_dir = os.path.join('data', 'raw')
            os.makedirs(raw_dir, exist_ok=True)
            filename = os.path.join(raw_dir, 'arizona_candidates_playwright.xlsx')
            
            # Create Excel writer with formatting
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Write the data
                df.to_excel(writer, index=False, sheet_name='Candidates')
                
                # Auto-adjust column widths
                worksheet = writer.sheets['Candidates']
                for idx, col in enumerate(df.columns):
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(col)
                    )
                    # Add a little extra space
                    worksheet.column_dimensions[chr(65 + idx)].width = min(max_length + 2, 50)
            
            print(f"\nSpreadsheet created: {filename}")
            print(f"Total candidates: {len(candidates_list)}")
            print(f"Total offices: {len(election_data['candidates'])}")
            
        except Exception as e:
            print(f"An error occurred: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            # Always close the browser
            await browser.close()

if __name__ == "__main__":
    asyncio.run(scrape_arizona_vote_playwright()) 