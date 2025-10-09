import asyncio
import time
import json
import random
import re
from datetime import datetime
import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import requests

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

async def try_alternative_sources():
    """Try alternative Arizona election data sources"""
    
    # List of alternative URLs to try
    alternative_urls = [
        "https://azsos.gov/elections/voting-election/candidate-information",
        "https://www.azcleanelections.gov/en/arizona-elections",
        "https://apps.azsos.gov/election/candidates",
        "https://www.azsos.gov/elections/voting-election",
        "https://azsos.gov/elections"
    ]
    
    print("Trying alternative Arizona election data sources...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = await context.new_page()
        
        candidates_list = []
        
        for url in alternative_urls:
            try:
                print(f"\nTrying URL: {url}")
                await page.goto(url, wait_until='domcontentloaded', timeout=30000)
                await asyncio.sleep(3)
                
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                print(f"Page title: {soup.title.text if soup.title else 'No title'}")
                
                # Look for candidate information in various formats
                # This is a generic approach to find candidate data
                
                # Look for tables with candidate data
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows[1:]:  # Skip header
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 2:
                            candidate_name = clean_text(cells[0].get_text())
                            if candidate_name and len(candidate_name) > 2:
                                candidates_list.append({
                                    'Source': url,
                                    'Candidate Name': candidate_name,
                                    'Office': clean_text(cells[1].get_text()) if len(cells) > 1 else '',
                                    'Party': clean_text(cells[2].get_text()) if len(cells) > 2 else '',
                                    'Phone': '',
                                    'Email': '',
                                    'Website': '',
                                    'Facebook': '',
                                    'Twitter': ''
                                })
                
                # Look for candidate cards or divs
                candidate_divs = soup.find_all(['div', 'article'], class_=re.compile(r'candidate|election|vote', re.I))
                for div in candidate_divs:
                    # Look for names in headers
                    headers = div.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                    for header in headers:
                        text = clean_text(header.get_text())
                        if text and len(text) > 2 and any(word in text.lower() for word in ['candidate', 'election', 'vote']):
                            candidates_list.append({
                                'Source': url,
                                'Candidate Name': text,
                                'Office': '',
                                'Party': '',
                                'Phone': '',
                                'Email': '',
                                'Website': '',
                                'Facebook': '',
                                'Twitter': ''
                            })
                
            except Exception as e:
                print(f"Error with {url}: {e}")
                continue
        
        await browser.close()
        
        if candidates_list:
            # Create DataFrame
            df = pd.DataFrame(candidates_list)
            
            # Generate filename
            filename = f'Arizona/arizona_candidates_alternative.xlsx'
            
            # Create Excel writer
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Candidates')
                
                # Auto-adjust column widths
                worksheet = writer.sheets['Candidates']
                for idx, col in enumerate(df.columns):
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(col)
                    )
                    worksheet.column_dimensions[chr(65 + idx)].width = min(max_length + 2, 50)
            
            print(f"\nAlternative data found and saved to: {filename}")
            print(f"Total entries: {len(candidates_list)}")
        else:
            print("\nNo alternative data sources found.")
            print("The Arizona elections website appears to have strong anti-bot protection.")
            print("Consider:")
            print("1. Manual data collection from the website")
            print("2. Contacting the Arizona Secretary of State for official data")
            print("3. Using public records requests for candidate information")

async def main():
    print("Arizona Election Data Scraper - Alternative Sources")
    print("=" * 50)
    
    await try_alternative_sources()

if __name__ == "__main__":
    asyncio.run(main()) 