import asyncio
import re
import pandas as pd
from playwright.async_api import async_playwright
import os

OUTPUT_FILE = os.path.join('data', 'raw', 'alaska_candidates.xlsx')
URL = 'https://www.elections.alaska.gov/candidates/?election=24genr'

# Normalize party names
PARTY_MAP = {
    'republican': 'Republican',
    'democrat': 'Democrat',
    'libertarian': 'Libertarian',
    'nonpartisan': 'Nonpartisan',
    'undeclared': 'Undeclared',
}

def normalize_party(party_str):
    if not party_str:
        return ''
    party_str = party_str.lower()
    for key, val in PARTY_MAP.items():
        if key in party_str:
            return val
    return party_str.title()

async def scrape_alaska():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(URL)
        await page.wait_for_selector('#field_stebs')

        # Get all election options (skip empty placeholder)
        elections = await page.query_selector_all('#field_stebs option')
        election_options = []
        for opt in elections:
            value = await opt.get_attribute('value')
            text = (await opt.inner_text()).strip()
            if value and text:
                election_options.append((value, text))

        all_rows = []
        for value, election_name in election_options:
            election_candidate_count = 0
            # Select the election
            await page.select_option('#field_stebs', value=value)
            
            # Click the search button to load the election data
            search_button = await page.query_selector('button.frm_button_submit.frm_final_submit')
            if search_button:
                # Wait for navigation after clicking search
                async with page.expect_navigation():
                    await search_button.click()
                # Wait for the page to load the new election data
                await page.wait_for_selector('h4')
                await asyncio.sleep(1)  # Give time for content to load
            else:
                print(f"Warning: No search button found for election {election_name}")
                continue

            # Get all office sections
            office_headers = await page.query_selector_all('h4')
            print(f"Processing {election_name}: Found {len(office_headers)} office sections")
            
            for office_header in office_headers:
                office = (await office_header.inner_text()).strip()
                # Use Playwright's locator API to get the first following sibling table
                table_locator = page.locator('h4', has_text=office).locator('xpath=following-sibling::table[1]')
                if not await table_locator.count():
                    continue
                row_locators = table_locator.locator('tbody > tr')
                row_count = await row_locators.count()
                for i in range(row_count):
                    row = row_locators.nth(i)
                    tds = await row.locator('td').all()
                    if len(tds) < 3:
                        continue
                    # Name, Party
                    name_html = await tds[0].inner_html()
                    name_match = re.search(r'<strong>(.*?)</strong>', name_html, re.DOTALL)
                    name = name_match.group(1).strip() if name_match else ''
                    party_match = re.search(r'<small>\((.*?)\)', name_html)
                    party = normalize_party(party_match.group(1).strip() if party_match else '')
                    # Address
                    address_html = await tds[1].inner_html()
                    address = re.sub(r'<.*?>', '', address_html).replace('\n', ' ').replace('  ', ' ').strip()
                    # Contact
                    contact_html = await tds[2].inner_html()
                    phone_match = re.search(r'(\(\d{3}\)\s*\d{3}-\d{4})', contact_html)
                    phone = phone_match.group(1) if phone_match else ''
                    email_match = re.search(r'Email:\s*<a[^>]*>([^<]+)</a>', contact_html, re.IGNORECASE)
                    email = email_match.group(1).strip() if email_match else ''
                    website_match = re.search(r'Website:.*?<a[^>]*>([^<]+)</a>', contact_html, re.IGNORECASE)
                    website = website_match.group(1).strip() if website_match else ''
                    all_rows.append({
                        'Election': election_name,
                        'Office': office,
                        'Name': name,
                        'Party': party,
                        'Address': address,
                        'Email': email,
                        'Website': website,
                        'Phone Number': phone,
                    })
                    election_candidate_count += 1
            print(f"  Scraped {election_candidate_count} candidates for {election_name}")
        await browser.close()
    # Output to Excel
    df = pd.DataFrame(all_rows)
    os.makedirs(os.path.join('data', 'raw'), exist_ok=True)
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"Scraped {len(df)} candidates. Output written to {OUTPUT_FILE}")

if __name__ == '__main__':
    asyncio.run(scrape_alaska()) 