import os
import sys
import logging
import subprocess
import signal
import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import cloudscraper
from playwright.sync_api import sync_playwright
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Sleep prevention
caffeinate_process = None

def prevent_sleep():
    global caffeinate_process
    try:
        caffeinate_process = subprocess.Popen(['caffeinate', '-i'])
        logging.info("Computer sleep prevention activated")
    except Exception as e:
        logging.warning(f"Could not prevent sleep: {e}")

def allow_sleep():
    global caffeinate_process
    if caffeinate_process:
        try:
            caffeinate_process.terminate()
            logging.info("Computer sleep prevention deactivated")
        except Exception as e:
            logging.warning(f"Could not deactivate sleep prevention: {e}")

def signal_handler(signum, frame):
    logging.info("Received interrupt signal, cleaning up...")
    allow_sleep()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def cleanup_old_files(script_dir, file_pattern, keep=2):
    import glob
    old_files = glob.glob(os.path.join(script_dir, file_pattern))
    old_files.sort(key=lambda x: os.path.getmtime(x))
    files_to_delete = old_files[:-keep]
    for old_file in files_to_delete:
        try:
            os.remove(old_file)
            logging.info(f"Deleted old file: {os.path.basename(old_file)}")
        except Exception as e:
            logging.warning(f"Could not delete {old_file}: {e}")

def save_progress(candidates, script_dir, progress_name):
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        cleanup_old_files(script_dir, "progress_*.xlsx", keep=2)
        if candidates:
            df = pd.DataFrame(candidates)
            progress_filename = f'progress_{progress_name}_{timestamp}.xlsx'
            progress_path = os.path.join(script_dir, progress_filename)
            with pd.ExcelWriter(progress_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Progress')
            logging.info(f"Saved progress: {progress_filename} with {len(candidates)} candidates")
    except Exception as e:
        logging.error(f"Error saving progress: {e}")

def fetch_election_options():
    url = "https://voter.votewa.gov/CandidateList.aspx?e=894"
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    select = soup.find('select', {'id': 'ddlElection'})
    options = select.find_all('option')
    election_list = []
    for opt in options:
        value = opt.get('value', '').strip()
        label = opt.text.strip()
        if not value or not label:
            continue
        election_list.append({'value': value, 'label': label})
    return election_list

def scrape_general_2025(script_dir):
    election_list = fetch_election_options()
    print("Parsed election labels:")
    for e in election_list:
        print(f"Label: '{e['label']}' | Value: {e['value']}")
    # Robust search: match labels that start with "general 2025"
    def normalize(s):
        return ' '.join(s.lower().strip().split())
    target = normalize('GENERAL 2025')
    print(f"Normalized target: '{target}'")
    general_2025 = None
    for e in election_list:
        norm_label = normalize(e['label'])
        print(f"Normalized label: '{norm_label}'")
        if norm_label.startswith(target):
            print(f"Match found: '{e['label']}'")
            general_2025 = e
            break
    if not general_2025:
        print("Could not find GENERAL 2025 in dropdown!")
        return
    election_value = general_2025['value']
    # Simulate selecting the dropdown (POST with __EVENTTARGET)
    url = "https://voter.votewa.gov/CandidateList.aspx?e=894"
    # Use cloudscraper instead of requests
    session = cloudscraper.create_scraper()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }
    # Get initial page to get VIEWSTATE and EVENTVALIDATION
    r = session.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    print("First 500 characters of HTML response:")
    print(r.text[:500])
    viewstate_input = soup.find('input', {'id': '__VIEWSTATE'})
    if not viewstate_input:
        print("Could not find __VIEWSTATE input!")
        print("First 2000 characters of HTML response:")
        print(r.text[:2000])
        return
    viewstate = viewstate_input['value']
    eventvalidation_input = soup.find('input', {'id': '__EVENTVALIDATION'})
    if not eventvalidation_input:
        print("Could not find __EVENTVALIDATION input!")
        print("First 2000 characters of HTML response:")
        print(r.text[:2000])
        return
    eventvalidation = eventvalidation_input['value']
    viewstategen_input = soup.find('input', {'id': '__VIEWSTATEGENERATOR'})
    if viewstategen_input:
        viewstategen = viewstategen_input['value']
    else:
        print("Proceeding without __VIEWSTATEGENERATOR input!")
        viewstategen = None
    data = {
        '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddlElection',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': viewstate,
        '__EVENTVALIDATION': eventvalidation,
        'ctl00$ContentPlaceHolder1$ddlElection': election_value
    }
    if viewstategen is not None:
        data['__VIEWSTATEGENERATOR'] = viewstategen
    # POST to select the election
    r2 = session.post(url, data=data, headers=headers)
    soup2 = BeautifulSoup(r2.text, 'html.parser')
    # Find the candidate table (updated ID)
    table = soup2.find('table', {'id': 'ctl00_ContentPlaceHolder1_grdCandidates_ctl00'})
    if not table:
        print("Could not find candidate table for GENERAL 2025!")
        print("First 2000 characters of HTML response:")
        print(r2.text[:2000])
        return
    # Parse table headers
    headers = [th.text.strip() for th in table.find_all('th')]
    print(f"Table headers: {headers}")
    # Parse rows
    candidates = []
    for row in table.find_all('tr'):
        cells = row.find_all('td')
        if not cells or len(cells) < 14:
            continue
        candidate = {
            'district_type': cells[0].text.strip(),
            'district': cells[1].text.strip(),
            'race': cells[2].text.strip(),
            'term_type': cells[3].text.strip(),
            'term_length': cells[4].text.strip(),
            'name': cells[5].text.strip(),
            'mailing_address': cells[6].text.strip(),
            'email': cells[7].text.strip(),
            'phone': cells[8].text.strip(),
            'filing_date': cells[9].text.strip(),
            'party_preference': cells[10].text.strip(),
            'status': cells[11].text.strip(),
            'election_status': cells[12].text.strip(),
            'ballot_order': cells[13].text.strip(),
            'election': general_2025['label']
        }
        candidates.append(candidate)
    # Save to Excel
    df = pd.DataFrame(candidates)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_filename = f'washington_candidates_2025_{timestamp}.xlsx'
    excel_path = os.path.join(script_dir, excel_filename)
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Candidates')
    print(f"Saved {len(candidates)} candidates to {excel_filename}")

def scrape_general_2025_playwright(script_dir):
    import pandas as pd
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        url = "https://voter.votewa.gov/CandidateList.aspx?e=894"
        page.goto(url)
        # Wait for dropdown
        page.wait_for_selector('#ddlElection')
        # Select GENERAL 2025
        options = page.query_selector_all('#ddlElection option')
        target_label = 'GENERAL 2025'
        found = False
        for opt in options:
            label = opt.inner_text().strip().lower()
            if label.startswith(target_label.lower()):
                value = opt.get_attribute('value')
                page.select_option('#ddlElection', value)
                found = True
                break
        if not found:
            print("Could not find GENERAL 2025 in dropdown!")
            browser.close()
            return
        # Wait for table to load (table id may change, so use partial match)
        page.wait_for_selector('table[id*=grdCandidates]')
        table = page.query_selector('table[id*=grdCandidates]')
        if not table:
            print("Could not find candidate table for GENERAL 2025!")
            browser.close()
            return
        # Parse headers
        headers = [th.inner_text().strip() for th in table.query_selector_all('th')]
        print(f"Table headers: {headers}")
        # Parse rows
        candidates = []
        for row in table.query_selector_all('tr'):
            cells = row.query_selector_all('td')
            if not cells or len(cells) < 14:
                continue
            candidate = {
                'district_type': cells[0].inner_text().strip(),
                'district': cells[1].inner_text().strip(),
                'race': cells[2].inner_text().strip(),
                'term_type': cells[3].inner_text().strip(),
                'term_length': cells[4].inner_text().strip(),
                'name': cells[5].inner_text().strip(),
                'mailing_address': cells[6].inner_text().strip(),
                'email': cells[7].inner_text().strip(),
                'phone': cells[8].inner_text().strip(),
                'filing_date': cells[9].inner_text().strip(),
                'party_preference': cells[10].inner_text().strip(),
                'status': cells[11].inner_text().strip(),
                'election_status': cells[12].inner_text().strip(),
                'ballot_order': cells[13].inner_text().strip(),
                'election': target_label
            }
            candidates.append(candidate)
        # Save to Excel
        df = pd.DataFrame(candidates)
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_filename = f'washington_candidates_2025_{timestamp}.xlsx'
        excel_path = os.path.join(script_dir, excel_filename)
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Candidates')
        print(f"Saved {len(candidates)} candidates to {excel_filename}")
        browser.close()

def scrape_all_elections_playwright(script_dir):
    import pandas as pd
    from datetime import datetime
    import re
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        url = "https://voter.votewa.gov/CandidateList.aspx?e=894"
        page.goto(url)
        page.wait_for_selector('#ddlElection')
        options = page.query_selector_all('#ddlElection option')
        # Build election list in dropdown order
        election_list = []
        for opt in options:
            value = opt.get_attribute('value')
            label = opt.inner_text().strip()
            if not value or not label:
                continue
            election_list.append({'value': value, 'label': label})
        # Find start and stop indices
        start_label = 'GENERAL 2025'
        stop_label = 'Primary (08/19/2008)'
        def norm(s): return ' '.join(s.lower().strip().split())
        start_idx = next((i for i, e in enumerate(election_list) if norm(e['label']).startswith(norm(start_label))), 0)
        stop_idx = next((i for i, e in enumerate(election_list) if norm(e['label']).startswith(norm(stop_label))), len(election_list)-1)
        # Accumulate all candidates in a single list
        all_candidates = []
        for i, election in enumerate(election_list[start_idx:stop_idx+1], start=start_idx):
            print(f"Scraping: {election['label']} | Value: {election['value']}")
            page.select_option('#ddlElection', election['value'])
            try:
                page.wait_for_selector('table[id*=grdCandidates]', timeout=10000)
            except Exception:
                print(f"No candidate table for {election['label']}, skipping.")
                continue
            table = page.query_selector('table[id*=grdCandidates]')
            if not table:
                print(f"Could not find candidate table for {election['label']}! Skipping.")
                continue
            headers = [th.inner_text().strip() for th in table.query_selector_all('th')]
            print(f"Table headers: {headers}")
            for row in table.query_selector_all('tr'):
                cells = row.query_selector_all('td')
                if not cells or len(cells) < 14:
                    continue
                candidate = {
                    'district_type': cells[0].inner_text().strip(),
                    'district': cells[1].inner_text().strip(),
                    'race': cells[2].inner_text().strip(),
                    'term_type': cells[3].inner_text().strip(),
                    'term_length': cells[4].inner_text().strip(),
                    'name': cells[5].inner_text().strip(),
                    'mailing_address': cells[6].inner_text().strip(),
                    'email': cells[7].inner_text().strip(),
                    'phone': cells[8].inner_text().strip(),
                    'filing_date': cells[9].inner_text().strip(),
                    'party_preference': cells[10].inner_text().strip(),
                    'status': cells[11].inner_text().strip(),
                    'election_status': cells[12].inner_text().strip(),
                    'ballot_order': cells[13].inner_text().strip(),
                    'election': election['label']
                }
                all_candidates.append(candidate)
                # Save progress every 1000 candidates
                if len(all_candidates) % 1000 == 0:
                    save_progress(all_candidates, script_dir, "all_elections")
        # Save all candidates to one Excel file
        if all_candidates:
            df = pd.DataFrame(all_candidates)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            excel_filename = f'washington_candidates_all_{timestamp}.xlsx'
            excel_path = os.path.join(script_dir, excel_filename)
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Candidates')
            print(f"Saved {len(all_candidates)} candidates to {excel_filename}")
        else:
            print("No candidates found to save.")
        browser.close()

def main():
    prevent_sleep()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        scrape_all_elections_playwright(script_dir)
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        allow_sleep()

if __name__ == "__main__":
    main() 