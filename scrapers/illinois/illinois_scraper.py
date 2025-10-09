from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
import pandas as pd
import time
import os
import traceback
from bs4 import BeautifulSoup
import subprocess

def setup_driver():
    # Set up Chrome options
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument('--headless')  # Commenting out headless mode for debugging
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-popup-blocking')
    
    # Initialize the driver
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(30)  # Set page load timeout
    return driver

def get_election_links(driver):
    print("Getting list of all elections...")
    driver.get("https://www.elections.il.gov/ElectionOperations/CandidateOfficeFilingSearch.aspx#")
    time.sleep(3)
    
    # Get the page source and parse it with BeautifulSoup
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    
    # Find the elections table
    table = soup.find('table', {'id': 'ContentPlaceHolder1_gvElections'})
    if not table:
        print("Could not find elections table")
        return []
    
    # Find all rows except header
    rows = table.find_all('tr')[1:]  # Skip header row
    election_links = []
    
    # List of election types we're interested in
    target_elections = [
        "GENERAL PRIMARY",
        "GENERAL ELECTION",
        "PRIMARY",
        "SPECIAL PRIMARY",
        "SPECIAL ELECTION"
    ]
    
    for row in rows:
        try:
            cols = row.find_all('td')
            if len(cols) >= 3:
                # Get election name and link
                name_col = cols[0]
                name_link = name_col.find('a')
                if name_link:
                    election_name = name_link.text.strip()
                    
                    # Skip consolidated and local elections
                    if "CONSOLIDATED" in election_name.upper() or "LOCAL" in election_name.upper():
                        print(f"Skipping local/consolidated election: {election_name}")
                        continue
                    
                    # Check if this is an election type we're interested in
                    is_target_election = any(election_type in election_name.upper() for election_type in target_elections)
                    if not is_target_election:
                        print(f"Skipping non-target election: {election_name}")
                        continue
                    
                    election_url = name_link['href']
                    
                    # Get election date
                    date_col = cols[1]
                    election_date = date_col.text.strip()
                    
                    # Extract year from date
                    try:
                        year = election_date.split('/')[-1]
                        # Add year to election name if not already present
                        if year not in election_name:
                            election_name = f"{election_name} {year}"
                    except:
                        print(f"Could not extract year from date: {election_date}")
                    
                    if not election_url.startswith('http'):
                        election_url = f"https://www.elections.il.gov/ElectionOperations/{election_url}"
                    
                    election_links.append({
                        'name': election_name,
                        'date': election_date,
                        'url': election_url
                    })
                    print(f"Found target election: {election_name} on {election_date} - {election_url}")
        except Exception as e:
            print(f"Error processing election row: {str(e)}")
            continue
    
    print(f"Found {len(election_links)} target elections")
    return election_links

def get_candidate_details(driver, candidate_link):
    try:
        print(f"Visiting candidate link: {candidate_link}")
        driver.get(candidate_link)
        time.sleep(2)
        
        # Get page source for candidate details
        detail_source = driver.page_source
        detail_soup = BeautifulSoup(detail_source, 'html.parser')
        
        # Find email - updated to match actual HTML structure
        email_link = detail_soup.find('a', {'id': 'ContentPlaceHolder1_hypEmail'})
        email = email_link.text.strip() if email_link else ""
        
        # Find website - updated to match actual HTML structure
        website_link = detail_soup.find('a', {'id': 'ContentPlaceHolder1_hplWebsite'})
        website = website_link.text.strip() if website_link else ""
        
        print(f"Found email: {email}")
        print(f"Found website: {website}")
        
        return email, website
    except Exception as e:
        print(f"Error in get_candidate_details: {str(e)}")
        return "", ""

def recover_session(driver, election_url, max_retries=3):
    for attempt in range(max_retries):
        try:
            print(f"Attempting to recover session (Attempt {attempt + 1}/{max_retries})")
            driver.quit()  # Close the invalid session
            time.sleep(2)
            driver = setup_driver()  # Create new session
            driver.get(election_url)
            time.sleep(3)
            return driver
        except Exception as e:
            print(f"Session recovery attempt {attempt + 1} failed: {str(e)}")
            if attempt == max_retries - 1:
                raise Exception("Failed to recover session after multiple attempts")
            time.sleep(5)
    return driver

def scrape_office_type(driver, office_type, election_name, election_url, max_retries=3):
    for attempt in range(max_retries):
        try:
            print(f"\nStarting to scrape {office_type} for {election_name} (Attempt {attempt + 1}/{max_retries})")
            
            # Navigate back to the election page
            print(f"Navigating to election page: {election_url}")
            try:
                driver.get(election_url)
                time.sleep(5)  # Increased wait time
            except Exception as e:
                if "invalid session id" in str(e).lower():
                    print("Session invalid, attempting to recover...")
                    driver = recover_session(driver, election_url)
                else:
                    raise e
            
            # Select the office type from dropdown
            print(f"Selecting office type: {office_type}")
            office_dropdown = WebDriverWait(driver, 15).until(  # Increased wait time
                EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_ddlOfficeGroup"))
            )
            
            # Print all available options
            select = Select(office_dropdown)
            available_options = [option.text for option in select.options]
            print(f"Available office types: {available_options}")
            
            # Try to find the closest matching option
            matching_option = None
            for option in available_options:
                if office_type.lower() in option.lower():
                    matching_option = option
                    break
            
            if matching_option:
                print(f"Found matching option: {matching_option}")
                select.select_by_visible_text(matching_option)
            else:
                print(f"Could not find matching option for {office_type}")
                print("Skipping this office type...")
                return []
            
            time.sleep(3)
            
            print("Clicking search button...")
            # Click search button
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "ContentPlaceHolder1_btnSubmit"))
            )
            search_button.click()
            time.sleep(3)
            
            # Initialize list to store data
            candidates_data = []
            
            # Process all pages
            current_page = 1
            while True:
                print(f"\nProcessing page {current_page} for {office_type}...")
                
                # Wait for table to be present
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_gvCandidates"))
                    )
                except Exception as e:
                    print(f"Could not find candidates table for {office_type}: {str(e)}")
                    if attempt < max_retries - 1:
                        print("Retrying...")
                        break
                    return candidates_data
                
                # Get the page source and parse it with BeautifulSoup
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                
                # Find the table
                table = soup.find('table', {'id': 'ContentPlaceHolder1_gvCandidates'})
                if not table:
                    print(f"Could not find candidates table for {office_type}")
                    if attempt < max_retries - 1:
                        print("Retrying...")
                        break
                    return candidates_data
                    
                # Get all rows except header
                rows = table.find_all('tr')[1:]  # Skip header row
                print(f"Found {len(rows)} candidate rows on page {current_page} for {office_type}")
                
                if len(rows) == 0:
                    print(f"No candidates found for {office_type}")
                    return candidates_data
                
                for row in rows:
                    try:
                        # Get all columns
                        cols = row.find_all('td')
                        if len(cols) >= 7:
                            # Get name and link
                            name_col = cols[0]
                            name_link = name_col.find('a')
                            if name_link:
                                name = name_link.text.strip()
                                candidate_link = name_link.get('href')
                                
                                # Get other information
                                office = cols[3].text.strip()
                                party = cols[4].text.strip()
                                date_filed = cols[5].text.strip()
                                
                                print(f"Processing candidate: {name} ({party})")
                                
                                # Convert relative URL to absolute URL if needed
                                if candidate_link and not candidate_link.startswith('http'):
                                    candidate_link = f"https://www.elections.il.gov/ElectionOperations/{candidate_link}"
                                
                                # Get additional details if link exists
                                email, website = "", ""
                                if candidate_link:
                                    email, website = get_candidate_details(driver, candidate_link)
                                    # Go back to search results
                                    driver.back()
                                    time.sleep(2)
                                
                                candidate_data = {
                                    'Election': election_name,
                                    'Office': office,
                                    'Name': name,
                                    'Party': party,
                                    'Date Filed': date_filed,
                                    'Email': email,
                                    'Website': website
                                }
                                
                                candidates_data.append(candidate_data)
                                print(f"Successfully added candidate: {name}")
                        
                    except Exception as e:
                        print(f"Error processing row: {str(e)}")
                        continue
                
                # Check if there's a next page
                next_page_link = soup.find('a', {'id': lambda x: x and 'PageNext' in x})
                if not next_page_link or 'GridViewPagerPageNumberLinkDisabled' in next_page_link.get('class', []):
                    print(f"No more pages to process for {office_type}")
                    return candidates_data
                    
                # Click next page
                try:
                    next_page = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.ID, f"ContentPlaceHolder1_gvCandidates_phPagerTemplate_gvCandidates_PageNext"))
                    )
                    next_page.click()
                    time.sleep(3)
                    current_page += 1
                except Exception as e:
                    print(f"Error clicking next page: {str(e)}")
                    if attempt < max_retries - 1:
                        print("Retrying...")
                        break
                    return candidates_data
            
            print(f"Successfully processed {len(candidates_data)} candidates for {office_type}")
            return candidates_data
            
        except Exception as e:
            print(f"Error in scrape_office_type: {str(e)}")
            if attempt < max_retries - 1:
                print("Retrying...")
                time.sleep(5)  # Wait before retry
            else:
                print(traceback.format_exc())
                return candidates_data

def prevent_sleep():
    """Start caffeinate to prevent system sleep"""
    print("Starting caffeinate to prevent system sleep...")
    return subprocess.Popen(['caffeinate', '-i'])

def main():
    # Start caffeinate process
    caffeinate_process = prevent_sleep()
    
    driver = setup_driver()
    all_candidates = []
    
    # List of office types to scrape - updated to match website options
    office_types = [
        "PRESIDENT AND VICE PRESIDENT",
        "REPRESENTATIVE IN CONGRESS",
        "DELEGATE/ALTERNATE",
        "STATE REPRESENTATIVE/STATE SENATOR",
        "TRUSTEE LEVEE AND SANITATION DIST.",
        "JUDGE-SUPREME",
        "JUDGE-APPELLATE",
        "JUDGE-COOK CIRCUIT/SUBCIRCUIT",
        "JUDGE-DOWNSTATE CIRCUIT/SUBCIRCUIT"
    ]
    
    try:
        # Get all election links
        election_links = get_election_links(driver)
        
        # Process each election
        for election in election_links:
            print(f"\n{'='*50}")
            print(f"Processing election: {election['name']} ({election['date']})")
            
            # Process each office type for this election
            for office_type in office_types:
                try:
                    candidates = scrape_office_type(driver, office_type, election['name'], election['url'])
                    if candidates:  # Only extend if we got candidates
                        all_candidates.extend(candidates)
                        print(f"Found {len(candidates)} candidates for {office_type}")
                    else:
                        print(f"No candidates found for {office_type}")
                except Exception as e:
                    print(f"Error processing {office_type}: {str(e)}")
                    if "invalid session id" in str(e).lower():
                        print("Attempting to recover session...")
                        driver = recover_session(driver, election['url'])
                    continue
            
            # Save progress after each election
            if all_candidates:
                df = pd.DataFrame(all_candidates)
                output_file = os.path.join("illinois", "illinois_all_elections_candidates.xlsx")
                df.to_excel(output_file, index=False)
                print(f"\nProgress saved to {output_file}")
                print(f"Total candidates collected so far: {len(all_candidates)}")
        
        # Final save
        if all_candidates:
            df = pd.DataFrame(all_candidates)
            output_file = os.path.join("illinois", "illinois_all_elections_candidates.xlsx")
            df.to_excel(output_file, index=False)
            print(f"\nFinal data saved to {output_file}")
            print(f"Total candidates collected: {len(all_candidates)}")
        else:
            print("\nNo candidates were collected!")
        
    except Exception as e:
        print(f"An error occurred in main: {str(e)}")
        print(traceback.format_exc())
    finally:
        try:
            driver.quit()
        except:
            pass
        # Stop caffeinate
        caffeinate_process.terminate()
        print("Stopped caffeinate process")

if __name__ == "__main__":
    main() 