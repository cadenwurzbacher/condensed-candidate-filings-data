import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import time
import logging
import os
import sys

# Add parent directory to path to import scraper_utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper_utils import ensure_raw_data_dir, save_to_formatted_excel, clean_phone_number

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'kansas_scraper.log')),
        logging.StreamHandler()
    ]
)

def setup_driver():
    """Set up and return an undetected Chrome WebDriver instance."""
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    driver = uc.Chrome(options=options)
    driver.implicitly_wait(10)
    return driver

def wait_for_element(driver, by, value, timeout=10):
    """Wait for an element to be present and visible."""
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
        return element
    except TimeoutException:
        logging.error(f"Timeout waiting for element: {value}")
        return None

def get_election_options(driver):
    """Get all election options from the dropdown."""
    try:
        # Wait longer for the page to load
        time.sleep(5)
        
        # Try different ways to find the dropdown
        select_element = None
        
        # Try by ID
        try:
            select_element = wait_for_element(driver, By.ID, "ddlElections", timeout=20)
        except:
            pass
            
        # Try by name
        if not select_element:
            try:
                select_element = wait_for_element(driver, By.NAME, "ddlElections", timeout=20)
            except:
                pass
                
        # Try by XPath
        if not select_element:
            try:
                select_element = wait_for_element(driver, By.XPATH, "//select[contains(@id, 'ddlElections')]", timeout=20)
            except:
                pass
        
        if not select_element:
            logging.error("Could not find election dropdown")
            # Print the first 2000 characters of the page source to the terminal for debugging
            print("\n--- PAGE SOURCE DEBUG START ---\n")
            print(driver.page_source[:2000])
            print("\n--- PAGE SOURCE DEBUG END ---\n")
            return []
            
        select = Select(select_element)
        options = []
        
        for option in select.options:
            if option.text.strip() and "Select" not in option.text:
                options.append({
                    'text': option.text.strip(),
                    'value': option.get_attribute('value')
                })
        
        return options
    except Exception as e:
        logging.error(f"Error getting election options: {str(e)}")
        return []

def get_candidate_data(driver, election_info):
    """Get candidate data for a specific election."""
    try:
        # Select the election
        select = Select(wait_for_element(driver, By.ID, "ddlElections", timeout=20))
        select.select_by_value(election_info['value'])
        
        # Click the submit button
        submit_button = wait_for_element(driver, By.ID, "btnSubmit", timeout=20)
        submit_button.click()
        
        # Wait for the table to load
        time.sleep(5)  # Give the page time to load after submission
        
        # Look for the table with candidates
        table = wait_for_element(driver, By.XPATH, "//table[contains(@class, 'gvResults')]", timeout=20)
        if not table:
            logging.warning(f"No candidates found for election: {election_info['text']}")
            return []
        
        candidates = []
        rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header row
        
        for row in rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 25:  # Ensure we have enough cells
                    # Clean up phone numbers using utility function
                    home_phone = clean_phone_number(cells[17].text)
                    cell_phone = clean_phone_number(cells[19].text)
                    
                    candidate = {
                        'Candidate': cells[0].text.strip(),  # Full name
                        'Office': cells[1].text.strip(),
                        'District': cells[2].text.strip(),
                        'Election': election_info['text'],
                        'Party': cells[5].text.strip(),  # Party
                        'Home Address': cells[11].text.strip(),  # Home Address
                        'Home City': cells[12].text.strip(),  # Home City
                        'Home Zip': cells[13].text.strip(),  # Home Zip
                        'Home Phone': home_phone,
                        'Cell Phone': cell_phone,
                        'Email': cells[20].text.strip(),  # Email
                        'Web Address': cells[21].text.strip(),  # Web
                        'Date Filed': cells[22].text.strip()  # Date filed
                    }
                    candidates.append(candidate)
                    logging.info(f"Processed candidate: {candidate['Candidate']}")
            except Exception as e:
                logging.error(f"Error processing row: {str(e)}")
                continue
        
        return candidates
    except Exception as e:
        logging.error(f"Error getting candidate data: {str(e)}")
        return []

def create_excel(candidates, script_dir):
    """Create an Excel file with the candidate data."""
    if not candidates:
        logging.error("No candidates to write to Excel")
        return

    # Create DataFrame
    df = pd.DataFrame(candidates)

    # Sort by Election and Candidate
    df = df.sort_values(['Election', 'Candidate'], ascending=[False, True])

    # Ensure raw output directory exists
    raw_dir = ensure_raw_data_dir()

    # Create Excel file using utility function
    excel_path = save_to_formatted_excel(
        df=df,
        state_name='kansas',
        output_dir=raw_dir
    )

    logging.info(f"Created Excel file: {os.path.basename(excel_path)}")
    return excel_path

def scrape_kansas_candidates():
    """Main function to scrape Kansas candidate data."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    url = "https://sos.ks.gov/elections/elections_upcoming_candidate.aspx"
    
    try:
        logging.info("Initializing scraper...")
        driver = setup_driver()
        
        logging.info("Loading main page...")
        driver.get(url)
        time.sleep(10)  # Increased initial wait time to 10 seconds
        
        # Get all election options
        election_options = get_election_options(driver)
        if not election_options:
            logging.error("No election options found!")
            return
        
        logging.info(f"Found {len(election_options)} elections")
        
        all_candidates = []
        
        # Process each election
        for election in election_options:
            try:
                logging.info(f"Processing election: {election['text']}")
                candidates = get_candidate_data(driver, election)
                all_candidates.extend(candidates)
                logging.info(f"Found {len(candidates)} candidates for {election['text']}")
                time.sleep(1)  # Be nice to the server
                
            except Exception as e:
                logging.error(f"Error processing election {election['text']}: {str(e)}")
                continue
        
        if not all_candidates:
            logging.error("No candidates found!")
            return
        
        # Create Excel file
        excel_path = create_excel(all_candidates, script_dir)
        logging.info(f"Successfully scraped {len(all_candidates)} candidates from Kansas")
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
    finally:
        driver.quit()

if __name__ == "__main__":
    scrape_kansas_candidates() 