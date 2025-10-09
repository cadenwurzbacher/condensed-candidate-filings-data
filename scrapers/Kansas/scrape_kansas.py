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
from datetime import datetime
import openpyxl

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
                    # Clean up phone numbers by removing prefixes
                    home_phone = cells[17].text.strip()
                    if home_phone.startswith('Home:'):
                        home_phone = home_phone[5:].strip()
                    
                    cell_phone = cells[19].text.strip()
                    if cell_phone.startswith('Cell:'):
                        cell_phone = cell_phone[5:].strip()
                    
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
    
    # Create Excel file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_file = f'kansas_candidates_{timestamp}.xlsx'
    excel_path = os.path.join(script_dir, excel_file)
    
    # Create Excel writer
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Candidates')
        
        # Get the workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets['Candidates']
        
        # Format headers
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
        
        # Add filters to headers
        worksheet.auto_filter.ref = worksheet.dimensions
    
    logging.info(f"Created Excel file: {excel_file}")
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