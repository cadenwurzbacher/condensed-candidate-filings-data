import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'missouri_scraper.log')),
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

def get_candidate_data(driver):
    """Get candidate data from the Missouri SOS website."""
    try:
        time.sleep(5)
        all_candidates = []
        office_headers = driver.find_elements(By.TAG_NAME, "h3")
        for idx, office_header in enumerate(office_headers):
            try:
                office_name = office_header.text.strip()
                if not office_name:
                    continue
                logging.info(f"Processing office: {office_name}")
                tables = []
                next_element = office_header
                while True:
                    try:
                        next_element = next_element.find_element(By.XPATH, "following-sibling::*[1]")
                    except Exception:
                        break
                    if next_element.tag_name.lower() == "h3":
                        break
                    if next_element.tag_name.lower() == "table":
                        tables.append(next_element)
                for table in tables:
                    try:
                        caption = table.find_element(By.TAG_NAME, "caption").text.strip()
                        party_name = caption
                        rows = table.find_elements(By.TAG_NAME, "tr")
                        for row in rows[1:]:  # Skip header row
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if len(cells) >= 4:
                                candidate = {
                                    'Office': office_name,
                                    'Name': cells[0].text.strip(),
                                    'Party': party_name,
                                    'Mailing Address': cells[1].text.strip(),
                                    'Date Filed': cells[3].text.strip()
                                }
                                all_candidates.append(candidate)
                                logging.info(f"Processed candidate: {candidate['Name']}")
                    except Exception as e:
                        logging.error(f"Error processing table for office {office_name}: {str(e)}")
            except Exception as e:
                logging.error(f"Error processing office section: {str(e)}")
                continue
        return all_candidates
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
    
    # Sort by Office and Name
    df = df.sort_values(['Office', 'Name'], ascending=[True, True])
    
    # Create Excel file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_file = f'missouri_candidates_{timestamp}.xlsx'
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

def scrape_missouri_candidates():
    """Main function to scrape Missouri candidate data."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    url = "https://s1.sos.mo.gov/candidatesonweb/displaycandidatesplacement.aspx"
    
    try:
        logging.info("Initializing scraper...")
        driver = setup_driver()
        
        logging.info("Loading main page...")
        driver.get(url)
        logging.info("Waiting for page to load...")
        time.sleep(20)  # Increased wait time
        
        # Log page title and URL to verify we're on the right page
        logging.info(f"Current page title: {driver.title}")
        logging.info(f"Current URL: {driver.current_url}")
        
        # Get page source for debugging
        page_source = driver.page_source
        logging.info(f"Page source length: {len(page_source)}")
        
        # Get candidate data
        candidates = get_candidate_data(driver)
        
        if not candidates:
            logging.error("No candidates found!")
            return
        
        # Create Excel file
        excel_path = create_excel(candidates, script_dir)
        logging.info(f"Successfully scraped {len(candidates)} candidates from Missouri")
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
    finally:
        driver.quit()

if __name__ == "__main__":
    scrape_missouri_candidates() 