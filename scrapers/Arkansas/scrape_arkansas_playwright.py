import asyncio
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from playwright.async_api import async_playwright
import logging
import openpyxl

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def scrape_arkansas_candidates_playwright():
    url = "https://candidates.arkansas.gov/"
    all_data = []
    headers = None
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Add user agent to appear more like a regular browser
        await page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        })
        
        logging.info(f"Navigating to {url}")
        await page.goto(url)
        
        # Wait longer for the page to fully load
        await page.wait_for_timeout(5000)
        
        # Wait for the table and at least one row to load
        logging.info("Waiting for table to load...")
        try:
            # Wait for the table to be present
            await page.wait_for_selector('#metl-2941-resultTable', timeout=60000)
            
            # Try to click any search or filter buttons that might load data
            try:
                # Look for search or filter buttons
                search_button = await page.query_selector('button[type="submit"], input[type="submit"], .search-btn, .filter-btn')
                if search_button:
                    logging.info("Found search/filter button, clicking it...")
                    await search_button.click()
                    await page.wait_for_timeout(3000)
            except Exception as e:
                logging.info(f"No search button found or error clicking: {e}")
            
            # Try to fill out any search form fields
            try:
                # Look for common search field names
                search_fields = await page.query_selector_all('input[type="text"], input[type="search"], select')
                if search_fields:
                    logging.info(f"Found {len(search_fields)} potential search fields")
                    
                    # Try to fill out any text fields with a wildcard or empty search
                    for field in search_fields:
                        field_type = await field.get_attribute('type')
                        field_name = await field.get_attribute('name') or await field.get_attribute('id') or ''
                        
                        if field_type in ['text', 'search']:
                            logging.info(f"Filling text field: {field_name}")
                            await field.fill('')
                            await page.wait_for_timeout(1000)
                        
                        elif field_type == 'select-one':
                            # Try to select the first option
                            options = await field.query_selector_all('option')
                            if len(options) > 1:
                                logging.info(f"Selecting first option in dropdown: {field_name}")
                                await field.select_option(index=0)
                                await page.wait_for_timeout(1000)
                    
                    # Try clicking search again after filling fields
                    search_button = await page.query_selector('button[type="submit"], input[type="submit"], .search-btn, .filter-btn')
                    if search_button:
                        logging.info("Clicking search button after filling fields...")
                        await search_button.click()
                        await page.wait_for_timeout(3000)
                        
            except Exception as e:
                logging.info(f"Error filling search fields: {e}")
            
            # Wait for the data to be loaded
            await page.wait_for_function('''
                () => {
                    const table = document.querySelector('#metl-2941-resultTable tbody');
                    return table && table.children.length > 0;
                }
            ''', timeout=60000)
            
            logging.info("Table and data found")
        except Exception as e:
            logging.error(f"Error waiting for table: {str(e)}")
            await browser.close()
            return
        
        page_num = 1
        while True:
            # Get the current page's data
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find('table', {'id': 'metl-2941-resultTable'})
            
            if not table:
                logging.error("Could not find the candidate table on the page.")
                break
                
            # Print the table HTML for debugging (first page only)
            if page_num == 1:
                logging.info("Table HTML (first page):\n" + str(table))
            
            # Get headers if we haven't already
            if headers is None:
                headers_row = table.find('thead').find_all('th')
                headers = [th.get_text(strip=True) for th in headers_row]
                logging.info(f"Found headers: {headers}")
            
            # Get data from current page
            rows = table.find('tbody').find_all('tr')
            logging.info(f"Found {len(rows)} rows on current page")
            
            for row in rows:
                cols = row.find_all(['td', 'th'])
                if len(cols) > 1:  # Skip empty rows
                    cols = cols[1:]  # Skip the first column (details link)
                    row_data = [col.get_text(strip=True) for col in cols]
                    if any(row_data):  # Only add non-empty rows
                        all_data.append(row_data)
                        logging.info(f"Processed row: {row_data}")
            
            # Try to click the "Next" button
            next_button = await page.query_selector('a.paginate_button.next:not(.disabled)')
            if not next_button:
                logging.info("No more pages to scrape.")
                break
                
            # Click the next button and wait for the table to update
            logging.info("Clicking next page button...")
            await next_button.click()
            
            # Wait for the new data to load
            try:
                await page.wait_for_function('''
                    () => {
                        const table = document.querySelector('#metl-2941-resultTable tbody');
                        return table && table.children.length > 0;
                    }
                ''', timeout=60000)
                await asyncio.sleep(2)  # Small delay to ensure the page has updated
            except Exception as e:
                logging.error(f"Error waiting for next page to load: {str(e)}")
                break
            page_num += 1
        
        await browser.close()
        
        if all_data:
            # Create DataFrame with all collected data
            df = pd.DataFrame(all_data, columns=headers)
            # Save to Excel
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            excel_filename = f"arkansas_candidates_{timestamp}.xlsx"
            raw_dir = os.path.join('data', 'raw')
            os.makedirs(raw_dir, exist_ok=True)
            excel_path = os.path.join(raw_dir, excel_filename)
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Candidates', index=False)
                workbook = writer.book
                worksheet = writer.sheets['Candidates']
                # Format the header row
                for cell in worksheet[1]:
                    cell.font = cell.font.copy(bold=True)
                    cell.fill = openpyxl.styles.PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
                # Adjust column widths
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = openpyxl.utils.get_column_letter(column[0].column)
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = (max_length + 2)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            logging.info(f"Successfully scraped {len(df)} candidates from all pages.")
            logging.info(f"Data saved to {excel_filename}.")
        else:
            logging.error("No candidate data was collected.")

if __name__ == "__main__":
    asyncio.run(scrape_arkansas_candidates_playwright()) 