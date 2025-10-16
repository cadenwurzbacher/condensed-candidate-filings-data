#!/usr/bin/env python3
"""
Test script for Florida Candidate Scraper

This script tests the basic functionality of the scraper without downloading files.
It verifies that we can navigate to the page and identify the election year dropdown.
"""

import asyncio
import logging
from playwright.async_api import async_playwright

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_florida_website():
    """Test basic navigation and element identification."""
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        try:
            # Navigate to the Florida Division of Elections page
            url = "https://dos.elections.myflorida.com/candidates/downloadcanlist.asp"
            logger.info(f"Navigating to {url}")
            
            await page.goto(url, wait_until='networkidle')
            await page.wait_for_load_state('domcontentloaded')
            
            # Take a screenshot for debugging
            await page.screenshot(path="florida_page_screenshot.png")
            logger.info("Screenshot saved as florida_page_screenshot.png")
            
            # Check if the election year dropdown exists
            election_year_selector = 'select[name="elecID"]'
            dropdown_exists = await page.query_selector(election_year_selector)
            
            if dropdown_exists:
                logger.info("✅ Election year dropdown found!")
                
                # Get all options
                options = await page.eval_on_selector_all(
                    f'{election_year_selector} option',
                    'options => options.map(option => ({value: option.value, text: option.textContent.trim()}))'
                )
                
                logger.info(f"Found {len(options)} options in dropdown:")
                for i, option in enumerate(options[:10]):  # Show first 10
                    logger.info(f"  {i+1}. {option['text']} (value: {option['value']})")
                
                if len(options) > 10:
                    logger.info(f"  ... and {len(options) - 10} more options")
                
            else:
                logger.error("❌ Election year dropdown not found!")
                
                # List all select elements on the page
                all_selects = await page.query_selector_all('select')
                logger.info(f"Found {len(all_selects)} select elements on the page:")
                for i, select in enumerate(all_selects):
                    name = await select.get_attribute('name')
                    id_attr = await select.get_attribute('id')
                    logger.info(f"  {i+1}. name='{name}', id='{id_attr}'")
            
            # Check for download button
            download_button = await page.query_selector('input[name="FormSubmit"][value="Download Candidate List"]')
            if not download_button:
                download_button = await page.query_selector('input[type="submit"][value*="Download"]')
            if not download_button:
                download_button = await page.query_selector('input[type="submit"]')
            
            if download_button:
                button_value = await download_button.get_attribute('value')
                logger.info(f"✅ Download button found! Value: '{button_value}'")
            else:
                logger.error("❌ Download button not found!")
                
                # List all input elements
                all_inputs = await page.query_selector_all('input[type="submit"]')
                logger.info(f"Found {len(all_inputs)} submit buttons:")
                for i, input_elem in enumerate(all_inputs):
                    value = await input_elem.get_attribute('value')
                    logger.info(f"  {i+1}. value='{value}'")
            
            # Get page title and URL
            title = await page.title()
            current_url = page.url
            logger.info(f"Page title: {title}")
            logger.info(f"Current URL: {current_url}")
            
        except Exception as e:
            logger.error(f"Test failed: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_florida_website()) 