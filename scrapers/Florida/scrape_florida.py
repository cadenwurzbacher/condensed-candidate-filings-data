#!/usr/bin/env python3
"""
Florida Candidate Scraper

This script scrapes candidate data from the Florida Division of Elections website.
It navigates through all election years, downloads candidate lists, and combines
them into one comprehensive Excel file.
"""

import asyncio
import os
import time
import pandas as pd
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, Page
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('florida_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FloridaCandidateScraper:
    def __init__(self, download_dir: str = "data/raw"):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        self.base_url = "https://dos.elections.myflorida.com/candidates/downloadcanlist.asp"
        self.downloaded_files = []
        self.backup_elections = ["20201103-GEN", "20161108-GEN"]  # 2020 and 2016 elections
        

    
    async def navigate_to_page(self):
        """Navigate to the Florida Division of Elections candidate download page."""
        try:
            logger.info(f"Navigating to {self.base_url}")
            await self.page.goto(self.base_url, wait_until='networkidle')
            await self.page.wait_for_load_state('domcontentloaded')
            logger.info("Successfully loaded the page")
            return True
        except Exception as e:
            logger.error(f"Failed to navigate to page: {e}")
            return False
    
    async def get_election_years(self):
        """Extract all available election years from the dropdown."""
        try:
            # Wait for the election year dropdown to be available
            await self.page.wait_for_selector('select[name="elecID"]', timeout=10000)
            
            # Get all options from the election year dropdown
            election_years = await self.page.eval_on_selector_all(
                'select[name="elecID"] option',
                'options => options.map(option => ({value: option.value, text: option.textContent.trim()}))'
            )
            
            # Filter out empty values and "Select Election Year" option
            valid_elections = [
                election for election in election_years 
                if election['value'] and election['value'] != '' and 'Select' not in election['text']
            ]
            
            logger.info(f"Found {len(valid_elections)} election years")
            for election in valid_elections:
                logger.info(f"  - {election['text']} (value: {election['value']})")
            
            return valid_elections
            
        except Exception as e:
            logger.error(f"Failed to get election years: {e}")
            return []
    
    async def select_election_year(self, election_value: str, election_text: str):
        """Select a specific election year from the dropdown."""
        try:
            logger.info(f"Selecting election year: {election_text}")
            
            # Select the election year
            await self.page.select_option('select[name="elecID"]', election_value)
            
            # Wait for the page to update (form submission)
            await self.page.wait_for_load_state('networkidle')
            
            # Wait a bit for any dynamic content to load
            await asyncio.sleep(2)
            
            logger.info(f"Successfully selected election year: {election_text}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to select election year {election_text}: {e}")
            return False
    
    async def download_candidate_list(self, election_text: str):
        """Download the candidate list for the currently selected election."""
        try:
            logger.info(f"Attempting to download candidate list for: {election_text}")
            
            # Look for the download button
            download_button = await self.page.query_selector('input[name="FormSubmit"][value="Download Candidate List"]')
            if not download_button:
                download_button = await self.page.query_selector('input[type="submit"][value*="Download"]')
            if not download_button:
                download_button = await self.page.query_selector('input[type="submit"]')
            
            if not download_button:
                logger.warning(f"No download button found for {election_text}")
                return None
            
            # Set up download listener
            download_promise = self.page.expect_download()
            
            # Click the download button
            await download_button.click()
            
            # Wait for download to start
            download = await download_promise
            
            # Wait for download to complete
            download_path = await download.path()
            
            # Move file to our download directory with a descriptive name
            safe_election_name = "".join(c for c in election_text if c.isalnum() or c in (' ', '-', '_')).rstrip()
            safe_election_name = safe_election_name.replace(' ', '_')
            new_filename = f"florida_candidates_{safe_election_name}.txt"
            new_path = self.download_dir / new_filename
            
            # Move the downloaded file
            download.save_as(new_path)
            
            logger.info(f"Successfully downloaded: {new_filename}")
            self.downloaded_files.append(new_path)
            
            return new_path
            
        except Exception as e:
            logger.error(f"Failed to download candidate list for {election_text}: {e}")
            return None
    
    async def convert_file_to_excel(self, txt_file_path: Path):
        """Convert a single text file to Excel format and return the dataframe."""
        try:
            logger.info(f"Converting {txt_file_path.name} to Excel format")
            
            # Try different encodings
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(txt_file_path, sep='\t', encoding=encoding)
                    logger.info(f"Successfully read file with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                logger.error(f"Could not read {txt_file_path.name} with any encoding")
                return None
            
            # Add source election information
            df['Source_File'] = txt_file_path.name
            df['Processing_Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            logger.info(f"Successfully processed: {txt_file_path.name}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to convert {txt_file_path.name}: {e}")
            return None
    
    async def create_backup_file(self, dataframes, backup_name: str):
        """Create a backup Excel file from dataframes."""
        try:
            logger.info(f"Creating backup: {backup_name}")
            
            if not dataframes:
                logger.warning("No dataframes to combine")
                return None
            
            # Combine all dataframes
            combined_df = pd.concat(dataframes, ignore_index=True)
            
            # Create backup Excel file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"florida_backup_{backup_name}_{timestamp}.xlsx"
            backup_path = self.download_dir / backup_filename
            
            with pd.ExcelWriter(backup_path, engine='openpyxl') as writer:
                combined_df.to_excel(writer, sheet_name='All_Candidates', index=False)
                
                # Auto-adjust column widths
                worksheet = writer.sheets['All_Candidates']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            logger.info(f"Successfully created backup: {backup_filename}")
            print(f"💾 BACKUP CREATED: {backup_filename}")
            logger.info(f"Total candidates in backup: {len(combined_df)}")
            
            return backup_path
            
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            return None
    
    async def combine_excel_files(self):
        """Combine all downloaded files into one comprehensive Excel file."""
        try:
            logger.info("Combining all downloaded files into one comprehensive file")
            
            # Convert all downloaded files to dataframes
            all_dataframes = []
            for txt_file in self.downloaded_files:
                df = await self.convert_file_to_excel(txt_file)
                if df is not None:
                    all_dataframes.append(df)
                    logger.info(f"Added {len(df)} records from {txt_file.name}")
            
            if not all_dataframes:
                logger.error("No dataframes to combine")
                return None
            
            # Combine all dataframes
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            
            # Create combined Excel file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            combined_filename = f"florida_all_candidates_{timestamp}.xlsx"
            combined_path = self.download_dir / combined_filename
            
            with pd.ExcelWriter(combined_path, engine='openpyxl') as writer:
                combined_df.to_excel(writer, sheet_name='All_Candidates', index=False)
                
                # Auto-adjust column widths
                worksheet = writer.sheets['All_Candidates']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            logger.info(f"Successfully created combined file: {combined_filename}")
            logger.info(f"Total candidates: {len(combined_df)}")
            
            return combined_path
            
        except Exception as e:
            logger.error(f"Failed to combine files: {e}")
            return None
    
    async def run_scraper(self):
        """Main method to run the complete scraping process."""
        
        async with async_playwright() as p:
            try:
                # Configure browser for downloads
                browser = await p.chromium.launch(
                    headless=False,  # Set to True for production
                    args=['--disable-blink-features=AutomationControlled']
                )
                
                # Set up download behavior
                context = await browser.new_context(
                    accept_downloads=True,
                    viewport={'width': 1920, 'height': 1080}
                )
                
                self.page = await context.new_page()
                
                # Set download directory
                await self.page.context.set_default_timeout(30000)
                
                # Navigate to the page
                if not await self.navigate_to_page():
                    return False
                
                # Get all election years
                election_years = await self.get_election_years()
                if not election_years:
                    logger.error("No election years found")
                    return False
                
                # Process each election year
                successful_downloads = 0
                total_elections = len(election_years)
                
                print(f"\n📊 Processing {total_elections} elections...")
                print("   (Progress will be shown in the browser and log file)")
                print("")
                
                for i, election in enumerate(election_years, 1):
                    try:
                        print(f"🔄 [{i}/{total_elections}] Processing: {election['text']}")
                        
                        # Select the election year
                        if await self.select_election_year(election['value'], election['text']):
                            # Download the candidate list
                            downloaded_file = await self.download_candidate_list(election['text'])
                            if downloaded_file:
                                successful_downloads += 1
                                print(f"   ✅ Downloaded successfully")
                            else:
                                print(f"   ❌ Download failed")
                            
                            # Check if we should create a backup after this election
                            if election['value'] in self.backup_elections:
                                print(f"\n💾 Creating backup after {election['text']}...")
                                
                                # Convert all downloaded files to dataframes
                                all_dataframes = []
                                for txt_file in self.downloaded_files:
                                    df = await self.convert_file_to_excel(txt_file)
                                    if df is not None:
                                        all_dataframes.append(df)
                                
                                if all_dataframes:
                                    # Create backup
                                    backup_name = "2020_election" if election['value'] == "20201103-GEN" else "2016_election"
                                    backup_file = await self.create_backup_file(all_dataframes, backup_name)
                                    if backup_file:
                                        print(f"   ✅ Backup saved: {backup_file.name}")
                                    else:
                                        print(f"   ❌ Backup failed")
                                
                                print(f"   📊 Progress: {successful_downloads} elections completed")
                            
                            # Add delay between downloads to be respectful
                            print(f"   ⏳ Waiting 3 seconds before next download...")
                            await asyncio.sleep(3)
                        else:
                            print(f"   ❌ Failed to select election year")
                        
                    except KeyboardInterrupt:
                        print(f"\n⚠️  Scraping interrupted by user at election {i}/{total_elections}")
                        print(f"   Processed {successful_downloads} elections successfully")
                        logger.warning(f"Scraping interrupted by user after {successful_downloads} downloads")
                        break
                    except Exception as e:
                        logger.error(f"Error processing election {election['text']}: {e}")
                        print(f"   ❌ Error: {str(e)[:100]}...")
                        continue
                
                logger.info(f"Successfully downloaded {successful_downloads} out of {total_elections} election files")
                
                if successful_downloads == 0:
                    print("\n❌ No files were downloaded successfully.")
                    return False
                
                print(f"\n🔗 Creating final combined Excel file...")
                
                # Create the final combined file
                combined_file = await self.combine_excel_files()
                if combined_file:
                    logger.info(f"Scraping completed successfully! Combined file: {combined_file}")
                    print(f"\n📊 SUMMARY:")
                    print(f"   - Downloaded: {successful_downloads}/{total_elections} elections")
                    print(f"   - Backup files created after 2020 and 2016 elections")
                    print(f"   - Final combined file: {combined_file.name}")
                    return True
                
                return False
                
            except Exception as e:
                logger.error(f"Scraping failed: {e}")
                return False
            
            finally:
                if context:
                    await context.close()
                if browser:
                    await browser.close()

async def main():
    """Main function to run the scraper."""
    
    print("=" * 80)
    print("🚨 IMPORTANT: FLORIDA CANDIDATE SCRAPER")
    print("=" * 80)
    print("⚠️  WARNING: This script will take a significant amount of time to complete.")
    print("   - Processing 92+ election years")
    print("   - Each download takes 3+ seconds")
    print("   - Total estimated time: 5-10 minutes")
    print("")
    print("💻 REQUIREMENTS:")
    print("   - Keep your computer ON and awake during the entire process")
    print("   - Do not close the browser window that opens")
    print("   - Ensure stable internet connection")
    print("   - Do not put computer to sleep or hibernate")
    print("")
    print("📁 OUTPUT:")
    print("   - Combined master Excel file with all candidates")
    print("   - Backup files after 2020 and 2016 elections")
    print("   - Detailed log file: florida_scraper.log")
    print("")
    print("=" * 80)
    
    # Ask for confirmation
    response = input("Do you want to continue? (y/N): ").strip().lower()
    if response not in ['y', 'yes']:
        print("Scraping cancelled by user.")
        return
    
    print("\n🚀 Starting Florida candidate scraper...")
    print("   (You can monitor progress in the browser window and log file)")
    print("")
    
    scraper = FloridaCandidateScraper()
    success = await scraper.run_scraper()
    
    if success:
        logger.info("Florida candidate scraping completed successfully!")
        print("\n✅ SCRAPING COMPLETED SUCCESSFULLY!")
        print("📊 Check the 'downloads' folder for your Excel files.")
    else:
        logger.error("Florida candidate scraping failed!")
        print("\n❌ SCRAPING FAILED!")
        print("📋 Check florida_scraper.log for error details.")

if __name__ == "__main__":
    asyncio.run(main()) 