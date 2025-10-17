# Michigan Election Candidate Data Scraper

This folder contains the web scraper for Michigan Secretary of State election candidate data.

## Overview

The scraper navigates through the Michigan SOS elections website:
- Main page: https://www.michigan.gov/sos/elections/election-results-and-data
- Extracts links to candidate listings for each election (e.g., https://mi-boe.entellitrak.com/candlist/98pri/98pri_cl.htm)
- Scrapes candidate data from each election's candidate listing page
- Exports data to Excel format (.xlsx and .csv)

## Features

- **Automated Navigation**: Finds all candidate listing links from the main elections page
- **Pagination Support**: Automatically clicks through all pages to find every election
- **Smart Button Filtering**: Only clicks on "Candidate listing" links, ignoring other button types
- **Election Data Extraction**: Captures election year, date, name, and type from the main page
- **Comprehensive Candidate Data**: Scrapes all candidate information from HTML tables including name, party, office, address, date filed, and more
- **Smart Context Detection**: Extracts election information from table rows and URL patterns
- **Header Normalization**: Automatically maps various header formats to standardized field names
- **Multiple Output Formats**: Saves data as both Excel (.xlsx) and CSV files
- **Robust Error Handling**: Continues scraping even if individual pages fail
- **Detailed Logging**: Provides progress updates and error messages
- **Sleep Prevention**: Automatically prevents your computer from sleeping during scraping (works on macOS, Windows, and Linux)

## Files

- `michigan_scraper.py` - Main scraper script with MichiganElectionScraper class
- `requirements.txt` - Python dependencies (Selenium, BeautifulSoup, Pandas, etc.)
- `setup.sh` - Automated setup script for quick installation
- `output/` - Directory for scraped Excel and CSV files
- `README.md` - This file

## Requirements

- Python 3.8+
- Chrome/Chromium browser (for Selenium WebDriver)
- Internet connection

## Quick Start

### Option 1: Using the setup script (Recommended)

```bash
# Run the setup script
./setup.sh

# Activate virtual environment
source venv/bin/activate

# Run the scraper
python michigan_scraper.py
```

### Option 2: Manual installation

```bash
# Create virtual environment (optional but recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the scraper
python michigan_scraper.py
```

## Output

The scraper will create timestamped files in the `output/` directory:
- `michigan_candidates_YYYYMMDD_HHMMSS.xlsx` - Excel format
- `michigan_candidates_YYYYMMDD_HHMMSS.csv` - CSV format

Each file contains:
- **election**: Name/description of the election
- **election_year**: Year of the election (extracted from the main page)
- **election_date**: Date of the election (if available)
- **election_type**: Type of election (Primary, General, Special, etc.)
- **source_url**: URL where the data was scraped from
- **Candidate data columns**: All columns from the original candidate listing tables including:
  - name/candidate_name
  - party
  - office
  - address, city, state, zip
  - date_filed
  - district, county
  - phone, email
  - And any other fields present in the source data

## Configuration

You can customize the scraper behavior by modifying the parameters in `michigan_scraper.py`:

```python
scraper = MichiganElectionScraper(
    headless=False,  # Set to True to run browser in background
    output_dir="output"  # Change output directory
)
```

## Troubleshooting

**Browser doesn't open**: Make sure Chrome is installed. The script will automatically download the correct ChromeDriver.

**No data scraped**: The website structure may have changed. Check the logs for specific errors.

**Installation errors**: Make sure you have Python 3.8+ installed and sufficient permissions.

## Technical Details

The scraper uses:
- **Selenium WebDriver**: For browser automation and JavaScript rendering
- **BeautifulSoup**: For HTML parsing
- **Pandas**: For data manipulation and Excel export
- **webdriver-manager**: For automatic ChromeDriver management
- **Sleep Prevention**: Uses OS-specific methods to prevent sleep:
  - macOS: `caffeinate` command
  - Windows: `SetThreadExecutionState` API
  - Linux: `systemd-inhibit` command

## License

This scraper is part of the condensed-candidate-filings-data project.

