# Illinois Elections Scraper

This scraper collects candidate information from the Illinois State Board of Elections website.

## Features

- Scrapes candidate information for multiple office types
- Collects basic information (name, party, office, date filed)
- Attempts to collect additional information (email, website) from candidate detail pages
- Exports data to Excel format

## Setup

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Make sure you have Chrome browser installed on your system

## Usage

Run the scraper:
```bash
python illinois_scraper.py
```

The script will:
1. Iterate through all office types
2. Collect candidate information
3. Save the data to `illinois_candidates.xlsx`

## Output

The Excel file will contain the following columns:
- Office
- Name
- Party
- Date Filed
- Email
- Website

## Notes

- The script runs in headless mode (no visible browser window)
- Includes error handling to continue running even if individual candidates or office types fail
- May take several minutes to complete depending on the number of candidates 