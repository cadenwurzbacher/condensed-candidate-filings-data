# Hawaii Candidate Scraper

This scraper extracts candidate information from the Hawaii Campaign Spending Commission website and exports the data to an Excel file.

## Website Source

**URL:** https://ags.hawaii.gov/campaign/ballot-legal-name/

This page contains a comprehensive list of candidates running in the 2024 Hawaii election with their ballot names, legal names, offices, parties, and filing dates.

## Data Extracted

The scraper extracts the following information for each candidate:

- **Ballot Name**: The name that appears on the ballot (may differ from legal name)
- **Legal Name**: The candidate's official legal name
- **Office**: The position they are running for
- **Party**: Political party affiliation
- **Filing Date**: Date when nomination papers were filed

## Files

- `scrape_hawaii.py` - Main scraper script
- `requirements.txt` - Python dependencies
- `README.md` - This documentation file

## Installation

1. Install Python dependencies:
   ```bash
   pip3 install -r requirements.txt
   ```

## Usage

Run the scraper:
```bash
python3 scrape_hawaii.py
```

The script will:
1. Fetch the webpage from the Hawaii Campaign Spending Commission
2. Parse the candidate table data
3. Export the data to an Excel file with timestamp
4. Display a summary of results

## Output

The scraper generates an Excel file named `hawaii_candidates_YYYYMMDD_HHMMSS.xlsx` containing all candidate data.

### Sample Output Summary

```
============================================================
HAWAII CANDIDATE SCRAPING COMPLETE
============================================================
Total candidates scraped: 259
Output file: hawaii_candidates_20250827_171028.xlsx

Party breakdown:
  NONPARTISAN SPECIAL: 107
  DEMOCRATIC: 97
  REPUBLICAN: 50
  GREEN: 2
  LIBERTARIAN: 2
  WE THE PEOPLE: 1
```

## Features

- **Robust Error Handling**: Comprehensive error handling and logging
- **Detailed Logging**: Creates a log file (`hawaii_scraper.log`) with detailed execution information
- **Data Validation**: Ensures all required columns are present before processing
- **Timestamped Output**: Each run creates a uniquely named Excel file
- **Summary Statistics**: Displays party breakdown and total candidate count

## Dependencies

- `requests` - HTTP library for web requests
- `beautifulsoup4` - HTML parsing library
- `pandas` - Data manipulation and analysis
- `openpyxl` - Excel file handling
- `lxml` - XML/HTML parser

## Logging

The scraper creates detailed logs in `hawaii_scraper.log` including:
- Webpage fetch status
- Parsing progress
- Error messages
- Success confirmations

## Error Handling

The scraper handles various error conditions:
- Network connectivity issues
- Website structure changes
- Missing or malformed data
- File I/O errors

## Legal Notice

This scraper is for educational and research purposes. Please respect the website's terms of service and robots.txt file. The data is publicly available information from the Hawaii Campaign Spending Commission.
