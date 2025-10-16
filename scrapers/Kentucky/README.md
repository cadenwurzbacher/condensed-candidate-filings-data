# Kentucky Candidate Scraper

This scraper extracts candidate information from the Kentucky Registry of Election Finance website and organizes the data into multiple Excel files by election years.

## Features

- Scrapes all candidate data from the Kentucky Registry of Election Finance
- Automatically handles pagination to get all results
- Organizes data into separate Excel files by election years:
  - `kentucky_candidates_2025_[timestamp].xlsx`
  - `kentucky_candidates_2024_[timestamp].xlsx`
  - `kentucky_candidates_2023_[timestamp].xlsx`
  - `kentucky_candidates_2022_[timestamp].xlsx`
  - `kentucky_candidates_2020_[timestamp].xlsx`
  - `kentucky_candidates_other_years_[timestamp].xlsx` (for all other years)
  - `kentucky_candidates_all_[timestamp].xlsx` (combined file with all data)

## Data Columns

Each Excel file contains the following columns:
- **last_name**: Candidate's last name
- **first_name**: Candidate's first name
- **office_sought**: The office the candidate is running for
- **location**: Geographic location/district
- **election_date**: Date of the election
- **election_type**: Type of election (Primary, General, etc.)
- **active_status**: Whether the candidate is active or closed

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the scraper:
```bash
python scrape_kentucky.py
```

The scraper will:
1. Connect to the Kentucky Registry of Election Finance website
2. Scrape all pages of candidate data
3. Parse and organize the data by election years
4. Create multiple Excel files with formatted data
5. Log progress and results

## Output

The scraper creates timestamped Excel files in the same directory as the script. Each file is properly formatted with:
- Bold headers with gray background
- Auto-adjusted column widths
- Clean, readable data

## Notes

- The scraper includes a 1-second delay between page requests to be respectful to the server
- All errors are logged for debugging purposes
- The scraper uses cloudscraper to handle any potential anti-bot measures
- Data is extracted from the Kentucky Registry of Election Finance public search interface 