# Indiana Candidate Scraper

This script scrapes candidate information from the Indiana Secretary of State's election division website by downloading PDF files and converting them to Excel format.

## Features

- Downloads candidate PDF files from the Indiana SOS website
- Extracts candidate data using both table extraction (tabula-py) and text parsing (pdfplumber)
- Converts data to a structured Excel format
- Handles multiple election types (Primary, General)
- Automatically cleans up downloaded PDF files
- Formats Excel output with proper styling

## Requirements

- Python 3.7+
- Java Runtime Environment (JRE) - required for tabula-py

## Installation

1. Install the required Python packages:
```bash
pip install -r requirements.txt
```

2. Install Java Runtime Environment (if not already installed):
   - macOS: `brew install java`
   - Ubuntu/Debian: `sudo apt-get install default-jre`
   - Windows: Download from Oracle or use OpenJDK

## Usage

Run the scraper:
```bash
python scrape_indiana.py
```

The script will:
1. Fetch the Indiana SOS candidate information page
2. Find all candidate-related PDF files
3. Download each PDF
4. Extract candidate data using table extraction or text parsing
5. Combine all data into a single Excel file
6. Clean up downloaded PDF files

## Output

The script generates an Excel file with the following columns:
- Year: Election year
- Election: Election type (Primary/General)
- Name: Candidate name
- Office: Office being sought
- Party: Political party (if available)
- District: District number (if applicable)
- Source: Source PDF file name

## Notes

- The script automatically detects and processes both table-based and text-based PDFs
- PDF files are temporarily downloaded and then cleaned up
- The script includes error handling for network issues and malformed PDFs
- Duplicate candidates are automatically removed
- Results are sorted by year (descending) and name (ascending)

## Troubleshooting

If you encounter issues:

1. **Java not found**: Ensure JRE is installed and in your PATH
2. **PDF extraction errors**: Some PDFs may not be parseable due to formatting
3. **Network errors**: Check your internet connection and try again
4. **Permission errors**: Ensure you have write permissions in the script directory

## Data Source

Data is sourced from the official Indiana Secretary of State website:
https://www.in.gov/sos/elections/candidate-information/ 