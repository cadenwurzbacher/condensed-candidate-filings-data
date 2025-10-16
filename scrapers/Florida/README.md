# Florida Candidate Data

This folder contains tools and data for processing Florida candidate information from the [Florida Division of Elections website](https://dos.elections.myflorida.com/candidates/downloadcanlist.asp).

## Files

- `scrape_florida.py` - **Main scraper** that downloads all candidate data from all election years and combines into one Excel file
- `test_scraper.py` - Test script to verify website navigation and element identification
- `convert_florida_candidates.py` - Standalone script to convert individual tab-separated candidate data to Excel format
- `requirements.txt` - Python dependencies needed for all scripts
- `florida_candidates_20250805_203714.xlsx` - Sample converted Excel file with Florida candidate data

## Data Summary

The converted Excel file contains **1,014 candidates** with the following breakdown:

### Office Types
- State Representative: 377 candidates
- Circuit Judge: 237 candidates  
- United States Representative: 139 candidates
- President of the United States: 70 candidates
- State Senator: 61 candidates

### Party Distribution
- Republican Party of Florida: 340 candidates
- No Party Affiliation (nonpartisan offices): 290 candidates
- Florida Democratic Party: 289 candidates
- Write-In: 45 candidates
- No Party Affiliation (Partisan): 27 candidates

### Status Distribution
- Elected: 443 candidates
- Defeated: 351 candidates
- Did Not Qualify: 104 candidates
- Withdrew: 68 candidates
- Retained: 25 candidates

## Data Fields

The Excel file contains 26 columns including:
- Account Number, Voter ID, Election ID
- Office information (code, description, jurisdiction)
- Status information (code, description)
- Party information (code, description)
- Candidate name (last, first, middle)
- Address information (suppress flag, address lines, city, state, zip, county)
- Phone number
- Treasurer information (name, email)
- Email address

## Usage

### Main Scraper (Recommended)

To scrape all Florida candidate data from all election years:

1. Install dependencies: `pip install -r requirements.txt`
2. Install Playwright browsers: `playwright install`
3. Run the main scraper: `python3 scrape_florida.py`

**⚠️ IMPORTANT REQUIREMENTS:**
- **Keep your computer ON and awake** during the entire process (5-10 minutes)
- **Do not close the browser window** that opens
- **Ensure stable internet connection**
- **Do not put computer to sleep or hibernate**
- The script will ask for confirmation before starting

The scraper will:
- Navigate to the Florida Division of Elections website
- Extract all available election years from the dropdown (92+ elections)
- Download candidate lists for each election year
- Create backup files after completing 2020 and 2016 elections
- Combine all data into one comprehensive Excel file
- Generate detailed logs of the process
- Show real-time progress updates

### Test the Scraper

To test the scraper without downloading files:

```bash
python3 test_scraper.py
```

This will:
- Navigate to the website
- Identify the election year dropdown and download button
- Take a screenshot for debugging
- Display all available election options

### Individual File Conversion

To convert a single candidate list file:

1. Install dependencies: `pip install -r requirements.txt`
2. Update the `input_file` path in `convert_florida_candidates.py`
3. Run: `python3 convert_florida_candidates.py`

The script will automatically:
- Detect the correct file encoding
- Convert the data to Excel format
- Auto-adjust column widths
- Generate a timestamped output filename
- Display data summary statistics 