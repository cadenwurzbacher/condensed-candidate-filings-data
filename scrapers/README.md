# Arizona Vote Scraper

This script scrapes election information from the Arizona Secretary of State's website.

## Setup

1. Make sure you have Python 3.7+ installed
2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the script:
```bash
python scrape_arizona_vote.py
```

The script will:
1. Fetch data from https://apps.arizona.vote/electioninfo/Election/47
2. Parse the HTML content
3. Save the extracted data to `arizona_vote_data.json`

## Output

The script generates a JSON file (`arizona_vote_data.json`) containing:
- Timestamp of when the data was scraped
- Source URL
- Structured data from the website

## Error Handling

The script includes error handling for:
- Network request failures
- HTML parsing issues
- File writing errors

## Note

Please be mindful of the website's terms of service and implement appropriate delays between requests if you plan to scrape multiple pages. 