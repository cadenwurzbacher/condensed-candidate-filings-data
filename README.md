# Condensed Candidate Filings Data Pipeline

This project contains data cleaning and processing scripts for candidate filing data from various US states, including Alaska, Colorado, Delaware, and Georgia.

## Project Structure

- `alaska_cleaner.py` - Data cleaning script for Alaska candidate data
- `colorado_cleaner.py` - Data cleaning script for Colorado candidate data  
- `delaware_cleaner.py` - Data cleaning script for Delaware candidate data
- `georgia_cleaner.py` - Data cleaning script for Georgia candidate data
- `cleaned_data/` - Directory containing processed and cleaned data files
- `Raw State Data - Current/` - Directory containing original state data files
- `requirements.txt` - Python dependencies

## Setup

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Place your raw state data files in the `Raw State Data - Current/` directory
4. Run the appropriate cleaner script for your state

## Usage

Each state has its own cleaning script. For example, to clean Georgia data:

```bash
python georgia_cleaner.py
```

## Data Sources

This project processes candidate filing data from state election offices and other official sources.

## Requirements

See `requirements.txt` for the complete list of Python dependencies.
