# CandidateFilings.com Data Pipeline

A comprehensive data processing pipeline for candidate filing data from all 50 US states, designed to standardize and clean candidate information for consistent analysis and storage.

## 🚀 Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full pipeline (no database)
python run_pipeline.py

# Run with database integration
python run_pipeline.py --with-db
```

## 📋 Overview

This pipeline processes raw candidate filing data from state election offices and transforms it into a standardized format suitable for analysis, reporting, and database storage. The system handles data from all 50 US states with state-specific cleaning and national standardization.

### Key Features

- **50-State Support**: Handles data from all US states with state-specific processing
- **5-Phase Pipeline**: Structured processing from raw data to final output
- **Centralized Standards**: All office and party mappings centralized in national standards
- **Comprehensive Cleaning**: Addresses data quality issues across all fields
- **Database Integration**: Optional PostgreSQL/Supabase integration
- **Extensible Architecture**: Easy to add new states or modify processing logic

## 🏗️ Architecture

### Pipeline Phases

1. **Structural Parsing** (`structural_cleaners/`)
   - Extracts structured data from raw files (Excel, CSV, TXT)
   - Handles state-specific file formats and column mappings
   - Generates initial structured DataFrame

2. **ID Generation** (`id_generator/`)
   - Creates stable, unique IDs for each candidate
   - Handles duplicate detection and resolution
   - Ensures data integrity across processing phases

3. **State-Specific Cleaning** (`state_cleaners/`)
   - Applies state-specific data cleaning rules
   - Handles name parsing, address formatting, district extraction
   - **No office or party standardization** (moved to national phase)

4. **National Standards** (`national_standards/`)
   - **Office Standardization**: Maps all office names to national standards
   - **Party Standardization**: Maps all party names to national standards
   - Cross-state consistency and data quality checks

5. **Output Generation** (`output_generator/`)
   - Creates final standardized output files
   - Generates reports and data quality metrics
   - Optional database integration

### Directory Structure

```
src/pipeline/
├── structural_cleaners/     # Phase 1: Raw data extraction
├── id_generator/           # Phase 2: Unique ID generation
├── state_cleaners/         # Phase 3: State-specific cleaning
├── national_standards/     # Phase 4: Cross-state standardization
├── output_generator/       # Phase 5: Final output
├── party_standardizer/     # Party name standardization
├── office_standardizer.py  # Office name standardization
└── main_pipeline.py        # Main orchestration
```

## 📊 Data Format Standards

### Final Output Schema

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | string | Unique stable identifier | `HI_2024_001` |
| `state` | string | State name (proper case) | `Hawaii` |
| `election_year` | int | Election year | `2024` |
| `candidate_name` | string | Full candidate name | `John Smith` |
| `first_name` | string | First name | `John` |
| `middle_name` | string | Middle name(s) | `Michael` |
| `last_name` | string | Last name | `Smith` |
| `prefix` | string | Name prefix | `Dr.` |
| `suffix` | string | Name suffix | `Jr.` |
| `nickname` | string | Nickname/alias | `Mike` |
| `full_name_display` | string | Display name | `Dr. John M. Smith Jr.` |
| `office` | string | **Standardized office name** | `State House` |
| `district` | string | District number | `11` |
| `party` | string | **Standardized party name** | `Republican` |
| `county` | string | County name | `Honolulu County` |
| `address` | string | Full address | `123 Main St, Honolulu, HI 96801` |
| `email` | string | Email address | `john.smith@email.com` |
| `phone` | string | Phone number | `(808) 555-0123` |
| `filing_date` | string | Filing date | `2024-01-15` |
| `raw_file` | string | Source file path | `data/raw/hawaii/2024_candidates.xlsx` |

### Office Standardization

All office names are mapped to these national standards:

| Category | Standard Names | Examples |
|----------|----------------|----------|
| **Federal** | `US President`, `US Senate`, `US House` | President, Senator, Representative |
| **State Executive** | `Governor`, `Lieutenant Governor`, `Attorney General` | Governor, Lt. Governor, AG |
| **State Legislature** | `State Senate`, `State House` | State Senator, State Representative |
| **Local** | `Mayor`, `County Commissioner`, `City Council` | Mayor, Commissioner, Council Member |
| **Judicial** | `Supreme Court Justice`, `District Court Judge` | Justice, Judge |
| **Special** | `School Board Member`, `Special District` | School Board, District Trustee |

### Party Standardization

All party names are mapped to these national standards:

| Standard Name | Common Variations |
|---------------|-------------------|
| `Democratic` | Dem, Democrat, Democratic Party |
| `Republican` | Rep, Republican Party, GOP |
| `Independent` | Ind, Independent Party |
| `Libertarian` | Lib, Libertarian Party |
| `Green` | Green Party, Green Party of the United States |
| `Unaffiliated` | NPA, No Party Affiliation, Unenrolled |
| `Nonpartisan` | Non, Nonpartisan, Nonpartisan Special |
| `Constitution Party` | Constitution, Constitutional Party |
| `Reform Party` | Reform, Reform Party |

### Name Parsing Standards

- **Prefixes**: `Dr.`, `Mr.`, `Mrs.`, `Ms.`, `Hon.`, `Sen.`, `Rep.`, `Gov.`
- **Suffixes**: `Jr.`, `Sr.`, `II`, `III`, `IV`, `V`, `VI`, `VII`, `VIII`, `IX`, `X`
- **Nicknames**: Extracted from parentheses, e.g., `John (Mike) Smith` → `Mike`
- **Display Name**: Combines all components: `Dr. John M. Smith Jr.`

### Address Standards

- **Format**: `Street Address, City, State ZIP`
- **County**: Proper case with "County" suffix
- **State**: Full state name (no abbreviations)
- **ZIP**: 5-digit format

### District Standards

- **Format**: Numeric only (no "District" prefix)
- **Special Cases**: 
  - `Statewide` for statewide offices
  - `At-Large` for at-large positions
  - Roman numerals converted to Arabic (e.g., `VII` → `7`)

## 🔧 Configuration

### Environment Variables

```bash
# Database (optional)
DATABASE_URL=postgresql://user:pass@host:port/db
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-key

# File paths
RAW_DATA_DIR=data/raw
PROCESSED_DATA_DIR=data/processed
FINAL_DATA_DIR=data/final
```

### State Configuration

Each state has its own configuration in the respective cleaner:

```python
# Example: Hawaii configuration
self.county_mappings = {
    'honolulu': 'Honolulu County',
    'hawaii': 'Hawaii County',
    # ...
}

# Office mappings removed - handled by national standards
# Party mappings removed - handled by national standards
```

## 📈 Data Quality Metrics

The pipeline tracks and reports on:

- **Completeness**: Percentage of records with required fields
- **Consistency**: Standardization success rates
- **Uniqueness**: Duplicate detection and resolution
- **Validity**: Data format compliance

## 🚀 Adding New States

1. **Create Structural Cleaner** (`structural_cleaners/state_structural_cleaner.py`)
   - Implement `_find_state_files()` method
   - Implement `_extract_record_from_row()` method
   - Handle state-specific file formats

2. **Create State Cleaner** (`state_cleaners/state_cleaner_refactored.py`)
   - Extend `BaseStateCleaner`
   - Implement `_parse_names()` method
   - Add state-specific county mappings
   - **No office or party mappings** (handled nationally)

3. **Update Main Pipeline** (`main_pipeline.py`)
   - Add imports for new cleaners
   - Add to `self.structural_cleaners` and `self.state_cleaners`

4. **Test Integration**
   - Run state-specific tests
   - Verify national standardization works
   - Check data quality metrics

## 🧪 Testing

```bash
# Run all tests
python -m pytest tests/

# Run specific state tests
python -m pytest tests/test_hawaii_integration.py

# Run data quality tests
python -m pytest tests/test_data_quality.py
```

## 📚 Documentation

- [Technical Architecture](docs/TECHNICAL_ARCHITECTURE.md) - Detailed system design
- [State Integration Guide](docs/STATE_INTEGRATION_GUIDE.md) - Adding new states
- [Data Standards Guide](docs/DATA_STANDARDS_GUIDE.md) - Formatting standards
- [API Reference](docs/API_REFERENCE.md) - Code documentation
- [Troubleshooting Guide](docs/TROUBLESHOOTING.md) - Common issues and solutions

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For questions or issues:
- Check the [Troubleshooting Guide](docs/TROUBLESHOOTING.md)
- Review the [API Reference](docs/API_REFERENCE.md)
- Open an issue on GitHub

---

**Last Updated**: December 2024  
**Version**: 2.0.0  
**Status**: Production Ready
