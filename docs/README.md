# CandidateFilings.com Pipeline Documentation

## Overview
This repository contains a comprehensive data processing pipeline for political candidate filings data from multiple US states. The pipeline processes raw data files, cleans and standardizes the data, and uploads it to a Supabase database.

## Pipeline Architecture

### 1. Main Pipeline (`src/pipeline/main_pipeline.py`)
The central orchestrator that coordinates all data processing steps:

- **State Cleaning**: Processes raw data from 24 states
- **Office Standardization**: Standardizes office names across all states
- **National Standardization**: Applies consistent formatting for parties, addresses, etc.
- **Deduplication**: Removes exact duplicate records
- **Data Audit**: Quality checks and validation
- **Database Upload**: Uploads to staging and production tables

### 2. State Cleaners (`src/pipeline/state_cleaners/`)
Individual cleaners for each state that handle:
- State-specific data formats
- Address parsing and standardization
- Office name cleaning
- Party name standardization
- Contact information cleaning

**Supported States**: Alaska, Arizona, Arkansas, Colorado, Delaware, Georgia, Idaho, Illinois, Indiana, Kansas, Kentucky, Louisiana, Missouri, Montana, Nebraska, New Mexico, Oklahoma, Oregon, South Carolina, South Dakota, Vermont, Washington, West Virginia, Wyoming

### 3. Office Standardizer (`src/pipeline/office_standardizer.py`)
Standardizes office names across all states using fuzzy matching and categorization.

### 4. Database Management (`src/config/database.py`)
Handles Supabase PostgreSQL connections and operations.

## Data Flow

```
Raw Data → State Cleaners → Office Standardization → National Standardization → Deduplication → Data Audit → Staging Database → Production Database
```

## Database Workflow

The pipeline uses a **two-phase approach** for safe database updates:

### Phase 1: Staging Upload (Automatic)
- Main pipeline automatically uploads processed data to `staging_candidates` table
- Data is available for review before going live
- Run with: `python scripts/run_pipeline.py`

### Phase 2: Production Upload (Manual Review)
- After reviewing staging data, manually promote to production
- Moves data from `staging_candidates` to `filings` table
- Run with: `python scripts/move_to_production.py`

### Review Staging Data
- Check staging data without moving to production: `python scripts/check_staging.py`
- Review record counts, data quality, and sample records
- Only promote to production when you're satisfied with the data

## File Structure

```
src/
├── pipeline/
│   ├── main_pipeline.py          # Main orchestrator
│   ├── office_standardizer.py    # Office name standardization
│   └── state_cleaners/          # State-specific data cleaners
├── config/
│   └── database.py              # Database connection management
└── scripts/
    ├── run_pipeline.py          # Pipeline runner script
    ├── move_to_production.py    # Move staging data to production
    ├── check_staging.py         # Review staging data
    ├── setup_supabase.py        # Supabase setup helper
    └── test_db_connection.py    # Database connection tester

data/
├── raw/                         # Raw state data files
├── processed/                   # Cleaned state data (1 file per state)
├── final/                       # Final pipeline outputs
├── reports/                     # Data audit reports
└── logs/                        # Pipeline execution logs
```

## Usage

### Quick Start
```bash
# 1. Set up environment variables
cp scripts/env_template.txt .env
# Edit .env with your Supabase credentials

# 2. Add raw data files to data/raw/
# Place state data files in the raw directory

# 3. Run the pipeline
python run_pipeline.py
```

### Environment Variables
```bash
SUPABASE_HOST=your-supabase-host
SUPABASE_PORT=5432
SUPABASE_USER=your-username
SUPABASE_PASSWORD=your-password
SUPABASE_DB=postgres
```

## Features

### Multi-File Support
- Automatically detects and merges multiple raw files per state
- Handles different file formats (.xlsx, .csv, .xls)
- Supports data from multiple years

### Data Quality
- Comprehensive data validation
- Address parsing and standardization
- Office name categorization
- Party name standardization
- Duplicate detection and removal

### Automation
- One-click pipeline execution
- Automatic file cleanup
- Log rotation and management
- Error handling and recovery

## Output Files

The pipeline generates several output files:

1. **State Cleaned Files**: One cleaned file per state in `data/processed/`
2. **Office Standardized**: Combined data with standardized office names
3. **Nationally Standardized**: Final standardized dataset
4. **Deduplicated**: Final dataset with duplicates removed
5. **Audit Report**: Comprehensive data quality report

## Database Schema

### Staging Table (`staging_candidates`)
- Intermediate processing table
- Used for data validation and quality checks

### Production Table (`filings`)
- Final production data
- Cleaned, standardized, and deduplicated records

## Monitoring and Maintenance

### Logs
- Pipeline execution logs in `data/logs/`
- Automatic log rotation (keeps last 5 files)
- Detailed error reporting

### Cleanup
- Automatic removal of old processed files
- Temporary file cleanup
- One file per state maintained

## Troubleshooting

### Common Issues
1. **Database Connection**: Check Supabase credentials and network access
2. **Missing Dependencies**: Install requirements with `pip install -r requirements.txt`
3. **File Permissions**: Ensure write access to data directories
4. **Memory Issues**: Large datasets may require increased memory allocation

### Debug Mode
Enable detailed logging by modifying the logging level in `main_pipeline.py`.

## Contributing

### Adding New States
1. Create a new cleaner in `src/pipeline/state_cleaners/`
2. Implement the required cleaning methods
3. Add the cleaner to the main pipeline mapping
4. Test with sample data

### Modifying Standards
- Office standardization rules in `office_standardizer.py`
- Party name mappings in state cleaners
- Address parsing logic in individual cleaners

## Performance

### Optimization Features
- Data sampling for large datasets during audits
- Parallel processing capabilities
- Efficient file I/O operations
- Memory-conscious data handling

### Scalability
- Designed to handle 100K+ records
- Modular architecture for easy scaling
- Configurable batch sizes and timeouts

## Security

- Environment variable-based configuration
- No hardcoded credentials
- Secure database connections with SSL
- Input validation and sanitization

## Support

For issues or questions:
1. Check the logs in `data/logs/`
2. Review the audit reports in `data/reports/`
3. Verify database connectivity
4. Check file permissions and paths
