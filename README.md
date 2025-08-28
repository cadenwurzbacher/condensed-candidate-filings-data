# CandidateFilings.com Data Pipeline

A comprehensive, one-click data processing pipeline for candidate filing data from all 50 states.

## 🎯 What This Pipeline Does

**One script to rule them all** - this pipeline handles the complete data processing workflow:

1. **State Cleaning** - Runs all 25 state-specific data cleaners
2. **Address Fixing** - Fixes address separation issues with state-specific logic
3. **Office Standardization** - Standardizes office names across all states
4. **National Standardization** - Standardizes party names, addresses, and other fields
5. **Deduplication** - Removes exact duplicate records
6. **Data Audit** - Comprehensive quality reporting
7. **Database Upload** - Uploads to Supabase with staging/production workflow

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Raw state data files in `data/raw/`
- Supabase database configured (optional)

### Run the Pipeline
```bash
# One command to run everything
python run_pipeline.py
```

That's it! The pipeline will:
- Process all state data automatically
- Generate comprehensive reports
- Save processed files to organized directories
- Upload to database (if configured)

## 📁 Repository Structure

```
├── run_pipeline.py              # 🚀 One-click pipeline runner
├── src/
│   ├── pipeline/
│   │   ├── main_pipeline.py     # 🎯 Main orchestrator (does everything)
│   │   ├── state_cleaners/      # 📋 State-specific data cleaners
│   │   ├── office_standardizer.py # 🏛️ Office name standardization
│   │   ├── fix_address_separation_issues.py # 🏠 Address fixing
│   │   ├── address_parsing_audit.py # 🔍 Address quality audit
│   │   ├── address_separation_audit.py # 📊 Address format audit
│   │   ├── data_audit.py        # 📈 Comprehensive data audit
│   │   ├── verify_address_fixes.py # ✅ Address fix verification
│   │   ├── kentucky_address_fix_example.py # 🐎 Kentucky-specific fixes
│   │   └── missouri_address_fix_example.py # 🐻 Missouri-specific fixes
│   └── config/
│       └── database.py          # 🗄️ Database connection management
├── data/
│   ├── raw/                     # 📥 Raw state data files
│   ├── processed/               # 🔧 Intermediate processed files
│   ├── final/                   # 🎯 Final output files
│   └── reports/                 # 📊 Audit and quality reports
├── scripts/                     # 🛠️ Setup and utility scripts
└── docs/                        # 📚 Documentation
```

## 🔄 Pipeline Flow

```
Raw State Data → State Cleaners → Address Fixing → Office Standardization → 
National Standardization → Deduplication → Data Audit → Database Upload
```

### Step-by-Step Breakdown

1. **State Cleaning** (`run_state_cleaners`)
   - Loads raw data for each state
   - Runs state-specific cleaning logic
   - Saves cleaned data to `data/processed/`

2. **Address Fixing** (`run_address_fixing`)
   - Combines all state data
   - Applies generic address fixes
   - Uses state-specific logic for Kentucky, Missouri, etc.
   - Saves address-fixed data

3. **Office Standardization** (`run_office_standardization`)
   - Standardizes office names across all states
   - Uses confidence scoring for accuracy
   - Saves office-standardized data

4. **National Standardization** (`run_national_standardization`)
   - Standardizes party names (Democratic, Republican, etc.)
   - Parses and standardizes addresses
   - Applies consistent formatting
   - Adds metadata and versioning

5. **Deduplication** (`run_deduplication`)
   - Removes exact duplicate records
   - Reports on duplicates found and removed

6. **Data Audit** (`run_data_audit`)
   - Comprehensive quality analysis
   - Address field validation
   - Column consistency checking
   - Generates detailed reports

7. **Database Upload** (`upload_to_database`)
   - Uploads to staging table
   - Moves to production table
   - Handles conflicts and updates

## 🎛️ Configuration

### Environment Variables
Create a `.env` file with your Supabase credentials:

```bash
# Supabase Database Connection
SUPABASE_HOST=aws-0-us-east-2.pooler.supabase.com
SUPABASE_PORT=5432
SUPABASE_DATABASE=postgres
SUPABASE_USER=postgres.bnvpsoppbufldabquoec
SUPABASE_PASSWORD=YOUR-DATABASE-PASSWORD-HERE
```

### Data Directories
The pipeline automatically creates these directories:
- `data/raw/` - Put your raw state data files here
- `data/processed/` - Intermediate files (cleaned, fixed, standardized)
- `data/final/` - Final output files
- `data/reports/` - Quality and audit reports

## 📊 Output Files

The pipeline generates several output files:

- **Cleaned Data**: `{state}_cleaned_{timestamp}.xlsx`
- **Address Fixed**: `{state}_addresses_fixed_{timestamp}.xlsx`
- **Office Standardized**: `all_states_offices_standardized_{timestamp}.xlsx`
- **Nationally Standardized**: `all_states_nationally_standardized_{timestamp}.xlsx`
- **Final Output**: `all_states_deduplicated_{timestamp}.xlsx`
- **Audit Reports**: `comprehensive_audit_{timestamp}.xlsx`

## 🔧 Customization

### Adding New States
1. Create a new cleaner in `src/pipeline/state_cleaners/`
2. Add the import and mapping in `main_pipeline.py`
3. Follow the existing pattern for consistency

### State-Specific Address Fixes
1. Create a new fixer (like `kentucky_address_fix_example.py`)
2. Add it to the `AddressFixer` class in `fix_address_separation_issues.py`
3. The pipeline will automatically use it for that state

### Custom Standardizations
1. Modify the `_standardize_parties()` method for party names
2. Update `_parse_addresses()` for address parsing
3. Add new methods to `_apply_national_standards()`

## 📈 Monitoring and Quality

### Built-in Auditing
- **Address Quality**: Checks for mixed separators, formatting issues
- **Data Consistency**: Validates column presence and data types
- **Processing Quality**: Tracks improvements through each step
- **Comprehensive Reporting**: Excel reports with multiple sheets

### Logging
- Detailed logging to console and files
- Timestamped log files for each run
- Error tracking and reporting
- Progress indicators for each step

## 🚨 Troubleshooting

### Common Issues

1. **No Raw Data Found**
   - Ensure files are in `data/raw/`
   - Check file extensions (.xlsx, .csv, .xls)

2. **State Cleaner Errors**
   - Verify state cleaner functions exist
   - Check import statements in `main_pipeline.py`

3. **Database Connection Issues**
   - Verify `.env` file exists and has correct credentials
   - Check Supabase connection settings

4. **Memory Issues with Large Datasets**
   - Process states individually if needed
   - Use chunked processing for very large files

### Getting Help

- Check the log files in the pipeline directory
- Review the audit reports in `data/reports/`
- Verify all required dependencies are installed

## 🔄 Daily Updates

This pipeline is designed for **sustainable daily updates**:

1. **Add new raw data** to `data/raw/`
2. **Run the pipeline**: `python run_pipeline.py`
3. **Review reports** in `data/reports/`
4. **Check final output** in `data/final/`

The pipeline automatically:
- Handles new data without conflicts
- Removes exact duplicates
- Maintains data quality standards
- Generates fresh audit reports

## 📚 Dependencies

Install required packages:
```bash
pip install -r requirements.txt
```

Key dependencies:
- `pandas` - Data processing
- `sqlalchemy` - Database operations
- `psycopg2-binary` - PostgreSQL connection
- `openpyxl` - Excel file handling
- `python-dotenv` - Environment variable management

## 🎉 Success!

You now have a **one-click pipeline** that:
- ✅ Processes all 25 states automatically
- ✅ Handles address fixing with state-specific logic
- ✅ Standardizes offices and party names
- ✅ Removes duplicates and ensures quality
- ✅ Generates comprehensive reports
- ✅ Uploads to database (optional)
- ✅ Is sustainable for daily updates

**Just run `python run_pipeline.py` and everything happens automatically!**
