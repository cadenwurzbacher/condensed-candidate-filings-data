# Quick Reference Guide

## Common Commands

### Run the Pipeline
```bash
python run_pipeline.py
```

### Test Database Connection
```bash
python scripts/test_db_connection.py
```

### Setup Supabase Environment
```bash
python scripts/setup_supabase.py
```

## File Locations

### Input Data
- **Raw Files**: `data/raw/`
- **File Formats**: .xlsx, .csv, .xls

### Output Data
- **Processed Files**: `data/processed/` (1 file per state)
- **Final Outputs**: `data/final/`
- **Reports**: `data/reports/`
- **Logs**: `data/logs/`

## State Cleaner Functions

### Required Function Signature
```python
def clean_[state]_candidates(input_file: str, output_dir: str = None) -> pd.DataFrame:
    """
    Clean and standardize [state] candidate data.
    
    Args:
        input_file: Path to raw data file
        output_dir: Directory to save cleaned data
        
    Returns:
        Cleaned DataFrame with standardized schema
    """
```

### Required Output Schema
```python
REQUIRED_COLUMNS = [
    'election_year', 'election_type', 'office', 'district', 'candidate_name',
    'first_name', 'middle_name', 'last_name', 'prefix', 'suffix', 'nickname',
    'full_name_display', 'party', 'phone', 'email', 'address', 'website',
    'state', 'original_name', 'original_state', 'original_election_year',
    'original_office', 'original_filing_date', 'id', 'stable_id', 'county',
    'city', 'zip_code', 'filing_date', 'election_date', 'facebook', 'twitter'
]
```

## Environment Variables

### Required (.env file)
```bash
SUPABASE_HOST=your-supabase-host
SUPABASE_PORT=5432
SUPABASE_USER=your-username
SUPABASE_PASSWORD=your-password
SUPABASE_DB=postgres
```

### Optional
```bash
SUPABASE_SSL_MODE=require
SUPABASE_CONNECT_TIMEOUT=30
```

## Recent Fixes Applied

### Import Path Issues ✅
- Fixed relative imports for `office_standardizer` and `database` modules
- Corrected import paths throughout the pipeline

### Error Handling ✅
- Added comprehensive try-catch blocks around all critical operations
- Added data validation checks for empty DataFrames
- Added graceful fallbacks when operations fail
- Added detailed error logging with full tracebacks

### Database Configuration ✅
- Fixed environment variable handling
- Added proper error messages for missing credentials
- Removed fallback defaults that would always fail

### Data Processing Robustness ✅
- Added validation for file operations
- Added checks for missing columns
- Added error handling for data transformations
- Added progress tracking throughout the pipeline

## Troubleshooting

### Pipeline Won't Start
1. Check Python version: `python --version` (needs 3.8+)
2. Install dependencies: `pip install -r requirements.txt`
3. Verify file permissions on data directories
4. Check .env file exists and has correct values
5. **NEW**: Run `python -c "from src.pipeline.main_pipeline import MainPipeline; print('Imports successful')"` to test imports

### Database Connection Issues
1. Verify Supabase credentials in .env
2. Check network connectivity to Supabase
3. Verify SSL settings
4. Check firewall/security group settings

### Memory Issues
1. Reduce batch sizes in main_pipeline.py
2. Enable data sampling for large datasets
3. Process states individually instead of all at once
4. Increase system RAM if available

### File Processing Errors
1. Check file formats (.xlsx, .csv, .xls)
2. Verify file permissions
3. **NEW**: Check logs in `data/logs/` for detailed error information
4. **NEW**: Pipeline now provides step-by-step progress tracking

### New Monitoring Features
- **Progress Tracking**: Pipeline shows completion status for each step
- **Detailed Logging**: All operations logged with timestamps and error details
- **Graceful Degradation**: Pipeline continues running even if individual steps fail
- **Data Validation**: Automatic checks for empty data and missing columns
3. Check for corrupted files
4. Review state cleaner error handling

## Performance Tips

### For Large Datasets
- Use data sampling in audit functions
- Process states sequentially if memory is limited
- Clean up temporary files regularly
- Monitor system resources during processing

### For Frequent Updates
- Keep only essential columns in processed files
- Use efficient file formats (.xlsx for small, .csv for large)
- Implement incremental processing where possible
- Cache frequently accessed data structures

## Log Analysis

### Log File Location
```
data/logs/pipeline_run_YYYYMMDD_HHMMSS.log
```

### Common Log Messages
- `✅ [state] cleaned successfully`: State processing completed
- `❌ Error cleaning [state]`: State processing failed
- `Removed [X] duplicate records`: Deduplication results
- `Database upload completed`: Database operation success

### Error Patterns
- `ModuleNotFoundError`: Missing state cleaner
- `'DataFrame' object has no attribute 'str'`: Data type issue
- `Connection refused`: Database connectivity problem
- `Permission denied`: File access issue

## Data Quality Checks

### Column Consistency
- All files should have the same column structure
- Column names should match required schema
- Data types should be consistent

### Record Completeness
- Check for missing required fields
- Verify address parsing accuracy
- Validate office name standardization
- Review party name consistency

### Duplicate Detection
- Exact duplicates are automatically removed
- Check for near-duplicates manually
- Verify stable_id generation
- Review deduplication logic

## Adding New States

### 1. Create Cleaner File
```python
# src/pipeline/state_cleaners/new_state_cleaner.py
def clean_new_state_candidates(input_file: str, output_dir: str = None) -> pd.DataFrame:
    # Implementation here
    pass
```

### 2. Add to Main Pipeline
```python
# In src/pipeline/main_pipeline.py
from state_cleaners.new_state_cleaner import clean_new_state_candidates

self.state_cleaners = {
    # ... existing states ...
    'new_state': clean_new_state_candidates,
}
```

### 3. Test with Sample Data
- Place raw data file in `data/raw/`
- Run pipeline for single state
- Verify output schema and quality
- Check logs for any errors

## Database Operations

### Check Table Schema
```sql
-- View staging table structure
\d staging_candidates

-- View production table structure  
\d filings

-- Check record counts
SELECT COUNT(*) FROM staging_candidates;
SELECT COUNT(*) FROM filings;
```

### Monitor Upload Progress
- Check logs for upload status
- Verify record counts match expected
- Monitor database performance during upload
- Check for constraint violations

## Maintenance Tasks

### Daily
- Review pipeline logs
- Check for processing errors
- Verify output file counts
- Monitor database performance

### Weekly
- Clean up old log files
- Review data quality metrics
- Check for duplicate files
- Verify database backups

### Monthly
- Update dependencies
- Review performance metrics
- Clean up old processed files
- Validate data quality standards
