# Date Tracking Guide

## Overview

The pipeline now includes comprehensive date tracking for all candidates to monitor when they were first discovered and when they were last updated.

## New Fields

### `first_added_date`
- **Purpose**: Records when a candidate was first discovered by the pipeline
- **Behavior**: 
  - For new candidates: Set to current timestamp
  - For existing candidates: Preserved from database (never overwritten)
- **Use Case**: Track candidate discovery timeline, identify new vs. existing candidates

### `last_updated_date`
- **Purpose**: Records when a candidate record was last modified/updated
- **Behavior**: Always set to current timestamp on every pipeline run
- **Use Case**: Track data freshness, identify recently updated candidates

## Implementation Details

### Database Schema Changes

Both `staging_candidates` and `filings` tables now include:
```sql
first_added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
last_updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
```

### Pipeline Flow

1. **Phase 2: ID Generation**
   - Generate stable ID from candidate data
   - Check database for existing candidate
   - Set appropriate dates based on existence

2. **Database Check Logic**
   - Checks only `filings` table (production data)
   - Uses existing dates if found, sets new dates if not

### Date Logic

```python
if existing_candidate:
    # Preserve existing first_added_date
    first_added_date = existing_candidate.first_added_date
    # Update last_updated_date to now
    last_updated_date = current_timestamp
else:
    # New candidate - set both dates to now
    first_added_date = current_timestamp
    last_updated_date = current_timestamp
```

## Migration

### Adding Columns to Existing Tables

Run the migration script to add columns to existing tables:
```bash
python scripts/cleaner/add_date_tracking_columns.py
```

This script will:
- Add missing columns to both tables
- Set initial dates for existing records
- Preserve existing `created_at` timestamps

### New Pipeline Runs

New pipeline runs will automatically:
- Generate appropriate dates for all candidates
- Preserve existing `first_added_date` values
- Update `last_updated_date` for all records

## Usage Examples

### Query New Candidates
```sql
-- Find candidates discovered in the last 30 days
SELECT * FROM filings 
WHERE first_added_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY first_added_date DESC;
```

### Query Recently Updated
```sql
-- Find candidates updated in the last 7 days
SELECT * FROM filings 
WHERE last_updated_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY last_updated_date DESC;
```

### Track Candidate Lifecycle
```sql
-- See candidate discovery and update patterns
SELECT 
    stable_id,
    full_name_display,
    first_added_date,
    last_updated_date,
    (last_updated_date - first_added_date) as days_since_discovery
FROM filings 
ORDER BY first_added_date DESC;
```

## Benefits

1. **Data Lineage**: Track when candidates were first discovered
2. **Change Detection**: Identify recently updated records
3. **Audit Trail**: Maintain history of data modifications
4. **Incremental Processing**: Only process new/updated candidates
5. **Quality Monitoring**: Track data freshness over time

## Testing

Test the date tracking functionality:
```bash
python scripts/test_date_tracking.py
```

This will verify:
- Date columns are created correctly
- Existing candidates preserve their first_added_date
- New candidates get current timestamps
- All records get updated last_updated_date

## Notes

- **Timezone**: All timestamps use the server's local timezone
- **Precision**: Timestamps include seconds for precise tracking
- **Performance**: Database queries are optimized with indexes on date columns
- **Backward Compatibility**: Existing pipeline runs continue to work unchanged
