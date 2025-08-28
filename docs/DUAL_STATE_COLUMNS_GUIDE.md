# Dual State Columns Implementation Guide

## Overview

This guide explains how to implement dual state columns in state cleaners to distinguish between:

1. **Election State** (`state`): Where the candidate is running for office (comes from the state cleaner)
2. **Address State** (`address_state`): Where the candidate actually lives/mails from (extracted from address)

## Why This Matters

- **Campaign mailings**: Need the address state for mailing
- **Geographic analysis**: Want to filter by election state for "Alaska candidates" 
- **Consulting**: May want to filter by election state vs. address state
- **Data quality**: Many candidates run in one state but live in another

## Implementation Pattern

### 1. Update Function Signature

Change the `parse_address` function to return 4 values instead of 3:

```python
# Before
def parse_address(address_str: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:

# After  
def parse_address(address_str: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
```

### 2. Update Return Statements

All return statements must return 4 values: `(city, zip_code, county, address_state)`

```python
# Before
return city, zip_code, county

# After
return city, zip_code, county, address_state
```

### 3. Update Column Assignment

```python
# Before
geographic_data = df['address'].apply(parse_address)
df['city'] = [data[0] for data in geographic_data]
df['zip_code'] = [data[1] for data in geographic_data] 
df['county'] = [data[2] for data in geographic_data]

# After
geographic_data = df['address'].apply(parse_address)
df['city'] = [data[0] for data in geographic_data]
df['zip_code'] = [data[1] for data in geographic_data]
df['county'] = [data[2] for data in geographic_data]
df['address_state'] = [data[3] for data in geographic_data]
```

### 4. Add to Required Columns

```python
required_columns = [
    'id', 'stable_id', 'county', 'city', 'zip_code', 'address_state', 'filing_date',
    'election_date', 'facebook', 'twitter', 'prefix', 'suffix', 'nickname'
]
```

## State-by-State Implementation Checklist

### ✅ Completed
- [x] **Alaska** (`alaska_cleaner.py`) - Implemented and tested

### 🔄 Pending Implementation
- [ ] **Virginia** (`virginia_cleaner.py`) - Needs dual state columns
- [ ] **Arizona** (`arizona_cleaner.py`) - Needs dual state columns  
- [ ] **Arkansas** (`arkansas_cleaner.py`) - Needs dual state columns
- [ ] **Colorado** (`colorado_cleaner.py`) - Needs dual state columns
- [ ] **Delaware** (`delaware_cleaner.py`) - Needs dual state columns
- [ ] **Georgia** (`georgia_cleaner.py`) - Needs dual state columns
- [ ] **Idaho** (`idaho_cleaner.py`) - Needs dual state columns
- [ ] **Illinois** (`illinois_cleaner.py`) - Needs dual state columns
- [ ] **Indiana** (`indiana_cleaner.py`) - Needs dual state columns
- [ ] **Iowa** (`iowa_cleaner.py`) - Needs dual state columns
- [ ] **Kansas** (`kansas_cleaner.py`) - Needs dual state columns
- [ ] **Kentucky** (`kentucky_cleaner.py`) - Needs dual state columns
- [ ] **Louisiana** (`louisiana_cleaner.py`) - Needs dual state columns
- [ ] **Maryland** (`maryland_cleaner.py`) - Needs dual state columns
- [ ] **Missouri** (`missouri_cleaner.py`) - Needs dual state columns
- [ ] **Montana** (`montana_cleaner.py`) - Needs dual state columns
- [ ] **Nebraska** (`nebraska_cleaner.py`) - Needs dual state columns
- [ ] **New Mexico** (`new_mexico_cleaner.py`) - Needs dual state columns
- [ ] **New York** (`new_york_cleaner.py`) - Needs dual state columns
- [ ] **North Carolina** (`north_carolina_cleaner.py`) - Needs dual state columns
- [ ] **Oklahoma** (`oklahoma_cleaner.py`) - Needs dual state columns
- [ ] **Oregon** (`oregon_cleaner.py`) - Needs dual state columns
- [ ] **Pennsylvania** (`pennsylvania_cleaner.py`) - Needs dual state columns
- [ ] **South Carolina** (`south_carolina_cleaner.py`) - Needs dual state columns
- [ ] **South Dakota** (`south_dakota_cleaner.py`) - Needs dual state columns
- [ ] **Vermont** (`vermont_cleaner.py`) - Needs dual state columns
- [ ] **Washington** (`washington_cleaner.py`) - Needs dual state columns
- [ ] **West Virginia** (`west_virginia_cleaner.py`) - Needs dual state columns
- [ ] **Wyoming** (`wyoming_cleaner.py`) - Needs dual state columns

## Testing Template

Use this test script pattern for each state:

```python
#!/usr/bin/env python3
"""
Test script for [STATE] cleaner with dual state columns
"""

import pandas as pd
import sys
import os

# Add the src directory to the path
sys.path.append('src/pipeline/state_cleaners')

try:
    from [state]_cleaner import [State]Cleaner
    
    print("Testing [STATE] cleaner with dual state columns...")
    
    # Load raw [STATE] data
    raw_file = 'data/raw/[state]_candidates_[year].xlsx'
    if not os.path.exists(raw_file):
        print(f"Raw file not found: {raw_file}")
        exit(1)
    
    print(f"Loading raw data from: {raw_file}")
    df = pd.read_excel(raw_file)
    print(f"Raw data shape: {df.shape}")
    
    # Add election year from filename (simulating main pipeline)
    df['election_year'] = [YEAR]
    print(f"Added election_year column: {df['election_year'].count()} rows")
    
    # Test the cleaner
    cleaner = [State]Cleaner(output_dir='data/processed')
    cleaned_df = cleaner.clean_[state]_data(df)
    
    print(f"\nCleaned data shape: {cleaned_df.shape}")
    
    print(f"\nKey columns data counts:")
    print(f"- election_year: {cleaned_df['election_year'].count()} rows")
    print(f"- state (election state): {cleaned_df['state'].count()} rows")
    print(f"- address_state: {cleaned_df['address_state'].count()} rows")
    print(f"- city: {cleaned_df['city'].count()} rows")
    print(f"- zip_code: {cleaned_df['zip_code'].count()} rows")
    print(f"- county: {cleaned_df['county'].count()} rows")
    
    print(f"\nSample data showing election state vs address state:")
    sample_data = cleaned_df[['address', 'state', 'address_state', 'city']].dropna(subset=['address_state']).head(10)
    print(sample_data.to_string())
    
    print(f"\nUnique address states found:")
    unique_address_states = cleaned_df['address_state'].dropna().unique()
    print(unique_address_states)
    
    print("\n[STATE] cleaner dual state test completed successfully!")
    
except Exception as e:
    print(f"Error testing [STATE] cleaner: {e}")
    import traceback
    traceback.print_exc()
```

## Expected Results

After implementation, each state cleaner should:

1. **Preserve election state**: The `state` column should always contain the state where the candidate is running
2. **Extract address state**: The `address_state` column should contain the state from the candidate's address
3. **Handle cross-state candidates**: Many candidates will have different election vs address states
4. **Maintain data quality**: All existing functionality should continue to work

## Example Output

```
Sample data showing election state vs address state:
                                              address   state address_state                      city
3                 PO Box 869 Crawfordsville, IN 47933  Alaska            IN            Crawfordsville
7   420 N. McKinley St. Ste. 111-512 Corona, CA 92879  Alaska            CA                    Corona
8              25 Guatemala Dr. Martinsburg, WV 25403  Alaska            WV               Martinsburg
9          125 Circle Dr. Cross Cross Lakes, WV 25313  Alaska            WV         Cross Cross Lakes
10            142 Webster St. NE Washington, DC 20011  Alaska            DC                Washington
```

## Next Steps

1. **Implement Virginia cleaner** - Use Alaska as template
2. **Systematically update all other state cleaners** - Follow the same pattern
3. **Test each implementation** - Use the testing template
4. **Update documentation** - Document any state-specific address parsing logic
5. **Validate data quality** - Ensure no data loss during migration

## Notes

- **Address parsing complexity**: Some states may have different address formats
- **County handling**: Some states use different geographic divisions (parishes, boroughs, etc.)
- **Testing**: Always test with real data to ensure address parsing works correctly
- **Backward compatibility**: Existing data should continue to work after updates
