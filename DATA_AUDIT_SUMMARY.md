# Data Audit Summary - CandidateFilings Pipeline

## Overview
**Dataset**: 227,510 records across 30 states  
**Date**: August 27, 2025  
**Pipeline Version**: 1.0

## 🔍 Column Completeness Analysis

### ✅ Well-Populated Columns (100% Complete)
- `office` - All records have office information
- `state` - All records have state information  
- `election_year` - All records have election year
- `address_state` - All records have address state (USPS codes)
- `raw_data` - All records preserve original source data
- `stable_id` - All records have unique stable IDs
- `action_type` - All records marked as INSERT
- `ran_in_primary/general/special` - All records have election type flags
- `full_name_display` - All records have parsed names
- `processing_timestamp` - All records have processing metadata
- `pipeline_version` - All records have version info
- `data_source` - All records have source attribution

### ⚠️ Partially Populated Columns
- `party`: 33.4% complete (75,079 records)
- `county`: 25.7% complete (58,560 records)  
- `district`: 17.9% complete (40,837 records)
- `address`: 29.0% complete (65,866 records)
- `city`: 57.5% complete (130,708 records)
- `zip_code`: 26.9% complete (61,224 records)
- `phone`: 20.3% complete (46,285 records)
- `email`: 16.0% complete (36,353 records)
- `filing_date`: 39.2% complete (89,090 records)

### ❌ Missing Data Issues
- `first_added_date`: 0% complete (all records missing)
- `last_updated_date`: 0% complete (all records missing)
- `facebook`: 0.7% complete (1,693 records)
- `twitter`: 0.4% complete (907 records)

## 🏛️ Office Standardization Issues

### US President Offices
**Status**: ✅ **GOOD** - No district numbers in office names  
**Total**: 309 records  
**Variations**: Properly standardized (U.s. President, US President, President Of The United States)

### US House Offices  
**Status**: ⚠️ **CRITICAL ISSUE** - District numbers embedded in office names  
**Total**: 658 records  
**Problem**: 553 records (84%) have district numbers in office field  
**Examples**:
- `US House Of Representatives District 13` (58 records)
- `US House Of Representatives District 03` (43 records)
- `U.s. House Of Representatives, District 4` (33 records)

**Expected**: Office should be "US House of Representatives", district should be separate field

### US Senate Offices
**Status**: ⚠️ **MINOR ISSUE** - Some district numbers in office names  
**Total**: 106 records  
**Problem**: 4 records have district numbers (e.g., "U.s. Senate 2")  
**Expected**: Office should be "US Senate", district should be separate field

### State House Offices
**Status**: ⚠️ **SIGNIFICANT ISSUE** - Many district numbers in office names  
**Total**: 10,069 records  
**Problem**: 4,535 records (45%) have district numbers in office field  
**Examples**:
- `State Representative Pos. 1` (273 records)
- `State Representative, 23rd District` (31 records)
- `State House Of Representatives, District 25` (21 records)

**Expected**: Office should be "State Representative" or "State House", district should be separate field

### State Senate Offices
**Status**: ⚠️ **SIGNIFICANT ISSUE** - Many district numbers in office names  
**Total**: 1,396 records  
**Problem**: 991 records (71%) have district numbers in office field  
**Examples**:
- `Nc State Senate District 36` (16 records)
- `Nc State Senate District 13` (16 records)
- `Nc State Senate District 19` (15 records)

**Expected**: Office should be "State Senate", district should be separate field

## 👤 Name Parsing Quality

### ✅ Good Name Parsing
- **Alaska**: Perfect name matching between parsed and raw data
- **Kentucky**: Names parsed correctly from structured format
- **North Carolina**: Names parsed correctly from structured format  
- **Illinois**: Names parsed correctly (some in "Last, First" format)

### 📊 Name Parsing by State
- **Kentucky**: 120,653 records - Good parsing
- **North Carolina**: 27,884 records - Good parsing
- **Illinois**: 13,575 records - Good parsing (Last, First format)
- **Indiana**: 8,732 records
- **South Carolina**: 6,292 records

## 🚨 Critical Issues Requiring Immediate Attention

### 1. Office Standardization (HIGH PRIORITY)
- **US House**: 553 records have district numbers embedded in office names
- **State House**: 4,535 records have district numbers embedded in office names  
- **State Senate**: 991 records have district numbers embedded in office names

**Impact**: This makes it impossible to properly categorize offices and districts separately

### 2. Missing Date Fields (MEDIUM PRIORITY)
- `first_added_date`: 0% complete (227,510 missing)
- `last_updated_date`: 0% complete (227,510 missing)

**Impact**: Cannot track when records were first added or last updated

### 3. Low Completeness in Key Fields (MEDIUM PRIORITY)
- `party`: Only 33.4% complete
- `county`: Only 25.7% complete
- `district`: Only 17.9% complete
- `address`: Only 29.0% complete

## 📋 Recommendations

### Immediate Actions Required
1. **Fix Office Standardization**: Remove district numbers from office names and populate district field
2. **Populate Date Fields**: Set `first_added_date` and `last_updated_date` for all records
3. **Review State Cleaners**: Check why district information isn't being properly extracted

### Data Quality Improvements
1. **Party Information**: Investigate why 66.6% of records lack party information
2. **Geographic Data**: Improve county, city, and address extraction
3. **Contact Information**: Enhance phone and email extraction

### Office Standardization Targets
- **US President**: ✅ Already correct
- **US House**: Should be "US House of Representatives" (no district in name)
- **US Senate**: Should be "US Senate" (no district in name)  
- **State House**: Should be "State Representative" or "State House" (no district in name)
- **State Senate**: Should be "State Senate" (no district in name)

## 📊 Data Quality Score
**Overall Quality**: 6.5/10  
**Strengths**: Complete core fields, good name parsing, proper election type handling  
**Weaknesses**: Office standardization, missing geographic data, incomplete contact info

---
*Report generated on August 27, 2025*
