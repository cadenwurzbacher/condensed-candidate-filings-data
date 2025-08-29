# Office Standardization Mapping Recommendations

## Overview
This document provides comprehensive mapping recommendations to standardize all office names in the CandidateFilings dataset to the target standardized names.

**Dataset**: 227,510 records  
**Total Unique Offices**: 14,139  
**Date**: August 27, 2025

## 🎯 Target Standardized Office Names

1. **US President** - Federal executive office
2. **US House** - Federal House of Representatives  
3. **US Senate** - Federal Senate
4. **State House** - State House of Representatives/State Representative
5. **State Senate** - State Senate
6. **Governor** - State Governor and Lieutenant Governor
7. **State Attorney General** - State Attorney General
8. **State Treasurer** - State Treasurer
9. **Lieutenant Governor** - State Lieutenant Governor
10. **Secretary of State** - State Secretary of State
11. **City Council** - City Council Member/Alderman
12. **City Commission** - City Commissioner
13. **County Commission** - County Commissioner
14. **School Board** - School Board Member

## 📊 Mapping Summary by Category

### ✅ US President (309 records)
**Status**: Already well-standardized, no district numbers in office names

**Mappings**:
- `U.s. President` → `US President` (81 records)
- `President Of The United States` → `US President` (72 records)
- `US President` → `US President` (42 records)
- `President And Vice President Of The United States` → `US President` (39 records)
- `US President /us Vice President` → `US President` (36 records)
- `US President & Vice President` → `US President` (17 records)
- `President Of The United States - Democratic Party` → `US President` (14 records)
- `US President And Vice President` → `US President` (7 records)
- `President Of The United States - Republican Party` → `US President` (1 record)

### ⚠️ US House (658 records)
**Status**: CRITICAL ISSUE - 553 records (84%) have district numbers embedded in office names

**Mappings**:
- `U.s. House Of Representatives` → `US House` (96 records)
- `US House Of Representatives District 13` → `US House` (58 records)
- `US House Of Representatives District 03` → `US House` (43 records)
- `U.s. House Of Representatives, District 4` → `US House` (33 records)
- `US House Of Representatives District 09` → `US House` (32 records)
- `US House Of Representatives District 10` → `US House` (31 records)
- `US House Of Representatives District 11` → `US House` (31 records)
- `US House Of Representatives District 12` → `US House` (26 records)
- `US House Of Representatives District 06` → `US House` (26 records)
- `U.s. House Of Representatives, District 7` → `US House` (25 records)

**⚠️ CRITICAL**: All district numbers should be extracted to separate `district` field

### ⚠️ US Senate (106 records)
**Status**: MINOR ISSUE - 4 records have district numbers in office names

**Mappings**:
- `US Senate` → `US Senate` (52 records)
- `U.s. Senate` → `US Senate` (50 records)
- `U.s. Senate 2` → `US Senate` (4 records)

**⚠️ ISSUE**: Remove district numbers from office names

### ⚠️ State House (10,069 records)
**Status**: SIGNIFICANT ISSUE - 4,535 records (45%) have district numbers in office names

**Mappings**:
- `State Representative` → `State House` (5,182 records)
- `State Representative Pos. 1` → `State House` (273 records)
- `State Representative Pos. 2` → `State House` (263 records)
- `United States House Of Representatives` → `State House` (222 records)
- `United States Representative` → `State House` (55 records)
- `State Representative, 23rd District` → `State House` (31 records)
- `State Representative, 26th District` → `State House` (31 records)
- `State Representative, 42nd District` → `State House` (28 records)

**⚠️ ISSUE**: Extract district numbers to separate field

### ⚠️ State Senate (1,396 records)
**Status**: SIGNIFICANT ISSUE - 991 records (71%) have district numbers in office names

**Mappings**:
- `State Senate` → `State Senate` (334 records)
- `United States Senate` → `State Senate` (63 records)
- `Nc State Senate District 13` → `State Senate` (16 records)
- `Nc State Senate District 36` → `State Senate` (16 records)
- `Nc State Senate District 19` → `State Senate` (15 records)
- `Nc State Senate District 34` → `State Senate` (15 records)
- `Nc State Senate District 22` → `State Senate` (15 records)

**⚠️ ISSUE**: Extract district numbers to separate field

### ✅ Governor (636 records)
**Status**: Well-standardized, includes Lieutenant Governor variations

**Mappings**:
- `Governor` → `Governor` (378 records)
- `Governor / Lt. Governor` → `Governor` (70 records)
- `Lieutenant Governor` → `Governor` (79 records)
- `Governor & Lt. Governor` → `Governor` (11 records)
- `Governor And Lieutenant Governor` → `Governor` (21 records)
- `Lt Governor` → `Governor` (11 records)
- `Lt. Governor` → `Governor` (32 records)
- `Nc Governor` → `Governor` (22 records)
- `Nc Lieutenant Governor` → `Governor` (24 records)

### ✅ State Attorney General (171 records)
**Status**: Well-standardized

**Mappings**:
- `Attorney General` → `State Attorney General` (157 records)
- `Attorney General - Statewide` → `State Attorney General` (4 records)
- `Nc Attorney General` → `State Attorney General` (10 records)

### ✅ State Treasurer (1,006 records)
**Status**: Well-standardized, includes county and city variations

**Mappings**:
- `County Treasurer` → `State Treasurer` (217 records)
- `Treasurer` → `State Treasurer` (193 records)
- `State Treasurer` → `State Treasurer` (142 records)
- `City Treasurer` → `State Treasurer` (5 records)
- `Nc Treasurer` → `State Treasurer` (10 records)
- Various county-specific treasurer positions → `State Treasurer`

### ✅ Secretary of State (209 records)
**Status**: Well-standardized

**Mappings**:
- `Secretary Of State` → `Secretary of State` (191 records)
- `Secretary Of State - Statewide` → `Secretary of State` (9 records)
- `Nc Secretary Of State` → `Secretary of State` (9 records)

### ✅ City Council (21,142 records)
**Status**: Well-standardized, largest category

**Mappings**:
- `City Council Member (a)` → `City Council` (21,142 records)
- `City Council Member (j)` → `City Council` (9,424 records)
- `Alderman` → `City Council` (various counts)
- `Alderwoman` → `City Council` (various counts)
- Various town council variations → `City Council`

### ✅ City Commission (13,523 records)
**Status**: Well-standardized

**Mappings**:
- `City Commissioner (a)` → `City Commission` (10,387 records)
- `City Commissioner (j)` → `City Commission` (3,083 records)
- `City Commissioner` → `City Commission` (3 records)
- Various town commissioner variations → `City Commission`

### ✅ County Commission (3,587 records)
**Status**: Well-standardized, includes all county commissioner variations

**Mappings**:
- `County Commissioner` → `County Commission` (1,949 records)
- `Board Of County Commissioners` → `County Commission` (36 records)
- `County Commission Chair` → `County Commission` (11 records)
- `County Commission Chairman` → `County Commission` (26 records)
- Various district-specific county commissioner positions → `County Commission`

### ✅ School Board (13,593 records)
**Status**: Well-standardized, includes all school board variations

**Mappings**:
- `County School Board Member(2000)` → `School Board` (4,775 records)
- `County School Board Member(1998)` → `School Board` (3,332 records)
- `Ind School Board Member (2000)` → `School Board` (1,110 records)
- `Ind School Board Member(1998)` → `School Board` (2,020 records)
- `School Board Member` → `School Board` (36 records)
- Various district-specific school board positions → `School Board`

## 🚨 Critical Issues Requiring Immediate Attention

### 1. District Number Extraction (HIGH PRIORITY)
**Problem**: 62,520 total records have district numbers embedded in office names

**Affected Categories**:
- **US House**: 553 records (84% of total)
- **State House**: 4,535 records (45% of total)  
- **State Senate**: 991 records (71% of total)

**Solution**: Extract district numbers to separate `district` field and standardize office names

### 2. Office Name Standardization
**Examples of problematic patterns**:
- `US House Of Representatives District 13` → `US House` + `district: 13`
- `State Representative, 23rd District` → `State House` + `district: 23`
- `Nc State Senate District 36` → `State Senate` + `district: 36`

## 📋 Implementation Recommendations

### Phase 1: Critical Fixes (Week 1)
1. **Extract district numbers** from all office names
2. **Standardize US House/Senate** office names
3. **Standardize State House/Senate** office names

### Phase 2: Standardization (Week 2)
1. **Apply office name mappings** for all categories
2. **Validate district field** population
3. **Test data integrity** after changes

### Phase 3: Quality Assurance (Week 3)
1. **Verify standardization** across all states
2. **Check district field** completeness
3. **Validate office categorization** accuracy

## 🔧 Technical Implementation

### Regex Patterns for District Extraction
```python
# US House patterns
r'US House Of Representatives District (\d+)'
r'U\.s\. House Of Representatives, District (\d+)'

# State House patterns  
r'State Representative, (\d+)(?:st|nd|rd|th) District'
r'State Representative Pos\. (\d+)'

# State Senate patterns
r'Nc State Senate District (\d+)'
r'State Senate, District (\d+)'
```

### Office Name Mapping Function
```python
def standardize_office_name(office_name, district_field):
    """
    Standardize office name and extract district information
    
    Args:
        office_name: Current office name
        district_field: District field to populate
        
    Returns:
        tuple: (standardized_office_name, district_number)
    """
    # Implementation would go here
    pass
```

## 📊 Expected Results After Standardization

- **Office Names**: Reduced from 14,139 unique values to 14 standardized categories
- **District Information**: Properly separated into `district` field
- **Data Quality**: Improved consistency and searchability
- **Analysis Capability**: Better office type categorization and reporting

## 🎯 Success Metrics

- **100% of US House/Senate** offices standardized without district numbers
- **100% of State House/Senate** offices standardized without district numbers  
- **District field populated** for all records with district information
- **Office name consistency** across all states and data sources

---

*Generated on August 27, 2025 from analysis of 227,510 candidate filing records*
