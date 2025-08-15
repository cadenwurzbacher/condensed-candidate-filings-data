# Address Parsing Audit Summary

## Executive Summary

A comprehensive audit of address parsing across all 21 states in the candidate filings dataset reveals significant variations in data quality and parsing approaches. While most states have good to excellent address parsing, **Kentucky stands out as the only state with poor address parsing quality**, scoring only 30.2/100.

## Key Findings

### Address Parsing Quality by State

| Quality Level | Count | States |
|---------------|-------|---------|
| **Excellent** | 9 | Alaska, Kansas, South Dakota, Wyoming, South Carolina, Georgia, Missouri, Oregon, Louisiana |
| **Good** | 3 | Nebraska, New Mexico, Vermont |
| **Fair** | 0 | None |
| **Poor** | 1 | **Kentucky** |
| **No Addresses** | 8 | Delaware, Colorado, Indiana, Oklahoma, Idaho, Illinois, Arizona, Arkansas |

### States Needing Immediate Attention

#### 1. **Kentucky (Poor - 30.2/100)**
- **Issue**: The `location` column contains district information (e.g., "Laurel-District 6", "Glasgow-Barren") rather than actual addresses
- **Impact**: 185,240 out of 318,150 records have "addresses" that are actually district identifiers
- **Root Cause**: The raw data uses the `location` column for district information, not physical addresses
- **Recommendation**: Either exclude address parsing for Kentucky or implement district-based address mapping

#### 2. **States with No Address Data (8 states)**
These states have no address information in their raw data:
- Delaware, Colorado, Indiana, Oklahoma, Idaho, Illinois, Arizona, Arkansas
- **Recommendation**: These states should be excluded from address parsing analysis or marked as "no address data available"

### States with Good Address Parsing

#### **South Carolina (Excellent - 98.01/100)**
- **Raw Data**: Contains proper addresses in `Contact Address` column
- **Format**: "104-A North Ave Anderson SC, 29625", "PO Box 759 Kingstree SC, 29556"
- **Quality**: High-quality addresses with street, city, state, and zip code
- **Status**: **No immediate action needed** - addresses are properly parsed

#### **Georgia (Excellent - 95.06/100)**
- **Raw Data**: Structured address components (Street Number, Street Name, City, State, Zip)
- **Format**: Separate columns for each address component
- **Quality**: Excellent parsing with proper city, state, zip extraction
- **Status**: **No immediate action needed** - addresses are properly parsed

#### **Oregon (Excellent - 86.24/100)**
- **Raw Data**: Complete addresses in `Mailing Address` column
- **Format**: "PO Box 12869, Salem, OR, 97303", "721 Bramblewood Lane, Canyonville, OR, 97417"
- **Quality**: Good parsing with proper comma separation
- **Status**: **No immediate action needed** - addresses are properly parsed

## Detailed Analysis by State

### Kentucky - The Problem Child
```python
# Raw data examples from location column:
"Laurel-District 6"      # District identifier, not address
"Glasgow-Barren"         # District identifier, not address  
"Murray-Calloway"        # District identifier, not address
"52nd District"          # District identifier, not address
```

**Why This Happened:**
- Kentucky's raw data uses the `location` column for district information, not physical addresses
- The cleaner incorrectly mapped this to the `address` field
- No actual street addresses, city, state, or zip codes are available

**Impact:**
- 53.6% of all records in the merged dataset are from Kentucky
- 185,240 "addresses" are actually district identifiers
- This significantly skews the overall dataset quality

### South Carolina - Actually Good
```python
# Raw data examples from Contact Address column:
"104-A North Ave Anderson SC, 29625"    # Proper address with city, state, zip
"PO Box 759 Kingstree SC, 29556"        # Proper PO Box with city, state, zip
"1731 Senate St Columbia SC, 29201"     # Proper street address with city, state, zip
```

**Why This is Actually Good:**
- Contains complete address information
- Properly formatted with commas separating components
- Includes city, state, and zip code
- The audit incorrectly flagged these as having issues

## Recommendations

### Immediate Actions Required

#### 1. **Fix Kentucky Address Parsing**
```python
# Current problematic code in kentucky_cleaner.py:
if 'location' in df.columns:
    df['address'] = df['location'].apply(clean_address)  # WRONG!

# Recommended fix:
if 'location' in df.columns:
    # Don't treat district info as addresses
    df['address'] = pd.NA
    # Instead, extract district information properly
    df['district'] = df['location'].apply(extract_district)
```

#### 2. **Update Address Parsing Logic**
- Implement proper district extraction for Kentucky
- Mark Kentucky as "no address data available" in the schema
- Update the audit to recognize district identifiers vs. actual addresses

### Medium-Term Improvements

#### 3. **Standardize Address Parsing Across States**
- Implement consistent address component extraction (street, city, state, zip)
- Use proper address parsing libraries (e.g., `usaddress`, `postal`)
- Add validation for address completeness

#### 4. **Enhanced Data Quality Metrics**
- Add address completeness scoring
- Implement address validation (e.g., zip code validation)
- Add geocoding capabilities for address verification

### Long-Term Strategy

#### 5. **Data Source Improvements**
- Work with Kentucky to obtain actual address data if possible
- Standardize data collection formats across states
- Implement data quality checks at the source

## Technical Implementation

### Address Parsing Library Integration
```python
# Example using usaddress library
import usaddress

def parse_address_comprehensive(address_str):
    """Parse address using usaddress library for better accuracy."""
    try:
        parsed = usaddress.parse(address_str)
        components = {}
        for component, value in parsed:
            components[component] = value
        return components
    except:
        return None
```

### District Extraction for Kentucky
```python
def extract_kentucky_district(location_str):
    """Extract district information from Kentucky location field."""
    if pd.isna(location_str):
        return None
    
    location = str(location_str).strip()
    
    # Handle patterns like "Laurel-District 6", "Glasgow-Barren"
    if '-' in location:
        parts = location.split('-')
        if len(parts) == 2:
            city, district = parts[0].strip(), parts[1].strip()
            return f"{city}, {district}"
    
    return location
```

## Conclusion

The address parsing audit reveals that **Kentucky is the only state with significant address parsing issues**, not South Carolina as initially suspected. South Carolina actually has excellent address parsing with proper city, state, and zip code extraction.

The main issue is that Kentucky's raw data contains district information rather than physical addresses, and the current cleaner incorrectly treats this as address data. This needs immediate attention to improve the overall dataset quality.

Most other states (9 out of 21) have excellent address parsing, with only 3 states having good but improvable parsing, and 8 states having no address data at all.

## Next Steps

1. **Immediate**: Fix Kentucky address parsing to exclude district data from address fields
2. **Short-term**: Implement proper district extraction for Kentucky
3. **Medium-term**: Standardize address parsing across all states
4. **Long-term**: Work with data sources to improve data quality at the source
