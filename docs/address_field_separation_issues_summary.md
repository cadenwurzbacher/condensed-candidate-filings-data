# Address Field Separation Issues Summary

## Executive Summary

The audit reveals that **4 states have severe address field separation issues** where city, state, and zip code information is embedded in the street address field instead of being properly separated into individual columns. This creates data quality problems and makes address analysis difficult.

## Critical Issues Found

### **States with POOR Address Separation (Immediate Action Required)**

| State | Embedded Percentage | Issue Description |
|-------|-------------------|-------------------|
| **Missouri** | **100.0%** | Every single address has city, state, zip embedded |
| **Alaska** | **99.2%** | Almost every address has city, state, zip embedded |
| **Wyoming** | **97.2%** | Nearly every address has city, state, zip embedded |
| **South Dakota** | **58.9%** | More than half of addresses have city, state, zip embedded |

### **States with FAIR Address Separation (Moderate Action Required)**

| State | Embedded Percentage | Issue Description |
|-------|-------------------|-------------------|
| **Georgia** | **38.2%** | Over 1/3 of addresses have embedded city, state, zip |
| **Oregon** | **30.5%** | About 1/3 of addresses have embedded city, state, zip |
| **Nebraska** | **17.7%** | Nearly 1/5 of addresses have embedded city, state, zip |
| **South Carolina** | **12.2%** | About 1/8 of addresses have embedded city, state, zip |
| **Kansas** | **10.1%** | About 1/10 of addresses have embedded city, state, zip |

## Detailed Examples of the Problem

### **Missouri (100% Problem)**
```python
# Current problematic format in address field:
"P. O. BOX 491 JEFFERSON CITY MO 65102"
"PO BOX 21621 ST. LOUIS MO 63109"
"1650 DES PERES RD., STE. 220 ST. LOUIS MO 63131"

# Should be separated into:
address: "P. O. BOX 491" or "1650 DES PERES RD., STE. 220"
city: "JEFFERSON CITY" or "ST. LOUIS"
state: "MO"
zip_code: "65102" or "63109" or "63131"
```

### **Alaska (99.2% Problem)**
```python
# Current problematic format in address field:
"PO Box 869 Crawfordsville, IN 47933"
"420 N. McKinley St. Ste. 111-512 Corona, CA 92879"
"25 Guatemala Dr. Martinsburg, WV 25403"

# Should be separated into:
address: "PO Box 869" or "420 N. McKinley St. Ste. 111-512" or "25 Guatemala Dr."
city: "Crawfordsville" or "Corona" or "Martinsburg"
state: "IN" or "CA" or "WV"
zip_code: "47933" or "92879" or "25403"
```

### **Wyoming (97.2% Problem)**
```python
# Current problematic format in address field:
"1100 S. Ocean Blvd. JD Vance Palm Beach, FL 33480"
"435 N. Kenter Avenue Tim Walz Los Angeles, CA 90049"
"2400 Henderson Mill Court Ne Mike ter Maat Atlanta, GA 30345"

# Should be separated into:
address: "1100 S. Ocean Blvd." or "435 N. Kenter Avenue" or "2400 Henderson Mill Court Ne"
city: "Palm Beach" or "Los Angeles" or "Atlanta"
state: "FL" or "CA" or "GA"
zip_code: "33480" or "90049" or "30345"
```

### **South Dakota (58.9% Problem)**
```python
# Current problematic format in address field:
"3907 Jewel Ave Las Vegas NV 89148"
"1900 L St NW Washington DC 20036"
"PO Box 58174 Philadelphia PA 19102"

# Should be separated into:
address: "3907 Jewel Ave" or "1900 L St NW" or "PO Box 58174"
city: "Las Vegas" or "Washington" or "Philadelphia"
state: "NV" or "DC" or "PA"
zip_code: "89148" or "20036" or "19102"
```

## Root Cause Analysis

### **Why This Happens**

1. **Raw Data Structure**: Some states provide addresses in a single field with city, state, zip appended
2. **Cleaner Logic**: The state cleaners are not parsing the address field to extract components
3. **Schema Mismatch**: The final schema expects separate fields, but data is being stored as-is
4. **Missing Parsing**: No address parsing logic to separate street address from city/state/zip

### **Impact on Data Quality**

1. **Address Analysis**: Cannot easily analyze addresses by city, state, or zip
2. **Geographic Queries**: Difficult to filter candidates by location
3. **Data Standardization**: Addresses are not in a consistent format
4. **Database Queries**: Complex queries needed to extract location information
5. **Reporting**: Geographic reports are difficult to generate

## Immediate Action Plan

### **Phase 1: Fix Critical States (Week 1)**

#### **Missouri (100% Problem)**
- Implement address parsing to extract city, state, zip from address field
- Populate city, state, zip columns with extracted data
- Clean address field to contain only street information

#### **Alaska (99.2% Problem)**
- Same approach as Missouri
- Focus on PO Box and street address patterns

#### **Wyoming (97.2% Problem)**
- Handle complex patterns with candidate names embedded
- Extract city, state, zip while preserving street address

#### **South Dakota (58.9% Problem)**
- Parse addresses without commas (space-separated format)
- Extract city, state, zip components

### **Phase 2: Fix Moderate States (Week 2)**

#### **Georgia (38.2% Problem)**
- Already has city, state, zip columns populated
- Clean address field to remove embedded components

#### **Oregon (30.5% Problem)**
- Extract city, state, zip from comma-separated format
- Populate city, state, zip columns

#### **Other States**
- Apply similar parsing logic based on their specific formats

## Technical Implementation

### **Address Parsing Library Integration**

```python
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

def extract_city_state_zip(address_str):
    """Extract city, state, zip from address string."""
    # Handle various formats
    patterns = [
        # City, ST 12345
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*,\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)',
        # City ST 12345
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)',
        # ST 12345
        r'([A-Z]{2})\s+(\d{5}(?:-\d{4})?)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, address_str)
        if match:
            return match.groups()
    
    return None, None, None
```

### **State-Specific Parsing Logic**

```python
def fix_missouri_addresses(df):
    """Fix Missouri addresses by extracting city, state, zip."""
    def parse_missouri_address(address):
        # Pattern: "PO BOX 491 JEFFERSON CITY MO 65102"
        parts = address.split()
        
        # Find state and zip (last two parts)
        if len(parts) >= 2:
            state = parts[-2]
            zip_code = parts[-1]
            
            # Everything before state is address + city
            address_city = ' '.join(parts[:-2])
            
            # Try to separate address from city
            # This is complex and may need manual review
            return {
                'address': address_city,  # May contain city
                'city': 'Unknown',  # Need better parsing
                'state': state,
                'zip_code': zip_code
            }
        
        return {'address': address, 'city': None, 'state': None, 'zip_code': None}
    
    # Apply parsing
    parsed = df['address'].apply(parse_missouri_address)
    
    # Update columns
    df['parsed_address'] = [p['address'] for p in parsed]
    df['parsed_city'] = [p['city'] for p in parsed]
    df['parsed_state'] = [p['state'] for p in parsed]
    df['parsed_zip'] = [p['zip_code'] for p in parsed]
    
    return df
```

## Success Metrics

### **Immediate Goals (Week 1)**
- [ ] Missouri: Reduce embedded city/state/zip from 100% to <5%
- [ ] Alaska: Reduce embedded city/state/zip from 99.2% to <5%
- [ ] Wyoming: Reduce embedded city/state/zip from 97.2% to <5%
- [ ] South Dakota: Reduce embedded city/state/zip from 58.9% to <10%

### **Medium-term Goals (Week 2)**
- [ ] All states: <5% embedded city/state/zip in address field
- [ ] All states: Properly populated city, state, zip columns
- [ ] Address field contains only street address information

### **Long-term Goals (Month 1)**
- [ ] Standardized address parsing across all states
- [ ] Data quality validation for address completeness
- [ ] Automated address parsing pipeline

## Conclusion

The address field separation issue affects **9 out of 21 states** with varying severity. **4 states require immediate attention** due to having over 50% of addresses with embedded city/state/zip information.

The solution involves implementing proper address parsing logic to extract city, state, and zip code components from the address field and populate the appropriate columns. This will significantly improve data quality and enable better geographic analysis of candidate filings.

**Priority order for fixes:**
1. **Missouri** (100% problem)
2. **Alaska** (99.2% problem) 
3. **Wyoming** (97.2% problem)
4. **South Dakota** (58.9% problem)
5. **Georgia** (38.2% problem)
6. **Oregon** (30.5% problem)
7. **Other moderate states**
