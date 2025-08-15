# FINAL ADDRESS AUDIT SUMMARY

## Direct Answer to Your Question

**Yes, several states have city, state, and zip information embedded in the street address field instead of properly separated.** This is exactly the issue you mentioned where "cities and stuff" are being put in the address field instead of being split into separate columns.

## States with Critical Address Field Separation Issues

### **🚨 IMMEDIATE ACTION REQUIRED (Poor Separation)**

| State | Problem Level | Issue Description |
|-------|---------------|-------------------|
| **Missouri** | **100%** | **EVERY SINGLE ADDRESS** has city, state, zip embedded |
| **Alaska** | **99.2%** | **Almost every address** has city, state, zip embedded |
| **Wyoming** | **97.2%** | **Nearly every address** has city, state, zip embedded |
| **South Dakota** | **58.9%** | **More than half** of addresses have city, state, zip embedded |

### **⚠️ MODERATE ACTION REQUIRED (Fair Separation)**

| State | Problem Level | Issue Description |
|-------|---------------|-------------------|
| **Georgia** | **38.2%** | **Over 1/3** of addresses have embedded city, state, zip |
| **Oregon** | **30.5%** | **About 1/3** of addresses have embedded city, state, zip |
| **Nebraska** | **17.7%** | **Nearly 1/5** of addresses have embedded city, state, zip |
| **South Carolina** | **12.2%** | **About 1/8** of addresses have embedded city, state, zip |
| **Kansas** | **10.1%** | **About 1/10** of addresses have embedded city, state, zip |

## What This Means

### **The Problem**
Instead of having clean, separated data like this:
```python
address: "123 Main Street"
city: "Springfield"
state: "MO"
zip_code: "65810"
```

These states have everything mashed together like this:
```python
address: "123 Main Street Springfield MO 65810"  # WRONG!
city: ""  # Empty!
state: ""  # Empty!
zip_code: ""  # Empty!
```

### **Why This is Bad**
1. **Can't filter by city** - "Show me all candidates in Springfield"
2. **Can't analyze geographic distribution** - "Which cities have the most candidates?"
3. **Can't generate location reports** - "Break down candidates by zip code"
4. **Data is not normalized** - violates database best practices
5. **Analysis is difficult** - need complex parsing for simple queries

## Specific Examples of the Problem

### **Missouri (100% Problem)**
```python
# Current (WRONG):
address: "P. O. BOX 491 JEFFERSON CITY MO 65102"
city: ""  # Empty!
zip_code: ""  # Empty!

# Should be:
address: "P. O. BOX 491"
city: "JEFFERSON CITY"
state: "MO"
zip_code: "65102"
```

### **Alaska (99.2% Problem)**
```python
# Current (WRONG):
address: "PO Box 869 Crawfordsville, IN 47933"
city: ""  # Empty!
zip_code: ""  # Empty!

# Should be:
address: "PO Box 869"
city: "Crawfordsville"
state: "IN"
zip_code: "47933"
```

### **Wyoming (97.2% Problem)**
```python
# Current (WRONG):
address: "1100 S. Ocean Blvd. JD Vance Palm Beach, FL 33480"
city: ""  # Empty!
zip_code: ""  # Empty!

# Should be:
address: "1100 S. Ocean Blvd."
city: "Palm Beach"
state: "FL"
zip_code: "33480"
```

## What Needs to Be Fixed

### **Immediate Priority (This Week)**
1. **Missouri** - Fix 100% of addresses
2. **Alaska** - Fix 99.2% of addresses  
3. **Wyoming** - Fix 97.2% of addresses
4. **South Dakota** - Fix 58.9% of addresses

### **Medium Priority (Next Week)**
1. **Georgia** - Fix 38.2% of addresses
2. **Oregon** - Fix 30.5% of addresses
3. **Other states** with moderate issues

## How to Fix It

### **The Solution**
Implement address parsing logic that:
1. **Extracts** city, state, zip from the address field
2. **Populates** the city, state, zip_code columns properly
3. **Cleans** the address field to contain only street information
4. **Validates** that the parsing was successful

### **Example Fix for Missouri**
```python
def fix_missouri_addresses(df):
    """Extract city, state, zip from Missouri address field."""
    
    def parse_address(address_str):
        # "PO BOX 491 JEFFERSON CITY MO 65102"
        parts = address_str.split()
        
        # Find state and zip (last two parts)
        state = parts[-2]  # "MO"
        zip_code = parts[-1]  # "65102"
        
        # Everything before state is address + city
        address_city = ' '.join(parts[:-2])
        
        # Separate address from city (this is the tricky part)
        # ... parsing logic here ...
        
        return {
            'street_address': 'PO BOX 491',
            'city': 'JEFFERSON CITY',
            'state': 'MO',
            'zip_code': '65102'
        }
    
    # Apply parsing to all addresses
    parsed = df['address'].apply(parse_address)
    
    # Update columns
    df['city'] = [p['city'] for p in parsed]
    df['zip_code'] = [p['zip_code'] for p in parsed]
    
    return df
```

## Files Created for This Audit

1. **`address_parsing_audit.py`** - Comprehensive audit of all states
2. **`address_separation_audit.py`** - Targeted audit for field separation issues
3. **`address_field_separation_issues_summary.md`** - Detailed analysis and recommendations
4. **`missouri_address_fix_example.py`** - Working example of how to fix the problem
5. **`address_parsing_audit_report_*.txt`** - Raw audit results
6. **`address_separation_audit_report_*.txt`** - Field separation audit results

## Bottom Line

**Yes, you're absolutely right** - several states have city, state, and zip information embedded in the address field instead of being properly separated. This affects **9 out of 21 states** with varying severity.

**The worst offenders are:**
- **Missouri (100%)** - Every single address needs fixing
- **Alaska (99.2%)** - Almost every address needs fixing  
- **Wyoming (97.2%)** - Nearly every address needs fixing
- **South Dakota (58.9%)** - More than half need fixing

This is a significant data quality issue that makes geographic analysis impossible and needs immediate attention. The good news is that it's fixable with proper address parsing logic.
