# Data Standards for CandidateFilings.com Pipeline

This document defines the standardization rules for all candidate filing data processed through the pipeline.

## File Format Standards

### Column Order
The final output file should have columns in this exact order:

1. `stable_id` - Unique identifier
2. `state` - Election state (full name)
3. `full_name_display` - Complete name for display
4. `prefix` - Title prefix (Dr., Mr., etc.)
5. `first_name` - First name
6. `middle_name` - Middle name or initial (WITH PERIODS)
7. `last_name` - Last name
8. `suffix` - Name suffix (Jr., Sr., etc.)
9. `nickname` - Nickname if provided
10. `office` - Standardized office name
11. `district` - District number/name
12. `party` - Political party
13. `street_address` - Complete street address
14. `city` - City name
15. `address_state` - Address state (abbreviation)
16. `zip_code` - ZIP code
17. `phone` - Phone number
18. `email` - Email address
19. `facebook` - Facebook URL
20. `twitter` - Twitter URL
21. `filing_date` - Filing date
22. `election_date` - Election date
23. `first_added_date` - When first added to database
24. `last_updated_date` - When last updated

**Additional columns** (after preferred order):
- `county` - County name
- `election_year` - Election year
- `action_type` - Action type
- `raw_data` - Original JSON data
- `file_source` - Source file
- `row_index` - Row index in source
- `source_office` - Original office name
- `source_district` - Original district
- `source_party` - Original party
- `ran_in_primary` - Ran in primary election
- `ran_in_general` - Ran in general election
- `ran_in_special` - Ran in special election
- `election_type_notes` - Election notes
- `processing_timestamp` - When processed
- `pipeline_version` - Pipeline version
- `data_source` - Data source

## Office Standardization

The following offices should be standardized to these exact values:

| Original Office Names | Standardized Name |
|---------------------|-------------------|
| President, POTUS, U.S. President, United States President | **US President** |
| House, U.S. House, United States House, Congressional District | **US House** |
| Senate, U.S. Senate, United States Senate | **US Senate** |
| State House, State Representative, House of Representatives, State Assembly | **State House** |
| State Senate, State Senator | **State Senate** |
| City Council, Council Member, Councilman, Councilwoman | **City Council** |
| County Commission, County Commissioner, County Board | **County Commission** |
| Governor, Guv | **Governor** |
| Secretary of State, SOS | **Secretary of State** |
| Sheriff, County Sheriff | **Sheriff** |
| Mayor, City Mayor | **Mayor** |
| Attorney General, State Attorney General, AG | **State Attorney General** |

**Alaska-Specific Rules:**
- House District XX (e.g., "House District 06") → **State House** with district "06"
- Any Alaska House office that is not US House should be mapped to **State House**

**Rules:**
- Remove "/ Lieutenant Governor" or "/ Vice President" from office names
- For offices with districts (State House, State Senate, US House, US Senate), district should be in `district` column
- Preserve original office name if mapping is not obvious

### District Handling

For offices that have districts, the district information should be stored in the `district` column:

- **US House**: District number (e.g., "1", "2", "At-Large")
- **US Senate**: State abbreviation (e.g., "AK", "NC") 
- **State House**: District number (e.g., "1", "2", "District A")
- **State Senate**: District number (e.g., "1", "2", "District A")

## State Standardization

### Election State (`state` column)
The state where the election is taking place should use the **full state name**:
- Alaska
- North Carolina
- New York
- South Dakota
- etc.

### Address State (`address_state` column)
The state in the candidate's address should use the **two-letter abbreviation**:
- AK (Alaska)
- NC (North Carolina)
- NY (New York)
- SD (South Dakota)
- etc.

## Phone Standardization

### Phone Number Format
Phone numbers should contain **digits only**:
- Remove all parentheses, dashes, spaces, and special characters
- Example: "(907) 555-1234" → "9075551234"
- Example: "555-1234 ext. 5" → "5551234"

## Name Standardization

### Name Components
Candidate names should be parsed into the following components:

- `first_name`: First name only
- `middle_name`: Middle name or initial (WITH PERIODS)
- `last_name`: Last name only
- `prefix`: Title prefix (Dr., Mr., Mrs., Ms., etc.)
- `suffix`: Name suffix (Jr., Sr., II, III, etc.) - WITH PERIODS
- `nickname`: Nickname if provided
- `full_name_display`: Complete name as it should be displayed

**Rules:**
- Middle initials should HAVE periods (M. not M)
- Suffixes should have periods (Jr. not Jr)
- Complex names with party information should be simplified
- Remove party information from name components (e.g., "Bill (Nonpartisan)" → "Bill")
- Remove party letter codes from names and update party column:
  - `(R)` or `(r)` → Remove from name, set party to "Republican" if empty
  - `(D)` or `(d)` → Remove from name, set party to "Democratic" if empty
  - `(G)` or `(g)` → Remove from name, set party to "Green" if empty
  - `(L)` or `(l)` → Remove from name, set party to "Libertarian" if empty
  - `(I)` or `(i)` → Remove from name, set party to "Independent" if empty
- Remove "/ Lieutenant Governor" or "/ Vice President" from office names
- **full_name_display capitalization**: Apply proper case while preserving legitimate abbreviations:
  - Regular names: "FRANK BERTONE" → "Frank Bertone"
  - Preserve Spanish/foreign prefixes: "DE LA FUENTE" → "DE LA FUENTE"
  - Preserve short abbreviations: "JD SMITH" → "JD SMITH"
  - Preserve initials: "J SMITH" → "J SMITH"

### Name Parsing Rules
- Remove extra whitespace
- Handle multiple middle names
- Preserve special characters and accents
- Handle hyphenated last names appropriately

## Address Standardization

### Address Components
Addresses should be parsed into:

- `street_address`: Complete street address
- `city`: City name
- `address_state`: State abbreviation (e.g., AK, NC, NY)
- `zip_code`: ZIP code

### Address Cleaning Rules
- Standardize street abbreviations (St., Ave., Blvd., etc.)
- Remove extra whitespace
- Handle apartment/unit numbers
- Standardize state abbreviations
- Validate ZIP code format

## Party Standardization

### Party Names
Political parties should be standardized to common names:
- Democratic Party → "Democratic"
- Republican Party → "Republican"
- Independent → "Independent"
- Libertarian Party → "Libertarian"
- Green Party → "Green"
- No Party Affiliation (nonpartisan Offices) → "Nonpartisan"
- Non-partisan → "Nonpartisan"
- No Party Preference → "Nonpartisan"
- Unaffiliated → "Nonpartisan"
