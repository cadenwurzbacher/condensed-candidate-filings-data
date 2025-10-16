# Complete Pipeline Flow - All Phases and Operations

## Phase 1: Structural Parsing (Structural Cleaners)
```
Raw Data Files (Excel/CSV/TXT) → Structural Cleaners
├── File Detection & Loading
│   ├── Find state-specific files (e.g., "pennsylvania_candidates_2024.xlsx")
│   ├── Handle different file formats (Excel, CSV, TXT)
│   ├── Handle encoding issues (utf-8, latin-1, cp1252, iso-8859-1)
│   └── Chunked reading for large files (>10MB)
├── Data Structure Extraction
│   ├── Extract raw data from complex file structures
│   ├── Handle multi-sheet Excel files
│   ├── Handle merged cells and headers
│   ├── Extract candidate names, addresses, offices, parties
│   └── Handle state-specific data layouts
├── Basic Data Cleaning
│   ├── Remove empty rows and columns
│   ├── Handle missing values
│   ├── Basic text cleaning (whitespace, punctuation)
│   └── Validate required fields exist
└── Output: Structured DataFrames
    ├── Standardized column names
    ├── Consistent data types
    ├── Basic data validation
    └── Ready for state-specific processing
```

## Phase 2: ID Generation (DataProcessor)
```
Structured Data → Stable ID Generation
├── Core Field Validation
│   ├── Check required fields: candidate_name, office, election_year
│   ├── Filter out invalid records (e.g., "No Nominations")
│   ├── Handle missing or null values
│   └── Validate data types and formats
├── Stable ID Creation
│   ├── Create hash key from: candidate_name + office + election_year + state
│   ├── Generate MD5 hash truncated to 12 characters
│   ├── Handle duplicate IDs (track and log)
│   ├── Handle float election years (2016.0 → "2016")
│   └── Normalize strings (lowercase, strip whitespace)
├── Duplicate Detection
│   ├── Track existing IDs across all states
│   ├── Count and log duplicate occurrences
│   ├── Preserve first occurrence of each ID
│   └── Handle hash collisions
└── Output: DataFrames with stable_id column
    ├── Unique identifier for each candidate
    ├── Deterministic across runs
    ├── Based on core identifying fields
    └── Ready for state-specific cleaning
```

## Phase 3: State Cleaning (State Cleaners)
```
Data with Stable IDs → State-Specific Cleaning
├── State-Specific Data Structure Cleaning
│   ├── Handle state-specific name formats
│   │   ├── Alaska: "Last, First" format
│   │   ├── Florida: Title removal (Dr., Mr., etc.)
│   │   ├── Pennsylvania: Northeastern naming patterns
│   │   └── Other states: Custom name parsing
│   ├── Handle state-specific address formats
│   │   ├── Alaska: PO Box and rural route patterns
│   │   ├── Colorado: Mountain region formats
│   │   ├── Florida: State abbreviation normalization
│   │   └── Other states: Custom address patterns
│   ├── Handle state-specific district formats
│   │   ├── Florida: "District 1", "Dist. 1" → "1"
│   │   ├── Pennsylvania: "HD 1", "SD 1" → "1"
│   │   ├── Alaska: Custom district numbering
│   │   └── Other states: Custom district formats
│   └── Handle state-specific office formats
│       ├── State-specific office naming conventions
│       ├── State-specific abbreviations
│       ├── State-specific hierarchies
│       └── Custom office structures
├── State-Specific Content Cleaning
│   ├── County standardization
│   │   ├── Map county names to standard format
│   │   ├── Handle county abbreviations
│   │   ├── Handle county variations
│   │   └── State-specific county mappings
│   ├── Party standardization (MOVED TO NATIONAL STANDARDS)
│   ├── State-specific formatting
│   │   ├── Date formats
│   │   ├── Phone number formats
│   │   ├── Email formats
│   │   └── Other state-specific patterns
│   └── State-specific validation
│       ├── Validate state-specific rules
│       ├── Handle state-specific edge cases
│       ├── State-specific data quality checks
│       └── Custom validation logic
├── Name Parsing (State-Specific)
│   ├── Extract first, middle, last names
│   ├── Extract prefixes (Dr., Mr., etc.)
│   ├── Extract suffixes (Jr., Sr., III, etc.)
│   ├── Extract nicknames
│   ├── Create full_name_display
│   └── Handle state-specific name patterns
└── Output: State-Cleaned DataFrames
    ├── State-specific structure cleaned
    ├── State-specific content cleaned
    ├── Names parsed into components
    ├── Addresses in raw format (ready for parsing)
    └── Ready for national standards
```

## Phase 4: National Standards (Unified Address Parser + National Standards)
```
State-Cleaned Data → National Standardization
├── Address Parsing (UnifiedAddressParser)
│   ├── State-Specific Pre-Processing
│   │   ├── Florida: FL → Florida, fl → Florida
│   │   ├── Alaska: PO Box pattern handling
│   │   ├── Colorado: Rural route handling
│   │   ├── Other states: Custom pre-processing
│   │   └── Remove trailing punctuation
│   ├── Universal Address Parsing
│   │   ├── ZIP Code Extraction
│   │   │   ├── Extract 5-digit ZIP codes
│   │   │   ├── Extract ZIP+4 codes
│   │   │   ├── Handle PO Box numbers (don't extract as ZIP)
│   │   │   └── Remove ZIP from address for further processing
│   │   ├── State Code Extraction
│   │   │   ├── Extract 2-letter state codes
│   │   │   ├── Extract full state names
│   │   │   ├── Handle state abbreviations
│   │   │   └── Remove state from address for further processing
│   │   ├── City Extraction
│   │   │   ├── Extract city names from comma-separated formats
│   │   │   ├── Handle "Street, City" format
│   │   │   ├── Handle "Street, City, State" format
│   │   │   ├── Handle Alaska-style "Street City, State" format
│   │   │   └── Remove city from address for further processing
│   │   └── Street Address Cleaning
│   │       ├── Clean street address formatting
│   │       ├── Handle address abbreviations
│   │       ├── Remove extra whitespace
│   │       └── Standardize address patterns
│   ├── State Normalization
│   │   ├── Convert full state names to USPS codes
│   │   ├── Pennsylvania → PA
│   │   ├── Washington → WA
│   │   ├── Colorado → CO
│   │   └── Other state mappings
│   └── State Backfilling
│       ├── Fill missing parsed states from main state column
│       ├── Handle cases where address parsing fails
│       ├── Preserve state information
│       └── Ensure state data completeness
├── Office Standardization (OfficeStandardizer)
│   ├── Map office names to standard format
│   │   ├── "County Commissioner" → "County Commissioner" (preserve "county")
│   │   ├── "County Levy Court Commissioner" → "County Levy Court Commissioner"
│   │   ├── "State Senator" → "State Senator"
│   │   ├── "U.S. Representative" → "U.S. Representative"
│   │   └── Other office mappings
│   ├── Preserve source_office column
│   ├── Handle office variations
│   ├── Handle office abbreviations
│   └── Standardize office hierarchies
├── Party Standardization (PartyStandardizer)
│   ├── Map party names to standard format
│   │   ├── "Democratic Party" → "Democratic"
│   │   ├── "Republican Party" → "Republican"
│   │   ├── "Independent" → "Independent"
│   │   ├── "No Party Affiliation" → "Independent"
│   │   └── Other party mappings
│   ├── Preserve source_party column
│   ├── Handle party variations
│   ├── Handle party abbreviations
│   └── Standardize party names
├── Smart Case Standardization
│   ├── Apply proper case to name columns
│   │   ├── first_name, middle_name, last_name
│   │   ├── prefix, suffix, nickname
│   │   └── Preserve important acronyms
│   ├── Apply title case to office columns
│   ├── Handle special cases and acronyms
│   └── Maintain readability
├── Statewide Candidate Deduplication
│   ├── Remove county-based duplicates for statewide offices
│   ├── Handle candidates running in multiple counties
│   ├── Preserve unique statewide candidates
│   └── Maintain data integrity
├── Election Type Standardization
│   ├── Convert election types to binary columns
│   ├── Primary elections
│   ├── General elections
│   ├── Special elections
│   └── Other election types
└── Final Deduplication
    ├── Remove duplicates based on stable IDs
    ├── Handle exact duplicates
    ├── Handle near-duplicates
    └── Preserve data quality
```

## Phase 5: Final Processing (DataProcessor)
```
National Standardized Data → Final Output
├── Name Case Conversion
│   ├── Convert ALL CAPS names to proper case
│   │   ├── first_name: "JOHN" → "John"
│   │   ├── middle_name: "MICHAEL" → "Michael"
│   │   ├── last_name: "SMITH" → "Smith"
│   │   ├── prefix: "DR" → "Dr"
│   │   ├── suffix: "JR" → "Jr"
│   │   └── nickname: "BOB" → "Bob"
│   ├── Handle None values properly
│   ├── Preserve existing proper case
│   └── Maintain name integrity
├── Email Case Conversion
│   ├── Convert email addresses to lowercase
│   │   ├── "John.Smith@EMAIL.COM" → "john.smith@email.com"
│   │   ├── "JANE@EXAMPLE.COM" → "jane@example.com"
│   │   └── Other email formats
│   ├── Handle None values properly
│   ├── Preserve email functionality
│   └── Standardize email format
├── Address Formatting Cleanup
│   ├── Fix float street numbers
│   │   ├── "123.0 Main St" → "123 Main St"
│   │   ├── "456.0 Oak Ave" → "456 Oak Ave"
│   │   └── Other float number patterns
│   ├── Fix common address formatting issues
│   │   ├── Multiple spaces → single space
│   │   ├── Trailing/leading whitespace
│   │   ├── Inconsistent punctuation
│   │   └── Other formatting issues
│   ├── Clean address readability
│   └── Maintain address accuracy
├── Raw Data Normalization
│   ├── Convert raw_data column to JSON-safe format
│   ├── Handle dictionary objects
│   ├── Handle complex data structures
│   └── Preserve original data
├── Nickname Column Initialization
│   ├── Create nickname column if missing
│   ├── Set default values
│   ├── Handle existing nicknames
│   └── Maintain data structure
├── Stable ID Backfilling
│   ├── Regenerate missing stable IDs
│   ├── Use core fields for deterministic generation
│   ├── Handle edge cases
│   └── Ensure data completeness
└── Final Data Validation
    ├── Check data integrity
    ├── Validate required columns
    ├── Handle any remaining issues
    └── Prepare for output
```

## Output: Final Cleaned Data
```
Final DataFrames with:
├── Standardized Structure
│   ├── Consistent column names
│   ├── Proper data types
│   ├── Required fields present
│   └── Clean data format
├── Complete Address Information
│   ├── parsed_street: Cleaned street address
│   ├── parsed_city: Extracted city name
│   ├── parsed_state: USPS state code
│   ├── parsed_zip: ZIP code
│   └── street_address: Original address (renamed from 'address')
├── Standardized Names
│   ├── first_name: Proper case
│   ├── middle_name: Proper case
│   ├── last_name: Proper case
│   ├── prefix: Proper case
│   ├── suffix: Proper case
│   ├── nickname: Proper case
│   └── full_name_display: Complete display name
├── Standardized Offices
│   ├── office: Standardized office name
│   ├── source_office: Original office name
│   └── office hierarchy information
├── Standardized Parties
│   ├── party: Standardized party name
│   ├── source_party: Original party name
│   └── Party affiliation information
├── Election Information
│   ├── election_year: Year of election
│   ├── election_type: Type of election
│   └── Election-specific data
├── Location Information
│   ├── state: USPS state code
│   ├── county: Standardized county name
│   ├── district: District information
│   └── Geographic data
├── Contact Information
│   ├── email: Lowercase email address
│   ├── phone: Phone number (if available)
│   └── Contact details
├── Metadata
│   ├── stable_id: Unique identifier
│   ├── raw_data: Original data (JSON-safe)
│   ├── source_file: Original file information
│   └── Processing metadata
└── Data Quality
    ├── Validated data
    ├── Deduplicated records
    ├── Complete information
    └── Ready for analysis
```
