-- Database Schema Update Script (FIXED VERSION)
-- Run this in your Supabase SQL editor to update both tables

-- Target columns (28 total)
-- election_year, election_type, office, district, full_name_display, first_name, middle_name, last_name, 
-- prefix, suffix, nickname, party, phone, email, address, website, state, county, city, zip_code, 
-- address_state, filing_date, election_date, facebook, twitter, processing_timestamp, pipeline_version, data_source

-- ========================================
-- UPDATE FILINGS TABLE
-- ========================================

-- Create new filings table with correct schema
CREATE TABLE filings_new (
    election_year INTEGER,
    election_type VARCHAR(100),
    office TEXT,
    district VARCHAR(200),
    full_name_display TEXT,
    first_name VARCHAR(200),
    middle_name VARCHAR(200),
    last_name VARCHAR(200),
    prefix VARCHAR(100),
    suffix VARCHAR(100),
    nickname VARCHAR(200),
    party VARCHAR(200),
    phone VARCHAR(100),
    email TEXT,
    address TEXT,
    website TEXT,
    state VARCHAR(100),
    county VARCHAR(200),
    city VARCHAR(200),
    zip_code VARCHAR(50),
    address_state VARCHAR(100),
    filing_date DATE,
    election_date DATE,
    facebook TEXT,
    twitter TEXT,
    processing_timestamp TIMESTAMP,
    pipeline_version VARCHAR(100),
    data_source VARCHAR(200)
);

-- Copy data from old filings table (only matching columns)
-- Handle missing columns and date conversion safely
INSERT INTO filings_new (
    election_year, election_type, office, district, full_name_display, 
    first_name, middle_name, last_name, prefix, suffix, nickname, 
    party, phone, email, address, website, state, county, city, 
    zip_code, address_state, filing_date, election_date, facebook, 
    twitter, processing_timestamp, pipeline_version, data_source
)
SELECT 
    election_year, 
    election_type, 
    office, 
    district, 
    full_name_display, 
    first_name, 
    middle_name, 
    last_name, 
    prefix, 
    suffix, 
    nickname, 
    party, 
    phone, 
    email, 
    address, 
    website, 
    state, 
    county, 
    city, 
    zip_code, 
    NULL as address_state,  -- Column doesn't exist in old table
    CASE 
        WHEN filing_date IS NULL OR filing_date = '' OR filing_date = 'NULL' OR filing_date = 'null' THEN NULL
        ELSE NULLIF(filing_date, '')::DATE
    END as filing_date, 
    CASE 
        WHEN election_date IS NULL OR election_date = '' OR election_date = 'NULL' OR election_date = 'null' THEN NULL
        ELSE NULLIF(election_date, '')::DATE
    END as election_date, 
    facebook, 
    twitter, 
    processing_timestamp, 
    pipeline_version, 
    data_source
FROM filings;

-- Drop old table and rename new one
DROP TABLE filings;
ALTER TABLE filings_new RENAME TO filings;

-- ========================================
-- UPDATE STAGING_CANDIDATES TABLE
-- ========================================

-- Create new staging_candidates table with correct schema
CREATE TABLE staging_candidates_new (
    election_year INTEGER,
    election_type VARCHAR(100),
    office TEXT,
    district VARCHAR(200),
    full_name_display TEXT,
    first_name VARCHAR(200),
    middle_name VARCHAR(200),
    last_name VARCHAR(200),
    prefix VARCHAR(100),
    suffix VARCHAR(100),
    nickname VARCHAR(200),
    party VARCHAR(200),
    phone VARCHAR(100),
    email TEXT,
    address TEXT,
    website TEXT,
    state VARCHAR(100),
    county VARCHAR(200),
    city VARCHAR(200),
    zip_code VARCHAR(50),
    address_state VARCHAR(100),
    filing_date DATE,
    election_date DATE,
    facebook TEXT,
    twitter TEXT,
    processing_timestamp TIMESTAMP,
    pipeline_version VARCHAR(100),
    data_source VARCHAR(200)
);

-- Copy data from old staging_candidates table (only matching columns)
-- Handle missing columns and date conversion safely
INSERT INTO staging_candidates_new (
    election_year, election_type, office, district, full_name_display, 
    first_name, middle_name, last_name, prefix, suffix, nickname, 
    party, phone, email, address, website, state, county, city, 
    zip_code, address_state, filing_date, election_date, facebook, 
    twitter, processing_timestamp, pipeline_version, data_source
)
SELECT 
    election_year, 
    election_type, 
    office, 
    district, 
    full_name_display, 
    first_name, 
    middle_name, 
    last_name, 
    prefix, 
    suffix, 
    nickname, 
    party, 
    phone, 
    email, 
    address, 
    website, 
    state, 
    county, 
    city, 
    zip_code, 
    NULL as address_state,  -- Column doesn't exist in old table
    CASE 
        WHEN filing_date IS NULL OR filing_date = '' OR filing_date = 'NULL' OR filing_date = 'null' THEN NULL
        ELSE NULLIF(filing_date, '')::DATE
    END as filing_date, 
    CASE 
        WHEN election_date IS NULL OR election_date = '' OR election_date = 'NULL' OR election_date = 'null' THEN NULL
        ELSE NULLIF(election_date, '')::DATE
    END as election_date, 
    facebook, 
    twitter, 
    processing_timestamp, 
    pipeline_version, 
    data_source
FROM staging_candidates;

-- Drop old table and rename new one
DROP TABLE staging_candidates;
ALTER TABLE staging_candidates_new RENAME TO staging_candidates;

-- ========================================
-- VERIFICATION
-- ========================================

-- Check filings table schema
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'filings' 
ORDER BY ordinal_position;

-- Check staging_candidates table schema
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'staging_candidates' 
ORDER BY ordinal_position;

-- Count records in each table
SELECT 'filings' as table_name, COUNT(*) as record_count FROM filings
UNION ALL
SELECT 'staging_candidates' as table_name, COUNT(*) as record_count FROM staging_candidates;
