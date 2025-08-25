-- Add Missing Columns to Database Schema
-- Run this after the main schema migration to add date tracking and action columns

BEGIN;

-- ========================================
-- ADD COLUMNS TO FILINGS TABLE
-- ========================================

-- Add date tracking columns
ALTER TABLE filings 
ADD COLUMN IF NOT EXISTS first_added_date TIMESTAMP,
ADD COLUMN IF NOT EXISTS last_updated_date TIMESTAMP;

-- Set default values for existing records
UPDATE filings 
SET 
    first_added_date = COALESCE(processing_timestamp, NOW()),
    last_updated_date = COALESCE(processing_timestamp, NOW())
WHERE first_added_date IS NULL OR last_updated_date IS NULL;

-- ========================================
-- ADD COLUMNS TO STAGING_CANDIDATES TABLE
-- ========================================

-- Add date tracking columns
ALTER TABLE staging_candidates 
ADD COLUMN IF NOT EXISTS first_added_date TIMESTAMP,
ADD COLUMN IF NOT EXISTS last_updated_date TIMESTAMP;

-- Add action type column for staging operations
ALTER TABLE staging_candidates 
ADD COLUMN IF NOT EXISTS action_type VARCHAR(20) DEFAULT 'INSERT';

-- Set default values for existing records
UPDATE staging_candidates 
SET 
    first_added_date = COALESCE(processing_timestamp, NOW()),
    last_updated_date = COALESCE(processing_timestamp, NOW())
WHERE first_added_date IS NULL OR last_updated_date IS NULL;

-- ========================================
-- VERIFICATION
-- ========================================

-- Check filings table schema
SELECT 'filings' as table_name, column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'filings' 
ORDER BY ordinal_position;

-- Check staging_candidates table schema  
SELECT 'staging_candidates' as table_name, column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'staging_candidates' 
ORDER BY ordinal_position;

-- Show the new columns specifically
SELECT 'filings' as table_name, 'first_added_date' as column_name, 
       (SELECT data_type FROM information_schema.columns WHERE table_name = 'filings' AND column_name = 'first_added_date') as data_type
UNION ALL
SELECT 'filings' as table_name, 'last_updated_date' as column_name,
       (SELECT data_type FROM information_schema.columns WHERE table_name = 'filings' AND column_name = 'last_updated_date') as data_type
UNION ALL
SELECT 'staging_candidates' as table_name, 'first_added_date' as column_name,
       (SELECT data_type FROM information_schema.columns WHERE table_name = 'staging_candidates' AND column_name = 'first_added_date') as data_type
UNION ALL
SELECT 'staging_candidates' as table_name, 'last_updated_date' as column_name,
       (SELECT data_type FROM information_schema.columns WHERE table_name = 'staging_candidates' AND column_name = 'last_updated_date') as data_type
UNION ALL
SELECT 'staging_candidates' as table_name, 'action_type' as column_name,
       (SELECT data_type FROM information_schema.columns WHERE table_name = 'staging_candidates' AND column_name = 'action_type') as data_type;

COMMIT;
