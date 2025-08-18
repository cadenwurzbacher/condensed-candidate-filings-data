# Database Schema Migration Guide

## Overview

This guide explains the database schema migration that consolidates office fields and adds enhanced functionality to your CandidateFilings database.

## 🎯 **What We're Implementing**

### **Office Field Consolidation**
- **Before**: Had both `office` (messy) and `office_standardized` (clean) fields
- **After**: `office` becomes the clean, standardized field; `original_office` preserves the raw data
- **Benefit**: Simpler schema, no duplicate fields, users get clean data by default

### **Enhanced Schema Features**
- **Data lineage**: Track which staging record became which filing record
- **Quality metrics**: Office standardization confidence, address parsing success
- **Processing metadata**: Pipeline version, processing timestamps, data sources
- **Audit trail**: Preserve original values while providing clean versions

## 📊 **Schema Changes**

### **Staging Table (`staging_candidates`)**
**New Columns Added:**
- `external_id` - Original state source ID
- `office_confidence` - Standardization confidence (0.00-1.00)
- `office_category` - Federal/State/Local grouping
- `party_standardized` - Cleaned party names
- `address_parsed` - Address parsing success flag
- `address_clean` - Standardized address format
- `has_zip` & `has_state` - Data quality flags
- `source_state` - Track origin state
- `processing_timestamp` - When processed
- `pipeline_version` - Pipeline version used
- `data_source` - Source file/state

**Office Field Changes:**
- `office` - Now contains the standardized, clean office names
- `original_office` - Preserves the original messy office names
- `office_standardized` - Can be dropped after consolidation

### **Production Table (`filings`)**
**New Columns Added:**
- `staging_id` - Link back to staging record
- `external_id` - Original state source ID
- `nickname` - Candidate nickname (was missing!)
- `office_confidence` - Standardization confidence
- `office_category` - Office categorization
- `party_standardized` - Cleaned party names
- `address_parsed` - Address parsing success
- `address_clean` - Standardized address
- `has_zip` & `has_state` - Data quality flags
- `source_state` - Origin state tracking
- `processing_timestamp` - Processing timestamp
- `pipeline_version` - Pipeline version
- `data_source` - Data source information

**Office Field Changes:**
- `office` - Clean, standardized office names (primary field)
- `original_office` - Original messy office names (for audit)

## 🚀 **Implementation Steps**

### **Step 1: Run Schema Migration**
```bash
cd scripts
python migrate_schema.py
```

This will:
- ✅ Create backups of your current tables
- ✅ Add all new columns
- ✅ Consolidate office fields in staging
- ✅ Preserve all existing data

### **Step 2: Update Your Pipeline**
Your pipeline will now populate the new fields:
- `office_confidence` from office standardization
- `office_category` from office classification
- `party_standardized` from party cleaning
- `address_parsed` from address processing
- `processing_timestamp` and `pipeline_version`

### **Step 3: Move to Production**
```bash
cd scripts
python move_to_production.py
```

The updated script will:
- ✅ Use consolidated office fields
- ✅ Populate all new columns
- ✅ Create proper data lineage with `staging_id`
- ✅ Maintain data quality metrics

## 🔍 **Data Flow Example**

### **Before Migration:**
```
Raw Data: "U.S. Senator" → office: "U.S. Senator", office_standardized: "United States Senate"
```

### **After Migration:**
```
Raw Data: "U.S. Senator" → office: "United States Senate", original_office: "U.S. Senator"
```

### **Benefits:**
- Users see clean office names by default
- Original data is preserved for debugging
- No duplicate fields to confuse users
- Simpler queries and data access

## 📈 **Quality Improvements**

### **Office Standardization**
- **Before**: Users had to choose between messy and clean office names
- **After**: Users get clean names by default, with confidence scores
- **Benefit**: Better search, consistent filtering, improved user experience

### **Data Lineage**
- **Before**: No way to trace production records back to staging
- **After**: Full audit trail with `staging_id` linking
- **Benefit**: Better debugging, data validation, compliance

### **Processing Metadata**
- **Before**: No way to track which pipeline version processed what
- **After**: Full processing history with timestamps and versions
- **Benefit**: Quality control, rollback capabilities, process improvement

## 🛡️ **Safety Features**

### **Automatic Backups**
- Tables are backed up before any changes
- Backup names include timestamps
- All original data is preserved

### **Rollback Capability**
- If migration fails, original tables remain intact
- Backups can be restored if needed
- No data loss during migration

### **Incremental Migration**
- Only adds missing columns
- Skips columns that already exist
- Safe to run multiple times

## 🔧 **Troubleshooting**

### **Common Issues**

**Column Already Exists Error:**
- This is normal and safe to ignore
- The migration script handles this gracefully

**Permission Errors:**
- Ensure your database user has ALTER TABLE permissions
- Check that you're connected to the right database

**Data Type Conversion Issues:**
- The migration preserves existing data types
- New columns use appropriate default values

### **Verification Commands**

**Check Migration Status:**
```sql
-- Check if new columns exist
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'filings' 
ORDER BY column_name;
```

**Verify Office Consolidation:**
```sql
-- Check office field consolidation
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN office != original_office THEN 1 END) as standardized_count
FROM staging_candidates;
```

## 📋 **Post-Migration Checklist**

- [ ] Schema migration completed successfully
- [ ] All new columns added to both tables
- [ ] Office fields consolidated in staging
- [ ] Backups created and verified
- [ ] Pipeline updated to populate new fields
- [ ] Test data transfer from staging to production
- [ ] Verify data quality and lineage

## 🎉 **Benefits Summary**

1. **Simpler Schema**: One clean office field instead of two confusing ones
2. **Better Data Quality**: Users get standardized data by default
3. **Full Audit Trail**: Complete data lineage and processing history
4. **Enhanced Functionality**: Office categorization, confidence scores, quality metrics
5. **Future-Proof**: Schema supports incremental processing and advanced features

## 📞 **Support**

If you encounter any issues during migration:
1. Check the migration logs for specific error messages
2. Verify database permissions and connectivity
3. Review the backup tables to ensure data safety
4. Contact the development team with specific error details

---

**Migration Date**: [Date when you run the migration]
**Pipeline Version**: [Version that will populate the new fields]
**Database**: [Your Supabase database name]
