# 🔬 Refactoring Verification Instructions

This guide explains how to verify that the refactored pipeline code produces **identical output** to the original code, ensuring no logic was changed during refactoring.

---

## 📋 Overview

We've created two scripts to help you verify the refactoring:

1. **`run_pipeline_for_verification.py`** - Runs the refactored pipeline and generates new output
2. **`verify_refactoring.py`** - Compares new output against baseline (original) output

---

## 🚀 Quick Start

### Step 1: Run the Refactored Pipeline

```bash
python3 run_pipeline_for_verification.py
```

This will:
- Run the refactored pipeline with all your data
- Generate a new output file: `data/final/candidate_filings_REFACTORED_YYYYMMDD_HHMMSS.csv`
- Print the output file path when complete

**Expected output:**
```
🚀 RUNNING REFACTORED PIPELINE
======================================================================

📦 Importing pipeline modules...
⚙️  Creating pipeline configuration...
🏗️  Initializing pipeline manager...

======================================================================
▶️  Starting pipeline execution...
======================================================================

[Pipeline execution logs...]

✅ Pipeline complete!
   Output file: data/final/candidate_filings_REFACTORED_20251015_160000.csv
   Rows: 123,456
   Columns: 45
   Size: 148.23 MB
```

### Step 2: Run the Verification Script

```bash
python3 verify_refactoring.py
```

When prompted, enter the path to your new output file (copy from Step 1):
```
Enter path to new output file (or press Enter to use latest):
data/final/candidate_filings_REFACTORED_20251015_160000.csv
```

---

## 📊 What the Verification Script Checks

The verification script performs a comprehensive comparison:

### 1. **File Hash Comparison** (Fastest)
- Computes SHA256 hash of both files
- If hashes match → files are 100% identical (verification complete!)
- If hashes differ → proceeds to detailed comparison

### 2. **Row Count Comparison**
- Compares number of rows in both files
- Reports any differences

### 3. **Column Count Comparison**
- Compares number of columns in both files
- Reports any differences

### 4. **Column Name Comparison**
- Checks that all column names match exactly
- Reports missing or extra columns

### 5. **Data Value Comparison** (Most Detailed)
- Compares every cell in every row and column
- Reports which columns have differences
- Shows sample mismatches for debugging

---

## ✅ Success Criteria

You'll see this if everything matches perfectly:

```
======================================================================
🎉 VERIFICATION COMPLETE: FILES ARE IDENTICAL
======================================================================
```

Or for detailed comparison:

```
======================================================================
📊 VERIFICATION SUMMARY
======================================================================
✅ SUCCESS: Data is identical!
   • Row counts match: ✓
   • Column counts match: ✓
   • Column names match: ✓
   • Data values match: ✓

🎉 Refactoring preserved all logic perfectly!
```

---

## ⚠️ If Differences Are Found

If the verification finds differences, you'll see:

```
======================================================================
📊 VERIFICATION SUMMARY
======================================================================
❌ DIFFERENCES FOUND:
   • Row counts match: ✗
   • Column counts match: ✓
   • Column names match: ✓
   • Data values match: ✗

📋 Differences:
   - Row count: baseline=123456, new=123450
   - Data mismatches in 3 columns, 127 total differences

⚠️  Refactoring may have changed logic - review differences above.
```

### Debugging Steps:

1. **Review the detailed output** - The script shows which columns differ and sample values
2. **Check recent changes** - Review the refactored code for that state/cleaner
3. **Compare specific rows** - Load both CSVs and compare specific problem rows
4. **Check for sorting differences** - Sometimes row order changes (data is same, just reordered)

---

## 🔍 Manual Verification (Alternative)

If you prefer to compare files manually:

### Using pandas:
```python
import pandas as pd

baseline = pd.read_csv('data/final/candidate_filings_FINAL_20251015_152653.csv')
new = pd.read_csv('data/final/candidate_filings_REFACTORED_20251015_160000.csv')

# Check basic stats
print(f"Baseline: {len(baseline)} rows, {len(baseline.columns)} cols")
print(f"New:      {len(new)} rows, {len(new.columns)} cols")

# Check if DataFrames are equal
print(baseline.equals(new))

# Find differences
if not baseline.equals(new):
    # Check which columns differ
    for col in baseline.columns:
        if col in new.columns:
            if not baseline[col].equals(new[col]):
                print(f"Column '{col}' differs")
```

### Using command line:
```bash
# Compare file sizes
ls -lh data/final/candidate_filings_*.csv

# Compare line counts
wc -l data/final/candidate_filings_*.csv

# Binary comparison (exact match)
cmp data/final/candidate_filings_FINAL_*.csv \
    data/final/candidate_filings_REFACTORED_*.csv
```

---

## 📝 Notes

### Baseline File
The verification script automatically uses the **most recent** file from `data/final/` as the baseline. Make sure your original (pre-refactoring) output is in this directory.

### Pandas Required
Both scripts require pandas to be installed:
```bash
pip install pandas
# or
uv pip install pandas
```

### Large Files
For very large files (>1GB), the comparison may take several minutes. The script will show progress indicators.

### Deterministic Output
Make sure your pipeline produces deterministic output (same input → same output every time). Non-deterministic elements like timestamps or random IDs may cause false positives.

---

## 🎯 Expected Results

Given that the refactoring:
- Created base classes (BaseStructuralCleaner, NameParser, FieldExtractor)
- Eliminated duplicate code
- Used dynamic imports
- **Did NOT change any data processing logic**

The verification should show **100% identical output**. Any differences would indicate an unintended logic change that needs investigation.

---

## 📞 Troubleshooting

### "No module named 'pandas'"
```bash
pip install pandas
# or with uv
uv pip install pandas
```

### "Baseline directory not found"
Make sure you're running the scripts from the project root directory and `data/final/` exists.

### "No baseline CSV files found"
Make sure you have at least one `candidate_filings_FINAL_*.csv` file in `data/final/`.

### Memory errors with large files
If files are very large (>2GB), you may need to:
- Increase Python memory limits
- Use chunked reading
- Compare files in batches

---

## ✨ Summary

1. Run: `python3 run_pipeline_for_verification.py`
2. Run: `python3 verify_refactoring.py`
3. Enter the new output file path
4. Review the comparison results
5. Celebrate if everything matches! 🎉

The verification provides **confidence** that your refactoring improved code quality without changing any data processing logic!
