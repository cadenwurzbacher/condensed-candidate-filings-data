#!/usr/bin/env python3
"""
Refactoring Verification Script

This script compares the output from the refactored pipeline code against
the baseline output from before refactoring to ensure no logic was changed.

It performs detailed row-by-row and column-by-column comparisons to verify
that the refactoring preserved all data processing logic exactly.
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
import hashlib


def get_latest_baseline_file(baseline_dir: str) -> Path:
    """Find the most recent baseline output file."""
    baseline_path = Path(baseline_dir)

    if not baseline_path.exists():
        raise FileNotFoundError(f"Baseline directory not found: {baseline_dir}")

    # Find all CSV files
    csv_files = list(baseline_path.glob("candidate_filings_FINAL_*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No baseline CSV files found in {baseline_dir}")

    # Get the most recent file
    latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)

    print(f"📁 Using baseline file: {latest_file.name}")
    print(f"   Modified: {datetime.fromtimestamp(latest_file.stat().st_mtime)}")
    print(f"   Size: {latest_file.stat().st_size / 1024 / 1024:.2f} MB")

    return latest_file


def compute_file_hash(filepath: Path) -> str:
    """Compute SHA256 hash of file."""
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()


def compare_dataframes(baseline_df: pd.DataFrame, new_df: pd.DataFrame, verbose: bool = True) -> dict:
    """
    Perform detailed comparison of two DataFrames.

    Returns dict with comparison results.
    """
    results = {
        'identical': False,
        'row_count_match': False,
        'column_count_match': False,
        'column_names_match': False,
        'data_match': False,
        'differences': [],
        'warnings': []
    }

    # 1. Check row counts
    baseline_rows = len(baseline_df)
    new_rows = len(new_df)
    results['row_count_match'] = (baseline_rows == new_rows)

    if verbose:
        print(f"\n📊 Row Count Comparison:")
        print(f"   Baseline: {baseline_rows:,} rows")
        print(f"   New:      {new_rows:,} rows")
        if results['row_count_match']:
            print(f"   ✅ Row counts match!")
        else:
            diff = new_rows - baseline_rows
            print(f"   ❌ Row counts differ by {abs(diff):,} rows ({'+' if diff > 0 else ''}{diff})")
            results['differences'].append(f"Row count: baseline={baseline_rows}, new={new_rows}")

    # 2. Check column counts
    baseline_cols = len(baseline_df.columns)
    new_cols = len(new_df.columns)
    results['column_count_match'] = (baseline_cols == new_cols)

    if verbose:
        print(f"\n📋 Column Count Comparison:")
        print(f"   Baseline: {baseline_cols} columns")
        print(f"   New:      {new_cols} columns")
        if results['column_count_match']:
            print(f"   ✅ Column counts match!")
        else:
            print(f"   ❌ Column counts differ by {abs(new_cols - baseline_cols)}")
            results['differences'].append(f"Column count: baseline={baseline_cols}, new={new_cols}")

    # 3. Check column names
    baseline_columns = set(baseline_df.columns)
    new_columns = set(new_df.columns)

    missing_in_new = baseline_columns - new_columns
    extra_in_new = new_columns - baseline_columns

    results['column_names_match'] = (baseline_columns == new_columns)

    if verbose:
        print(f"\n🏷️  Column Name Comparison:")
        if results['column_names_match']:
            print(f"   ✅ All column names match!")
        else:
            print(f"   ❌ Column names differ:")
            if missing_in_new:
                print(f"   Missing in new: {missing_in_new}")
                results['differences'].append(f"Missing columns: {missing_in_new}")
            if extra_in_new:
                print(f"   Extra in new: {extra_in_new}")
                results['differences'].append(f"Extra columns: {extra_in_new}")

    # 4. Compare data values (only for matching columns)
    if results['row_count_match'] and results['column_names_match']:
        if verbose:
            print(f"\n🔍 Data Value Comparison:")
            print(f"   Comparing {len(baseline_df):,} rows × {len(baseline_df.columns)} columns...")

        # Sort both DataFrames by all columns for consistent comparison
        try:
            # Get common columns
            common_cols = list(baseline_columns.intersection(new_columns))

            # Sort by all columns (or a subset if there are too many)
            sort_cols = common_cols[:5] if len(common_cols) > 5 else common_cols
            baseline_sorted = baseline_df[common_cols].sort_values(by=sort_cols).reset_index(drop=True)
            new_sorted = new_df[common_cols].sort_values(by=sort_cols).reset_index(drop=True)

            # Compare each column
            mismatches = []
            for col in common_cols:
                # Handle NaN values properly
                baseline_col = baseline_sorted[col].fillna('__NA__')
                new_col = new_sorted[col].fillna('__NA__')

                # Compare as strings to handle different types
                baseline_col = baseline_col.astype(str)
                new_col = new_col.astype(str)

                if not baseline_col.equals(new_col):
                    # Count differences
                    diff_count = (baseline_col != new_col).sum()
                    mismatches.append((col, diff_count))

                    if verbose and diff_count < 10:
                        # Show sample differences for small mismatch counts
                        diff_indices = baseline_col[baseline_col != new_col].index[:5]
                        print(f"\n   Column '{col}' has {diff_count} differences:")
                        for idx in diff_indices:
                            print(f"      Row {idx}: '{baseline_col.iloc[idx]}' → '{new_col.iloc[idx]}'")

            if mismatches:
                results['data_match'] = False
                total_mismatches = sum(count for _, count in mismatches)
                if verbose:
                    print(f"\n   ❌ Found {len(mismatches)} columns with differences:")
                    for col, count in mismatches[:10]:  # Show first 10
                        print(f"      {col}: {count:,} different values")
                    if len(mismatches) > 10:
                        print(f"      ... and {len(mismatches) - 10} more columns")
                results['differences'].append(f"Data mismatches in {len(mismatches)} columns, {total_mismatches:,} total differences")
            else:
                results['data_match'] = True
                if verbose:
                    print(f"   ✅ All data values match perfectly!")

        except Exception as e:
            if verbose:
                print(f"   ⚠️  Could not compare data values: {e}")
            results['warnings'].append(f"Data comparison error: {e}")
    else:
        if verbose:
            print(f"\n⚠️  Skipping data comparison (row/column structure differs)")
        results['warnings'].append("Data comparison skipped due to structural differences")

    # Set overall result
    results['identical'] = (
        results['row_count_match'] and
        results['column_count_match'] and
        results['column_names_match'] and
        results['data_match']
    )

    return results


def main():
    """Main verification function."""
    print("=" * 70)
    print("🔬 REFACTORING VERIFICATION SCRIPT")
    print("=" * 70)
    print()

    # Configuration
    baseline_dir = "data/final"
    new_output_dir = "data/final"  # Will be the newly generated file

    try:
        # 1. Find baseline file
        print("📂 Step 1: Locating baseline file...")
        baseline_file = get_latest_baseline_file(baseline_dir)

        # 2. Ask user for new output file or run pipeline
        print("\n" + "=" * 70)
        print("📝 Note: Please run your pipeline to generate new output, then provide the path.")
        print("   Or provide the path to an existing comparison file.")
        print("=" * 70)

        # For now, let's use the most recent file as both (you'll run the pipeline separately)
        new_file_path = input("\nEnter path to new output file (or press Enter to use latest): ").strip()

        if not new_file_path:
            # Use the latest file (for testing)
            new_file = baseline_file
            print(f"⚠️  Using baseline file for both (for testing purposes)")
        else:
            new_file = Path(new_file_path)
            if not new_file.exists():
                print(f"❌ File not found: {new_file}")
                return 1

        # 3. Compute file hashes
        print(f"\n🔐 Step 2: Computing file hashes...")
        baseline_hash = compute_file_hash(baseline_file)
        new_hash = compute_file_hash(new_file)

        print(f"   Baseline hash: {baseline_hash[:16]}...")
        print(f"   New hash:      {new_hash[:16]}...")

        if baseline_hash == new_hash:
            print(f"   ✅ File hashes match - files are identical!")
            print(f"\n{'=' * 70}")
            print(f"🎉 VERIFICATION COMPLETE: FILES ARE IDENTICAL")
            print(f"{'=' * 70}")
            return 0
        else:
            print(f"   ℹ️  File hashes differ - performing detailed comparison...")

        # 4. Load DataFrames
        print(f"\n📥 Step 3: Loading data files...")
        print(f"   Loading baseline... ", end='', flush=True)
        baseline_df = pd.read_csv(baseline_file, low_memory=False)
        print(f"✓ ({len(baseline_df):,} rows)")

        print(f"   Loading new file... ", end='', flush=True)
        new_df = pd.read_csv(new_file, low_memory=False)
        print(f"✓ ({len(new_df):,} rows)")

        # 5. Perform comparison
        print(f"\n🔍 Step 4: Performing detailed comparison...")
        results = compare_dataframes(baseline_df, new_df, verbose=True)

        # 6. Print summary
        print(f"\n{'=' * 70}")
        print(f"📊 VERIFICATION SUMMARY")
        print(f"{'=' * 70}")

        if results['identical']:
            print(f"✅ SUCCESS: Data is identical!")
            print(f"   • Row counts match: ✓")
            print(f"   • Column counts match: ✓")
            print(f"   • Column names match: ✓")
            print(f"   • Data values match: ✓")
            print(f"\n🎉 Refactoring preserved all logic perfectly!")
            return 0
        else:
            print(f"❌ DIFFERENCES FOUND:")
            print(f"   • Row counts match: {'✓' if results['row_count_match'] else '✗'}")
            print(f"   • Column counts match: {'✓' if results['column_count_match'] else '✗'}")
            print(f"   • Column names match: {'✓' if results['column_names_match'] else '✗'}")
            print(f"   • Data values match: {'✓' if results['data_match'] else '✗'}")

            if results['differences']:
                print(f"\n📋 Differences:")
                for diff in results['differences']:
                    print(f"   - {diff}")

            if results['warnings']:
                print(f"\n⚠️  Warnings:")
                for warn in results['warnings']:
                    print(f"   - {warn}")

            print(f"\n⚠️  Refactoring may have changed logic - review differences above.")
            return 1

    except Exception as e:
        print(f"\n❌ Error during verification: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        print(f"\n{'=' * 70}")


if __name__ == "__main__":
    sys.exit(main())
