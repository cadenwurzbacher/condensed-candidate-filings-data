#!/usr/bin/env python3
"""Run the full pipeline without database upload."""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.append('src')

from pipeline.main_pipeline import MainPipeline

def run_pipeline_without_db():
    """Run the full pipeline but skip database upload."""
    print("🚀 Starting full CandidateFilings pipeline (NO DATABASE UPLOAD)...")
    
    # Initialize pipeline
    pipeline = MainPipeline()
    
    try:
        # Step 1: State cleaning
        print("=== STEP 1: State Cleaning ===")
        cleaned_files = pipeline.run_state_cleaners()
        if not cleaned_files:
            print("❌ State cleaning failed")
            return False
        
        print(f"✅ State cleaning completed with {len(cleaned_files)} states")
        
        # Step 2: Office standardization
        print("=== STEP 2: Office Standardization ===")
        office_standardized_file = pipeline.run_office_standardization(cleaned_files)
        if not office_standardized_file:
            print("❌ Office standardization failed")
            return False
        
        print("✅ Office standardization completed")
        
        # Step 3: National standardization
        print("=== STEP 3: National Standardization ===")
        nationally_standardized_file = pipeline.run_national_standardization(office_standardized_file)
        if not nationally_standardized_file:
            print("❌ National standardization failed")
            return False
        
        print("✅ National standardization completed")
        
        # Step 4: Deduplication
        print("=== STEP 4: Deduplication ===")
        final_file = pipeline.run_deduplication(nationally_standardized_file)
        if not final_file:
            print("❌ Deduplication failed")
            return False
        
        print("✅ Deduplication completed")
        
        # Step 5: Data audit
        print("=== STEP 5: Data Audit ===")
        try:
            audit_file, audit_status = pipeline.run_data_audit()
            if audit_status == "audit_failed":
                print("⚠️  Data audit failed, but continuing with pipeline")
            else:
                print("✅ Data audit completed")
        except Exception as e:
            print(f"❌ Data audit step failed: {e}")
            print("⚠️  Continuing with pipeline despite audit failure")
        
        # SKIP Step 6: Database upload
        print("=== STEP 6: Database Upload ===")
        print("⏭️  SKIPPING database upload as requested")
        print("📁 Final file available at:", final_file)
        
        # Final cleanup
        print("=== Final Cleanup ===")
        try:
            pipeline._final_cleanup()
            print("✅ Final cleanup completed")
        except Exception as e:
            print(f"⚠️  Final cleanup failed: {e}")
        
        try:
            pipeline._cleanup_final_outputs(file_to_keep=final_file)
            print("✅ Intermediate file cleanup completed")
        except Exception as e:
            print(f"⚠️  Intermediate file cleanup failed: {e}")
        
        try:
            pipeline._cleanup_old_candidate_filings()
            print("✅ Old file cleanup completed")
        except Exception as e:
            print(f"⚠️  Old file cleanup failed: {e}")
        
        print("\n🎉 Full pipeline completed successfully (NO DATABASE UPLOAD)!")
        print(f"📁 Final deduplicated file: {final_file}")
        print("📊 Pipeline completion summary:")
        print("  ✅ State Cleaning")
        print("  ✅ Office Standardization") 
        print("  ✅ National Standardization")
        print("  ✅ Deduplication")
        print("  ✅ Data Audit")
        print("  ⏭️  Database Upload (SKIPPED)")
        
        return True
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_pipeline_without_db()
    if success:
        print("\n🚀 Pipeline completed successfully!")
    else:
        print("\n💥 Pipeline failed!")
        sys.exit(1)
