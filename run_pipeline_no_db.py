#!/usr/bin/env python3
"""
Run Pipeline Without Database Upload

This script runs the CandidateFilings pipeline up to Step 5 (Data Audit),
stopping before the database upload step.
"""

import sys
import os
from pathlib import Path

# Add the src directory to Python path
sys.path.append(str(Path(__file__).parent / "src"))

from pipeline.main_pipeline import MainPipeline
import logging

def run_pipeline_without_db():
    """Run the pipeline up to Step 5 (before database upload)."""
    
    # Create a custom pipeline class that overrides the database upload
    class PipelineWithoutDB(MainPipeline):
        def run_full_pipeline(self) -> bool:
            """Run the complete pipeline from start to finish, stopping before database upload."""
            logger = logging.getLogger(__name__)
            logger.info("🚀 Starting CandidateFilings pipeline (NO DATABASE UPLOAD)...")
            
            # Track pipeline progress for recovery
            pipeline_state = {
                'state_cleaning_completed': False,
                'office_standardization_completed': False,
                'national_standardization_completed': False,
                'deduplication_completed': False,
                'audit_completed': False,
                'staging_upload_completed': False  # Will remain False
            }
            
            try:
                # Step 1: State cleaning
                logger.info("=== STEP 1: State Cleaning ===")
                cleaned_files = self.run_state_cleaners()
                if not cleaned_files:
                    logger.error("State cleaning failed")
                    return False
                
                pipeline_state['state_cleaning_completed'] = True
                logger.info(f"✅ State cleaning completed with {len(cleaned_files)} states")
                
                # Step 2: Office standardization
                logger.info("=== STEP 2: Office Standardization ===")
                office_standardized_file = self.run_office_standardization(cleaned_files)
                if not office_standardized_file:
                    logger.error("Office standardization failed")
                    return False
                
                pipeline_state['office_standardization_completed'] = True
                logger.info("✅ Office standardization completed")
                
                # Step 3: National standardization
                logger.info("=== STEP 3: National Standardization ===")
                nationally_standardized_file = self.run_national_standardization(office_standardized_file)
                if not nationally_standardized_file:
                    logger.error("National standardization failed")
                    return False
                
                pipeline_state['national_standardization_completed'] = True
                logger.info("✅ National standardization completed")
                
                # Step 4: Deduplication
                logger.info("=== STEP 4: Deduplication ===")
                final_file = self.run_deduplication(nationally_standardized_file)
                if not final_file:
                    logger.error("Deduplication failed")
                    return False
                
                pipeline_state['deduplication_completed'] = True
                logger.info("✅ Deduplication completed")
                
                # Step 5: Data audit
                logger.info("=== STEP 5: Data Audit ===")
                try:
                    audit_file, audit_status = self.run_data_audit()
                    if audit_status == "audit_failed":
                        logger.warning("Data audit failed, but continuing with pipeline")
                    else:
                        pipeline_state['audit_completed'] = True
                        logger.info("✅ Data audit completed")
                except Exception as e:
                    logger.error(f"Data audit step failed: {e}")
                    logger.warning("Continuing with pipeline despite audit failure")
                
                # STOP HERE - Skip database upload
                logger.info("🛑 STOPPING PIPELINE BEFORE DATABASE UPLOAD")
                logger.info("📁 Final output file created at: " + str(final_file))
                logger.info("📋 To upload to database later, run the full pipeline or use the database scripts")
                
                # Log pipeline completion summary
                logger.info("🎉 Pipeline completed successfully (NO DATABASE UPLOAD)!")
                logger.info("📊 Pipeline completion summary:")
                for step, completed in pipeline_state.items():
                    status = "✅" if completed else "❌"
                    step_name = step.replace('_', ' ').title()
                    logger.info(f"  {status} {step_name}")
                
                # Final cleanup - keep only the latest cleaned file per state
                try:
                    self._final_cleanup()
                except Exception as e:
                    logger.warning(f"Final cleanup failed: {e}")
                    # Don't fail the pipeline for cleanup issues
                
                # Remove intermediate final outputs so only one final file remains
                try:
                    self._cleanup_final_outputs(file_to_keep=final_file)
                except Exception as e:
                    logger.warning(f"Could not clean up intermediate final outputs: {e}")
                
                # Clean up old candidate_filings files, keep only the most recent
                try:
                    self._cleanup_old_candidate_filings()
                except Exception as e:
                    logger.warning(f"Could not clean up old candidate_filings files: {e}")
                
                return True
                
            except Exception as e:
                logger.error(f"❌ Pipeline failed: {e}")
                return False
    
    # Run the modified pipeline
    pipeline = PipelineWithoutDB()
    
    # Run the pipeline without database upload
    success = pipeline.run_full_pipeline()
    
    if success:
        print("\n✅ Pipeline completed successfully (NO DATABASE UPLOAD)!")
        print("📁 Check the output files in data/final/ for the processed data")
        print("📋 To upload to database later, run the full pipeline or use the database scripts")
    else:
        print("\n❌ Pipeline failed!")
        print("Check the logs for error details.")

if __name__ == "__main__":
    run_pipeline_without_db()
