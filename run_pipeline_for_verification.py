#!/usr/bin/env python3
"""
Run Pipeline for Verification

This script runs the refactored pipeline and generates output for comparison
against the baseline data from before refactoring.
"""

import sys
import os
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def main():
    """Run the pipeline and save output."""
    print("=" * 70)
    print("🚀 RUNNING REFACTORED PIPELINE")
    print("=" * 70)
    print()

    try:
        # Import the pipeline
        print("📦 Importing pipeline modules...")
        from pipeline.managers.pipeline_manager import PipelineManager
        from pipeline.config.pipeline_config import PipelineConfig

        # Create config
        print("⚙️  Creating pipeline configuration...")
        config = PipelineConfig(
            data_dir="data",
            enable_phase_1_structural=True,
            enable_phase_3_state_cleaning=True,
            enable_phase_4_national_standards=True,
            enable_phase_5_final=True,
            continue_on_state_error=True
        )

        # Create pipeline manager
        print("🏗️  Initializing pipeline manager...")
        pipeline = PipelineManager(config)

        # Run pipeline
        print("\n" + "=" * 70)
        print("▶️  Starting pipeline execution...")
        print("=" * 70)
        print()

        result_df = pipeline.run()

        # Save output
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"data/final/candidate_filings_REFACTORED_{timestamp}.csv"

        print(f"\n💾 Saving output to: {output_file}")
        result_df.to_csv(output_file, index=False)

        print(f"✅ Pipeline complete!")
        print(f"   Output file: {output_file}")
        print(f"   Rows: {len(result_df):,}")
        print(f"   Columns: {len(result_df.columns)}")
        print(f"   Size: {Path(output_file).stat().st_size / 1024 / 1024:.2f} MB")

        print(f"\n{'=' * 70}")
        print(f"📊 Next step: Run verification script")
        print(f"{'=' * 70}")
        print(f"\nRun: python3 verify_refactoring.py")
        print(f"Then enter: {output_file}")
        print()

        return 0

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
