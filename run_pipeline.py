#!/usr/bin/env python3
"""
Simple Pipeline Runner

This script runs the main CandidateFilings pipeline with configurable options.
"""

import sys
import os
import argparse
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.append(str(src_path))

from pipeline.main_pipeline import MainPipeline
from pipeline.pipeline_config import PipelineConfig

def main():
    """Main function to run the pipeline with command-line options."""
    parser = argparse.ArgumentParser(
        description='Run CandidateFilings Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py                    # Run full pipeline with database
  python run_pipeline.py --no-db            # Run without database operations
  python run_pipeline.py --phases 1 3 5     # Run only phases 1, 3, and 5
  python run_pipeline.py --no-db --phases 4 5  # Run phases 4-5 without database
        """
    )
    
    # Database options
    parser.add_argument('--no-db', action='store_true',
                       help='Disable all database operations (staging, production, smart staging)')

    # Phase control
    parser.add_argument('--phases', nargs='+', choices=['1','2','3','4','5'],
                       help='Enable specific phases (default: all phases enabled)')
    
    args = parser.parse_args()

    # Create configuration
    config = PipelineConfig()

    # Apply command-line options
    if args.no_db:
        config.set_no_database_mode()
        print("📝 Database operations disabled")

    # Phase control
    if args.phases:
        # Disable all phases, then enable specified ones
        config.enable_phase_1_structural = '1' in args.phases
        config.enable_phase_2_id_generation = '2' in args.phases
        config.enable_phase_3_state_cleaning = '3' in args.phases
        config.enable_phase_4_national_standards = '4' in args.phases
        config.enable_phase_5_final_processing = '5' in args.phases
        print(f"🎯 Enabled phases: {', '.join(args.phases)}")
    
    # Initialize pipeline with configuration
    pipeline = MainPipeline(config=config)
    
    # Log pipeline status
    print("🚀 Starting CandidateFilings Pipeline...")
    print("Configuration:")
    print(str(config))
    print()
    
    # Run the pipeline
    try:
        final_data = pipeline.run_pipeline()
        
        if final_data is not None and not final_data.empty:
            print(f"\n✅ Pipeline completed successfully!")
            print(f"📊 Final dataset: {len(final_data)} records")
            print(f"🌍 States represented: {final_data['state'].nunique()}")
            print(f"🏛️ Offices represented: {final_data['office'].nunique()}")
            print("📁 Check the data/final/ directory for output files")
            print("📋 Check the data/logs/ directory for detailed logs")
        else:
            print("\n⚠️ Pipeline completed but no data was processed!")
            print("Check the logs for details.")
            
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        print("Check the logs for error details.")
        raise

if __name__ == "__main__":
    main()
