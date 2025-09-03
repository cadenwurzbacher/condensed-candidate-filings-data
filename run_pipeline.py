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
  python run_pipeline.py --debug            # Run in debug mode (save all files)
  python run_pipeline.py --memory-only       # Run in memory-only mode
  python run_pipeline.py --phases 1 3 5     # Run only phases 1, 3, and 5
        """
    )
    
    # Database options
    parser.add_argument('--no-db', action='store_true', 
                       help='Disable all database operations (staging, production, smart staging)')
    
    # Processing modes
    parser.add_argument('--debug', action='store_true', 
                       help='Enable debug mode (save all intermediate files and reports)')
    parser.add_argument('--memory-only', action='store_true', 
                       help='Memory-only processing (minimal file output, only final file)')
    parser.add_argument('--file-based', action='store_true', 
                       help='File-based processing (save all intermediate files)')
    
    # Phase control
    parser.add_argument('--phases', nargs='+', choices=['1','2','3','4','5'], 
                       help='Enable specific phases (default: all phases enabled)')
    parser.add_argument('--skip-phases', nargs='+', choices=['1','2','3','4','5'], 
                       help='Skip specific phases (alternative to --phases)')
    
    # Data processing options
    parser.add_argument('--no-address-parsing', action='store_true',
                       help='Disable address parsing')
    parser.add_argument('--no-office-standardization', action='store_true',
                       help='Disable office standardization')
    parser.add_argument('--no-party-standardization', action='store_true',
                       help='Disable party standardization')
    parser.add_argument('--no-deduplication', action='store_true',
                       help='Disable deduplication')
    
    # File output options
    parser.add_argument('--no-final-file', action='store_true',
                       help='Skip saving final output file')
    parser.add_argument('--no-audit-reports', action='store_true',
                       help='Skip generating audit reports')
    
    # Performance options
    parser.add_argument('--parallel', action='store_true',
                       help='Enable parallel processing')
    parser.add_argument('--workers', type=int, default=4,
                       help='Number of parallel workers (default: 4)')
    
    # Error handling
    parser.add_argument('--continue-on-error', action='store_true',
                       help='Continue processing even if individual states fail')
    parser.add_argument('--retry-failed', action='store_true',
                       help='Retry failed states (default: enabled)')
    
    args = parser.parse_args()
    
    # Create configuration
    config = PipelineConfig()
    
    # Apply command-line options
    if args.no_db:
        config.set_no_database_mode()
        print("📝 Database operations disabled")
    
    if args.debug:
        config.set_debug_mode()
        print("🐛 Debug mode enabled")
    
    if args.memory_only:
        config.set_memory_only_mode()
        print("💾 Memory-only mode enabled")
    
    if args.file_based:
        config.set_file_based_mode()
        print("📁 File-based mode enabled")
    
    # Phase control
    if args.phases:
        # Disable all phases, then enable specified ones
        config.enable_phase_1_structural = '1' in args.phases
        config.enable_phase_2_id_generation = '2' in args.phases
        config.enable_phase_3_state_cleaning = '3' in args.phases
        config.enable_phase_4_national_standards = '4' in args.phases
        config.enable_phase_5_final_processing = '5' in args.phases
        print(f"🎯 Enabled phases: {args.phases}")
    
    if args.skip_phases:
        # Disable specified phases
        if '1' in args.skip_phases:
            config.enable_phase_1_structural = False
        if '2' in args.skip_phases:
            config.enable_phase_2_id_generation = False
        if '3' in args.skip_phases:
            config.enable_phase_3_state_cleaning = False
        if '4' in args.skip_phases:
            config.enable_phase_4_national_standards = False
        if '5' in args.skip_phases:
            config.enable_phase_5_final_processing = False
        print(f"⏭️ Skipped phases: {args.skip_phases}")
    
    # Data processing options
    if args.no_address_parsing:
        config.enable_address_parsing = False
    if args.no_office_standardization:
        config.enable_office_standardization = False
    if args.no_party_standardization:
        config.enable_party_standardization = False
    if args.no_deduplication:
        config.enable_deduplication = False
    
    # File output options
    if args.no_final_file:
        config.save_final_file = False
    if args.no_audit_reports:
        config.save_audit_reports = False
    
    # Performance options
    if args.parallel:
        config.enable_parallel_processing = True
        config.max_workers = args.workers
        print(f"⚡ Parallel processing enabled with {args.workers} workers")
    
    # Error handling
    if args.continue_on_error:
        config.continue_on_state_error = True
        config.continue_on_phase_error = True
    if not args.retry_failed:
        config.retry_failed_states = False
    
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
