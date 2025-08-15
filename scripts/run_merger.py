#!/usr/bin/env python3
"""
Simple script to run the master data merger.
Just run: python run_merger.py
"""

from master_merger_simple import SimpleMasterMerger

def main():
    print("🚀 Starting Master Data Merger")
    print("=" * 40)
    
    # Initialize merger with default settings
    merger = SimpleMasterMerger()
    
    # Run the full pipeline (now includes deduplication)
    print("Processing all 24 states...")
    output_file = merger.run_full_pipeline(
        use_parallel=True,      # Enable parallel processing
        max_workers=4,          # Use 4 parallel workers
        cleanup_temp=True,      # Clean up temporary files
        remove_duplicates=True  # Remove exact duplicates (default: True)
    )
    
    if output_file:
        print(f"\n✅ Success! Merged and deduplicated data saved to:")
        print(f"   {output_file}")
        print(f"\n📊 Check the 'merged_data' directory for additional reports")
        print(f"🔍 The pipeline automatically removed exact duplicates")
    else:
        print("\n❌ Merger failed. Check the logs for details.")

if __name__ == "__main__":
    main()
