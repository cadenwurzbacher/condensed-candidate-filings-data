#!/usr/bin/env python3
"""
Pipeline Runner Without Database

This script runs the main CandidateFilings pipeline without database connectivity.
It sets environment variables that will cause the database connection to fail gracefully,
allowing the pipeline to run in file-only mode.
"""

import sys
import os
from pathlib import Path

# Set environment variables to disable database connection
os.environ['SUPABASE_HOST'] = 'localhost'
os.environ['SUPABASE_USER'] = 'dummy'
os.environ['SUPABASE_PASSWORD'] = 'dummy'
os.environ['SUPABASE_DATABASE'] = 'dummy'
os.environ['SUPABASE_PORT'] = '5432'

# Add src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.append(str(src_path))

from pipeline.main_pipeline import main

if __name__ == "__main__":
    print("🚀 Starting CandidateFilings Pipeline (No Database Mode)...")
    print("📝 Database connection will be disabled - pipeline will run in file-only mode")
    print("📁 Output will be saved to data/final/ directory")
    print()
    
    main()
    
    print("\n✅ Pipeline completed in file-only mode!")
    print("📊 Check the data/final/ directory for output files")
    print("📋 Check the data/logs/ directory for detailed logs")
