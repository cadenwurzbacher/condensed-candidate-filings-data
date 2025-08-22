#!/usr/bin/env python3
"""
Simple Pipeline Runner

This script runs the main CandidateFilings pipeline.
"""

import sys
import os
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent.parent / "src"
sys.path.append(str(src_path))

from pipeline.main_pipeline import main

if __name__ == "__main__":
    print("🚀 Starting CandidateFilings Pipeline...")
    main()
