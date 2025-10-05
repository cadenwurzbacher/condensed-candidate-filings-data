#!/usr/bin/env python3
"""
Test runner for CandidateFilings pipeline tests.
"""

import sys
import subprocess
from pathlib import Path

def run_tests():
    """Run all tests using pytest."""
    print("🧪 Running CandidateFilings Pipeline Tests...")
    print("=" * 50)
    
    # Get the tests directory
    tests_dir = Path(__file__).parent
    
    # Run pytest
    try:
        result = subprocess.run([
            sys.executable, '-m', 'pytest', 
            str(tests_dir),
            '-v',  # Verbose output
            '--tb=short',  # Short traceback format
            '--color=yes'  # Colored output
        ], capture_output=False, text=True)
        
        if result.returncode == 0:
            print("\n🎉 All tests passed!")
            return True
        else:
            print(f"\n❌ Some tests failed (exit code: {result.returncode})")
            return False
            
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False

def run_specific_test(test_file):
    """Run a specific test file."""
    print(f"🧪 Running specific test: {test_file}")
    print("=" * 50)
    
    test_path = Path(__file__).parent / test_file
    
    if not test_path.exists():
        print(f"❌ Test file not found: {test_path}")
        return False
    
    try:
        result = subprocess.run([
            sys.executable, '-m', 'pytest', 
            str(test_path),
            '-v',
            '--tb=short',
            '--color=yes'
        ], capture_output=False, text=True)
        
        if result.returncode == 0:
            print(f"\n🎉 Test {test_file} passed!")
            return True
        else:
            print(f"\n❌ Test {test_file} failed!")
            return False
            
    except Exception as e:
        print(f"❌ Error running test {test_file}: {e}")
        return False

def main():
    """Main test runner function."""
    if len(sys.argv) > 1:
        # Run specific test file
        test_file = sys.argv[1]
        success = run_specific_test(test_file)
    else:
        # Run all tests
        success = run_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
