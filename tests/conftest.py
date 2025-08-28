"""
Pytest configuration and common fixtures for CandidateFilings pipeline tests.
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch

# Add src to path for imports
import sys
src_path = Path(__file__).parent.parent / "src"
sys.path.append(str(src_path))

@pytest.fixture
def sample_candidate_data():
    """Sample candidate data for testing."""
    return pd.DataFrame({
        'election_year': [2024, 2024, 2024],
        'election_type': ['General', 'General', 'General'],
        'office': ['US Senate', 'State House', 'Mayor'],
        'district': ['Statewide', 'District 5', 'Citywide'],
        'full_name_display': ['John Smith', 'Jane Doe', 'Bob Johnson'],
        'first_name': ['John', 'Jane', 'Bob'],
        'last_name': ['Smith', 'Doe', 'Johnson'],
        'party': ['Democratic', 'Republican', 'Independent'],
        'address': ['123 Main St, City, ST 12345', '456 Oak Ave, Town, ST 67890', '789 Pine Rd, Village, ST 11111'],
        'state': ['Alaska', 'Arizona', 'Arkansas']
    })

@pytest.fixture
def temp_data_dir():
    """Temporary directory for test data files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir

@pytest.fixture
def mock_database():
    """Mock database connection for testing."""
    mock_db = Mock()
    mock_db.connect.return_value = True
    mock_db.test_connection.return_value = True
    mock_db.disconnect.return_value = None
    mock_db.execute_query.return_value = pd.DataFrame({'test': [1]})
    mock_db.table_exists.return_value = False
    mock_db.upload_dataframe.return_value = True
    return mock_db

@pytest.fixture
def sample_state_files(temp_data_dir):
    """Create sample state data files for testing."""
    files = {}
    
    # Create sample Alaska data
    alaska_data = pd.DataFrame({
        'Election': ['2024 General'],
        'Office': ['US Senate'],
        'Name': ['John Smith'],
        'Party': ['Democratic'],
        'Address': ['123 Main St, Anchorage, AK 99501']
    })
    alaska_file = os.path.join(temp_data_dir, 'alaska_candidates_2024.xlsx')
    alaska_data.to_excel(alaska_file, index=False)
    files['alaska'] = alaska_file
    
    # Create sample Arizona data
    arizona_data = pd.DataFrame({
        'Election': ['2024 General'],
        'Office': ['State House'],
        'Name': ['Jane Doe'],
        'Party': ['Republican'],
        'Address': ['456 Oak Ave, Phoenix, AZ 85001']
    })
    arizona_file = os.path.join(temp_data_dir, 'arizona_candidates_2024.xlsx')
    arizona_data.to_excel(arizona_file, index=False)
    files['arizona'] = arizona_file
    
    return files
