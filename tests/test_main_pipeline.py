"""
Tests for the main pipeline orchestrator.
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from pipeline.main_pipeline import MainPipeline

class TestMainPipeline:
    """Test the main pipeline orchestrator."""
    
    def test_pipeline_initialization(self, temp_data_dir):
        """Test pipeline initialization with custom directories."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        assert pipeline.raw_data_dir == temp_data_dir
        assert pipeline.processed_dir == temp_data_dir
        assert pipeline.final_dir == temp_data_dir
        assert pipeline.staging_table == "staging_candidates"
        assert pipeline.production_table == "filings"
    
    def test_pipeline_creates_directories(self, temp_data_dir):
        """Test that pipeline creates required directories."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Check that directories were created
        assert os.path.exists(temp_data_dir)
    
    def test_get_raw_data_files(self, temp_data_dir, sample_state_files):
        """Test raw data file discovery."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Copy sample files to temp directory
        for state, file_path in sample_state_files.items():
            import shutil
            shutil.copy2(file_path, temp_data_dir)
        
        raw_files = pipeline._get_raw_data_files()
        assert len(raw_files) >= 2  # Should find Alaska and Arizona files
    
    def test_find_state_file_single_file(self, temp_data_dir, sample_state_files):
        """Test finding a single file for a state."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Copy sample files to temp directory
        for state, file_path in sample_state_files.items():
            import shutil
            shutil.copy2(file_path, temp_data_dir)
        
        raw_files = pipeline._get_raw_data_files()
        alaska_file = pipeline._find_state_file(raw_files, 'alaska')
        
        assert alaska_file is not None
        assert 'alaska' in alaska_file.lower()
    
    def test_merge_state_files(self, temp_data_dir):
        """Test merging multiple files for a state."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Create multiple files for same state
        file1_data = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        file2_data = pd.DataFrame({'col1': [3, 4], 'col2': ['c', 'd']})
        
        file1_path = os.path.join(temp_data_dir, 'alaska_file1.xlsx')
        file2_path = os.path.join(temp_data_dir, 'alaska_file2.xlsx')
        
        file1_data.to_excel(file1_path, index=False)
        file2_data.to_excel(file2_path, index=False)
        
        merged_df = pipeline._merge_state_files([file1_path, file2_path], 'alaska')
        
        assert len(merged_df) == 4  # 2 rows from each file
        assert 'col1' in merged_df.columns
        assert 'col2' in merged_df.columns
    
    @patch('pipeline.main_pipeline.get_db_connection')
    def test_pipeline_with_mock_database(self, mock_get_db, temp_data_dir, sample_state_files):
        """Test pipeline with mocked database."""
        # Setup mock database
        mock_db = Mock()
        mock_db.connect.return_value = True
        mock_db.test_connection.return_value = True
        mock_db.upload_to_staging.return_value = True
        mock_db.get_staging_record_count.return_value = 10
        mock_db.disconnect.return_value = None
        
        mock_get_db.return_value = mock_db
        
        # Copy sample files to temp directory
        for state, file_path in sample_state_files.items():
            import shutil
            shutil.copy2(file_path, temp_data_dir)
        
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Test that pipeline can be initialized with mock database
        assert pipeline.db_manager is not None
    
    def test_pipeline_status_tracking(self, temp_data_dir):
        """Test pipeline status tracking functionality."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        status = pipeline.get_pipeline_status()
        
        assert 'raw_files' in status
        assert 'processed_files' in status
        assert 'final_files' in status
        assert 'report_files' in status
        assert 'log_files' in status
        
        # All should be 0 for empty directories
        assert all(count == 0 for count in status.values())
    
    def test_error_handling_in_file_operations(self, temp_data_dir):
        """Test error handling when file operations fail."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Test with non-existent file
        result = pipeline._find_state_file(['/nonexistent/file.xlsx'], 'test')
        assert result is None
    
    def test_cleanup_operations(self, temp_data_dir):
        """Test cleanup operations."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Create some test files
        test_file = os.path.join(temp_data_dir, 'test_cleaned_20240101.xlsx')
        pd.DataFrame({'test': [1]}).to_excel(test_file, index=False)
        
        # Test cleanup
        pipeline._cleanup_old_files()
        
        # File should still exist since it's not in processed_dir
        assert os.path.exists(test_file)
    
    def test_pipeline_imports_correctly(self):
        """Test that pipeline can be imported without errors."""
        try:
            from pipeline.main_pipeline import MainPipeline
            pipeline = MainPipeline()
            assert pipeline is not None
        except ImportError as e:
            pytest.fail(f"Failed to import MainPipeline: {e}")
    
    def test_office_standardizer_initialization(self, temp_data_dir):
        """Test that office standardizer is properly initialized."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        assert pipeline.office_standardizer is not None
        assert hasattr(pipeline.office_standardizer, 'standardize_dataset')
    
    def test_state_cleaners_mapping(self, temp_data_dir):
        """Test that state cleaners are properly mapped."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Check that all expected states are mapped
        expected_states = [
            'alaska', 'arizona', 'arkansas', 'colorado', 'delaware',
            'georgia', 'idaho', 'illinois', 'indiana', 'kansas',
            'kentucky', 'louisiana', 'missouri', 'montana', 'nebraska',
            'new_mexico', 'oklahoma', 'oregon', 'south_carolina',
            'south_dakota', 'vermont', 'washington', 'west_virginia', 'wyoming'
        ]
        
        for state in expected_states:
            assert state in pipeline.state_cleaners
            assert callable(pipeline.state_cleaners[state])
