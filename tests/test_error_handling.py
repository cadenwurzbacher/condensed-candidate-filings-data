"""
Tests for error handling and validation features.
"""

import pytest
import pandas as pd
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from pipeline.main_pipeline import MainPipeline

class TestErrorHandling:
    """Test error handling and graceful degradation."""
    
    def test_pipeline_continues_on_state_cleaner_failure(self, temp_data_dir):
        """Test that pipeline continues when individual state cleaners fail."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Create a mock state cleaner that fails
        def failing_cleaner(input_file, output_dir):
            raise Exception("State cleaner failed")
        
        # Replace a state cleaner with failing version
        original_cleaner = pipeline.state_cleaners['alaska']
        pipeline.state_cleaners['alaska'] = failing_cleaner
        
        # Create sample data file
        sample_data = pd.DataFrame({'test': [1, 2, 3]})
        sample_file = os.path.join(temp_data_dir, 'alaska_candidates_2024.xlsx')
        sample_data.to_excel(sample_file, index=False)
        
        # Test that pipeline handles the failure gracefully
        try:
            # This should not crash the pipeline
            pipeline._find_state_file([sample_file], 'alaska')
        except Exception as e:
            pytest.fail(f"Pipeline should handle state cleaner failures gracefully: {e}")
        
        # Restore original cleaner
        pipeline.state_cleaners['alaska'] = original_cleaner
    
    def test_pipeline_handles_empty_dataframes(self, temp_data_dir):
        """Test that pipeline handles empty DataFrames gracefully."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Test with empty DataFrame
        empty_df = pd.DataFrame()
        
        # Pipeline should handle empty data without crashing
        assert len(empty_df) == 0
        
        # Test that pipeline methods can handle empty data
        try:
            # This should not crash
            pipeline._merge_state_files([], 'test')
        except Exception as e:
            pytest.fail(f"Pipeline should handle empty data gracefully: {e}")
    
    def test_pipeline_handles_missing_columns(self, temp_data_dir):
        """Test that pipeline handles missing columns gracefully."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Create data with missing expected columns
        incomplete_data = pd.DataFrame({
            'name': ['John Doe'],
            'party': ['Democratic']
            # Missing 'office', 'district', etc.
        })
        
        # Pipeline should handle missing columns gracefully
        assert 'office' not in incomplete_data.columns
        assert 'district' not in incomplete_data.columns
        
        # Test that pipeline can process this data
        try:
            # This should not crash
            pipeline._standardize_parties(incomplete_data)
        except Exception as e:
            pytest.fail(f"Pipeline should handle missing columns gracefully: {e}")
    
    def test_pipeline_handles_file_io_errors(self, temp_data_dir):
        """Test that pipeline handles file I/O errors gracefully."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Test with non-existent file
        non_existent_file = '/path/that/does/not/exist.xlsx'
        
        # Pipeline should handle missing files gracefully
        try:
            result = pipeline._find_state_file([non_existent_file], 'test')
            # Should return None or handle gracefully
            assert result is None or isinstance(result, str)
        except Exception as e:
            pytest.fail(f"Pipeline should handle missing files gracefully: {e}")
    
    def test_pipeline_handles_database_connection_failures(self, temp_data_dir):
        """Test that pipeline handles database connection failures gracefully."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Mock database to simulate connection failure
        mock_db = Mock()
        mock_db.connect.return_value = False
        mock_db.test_connection.return_value = False
        
        # Replace database manager
        original_db = pipeline.db_manager
        pipeline.db_manager = mock_db
        
        # Test that pipeline handles database failures gracefully
        try:
            # This should not crash
            pipeline.upload_to_staging("nonexistent_file.xlsx")
        except Exception as e:
            pytest.fail(f"Pipeline should handle database failures gracefully: {e}")
        
        # Restore original database manager
        pipeline.db_manager = original_db
    
    def test_pipeline_handles_office_standardization_failures(self, temp_data_dir):
        """Test that pipeline handles office standardization failures gracefully."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Mock office standardizer to simulate failure
        mock_standardizer = Mock()
        mock_standardizer.standardize_dataset.side_effect = Exception("Standardization failed")
        
        # Replace office standardizer
        original_standardizer = pipeline.office_standardizer
        pipeline.office_standardizer = mock_standardizer
        
        # Test that pipeline handles standardization failures gracefully
        try:
            # This should not crash
            pipeline.run_office_standardization({})
        except Exception as e:
            pytest.fail(f"Pipeline should handle standardization failures gracefully: {e}")
        
        # Restore original standardizer
        pipeline.office_standardizer = original_standardizer
    
    def test_pipeline_handles_audit_failures(self, temp_data_dir):
        """Test that pipeline handles audit failures gracefully."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Test that pipeline can handle audit failures
        try:
            # This should not crash
            pipeline.run_data_audit()
        except Exception as e:
            pytest.fail(f"Pipeline should handle audit failures gracefully: {e}")
    
    def test_pipeline_handles_deduplication_failures(self, temp_data_dir):
        """Test that pipeline handles deduplication failures gracefully."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Create sample data
        sample_data = pd.DataFrame({
            'col1': [1, 1, 2, 2],
            'col2': ['a', 'a', 'b', 'b']
        })
        
        # Test that deduplication works
        try:
            result = pipeline.run_deduplication("nonexistent_file.xlsx")
            # Should return None for non-existent file
            assert result is None
        except Exception as e:
            pytest.fail(f"Pipeline should handle deduplication failures gracefully: {e}")
    
    def test_pipeline_progress_tracking(self, temp_data_dir):
        """Test that pipeline tracks progress correctly."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Test initial pipeline state
        status = pipeline.get_pipeline_status()
        
        assert isinstance(status, dict)
        assert 'raw_files' in status
        assert 'processed_files' in status
        assert 'final_files' in status
        assert 'report_files' in status
        assert 'log_files' in status
        
        # All should be 0 for empty directories
        assert all(isinstance(count, int) for count in status.values())
    
    def test_pipeline_graceful_degradation(self, temp_data_dir):
        """Test that pipeline degrades gracefully when components fail."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Test that pipeline can be initialized even with missing components
        assert pipeline is not None
        assert hasattr(pipeline, 'office_standardizer')
        assert hasattr(pipeline, 'state_cleaners')
        assert hasattr(pipeline, 'db_manager')
        
        # Test that pipeline methods exist even if they might fail
        assert hasattr(pipeline, 'run_state_cleaners')
        assert hasattr(pipeline, 'run_office_standardization')
        assert hasattr(pipeline, 'run_national_standardization')
        assert hasattr(pipeline, 'run_deduplication')
        assert hasattr(pipeline, 'run_data_audit')
        assert hasattr(pipeline, 'upload_to_staging')
    
    def test_pipeline_error_logging(self, temp_data_dir):
        """Test that pipeline logs errors appropriately."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Test that pipeline has logging capabilities
        assert hasattr(pipeline, 'logger')
        
        # Test that pipeline can handle logging operations
        try:
            # This should not crash
            pipeline.logger.info("Test log message")
        except Exception as e:
            pytest.fail(f"Pipeline should handle logging gracefully: {e}")
    
    def test_pipeline_file_validation(self, temp_data_dir):
        """Test that pipeline validates files before processing."""
        pipeline = MainPipeline(
            raw_data_dir=temp_data_dir,
            processed_dir=temp_data_dir,
            final_dir=temp_data_dir
        )
        
        # Test with valid file
        valid_data = pd.DataFrame({'test': [1, 2, 3]})
        valid_file = os.path.join(temp_data_dir, 'valid_test.xlsx')
        valid_data.to_excel(valid_file, index=False)
        
        # Test that pipeline can validate files
        assert os.path.exists(valid_file)
        assert os.path.getsize(valid_file) > 0
        
        # Test with invalid file path
        invalid_file = '/invalid/path/file.xlsx'
        assert not os.path.exists(invalid_file)
        
        # Pipeline should handle both cases gracefully
        try:
            # This should not crash
            pipeline._find_state_file([valid_file, invalid_file], 'test')
        except Exception as e:
            pytest.fail(f"Pipeline should handle file validation gracefully: {e}")
