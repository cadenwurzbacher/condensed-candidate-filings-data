"""
Tests for the database connection and operations.
"""

import pytest
import pandas as pd
import os
from unittest.mock import Mock, patch, MagicMock
from unittest.mock import patch

# Mock environment variables for testing
@pytest.fixture(autouse=True)
def mock_env_vars():
    """Mock environment variables for database testing."""
    with patch.dict(os.environ, {
        'SUPABASE_HOST': 'test.supabase.co',
        'SUPABASE_PORT': '5432',
        'SUPABASE_DATABASE': 'postgres',
        'SUPABASE_USER': 'test_user',
        'SUPABASE_PASSWORD': 'test_password'
    }):
        yield

class TestDatabaseManager:
    """Test the database manager component."""
    
    @patch('sqlalchemy.create_engine')
    def test_database_initialization(self, mock_create_engine):
        """Test database manager initialization."""
        from config.database import DatabaseManager
        
        # Mock the engine creation
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        db_manager = DatabaseManager()
        
        assert db_manager.engine is None
        assert 'test.supabase.co' in db_manager.connection_string
        assert 'test_user' in db_manager.connection_string
        assert 'test_password' in db_manager.connection_string
    
    @patch('sqlalchemy.create_engine')
    def test_database_connection_success(self, mock_create_engine):
        """Test successful database connection."""
        from config.database import DatabaseManager
        
        # Mock the engine and connection
        mock_engine = Mock()
        mock_connection = Mock()
        mock_connection.execute.return_value = Mock()
        
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        db_manager = DatabaseManager()
        
        # Test connection
        result = db_manager.connect()
        
        assert result is True
        assert db_manager.engine is not None
        mock_create_engine.assert_called_once()
    
    @patch('sqlalchemy.create_engine')
    def test_database_connection_failure(self, mock_create_engine):
        """Test database connection failure handling."""
        from config.database import DatabaseManager
        from sqlalchemy.exc import SQLAlchemyError
        
        # Mock connection failure
        mock_create_engine.side_effect = SQLAlchemyError("Connection failed")
        
        db_manager = DatabaseManager()
        
        # Test connection failure
        result = db_manager.connect()
        
        assert result is False
        assert db_manager.engine is None
    
    @patch('sqlalchemy.create_engine')
    def test_database_disconnect(self, mock_create_engine):
        """Test database disconnection."""
        from config.database import DatabaseManager
        
        # Mock the engine
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        db_manager = DatabaseManager()
        db_manager.engine = mock_engine
        
        # Test disconnection
        db_manager.disconnect()
        
        mock_engine.dispose.assert_called_once()
    
    @patch('sqlalchemy.create_engine')
    def test_database_test_connection(self, mock_create_engine):
        """Test database connection testing."""
        from config.database import DatabaseManager
        
        # Mock the engine and connection
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = ['PostgreSQL 14.0']
        
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        db_manager = DatabaseManager()
        db_manager.engine = mock_engine
        
        # Test connection test
        result = db_manager.test_connection()
        
        assert result is True
        mock_connection.execute.assert_called_once()
    
    @patch('sqlalchemy.create_engine')
    def test_database_execute_query(self, mock_create_engine):
        """Test database query execution."""
        from config.database import DatabaseManager
        
        # Mock the engine and connection
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.returns_rows = True
        mock_result.fetchall.return_value = [['test_value']]
        mock_result.keys.return_value = ['test_column']
        
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        db_manager = DatabaseManager()
        db_manager.engine = mock_engine
        
        # Test query execution
        result = db_manager.execute_query("SELECT test")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert 'test_column' in result.columns
        assert result.iloc[0, 0] == 'test_value'
    
    @patch('sqlalchemy.create_engine')
    def test_database_upload_dataframe(self, mock_create_engine):
        """Test DataFrame upload to database."""
        from config.database import DatabaseManager
        
        # Mock the engine
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        db_manager = DatabaseManager()
        db_manager.engine = mock_engine
        
        # Create test DataFrame
        test_df = pd.DataFrame({'test_col': [1, 2, 3]})
        
        # Test upload
        result = db_manager.upload_dataframe(test_df, 'test_table')
        
        assert result is True
    
    @patch('sqlalchemy.create_engine')
    def test_database_table_exists(self, mock_create_engine):
        """Test table existence checking."""
        from config.database import DatabaseManager
        
        # Mock the engine and connection
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.iloc = [[True]]  # Table exists
        
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        db_manager = DatabaseManager()
        db_manager.engine = mock_engine
        
        # Test table existence check
        result = db_manager.table_exists('test_table')
        
        assert result is True
    
    @patch('sqlalchemy.create_engine')
    def test_database_get_table_info(self, mock_create_engine):
        """Test getting table information."""
        from config.database import DatabaseManager
        
        # Mock the engine and connection
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.returns_rows = True
        mock_result.fetchall.return_value = [
            ['id', 'integer', 'NO', None],
            ['name', 'character varying', 'YES', None]
        ]
        mock_result.keys.return_value = ['column_name', 'data_type', 'is_nullable', 'column_default']
        
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        db_manager = DatabaseManager()
        db_manager.engine = mock_engine
        
        # Test getting table info
        result = db_manager.get_table_info('test_table')
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'column_name' in result.columns
        assert 'data_type' in result.columns
    
    @patch('sqlalchemy.create_engine')
    def test_database_clear_staging_table(self, mock_create_engine):
        """Test clearing staging table."""
        from config.database import DatabaseManager
        
        # Mock the engine and connection
        mock_engine = Mock()
        mock_connection = Mock()
        mock_connection.execute.return_value = None
        mock_connection.commit.return_value = None
        
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        db_manager = DatabaseManager()
        db_manager.engine = mock_engine
        
        # Mock table exists
        with patch.object(db_manager, 'table_exists', return_value=True):
            result = db_manager.clear_staging_table()
            
            assert result is True
            mock_connection.execute.assert_called_once()
            mock_connection.commit.assert_called_once()
    
    @patch('sqlalchemy.create_engine')
    def test_database_upload_to_staging(self, mock_create_engine):
        """Test uploading data to staging table."""
        from config.database import DatabaseManager
        
        # Mock the engine
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        db_manager = DatabaseManager()
        db_manager.engine = mock_engine
        
        # Mock successful operations
        with patch.object(db_manager, 'test_connection', return_value=True), \
             patch.object(db_manager, 'table_exists', return_value=True), \
             patch.object(db_manager, 'upload_dataframe', return_value=True):
            
            # Create test DataFrame
            test_df = pd.DataFrame({'test_col': [1, 2, 3]})
            
            # Test upload to staging
            result = db_manager.upload_to_staging(test_df)
            
            assert result is True
    
    def test_database_missing_environment_variables(self):
        """Test database manager with missing environment variables."""
        from config.database import DatabaseManager
        
        # Clear environment variables
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="SUPABASE_HOST, SUPABASE_USER, and SUPABASE_PASSWORD must be set"):
                db_manager = DatabaseManager()
                db_manager._get_connection_string()
    
    @patch('sqlalchemy.create_engine')
    def test_database_chunked_upload(self, mock_create_engine):
        """Test chunked DataFrame upload for large datasets."""
        from config.database import DatabaseManager
        
        # Mock the engine
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        db_manager = DatabaseManager()
        db_manager.engine = mock_engine
        
        # Create large test DataFrame
        large_df = pd.DataFrame({'test_col': range(15000)})  # More than 10000 rows
        
        # Mock the chunked upload method
        with patch.object(db_manager, '_upload_dataframe_chunked', return_value=True):
            result = db_manager.upload_dataframe(large_df, 'test_table')
            
            assert result is True
            # Should call chunked upload for large datasets
            db_manager._upload_dataframe_chunked.assert_called_once()
    
    def test_database_connection_string_format(self):
        """Test database connection string format."""
        from config.database import DatabaseManager
        
        with patch.dict(os.environ, {
            'SUPABASE_HOST': 'test.supabase.co',
            'SUPABASE_PORT': '5432',
            'SUPABASE_DATABASE': 'postgres',
            'SUPABASE_USER': 'test_user',
            'SUPABASE_PASSWORD': 'test_password'
        }):
            db_manager = DatabaseManager()
            connection_string = db_manager._get_connection_string()
            
            assert 'postgresql://' in connection_string
            assert 'test.supabase.co:5432' in connection_string
            assert 'test_user:test_password' in connection_string
            assert 'sslmode=require' in connection_string
